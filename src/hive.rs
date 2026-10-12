//! The daemon: one listener task per registration, a shared slot pool, and
//! periodic re-sync of targets and the worker package.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Result, bail};
use tokio::sync::watch;
use tokio::task::JoinSet;

use crate::config::Config;
use crate::github::GitHub;
use crate::identity::{self, Identity};
use crate::listener::{Listener, ListenerExit, Shared};
use crate::slots::Slots;
use crate::sync;
use crate::worker::package;

struct Running {
    stop: watch::Sender<bool>,
}

pub async fn run(cfg: Config, initial_sync: bool) -> Result<()> {
    let token = cfg.resolve_token()?;
    let http = crate::http::build_client()?;
    let gh = GitHub::new(http.clone(), &cfg.github.url, token);

    let pkg = package::ensure(&cfg, &gh).await?;
    tracing::info!("jobs run with the official Runner.Worker {}", pkg.version);

    if initial_sync && !cfg.targets.is_empty() {
        match sync::reconcile(&cfg, &gh, &pkg.version, false).await {
            Ok(r) => r.log(false),
            Err(e) => tracing::error!("sync failed, continuing with existing registrations: {e:#}"),
        }
    }

    let identities = identity::load_all(&cfg.data_dir)?;
    if identities.is_empty() {
        bail!("no registrations; add a [[target]] to the config and run `runner-hive sync`");
    }

    let mut env_remove = cfg.secret_env_vars();
    env_remove.push("RUST_LOG".into());
    let shared = Arc::new(Shared {
        clients: Default::default(),
        package: RwLock::new(Arc::new(pkg)),
        slots: Slots::new(cfg.max_jobs),
        data_dir: cfg.data_dir.clone(),
        env_set: cfg
            .env
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        env_remove,
        log_output: cfg.worker.log_output,
        clean_workspace: cfg.worker.clean_workspace,
        run_as: crate::resolve_run_as(&cfg)?,
        slot_home: cfg.worker.slot_home,
    });

    let mut tasks: JoinSet<(String, ListenerExit)> = JoinSet::new();
    let mut running: HashMap<String, Running> = HashMap::new();
    // Stagger startup so hundreds of registrations do not all request OAuth
    // tokens and sessions in the same instant.
    for (i, id) in identities.into_iter().enumerate() {
        let delay = Duration::from_millis(40 * i as u64);
        start(&shared, &mut tasks, &mut running, id, delay);
    }
    tracing::info!(
        "runner-hive: {} registrations listening, up to {} jobs at once",
        running.len(),
        cfg.max_jobs
    );

    let sync_enabled = cfg.sync_interval_minutes > 0;
    let sync_every = Duration::from_secs(cfg.sync_interval_minutes.max(1) * 60);
    let mut sync_timer =
        tokio::time::interval_at(tokio::time::Instant::now() + sync_every, sync_every);
    let mut resync_soon = false;
    let mut retry_timer = tokio::time::interval(Duration::from_secs(120));

    loop {
        tokio::select! {
            _ = shutdown_signal() => break,
            Some(joined) = tasks.join_next() => {
                let Ok((key, exit)) = joined else { continue };
                running.remove(&key);
                match exit {
                    ListenerExit::Shutdown => {}
                    ListenerExit::RegistrationGone(why) => {
                        // Deleted on GitHub (or pruned by us). If we still want
                        // it, the next sync registers it again.
                        tracing::warn!("{key}: registration is gone ({why})");
                        if let Some(id) = find_identity(&cfg, &key) {
                            let _ = id.remove_file(&cfg.data_dir);
                            resync_soon = true;
                        }
                    }
                    ListenerExit::Fatal(why) => {
                        tracing::error!("{key}: listener stopped: {why}; retrying in a few minutes");
                        resync_soon = true;
                    }
                }
            }
            _ = sync_timer.tick(), if sync_enabled => {
                refresh(&cfg, &gh, &shared).await;
                restart_missing(&cfg, &shared, &mut tasks, &mut running);
            }
            _ = retry_timer.tick(), if resync_soon => {
                resync_soon = false;
                refresh(&cfg, &gh, &shared).await;
                restart_missing(&cfg, &shared, &mut tasks, &mut running);
            }
        }
    }

    tracing::info!(
        "shutting down: closing sessions and stopping running jobs (Ctrl-C again to force)"
    );
    for r in running.values() {
        let _ = r.stop.send(true);
    }
    tokio::select! {
        _ = async { while tasks.join_next().await.is_some() {} } => {}
        _ = shutdown_signal() => {
            tracing::warn!("forced exit");
            std::process::exit(130);
        }
    }
    Ok(())
}

fn start(
    shared: &Arc<Shared>,
    tasks: &mut JoinSet<(String, ListenerExit)>,
    running: &mut HashMap<String, Running>,
    id: Identity,
    delay: Duration,
) {
    let key = id.display_key();
    if running.contains_key(&key) {
        return;
    }
    let (tx, rx) = watch::channel(false);
    let http = match shared.clients.lock().expect("client pool poisoned").next() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("{key}: cannot create an HTTP client: {e:#}");
            return;
        }
    };
    match Listener::new(shared.clone(), id, rx, http) {
        Ok(listener) => {
            let k = key.clone();
            tasks.spawn(async move {
                tokio::time::sleep(delay).await;
                (k, listener.run().await)
            });
            running.insert(key, Running { stop: tx });
        }
        Err(e) => tracing::error!("{key}: cannot start: {e:#}"),
    }
}

fn find_identity(cfg: &Config, key: &str) -> Option<Identity> {
    identity::load_all(&cfg.data_dir)
        .ok()?
        .into_iter()
        .find(|i| i.display_key() == key)
}

/// Pick up new worker releases and new/removed repos.
async fn refresh(cfg: &Config, gh: &GitHub, shared: &Arc<Shared>) {
    if cfg.worker.path.is_none() && cfg.worker.version == "latest" {
        match package::ensure(cfg, gh).await {
            Ok(pkg) => {
                let current = shared.package();
                if pkg.version != current.version {
                    tracing::info!(
                        "worker updated {} -> {}; new jobs use it",
                        current.version,
                        pkg.version
                    );
                    *shared.package.write().expect("package lock poisoned") = Arc::new(pkg);
                }
            }
            Err(e) => tracing::warn!("worker update check failed: {e:#}"),
        }
    }
    if !cfg.targets.is_empty() {
        let version = shared.package().version.clone();
        match sync::reconcile(cfg, gh, &version, false).await {
            Ok(r)
                if r.registered.is_empty()
                    && r.removed.is_empty()
                    && r.relabeled.is_empty()
                    && r.failed.is_empty() => {}
            Ok(r) => r.log(false),
            Err(e) => tracing::warn!("sync failed: {e:#}"),
        }
    }
}

/// Start listeners for registrations that exist on disk but are not running,
/// and stop the ones whose registration was removed.
fn restart_missing(
    cfg: &Config,
    shared: &Arc<Shared>,
    tasks: &mut JoinSet<(String, ListenerExit)>,
    running: &mut HashMap<String, Running>,
) {
    let Ok(ids) = identity::load_all(&cfg.data_dir) else {
        return;
    };
    let keys: std::collections::HashSet<String> = ids.iter().map(|i| i.display_key()).collect();
    for (k, r) in running.iter() {
        if !keys.contains(k) {
            let _ = r.stop.send(true);
        }
    }
    for id in ids {
        start(shared, tasks, running, id, Duration::ZERO);
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate()).expect("cannot install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
