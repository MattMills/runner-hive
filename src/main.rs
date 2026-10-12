//! runner-hive: one small daemon that serves GitHub Actions jobs for many
//! repositories, instead of one heavyweight self-hosted runner per repo.

mod config;
mod github;
mod hive;
mod http;
mod identity;
mod listener;
mod oauth;
mod protocol;
mod register;
mod slots;
mod sync;
mod util;
mod worker;

use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};

use crate::config::Config;
use crate::github::GitHub;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Path to the config file.
    #[arg(
        short,
        long,
        global = true,
        default_value = "hive.toml",
        env = "RUNNER_HIVE_CONFIG"
    )]
    config: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Write an example config file.
    Init,
    /// Register runners for every target, re-label or prune as needed.
    Sync {
        /// Show what would change without touching GitHub.
        #[arg(long)]
        dry_run: bool,
    },
    /// Run the daemon: listen for jobs on every registration.
    Run {
        /// Skip the sync on startup.
        #[arg(long)]
        no_sync: bool,
    },
    /// List local registrations (and their GitHub status with --remote).
    Status {
        #[arg(long)]
        remote: bool,
    },
    /// Remove registrations from GitHub and delete their local credentials.
    Unregister {
        /// Remove every registration.
        #[arg(long)]
        all: bool,
        /// "owner/repo" or "org" scopes to remove.
        scopes: Vec<String>,
    },
    /// Download (or update) the official worker package.
    InstallWorker,
    /// Check that the official worker starts and reads messages from us.
    SelfTest,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_target(false)
        .init();

    let cli = Cli::parse();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    rt.block_on(async_main(cli))
}

async fn async_main(cli: Cli) -> Result<()> {
    if let Command::Init = cli.command {
        if cli.config.exists() {
            bail!("{} already exists", cli.config.display());
        }
        std::fs::write(&cli.config, config::EXAMPLE)?;
        println!(
            "wrote {}; edit the [[target]] section, then run `runner-hive sync`",
            cli.config.display()
        );
        return Ok(());
    }

    let cfg = Config::load(&cli.config)?;
    std::fs::create_dir_all(&cfg.data_dir)
        .with_context(|| format!("cannot create data_dir {}", cfg.data_dir.display()))?;

    match cli.command {
        Command::Init => unreachable!(),
        Command::Run { no_sync } => hive::run(cfg, !no_sync).await,
        Command::Sync { dry_run } => {
            let gh = github_client(&cfg)?;
            let pkg = worker::package::ensure(&cfg, &gh).await?;
            let report = sync::reconcile(&cfg, &gh, &pkg.version, dry_run).await?;
            report.log(dry_run);
            if !report.failed.is_empty() {
                bail!("{} registrations failed", report.failed.len());
            }
            Ok(())
        }
        Command::Status { remote } => status(&cfg, remote).await,
        Command::Unregister { all, scopes } => {
            let gh = github_client(&cfg)?;
            let wanted: Vec<identity::Scope> =
                scopes.iter().map(|s| s.parse()).collect::<Result<_>>()?;
            if !all && wanted.is_empty() {
                bail!("name scopes to remove, or pass --all");
            }
            let mut failed = 0;
            for id in identity::load_all(&cfg.data_dir)? {
                if !all && !wanted.contains(&id.scope) {
                    continue;
                }
                match register::unregister(&gh, &id, &cfg.data_dir).await {
                    Ok(()) => tracing::info!("removed {}", id.display_key()),
                    Err(e) => {
                        failed += 1;
                        tracing::error!("{}: {e:#}", id.display_key());
                    }
                }
            }
            if failed > 0 {
                bail!("{failed} removals failed");
            }
            Ok(())
        }
        Command::InstallWorker => {
            let gh = github_client(&cfg)?;
            let pkg = worker::package::ensure(&cfg, &gh).await?;
            println!("{} {}", pkg.version, pkg.root.display());
            Ok(())
        }
        Command::SelfTest => {
            let gh = github_client(&cfg)?;
            let pkg = worker::package::ensure(&cfg, &gh).await?;
            worker::self_test(&cfg.data_dir, &pkg).await?;
            println!(
                "ok: Runner.Worker {} received and parsed a job message over the pipe",
                pkg.version
            );
            Ok(())
        }
    }
}

fn github_client(cfg: &Config) -> Result<GitHub> {
    let token = cfg.resolve_token()?;
    Ok(GitHub::new(http::build_client()?, &cfg.github.url, token))
}

async fn status(cfg: &Config, remote: bool) -> Result<()> {
    let ids = identity::load_all(&cfg.data_dir)?;
    if ids.is_empty() {
        println!("no registrations in {}", cfg.data_dir.display());
        return Ok(());
    }
    let gh = if remote {
        Some(github_client(cfg)?)
    } else {
        None
    };
    for id in &ids {
        let mut line = format!(
            "{:<50} {:<24} id={:<8} labels={}",
            id.scope.to_string(),
            id.name,
            id.agent_id,
            id.labels.join(",")
        );
        if let Some(gh) = &gh {
            let state = match gh.list_runners(&id.scope).await {
                Ok(rs) => match rs.iter().find(|r| r.id == id.agent_id) {
                    Some(r) if r.busy => format!("{} (busy)", r.status),
                    Some(r) => r.status.clone(),
                    None => "MISSING on GitHub".into(),
                },
                Err(e) => format!("error: {e:#}"),
            };
            line.push_str(&format!("  {state}"));
        }
        println!("{line}");
    }
    println!("{} registrations", ids.len());
    Ok(())
}
