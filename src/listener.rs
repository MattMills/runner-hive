//! One listener per registration: the message loop of Runner.Listener
//! (Runner.cs + MessageListener.cs / BrokerMessageListener.cs), minus
//! everything that costs resources while idle. An idle listener is one
//! pending HTTP long-poll.

use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use reqwest::Client;
use tokio::sync::{mpsc, watch};

use crate::http::{http_error_of, status_of};
use crate::identity::Identity;
use crate::oauth::{TokenSource, oauth_error_of};
use crate::protocol::{
    self, AgentReference, BrokerMigration, CreateSession, EMPTY_GUID, JobCancelMessage,
    RunnerJobRequestRef, TaskAgentMessage, broker, message_types as mt, pipelines, run_service,
};
use crate::slots::Slots;
use crate::worker::package::WorkerPackage;
use crate::worker::{self, JobControl, JobOutcome, JobRun};

/// State shared by every listener in the hive.
pub struct Shared {
    pub http: Client,
    pub package: RwLock<Arc<WorkerPackage>>,
    pub slots: Arc<Slots>,
    pub data_dir: PathBuf,
    pub env_set: Vec<(String, String)>,
    pub env_remove: Vec<String>,
    pub log_output: bool,
    pub clean_workspace: bool,
}

impl Shared {
    pub fn package(&self) -> Arc<WorkerPackage> {
        self.package.read().expect("package lock poisoned").clone()
    }

    fn runner_version(&self) -> String {
        self.package().version.clone()
    }
}

#[derive(Debug)]
pub enum ListenerExit {
    Shutdown,
    /// The registration no longer exists on GitHub; re-register to recover.
    RegistrationGone(String),
    Fatal(String),
}

#[derive(Clone)]
enum Mode {
    Broker { url: String },
    Pipelines { migrated_broker: Option<String> },
}

#[derive(Clone)]
struct Session {
    id: String,
    mode: Mode,
    aes_key: Option<Vec<u8>>,
    last_message_id: Option<i64>,
}

impl Session {
    /// Broker URL for acknowledgements, if this session talks to a broker.
    fn broker_url(&self) -> Option<&str> {
        match &self.mode {
            Mode::Broker { url } => Some(url),
            Mode::Pipelines { migrated_broker } => migrated_broker.as_deref(),
        }
    }
}

struct Polled {
    message: TaskAgentMessage,
    from_pipelines: bool,
    migrated_broker: Option<String>,
}

enum Failure {
    SessionExpired,
    RegistrationGone(String),
    VersionTooOld(String),
    Transient,
}

fn classify(err: &anyhow::Error) -> Failure {
    if let Some(o) = oauth_error_of(err)
        && o.registration_deleted()
    {
        return Failure::RegistrationGone(o.to_string());
    }
    if let Some(h) = http_error_of(err) {
        match h.broker_error_kind().as_deref() {
            Some("RunnerNotFound") => return Failure::RegistrationGone(h.to_string()),
            Some("RunnerVersionTooOld") => return Failure::VersionTooOld(h.to_string()),
            Some("RunnerSessionInvalid") => return Failure::SessionExpired,
            _ => {}
        }
        if h.body.contains("TaskAgentSessionExpiredException") {
            return Failure::SessionExpired;
        }
        if h.body.contains("TaskAgentNotFoundException")
            || h.body.contains("TaskAgentPoolNotFoundException")
        {
            return Failure::RegistrationGone(h.to_string());
        }
    }
    Failure::Transient
}

struct RunningJob {
    control: mpsc::Sender<JobControl>,
    handle: tokio::task::JoinHandle<()>,
}

pub struct Listener {
    shared: Arc<Shared>,
    identity: Arc<Identity>,
    tokens: Arc<TokenSource>,
    key: rsa::RsaPrivateKey,
    shutdown: watch::Receiver<bool>,
    tag: String,
}

impl Listener {
    pub fn new(
        shared: Arc<Shared>,
        identity: Identity,
        shutdown: watch::Receiver<bool>,
    ) -> Result<Self> {
        let identity = Arc::new(identity);
        let tokens = Arc::new(TokenSource::new(shared.http.clone(), identity.clone())?);
        let key = identity.key()?;
        let tag = identity.display_key();
        Ok(Self {
            shared,
            identity,
            tokens,
            key,
            shutdown,
            tag,
        })
    }

    fn stopping(&self) -> bool {
        *self.shutdown.borrow()
    }

    pub async fn run(mut self) -> ListenerExit {
        let mut session = match self.open_session().await {
            Ok(Some(s)) => s,
            Ok(None) => return ListenerExit::Shutdown,
            Err(exit) => return exit,
        };
        tracing::info!(
            "{}: listening ({})",
            self.tag,
            match &session.mode {
                Mode::Broker { .. } => "broker",
                Mode::Pipelines { .. } => "pipelines",
            }
        );

        let mut job: Option<RunningJob> = None;
        let (done_tx, mut done_rx) = mpsc::channel::<()>(4);
        let mut slots_rx = self.shared.slots.subscribe();
        let mut stop_rx = self.shutdown.clone();
        let mut failures = 0u32;

        let exit = loop {
            if self.stopping() {
                break ListenerExit::Shutdown;
            }
            let status = self.status(job.is_some());
            let view = session.clone();

            // Poll, but re-poll with a new status if our availability changes.
            let outcome = {
                let poll = self.poll(&view, status);
                tokio::pin!(poll);
                loop {
                    tokio::select! {
                        r = &mut poll => break Some(r),
                        _ = done_rx.recv() => {
                            job = None;
                            if self.status(false) != status { break None; }
                        }
                        changed = slots_rx.changed(), if job.is_none() => {
                            if changed.is_err() { break None; }
                            // Debounce so a burst of job starts/finishes does not
                            // make every idle listener reconnect at once.
                            tokio::time::sleep(crate::util::jitter(Duration::from_millis(200), Duration::from_secs(2))).await;
                            if self.status(job.is_some()) != status { break None; }
                        }
                        _ = stop_rx.changed() => break None,
                    }
                }
            };

            let polled = match outcome {
                None => continue,
                Some(Ok(p)) => {
                    failures = 0;
                    p
                }
                Some(Err(e)) => match classify(&e) {
                    Failure::RegistrationGone(why) => break ListenerExit::RegistrationGone(why),
                    Failure::SessionExpired => {
                        tracing::info!("{}: session expired, creating a new one", self.tag);
                        match self.open_session().await {
                            Ok(Some(s)) => {
                                session = s;
                                continue;
                            }
                            Ok(None) => break ListenerExit::Shutdown,
                            Err(exit) => break exit,
                        }
                    }
                    Failure::VersionTooOld(why) => {
                        tracing::error!(
                            "{}: GitHub rejects worker version {}: {why}",
                            self.tag,
                            self.shared.runner_version()
                        );
                        self.sleep(Duration::from_secs(600)).await;
                        continue;
                    }
                    Failure::Transient => {
                        failures += 1;
                        if status_of(&e) == Some(401) {
                            self.tokens.invalidate().await;
                        }
                        let delay = if failures <= 5 {
                            crate::util::jitter(Duration::from_secs(15), Duration::from_secs(30))
                        } else {
                            crate::util::jitter(Duration::from_secs(30), Duration::from_secs(60))
                        };
                        if failures == 1 || failures.is_multiple_of(20) {
                            tracing::warn!(
                                "{}: message poll failed ({failures}x), retrying: {e:#}",
                                self.tag
                            );
                        }
                        self.sleep(delay).await;
                        continue;
                    }
                },
            };

            let Some(p) = polled else { continue };
            if let Some(url) = p.migrated_broker.clone()
                && let Mode::Pipelines { migrated_broker } = &mut session.mode
            {
                *migrated_broker = Some(url);
            }
            session.last_message_id = Some(p.message.message_id);
            self.handle(&session, &p.message, &mut job, &done_tx).await;
            if p.from_pipelines {
                self.delete_pipelines_message(&session, p.message.message_id)
                    .await;
            }
        };

        if let Some(j) = job.take() {
            tracing::info!("{}: stopping running job", self.tag);
            let _ = j.control.send(JobControl::Shutdown).await;
            let _ = j.handle.await;
        }
        self.close_session(&session).await;
        exit
    }

    fn status(&self, busy: bool) -> &'static str {
        if busy || !self.shared.slots.is_available() {
            "Busy"
        } else {
            "Online"
        }
    }

    async fn sleep(&mut self, d: Duration) {
        tokio::select! {
            _ = tokio::time::sleep(d) => {}
            _ = self.shutdown.changed() => {}
        }
    }

    // ------------------------------------------------------------- sessions

    fn session_request(&self, broker: bool) -> CreateSession {
        CreateSession {
            session_id: EMPTY_GUID.to_string(),
            owner_name: format!("{} (PID: {})", crate::util::hostname(), std::process::id()),
            agent: AgentReference {
                id: self.identity.agent_id,
                name: self.identity.name.clone(),
                version: self.shared.runner_version(),
                os_description: crate::util::os_description(),
            },
            use_fips_encryption: false,
            broker_session: broker,
        }
    }

    fn broker_flow(&self) -> Option<String> {
        if self.identity.use_v2_flow {
            self.identity.server_url_v2.clone()
        } else {
            None
        }
    }

    /// Create a session, retrying like the official runner (30s apart; a
    /// conflicting session is given up on after 4 minutes).
    async fn open_session(&mut self) -> Result<Option<Session>, ListenerExit> {
        self.delete_stale_session().await;
        let started = Instant::now();
        let mut logged = false;
        loop {
            if self.stopping() {
                return Ok(None);
            }
            match self.try_open_session().await {
                Ok(s) => {
                    self.remember_session(Some(s.id.clone()));
                    if logged {
                        tracing::info!("{}: reconnected", self.tag);
                    }
                    return Ok(Some(s));
                }
                Err(e) => {
                    if let Failure::RegistrationGone(why) = classify(&e) {
                        return Err(ListenerExit::RegistrationGone(why));
                    }
                    let status = status_of(&e);
                    if status == Some(409) && started.elapsed() > Duration::from_secs(240) {
                        return Err(ListenerExit::Fatal(format!(
                            "another process holds this runner's session: {e:#}"
                        )));
                    }
                    if matches!(status, Some(400) | Some(403)) {
                        return Err(ListenerExit::Fatal(format!("cannot create session: {e:#}")));
                    }
                    if status == Some(401) {
                        self.tokens.invalidate().await;
                    }
                    if !logged {
                        tracing::warn!("{}: cannot create session, retrying: {e:#}", self.tag);
                        logged = true;
                    }
                    self.sleep(crate::util::jitter(
                        Duration::from_secs(25),
                        Duration::from_secs(35),
                    ))
                    .await;
                }
            }
        }
    }

    async fn try_open_session(&self) -> Result<Session> {
        let token = self.tokens.get().await?;
        if let Some(url) = self.broker_flow() {
            let url = crate::util::with_trailing_slash(&url);
            let resp = broker::create_session(
                &self.shared.http,
                &url,
                &token,
                &self.session_request(true),
            )
            .await?;
            return Ok(Session {
                id: resp.session_id,
                mode: Mode::Broker { url },
                aes_key: None,
                last_message_id: None,
            });
        }
        let resp = pipelines::create_session(
            &self.shared.http,
            &self.identity.server_url,
            &token,
            self.identity.pool_id,
            &self.session_request(false),
        )
        .await?;
        let aes_key = protocol::unwrap_session_key(&self.key, &resp)?;
        Ok(Session {
            id: resp.session_id,
            mode: Mode::Pipelines {
                migrated_broker: resp
                    .broker_migration_message
                    .map(|m| crate::util::with_trailing_slash(&m.broker_base_url)),
            },
            aes_key,
            last_message_id: None,
        })
    }

    /// Persist the session id so a crashed hive can clean it up on restart
    /// instead of waiting out a session conflict.
    fn remember_session(&self, id: Option<String>) {
        let mut copy = (*self.identity).clone();
        copy.last_session_id = id;
        if let Err(e) = copy.save(&self.shared.data_dir) {
            tracing::debug!("{}: cannot persist session id: {e:#}", self.tag);
        }
    }

    async fn delete_stale_session(&self) {
        let path = self.identity.path(&self.shared.data_dir);
        let stale = std::fs::read(&path)
            .ok()
            .and_then(|raw| serde_json::from_slice::<Identity>(&raw).ok())
            .and_then(|id| id.last_session_id);
        let Some(old) = stale else { return };
        let Ok(token) = self.tokens.get().await else {
            return;
        };
        let r = match self.broker_flow() {
            Some(url) => {
                broker::delete_session(
                    &self.shared.http,
                    &crate::util::with_trailing_slash(&url),
                    &token,
                    &old,
                )
                .await
            }
            None => {
                pipelines::delete_session(
                    &self.shared.http,
                    &self.identity.server_url,
                    &token,
                    self.identity.pool_id,
                    &old,
                )
                .await
            }
        };
        if r.is_ok() {
            tracing::debug!("{}: removed stale session {old}", self.tag);
        }
        self.remember_session(None);
    }

    async fn close_session(&self, session: &Session) {
        let Ok(token) = self.tokens.get().await else {
            return;
        };
        let r = match &session.mode {
            Mode::Broker { url } => {
                broker::delete_session(&self.shared.http, url, &token, &session.id).await
            }
            Mode::Pipelines { .. } => {
                pipelines::delete_session(
                    &self.shared.http,
                    &self.identity.server_url,
                    &token,
                    self.identity.pool_id,
                    &session.id,
                )
                .await
            }
        };
        match r {
            Ok(()) => self.remember_session(None),
            Err(e) => tracing::debug!("{}: cannot delete session: {e:#}", self.tag),
        }
    }

    // -------------------------------------------------------------- polling

    async fn poll(&self, session: &Session, status: &str) -> Result<Option<Polled>> {
        match self.poll_once(session, status).await {
            Err(e) if status_of(&e) == Some(401) => {
                self.tokens.invalidate().await;
                self.poll_once(session, status).await
            }
            other => other,
        }
    }

    async fn poll_once(&self, session: &Session, status: &str) -> Result<Option<Polled>> {
        let token = self.tokens.get().await?;
        let version = self.shared.runner_version();
        let http = &self.shared.http;
        match &session.mode {
            Mode::Broker { url } => {
                let m = broker::get_message(http, url, &token, &session.id, status, &version, true)
                    .await?;
                Ok(m.map(|message| Polled {
                    message,
                    from_pipelines: false,
                    migrated_broker: None,
                }))
            }
            Mode::Pipelines { .. } => {
                let m = pipelines::get_message(
                    http,
                    &self.identity.server_url,
                    &token,
                    self.identity.pool_id,
                    &session.id,
                    session.last_message_id,
                    status,
                    &version,
                    true,
                )
                .await?;
                let Some(mut message) = m else {
                    return Ok(None);
                };
                if let (Some(iv), Some(key), Some(body)) = (
                    message.iv.as_deref().filter(|s| !s.is_empty()),
                    &session.aes_key,
                    &message.body,
                ) {
                    message.body = Some(protocol::decrypt_body(key, iv, body)?);
                }
                if message
                    .message_type
                    .eq_ignore_ascii_case(mt::BROKER_MIGRATION)
                {
                    // The pipelines queue hands us off to the broker; long-poll
                    // there with the same session (MessageListener.cs).
                    let mig: BrokerMigration =
                        serde_json::from_str(message.body.as_deref().unwrap_or("{}"))
                            .context("bad BrokerMigration message")?;
                    let url = crate::util::with_trailing_slash(&mig.broker_base_url);
                    let m = broker::get_message(
                        http,
                        &url,
                        &token,
                        &session.id,
                        status,
                        &version,
                        true,
                    )
                    .await?;
                    return Ok(m.map(|message| Polled {
                        message,
                        from_pipelines: false,
                        migrated_broker: Some(url.clone()),
                    }));
                }
                Ok(Some(Polled {
                    message,
                    from_pipelines: true,
                    migrated_broker: None,
                }))
            }
        }
    }

    async fn delete_pipelines_message(&self, session: &Session, message_id: i64) {
        let Ok(token) = self.tokens.get().await else {
            return;
        };
        if let Err(e) = pipelines::delete_message(
            &self.shared.http,
            &self.identity.server_url,
            &token,
            self.identity.pool_id,
            &session.id,
            message_id,
        )
        .await
        {
            tracing::debug!("{}: cannot delete message {message_id}: {e:#}", self.tag);
        }
    }

    // ------------------------------------------------------------- messages

    async fn handle(
        &self,
        session: &Session,
        msg: &TaskAgentMessage,
        job: &mut Option<RunningJob>,
        done_tx: &mpsc::Sender<()>,
    ) {
        let body = msg.body.as_deref().unwrap_or("");
        let kind = msg.message_type.as_str();
        if kind.eq_ignore_ascii_case(mt::RUNNER_JOB_REQUEST) {
            let r: RunnerJobRequestRef = match serde_json::from_str(body) {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!("{}: unreadable job request: {e}", self.tag);
                    return;
                }
            };
            if r.should_acknowledge
                && let Some(url) = session.broker_url()
                && let Err(e) = self
                    .acknowledge(url, &session.id, &r.runner_request_id)
                    .await
            {
                tracing::debug!("{}: acknowledge failed (best effort): {e:#}", self.tag);
            }
            if let Some(old) = job.take() {
                // The service only does this if it considers the old job over.
                tracing::warn!(
                    "{}: new job while one is running; cancelling the old one",
                    self.tag
                );
                let _ = old
                    .control
                    .send(JobControl::Cancel(Duration::from_secs(60)))
                    .await;
            }
            *job = Some(self.spawn_job(r, done_tx.clone()));
        } else if kind.eq_ignore_ascii_case(mt::JOB_CANCELLATION) {
            match serde_json::from_str::<JobCancelMessage>(body) {
                Ok(c) => {
                    let timeout = c
                        .timeout
                        .as_deref()
                        .and_then(crate::util::parse_timespan)
                        .unwrap_or(Duration::from_secs(300));
                    if let Some(j) = job.as_ref() {
                        let _ = j.control.send(JobControl::Cancel(timeout)).await;
                    } else {
                        tracing::debug!(
                            "{}: cancellation for job {} that is not running here",
                            self.tag,
                            c.job_id
                        );
                    }
                }
                Err(e) => tracing::warn!("{}: unreadable cancellation: {e}", self.tag),
            }
        } else if kind.eq_ignore_ascii_case(mt::FORCE_TOKEN_REFRESH) {
            self.tokens.invalidate().await;
        } else if kind.eq_ignore_ascii_case(mt::AGENT_REFRESH)
            || kind.eq_ignore_ascii_case(mt::RUNNER_REFRESH)
        {
            tracing::info!(
                "{}: GitHub asked for a runner update; the hive updates the worker package itself (worker.version)",
                self.tag
            );
        } else if kind.eq_ignore_ascii_case(mt::RUNNER_REFRESH_CONFIG) {
            tracing::info!(
                "{}: ignoring RunnerRefreshConfig (not supported yet)",
                self.tag
            );
        } else if kind.eq_ignore_ascii_case(mt::PIPELINE_AGENT_JOB_REQUEST) {
            tracing::error!(
                "{}: received a legacy PipelineAgentJobRequest, which runner-hive does not support",
                self.tag
            );
        } else if kind.eq_ignore_ascii_case(mt::HOSTED_RUNNER_SHUTDOWN) {
            tracing::debug!("{}: ignoring hosted runner shutdown", self.tag);
        } else {
            tracing::warn!("{}: unknown message type '{kind}'", self.tag);
        }
    }

    async fn acknowledge(
        &self,
        broker_url: &str,
        session_id: &str,
        request_id: &str,
    ) -> Result<()> {
        let token = self.tokens.get().await?;
        broker::acknowledge(
            &self.shared.http,
            broker_url,
            &token,
            session_id,
            "Busy",
            &self.shared.runner_version(),
            request_id,
        )
        .await
    }

    fn spawn_job(&self, r: RunnerJobRequestRef, done_tx: mpsc::Sender<()>) -> RunningJob {
        let (control_tx, control_rx) = mpsc::channel(8);
        let ctx = JobTask {
            shared: self.shared.clone(),
            identity: self.identity.clone(),
            tokens: self.tokens.clone(),
            tag: self.tag.clone(),
        };
        let handle = tokio::spawn(async move {
            ctx.run(r, control_rx).await;
            let _ = done_tx.send(()).await;
        });
        RunningJob {
            control: control_tx,
            handle,
        }
    }
}

struct JobTask {
    shared: Arc<Shared>,
    identity: Arc<Identity>,
    tokens: Arc<TokenSource>,
    tag: String,
}

impl JobTask {
    async fn run(self, r: RunnerJobRequestRef, mut control: mpsc::Receiver<JobControl>) {
        let queued = Instant::now();
        // Wait for a global slot; give up if the job is cancelled meanwhile.
        let lease = tokio::select! {
            lease = self.shared.slots.acquire() => lease,
            ctl = control.recv() => {
                if ctl.is_some() {
                    tracing::info!("{}: job cancelled while waiting for a slot", self.tag);
                }
                return;
            }
        };
        let waited = queued.elapsed();

        let json = match self.acquire(&r).await {
            Ok(j) => j,
            Err(e) => {
                match run_service::job_gone(&e) {
                    Some(why) => tracing::info!("{}: job request skipped ({why:?})", self.tag),
                    None => tracing::error!("{}: cannot acquire job: {e:#}", self.tag),
                }
                return;
            }
        };
        let info = match worker::parse_job(&json) {
            Ok(i) => i,
            Err(e) => {
                tracing::error!("{}: unusable job message: {e:#}", self.tag);
                return;
            }
        };

        let package = self.shared.package();
        let slot_root = {
            let data_dir = self.shared.data_dir.clone();
            let pkg = package.clone();
            let index = lease.index;
            match tokio::task::spawn_blocking(move || worker::slot::prepare(&data_dir, index, &pkg))
                .await
            {
                Ok(Ok(root)) => root,
                Ok(Err(e)) => {
                    tracing::error!("{}: cannot prepare slot {}: {e:#}", self.tag, lease.index);
                    return;
                }
                Err(e) => {
                    tracing::error!("{}: slot preparation panicked: {e}", self.tag);
                    return;
                }
            }
        };

        let repo = info
            .repository
            .clone()
            .unwrap_or_else(|| self.identity.scope.to_string());
        tracing::info!(
            "{repo}: job '{}' started in slot {} (waited {:.1}s for a slot, {} of {} running)",
            info.display_name,
            lease.index,
            waited.as_secs_f64(),
            self.shared.slots.running(),
            self.shared.slots.total()
        );
        let started = Instant::now();
        let outcome = worker::run_job(
            JobRun {
                http: &self.shared.http,
                identity: &self.identity,
                slot_root: &slot_root,
                job_json: &json,
                info: &info,
                env_set: &self.shared.env_set,
                env_remove: &self.shared.env_remove,
                log_output: self.shared.log_output,
            },
            control,
        )
        .await;
        let took = started.elapsed().as_secs();
        match outcome {
            Ok(JobOutcome::Finished(result)) => {
                tracing::info!(
                    "{repo}: job '{}' {result} in {}m{:02}s",
                    info.display_name,
                    took / 60,
                    took % 60
                )
            }
            Ok(JobOutcome::NotStarted(why)) => {
                tracing::warn!("{repo}: job '{}' not started: {why}", info.display_name)
            }
            Ok(JobOutcome::CanceledBeforeStart) => {
                tracing::info!(
                    "{repo}: job '{}' cancelled before it started",
                    info.display_name
                )
            }
            Ok(JobOutcome::Crashed(detail)) => {
                tracing::error!(
                    "{repo}: job '{}' worker crashed:\n{detail}",
                    info.display_name
                )
            }
            Err(e) => tracing::error!("{repo}: job '{}' failed to run: {e:#}", info.display_name),
        }
        if self.shared.clean_workspace
            && let Err(e) = worker::slot::clean_work(&slot_root)
        {
            tracing::warn!("slot {}: cleanup failed: {e:#}", lease.index);
        }
        drop(lease);
    }

    async fn acquire(&self, r: &RunnerJobRequestRef) -> Result<String> {
        let mut retried = false;
        loop {
            let token = self.tokens.get().await?;
            let result = match r.run_service_url.as_deref().filter(|u| !u.is_empty()) {
                Some(url) => {
                    run_service::acquire_job(
                        &self.shared.http,
                        url,
                        &token,
                        &r.runner_request_id,
                        r.billing_owner_id.as_deref(),
                    )
                    .await
                }
                None => {
                    pipelines::runner_message(
                        &self.shared.http,
                        &self.identity.server_url,
                        &token,
                        &r.runner_request_id,
                    )
                    .await
                }
            };
            match result {
                Err(e) if !retried && status_of(&e) == Some(401) => {
                    self.tokens.invalidate().await;
                    retried = true;
                }
                other => return other,
            }
        }
    }
}
