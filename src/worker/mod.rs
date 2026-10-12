//! Running one job: the part of Runner.Listener/JobDispatcher.cs that sits
//! around the worker process. The official `Runner.Worker` does the actual
//! work (steps, actions, containers, logs, results) and completes the job
//! itself; we keep the job lock renewed, relay cancellation, and fail the job
//! if the worker crashes before it can report.

pub mod package;
pub mod process;
pub mod slot;

use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::Value;
use tokio::sync::mpsc;

use crate::identity::Identity;
use crate::protocol::run_service::{self, JobGone};
use process::{MessageType, SpawnOptions, WorkerProcess};

/// What we need from the job message; the message itself is passed through.
#[derive(Debug, Clone)]
pub struct JobInfo {
    pub plan_id: String,
    pub job_id: String,
    pub display_name: String,
    pub system_url: String,
    pub access_token: String,
    pub billing_owner_id: Option<String>,
    pub repository: Option<String>,
}

pub fn parse_job(json: &str) -> Result<JobInfo> {
    let v: Value = serde_json::from_str(json).context("job message is not JSON")?;
    let s = |v: &Value| v.as_str().map(String::from);
    let endpoint = v["resources"]["endpoints"]
        .as_array()
        .and_then(|eps| {
            eps.iter().find(|e| {
                e["name"]
                    .as_str()
                    .is_some_and(|n| n.eq_ignore_ascii_case("SystemVssConnection"))
            })
        })
        .context("job message has no SystemVssConnection endpoint")?;
    let params = &endpoint["authorization"]["parameters"];
    let access_token = params
        .as_object()
        .and_then(|o| {
            o.iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("AccessToken"))
        })
        .and_then(|(_, v)| s(v))
        .context("job message has no access token")?;
    Ok(JobInfo {
        plan_id: s(&v["plan"]["planId"]).context("job message has no plan id")?,
        job_id: s(&v["jobId"]).context("job message has no job id")?,
        display_name: s(&v["jobDisplayName"]).unwrap_or_else(|| "job".into()),
        system_url: s(&endpoint["url"]).context("SystemVssConnection has no url")?,
        access_token,
        billing_owner_id: s(&v["billingOwnerId"]),
        repository: context_string(&v["contextData"]["github"], "repository"),
    })
}

/// Look up a key in serialized `PipelineContextData` (`{"t":2,"d":[{"k","v"}]}`).
fn context_string(dict: &Value, key: &str) -> Option<String> {
    dict["d"].as_array()?.iter().find_map(|kv| {
        if kv["k"].as_str()? != key {
            return None;
        }
        match &kv["v"] {
            Value::String(s) => Some(s.clone()),
            other => other["s"].as_str().map(String::from),
        }
    })
}

pub enum JobControl {
    /// Cancel the job; the worker gets at least this long to clean up.
    Cancel(Duration),
    /// The hive is shutting down.
    Shutdown,
}

#[derive(Debug)]
pub enum JobOutcome {
    /// Worker exited normally and reported this result itself.
    Finished(&'static str),
    /// Never reached the worker (job gone or first lock renewal failed).
    NotStarted(String),
    /// Cancelled before the worker started.
    CanceledBeforeStart,
    /// The worker died without reporting; we failed the job on its behalf.
    Crashed(String),
}

const RESULTS: [&str; 6] = [
    "succeeded",
    "succeededWithIssues",
    "failed",
    "canceled",
    "skipped",
    "abandoned",
];

pub struct JobRun<'a> {
    pub http: &'a Client,
    pub identity: &'a Identity,
    pub slot_root: &'a Path,
    pub job_json: &'a str,
    pub info: &'a JobInfo,
    pub env_set: &'a [(String, String)],
    pub env_remove: &'a [String],
    pub log_output: bool,
    pub run_as: Option<&'a crate::user::RunAs>,
}

pub async fn run_job(
    run: JobRun<'_>,
    mut control: mpsc::Receiver<JobControl>,
) -> Result<JobOutcome> {
    let info = run.info;

    // Like the official dispatcher: do not start the worker until the first
    // lock renewal succeeds (5 tries, ~10s apart).
    let mut renewed = false;
    for attempt in 0..6 {
        tokio::select! {
            r = run_service::renew_job(run.http, &info.system_url, &info.access_token, &info.plan_id, &info.job_id) => {
                match r {
                    Ok(_) => { renewed = true; break; }
                    Err(e) if run_service::job_gone(&e) == Some(JobGone::NotFound) => {
                        return Ok(JobOutcome::NotStarted("job no longer exists".into()));
                    }
                    Err(e) => {
                        tracing::warn!("job {}: first lock renewal failed (attempt {}): {e:#}", info.job_id, attempt + 1);
                        tokio::time::sleep(crate::util::jitter(Duration::from_secs(1), Duration::from_secs(10))).await;
                    }
                }
            }
            ctl = control.recv() => {
                if ctl.is_some() {
                    return Ok(JobOutcome::CanceledBeforeStart);
                }
            }
        }
    }
    if !renewed {
        return Ok(JobOutcome::NotStarted(
            "could not renew the job lock".into(),
        ));
    }

    slot::write_runner_config(run.slot_root, run.identity, run.run_as)?;
    let exe = run.slot_root.join("bin").join(package::worker_exe_name());
    let prefix = format!("[{} {}]", run.identity.scope, info.display_name);
    let mut worker = WorkerProcess::spawn(SpawnOptions {
        exe: &exe,
        env_set: run.env_set,
        env_remove: run.env_remove,
        log_output: run.log_output,
        log_prefix: prefix,
        run_as: run.run_as,
    })?;

    if let Err(e) = worker.send(MessageType::NewJobRequest, run.job_json).await {
        worker.kill_tree().await;
        // The worker never got the job, so (like the official runner) we do
        // not report a result; the service will time the job out.
        return Ok(JobOutcome::NotStarted(format!(
            "cannot hand the job to the worker: {e:#}"
        )));
    }

    let renew = renew_loop(run.http, info);
    tokio::pin!(renew);
    let mut renewing = true;
    let mut kill_at: Option<Instant> = None;
    // The worker reads exactly one control message after the job, and the
    // broker re-delivers cancellations, so only the first one is forwarded.
    let mut stop_sent = false;

    let status = loop {
        let kill_timer = async {
            match kill_at {
                Some(t) => tokio::time::sleep_until(t.into()).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::select! {
            status = worker.child.wait() => break status?,
            _ = &mut renew, if renewing => {
                renewing = false;
                tracing::warn!("job {}: lost the job lock, cancelling the worker", info.job_id);
                if !std::mem::replace(&mut stop_sent, true) {
                    let _ = worker.send(MessageType::CancelRequest, "").await;
                }
                kill_at.get_or_insert(Instant::now() + Duration::from_secs(300));
            }
            ctl = control.recv() => match ctl {
                Some(JobControl::Cancel(timeout)) => {
                    if !std::mem::replace(&mut stop_sent, true) {
                        tracing::info!("job {}: cancellation requested", info.job_id);
                        if worker.send(MessageType::CancelRequest, "").await.is_err() {
                            worker.kill_tree().await;
                        }
                    }
                    // Official runner: at least 60s, kill 15s before the deadline.
                    let deadline = Instant::now() + timeout.max(Duration::from_secs(60)) - Duration::from_secs(15);
                    kill_at = Some(kill_at.map_or(deadline, |k| k.min(deadline)));
                }
                Some(JobControl::Shutdown) => {
                    if !std::mem::replace(&mut stop_sent, true)
                        && worker.send(MessageType::RunnerShutdown, "").await.is_err()
                    {
                        worker.kill_tree().await;
                    }
                    kill_at.get_or_insert(Instant::now() + Duration::from_secs(300));
                }
                None => {}
            },
            _ = kill_timer => {
                tracing::warn!("job {}: worker did not exit in time, killing it", info.job_id);
                worker.kill_tree().await;
                kill_at = None;
            }
        }
    };

    let code = status.code();
    if let Some(c) = code
        && (100..=105).contains(&c)
    {
        return Ok(JobOutcome::Finished(RESULTS[(c - 100) as usize]));
    }

    // Worker crashed: fail the job with its output so the UI shows why.
    let tail = worker.tail();
    let detail = format!(
        "runner-hive: Runner.Worker exited unexpectedly ({}).\n{}",
        code.map_or("killed by signal".to_string(), |c| format!("exit code {c}")),
        tail
    );
    if let Err(e) = run_service::complete_job(
        run.http,
        &info.system_url,
        &info.access_token,
        &info.plan_id,
        &info.job_id,
        "failed",
        Some(&detail),
        info.billing_owner_id.as_deref(),
    )
    .await
    {
        tracing::warn!("job {}: cannot report worker crash: {e:#}", info.job_id);
    }
    Ok(JobOutcome::Crashed(detail))
}

/// Start the real worker in a scratch slot and send it a job message that is
/// valid JSON but has no `resources`. A worker that received and parsed our
/// message fails with a complaint about `Resources`; any other failure means
/// the pipe handoff or framing is wrong on this platform.
pub async fn self_test(
    data_dir: &Path,
    pkg: &package::WorkerPackage,
    run_as: Option<&crate::user::RunAs>,
) -> Result<()> {
    let pkg = pkg.clone();
    let root_dir = data_dir.join("slots").join("self-test");
    let root = tokio::task::spawn_blocking(move || slot::prepare_at(root_dir, &pkg)).await??;
    for dir in ["_work", "_diag"] {
        let p = root.join(dir);
        std::fs::create_dir_all(&p)?;
        if let Some(who) = run_as {
            crate::user::give(&p, who)?;
        }
    }
    let exe = root.join("bin").join(package::worker_exe_name());
    let mut worker = WorkerProcess::spawn(SpawnOptions {
        exe: &exe,
        env_set: &[],
        env_remove: &[],
        log_output: false,
        log_prefix: String::new(),
        run_as,
    })?;
    worker
        .send(
            MessageType::NewJobRequest,
            r#"{"messageType":"RunnerJobRequest"}"#,
        )
        .await
        .context("could not write the job message to the worker")?;
    let status = match tokio::time::timeout(Duration::from_secs(90), worker.child.wait()).await {
        Ok(s) => s?,
        Err(_) => {
            worker.kill_tree().await;
            anyhow::bail!("the worker did not exit within 90s:\n{}", worker.tail());
        }
    };
    // Give the output forwarders a moment to drain.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let tail = worker.tail();
    if status.code() == Some(1) && tail.contains("Resources") {
        Ok(())
    } else {
        anyhow::bail!("unexpected worker result ({status}):\n{tail}")
    }
}

/// Renew the job lock every 60s. Returns only when the lock is lost.
async fn renew_loop(http: &Client, info: &JobInfo) {
    let mut last_ok = Instant::now();
    let mut errors = 0u32;
    let mut delay = Duration::from_secs(60);
    loop {
        tokio::time::sleep(delay).await;
        match run_service::renew_job(
            http,
            &info.system_url,
            &info.access_token,
            &info.plan_id,
            &info.job_id,
        )
        .await
        {
            Ok(_) => {
                last_ok = Instant::now();
                errors = 0;
                delay = Duration::from_secs(60);
            }
            Err(e) if run_service::job_gone(&e) == Some(JobGone::NotFound) => return,
            Err(e) => {
                errors += 1;
                tracing::warn!("job {}: lock renewal failed: {e:#}", info.job_id);
                // The lock is valid for a while; give up after 10 minutes.
                if last_ok.elapsed() > Duration::from_secs(600) {
                    return;
                }
                delay = if errors > 5 {
                    crate::util::jitter(Duration::from_secs(15), Duration::from_secs(30))
                } else {
                    crate::util::jitter(Duration::from_secs(5), Duration::from_secs(15))
                };
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_job_message_essentials() {
        let msg = r#"{
            "messageType": "RunnerJobRequest",
            "plan": {"planId": "11111111-1111-1111-1111-111111111111"},
            "jobId": "22222222-2222-2222-2222-222222222222",
            "jobDisplayName": "build (ubuntu)",
            "billingOwnerId": "B",
            "resources": {"endpoints": [
                {"name": "SystemVssConnection", "url": "https://run.example/91/",
                 "authorization": {"scheme": "OAuth", "parameters": {"AccessToken": "tok"}}}
            ]},
            "contextData": {"github": {"t": 2, "d": [{"k": "repository", "v": "octo/demo"}]}}
        }"#;
        let info = parse_job(msg).unwrap();
        assert_eq!(info.job_id, "22222222-2222-2222-2222-222222222222");
        assert_eq!(info.system_url, "https://run.example/91/");
        assert_eq!(info.access_token, "tok");
        assert_eq!(info.repository.as_deref(), Some("octo/demo"));
        assert_eq!(info.billing_owner_id.as_deref(), Some("B"));
    }
}
