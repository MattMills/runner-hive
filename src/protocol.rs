//! Wire protocol of the GitHub Actions runner service, reverse engineered
//! from `actions/runner` (Runner.Listener / Sdk). Three services are involved:
//!
//! * **pipelines** (legacy tenant, `*.actions.githubusercontent.com/<id>/`):
//!   registration, sessions and the message queue for older registrations.
//! * **broker** (`broker.actions.githubusercontent.com`): sessions and the
//!   long-poll message queue for the v2 flow; legacy sessions are migrated
//!   to it with a `BrokerMigration` message.
//! * **run service**: acquire / renew / complete a job.

use std::time::Duration;

use anyhow::{Context, Result, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use reqwest::{Client, Method, RequestBuilder};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::http::HttpError;

/// Long-polls are held ~50s by the service; give them headroom.
pub const LONG_POLL_TIMEOUT: Duration = Duration::from_secs(100);
const CALL_TIMEOUT: Duration = Duration::from_secs(60);

pub const EMPTY_GUID: &str = "00000000-0000-0000-0000-000000000000";

// ----------------------------------------------------------------- messages

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskAgentMessage {
    #[serde(default)]
    pub message_id: i64,
    #[serde(default)]
    pub message_type: String,
    #[serde(default, alias = "iV", alias = "IV")]
    pub iv: Option<String>,
    #[serde(default)]
    pub body: Option<String>,
}

/// Body of a `RunnerJobRequest` message.
#[derive(Debug, Clone, Deserialize)]
pub struct RunnerJobRequestRef {
    pub runner_request_id: String,
    #[serde(default)]
    pub should_acknowledge: bool,
    #[serde(default)]
    pub run_service_url: Option<String>,
    #[serde(default)]
    pub billing_owner_id: Option<String>,
}

/// Body of a `JobCancellation` message.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobCancelMessage {
    pub job_id: String,
    #[serde(default)]
    pub timeout: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerMigration {
    pub broker_base_url: String,
}

pub mod message_types {
    pub const RUNNER_JOB_REQUEST: &str = "RunnerJobRequest";
    pub const PIPELINE_AGENT_JOB_REQUEST: &str = "PipelineAgentJobRequest";
    pub const JOB_CANCELLATION: &str = "JobCancellation";
    pub const BROKER_MIGRATION: &str = "BrokerMigration";
    pub const AGENT_REFRESH: &str = "AgentRefresh";
    pub const RUNNER_REFRESH: &str = "RunnerRefresh";
    pub const RUNNER_REFRESH_CONFIG: &str = "RunnerRefreshConfig";
    pub const FORCE_TOKEN_REFRESH: &str = "ForceTokenRefresh";
    pub const HOSTED_RUNNER_SHUTDOWN: &str = "HostedRunnerShutdown";
}

// ----------------------------------------------------------------- sessions

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentReference {
    pub id: u64,
    pub name: String,
    pub version: String,
    pub os_description: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateSession {
    pub session_id: String,
    pub owner_name: String,
    pub agent: AgentReference,
    pub use_fips_encryption: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub broker_session: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionKey {
    #[serde(default)]
    pub encrypted: bool,
    #[serde(default)]
    pub value: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionResponse {
    pub session_id: String,
    #[serde(default)]
    pub encryption_key: Option<SessionKey>,
    #[serde(default)]
    pub use_fips_encryption: bool,
    #[serde(default)]
    pub broker_migration_message: Option<BrokerMigration>,
}

// ------------------------------------------------------------------ helpers

fn vss(req: RequestBuilder, token: &str, api_version: &str) -> RequestBuilder {
    req.bearer_auth(token)
        .header(
            "Accept",
            format!("application/json; api-version={api_version}"),
        )
        .header("X-TFS-FedAuthRedirect", "Suppress")
        .header("X-VSS-E2EID", uuid::Uuid::new_v4().to_string())
}

fn json_body<T: Serialize>(
    req: RequestBuilder,
    body: &T,
    api_version: Option<&str>,
) -> Result<RequestBuilder> {
    let ct = match api_version {
        Some(v) => format!("application/json; charset=utf-8; api-version={v}"),
        None => "application/json; charset=utf-8".to_string(),
    };
    Ok(req
        .header("Content-Type", ct)
        .body(serde_json::to_vec(body)?))
}

async fn send(req: RequestBuilder, method: &Method, url: &str) -> Result<reqwest::Response> {
    let resp = req
        .send()
        .await
        .with_context(|| format!("{method} {}", redact(url)))?;
    if resp.status().is_success() {
        return Ok(resp);
    }
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    Err(HttpError {
        status,
        method: method.to_string(),
        url: redact(url),
        body,
    }
    .into())
}

/// Strip the query string from URLs that end up in logs.
fn redact(url: &str) -> String {
    url.split('?').next().unwrap_or(url).to_string()
}

/// Parse a possibly-empty JSON body (long-polls return 200/202/204 with no
/// body when nothing arrived).
async fn optional_json<T: for<'de> Deserialize<'de>>(resp: reqwest::Response) -> Result<Option<T>> {
    let text = resp.text().await?;
    if text.trim().is_empty() || text.trim() == "null" {
        return Ok(None);
    }
    Ok(Some(serde_json::from_str(&text).with_context(|| {
        format!(
            "unexpected response: {}",
            text.chars().take(200).collect::<String>()
        )
    })?))
}

fn tenant(server_url: &str, path: &str) -> String {
    format!("{}{}", crate::util::with_trailing_slash(server_url), path)
}

// --------------------------------------------------------- pipelines (v1)

pub mod pipelines {
    use super::*;

    #[derive(Debug, Clone, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Pool {
        pub id: i64,
        pub name: String,
        #[serde(default)]
        pub is_hosted: bool,
        #[serde(default)]
        pub is_internal: bool,
    }

    #[derive(Deserialize)]
    struct ListResp<T> {
        #[serde(default = "Vec::new")]
        value: Vec<T>,
    }

    pub async fn pools(http: &Client, server_url: &str, token: &str) -> Result<Vec<Pool>> {
        let url = tenant(server_url, "_apis/distributedtask/pools");
        let m = Method::GET;
        let req = vss(http.get(&url), token, "5.1-preview.1").timeout(CALL_TIMEOUT);
        let r: ListResp<Pool> = send(req, &m, &url).await?.json().await?;
        Ok(r.value)
    }

    /// Agents named `name` in `pool` (raw JSON, since we only need a few fields).
    pub async fn agents_named(
        http: &Client,
        server_url: &str,
        token: &str,
        pool: i64,
        name: &str,
    ) -> Result<Vec<Value>> {
        let mut url = url::Url::parse(&tenant(
            server_url,
            &format!("_apis/distributedtask/pools/{pool}/agents"),
        ))?;
        url.query_pairs_mut().append_pair("agentName", name);
        let m = Method::GET;
        let req = vss(http.get(url.as_str()), token, "6.0-preview.2").timeout(CALL_TIMEOUT);
        let r: ListResp<Value> = send(req, &m, url.as_str()).await?.json().await?;
        Ok(r.value)
    }

    pub async fn add_agent(
        http: &Client,
        server_url: &str,
        token: &str,
        pool: i64,
        agent: &Value,
    ) -> Result<Value> {
        let url = tenant(
            server_url,
            &format!("_apis/distributedtask/pools/{pool}/agents"),
        );
        let m = Method::POST;
        let req = json_body(
            vss(http.post(&url), token, "6.0-preview.2"),
            agent,
            Some("6.0-preview.2"),
        )?
        .timeout(CALL_TIMEOUT);
        Ok(send(req, &m, &url).await?.json().await?)
    }

    pub async fn replace_agent(
        http: &Client,
        server_url: &str,
        token: &str,
        pool: i64,
        agent_id: u64,
        agent: &Value,
    ) -> Result<Value> {
        let url = tenant(
            server_url,
            &format!("_apis/distributedtask/pools/{pool}/agents/{agent_id}"),
        );
        let m = Method::PUT;
        let req = json_body(
            vss(http.put(&url), token, "6.0-preview.2"),
            agent,
            Some("6.0-preview.2"),
        )?
        .timeout(CALL_TIMEOUT);
        Ok(send(req, &m, &url).await?.json().await?)
    }

    pub async fn create_session(
        http: &Client,
        server_url: &str,
        token: &str,
        pool: i64,
        session: &CreateSession,
    ) -> Result<SessionResponse> {
        let url = tenant(
            server_url,
            &format!("_apis/distributedtask/pools/{pool}/sessions"),
        );
        let m = Method::POST;
        let req = json_body(
            vss(http.post(&url), token, "5.1-preview.1"),
            session,
            Some("5.1-preview.1"),
        )?
        .timeout(CALL_TIMEOUT);
        Ok(send(req, &m, &url).await?.json().await?)
    }

    pub async fn delete_session(
        http: &Client,
        server_url: &str,
        token: &str,
        pool: i64,
        session_id: &str,
    ) -> Result<()> {
        let url = tenant(
            server_url,
            &format!("_apis/distributedtask/pools/{pool}/sessions/{session_id}"),
        );
        let m = Method::DELETE;
        let req = vss(http.delete(&url), token, "5.1-preview.1").timeout(Duration::from_secs(30));
        send(req, &m, &url).await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn get_message(
        http: &Client,
        server_url: &str,
        token: &str,
        pool: i64,
        session_id: &str,
        last_message_id: Option<i64>,
        status: &str,
        runner_version: &str,
        disable_update: bool,
    ) -> Result<Option<TaskAgentMessage>> {
        let mut url = url::Url::parse(&tenant(
            server_url,
            &format!("_apis/distributedtask/pools/{pool}/messages"),
        ))?;
        {
            let mut q = url.query_pairs_mut();
            q.append_pair("sessionId", session_id);
            if let Some(id) = last_message_id {
                q.append_pair("lastMessageId", &id.to_string());
            }
            q.append_pair("status", status);
            q.append_pair("runnerVersion", runner_version);
            q.append_pair("os", crate::util::os_name());
            q.append_pair("architecture", crate::util::arch_name());
            q.append_pair(
                "disableUpdate",
                if disable_update { "true" } else { "false" },
            );
        }
        let m = Method::GET;
        let req = vss(http.get(url.as_str()), token, "6.0-preview.1").timeout(LONG_POLL_TIMEOUT);
        optional_json(send(req, &m, url.as_str()).await?).await
    }

    pub async fn delete_message(
        http: &Client,
        server_url: &str,
        token: &str,
        pool: i64,
        session_id: &str,
        message_id: i64,
    ) -> Result<()> {
        let mut url = url::Url::parse(&tenant(
            server_url,
            &format!("_apis/distributedtask/pools/{pool}/messages/{message_id}"),
        ))?;
        url.query_pairs_mut().append_pair("sessionId", session_id);
        let m = Method::DELETE;
        let req =
            vss(http.delete(url.as_str()), token, "5.1-preview.1").timeout(Duration::from_secs(30));
        send(req, &m, url.as_str()).await?;
        Ok(())
    }

    /// Legacy run-service job fetch, used when a `RunnerJobRequest` carries no
    /// `run_service_url`.
    pub async fn runner_message(
        http: &Client,
        server_url: &str,
        token: &str,
        message_id: &str,
    ) -> Result<String> {
        let url = tenant(
            server_url,
            &format!("_apis/distributedtask/runnermessages/{message_id}"),
        );
        let m = Method::GET;
        let req = vss(http.get(&url), token, "6.0-preview.1").timeout(CALL_TIMEOUT);
        Ok(send(req, &m, &url).await?.text().await?)
    }
}

// -------------------------------------------------------------- broker (v2)

pub mod broker {
    use super::*;

    fn bearer(req: RequestBuilder, token: &str) -> RequestBuilder {
        req.bearer_auth(token).header("Accept", "application/json")
    }

    pub async fn create_session(
        http: &Client,
        broker_url: &str,
        token: &str,
        session: &CreateSession,
    ) -> Result<SessionResponse> {
        let url = crate::util::join_url(broker_url, "session")?;
        let m = Method::POST;
        let req =
            json_body(bearer(http.post(url.clone()), token), session, None)?.timeout(CALL_TIMEOUT);
        Ok(send(req, &m, url.as_str()).await?.json().await?)
    }

    pub async fn delete_session(
        http: &Client,
        broker_url: &str,
        token: &str,
        session_id: &str,
    ) -> Result<()> {
        let mut url = crate::util::join_url(broker_url, "session")?;
        url.query_pairs_mut().append_pair("sessionId", session_id);
        let m = Method::DELETE;
        let req = bearer(http.delete(url.clone()), token).timeout(Duration::from_secs(30));
        send(req, &m, url.as_str()).await?;
        Ok(())
    }

    fn runner_query(url: &mut url::Url, session_id: &str, status: &str, runner_version: &str) {
        let mut q = url.query_pairs_mut();
        q.append_pair("sessionId", session_id);
        q.append_pair("status", status);
        q.append_pair("runnerVersion", runner_version);
        q.append_pair("os", crate::util::os_name());
        q.append_pair("architecture", crate::util::arch_name());
    }

    pub async fn get_message(
        http: &Client,
        broker_url: &str,
        token: &str,
        session_id: &str,
        status: &str,
        runner_version: &str,
        disable_update: bool,
    ) -> Result<Option<TaskAgentMessage>> {
        let mut url = crate::util::join_url(broker_url, "message")?;
        runner_query(&mut url, session_id, status, runner_version);
        url.query_pairs_mut().append_pair(
            "disableUpdate",
            if disable_update { "true" } else { "false" },
        );
        let m = Method::GET;
        let req = bearer(http.get(url.clone()), token).timeout(LONG_POLL_TIMEOUT);
        optional_json(send(req, &m, url.as_str()).await?).await
    }

    pub async fn acknowledge(
        http: &Client,
        broker_url: &str,
        token: &str,
        session_id: &str,
        status: &str,
        runner_version: &str,
        runner_request_id: &str,
    ) -> Result<()> {
        let mut url = crate::util::join_url(broker_url, "acknowledge")?;
        runner_query(&mut url, session_id, status, runner_version);
        let body = serde_json::json!({ "runnerRequestId": runner_request_id });
        let m = Method::POST;
        let req = json_body(bearer(http.post(url.clone()), token), &body, None)?
            .timeout(Duration::from_secs(5));
        send(req, &m, url.as_str()).await?;
        Ok(())
    }
}

// -------------------------------------------------------------- run service

pub mod run_service {
    use super::*;

    /// Why a run-service call failed, mirroring the official exception types.
    #[derive(Debug, PartialEq, Eq)]
    pub enum JobGone {
        NotFound,
        AlreadyAcquired,
        Unprocessable,
    }

    pub fn job_gone(err: &anyhow::Error) -> Option<JobGone> {
        let e = crate::http::http_error_of(err)?;
        let code = e.run_service_code().unwrap_or(e.status);
        match code {
            404 => Some(JobGone::NotFound),
            409 => Some(JobGone::AlreadyAcquired),
            422 => Some(JobGone::Unprocessable),
            _ => None,
        }
    }

    /// POST `acquirejob`; returns the job message JSON exactly as served,
    /// which is passed through to `Runner.Worker` untouched.
    pub async fn acquire_job(
        http: &Client,
        run_service_url: &str,
        token: &str,
        message_id: &str,
        billing_owner_id: Option<&str>,
    ) -> Result<String> {
        let url = crate::util::join_url(run_service_url, "acquirejob")?;
        let mut body = serde_json::json!({
            "jobMessageId": message_id,
            "runnerOS": crate::util::os_name(),
        });
        if let Some(b) = billing_owner_id.filter(|b| !b.is_empty()) {
            body["billingOwnerId"] = Value::String(b.to_string());
        }
        let m = Method::POST;
        let req = json_body(http.post(url.clone()).bearer_auth(token), &body, None)?
            .timeout(CALL_TIMEOUT);
        Ok(send(req, &m, url.as_str()).await?.text().await?)
    }

    /// POST `renewjob` with the job's own access token. Returns `lockedUntil`.
    pub async fn renew_job(
        http: &Client,
        system_url: &str,
        job_token: &str,
        plan_id: &str,
        job_id: &str,
    ) -> Result<Option<String>> {
        let url = crate::util::join_url(system_url, "renewjob")?;
        let body = serde_json::json!({ "planId": plan_id, "jobId": job_id });
        let m = Method::POST;
        let req = json_body(http.post(url.clone()).bearer_auth(job_token), &body, None)?
            .timeout(CALL_TIMEOUT);
        let v: Option<Value> = optional_json(send(req, &m, url.as_str()).await?).await?;
        Ok(v.and_then(|v| {
            v.get("lockedUntil")
                .and_then(|x| x.as_str().map(String::from))
        }))
    }

    /// POST `completejob`. Only used when the worker crashed before it could
    /// report a result itself (the worker normally completes the job).
    #[allow(clippy::too_many_arguments)]
    pub async fn complete_job(
        http: &Client,
        system_url: &str,
        job_token: &str,
        plan_id: &str,
        job_id: &str,
        conclusion: &str,
        annotation: Option<&str>,
        billing_owner_id: Option<&str>,
    ) -> Result<()> {
        let url = crate::util::join_url(system_url, "completejob")?;
        let mut body = serde_json::json!({
            "planId": plan_id,
            "jobId": job_id,
            "conclusion": conclusion,
        });
        if let Some(msg) = annotation {
            body["annotations"] = serde_json::json!([{
                "level": "failure",
                "message": msg,
                "isInfrastructureIssue": true,
            }]);
        }
        if let Some(b) = billing_owner_id.filter(|b| !b.is_empty()) {
            body["billingOwnerId"] = Value::String(b.to_string());
        }
        let m = Method::POST;
        let req = json_body(http.post(url.clone()).bearer_auth(job_token), &body, None)?
            .timeout(CALL_TIMEOUT);
        send(req, &m, url.as_str()).await?;
        Ok(())
    }
}

// --------------------------------------------------------------- encryption

/// Session encryption for legacy (pipelines) sessions: the service sends an
/// AES key wrapped with the runner's RSA public key, and message bodies are
/// AES-CBC encrypted with a per-message IV.
pub fn unwrap_session_key(
    key: &rsa::RsaPrivateKey,
    session: &SessionResponse,
) -> Result<Option<Vec<u8>>> {
    let Some(sk) = &session.encryption_key else {
        return Ok(None);
    };
    let Some(value) = sk.value.as_deref().filter(|v| !v.is_empty()) else {
        return Ok(None);
    };
    let raw = STANDARD
        .decode(value)
        .context("session key is not base64")?;
    if !sk.encrypted {
        return Ok(Some(raw));
    }
    let plain = if session.use_fips_encryption {
        key.decrypt(rsa::Oaep::new::<sha2::Sha256>(), &raw)
    } else {
        key.decrypt(rsa::Oaep::new::<sha1::Sha1>(), &raw)
    }
    .context("cannot unwrap session key")?;
    Ok(Some(plain))
}

pub fn decrypt_body(session_key: &[u8], iv_b64: &str, body_b64: &str) -> Result<String> {
    use aes::cipher::{BlockModeDecrypt, KeyIvInit, block_padding::Pkcs7};
    let iv = STANDARD
        .decode(iv_b64)
        .context("message IV is not base64")?;
    let data = STANDARD
        .decode(body_b64)
        .context("message body is not base64")?;
    let plain = match session_key.len() {
        16 => cbc::Decryptor::<aes::Aes128>::new_from_slices(session_key, &iv)
            .map_err(|_| anyhow::anyhow!("bad AES key/IV length"))?
            .decrypt_padded_vec::<Pkcs7>(&data),
        24 => cbc::Decryptor::<aes::Aes192>::new_from_slices(session_key, &iv)
            .map_err(|_| anyhow::anyhow!("bad AES key/IV length"))?
            .decrypt_padded_vec::<Pkcs7>(&data),
        32 => cbc::Decryptor::<aes::Aes256>::new_from_slices(session_key, &iv)
            .map_err(|_| anyhow::anyhow!("bad AES key/IV length"))?
            .decrypt_padded_vec::<Pkcs7>(&data),
        n => bail!("unsupported session key length {n}"),
    }
    .map_err(|_| anyhow::anyhow!("message body padding is invalid"))?;
    // .NET's StreamReader skips a UTF-8 BOM if one is present.
    let plain = plain.strip_prefix(&[0xEF, 0xBB, 0xBF]).unwrap_or(&plain);
    Ok(String::from_utf8(plain.to_vec())?)
}

/// Read a value out of a .NET `PropertiesCollection`, which serializes either
/// as a plain value or as `{"$type": "...", "$value": "..."}`.
pub fn property<'a>(props: &'a Value, name: &str) -> Option<&'a Value> {
    let obj = props.as_object()?;
    let v = obj
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v)?;
    match v.get("$value") {
        Some(inner) => Some(inner),
        None => Some(v),
    }
}

pub fn property_bool(props: &Value, name: &str) -> Option<bool> {
    match property(props, name)? {
        Value::Bool(b) => Some(*b),
        Value::String(s) => {
            s.parse::<bool>()
                .ok()
                .or_else(|| match s.to_ascii_lowercase().as_str() {
                    "true" => Some(true),
                    "false" => Some(false),
                    _ => None,
                })
        }
        _ => None,
    }
}

pub fn property_str(props: &Value, name: &str) -> Option<String> {
    match property(props, name)? {
        Value::String(s) => Some(s.clone()),
        Value::Null => None,
        other => Some(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn properties_collection_forms() {
        let props = serde_json::json!({
            "ServerUrlV2": {"$type": "System.String", "$value": "https://broker.example/"},
            "UseV2Flow": {"$type": "System.Boolean", "$value": "True"},
            "RequireFipsCryptography": true,
        });
        assert_eq!(
            property_str(&props, "serverurlv2").as_deref(),
            Some("https://broker.example/")
        );
        assert_eq!(property_bool(&props, "UseV2Flow"), Some(true));
        assert_eq!(property_bool(&props, "RequireFipsCryptography"), Some(true));
        assert_eq!(property_bool(&props, "Missing"), None);
    }

    #[test]
    fn message_decryption_round_trip() {
        use aes::cipher::{BlockModeEncrypt, KeyIvInit, block_padding::Pkcs7};
        let key = [7u8; 32];
        let iv = [9u8; 16];
        let mut plain = vec![0xEF, 0xBB, 0xBF];
        plain.extend_from_slice(br#"{"hello":"world"}"#);
        let ct = cbc::Encryptor::<aes::Aes256>::new_from_slices(&key, &iv)
            .unwrap()
            .encrypt_padded_vec::<Pkcs7>(&plain);
        let out = decrypt_body(&key, &STANDARD.encode(iv), &STANDARD.encode(ct)).unwrap();
        assert_eq!(out, r#"{"hello":"world"}"#);
    }

    #[test]
    fn session_key_unwrap() {
        let key = rsa::RsaPrivateKey::new(&mut rand::rngs::OsRng, 2048).unwrap();
        let public = rsa::RsaPublicKey::from(&key);
        let aes_key = [3u8; 32];
        let wrapped = public
            .encrypt(
                &mut rand::rngs::OsRng,
                rsa::Oaep::new::<sha1::Sha1>(),
                &aes_key,
            )
            .unwrap();
        let resp = SessionResponse {
            session_id: "s".into(),
            encryption_key: Some(SessionKey {
                encrypted: true,
                value: Some(STANDARD.encode(wrapped)),
            }),
            use_fips_encryption: false,
            broker_migration_message: None,
        };
        assert_eq!(
            unwrap_session_key(&key, &resp).unwrap().unwrap(),
            aes_key.to_vec()
        );
    }

    #[test]
    fn job_request_ref_parses() {
        let body = r#"{"id":"1","runner_request_id":"abc","should_acknowledge":true,"run_service_url":"https://run.example/1/","billing_owner_id":"o"}"#;
        let r: RunnerJobRequestRef = serde_json::from_str(body).unwrap();
        assert_eq!(r.runner_request_id, "abc");
        assert!(r.should_acknowledge);
    }
}
