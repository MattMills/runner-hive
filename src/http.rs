//! HTTP client construction and a typed error for non-2xx responses.

use std::time::Duration;

use anyhow::Result;
use reqwest::{Client, Response};

/// Raised for any non-success HTTP status so callers can branch on the code.
#[derive(Debug)]
pub struct HttpError {
    pub status: u16,
    pub method: String,
    pub url: String,
    pub body: String,
}

impl std::fmt::Display for HttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut body = self.body.trim().to_string();
        if body.len() > 400 {
            body.truncate(400);
            body.push_str("...");
        }
        if self.method.is_empty() {
            write!(f, "{} -> HTTP {}: {}", self.url, self.status, body)
        } else {
            write!(
                f,
                "{} {} -> HTTP {}: {}",
                self.method, self.url, self.status, body
            )
        }
    }
}

impl std::error::Error for HttpError {}

impl HttpError {
    /// `errorKind` from a broker error body, if the body is one.
    pub fn broker_error_kind(&self) -> Option<String> {
        let v: serde_json::Value = serde_json::from_str(&self.body).ok()?;
        if v.get("source")?.as_str()? != "actions-broker-listener" {
            return None;
        }
        Some(v.get("errorKind")?.as_str()?.to_string())
    }

    /// `statusCode` from a run-service error body, if the body is one.
    pub fn run_service_code(&self) -> Option<u16> {
        let v: serde_json::Value = serde_json::from_str(&self.body).ok()?;
        if v.get("source")?.as_str()? != "actions-run-service" {
            return None;
        }
        v.get("statusCode")?.as_u64().map(|c| c as u16)
    }
}

/// Status code of an `HttpError` anywhere in the chain.
pub fn status_of(err: &anyhow::Error) -> Option<u16> {
    err.chain()
        .find_map(|e| e.downcast_ref::<HttpError>())
        .map(|e| e.status)
}

pub fn http_error_of(err: &anyhow::Error) -> Option<&HttpError> {
    err.chain().find_map(|e| e.downcast_ref::<HttpError>())
}

/// Turn a non-success response into an `HttpError`.
pub async fn check(resp: Response) -> Result<Response> {
    if resp.status().is_success() {
        return Ok(resp);
    }
    let status = resp.status().as_u16();
    let url = resp.url().to_string();
    let body = resp.text().await.unwrap_or_default();
    Err(HttpError {
        status,
        method: String::new(),
        url,
        body,
    }
    .into())
}

pub fn user_agent() -> String {
    format!(
        "runner-hive/{} ({})",
        env!("CARGO_PKG_VERSION"),
        crate::util::runner_package_platform().unwrap_or("unknown")
    )
}

/// One shared client: connection pooling and HTTP/2 multiplexing mean that
/// hundreds of long-polls to the same broker share a handful of sockets.
pub fn build_client() -> Result<Client> {
    Ok(Client::builder()
        .user_agent(user_agent())
        .connect_timeout(Duration::from_secs(30))
        .pool_idle_timeout(Duration::from_secs(90))
        .http2_keep_alive_interval(Duration::from_secs(30))
        .http2_keep_alive_while_idle(true)
        .build()?)
}

/// HTTP/2 servers cap concurrent streams per connection (the broker allows
/// 100) and hyper queues requests beyond the cap instead of opening another
/// connection, so a long-poll stuck in that queue times out. Listeners are
/// therefore spread over one client (one connection per host) per this many.
pub const LISTENERS_PER_CLIENT: usize = 50;

#[derive(Default)]
pub struct ClientPool {
    clients: Vec<Client>,
    assigned: usize,
}

impl ClientPool {
    pub fn next(&mut self) -> Result<Client> {
        if self.assigned.is_multiple_of(LISTENERS_PER_CLIENT) {
            self.clients.push(build_client()?);
        }
        self.assigned += 1;
        Ok(self.clients.last().expect("pushed above").clone())
    }
}
