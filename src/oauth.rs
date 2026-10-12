//! Runner OAuth: trade a signed client assertion for a short-lived bearer
//! token, the same exchange `VssOAuthJwtBearerClientCredential` performs.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use reqwest::Client;
use rsa::RsaPrivateKey;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::identity::Identity;

/// The token service rejected our credential. `invalid_client` means the
/// registration was deleted on GitHub's side.
#[derive(Debug)]
pub struct OAuthError {
    pub status: u16,
    pub error: String,
    pub description: String,
}

impl std::fmt::Display for OAuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "OAuth token request failed (HTTP {}): {} {}",
            self.status, self.error, self.description
        )
    }
}

impl std::error::Error for OAuthError {}

impl OAuthError {
    pub fn registration_deleted(&self) -> bool {
        self.error.eq_ignore_ascii_case("invalid_client")
    }
}

pub fn oauth_error_of(err: &anyhow::Error) -> Option<&OAuthError> {
    err.chain().find_map(|e| e.downcast_ref::<OAuthError>())
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    expires_in: Option<u64>,
}

#[derive(Deserialize, Default)]
struct TokenErrorResponse {
    #[serde(default)]
    error: String,
    #[serde(default)]
    error_description: String,
}

struct Cached {
    token: String,
    refresh_after: Instant,
}

pub struct TokenSource {
    http: Client,
    identity: Arc<Identity>,
    key: RsaPrivateKey,
    cache: Mutex<Option<Cached>>,
}

impl TokenSource {
    pub fn new(http: Client, identity: Arc<Identity>) -> Result<Self> {
        let key = identity.key()?;
        Ok(Self {
            http,
            identity,
            key,
            cache: Mutex::new(None),
        })
    }

    /// A valid bearer token, fetching a new one when the cached one is close
    /// to expiry.
    pub async fn get(&self) -> Result<String> {
        let mut cache = self.cache.lock().await;
        if let Some(c) = cache.as_ref()
            && Instant::now() < c.refresh_after
        {
            return Ok(c.token.clone());
        }
        let (token, lifetime) = self.fetch_any().await?;
        // Refresh at 80% of the lifetime, and never hold one past 50 minutes.
        let ttl = Duration::from_secs((lifetime * 4 / 5).clamp(60, 3000));
        *cache = Some(Cached {
            token: token.clone(),
            refresh_after: Instant::now() + ttl,
        });
        Ok(token)
    }

    /// Drop the cached token (after a 401 or a ForceTokenRefresh message).
    pub async fn invalidate(&self) {
        *self.cache.lock().await = None;
    }

    async fn fetch_any(&self) -> Result<(String, u64)> {
        // The official runner prefers the v2 authorization URL when the
        // service hands one out ("auth migration"), falling back to legacy.
        if let Some(v2) = self.identity.authorization_url_v2.as_deref() {
            match self.fetch(v2).await {
                Ok(t) => return Ok(t),
                Err(e) => tracing::debug!(
                    runner = %self.identity.display_key(),
                    "v2 token endpoint failed, using legacy: {e:#}"
                ),
            }
        }
        self.fetch(&self.identity.authorization_url).await
    }

    async fn fetch(&self, url: &str) -> Result<(String, u64)> {
        let assertion = self.identity.client_assertion(&self.key, url)?;
        let form = [
            (
                "client_assertion_type",
                "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
            ),
            ("client_assertion", assertion.as_str()),
            ("grant_type", "client_credentials"),
        ];
        let body = form
            .iter()
            .map(|(k, v)| format!("{k}={}", urlencode(v)))
            .collect::<Vec<_>>()
            .join("&");
        let resp = self
            .http
            .post(url)
            .header(
                "Content-Type",
                "application/x-www-form-urlencoded; charset=utf-8",
            )
            .header("Accept", "application/json")
            .body(body)
            .timeout(Duration::from_secs(60))
            .send()
            .await
            .with_context(|| format!("cannot reach token service {url}"))?;
        let status = resp.status().as_u16();
        let text = resp.text().await.unwrap_or_default();
        if !(200..300).contains(&status) {
            let parsed: TokenErrorResponse = serde_json::from_str(&text).unwrap_or_default();
            return Err(OAuthError {
                status,
                error: if parsed.error.is_empty() {
                    "http_error".into()
                } else {
                    parsed.error
                },
                description: if parsed.error_description.is_empty() {
                    text.chars().take(300).collect()
                } else {
                    parsed.error_description
                },
            }
            .into());
        }
        let tr: TokenResponse =
            serde_json::from_str(&text).context("unexpected token service response")?;
        Ok((tr.access_token, tr.expires_in.unwrap_or(3600)))
    }
}

fn urlencode(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
}
