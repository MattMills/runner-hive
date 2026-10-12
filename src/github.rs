//! GitHub REST API calls made with the operator's token (PAT or `gh` token).
//! Only used for discovery, (un)registration and downloading the worker; job
//! traffic never touches the REST API, so 150 repos cost no rate limit.

use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::{Client, Method};
use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::http::HttpError;
use crate::identity::Scope;

#[derive(Clone)]
pub struct GitHub {
    http: Client,
    pub web_url: String,
    pub api_url: String,
    token: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RepoInfo {
    pub name: String,
    pub full_name: String,
    #[serde(default)]
    pub private: bool,
    #[serde(default)]
    pub archived: bool,
    #[serde(default)]
    pub fork: bool,
    #[serde(default)]
    pub disabled: bool,
    #[serde(default)]
    pub permissions: Option<Permissions>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Permissions {
    #[serde(default)]
    pub admin: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RunnerInfo {
    pub id: u64,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub busy: bool,
}

#[derive(Deserialize)]
struct RunnerList {
    #[serde(default)]
    runners: Vec<RunnerInfo>,
}

#[derive(Deserialize)]
struct TokenResp {
    token: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Release {
    pub tag_name: String,
    #[serde(default)]
    pub body: Option<String>,
    #[serde(default)]
    pub assets: Vec<Asset>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Asset {
    pub name: String,
    pub browser_download_url: String,
}

#[derive(Deserialize)]
struct UserResp {
    login: String,
    #[serde(rename = "type", default)]
    kind: String,
}

impl GitHub {
    pub fn new(http: Client, server_url: &str, token: String) -> Self {
        let web_url = server_url.trim_end_matches('/').to_string();
        let api_url = if web_url.eq_ignore_ascii_case("https://github.com") {
            "https://api.github.com".to_string()
        } else {
            format!("{web_url}/api/v3")
        };
        Self {
            http,
            web_url,
            api_url,
            token,
        }
    }

    pub fn http(&self) -> &Client {
        &self.http
    }

    async fn send(&self, method: Method, path: &str) -> Result<reqwest::Response> {
        let url = if path.starts_with("http") {
            path.to_string()
        } else {
            format!("{}/{}", self.api_url, path.trim_start_matches('/'))
        };
        let resp = self
            .http
            .request(method.clone(), &url)
            .bearer_auth(&self.token)
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .timeout(Duration::from_secs(60))
            .send()
            .await
            .with_context(|| format!("{method} {url}"))?;
        if resp.status().is_success() {
            return Ok(resp);
        }
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        Err(HttpError {
            status,
            method: method.to_string(),
            url,
            body,
        }
        .into())
    }

    async fn json<T: DeserializeOwned>(&self, method: Method, path: &str) -> Result<T> {
        let resp = self.send(method, path).await?;
        Ok(resp.json().await?)
    }

    pub async fn login(&self) -> Result<String> {
        let u: UserResp = self.json(Method::GET, "user").await?;
        Ok(u.login)
    }

    /// All repositories under `owner` that the token can see. For the
    /// authenticated user this includes private repos.
    pub async fn list_repos(&self, owner: &str) -> Result<Vec<RepoInfo>> {
        let me = self.login().await?;
        let base = if owner.eq_ignore_ascii_case(&me) {
            "user/repos?affiliation=owner&visibility=all".to_string()
        } else {
            let u: UserResp = self.json(Method::GET, &format!("users/{owner}")).await?;
            if u.kind == "Organization" {
                format!("orgs/{}/repos?type=all", u.login)
            } else {
                format!("users/{}/repos?type=owner", u.login)
            }
        };
        let mut out = Vec::new();
        for page in 1.. {
            let batch: Vec<RepoInfo> = self
                .json(Method::GET, &format!("{base}&per_page=100&page={page}"))
                .await?;
            let n = batch.len();
            out.extend(batch);
            if n < 100 {
                break;
            }
        }
        Ok(out)
    }

    pub async fn registration_token(&self, scope: &Scope) -> Result<String> {
        let t: TokenResp = self
            .json(
                Method::POST,
                &format!("{}/actions/runners/registration-token", scope.api_prefix()),
            )
            .await
            .with_context(|| {
                format!(
                    "cannot get a registration token for {scope} (the token needs admin rights)"
                )
            })?;
        Ok(t.token)
    }

    pub async fn list_runners(&self, scope: &Scope) -> Result<Vec<RunnerInfo>> {
        let mut out = Vec::new();
        for page in 1.. {
            let list: RunnerList = self
                .json(
                    Method::GET,
                    &format!(
                        "{}/actions/runners?per_page=100&page={page}",
                        scope.api_prefix()
                    ),
                )
                .await?;
            let n = list.runners.len();
            out.extend(list.runners);
            if n < 100 {
                break;
            }
        }
        Ok(out)
    }

    pub async fn delete_runner(&self, scope: &Scope, runner_id: u64) -> Result<()> {
        match self
            .send(
                Method::DELETE,
                &format!("{}/actions/runners/{runner_id}", scope.api_prefix()),
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if crate::http::status_of(&e) == Some(404) => Ok(()),
            Err(e) => Err(e),
        }
    }

    pub async fn runner_release(&self, version: Option<&str>) -> Result<Release> {
        let path = match version {
            None => "repos/actions/runner/releases/latest".to_string(),
            Some(v) => format!(
                "repos/actions/runner/releases/tags/v{}",
                v.trim_start_matches('v')
            ),
        };
        // Releases live on github.com even when targeting GHES.
        let url = format!("https://api.github.com/{path}");
        let mut req = self
            .http
            .get(&url)
            .header("Accept", "application/vnd.github+json")
            .timeout(Duration::from_secs(60));
        // A GHES token means nothing to github.com; go anonymous there.
        if self.api_url == "https://api.github.com" {
            req = req.bearer_auth(&self.token);
        }
        let resp = req.send().await?;
        let resp = crate::http::check(resp).await?;
        Ok(resp.json().await?)
    }
}
