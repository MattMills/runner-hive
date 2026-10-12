//! Registering and removing runners, mirroring `config.sh` / `config.sh remove`
//! (Runner.Listener/Configuration/ConfigurationManager.cs).

use std::time::Duration;

use anyhow::{Context, Result, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use reqwest::Method;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::github::GitHub;
use crate::identity::{self, Identity, Scope};
use crate::protocol::{self, pipelines};

pub struct RegisterRequest<'a> {
    pub scope: &'a Scope,
    pub name: &'a str,
    pub labels: &'a [String],
    pub no_default_labels: bool,
    pub runner_group: Option<&'a str>,
    pub runner_version: &'a str,
}

#[derive(Deserialize)]
struct TenantAuth {
    url: String,
    token: String,
    #[serde(default)]
    token_schema: String,
    #[serde(default)]
    use_v2_flow: bool,
}

/// Labels the official runner adds unless `--no-default-labels`.
pub fn default_labels() -> Vec<String> {
    vec![
        "self-hosted".to_string(),
        crate::util::os_name().to_string(),
        crate::util::arch_name().to_string(),
    ]
}

fn label_list(req: &RegisterRequest) -> Vec<Value> {
    let mut out = Vec::new();
    if !req.no_default_labels {
        for l in default_labels() {
            out.push(json!({ "name": l, "type": "system" }));
        }
    }
    for l in req.labels {
        if !out.iter().any(|v| v["name"].as_str() == Some(l.as_str())) {
            out.push(json!({ "name": l, "type": "user" }));
        }
    }
    out
}

async fn remote_auth(
    gh: &GitHub,
    method: Method,
    url: &str,
    reg_token: &str,
    body: Option<&Value>,
) -> Result<reqwest::Response> {
    let mut req = gh
        .http()
        .request(method.clone(), url)
        .header("Authorization", format!("RemoteAuth {reg_token}"))
        .header("Accept", "application/json")
        .timeout(Duration::from_secs(60));
    if let Some(b) = body {
        req = req
            .header("Content-Type", "application/json")
            .body(serde_json::to_vec(b)?);
    }
    let resp = req
        .send()
        .await
        .with_context(|| format!("{method} {url}"))?;
    if resp.status().is_success() {
        return Ok(resp);
    }
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap_or_default();
    Err(crate::http::HttpError {
        status,
        method: method.to_string(),
        url: url.to_string(),
        body: text,
    }
    .into())
}

pub async fn register(gh: &GitHub, req: RegisterRequest<'_>) -> Result<Identity> {
    let github_url = req.scope.github_url(&gh.web_url);
    let reg_token = gh.registration_token(req.scope).await?;

    let auth: TenantAuth = remote_auth(
        gh,
        Method::POST,
        &format!("{}/actions/runner-registration", gh.api_url),
        &reg_token,
        Some(&json!({ "url": github_url, "runner_event": "register" })),
    )
    .await
    .context("runner-registration failed")?
    .json()
    .await?;

    let key = identity::generate_key()?;
    let labels: Vec<String> = label_list(&req)
        .iter()
        .filter_map(|v| v["name"].as_str().map(String::from))
        .collect();

    let id = if auth.use_v2_flow {
        register_runner_admin(gh, &req, &github_url, &reg_token, &auth, &key, labels).await?
    } else {
        if !auth.token_schema.eq_ignore_ascii_case("OAuthAccessToken") {
            bail!("unsupported tenant token schema '{}'", auth.token_schema);
        }
        register_legacy(gh, &req, &github_url, &auth, &key, labels).await?
    };
    Ok(id)
}

/// Legacy flow: talk to the pipelines tenant with the tenant token.
async fn register_legacy(
    gh: &GitHub,
    req: &RegisterRequest<'_>,
    github_url: &str,
    auth: &TenantAuth,
    key: &rsa::RsaPrivateKey,
    labels: Vec<String>,
) -> Result<Identity> {
    let http = gh.http();
    let pools = pipelines::pools(http, &auth.url, &auth.token).await?;
    let pool = pick_pool(
        pools
            .iter()
            .map(|p| (p.id, p.name.as_str(), p.is_hosted, p.is_internal)),
        req.runner_group,
    )?;

    let (e, n) = identity::public_parts(key);
    let mut agent = json!({
        "name": req.name,
        "version": req.runner_version,
        "osDescription": crate::util::os_description(),
        "maxParallelism": 1,
        "ephemeral": false,
        "disableUpdate": true,
        "labels": label_list(req),
        "authorization": {
            "publicKey": { "exponent": STANDARD.encode(e), "modulus": STANDARD.encode(n) }
        },
    });

    let existing = pipelines::agents_named(http, &auth.url, &auth.token, pool.0, req.name).await?;
    let created = if let Some(old) = existing.first() {
        let old_id = old["id"].as_u64().context("existing runner has no id")?;
        tracing::info!(
            "{}: replacing existing runner '{}' (id {old_id})",
            req.scope,
            req.name
        );
        agent["id"] = json!(old_id);
        pipelines::replace_agent(http, &auth.url, &auth.token, pool.0, old_id, &agent).await?
    } else {
        pipelines::add_agent(http, &auth.url, &auth.token, pool.0, &agent).await?
    };

    let agent_id = created["id"]
        .as_u64()
        .context("registration returned no runner id")?;
    let authz = &created["authorization"];
    let client_id = authz["clientId"]
        .as_str()
        .context("registration returned no OAuth client id")?
        .to_string();
    let authorization_url = authz["authorizationUrl"]
        .as_str()
        .context("registration returned no authorization url")?
        .to_string();
    let props = created.get("properties").cloned().unwrap_or(Value::Null);
    let server_url_v2 = protocol::property_str(&props, "ServerUrlV2").filter(|s| !s.is_empty());
    let use_v2_flow = protocol::property_bool(&props, "UseV2Flow").unwrap_or(false);
    // The official runner defaults this to true when the property is absent.
    let require_fips = protocol::property_bool(&props, "RequireFipsCryptography").unwrap_or(true);

    Ok(Identity {
        scope: req.scope.clone(),
        github_url: github_url.to_string(),
        name: req.name.to_string(),
        agent_id,
        pool_id: pool.0,
        pool_name: pool.1,
        server_url: auth.url.clone(),
        server_url_v2,
        use_v2_flow,
        client_id,
        authorization_url,
        authorization_url_v2: None,
        require_fips,
        labels,
        private_key_pem: identity::key_to_pem(key)?,
        registered_version: req.runner_version.to_string(),
        last_session_id: None,
    })
}

#[derive(Deserialize)]
struct RunnerGroups {
    #[serde(default)]
    runner_groups: Vec<RunnerGroup>,
}

#[derive(Deserialize)]
struct RunnerGroup {
    id: i64,
    name: String,
    #[serde(default)]
    default: bool,
    #[serde(default)]
    is_hosted: bool,
}

#[derive(Deserialize)]
struct AdminRunner {
    id: u64,
    authorization: AdminAuthorization,
}

#[derive(Deserialize)]
struct AdminAuthorization {
    authorization_url: String,
    #[serde(default)]
    legacy_authorization_url: Option<String>,
    #[serde(default)]
    server_url: Option<String>,
    client_id: String,
}

#[derive(Deserialize)]
struct AdminRunnerList {
    #[serde(default)]
    runners: Vec<AdminRunnerRef>,
}

#[derive(Deserialize)]
struct AdminRunnerRef {
    id: u64,
    name: String,
}

/// Runner-admin (v2) flow: everything through api.github.com with the
/// registration token.
async fn register_runner_admin(
    gh: &GitHub,
    req: &RegisterRequest<'_>,
    github_url: &str,
    reg_token: &str,
    auth: &TenantAuth,
    key: &rsa::RsaPrivateKey,
    labels: Vec<String>,
) -> Result<Identity> {
    let scoped = format!("{}/{}/actions", gh.api_url, req.scope.api_prefix());
    let groups: RunnerGroups = remote_auth(
        gh,
        Method::GET,
        &format!("{scoped}/runner-groups"),
        reg_token,
        None,
    )
    .await?
    .json()
    .await?;
    let pool = pick_pool(
        groups
            .runner_groups
            .iter()
            .map(|g| (g.id, g.name.as_str(), g.is_hosted, g.default)),
        req.runner_group,
    )?;

    let existing: AdminRunnerList = remote_auth(
        gh,
        Method::GET,
        &format!(
            "{scoped}/runners?name={}",
            url::form_urlencoded::byte_serialize(req.name.as_bytes()).collect::<String>()
        ),
        reg_token,
        None,
    )
    .await?
    .json()
    .await?;

    let mut body = json!({
        "url": github_url,
        "group_id": pool.0,
        "name": req.name,
        "version": req.runner_version,
        "updates_disabled": true,
        "ephemeral": false,
        "labels": label_list(req),
        "public_key": identity::public_key_xml(key),
    });
    if let Some(old) = existing.runners.iter().find(|r| r.name == req.name) {
        tracing::info!(
            "{}: replacing existing runner '{}' (id {})",
            req.scope,
            req.name,
            old.id
        );
        body["runner_id"] = json!(old.id);
        body["replace"] = json!(true);
    }
    let runner: AdminRunner = remote_auth(
        gh,
        Method::POST,
        &format!("{}/actions/runners/register", gh.api_url),
        reg_token,
        Some(&body),
    )
    .await
    .context("runner registration failed")?
    .json()
    .await?;

    let a = runner.authorization;
    let (authorization_url, authorization_url_v2) =
        match a.legacy_authorization_url.filter(|u| !u.is_empty()) {
            Some(legacy) => (legacy, Some(a.authorization_url)),
            None => (a.authorization_url, None),
        };
    Ok(Identity {
        scope: req.scope.clone(),
        github_url: github_url.to_string(),
        name: req.name.to_string(),
        agent_id: runner.id,
        pool_id: pool.0,
        pool_name: pool.1,
        server_url: auth.url.clone(),
        server_url_v2: a.server_url.filter(|s| !s.is_empty()),
        use_v2_flow: true,
        client_id: a.client_id,
        authorization_url,
        authorization_url_v2,
        require_fips: true,
        labels,
        private_key_pem: identity::key_to_pem(key)?,
        registered_version: req.runner_version.to_string(),
        last_session_id: None,
    })
}

/// Choose the runner group: the named one, else the default (internal) one.
fn pick_pool<'a>(
    pools: impl Iterator<Item = (i64, &'a str, bool, bool)>,
    wanted: Option<&str>,
) -> Result<(i64, String)> {
    let pools: Vec<_> = pools.filter(|p| !p.2).collect();
    let chosen = match wanted {
        Some(w) => pools.iter().find(|p| p.1.eq_ignore_ascii_case(w)),
        None => pools.iter().find(|p| p.3).or(pools.first()),
    };
    match chosen {
        Some(p) => Ok((p.0, p.1.to_string())),
        None => match wanted {
            Some(w) => bail!("no self-hosted runner group named '{w}'"),
            None => bail!("no self-hosted runner group available"),
        },
    }
}

/// Delete the registration on GitHub and the local credential.
pub async fn unregister(gh: &GitHub, id: &Identity, data_dir: &std::path::Path) -> Result<()> {
    gh.delete_runner(&id.scope, id.agent_id)
        .await
        .with_context(|| format!("cannot remove runner {} from {}", id.name, id.scope))?;
    id.remove_file(data_dir)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_selection() {
        let pools = vec![
            (2, "GitHub Actions", true, false),
            (1, "Default", false, true),
            (5, "big", false, false),
        ];
        assert_eq!(pick_pool(pools.clone().into_iter(), None).unwrap().0, 1);
        assert_eq!(
            pick_pool(pools.clone().into_iter(), Some("BIG")).unwrap().0,
            5
        );
        assert!(pick_pool(pools.into_iter(), Some("GitHub Actions")).is_err());
    }

    #[test]
    fn labels_dedupe_and_types() {
        let extra = vec!["hive".to_string(), "self-hosted".to_string()];
        let scope: Scope = "a/b".parse().unwrap();
        let req = RegisterRequest {
            scope: &scope,
            name: "x",
            labels: &extra,
            no_default_labels: false,
            runner_group: None,
            runner_version: "2.338.0",
        };
        let l = label_list(&req);
        assert_eq!(l.len(), 4);
        assert_eq!(l[0]["type"], "system");
        assert_eq!(l[3]["name"], "hive");
        assert_eq!(l[3]["type"], "user");
    }
}
