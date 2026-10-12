//! `hive.toml`: which repos to serve, how many jobs at once, and where the
//! official worker lives.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Where registrations, the worker package and job slots live.
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,

    /// Jobs that may run at the same time across all repos.
    #[serde(default = "default_max_jobs")]
    pub max_jobs: usize,

    /// Registrations per repo (or org). A registration runs one job at a
    /// time, so this caps per-repo parallelism (e.g. matrix builds).
    #[serde(default = "default_one")]
    pub runners_per_repo: usize,

    /// Runner name shown in GitHub. `{hostname}` is substituted; with
    /// `runners_per_repo > 1` a `-N` suffix is added.
    #[serde(default = "default_runner_name")]
    pub runner_name: String,

    /// Extra labels, on top of `self-hosted`, the OS and the architecture.
    #[serde(default)]
    pub labels: Vec<String>,

    #[serde(default)]
    pub no_default_labels: bool,

    /// Runner group for org registrations (repo runners always use Default).
    #[serde(default)]
    pub runner_group: Option<String>,

    /// Re-discover repos this often while running (0 disables).
    #[serde(default = "default_sync_minutes")]
    pub sync_interval_minutes: u64,

    /// Remove registrations for repos that no longer match any target.
    #[serde(default = "default_true")]
    pub prune: bool,

    #[serde(default)]
    pub github: GitHubConfig,

    #[serde(default)]
    pub worker: WorkerConfig,

    #[serde(default, rename = "target")]
    pub targets: Vec<Target>,

    /// Extra environment for every job (the runner's `.env` file equivalent).
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitHubConfig {
    #[serde(default = "default_github_url")]
    pub url: String,
    /// Environment variable holding the admin token (PAT / gh token).
    #[serde(default)]
    pub token_env: Option<String>,
    /// Command whose stdout is the token, e.g. `gh auth token`.
    #[serde(default)]
    pub token_command: Option<String>,
}

impl Default for GitHubConfig {
    fn default() -> Self {
        Self {
            url: default_github_url(),
            token_env: None,
            token_command: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerConfig {
    /// Official runner version to execute jobs with: "latest" or "2.338.0".
    #[serde(default = "default_latest")]
    pub version: String,
    /// Use an already-extracted actions/runner directory instead of
    /// downloading one.
    #[serde(default)]
    pub path: Option<PathBuf>,
    /// Forward each worker's stdout/stderr into the hive log.
    #[serde(default)]
    pub log_output: bool,
    /// Delete a repo's checkout under `_work` after each job.
    #[serde(default)]
    pub clean_workspace: bool,
    /// Run jobs as this user while the daemon (root) keeps the admin token and
    /// runner keys out of their reach. Unix only.
    #[serde(default)]
    pub run_as: Option<String>,
    /// Give each slot its own HOME (`<slot>/home`). Set CARGO_HOME /
    /// RUSTUP_HOME in [env] if toolchains live in the job user's real home.
    #[serde(default)]
    pub slot_home: bool,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            version: default_latest(),
            path: None,
            log_output: false,
            clean_workspace: false,
            run_as: None,
            slot_home: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Target {
    /// One repository: "owner/name".
    #[serde(default)]
    pub repo: Option<String>,
    /// Every repository of a user or organization, registered per repo.
    #[serde(default)]
    pub owner: Option<String>,
    /// One organization-level registration that serves all of its repos.
    #[serde(default)]
    pub org: Option<String>,

    /// Glob patterns on the repo name (owner targets only).
    #[serde(default)]
    pub include: Vec<String>,
    #[serde(default)]
    pub exclude: Vec<String>,
    #[serde(default)]
    pub include_archived: bool,
    #[serde(default = "default_true")]
    pub include_forks: bool,
    /// Self-hosted runners on public repos can run code from fork pull
    /// requests. Off unless you opt in.
    #[serde(default)]
    pub include_public: bool,

    /// Per-target override of `runners_per_repo`.
    #[serde(default)]
    pub runners_per_repo: Option<usize>,
    /// Labels added for this target only.
    #[serde(default)]
    pub labels: Vec<String>,
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("hive-data")
}
fn default_max_jobs() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get().div_ceil(2))
        .unwrap_or(2)
        .max(1)
}
fn default_one() -> usize {
    1
}
fn default_runner_name() -> String {
    "hive-{hostname}".to_string()
}
fn default_sync_minutes() -> u64 {
    30
}
fn default_true() -> bool {
    true
}
fn default_github_url() -> String {
    "https://github.com".to_string()
}
fn default_latest() -> String {
    "latest".to_string()
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("cannot read config {}", path.display()))?;
        let mut cfg: Config =
            toml::from_str(&text).with_context(|| format!("invalid config {}", path.display()))?;
        // Relative data_dir is relative to the config file, not the cwd.
        if cfg.data_dir.is_relative() {
            let base = path.parent().unwrap_or(Path::new("."));
            cfg.data_dir = base.join(&cfg.data_dir);
        }
        cfg.data_dir = std::path::absolute(&cfg.data_dir)?;
        if let Some(p) = cfg.worker.path.as_mut()
            && p.is_relative()
        {
            let base = path.parent().unwrap_or(Path::new("."));
            *p = base.join(&*p);
        }
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<()> {
        if self.max_jobs == 0 {
            bail!("max_jobs must be at least 1");
        }
        if self.runners_per_repo == 0 {
            bail!("runners_per_repo must be at least 1");
        }
        for (i, t) in self.targets.iter().enumerate() {
            let kinds = [t.repo.is_some(), t.owner.is_some(), t.org.is_some()]
                .iter()
                .filter(|x| **x)
                .count();
            if kinds != 1 {
                bail!(
                    "[[target]] #{} must set exactly one of repo, owner or org",
                    i + 1
                );
            }
        }
        Ok(())
    }

    pub fn base_runner_name(&self) -> String {
        self.runner_name
            .replace("{hostname}", &crate::util::hostname())
    }

    /// Resolve the admin token: config env var, `GITHUB_TOKEN`/`GH_TOKEN`,
    /// a configured command, then `gh auth token`.
    pub fn resolve_token(&self) -> Result<String> {
        if let Some(var) = &self.github.token_env {
            return std::env::var(var)
                .with_context(|| format!("token_env '{var}' is not set"))
                .map(|t| t.trim().to_string());
        }
        for var in ["RUNNER_HIVE_TOKEN", "GITHUB_TOKEN", "GH_TOKEN"] {
            if let Ok(t) = std::env::var(var)
                && !t.trim().is_empty()
            {
                return Ok(t.trim().to_string());
            }
        }
        let cmd = self
            .github
            .token_command
            .clone()
            .unwrap_or_else(|| "gh auth token".to_string());
        let mut parts = cmd.split_whitespace();
        let prog = parts.next().context("empty token_command")?;
        let out = std::process::Command::new(prog)
            .args(parts)
            .output()
            .with_context(|| {
                format!("no token: set GITHUB_TOKEN or install gh ('{cmd}' failed)")
            })?;
        if !out.status.success() {
            bail!(
                "'{cmd}' failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        let token = String::from_utf8(out.stdout)?.trim().to_string();
        if token.is_empty() {
            bail!("'{cmd}' printed no token");
        }
        Ok(token)
    }

    /// Environment variables that hold the admin token. They are scrubbed
    /// from every job's environment.
    pub fn secret_env_vars(&self) -> Vec<String> {
        let mut v = vec![
            "RUNNER_HIVE_TOKEN".to_string(),
            "GITHUB_TOKEN".to_string(),
            "GH_TOKEN".to_string(),
        ];
        if let Some(t) = &self.github.token_env {
            v.push(t.clone());
        }
        v
    }
}

pub const EXAMPLE: &str = r#"# runner-hive configuration.
# One daemon, one long-poll per registration, jobs run by the official
# GitHub Actions worker only while a job is running.

data_dir = "hive-data"     # registrations (private keys!), worker package, job slots
max_jobs = 4               # jobs running at once across ALL repos
runners_per_repo = 1       # registrations per repo = max parallel jobs per repo
runner_name = "hive-{hostname}"
labels = ["hive"]          # extra labels; self-hosted/OS/arch are added automatically
sync_interval_minutes = 30 # pick up new repos while running (0 = off)
prune = true               # unregister repos that no longer match a target

[github]
url = "https://github.com"
# Token with admin rights on the repos. Defaults: $RUNNER_HIVE_TOKEN,
# $GITHUB_TOKEN, $GH_TOKEN, then `gh auth token`.
# token_env = "MY_TOKEN_VAR"
# token_command = "gh auth token"

[worker]
version = "latest"         # official actions/runner release used to run jobs
# path = "/opt/actions-runner"  # or use an existing extracted runner
log_output = false
clean_workspace = false
# run_as = "runner"        # Unix: daemon runs as root, jobs as this user

# Every repo you own. Self-hosted runners on PUBLIC repos can run code from
# fork pull requests; only opt in if you understand that.
[[target]]
owner = "your-user-or-org"
exclude = ["*-archive"]
include_archived = false
include_forks = true
include_public = false

# A single repository:
# [[target]]
# repo = "owner/name"
# runners_per_repo = 2

# One org-level registration that serves every repo in the org:
# [[target]]
# org = "my-org"

[env]
# EXTRA_VAR = "value"
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn example_parses() {
        let cfg: Config = toml::from_str(EXAMPLE).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.max_jobs, 4);
        assert_eq!(cfg.targets.len(), 1);
        assert_eq!(cfg.targets[0].owner.as_deref(), Some("your-user-or-org"));
        assert!(!cfg.targets[0].include_public);
    }

    #[test]
    fn target_must_be_unambiguous() {
        let cfg: Config = toml::from_str("[[target]]\nrepo = \"a/b\"\norg = \"c\"\n").unwrap();
        assert!(cfg.validate().is_err());
    }
}
