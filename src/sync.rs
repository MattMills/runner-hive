//! Reconcile registrations with the `[[target]]`s in the config: register
//! what is missing, re-register when labels change, prune what is gone.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, bail};
use futures_util::StreamExt;
use globset::{Glob, GlobSet, GlobSetBuilder};

use crate::config::{Config, Target};
use crate::github::GitHub;
use crate::identity::{self, Identity, Scope};
use crate::register::{self, RegisterRequest};

#[derive(Debug, Clone)]
pub struct Wanted {
    pub scope: Scope,
    pub name: String,
    /// Every label the runner should end up with (for change detection).
    pub labels: Vec<String>,
    /// Labels beyond the defaults (what is sent as user labels).
    pub extra_labels: Vec<String>,
}

#[derive(Debug, Default)]
pub struct Report {
    pub registered: Vec<String>,
    pub relabeled: Vec<String>,
    pub removed: Vec<String>,
    pub unchanged: usize,
    pub failed: Vec<(String, String)>,
    pub notes: Vec<String>,
}

fn globs(patterns: &[String]) -> Result<Option<GlobSet>> {
    if patterns.is_empty() {
        return Ok(None);
    }
    let mut b = GlobSetBuilder::new();
    for p in patterns {
        b.add(Glob::new(&p.to_lowercase())?);
    }
    Ok(Some(b.build()?))
}

fn names_for(base: &str, count: usize) -> Vec<String> {
    if count <= 1 {
        vec![base.to_string()]
    } else {
        (1..=count).map(|i| format!("{base}-{i}")).collect()
    }
}

fn full_labels(cfg: &Config, target: &Target) -> Vec<String> {
    let mut out = if cfg.no_default_labels {
        Vec::new()
    } else {
        register::default_labels()
    };
    for l in cfg.labels.iter().chain(target.labels.iter()) {
        if !out.iter().any(|x| x.eq_ignore_ascii_case(l)) {
            out.push(l.clone());
        }
    }
    out
}

/// Expand targets into the exact set of registrations we want.
pub async fn wanted(cfg: &Config, gh: &GitHub) -> Result<(Vec<Wanted>, Vec<String>)> {
    let base = cfg.base_runner_name();
    let mut out: BTreeMap<(Scope, String), Wanted> = BTreeMap::new();
    let mut notes = Vec::new();
    for t in &cfg.targets {
        let count = t.runners_per_repo.unwrap_or(cfg.runners_per_repo).max(1);
        let labels = full_labels(cfg, t);
        let extra_labels: Vec<String> = cfg.labels.iter().chain(t.labels.iter()).cloned().collect();
        let mut scopes = Vec::new();
        if let Some(r) = &t.repo {
            scopes.push(r.parse::<Scope>()?);
        } else if let Some(o) = &t.org {
            scopes.push(Scope::Org { org: o.clone() });
        } else if let Some(owner) = &t.owner {
            let include = globs(&t.include)?;
            let exclude = globs(&t.exclude)?;
            let repos = gh.list_repos(owner).await?;
            let (mut archived, mut public, mut forks, mut no_admin, mut filtered) = (0, 0, 0, 0, 0);
            for r in repos {
                let name = r.name.to_lowercase();
                if r.archived && !t.include_archived {
                    archived += 1;
                    continue;
                }
                if r.disabled {
                    continue;
                }
                if r.fork && !t.include_forks {
                    forks += 1;
                    continue;
                }
                if !r.private && !t.include_public {
                    public += 1;
                    continue;
                }
                if include.as_ref().is_some_and(|g| !g.is_match(&name))
                    || exclude.as_ref().is_some_and(|g| g.is_match(&name))
                {
                    filtered += 1;
                    continue;
                }
                if r.permissions.as_ref().is_some_and(|p| !p.admin) {
                    no_admin += 1;
                    continue;
                }
                scopes.push(r.full_name.parse::<Scope>()?);
            }
            let mut skipped = Vec::new();
            if archived > 0 {
                skipped.push(format!("{archived} archived"));
            }
            if public > 0 {
                skipped.push(format!(
                    "{public} public (set include_public = true to serve them)"
                ));
            }
            if forks > 0 {
                skipped.push(format!("{forks} forks"));
            }
            if filtered > 0 {
                skipped.push(format!("{filtered} filtered by include/exclude"));
            }
            if no_admin > 0 {
                skipped.push(format!("{no_admin} without admin rights"));
            }
            notes.push(format!(
                "{owner}: {} repos selected{}",
                scopes.len(),
                if skipped.is_empty() {
                    String::new()
                } else {
                    format!(", skipped {}", skipped.join(", "))
                }
            ));
        }
        for scope in scopes {
            for name in names_for(&base, count) {
                out.insert(
                    (scope.clone(), name.clone()),
                    Wanted {
                        scope: scope.clone(),
                        name,
                        labels: labels.clone(),
                        extra_labels: extra_labels.clone(),
                    },
                );
            }
        }
    }
    Ok((out.into_values().collect(), notes))
}

fn same_labels(a: &[String], b: &[String]) -> bool {
    let a: BTreeSet<String> = a.iter().map(|s| s.to_lowercase()).collect();
    let b: BTreeSet<String> = b.iter().map(|s| s.to_lowercase()).collect();
    a == b
}

pub async fn reconcile(
    cfg: &Config,
    gh: &GitHub,
    runner_version: &str,
    dry_run: bool,
) -> Result<Report> {
    let (wanted, notes) = wanted(cfg, gh).await?;
    let existing = identity::load_all(&cfg.data_dir)?;
    let mut report = Report {
        notes,
        ..Default::default()
    };

    if cfg.targets.is_empty() {
        bail!("no [[target]] configured");
    }

    let have: BTreeMap<(Scope, String), &Identity> = existing
        .iter()
        .map(|i| ((i.scope.clone(), i.name.clone()), i))
        .collect();
    let want_keys: BTreeSet<(Scope, String)> = wanted
        .iter()
        .map(|w| (w.scope.clone(), w.name.clone()))
        .collect();

    let mut to_register = Vec::new();
    for w in &wanted {
        match have.get(&(w.scope.clone(), w.name.clone())) {
            None => to_register.push((w.clone(), false)),
            Some(id) if !same_labels(&id.labels, &w.labels) => to_register.push((w.clone(), true)),
            Some(_) => report.unchanged += 1,
        }
    }
    let to_remove: Vec<&Identity> = if cfg.prune {
        existing
            .iter()
            .filter(|i| !want_keys.contains(&(i.scope.clone(), i.name.clone())))
            .collect()
    } else {
        Vec::new()
    };

    if dry_run {
        for (w, relabel) in &to_register {
            let k = format!("{}#{}", w.scope, w.name);
            if *relabel {
                report.relabeled.push(k)
            } else {
                report.registered.push(k)
            }
        }
        report.removed = to_remove.iter().map(|i| i.display_key()).collect();
        return Ok(report);
    }

    // A handful at a time: each registration is ~4 API calls.
    let results: Vec<_> = futures_util::stream::iter(to_register)
        .map(|(w, relabel)| async move {
            let res = register::register(
                gh,
                RegisterRequest {
                    scope: &w.scope,
                    name: &w.name,
                    labels: &w.extra_labels[..],
                    no_default_labels: cfg.no_default_labels,
                    runner_group: if matches!(w.scope, Scope::Org { .. }) {
                        cfg.runner_group.as_deref()
                    } else {
                        None
                    },
                    runner_version,
                },
            )
            .await
            .and_then(|id| {
                id.save(&cfg.data_dir)?;
                Ok(id)
            });
            (w, relabel, res)
        })
        .buffer_unordered(6)
        .collect()
        .await;
    for (w, relabel, res) in results {
        let key = format!("{}#{}", w.scope, w.name);
        match res {
            Ok(_) if relabel => report.relabeled.push(key),
            Ok(_) => report.registered.push(key),
            Err(e) => report.failed.push((key, format!("{e:#}"))),
        }
    }

    for id in to_remove {
        match register::unregister(gh, id, &cfg.data_dir).await {
            Ok(()) => report.removed.push(id.display_key()),
            Err(e) => report.failed.push((id.display_key(), format!("{e:#}"))),
        }
    }
    Ok(report)
}

impl Report {
    pub fn log(&self, dry_run: bool) {
        for n in &self.notes {
            tracing::info!("{n}");
        }
        let verb = if dry_run { "would " } else { "" };
        for r in &self.registered {
            tracing::info!("{verb}register {r}");
        }
        for r in &self.relabeled {
            tracing::info!("{verb}re-register {r} (labels changed)");
        }
        for r in &self.removed {
            tracing::info!("{verb}remove {r}");
        }
        for (k, e) in &self.failed {
            tracing::error!("{k}: {e}");
        }
        tracing::info!(
            "sync: {} new, {} relabeled, {} removed, {} unchanged, {} failed",
            self.registered.len(),
            self.relabeled.len(),
            self.removed.len(),
            self.unchanged,
            self.failed.len()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runner_names() {
        assert_eq!(names_for("hive-x", 1), vec!["hive-x"]);
        assert_eq!(
            names_for("hive-x", 3),
            vec!["hive-x-1", "hive-x-2", "hive-x-3"]
        );
    }

    #[test]
    fn label_sets_compare_case_insensitively() {
        assert!(same_labels(
            &["A".into(), "b".into()],
            &["B".into(), "a".into()]
        ));
        assert!(!same_labels(&["a".into()], &["a".into(), "b".into()]));
    }
}
