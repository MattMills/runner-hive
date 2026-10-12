//! Job slots. `Runner.Worker` derives its root directory from the location
//! of its own binary and reads `.runner` / `.credentials` from there, so each
//! concurrently running job needs its own root. A slot is a hardlinked copy
//! of the package (near-zero disk cost) plus per-job config and a `_work` dir.
//! Slots are per concurrency level, not per repo: 4 slots serve 150 repos.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde_json::json;

use super::package::WorkerPackage;
use crate::identity::Identity;

const MARKER: &str = ".hive-slot";

/// Entries in a package root that are per-runner state, never linked.
fn is_state(name: &str) -> bool {
    name == "_work"
        || name == "_diag"
        || name.starts_with(".runner")
        || name.starts_with(".credentials")
        || name.starts_with(".hive")
        || name == ".env"
        || name == ".path"
        || name == ".service"
}

pub fn slot_root(data_dir: &Path, index: usize) -> PathBuf {
    data_dir.join("slots").join(index.to_string())
}

/// Make sure slot `index` mirrors `pkg`, relinking if the package changed.
pub fn prepare(data_dir: &Path, index: usize, pkg: &WorkerPackage) -> Result<PathBuf> {
    prepare_at(slot_root(data_dir, index), pkg)
}

pub fn prepare_at(root: PathBuf, pkg: &WorkerPackage) -> Result<PathBuf> {
    let stamp = format!("{}\n{}", pkg.version, pkg.root.display());
    let marker = root.join(MARKER);
    if std::fs::read_to_string(&marker).ok().as_deref() == Some(stamp.as_str()) {
        return Ok(root);
    }
    std::fs::create_dir_all(&root)?;
    // Remove the previous package's files but keep work and diagnostics.
    for entry in std::fs::read_dir(&root)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name == "_work" || name == "_diag" {
            continue;
        }
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            std::fs::remove_dir_all(&path)?;
        } else {
            std::fs::remove_file(&path)?;
        }
    }
    let mut copied = 0usize;
    for entry in std::fs::read_dir(&pkg.root)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if is_state(&name) {
            continue;
        }
        copied += link_tree(&entry.path(), &root.join(&name))?;
    }
    std::fs::write(&marker, &stamp)?;
    tracing::info!(
        "{}: linked worker {} ({copied} files copied instead of hardlinked)",
        root.display(),
        pkg.version
    );
    Ok(root)
}

/// Recreate `src` at `dst` with hardlinks (falling back to copies across
/// filesystems). Returns how many files had to be copied.
fn link_tree(src: &Path, dst: &Path) -> Result<usize> {
    let meta = std::fs::symlink_metadata(src)?;
    let ft = meta.file_type();
    if ft.is_symlink() {
        let target = std::fs::read_link(src)?;
        #[cfg(unix)]
        std::os::unix::fs::symlink(&target, dst)?;
        #[cfg(windows)]
        {
            let _ = target;
            std::fs::copy(src, dst)?;
        }
        return Ok(0);
    }
    if ft.is_dir() {
        std::fs::create_dir_all(dst)?;
        let mut copied = 0;
        for entry in std::fs::read_dir(src)? {
            let entry = entry?;
            copied += link_tree(&entry.path(), &dst.join(entry.file_name()))?;
        }
        return Ok(copied);
    }
    match std::fs::hard_link(src, dst) {
        Ok(()) => Ok(0),
        Err(_) => {
            std::fs::copy(src, dst)
                .with_context(|| format!("cannot link or copy {}", src.display()))?;
            Ok(1)
        }
    }
}

/// Write the per-job `.runner` and `.credentials` that the worker reads. The
/// credentials file carries only the client id (used for display); the
/// private key never enters a slot, where job steps could read it.
pub fn write_runner_config(
    root: &Path,
    id: &Identity,
    run_as: Option<&crate::user::RunAs>,
) -> Result<()> {
    let mut runner = json!({
        "agentId": id.agent_id,
        "agentName": id.name,
        "poolId": id.pool_id,
        "poolName": id.pool_name,
        "serverUrl": id.server_url,
        "gitHubUrl": id.github_url,
        "workFolder": "_work",
        "disableUpdate": true,
    });
    if id.use_v2_flow {
        runner["useV2Flow"] = json!(true);
    }
    if let Some(v2) = &id.server_url_v2 {
        runner["serverUrlV2"] = json!(v2);
    }
    std::fs::write(root.join(".runner"), serde_json::to_vec_pretty(&runner)?)?;
    let creds = json!({
        "scheme": "OAuth",
        "data": {
            "clientId": id.client_id,
            "authorizationUrl": id.authorization_url,
            "requireFipsCryptography": if id.require_fips { "True" } else { "False" },
        }
    });
    std::fs::write(
        root.join(".credentials"),
        serde_json::to_vec_pretty(&creds)?,
    )?;
    // The worker writes only to _work and _diag. With run_as, those two belong
    // to the job user while the slot root, bin/ and the config stay owned by
    // the daemon, so a job cannot swap the worker binary for the next job.
    for dir in ["_work", "_diag"] {
        let p = root.join(dir);
        std::fs::create_dir_all(&p)?;
        if let Some(who) = run_as {
            crate::user::give(&p, who)?;
        }
    }
    Ok(())
}

/// Whether slot `index` already holds a checkout of `repo` (its `_work/<repo>`),
/// so jobs can return to warm caches such as incremental `target/` dirs.
pub fn has_repo(data_dir: &Path, index: usize, repo: &str) -> bool {
    !repo.is_empty() && slot_root(data_dir, index).join("_work").join(repo).is_dir()
}

/// Remove checkouts left in `_work`, keeping the tool and action caches.
pub fn clean_work(root: &Path) -> Result<()> {
    let work = root.join("_work");
    if !work.exists() {
        return Ok(());
    }
    for entry in std::fs::read_dir(&work)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name == "_tool" || name == "_actions" {
            continue;
        }
        let path = entry.path();
        let r = if entry.file_type()?.is_dir() {
            std::fs::remove_dir_all(&path)
        } else {
            std::fs::remove_file(&path)
        };
        if let Err(e) = r {
            tracing::warn!("cannot clean {}: {e}", path.display());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepare_links_and_skips_state() {
        let tmp = std::env::temp_dir().join(format!("hive-slot-test-{}", uuid::Uuid::new_v4()));
        let pkg_root = tmp.join("pkg");
        std::fs::create_dir_all(pkg_root.join("bin")).unwrap();
        std::fs::write(pkg_root.join("bin").join("Runner.Worker"), b"exe").unwrap();
        std::fs::write(pkg_root.join(".runner"), b"{}").unwrap();
        std::fs::create_dir_all(pkg_root.join("_work").join("x")).unwrap();
        let pkg = WorkerPackage {
            root: pkg_root.clone(),
            version: "1.2.3".into(),
        };

        let data = tmp.join("data");
        let root = prepare(&data, 0, &pkg).unwrap();
        assert_eq!(
            std::fs::read(root.join("bin").join("Runner.Worker")).unwrap(),
            b"exe"
        );
        assert!(!root.join(".runner").exists());
        assert!(!root.join("_work").join("x").exists());

        // Idempotent, and keeps _work across a package change.
        std::fs::create_dir_all(root.join("_work").join("keep")).unwrap();
        prepare(&data, 0, &pkg).unwrap();
        let pkg2 = WorkerPackage {
            root: pkg_root,
            version: "1.2.4".into(),
        };
        prepare(&data, 0, &pkg2).unwrap();
        assert!(root.join("_work").join("keep").exists());
        assert!(root.join("bin").join("Runner.Worker").exists());
        std::fs::remove_dir_all(&tmp).unwrap();
    }
}
