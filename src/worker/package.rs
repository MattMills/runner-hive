//! The official actions/runner release, used only for its `Runner.Worker`.
//! Downloaded once per version, SHA-256 verified against the release notes.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use futures_util::StreamExt;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

use crate::config::Config;
use crate::github::GitHub;

#[derive(Debug, Clone)]
pub struct WorkerPackage {
    pub root: PathBuf,
    pub version: String,
}

pub fn worker_exe_name() -> &'static str {
    if cfg!(windows) {
        "Runner.Worker.exe"
    } else {
        "Runner.Worker"
    }
}

fn listener_exe_name() -> &'static str {
    if cfg!(windows) {
        "Runner.Listener.exe"
    } else {
        "Runner.Listener"
    }
}

const COMPLETE_MARKER: &str = ".hive-complete";

pub async fn ensure(cfg: &Config, gh: &GitHub) -> Result<WorkerPackage> {
    if let Some(path) = &cfg.worker.path {
        let exe = path.join("bin").join(worker_exe_name());
        if !exe.exists() {
            bail!(
                "{} not found; worker.path must be an extracted actions/runner directory",
                exe.display()
            );
        }
        let version = detect_version(path).await?;
        return Ok(WorkerPackage {
            root: path.clone(),
            version,
        });
    }

    let requested = cfg
        .worker
        .version
        .trim()
        .trim_start_matches('v')
        .to_string();
    let packages = cfg.data_dir.join("packages");

    // Pinned and already installed: no network needed.
    if requested != "latest" && packages.join(&requested).join(COMPLETE_MARKER).exists() {
        return Ok(WorkerPackage {
            root: packages.join(&requested),
            version: requested,
        });
    }

    let release = match gh
        .runner_release(if requested == "latest" {
            None
        } else {
            Some(&requested)
        })
        .await
    {
        Ok(r) => r,
        Err(e) if requested == "latest" => {
            // Offline: fall back to the newest package we already have.
            if let Some(pkg) = newest_installed(&packages)? {
                tracing::warn!(
                    "cannot check for the latest runner ({e:#}); using installed {}",
                    pkg.version
                );
                return Ok(pkg);
            }
            return Err(e);
        }
        Err(e) => return Err(e),
    };
    let version = release.tag_name.trim_start_matches('v').to_string();
    let dest = packages.join(&version);
    if dest.join(COMPLETE_MARKER).exists() {
        return Ok(WorkerPackage {
            root: dest,
            version,
        });
    }

    let platform = crate::util::runner_package_platform()?;
    let ext = if cfg!(windows) { "zip" } else { "tar.gz" };
    let asset_name = format!("actions-runner-{platform}-{version}.{ext}");
    let asset = release
        .assets
        .iter()
        .find(|a| a.name == asset_name)
        .with_context(|| format!("release v{version} has no asset {asset_name}"))?;
    let expected_sha = release
        .body
        .as_deref()
        .and_then(|b| sha_from_notes(b, platform));

    std::fs::create_dir_all(&packages)?;
    let archive = packages.join(format!(".{asset_name}.partial"));
    tracing::info!("downloading {asset_name}");
    let actual_sha = download(gh.http(), &asset.browser_download_url, &archive).await?;
    match expected_sha {
        Some(exp) if !exp.eq_ignore_ascii_case(&actual_sha) => {
            let _ = std::fs::remove_file(&archive);
            bail!("checksum mismatch for {asset_name}: expected {exp}, got {actual_sha}");
        }
        Some(_) => tracing::info!("verified sha256 {actual_sha}"),
        None => tracing::warn!("release notes carry no checksum for {platform}; not verified"),
    }

    let staging = packages.join(format!(".extract-{version}"));
    if staging.exists() {
        std::fs::remove_dir_all(&staging)?;
    }
    std::fs::create_dir_all(&staging)?;
    {
        let archive = archive.clone();
        let staging = staging.clone();
        tokio::task::spawn_blocking(move || extract(&archive, &staging, cfg!(windows)))
            .await?
            .with_context(|| format!("cannot extract {asset_name}"))?;
    }
    if !staging.join("bin").join(worker_exe_name()).exists() {
        bail!("{asset_name} does not contain bin/{}", worker_exe_name());
    }
    std::fs::write(staging.join(COMPLETE_MARKER), &version)?;
    if dest.exists() {
        std::fs::remove_dir_all(&dest)?;
    }
    std::fs::rename(&staging, &dest)?;
    let _ = std::fs::remove_file(&archive);
    tracing::info!("worker package {version} ready at {}", dest.display());
    Ok(WorkerPackage {
        root: dest,
        version,
    })
}

fn newest_installed(packages: &Path) -> Result<Option<WorkerPackage>> {
    if !packages.exists() {
        return Ok(None);
    }
    let mut best: Option<(Vec<u64>, PathBuf, String)> = None;
    for entry in std::fs::read_dir(packages)? {
        let path = entry?.path();
        if !path.join(COMPLETE_MARKER).exists() {
            continue;
        }
        let Some(v) = path.file_name().and_then(|n| n.to_str()).map(String::from) else {
            continue;
        };
        let parsed: Vec<u64> = v.split('.').filter_map(|p| p.parse().ok()).collect();
        if best.as_ref().map(|b| parsed > b.0).unwrap_or(true) {
            best = Some((parsed, path, v));
        }
    }
    Ok(best.map(|(_, root, version)| WorkerPackage { root, version }))
}

/// `- actions-runner-linux-x64-2.338.0.tar.gz <!-- BEGIN SHA linux-x64 -->HEX<!-- END SHA linux-x64 -->`
fn sha_from_notes(notes: &str, platform: &str) -> Option<String> {
    let begin = format!("<!-- BEGIN SHA {platform} -->");
    let end = format!("<!-- END SHA {platform} -->");
    let start = notes.find(&begin)? + begin.len();
    let stop = notes[start..].find(&end)? + start;
    let sha = notes[start..stop].trim();
    (sha.len() == 64 && sha.chars().all(|c| c.is_ascii_hexdigit())).then(|| sha.to_lowercase())
}

async fn download(http: &reqwest::Client, url: &str, dest: &Path) -> Result<String> {
    let resp = http
        .get(url)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;
    let resp = crate::http::check(resp).await?;
    let mut file = tokio::fs::File::create(dest).await?;
    let mut hasher = Sha256::new();
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        hasher.update(&chunk);
        file.write_all(&chunk).await?;
    }
    file.flush().await?;
    Ok(hex(&hasher.finalize()))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn extract(archive: &Path, dest: &Path, is_zip: bool) -> Result<()> {
    let file = std::fs::File::open(archive)?;
    if is_zip {
        let mut zip = zip::ZipArchive::new(file)?;
        zip.extract(dest)?;
    } else {
        let gz = flate2::read::GzDecoder::new(file);
        let mut tar = tar::Archive::new(gz);
        tar.set_preserve_permissions(true);
        tar.unpack(dest)?;
    }
    Ok(())
}

/// Version of a user-supplied runner directory (`Runner.Listener --version`).
async fn detect_version(root: &Path) -> Result<String> {
    let listener = root.join("bin").join(listener_exe_name());
    let out = tokio::process::Command::new(&listener)
        .arg("--version")
        .output()
        .await
        .with_context(|| format!("cannot run {} --version", listener.display()))?;
    let v = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if v.split('.').count() < 3 {
        bail!(
            "unexpected version output '{v}' from {}",
            listener.display()
        );
    }
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha_parsing() {
        let notes = "- actions-runner-linux-x64-2.338.0.tar.gz <!-- BEGIN SHA linux-x64 -->AF4B794C1BC41D73D40535E3FE092A39F9679CD8D965954C2ACA25A05CA41D32<!-- END SHA linux-x64 -->";
        assert_eq!(
            sha_from_notes(notes, "linux-x64").as_deref(),
            Some("af4b794c1bc41d73d40535e3fe092a39f9679cd8d965954c2aca25a05ca41d32")
        );
        assert_eq!(sha_from_notes(notes, "win-x64"), None);
    }
}
