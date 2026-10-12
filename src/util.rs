//! Small platform and HTTP helpers shared across the crate.

use std::time::Duration;

use anyhow::{Context, Result};
use rand::Rng;
use url::Url;

/// `VarUtil.OS` in the official runner: the value GitHub expects in the
/// `os` query parameter and as the default OS label.
pub fn os_name() -> &'static str {
    if cfg!(target_os = "windows") {
        "Windows"
    } else if cfg!(target_os = "macos") {
        "macOS"
    } else {
        "Linux"
    }
}

/// `VarUtil.OSArchitecture` in the official runner.
pub fn arch_name() -> &'static str {
    if cfg!(target_arch = "x86_64") {
        "X64"
    } else if cfg!(target_arch = "aarch64") {
        "ARM64"
    } else if cfg!(target_arch = "arm") {
        "ARM"
    } else {
        "X86"
    }
}

/// Platform segment of the official runner release asset name,
/// e.g. `actions-runner-linux-x64-2.338.0.tar.gz`.
pub fn runner_package_platform() -> Result<&'static str> {
    let os = if cfg!(target_os = "windows") {
        "win"
    } else if cfg!(target_os = "macos") {
        "osx"
    } else if cfg!(target_os = "linux") {
        "linux"
    } else {
        anyhow::bail!("the official runner is not published for this operating system")
    };
    let arch = if cfg!(target_arch = "x86_64") {
        "x64"
    } else if cfg!(target_arch = "aarch64") {
        "arm64"
    } else if cfg!(target_arch = "arm") {
        "arm"
    } else {
        anyhow::bail!("the official runner is not published for this CPU architecture")
    };
    Ok(match (os, arch) {
        ("win", "x64") => "win-x64",
        ("win", "arm64") => "win-arm64",
        ("osx", "x64") => "osx-x64",
        ("osx", "arm64") => "osx-arm64",
        ("linux", "x64") => "linux-x64",
        ("linux", "arm64") => "linux-arm64",
        ("linux", "arm") => "linux-arm",
        _ => anyhow::bail!("no official runner package for {os}-{arch}"),
    })
}

pub fn os_description() -> String {
    format!(
        "{} {} (runner-hive {})",
        std::env::consts::OS,
        std::env::consts::ARCH,
        env!("CARGO_PKG_VERSION")
    )
}

pub fn hostname() -> String {
    hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_else(|| "host".to_string())
}

/// Resolve `rel` against `base` exactly like .NET's `new Uri(base, rel)`,
/// which is what the official runner does for broker and run-service URLs.
pub fn join_url(base: &str, rel: &str) -> Result<Url> {
    let base = Url::parse(base).with_context(|| format!("invalid url '{base}'"))?;
    base.join(rel)
        .with_context(|| format!("cannot resolve '{rel}' against '{base}'"))
}

/// Ensure a URL used as a base address ends with `/` so relative joins append.
pub fn with_trailing_slash(url: &str) -> String {
    if url.ends_with('/') {
        url.to_string()
    } else {
        format!("{url}/")
    }
}

/// Random duration in `[min, max]`, used for retry backoff so that hundreds of
/// listeners do not reconnect in lock-step after a service blip.
pub fn jitter(min: Duration, max: Duration) -> Duration {
    if max <= min {
        return min;
    }
    let span = (max - min).as_millis() as u64;
    min + Duration::from_millis(rand::thread_rng().gen_range(0..=span))
}

/// Parse a .NET `TimeSpan` string such as `00:05:00`, `1.02:03:04` or
/// `00:00:30.5000000`.
pub fn parse_timespan(s: &str) -> Option<Duration> {
    let (days, rest) = match s.split_once('.') {
        Some((d, r)) if !d.contains(':') => (d.parse::<u64>().ok()?, r),
        _ => (0, s),
    };
    let mut parts = rest.split(':');
    let h: u64 = parts.next()?.parse().ok()?;
    let m: u64 = parts.next()?.parse().ok()?;
    let sec: f64 = parts.next()?.parse().ok()?;
    Some(Duration::from_secs(days * 86400 + h * 3600 + m * 60) + Duration::from_secs_f64(sec))
}

pub fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Write a file atomically and, on Unix, readable by the owner only.
pub fn write_private(path: &std::path::Path, contents: &[u8]) -> Result<()> {
    let dir = path.parent().context("path has no parent directory")?;
    std::fs::create_dir_all(dir)?;
    let tmp = path.with_extension("tmp");
    {
        use std::io::Write;
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let mut f = opts
            .open(&tmp)
            .with_context(|| format!("cannot write {}", tmp.display()))?;
        f.write_all(contents)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path).with_context(|| format!("cannot replace {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timespan_parsing() {
        assert_eq!(parse_timespan("00:05:00"), Some(Duration::from_secs(300)));
        assert_eq!(
            parse_timespan("1.00:00:01"),
            Some(Duration::from_secs(86401))
        );
        assert_eq!(
            parse_timespan("00:00:30.5000000"),
            Some(Duration::from_millis(30500))
        );
        assert_eq!(parse_timespan("junk"), None);
    }

    #[test]
    fn url_join_matches_dotnet_semantics() {
        assert_eq!(
            join_url("https://broker.actions.githubusercontent.com/", "message")
                .unwrap()
                .as_str(),
            "https://broker.actions.githubusercontent.com/message"
        );
        assert_eq!(
            join_url("https://run.example.com/91/", "acquirejob")
                .unwrap()
                .as_str(),
            "https://run.example.com/91/acquirejob"
        );
        // Without a trailing slash the last segment is replaced, as in .NET.
        assert_eq!(
            join_url("https://run.example.com/91", "acquirejob")
                .unwrap()
                .as_str(),
            "https://run.example.com/acquirejob"
        );
    }
}
