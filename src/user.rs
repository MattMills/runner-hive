//! Running jobs as a different OS user than the daemon (Unix).
//!
//! The daemon holds the admin token and every registration's private key.
//! With `worker.run_as`, the daemon runs as root and each `Runner.Worker` is
//! started as the given unprivileged user, so job steps can read neither the
//! daemon's environment nor `data_dir/runners`.

use std::path::PathBuf;

use anyhow::Result;
#[cfg(not(unix))]
use anyhow::bail;

#[derive(Clone, Debug)]
pub struct RunAs {
    pub name: String,
    pub uid: u32,
    pub gid: u32,
    pub home: PathBuf,
}

#[cfg(unix)]
pub fn lookup(name: &str) -> Result<RunAs> {
    use std::ffi::{CStr, CString};
    let cname = CString::new(name)?;
    let mut pwd: libc::passwd = unsafe { std::mem::zeroed() };
    let mut buf = vec![0u8; 16 * 1024];
    let mut found: *mut libc::passwd = std::ptr::null_mut();
    let rc = unsafe {
        libc::getpwnam_r(
            cname.as_ptr(),
            &mut pwd,
            buf.as_mut_ptr() as *mut libc::c_char,
            buf.len(),
            &mut found,
        )
    };
    if rc != 0 || found.is_null() {
        anyhow::bail!("worker.run_as: no such user '{name}'");
    }
    let home = unsafe { CStr::from_ptr(pwd.pw_dir) }
        .to_string_lossy()
        .into_owned();
    Ok(RunAs {
        name: name.to_string(),
        uid: pwd.pw_uid,
        gid: pwd.pw_gid,
        home: home.into(),
    })
}

#[cfg(not(unix))]
pub fn lookup(_name: &str) -> Result<RunAs> {
    bail!("worker.run_as is only supported on Unix")
}

#[cfg(unix)]
pub fn is_root() -> bool {
    unsafe { libc::geteuid() == 0 }
}

#[cfg(not(unix))]
pub fn is_root() -> bool {
    false
}

/// Give `run_as` ownership of a directory the worker must write to.
#[cfg(unix)]
pub fn give(path: &std::path::Path, who: &RunAs) -> Result<()> {
    std::os::unix::fs::chown(path, Some(who.uid), Some(who.gid))?;
    Ok(())
}

#[cfg(not(unix))]
pub fn give(_path: &std::path::Path, _who: &RunAs) -> Result<()> {
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    #[test]
    fn looks_up_root() {
        let r = super::lookup("root").unwrap();
        assert_eq!(r.uid, 0);
        assert!(super::lookup("no-such-user-hopefully").is_err());
    }
}
