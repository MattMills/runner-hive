//! Launching `Runner.Worker` and speaking its IPC protocol.
//!
//! The official listener creates two anonymous pipes
//! (`AnonymousPipeServerStream`, Runner.Common/ProcessChannel.cs), starts
//! `Runner.Worker spawnclient <readHandle> <writeHandle>` with the client ends
//! inherited, and sends framed messages: little-endian i32 message type,
//! little-endian i32 byte length, then the body as UTF-16LE
//! (Runner.Common/StreamString.cs). Handles are passed as decimal numbers:
//! file descriptors on Unix, HANDLE values on Windows.

use std::collections::VecDeque;
use std::io::Write;
use std::path::Path;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum MessageType {
    NewJobRequest = 1,
    CancelRequest = 2,
    RunnerShutdown = 3,
    #[allow(dead_code)]
    OperatingSystemShutdown = 4,
}

const MAX_STRING_BYTES: usize = 50 * 1_000_000;

/// Frame one message the way `ProcessChannel.SendAsync` does.
pub fn encode(kind: MessageType, body: &str) -> Result<Vec<u8>> {
    let utf16: Vec<u8> = body.encode_utf16().flat_map(|u| u.to_le_bytes()).collect();
    if utf16.len() > MAX_STRING_BYTES {
        bail!(
            "message of {} bytes exceeds the worker's 50MB limit",
            utf16.len()
        );
    }
    let mut out = Vec::with_capacity(8 + utf16.len());
    out.extend_from_slice(&(kind as i32).to_le_bytes());
    out.extend_from_slice(&(utf16.len() as i32).to_le_bytes());
    out.extend_from_slice(&utf16);
    Ok(out)
}

pub struct WorkerProcess {
    pub child: Child,
    to_worker: Arc<Mutex<std::fs::File>>,
    // Held open so the worker never sees a broken pipe if it writes.
    _from_worker: std::fs::File,
    pub output_tail: Arc<Mutex<VecDeque<String>>>,
}

pub struct SpawnOptions<'a> {
    pub exe: &'a Path,
    pub env_set: &'a [(String, String)],
    pub env_remove: &'a [String],
    pub log_output: bool,
    pub log_prefix: String,
}

const TAIL_LINES: usize = 60;

impl WorkerProcess {
    pub fn spawn(opts: SpawnOptions<'_>) -> Result<Self> {
        // Absolute: on Unix a relative program path is resolved after the
        // child changes into `current_dir`.
        let exe = std::path::absolute(opts.exe)?;
        let bin_dir = exe.parent().context("worker exe has no parent dir")?;
        let mut cmd = Command::new(&exe);
        cmd.current_dir(bin_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        for k in opts.env_remove {
            cmd.env_remove(k);
        }
        for (k, v) in opts.env_set {
            cmd.env(k, v);
        }

        let (mut child, to_worker, from_worker) = platform::spawn_with_pipes(cmd)?;

        let tail = Arc::new(Mutex::new(VecDeque::with_capacity(TAIL_LINES)));
        if let Some(out) = child.stdout.take() {
            forward(
                out,
                tail.clone(),
                opts.log_output,
                opts.log_prefix.clone(),
                false,
            );
        }
        if let Some(err) = child.stderr.take() {
            forward(err, tail.clone(), opts.log_output, opts.log_prefix, true);
        }

        Ok(Self {
            child,
            to_worker: Arc::new(Mutex::new(to_worker)),
            _from_worker: from_worker,
            output_tail: tail,
        })
    }

    /// Send one message, giving up after the official 30s channel timeout.
    pub async fn send(&self, kind: MessageType, body: &str) -> Result<()> {
        let frame = encode(kind, body)?;
        let pipe = self.to_worker.clone();
        let write = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            let mut f = pipe.lock().expect("pipe mutex poisoned");
            f.write_all(&frame)?;
            f.flush()
        });
        match tokio::time::timeout(Duration::from_secs(30), write).await {
            Ok(joined) => joined?.context("cannot write to worker pipe"),
            Err(_) => bail!("timed out sending {kind:?} to the worker"),
        }
    }

    pub fn tail(&self) -> String {
        let t = self.output_tail.lock().expect("tail mutex poisoned");
        t.iter().cloned().collect::<Vec<_>>().join("\n")
    }

    /// Kill the worker and everything it started.
    pub async fn kill_tree(&mut self) {
        if let Some(pid) = self.child.id() {
            platform::kill_tree(pid);
        }
        let _ = self.child.kill().await;
    }
}

fn forward<R>(
    stream: R,
    tail: Arc<Mutex<VecDeque<String>>>,
    log: bool,
    prefix: String,
    is_err: bool,
) where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut lines = BufReader::new(stream).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if log {
                if is_err {
                    tracing::warn!("{prefix} {line}");
                } else {
                    tracing::info!("{prefix} {line}");
                }
            }
            // Lines starting with "[WORKER" are the worker's own trace echo.
            if !line.starts_with("[WORKER") {
                let mut t = tail.lock().expect("tail mutex poisoned");
                if t.len() == TAIL_LINES {
                    t.pop_front();
                }
                t.push_back(line);
            }
        }
    });
}

#[cfg(unix)]
mod platform {
    use super::*;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

    fn cvt(r: libc::c_int) -> std::io::Result<libc::c_int> {
        if r < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(r)
        }
    }

    /// A pipe whose ends are close-on-exec and above stdio, so that `Command`
    /// redirecting fds 0-2 can never clobber them and no other child inherits them.
    fn pipe() -> std::io::Result<(OwnedFd, OwnedFd)> {
        let mut fds = [0 as RawFd; 2];
        unsafe {
            cvt(libc::pipe(fds.as_mut_ptr()))?;
            let r = OwnedFd::from_raw_fd(fds[0]);
            let w = OwnedFd::from_raw_fd(fds[1]);
            Ok((high_cloexec(r)?, high_cloexec(w)?))
        }
    }

    fn high_cloexec(fd: OwnedFd) -> std::io::Result<OwnedFd> {
        unsafe {
            let dup = cvt(libc::fcntl(fd.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 3))?;
            Ok(OwnedFd::from_raw_fd(dup))
        }
    }

    pub fn spawn_with_pipes(mut cmd: Command) -> Result<(Child, std::fs::File, std::fs::File)> {
        // listener -> worker
        let (child_in, parent_out) = pipe()?;
        // worker -> listener
        let (parent_in, child_out) = pipe()?;
        let a = child_in.as_raw_fd();
        let b = child_out.as_raw_fd();
        cmd.arg("spawnclient").arg(a.to_string()).arg(b.to_string());
        // Own process group so a forced kill takes node, docker clients, etc.
        cmd.process_group(0);
        unsafe {
            cmd.pre_exec(move || {
                // Runs in the child only: let exactly these two fds survive exec.
                for fd in [a, b] {
                    let flags = libc::fcntl(fd, libc::F_GETFD);
                    if flags < 0 || libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) < 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                }
                Ok(())
            });
        }
        let child = cmd.spawn().context("cannot start Runner.Worker")?;
        drop(child_in);
        drop(child_out);
        Ok((
            child,
            std::fs::File::from(parent_out),
            std::fs::File::from(parent_in),
        ))
    }

    pub fn kill_tree(pid: u32) {
        unsafe {
            libc::kill(-(pid as libc::pid_t), libc::SIGKILL);
        }
    }
}

#[cfg(windows)]
mod platform {
    use super::*;
    use std::os::windows::io::{FromRawHandle, OwnedHandle, RawHandle};
    use windows_sys::Win32::Foundation::{HANDLE, HANDLE_FLAG_INHERIT, SetHandleInformation};
    use windows_sys::Win32::System::Pipes::CreatePipe;

    /// Inheritable handles leak into *every* process created while they are
    /// marked inheritable, so worker spawns are serialized.
    static SPAWN_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn pipe() -> std::io::Result<(OwnedHandle, OwnedHandle)> {
        let mut r: HANDLE = std::ptr::null_mut();
        let mut w: HANDLE = std::ptr::null_mut();
        if unsafe { CreatePipe(&mut r, &mut w, std::ptr::null(), 0) } == 0 {
            return Err(std::io::Error::last_os_error());
        }
        unsafe {
            Ok((
                OwnedHandle::from_raw_handle(r as RawHandle),
                OwnedHandle::from_raw_handle(w as RawHandle),
            ))
        }
    }

    fn set_inherit(h: &OwnedHandle, on: bool) -> std::io::Result<()> {
        use std::os::windows::io::AsRawHandle;
        let flag = if on { HANDLE_FLAG_INHERIT } else { 0 };
        if unsafe { SetHandleInformation(h.as_raw_handle() as HANDLE, HANDLE_FLAG_INHERIT, flag) }
            == 0
        {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    pub fn spawn_with_pipes(mut cmd: Command) -> Result<(Child, std::fs::File, std::fs::File)> {
        use std::os::windows::io::AsRawHandle;
        let (child_in, parent_out) = pipe()?;
        let (parent_in, child_out) = pipe()?;
        cmd.arg("spawnclient")
            .arg((child_in.as_raw_handle() as usize).to_string())
            .arg((child_out.as_raw_handle() as usize).to_string());
        // CREATE_NO_WINDOW, as .NET's ProcessStartInfo.CreateNoWindow.
        cmd.creation_flags(windows_sys::Win32::System::Threading::CREATE_NO_WINDOW);

        let child = {
            let _guard = SPAWN_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            set_inherit(&child_in, true)?;
            set_inherit(&child_out, true)?;
            let spawned = cmd.spawn();
            let _ = set_inherit(&child_in, false);
            let _ = set_inherit(&child_out, false);
            spawned.context("cannot start Runner.Worker")?
        };
        drop(child_in);
        drop(child_out);
        Ok((
            child,
            std::fs::File::from(parent_out),
            std::fs::File::from(parent_in),
        ))
    }

    pub fn kill_tree(pid: u32) {
        let _ = std::process::Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T", "/F"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn framing_matches_stream_string() {
        let f = encode(MessageType::CancelRequest, "hé").unwrap();
        // type 2, length 4 (two UTF-16 code units), then UTF-16LE bytes.
        assert_eq!(f, vec![2, 0, 0, 0, 4, 0, 0, 0, b'h', 0, 0xE9, 0]);
        let empty = encode(MessageType::RunnerShutdown, "").unwrap();
        assert_eq!(empty, vec![3, 0, 0, 0, 0, 0, 0, 0]);
    }
}
