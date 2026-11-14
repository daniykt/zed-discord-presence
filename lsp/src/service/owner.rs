//! Decides which language server instance owns the Discord presence.
//!
//! Zed starts one server per worktree, and each one has its own Discord IPC
//! connection. The instance that saw the most recent user event records its
//! PID in a small file under the OS temp directory; the others stay quiet.
//! Every failure fails open: an IO error never blocks the presence.

use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::{Mutex as StdMutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::Instant;
use tracing::debug;

/// A claim is not rewritten more often than this while we still own the file.
const CLAIM_THROTTLE: Duration = Duration::from_secs(1);

#[derive(Debug)]
pub struct PresenceOwner {
    path: PathBuf,
    pid: u32,
    last_claim: StdMutex<Option<Instant>>,
}

impl PresenceOwner {
    #[must_use]
    pub fn new() -> Self {
        Self::with_path(default_path(), std::process::id())
    }

    fn with_path(path: PathBuf, pid: u32) -> Self {
        Self {
            path,
            pid,
            last_claim: StdMutex::new(None),
        }
    }

    /// Records this process as the presence owner. Skips the write if this
    /// process wrote less than `CLAIM_THROTTLE` ago and the file still names it,
    /// so returning to an instance always takes ownership back immediately.
    pub fn claim(&self) {
        let now = Instant::now();
        let mut last_claim = self
            .last_claim
            .lock()
            .unwrap_or_else(PoisonError::into_inner);

        let recently_claimed =
            last_claim.is_some_and(|claimed_at| now.duration_since(claimed_at) < CLAIM_THROTTLE);
        if recently_claimed && self.read_owner() == Some(self.pid) {
            return;
        }

        match self.write_record() {
            Ok(()) => *last_claim = Some(now),
            Err(e) => debug!("Failed to claim presence ownership: {}", e),
        }
    }

    /// Returns whether this process may send presence updates: true if the
    /// record names this process, names a process that is no longer alive, or
    /// is missing or unreadable.
    #[must_use]
    pub fn is_owner(&self) -> bool {
        match self.read_owner() {
            None => true,
            Some(pid) if pid == self.pid => true,
            Some(pid) => !process_alive(pid),
        }
    }

    fn read_owner(&self) -> Option<u32> {
        let record = fs::read_to_string(&self.path).ok()?;
        record.split_whitespace().next()?.parse().ok()
    }

    /// Writes `<pid> <unix_ms>` atomically (temp file + rename), so readers
    /// never observe a partial record.
    fn write_record(&self) -> io::Result<()> {
        if let Some(dir) = self.path.parent() {
            fs::create_dir_all(dir)?;
        }

        let unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_millis());
        let tmp_path = self.path.with_extension(format!("{}.tmp", self.pid));

        fs::write(&tmp_path, format!("{} {unix_ms}\n", self.pid))?;
        fs::rename(&tmp_path, &self.path)
    }
}

impl Default for PresenceOwner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(not(test))]
fn default_path() -> PathBuf {
    std::env::temp_dir()
        .join("discord-presence-lsp")
        .join("owner")
}

/// Tests get a fresh, never-written path, so they neither read nor change the
/// record of a real Zed session running on the same machine.
#[cfg(test)]
fn default_path() -> PathBuf {
    use std::sync::atomic::{AtomicUsize, Ordering};

    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let id = NEXT.fetch_add(1, Ordering::SeqCst);

    std::env::temp_dir()
        .join("discord-presence-lsp-tests")
        .join(format!("{}-{id}", std::process::id()))
        .join("owner")
}

#[cfg(windows)]
fn process_alive(pid: u32) -> bool {
    use std::ffi::c_void;

    const PROCESS_QUERY_LIMITED_INFORMATION: u32 = 0x1000;
    const STILL_ACTIVE: u32 = 259;
    const ERROR_ACCESS_DENIED: i32 = 5;

    #[link(name = "kernel32")]
    extern "system" {
        fn OpenProcess(desired_access: u32, inherit_handle: i32, process_id: u32) -> *mut c_void;
        fn GetExitCodeProcess(process: *mut c_void, exit_code: *mut u32) -> i32;
        fn CloseHandle(handle: *mut c_void) -> i32;
    }

    // SAFETY: plain Win32 calls; the handle is checked for null, only used
    // while open, and closed exactly once.
    unsafe {
        let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
        if handle.is_null() {
            // Access denied means the process exists but is protected.
            return io::Error::last_os_error().raw_os_error() == Some(ERROR_ACCESS_DENIED);
        }

        let mut exit_code = 0u32;
        let queried = GetExitCodeProcess(handle, &raw mut exit_code);
        CloseHandle(handle);

        // If the query fails, report "not alive" so ownership fails open.
        queried != 0 && exit_code == STILL_ACTIVE
    }
}

#[cfg(unix)]
fn process_alive(pid: u32) -> bool {
    const EPERM: i32 = 1;

    extern "C" {
        fn kill(pid: i32, signal: i32) -> i32;
    }

    let Ok(pid) = i32::try_from(pid) else {
        return false;
    };

    // SAFETY: signal 0 sends nothing; it only checks that the process exists.
    if unsafe { kill(pid, 0) } == 0 {
        return true;
    }
    // Permission denied means the process exists but belongs to someone else.
    io::Error::last_os_error().raw_os_error() == Some(EPERM)
}

#[cfg(not(any(windows, unix)))]
fn process_alive(_pid: u32) -> bool {
    false
}

/// Test-only hooks so other modules can simulate other instances.
#[cfg(test)]
impl PresenceOwner {
    /// An owner for `pid` with a fresh, never-written record path.
    pub(crate) fn with_pid(pid: u32) -> Self {
        Self::with_path(default_path(), pid)
    }

    /// Overwrites the record as if the instance with `pid` had claimed it.
    pub(crate) fn set_record_owner(&self, pid: u32) {
        fs::create_dir_all(self.path.parent().unwrap()).unwrap();
        fs::write(&self.path, format!("{pid} 0\n")).unwrap();
    }

    /// Deletes the record and its test directory.
    pub(crate) fn remove_record(&self) {
        let _ = fs::remove_dir_all(self.path.parent().unwrap());
    }
}

/// PID of a process that has already exited.
#[cfg(test)]
pub(crate) fn dead_test_pid() -> u32 {
    let mut command = if cfg!(windows) {
        let mut command = std::process::Command::new("cmd");
        command.args(["/C", "exit 0"]);
        command
    } else {
        std::process::Command::new("true")
    };
    let mut child = command.spawn().unwrap();
    let pid = child.id();
    child.wait().unwrap();
    pid
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A PID that is assumed not to belong to the test process.
    const OTHER_PID: u32 = u32::MAX - 7;

    fn test_owner(pid: u32) -> PresenceOwner {
        PresenceOwner::with_path(default_path(), pid)
    }

    fn write_raw(owner: &PresenceOwner, contents: &str) {
        fs::create_dir_all(owner.path.parent().unwrap()).unwrap();
        fs::write(&owner.path, contents).unwrap();
    }

    fn read_raw(owner: &PresenceOwner) -> String {
        fs::read_to_string(&owner.path).unwrap()
    }

    fn cleanup(owner: &PresenceOwner) {
        let _ = fs::remove_dir_all(owner.path.parent().unwrap());
    }

    #[test]
    fn test_current_process_is_alive() {
        assert!(process_alive(std::process::id()));
    }

    #[test]
    fn test_is_owner_when_record_is_missing() {
        let owner = test_owner(OTHER_PID);

        assert!(owner.is_owner());
    }

    #[test]
    fn test_is_not_owner_when_another_live_process_owns_the_record() {
        let owner = test_owner(OTHER_PID);
        // The test process itself is the live "other" owner here.
        write_raw(&owner, &format!("{} 0\n", std::process::id()));

        assert!(!owner.is_owner());
        cleanup(&owner);
    }

    #[test]
    fn test_is_owner_when_recorded_process_is_dead() {
        let owner = test_owner(OTHER_PID);
        write_raw(&owner, &format!("{} 0\n", dead_test_pid()));

        assert!(owner.is_owner());
        cleanup(&owner);
    }

    #[test]
    fn test_is_owner_when_record_is_garbage() {
        let owner = test_owner(OTHER_PID);
        write_raw(&owner, "not a pid");

        assert!(owner.is_owner());
        cleanup(&owner);
    }

    #[tokio::test(start_paused = true)]
    async fn test_claim_is_throttled_while_still_owner() {
        let owner = test_owner(OTHER_PID);
        owner.claim();
        assert!(owner.is_owner());

        // Same PID with a marker timestamp: a rewrite would replace the marker.
        let marker = format!("{OTHER_PID} 0\n");
        write_raw(&owner, &marker);

        tokio::time::advance(Duration::from_millis(500)).await;
        owner.claim();
        assert_eq!(
            read_raw(&owner),
            marker,
            "Claim within 1 s should not write"
        );

        tokio::time::advance(CLAIM_THROTTLE).await;
        owner.claim();
        assert_ne!(read_raw(&owner), marker, "Claim after 1 s should write");
        cleanup(&owner);
    }

    #[tokio::test(start_paused = true)]
    async fn test_claim_retakes_ownership_within_throttle_window() {
        let owner = test_owner(OTHER_PID);
        owner.claim();

        // Another live instance takes over right after our claim.
        write_raw(&owner, &format!("{} 0\n", std::process::id()));
        assert!(!owner.is_owner());

        tokio::time::advance(Duration::from_millis(100)).await;
        owner.claim();

        assert!(
            owner.is_owner(),
            "Returning to an instance must reclaim at once"
        );
        cleanup(&owner);
    }
}
