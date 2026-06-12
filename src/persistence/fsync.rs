//! Low-level durable filesystem operations.
//!
//! These functions ensure data is persisted to disk before returning.
//! Both file and directory fsyncs are required for crash safety.
//!
//! # Why Directory fsync?
//!
//! On POSIX systems, creating or renaming a file updates the directory entry.
//! Without fsync on the directory, this entry may not survive a power loss
//! even if the file contents were synced. This is a common source of data loss.
//!
//! # Fault injection
//!
//! Every function here is an instrumented seam: under `cfg(test)`, the
//! [`fault`] module can fail the k-th operation on the current thread (or
//! every operation from the k-th on, modelling a dead disk). Crash-safety
//! tests use this to sweep "what if the disk failed exactly here?" across
//! whole multi-step procedures like compaction, instead of hand-simulating
//! a few chosen crash points. In non-test builds the wrappers compile down
//! to the bare `std::fs` calls.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

/// Syncs a file's contents and metadata to disk.
///
/// This is equivalent to calling `fsync(2)` on the file descriptor.
/// After this returns, the file's contents are guaranteed to be on disk.
pub fn fsync_file(file: &File) -> io::Result<()> {
    #[cfg(test)]
    fault::check(fault::OpKind::FsyncFile, Path::new(""))?;
    file.sync_all()
}

/// Syncs a directory to disk, ensuring directory entries are durable.
///
/// Required for crash safety. Without this:
/// - A newly created file might be lost (directory entry not persisted)
/// - A renamed file might revert to its old name
/// - A deleted file might reappear
///
/// # Errors
///
/// Returns an error if the path doesn't exist or if the fsync system call fails.
///
/// # Note
///
/// This function uses `sync_all()` on a file handle, which technically works
/// on regular files too. However, it's semantically intended for directories
/// and should only be called with directory paths.
pub fn fsync_dir(dir_path: &Path) -> io::Result<()> {
    #[cfg(test)]
    fault::check(fault::OpKind::FsyncDir, dir_path)?;
    // Open the directory as a file (read-only is sufficient for fsync)
    let dir = OpenOptions::new().read(true).open(dir_path)?;
    dir.sync_all()
}

/// Renames `from` to `to`.
///
/// Identical to [`std::fs::rename`] in non-test builds; exists so that
/// crash-safety tests can inject a failure at exactly this operation.
/// Callers in the persistence layer must use this instead of `std::fs`
/// directly, or the fault-injection sweep silently loses coverage of them.
pub fn rename(from: &Path, to: &Path) -> io::Result<()> {
    #[cfg(test)]
    fault::check(fault::OpKind::Rename, to)?;
    std::fs::rename(from, to)
}

/// Removes the file at `path`.
///
/// Identical to [`std::fs::remove_file`] in non-test builds; exists so that
/// crash-safety tests can inject a failure at exactly this operation.
/// Callers in the persistence layer must use this instead of `std::fs`
/// directly, or the fault-injection sweep silently loses coverage of them.
pub fn remove_file(path: &Path) -> io::Result<()> {
    #[cfg(test)]
    fault::check(fault::OpKind::RemoveFile, path)?;
    std::fs::remove_file(path)
}

/// Test-only fault injection for the instrumented operations above.
///
/// A thread-local *plan* counts every instrumented operation on the current
/// thread and decides which ones fail. An injected failure happens **before**
/// the underlying syscall, so a "failed" rename/unlink leaves the filesystem
/// untouched — modelling an I/O layer that rejected the request.
///
/// # Usage discipline
///
/// The plan sees *every* instrumented op on the thread, not just the ones a
/// test cares about: critical-event appends fsync, `recover` performs many
/// ops, and the spool module (`spool::delivery`, `spool::drain`) shares
/// `fsync_file`/`fsync_dir`. Arm immediately before the operation under test
/// and let the [`ArmedGuard`] disarm immediately after — never hold a plan
/// across appends, drops, or `recover`. Indices shift whenever the code under
/// test changes, so locate them with [`FaultMode::Record`] on an identical
/// setup rather than hardcoding.
#[cfg(test)]
pub(crate) mod fault {
    use std::cell::RefCell;
    use std::io;
    use std::path::{Path, PathBuf};

    /// The kind of an instrumented operation.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum OpKind {
        FsyncFile,
        FsyncDir,
        Rename,
        RemoveFile,
    }

    /// What an armed plan does to instrumented operations.
    #[derive(Debug, Clone, Copy)]
    pub enum FaultMode {
        /// Fail exactly the op with this 0-based index; all others succeed.
        FailOnly(usize),
        /// Fail every op from this 0-based index on (a dead disk).
        FailFrom(usize),
        /// Fail nothing; useful purely for the recording.
        Record,
    }

    /// One instrumented operation, as recorded by an armed plan.
    #[derive(Debug, Clone)]
    pub struct RecordedOp {
        pub kind: OpKind,
        /// The operation's primary path: the destination for renames, the
        /// file or directory path otherwise. Empty for [`OpKind::FsyncFile`],
        /// where only a file handle is available.
        pub path: PathBuf,
    }

    struct Plan {
        mode: FaultMode,
        counter: usize,
        recording: Vec<RecordedOp>,
    }

    thread_local! {
        static PLAN: RefCell<Option<Plan>> = const { RefCell::new(None) };
    }

    /// Arms the thread-local plan. The returned guard disarms it on drop,
    /// so a panicking test cannot leak an armed plan into the next test on
    /// the same thread.
    #[must_use]
    pub fn arm(mode: FaultMode) -> ArmedGuard {
        PLAN.with(|p| {
            let prev = p.borrow_mut().replace(Plan {
                mode,
                counter: 0,
                recording: Vec::new(),
            });
            assert!(prev.is_none(), "fault plan armed while already armed");
        });
        ArmedGuard { _private: () }
    }

    /// Disarms the plan on drop; see [`arm`].
    pub struct ArmedGuard {
        _private: (),
    }

    impl ArmedGuard {
        /// Disarms the plan and returns every operation it observed.
        pub fn take_recording(self) -> Vec<RecordedOp> {
            PLAN.with(|p| p.borrow_mut().take())
                .map(|plan| plan.recording)
                .unwrap_or_default()
        }
    }

    impl Drop for ArmedGuard {
        fn drop(&mut self) {
            PLAN.with(|p| p.borrow_mut().take());
        }
    }

    /// Called by each instrumented operation before executing. Returns an
    /// error (and suppresses the operation) if the plan says this op fails.
    ///
    /// Injected errors are `ErrorKind::Other`, never `NotFound`: several
    /// callers deliberately tolerate `NotFound` (idempotent deletes), and an
    /// injected failure must not be swallowed by those paths.
    pub(super) fn check(kind: OpKind, path: &Path) -> io::Result<()> {
        PLAN.with(|p| {
            let mut p = p.borrow_mut();
            let Some(plan) = p.as_mut() else {
                return Ok(());
            };
            let index = plan.counter;
            plan.counter += 1;
            plan.recording.push(RecordedOp {
                kind,
                path: path.to_path_buf(),
            });
            let fail = match plan.mode {
                FaultMode::FailOnly(k) => index == k,
                FaultMode::FailFrom(k) => index >= k,
                FaultMode::Record => false,
            };
            if fail {
                Err(io::Error::other(format!(
                    "injected fault: op {index} ({kind:?}) on {}",
                    path.display()
                )))
            } else {
                Ok(())
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn fsync_file_works() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.txt");

        let mut file = File::create(&path).unwrap();
        file.write_all(b"test data").unwrap();

        // Should not panic or error
        fsync_file(&file).unwrap();
    }

    #[test]
    fn fsync_dir_works() {
        let dir = tempdir().unwrap();

        // Create a file in the directory
        let path = dir.path().join("test.txt");
        File::create(&path).unwrap();

        // Should not panic or error
        fsync_dir(dir.path()).unwrap();
    }

    #[test]
    fn fsync_dir_on_file_does_not_error() {
        // sync_all() works on regular files too, even though fsync_dir
        // is semantically intended for directories. This test documents
        // that behavior (callers should still only pass directories).
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.txt");
        File::create(&path).unwrap();

        // Should succeed (sync_all works on any file handle)
        fsync_dir(&path).unwrap();
    }

    #[test]
    fn fsync_dir_fails_on_nonexistent() {
        let result = fsync_dir(Path::new("/nonexistent/path/that/does/not/exist"));
        assert!(result.is_err());
    }

    #[test]
    fn fault_fail_only_fails_exactly_that_op_and_suppresses_it() {
        let dir = tempdir().unwrap();
        let a = dir.path().join("a");
        let b = dir.path().join("b");
        File::create(&a).unwrap();

        let guard = fault::arm(fault::FaultMode::FailOnly(1));
        // Op 0 succeeds.
        rename(&a, &b).unwrap();
        // Op 1 fails, and the underlying operation must not be performed.
        let err = remove_file(&b).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Other, "never NotFound: {err}");
        assert!(b.exists(), "an injected failure must suppress the op");
        // Op 2 succeeds again.
        remove_file(&b).unwrap();

        let ops = guard.take_recording();
        assert_eq!(ops.len(), 3, "every instrumented op is recorded");
        assert_eq!(ops[0].kind, fault::OpKind::Rename);
        assert_eq!(ops[0].path, b, "renames record their destination");
        assert_eq!(ops[1].kind, fault::OpKind::RemoveFile);

        // The guard disarmed the plan: ops run bare again.
        File::create(&a).unwrap();
        remove_file(&a).unwrap();
    }
}
