//! Low-level fsync operations for durability.
//!
//! These functions ensure data is persisted to disk before returning.
//! Both file and directory fsyncs are required for crash safety.
//!
//! # Why Directory fsync?
//!
//! On POSIX systems, creating or renaming a file updates the directory entry.
//! Without fsync on the directory, this entry may not survive a power loss
//! even if the file contents were synced. This is a common source of data loss.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

/// Syncs a file's contents and metadata to disk.
///
/// This is equivalent to calling `fsync(2)` on the file descriptor.
/// After this returns, the file's contents are guaranteed to be on disk.
pub fn fsync_file(file: &File) -> io::Result<()> {
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
    // Open the directory as a file (read-only is sufficient for fsync)
    let dir = OpenOptions::new().read(true).open(dir_path)?;
    dir.sync_all()
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
}
