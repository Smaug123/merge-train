//! Worktree cleanup and recovery functions.
//!
//! These functions handle cleanup of worktrees in various failure scenarios:
//!
//! - **On abort**: When a cascade is aborted (e.g., merge conflict), the worktree
//!   may be left in an unmerged state. We must clean up before transitioning to
//!   aborted state.
//!
//! - **On restart**: If the process dies mid-git operation, worktrees may be left
//!   in a dirty state. Since worktrees use detached HEAD mode, cleanup must not
//!   rely on `origin/<branch>` being available.
//!
//! - **Orphan detection**: On startup, detect worktrees not associated with active
//!   trains and remove them if stale.

use std::path::Path;

use super::{GitConfig, GitResult, run_git_stdout, run_git_sync, worktree_path_str};
use crate::types::PrNumber;

/// Result of worktree cleanup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CleanupResult {
    /// Worktree was cleaned successfully.
    Cleaned,
    /// Worktree was too corrupted and was deleted.
    Deleted,
    /// Worktree didn't need cleanup (already clean).
    AlreadyClean,
    /// Worktree doesn't exist.
    NotFound,
}

/// Clean up a worktree on abort.
///
/// When an abort occurs (especially due to merge conflicts), the worktree may be
/// left in an unmerged state with conflict markers in the index. This would cause
/// subsequent `git checkout --detach` calls to fail.
///
/// This function:
/// 1. Aborts any in-progress merge
/// 2. Hard resets to HEAD
/// 3. Cleans untracked files
///
/// # Arguments
///
/// * `worktree` - Path to the worktree
///
/// # Returns
///
/// The result of the cleanup operation.
pub fn cleanup_worktree_on_abort(worktree_path: &Path) -> GitResult<CleanupResult> {
    if !worktree_path.exists() {
        return Ok(CleanupResult::NotFound);
    }

    // Step 1: Abort any in-progress merge
    // This may fail if there's no merge in progress - that's fine
    let _ = run_git_sync(worktree_path, &["merge", "--abort"]);

    // Step 2: Hard reset to HEAD
    // This discards staged/unstaged changes and resolves any index conflicts
    run_git_sync(worktree_path, &["reset", "--hard", "HEAD"])?;

    // Step 3: Clean untracked files and directories
    // -f = force, -d = directories
    run_git_sync(worktree_path, &["clean", "-fd"])?;

    Ok(CleanupResult::Cleaned)
}

/// Clean up a worktree on restart.
///
/// If the process dies mid-git operation, worktrees may be left in a dirty state
/// (in-progress merge, uncommitted changes). Since worktrees use detached HEAD
/// mode (not checked-out branches), and branches may be deleted after PR merge,
/// the cleanup approach must not rely on `origin/<branch>` being available.
///
/// This function:
/// 1. Aborts any in-progress merge
/// 2. Hard resets (just `HEAD`, not to any remote branch)
/// 3. Cleans untracked files
/// 4. If cleanup fails, deletes and recreates the worktree
///
/// # Arguments
///
/// * `config` - Git configuration
/// * `root_pr` - The PR number associated with the worktree
///
/// # Returns
///
/// The result of the cleanup operation.
pub fn cleanup_worktree_on_restart(
    config: &GitConfig,
    root_pr: PrNumber,
) -> GitResult<CleanupResult> {
    let worktree_path = config.worktree_path(root_pr);

    if !worktree_path.exists() {
        return Ok(CleanupResult::NotFound);
    }

    // Try to clean the worktree
    let cleanup_result = attempt_worktree_cleanup(&worktree_path);

    match cleanup_result {
        Ok(_) => {
            // Worktree is clean, but we're in detached HEAD at an unknown commit.
            // The cascade engine will handle checking out the right commit.
            Ok(CleanupResult::Cleaned)
        }
        Err(e) => {
            // Worktree is too corrupted — delete and recreate
            tracing::warn!(
                worktree = %worktree_path.display(),
                error = %e,
                "worktree corrupted, deleting and recreating"
            );
            delete_worktree_force(&config.clone_dir(), &worktree_path)?;
            // Worktree will be recreated on first cascade operation
            Ok(CleanupResult::Deleted)
        }
    }
}

/// Attempt to clean a worktree.
fn attempt_worktree_cleanup(worktree_path: &Path) -> GitResult<()> {
    // Abort any in-progress merge
    let _ = run_git_sync(worktree_path, &["merge", "--abort"]);

    // Hard reset and clean
    run_git_sync(worktree_path, &["reset", "--hard"])?;
    run_git_sync(worktree_path, &["clean", "-fd"])?;

    Ok(())
}

/// Force delete a worktree that may be corrupted.
fn delete_worktree_force(clone_dir: &Path, worktree_path: &Path) -> GitResult<()> {
    // First try git worktree remove
    let result = run_git_sync(
        clone_dir,
        &[
            "worktree",
            "remove",
            "--force",
            worktree_path_str(worktree_path)?,
        ],
    );

    if result.is_err() {
        // If that fails, manually remove the directory and prune
        if worktree_path.exists() {
            std::fs::remove_dir_all(worktree_path)?;
        }
        // Prune stale worktree metadata
        run_git_sync(clone_dir, &["worktree", "prune"])?;
    }

    Ok(())
}

/// Check if a worktree is in a dirty state.
///
/// A worktree is dirty if:
/// - There are uncommitted changes
/// - There is an in-progress merge
/// - The index has conflicts
pub fn is_worktree_dirty(worktree_path: &Path) -> GitResult<bool> {
    if !worktree_path.exists() {
        return Ok(false);
    }

    // Check for uncommitted changes
    let status = run_git_sync(worktree_path, &["status", "--porcelain"]);
    match status {
        Ok(output) => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            if !stdout.trim().is_empty() {
                return Ok(true);
            }
        }
        Err(_) => {
            // If status fails, worktree is definitely in a bad state
            return Ok(true);
        }
    }

    // Check for an in-progress merge. MERGE_HEAD lives in the worktree's
    // gitdir, which for a linked worktree is behind a `gitdir:` redirect in
    // the `.git` file — possibly with a path relative to the worktree, never
    // to the process CWD. Let git resolve it; the returned path is relative
    // to the git process's CWD (the worktree).
    match run_git_stdout(worktree_path, &["rev-parse", "--git-path", "MERGE_HEAD"]) {
        Ok(merge_head_str) => {
            let merge_head = Path::new(&merge_head_str);
            let merge_head = if merge_head.is_absolute() {
                merge_head.to_path_buf()
            } else {
                worktree_path.join(merge_head)
            };
            Ok(merge_head.exists())
        }
        // Cannot determine merge state: report dirty so the caller cleans up
        // rather than proceeding on ambiguous state.
        Err(_) => Ok(true),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::git::run_git_stdout;
    use crate::git::test_support::create_test_repo;
    use crate::git::worktree::worktree_for_stack;

    /// True if the worktree's index has unmerged (conflicted) entries.
    fn has_merge_conflict(worktree_path: &Path) -> bool {
        let output = crate::git::git_command(worktree_path)
            .args(["diff", "--name-only", "--diff-filter=U"])
            .output()
            .unwrap();
        assert!(output.status.success(), "git diff --diff-filter=U failed");
        !String::from_utf8_lossy(&output.stdout).trim().is_empty()
    }

    #[test]
    fn cleanup_worktree_on_abort_cleans_dirty_state() {
        let (_temp_dir, config, _) = create_test_repo();

        // Create a worktree and make it dirty
        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
        run_git_sync(&worktree, &["config", "user.email", "test@test.com"]).unwrap();
        run_git_sync(&worktree, &["config", "user.name", "Test"]).unwrap();

        // Create an untracked file
        std::fs::write(worktree.join("untracked.txt"), "untracked").unwrap();

        // Create a staged change
        std::fs::write(worktree.join("staged.txt"), "staged").unwrap();
        run_git_sync(&worktree, &["add", "staged.txt"]).unwrap();

        // Verify dirty
        assert!(is_worktree_dirty(&worktree).unwrap());

        // Clean up
        let result = cleanup_worktree_on_abort(&worktree).unwrap();
        assert_eq!(result, CleanupResult::Cleaned);

        // Verify clean
        assert!(!is_worktree_dirty(&worktree).unwrap());
        assert!(!worktree.join("untracked.txt").exists());
        assert!(!worktree.join("staged.txt").exists());
    }

    #[test]
    fn cleanup_worktree_on_abort_handles_merge_conflict() {
        let (_temp_dir, config, _) = create_test_repo();

        // Create a worktree
        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
        run_git_sync(&worktree, &["config", "user.email", "test@test.com"]).unwrap();
        run_git_sync(&worktree, &["config", "user.name", "Test"]).unwrap();

        // Create a conflicting branch
        std::fs::write(worktree.join("conflict.txt"), "version 1").unwrap();
        run_git_sync(&worktree, &["add", "."]).unwrap();
        run_git_sync(&worktree, &["commit", "-m", "Version 1"]).unwrap();

        // Create another branch with conflicting content
        run_git_sync(&worktree, &["checkout", "-b", "branch2", "HEAD~1"]).unwrap();
        std::fs::write(worktree.join("conflict.txt"), "version 2").unwrap();
        run_git_sync(&worktree, &["add", "."]).unwrap();
        run_git_sync(&worktree, &["commit", "-m", "Version 2"]).unwrap();

        // Try to merge (will conflict)
        let merge_result = run_git_sync(&worktree, &["merge", "HEAD@{2}", "-m", "Merge"]);
        assert!(merge_result.is_err()); // Should fail due to conflict

        // Verify we have a conflict
        assert!(has_merge_conflict(&worktree));

        // Clean up
        let result = cleanup_worktree_on_abort(&worktree).unwrap();
        assert_eq!(result, CleanupResult::Cleaned);

        // Verify clean
        assert!(!has_merge_conflict(&worktree));
        assert!(!is_worktree_dirty(&worktree).unwrap());
    }

    #[test]
    fn cleanup_worktree_on_abort_returns_not_found() {
        let result = cleanup_worktree_on_abort(Path::new("/nonexistent/path")).unwrap();
        assert_eq!(result, CleanupResult::NotFound);
    }

    #[test]
    fn cleanup_worktree_on_restart_cleans_dirty() {
        let (_temp_dir, config, _) = create_test_repo();

        // Create a dirty worktree
        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
        std::fs::write(worktree.join("dirty.txt"), "dirty").unwrap();

        // Clean on restart
        let result = cleanup_worktree_on_restart(&config, PrNumber(123)).unwrap();
        assert_eq!(result, CleanupResult::Cleaned);

        // Verify clean
        assert!(!worktree.join("dirty.txt").exists());
    }

    #[test]
    fn cleanup_worktree_on_restart_handles_missing() {
        let (_temp_dir, config, _) = create_test_repo();

        // Try to clean a non-existent worktree
        let result = cleanup_worktree_on_restart(&config, PrNumber(999)).unwrap();
        assert_eq!(result, CleanupResult::NotFound);
    }

    #[test]
    fn is_worktree_dirty_detects_uncommitted_changes() {
        let (_temp_dir, config, _) = create_test_repo();

        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();

        // Initially clean
        assert!(!is_worktree_dirty(&worktree).unwrap());

        // Add untracked file
        std::fs::write(worktree.join("untracked.txt"), "untracked").unwrap();
        assert!(is_worktree_dirty(&worktree).unwrap());

        // Clean up
        std::fs::remove_file(worktree.join("untracked.txt")).unwrap();
        assert!(!is_worktree_dirty(&worktree).unwrap());
    }

    /// An in-progress merge with a clean tree (e.g. `merge -s ours --no-commit`
    /// interrupted before commit) is only visible via MERGE_HEAD. For a linked
    /// worktree, `.git` is a `gitdir:` redirect file — possibly with a path
    /// relative to the worktree, never to the process CWD — so MERGE_HEAD must
    /// be located through the redirect.
    #[test]
    fn is_worktree_dirty_detects_merge_head_behind_relative_gitdir() {
        let (_temp_dir, config, _) = create_test_repo();
        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();

        // The linked worktree's gitdir lives under <clone>/worktrees/<name>.
        let gitdir = config.clone_dir().join("worktrees").join("stack-123");
        assert!(
            gitdir.is_dir(),
            "expected linked worktree gitdir at {}",
            gitdir.display()
        );

        // Rewrite the redirect to a relative path; git resolves it against the
        // worktree directory, not the process CWD.
        std::fs::write(
            worktree.join(".git"),
            "gitdir: ../../clone/worktrees/stack-123\n",
        )
        .unwrap();

        // The relative redirect must be valid as far as git is concerned.
        let head = run_git_stdout(&worktree, &["rev-parse", "HEAD"]).unwrap();
        assert_eq!(head.len(), 40);
        assert!(!is_worktree_dirty(&worktree).unwrap());

        // Simulate a merge interrupted before commit: MERGE_HEAD exists but the
        // tree and index are clean.
        std::fs::write(gitdir.join("MERGE_HEAD"), format!("{}\n", head)).unwrap();
        assert!(
            is_worktree_dirty(&worktree).unwrap(),
            "MERGE_HEAD behind a relative gitdir redirect must mark the worktree dirty"
        );

        std::fs::remove_file(gitdir.join("MERGE_HEAD")).unwrap();
        assert!(!is_worktree_dirty(&worktree).unwrap());
    }

    #[test]
    fn cleanup_worktree_on_restart_deletes_corrupted() {
        let (_temp_dir, config, _) = create_test_repo();

        // Create a worktree
        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
        assert!(worktree.exists());

        // Corrupt the worktree by replacing .git (which points to the real gitdir)
        // with invalid content. This will cause git commands to fail.
        let git_file = worktree.join(".git");
        std::fs::write(&git_file, "gitdir: /nonexistent/path/that/does/not/exist").unwrap();

        // Clean on restart - should detect corruption and delete
        let result = cleanup_worktree_on_restart(&config, PrNumber(123)).unwrap();
        assert_eq!(result, CleanupResult::Deleted);

        // Worktree should be gone
        assert!(!worktree.exists());
    }
}
