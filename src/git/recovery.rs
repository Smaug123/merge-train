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

use super::{GitConfig, GitError, GitResult, run_git_sync, worktree};
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
            worktree_path.to_str().unwrap(),
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

    // Check for in-progress merge
    let merge_head = worktree_path.join(".git/MERGE_HEAD");
    if merge_head.exists() {
        return Ok(true);
    }

    // Also check the worktree-specific gitdir (for worktrees, .git is a file pointing to the real gitdir)
    let git_path = worktree_path.join(".git");
    if git_path.is_file()
        && let Ok(content) = std::fs::read_to_string(&git_path)
        && let Some(gitdir) = content.trim().strip_prefix("gitdir: ")
    {
        let merge_head = Path::new(gitdir).join("MERGE_HEAD");
        if merge_head.exists() {
            return Ok(true);
        }
    }

    Ok(false)
}

/// Check if a worktree is in a merge conflict state.
pub fn has_merge_conflict(worktree_path: &Path) -> GitResult<bool> {
    if !worktree_path.exists() {
        return Ok(false);
    }

    // Use git_command for consistent, non-interactive, config-isolated environment.
    // This prevents hangs on auth prompts and ensures reproducible behavior.
    let output = super::git_command(worktree_path)
        .args(["diff", "--name-only", "--diff-filter=U"])
        .output()?;

    if !output.status.success() {
        // If this command fails, we can't determine conflict state
        return Err(GitError::CommandFailed {
            command: "git diff --name-only --diff-filter=U".to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        });
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(!stdout.trim().is_empty())
}

/// Verify preparation was completed for a descendant.
///
/// Before reconciliation, we must verify that the descendant was prepared
/// (predecessor head merged into it) before the squash. This is checked by
/// verifying that the predecessor's pre-squash head is an ancestor of the
/// descendant's head.
///
/// # Arguments
///
/// * `worktree` - Path to the worktree
/// * `predecessor_head` - The predecessor's head SHA (before squash)
/// * `descendant_head` - The descendant's current head SHA
///
/// # Returns
///
/// `true` if preparation was completed (predecessor head is an ancestor).
pub fn verify_preparation_completed(
    worktree: &Path,
    predecessor_head: &crate::types::Sha,
    descendant_head: &crate::types::Sha,
) -> GitResult<bool> {
    super::is_ancestor(worktree, predecessor_head, descendant_head)
}

/// Get the status of all worktrees in a repo.
///
/// Returns a list of (PrNumber, dirty, age_secs) tuples.
pub fn get_worktree_statuses(config: &GitConfig) -> GitResult<Vec<(PrNumber, bool, u64)>> {
    let worktrees = worktree::list_worktrees(config)?;
    let mut statuses = Vec::new();

    for (pr_number, path) in worktrees {
        let dirty = is_worktree_dirty(&path).unwrap_or(true);
        let age = worktree::worktree_age(config, pr_number)?
            .map(|d| d.as_secs())
            .unwrap_or(0);
        statuses.push((pr_number, dirty, age));
    }

    Ok(statuses)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::git::worktree::worktree_for_stack;
    use crate::git::{GitConfig, run_git_stdout};
    use crate::types::Sha;
    use std::time::Duration;
    use tempfile::TempDir;

    /// Create a minimal git repo for testing.
    fn create_test_repo() -> (TempDir, GitConfig) {
        let temp_dir = TempDir::new().unwrap();
        let base_dir = temp_dir.path().to_path_buf();

        let config = GitConfig {
            base_dir: base_dir.clone(),
            owner: "test".to_string(),
            repo: "repo".to_string(),
            default_branch: "main".to_string(),
            worktree_max_age: Duration::from_secs(24 * 3600),
            commit_identity: crate::git::CommitIdentity {
                name: "Test".to_string(),
                email: "test@test.com".to_string(),
                signing_key: None,
            },
        };

        // Create the clone directory and initialize a bare repo
        let clone_dir = config.clone_dir();
        std::fs::create_dir_all(&clone_dir).unwrap();
        run_git_sync(&clone_dir, &["init", "--bare"]).unwrap();

        // Create a temporary working repo to make an initial commit
        let work_dir = temp_dir.path().join("work");
        std::fs::create_dir_all(&work_dir).unwrap();
        run_git_sync(&work_dir, &["init"]).unwrap();
        run_git_sync(&work_dir, &["config", "user.email", "test@test.com"]).unwrap();
        run_git_sync(&work_dir, &["config", "user.name", "Test"]).unwrap();

        // Create initial commit
        std::fs::write(work_dir.join("README.md"), "# Test").unwrap();
        run_git_sync(&work_dir, &["add", "."]).unwrap();
        run_git_sync(&work_dir, &["commit", "-m", "Initial commit"]).unwrap();

        // Push to the bare repo
        run_git_sync(
            &work_dir,
            &["remote", "add", "origin", clone_dir.to_str().unwrap()],
        )
        .unwrap();
        run_git_sync(&work_dir, &["push", "-u", "origin", "HEAD:main"]).unwrap();

        // Update HEAD in the bare repo
        run_git_sync(&clone_dir, &["symbolic-ref", "HEAD", "refs/heads/main"]).unwrap();

        (temp_dir, config)
    }

    #[test]
    fn cleanup_worktree_on_abort_cleans_dirty_state() {
        let (_temp_dir, config) = create_test_repo();

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
        let (_temp_dir, config) = create_test_repo();

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
        assert!(has_merge_conflict(&worktree).unwrap());

        // Clean up
        let result = cleanup_worktree_on_abort(&worktree).unwrap();
        assert_eq!(result, CleanupResult::Cleaned);

        // Verify clean
        assert!(!has_merge_conflict(&worktree).unwrap());
        assert!(!is_worktree_dirty(&worktree).unwrap());
    }

    #[test]
    fn cleanup_worktree_on_abort_returns_not_found() {
        let result = cleanup_worktree_on_abort(Path::new("/nonexistent/path")).unwrap();
        assert_eq!(result, CleanupResult::NotFound);
    }

    #[test]
    fn cleanup_worktree_on_restart_cleans_dirty() {
        let (_temp_dir, config) = create_test_repo();

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
        let (_temp_dir, config) = create_test_repo();

        // Try to clean a non-existent worktree
        let result = cleanup_worktree_on_restart(&config, PrNumber(999)).unwrap();
        assert_eq!(result, CleanupResult::NotFound);
    }

    #[test]
    fn is_worktree_dirty_detects_uncommitted_changes() {
        let (_temp_dir, config) = create_test_repo();

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

    #[test]
    fn verify_preparation_completed_works() {
        let (_temp_dir, config) = create_test_repo();

        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
        run_git_sync(&worktree, &["config", "user.email", "test@test.com"]).unwrap();
        run_git_sync(&worktree, &["config", "user.name", "Test"]).unwrap();

        // Get initial commit
        let initial = run_git_stdout(&worktree, &["rev-parse", "HEAD"]).unwrap();
        let initial = Sha::parse(&initial).unwrap();

        // Create a new commit
        std::fs::write(worktree.join("new.txt"), "new").unwrap();
        run_git_sync(&worktree, &["add", "."]).unwrap();
        run_git_sync(&worktree, &["commit", "-m", "New commit"]).unwrap();

        let new_head = run_git_stdout(&worktree, &["rev-parse", "HEAD"]).unwrap();
        let new_head = Sha::parse(&new_head).unwrap();

        // Initial should be ancestor of new
        assert!(verify_preparation_completed(&worktree, &initial, &new_head).unwrap());

        // New should not be ancestor of initial
        assert!(!verify_preparation_completed(&worktree, &new_head, &initial).unwrap());
    }

    #[test]
    fn get_worktree_statuses_lists_all() {
        let (_temp_dir, config) = create_test_repo();

        // Create some worktrees
        worktree_for_stack(&config, PrNumber(100)).unwrap();
        let wt2 = worktree_for_stack(&config, PrNumber(200)).unwrap();

        // Make one dirty
        std::fs::write(wt2.join("dirty.txt"), "dirty").unwrap();

        let statuses = get_worktree_statuses(&config).unwrap();

        assert_eq!(statuses.len(), 2);

        // Find statuses by PR number
        let status_100 = statuses.iter().find(|(pr, _, _)| pr.0 == 100).unwrap();
        let status_200 = statuses.iter().find(|(pr, _, _)| pr.0 == 200).unwrap();

        assert!(!status_100.1); // PR 100 is clean
        assert!(status_200.1); // PR 200 is dirty
    }
}
