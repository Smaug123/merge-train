//! Worktree management for per-stack isolation.
//!
//! Each stack gets its own isolated git worktree, enabling:
//! - True stack isolation (stop on stack A doesn't affect stack B)
//! - No partial merge state leaks between stacks
//! - Each stack can have its own checked-out commit (in detached HEAD mode)
//!
//! Worktrees are keyed by the **original** root PR number (the PR that received
//! `@merge-train start`), not the current root. This provides a stable identifier
//! throughout the cascade.

use std::collections::HashSet;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use crate::types::PrNumber;

use super::{GitConfig, GitResult, parse_stack_dir_name, run_git_sync};

/// Get or create a worktree for a stack.
///
/// Worktrees are created in detached HEAD mode to avoid git's restriction that
/// a branch can only be checked out in one worktree at a time. Operations within
/// the worktree use `git checkout --detach origin/<branch>` and push with
/// `git push origin HEAD:refs/heads/<branch>`.
///
/// # Arguments
///
/// * `config` - Git configuration with paths
/// * `root_pr` - The original root PR number (stable identifier for the stack)
///
/// # Returns
///
/// The path to the worktree directory.
pub fn worktree_for_stack(config: &GitConfig, root_pr: PrNumber) -> GitResult<PathBuf> {
    let worktree_path = config.worktree_path(root_pr);

    if !worktree_path.exists() {
        // Ensure the worktrees directory exists
        std::fs::create_dir_all(config.worktrees_dir())?;

        // Create worktree from the shared clone in detached HEAD mode.
        // Using --detach ensures no branch is "checked out" in this worktree,
        // avoiding conflicts with other worktrees or the main clone.
        run_git_sync(
            &config.clone_dir(),
            &[
                "worktree",
                "add",
                "--detach",
                worktree_path.to_str().unwrap(),
                "HEAD",
            ],
        )?;
    }

    Ok(worktree_path)
}

/// Remove a stack's worktree.
///
/// Called after stack completes successfully, on stop command, or when
/// `cleanup_worktree_on_abort` fails (worktree corrupted beyond repair).
///
/// This function is idempotent - if the worktree doesn't exist, it succeeds.
pub fn remove_worktree(config: &GitConfig, root_pr: PrNumber) -> GitResult<()> {
    let worktree_path = config.worktree_path(root_pr);

    if worktree_path.exists() {
        // Use --force to remove even if there are uncommitted changes
        // (we've already cleaned up or decided to discard the state)
        run_git_sync(
            &config.clone_dir(),
            &[
                "worktree",
                "remove",
                "--force",
                worktree_path.to_str().unwrap(),
            ],
        )?;
    }

    Ok(())
}

/// Prune stale worktree references (housekeeping).
///
/// This cleans up git's internal worktree metadata for worktrees that
/// were deleted manually or whose directories no longer exist.
pub fn prune_worktrees(config: &GitConfig) -> GitResult<()> {
    run_git_sync(&config.clone_dir(), &["worktree", "prune"])?;
    Ok(())
}

/// Clean up stale worktrees on startup.
///
/// If the bot crashes mid-operation, worktrees may be left behind. This function
/// removes worktrees that are either:
/// - Not associated with an active train (orphaned)
/// - Older than the max age threshold (stale from a previous crashed instance)
///
/// Call this during startup after loading active trains from the state file.
///
/// # Arguments
///
/// * `config` - Git configuration with paths and max age
/// * `active_train_roots` - Set of PR numbers for currently active trains
///
/// # Returns
///
/// A list of PR numbers whose worktrees were removed.
pub fn cleanup_stale_worktrees(
    config: &GitConfig,
    active_train_roots: &HashSet<PrNumber>,
) -> GitResult<Vec<PrNumber>> {
    let worktrees_dir = config.worktrees_dir();
    if !worktrees_dir.exists() {
        return Ok(vec![]);
    }

    let mut removed = Vec::new();

    for entry in std::fs::read_dir(&worktrees_dir)? {
        let entry = entry?;
        let path = entry.path();

        // Parse "stack-NNN" directory name to extract PR number
        let Some(pr_number) = parse_stack_dir_name(&path) else {
            // Not a stack directory, skip
            continue;
        };

        // Keep if this is an active train
        if active_train_roots.contains(&pr_number) {
            continue;
        }

        // Check directory age via mtime
        let metadata = std::fs::metadata(&path)?;
        let modified = metadata.modified()?;
        let age = SystemTime::now()
            .duration_since(modified)
            .unwrap_or(Duration::MAX);

        // Use >= to avoid flakiness with coarse timestamp resolution.
        // When max_age is zero, we want to clean up immediately.
        if age >= config.worktree_max_age {
            tracing::info!(
                path = %path.display(),
                age_hours = age.as_secs() / 3600,
                pr = pr_number.0,
                "removing stale worktree"
            );
            remove_worktree(config, pr_number)?;
            removed.push(pr_number);
        }
    }

    // Also run git worktree prune to clean up any dangling references
    prune_worktrees(config)?;

    Ok(removed)
}

/// Check if a worktree exists for the given stack.
pub fn worktree_exists(config: &GitConfig, root_pr: PrNumber) -> bool {
    config.worktree_path(root_pr).exists()
}

/// List all existing worktrees and their associated PR numbers.
pub fn list_worktrees(config: &GitConfig) -> GitResult<Vec<(PrNumber, PathBuf)>> {
    let worktrees_dir = config.worktrees_dir();
    if !worktrees_dir.exists() {
        return Ok(vec![]);
    }

    let mut worktrees = Vec::new();

    for entry in std::fs::read_dir(&worktrees_dir)? {
        let entry = entry?;
        let path = entry.path();

        if let Some(pr_number) = parse_stack_dir_name(&path) {
            worktrees.push((pr_number, path));
        }
    }

    // Sort by PR number for deterministic ordering
    worktrees.sort_by_key(|(pr, _)| pr.0);

    Ok(worktrees)
}

/// Get the age of a worktree (time since last modification).
pub fn worktree_age(config: &GitConfig, root_pr: PrNumber) -> GitResult<Option<Duration>> {
    let worktree_path = config.worktree_path(root_pr);

    if !worktree_path.exists() {
        return Ok(None);
    }

    let metadata = std::fs::metadata(&worktree_path)?;
    let modified = metadata.modified()?;
    let age = SystemTime::now()
        .duration_since(modified)
        .unwrap_or(Duration::MAX);

    Ok(Some(age))
}

/// Initialize the worktrees directory structure if needed.
///
/// This creates the necessary directories but does not create any worktrees.
/// Worktrees are created lazily by `worktree_for_stack`.
pub fn init_worktree_dir(config: &GitConfig) -> GitResult<()> {
    std::fs::create_dir_all(config.worktrees_dir())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn worktree_for_stack_creates_worktree() {
        let (_temp_dir, config) = create_test_repo();

        let worktree_path = worktree_for_stack(&config, PrNumber(123)).unwrap();

        assert!(worktree_path.exists());
        assert_eq!(worktree_path, config.worktree_path(PrNumber(123)));

        // Check that we're in detached HEAD mode
        let head =
            super::super::run_git_stdout(&worktree_path, &["rev-parse", "--abbrev-ref", "HEAD"])
                .unwrap();
        assert_eq!(head, "HEAD", "Should be in detached HEAD mode");
    }

    #[test]
    fn worktree_for_stack_is_idempotent() {
        let (_temp_dir, config) = create_test_repo();

        let path1 = worktree_for_stack(&config, PrNumber(123)).unwrap();
        let path2 = worktree_for_stack(&config, PrNumber(123)).unwrap();

        assert_eq!(path1, path2);
    }

    #[test]
    fn remove_worktree_removes_existing() {
        let (_temp_dir, config) = create_test_repo();

        // Create a worktree
        let worktree_path = worktree_for_stack(&config, PrNumber(123)).unwrap();
        assert!(worktree_path.exists());

        // Remove it
        remove_worktree(&config, PrNumber(123)).unwrap();
        assert!(!worktree_path.exists());
    }

    #[test]
    fn remove_worktree_is_idempotent() {
        let (_temp_dir, config) = create_test_repo();

        // Create and remove a worktree
        worktree_for_stack(&config, PrNumber(123)).unwrap();
        remove_worktree(&config, PrNumber(123)).unwrap();

        // Removing again should succeed
        remove_worktree(&config, PrNumber(123)).unwrap();
    }

    #[test]
    fn list_worktrees_returns_all() {
        let (_temp_dir, config) = create_test_repo();

        // Create some worktrees
        worktree_for_stack(&config, PrNumber(100)).unwrap();
        worktree_for_stack(&config, PrNumber(200)).unwrap();
        worktree_for_stack(&config, PrNumber(300)).unwrap();

        let worktrees = list_worktrees(&config).unwrap();
        let pr_numbers: Vec<_> = worktrees.iter().map(|(pr, _)| pr.0).collect();

        assert_eq!(pr_numbers, vec![100, 200, 300]);
    }

    #[test]
    fn cleanup_stale_worktrees_preserves_active() {
        let (_temp_dir, config) = create_test_repo();

        // Create worktrees
        worktree_for_stack(&config, PrNumber(100)).unwrap();
        worktree_for_stack(&config, PrNumber(200)).unwrap();

        // Mark 100 as active
        let mut active = HashSet::new();
        active.insert(PrNumber(100));

        // Cleanup with 0 max age (everything is "stale")
        let mut config_zero_age = config.clone();
        config_zero_age.worktree_max_age = Duration::ZERO;

        let removed = cleanup_stale_worktrees(&config_zero_age, &active).unwrap();

        // Only 200 should be removed (100 is active)
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0], PrNumber(200));

        // Verify 100 still exists, 200 is gone
        assert!(worktree_exists(&config, PrNumber(100)));
        assert!(!worktree_exists(&config, PrNumber(200)));
    }

    #[test]
    fn worktree_age_returns_none_for_missing() {
        let (_temp_dir, config) = create_test_repo();

        let age = worktree_age(&config, PrNumber(999)).unwrap();
        assert!(age.is_none());
    }

    #[test]
    fn worktree_age_returns_some_for_existing() {
        let (_temp_dir, config) = create_test_repo();

        worktree_for_stack(&config, PrNumber(123)).unwrap();

        let age = worktree_age(&config, PrNumber(123)).unwrap();
        assert!(age.is_some());
        // Should be very recent
        assert!(age.unwrap() < Duration::from_secs(60));
    }

    #[test]
    fn multiple_worktrees_are_isolated() {
        let (_temp_dir, config) = create_test_repo();

        // Create two worktrees
        let wt1 = worktree_for_stack(&config, PrNumber(100)).unwrap();
        let wt2 = worktree_for_stack(&config, PrNumber(200)).unwrap();

        // Create a file in worktree 1
        std::fs::write(wt1.join("file1.txt"), "content1").unwrap();
        run_git_sync(&wt1, &["add", "file1.txt"]).unwrap();

        // Worktree 2 should not have this file
        assert!(!wt2.join("file1.txt").exists());

        // Create a different file in worktree 2
        std::fs::write(wt2.join("file2.txt"), "content2").unwrap();
        run_git_sync(&wt2, &["add", "file2.txt"]).unwrap();

        // Worktree 1 should not have worktree 2's file
        assert!(!wt1.join("file2.txt").exists());
    }
}
