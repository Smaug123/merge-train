//! Push operations for cascade.
//!
//! All push operations use detached HEAD mode, pushing with
//! `HEAD:refs/heads/<branch>` refspecs.
//!
//! Push idempotency is handled by comparing tree SHAs and verifying
//! parent chains, as merge commits are not reproducible across retries
//! (timestamps, GPG signatures vary).

use std::path::Path;

use crate::types::Sha;

use super::{GitError, GitResult, get_parents, get_tree_sha, rev_parse, run_git_sync};

/// Result of a push operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PushResult {
    /// Push succeeded.
    Success {
        /// The SHA that was pushed.
        pushed_sha: Sha,
    },

    /// Push was rejected (non-fast-forward).
    Rejected {
        /// Details about why the push was rejected.
        details: String,
    },

    /// Push was a no-op (remote already has this commit).
    AlreadyUpToDate,
}

impl PushResult {
    /// Returns true if the push succeeded.
    pub fn is_success(&self) -> bool {
        matches!(self, PushResult::Success { .. })
    }

    /// Returns true if the push was rejected.
    pub fn is_rejected(&self) -> bool {
        matches!(self, PushResult::Rejected { .. })
    }
}

/// Push the current HEAD to a remote branch.
///
/// Uses detached HEAD mode: pushes with `HEAD:refs/heads/<branch>`.
///
/// # Arguments
///
/// * `worktree` - Path to the worktree
/// * `branch` - The remote branch name to push to
///
/// # Returns
///
/// The result of the push operation.
pub fn push_head_to_branch(worktree: &Path, branch: &str) -> GitResult<PushResult> {
    let refspec = format!("HEAD:refs/heads/{}", branch);
    let head_sha = rev_parse(worktree, "HEAD")?;

    // Use git_command for consistent, non-interactive, config-isolated environment.
    // This prevents hangs on auth prompts and ensures reproducible behavior.
    let output = super::git_command(worktree)
        .args(["push", "origin", &refspec])
        .output()?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    if output.status.success() {
        // Check if it was already up-to-date
        if stdout.contains("Everything up-to-date") || stderr.contains("Everything up-to-date") {
            return Ok(PushResult::AlreadyUpToDate);
        }
        return Ok(PushResult::Success {
            pushed_sha: head_sha,
        });
    }

    // Check for rejection
    if stderr.contains("non-fast-forward")
        || stderr.contains("rejected")
        || stderr.contains("failed to push")
    {
        return Ok(PushResult::Rejected {
            details: stderr.to_string(),
        });
    }

    // Other error
    Err(GitError::CommandFailed {
        command: format!("git push origin {}", refspec),
        stderr: stderr.to_string(),
    })
}

/// Get the remote ref SHA for a branch.
///
/// Returns `None` if the branch doesn't exist on the remote.
pub fn get_remote_ref(worktree: &Path, branch: &str) -> GitResult<Option<Sha>> {
    // Use git_command for consistent, non-interactive, config-isolated environment.
    // This prevents hangs on auth prompts and ensures reproducible behavior.
    let output = super::git_command(worktree)
        .args(["ls-remote", "origin", &format!("refs/heads/{}", branch)])
        .output()?;

    if !output.status.success() {
        return Err(GitError::CommandFailed {
            command: format!("git ls-remote origin refs/heads/{}", branch),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        });
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let line = stdout.trim();

    if line.is_empty() {
        return Ok(None);
    }

    // Format: "SHA\trefs/heads/branch"
    let sha_str = line.split('\t').next().unwrap_or("");
    Sha::parse(sha_str)
        .map(Some)
        .map_err(|_| GitError::InvalidSha(sha_str.to_string()))
}

/// Check if a push has already been completed.
///
/// Merge commits are not reproducible (timestamps, GPG signatures vary),
/// so we compare tree SHAs and verify the parent chain includes the
/// expected pre-push SHA.
///
/// # Arguments
///
/// * `worktree` - Path to the worktree
/// * `branch` - The branch to check
/// * `expected_tree` - The expected tree SHA after push
/// * `pre_push_sha` - The SHA the remote had before our push
/// * `expected_second_parent` - For ours-merge scenarios, the expected second parent SHA
///
/// # Returns
///
/// `true` if the push was already completed (remote has our content).
///
/// # Note on ours merges
///
/// For `-s ours` reconciliation merges, the tree doesn't change. In this case,
/// tree comparison alone cannot distinguish "our commit made it" from "someone
/// else pushed a commit with the same tree". We detect this case (expected_tree
/// == pre_push tree) and validate both parents to confirm our specific merge commit.
pub fn is_push_completed(
    worktree: &Path,
    branch: &str,
    expected_tree: &Sha,
    pre_push_sha: &Sha,
    expected_second_parent: Option<&Sha>,
) -> GitResult<bool> {
    // Get the current remote ref
    let Some(remote_sha) = get_remote_ref(worktree, branch)? else {
        return Ok(false);
    };

    // Fetch the remote ref to ensure we have it locally
    run_git_sync(worktree, &["fetch", "origin", "--", branch])?;

    // Compare tree SHAs
    let remote_tree = get_tree_sha(worktree, remote_sha.as_str())?;
    if remote_tree != *expected_tree {
        return Ok(false);
    }

    // Verify the parent chain includes pre_push_sha
    let is_ancestor = super::is_ancestor(worktree, pre_push_sha, &remote_sha)?;
    if !is_ancestor {
        return Ok(false);
    }

    // Check for ours merge scenario: if the expected tree equals the pre-push tree,
    // the tree didn't change (e.g., `-s ours` reconciliation). In this case, tree
    // comparison alone can't distinguish "our merge commit made it" from "someone
    // else pushed a commit with the same tree". We validate both parents to confirm
    // our specific merge commit is there.
    let pre_push_tree = get_tree_sha(worktree, pre_push_sha.as_str())?;
    if *expected_tree == pre_push_tree {
        // Tree didn't change. Validate the parent chain.
        let parents = get_parents(worktree, remote_sha.as_str())?;

        // First parent must match pre_push_sha
        if parents.first() != Some(pre_push_sha) {
            return Ok(false);
        }

        // If we have an expected second parent, validate it too
        if let Some(expected_p2) = expected_second_parent
            && parents.get(1) != Some(expected_p2)
        {
            // Second parent doesn't match - this is a different merge commit
            return Ok(false);
        }

        // Parents match - our merge commit made it
        return Ok(true);
    }

    Ok(true)
}

/// Information needed to verify push completion during recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushIntent {
    /// The branch being pushed to.
    pub branch: String,
    /// The expected tree SHA after push.
    pub expected_tree: Sha,
    /// The SHA the remote had before our push.
    pub pre_push_sha: Sha,
    /// Expected second parent for ours-merge scenarios.
    /// When present, validates that the merge commit has this exact second parent.
    pub expected_second_parent: Option<Sha>,
}

impl PushIntent {
    /// Create a new push intent.
    pub fn new(branch: impl Into<String>, expected_tree: Sha, pre_push_sha: Sha) -> Self {
        Self {
            branch: branch.into(),
            expected_tree,
            pre_push_sha,
            expected_second_parent: None,
        }
    }

    /// Create a push intent for an ours-merge with expected second parent.
    pub fn with_second_parent(
        branch: impl Into<String>,
        expected_tree: Sha,
        pre_push_sha: Sha,
        second_parent: Sha,
    ) -> Self {
        Self {
            branch: branch.into(),
            expected_tree,
            pre_push_sha,
            expected_second_parent: Some(second_parent),
        }
    }

    /// Check if this push has been completed.
    pub fn is_completed(&self, worktree: &Path) -> GitResult<bool> {
        is_push_completed(
            worktree,
            &self.branch,
            &self.expected_tree,
            &self.pre_push_sha,
            self.expected_second_parent.as_ref(),
        )
    }
}

/// Record the current state before a push for recovery purposes.
///
/// Returns the tree SHA of HEAD and the current remote ref SHA.
pub fn capture_pre_push_state(worktree: &Path, branch: &str) -> GitResult<(Sha, Option<Sha>)> {
    let tree_sha = get_tree_sha(worktree, "HEAD")?;
    let remote_sha = get_remote_ref(worktree, branch)?;
    Ok((tree_sha, remote_sha))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::git::worktree::worktree_for_stack;
    use crate::git::{GitConfig, run_git_sync};
    use crate::types::PrNumber;
    use std::time::Duration;
    use tempfile::TempDir;

    /// Create a minimal git repo with a main branch and initial commit.
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

        // Add origin pointing to itself so worktrees can push/fetch
        run_git_sync(
            &clone_dir,
            &["remote", "add", "origin", clone_dir.to_str().unwrap()],
        )
        .unwrap();

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
    fn push_head_to_branch_creates_branch() {
        let (_temp_dir, config) = create_test_repo();

        // Get a worktree and make a commit
        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
        run_git_sync(&worktree, &["config", "user.email", "test@test.com"]).unwrap();
        run_git_sync(&worktree, &["config", "user.name", "Test"]).unwrap();

        std::fs::write(worktree.join("new.txt"), "new content").unwrap();
        run_git_sync(&worktree, &["add", "."]).unwrap();
        run_git_sync(&worktree, &["commit", "-m", "Add new file"]).unwrap();

        // Push to a new branch
        let result = push_head_to_branch(&worktree, "feature-123").unwrap();

        assert!(result.is_success());

        // Verify the branch exists on the remote
        let remote_ref = get_remote_ref(&worktree, "feature-123").unwrap();
        assert!(remote_ref.is_some());
    }

    #[test]
    fn push_head_to_branch_updates_branch() {
        let (_temp_dir, config) = create_test_repo();

        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
        run_git_sync(&worktree, &["config", "user.email", "test@test.com"]).unwrap();
        run_git_sync(&worktree, &["config", "user.name", "Test"]).unwrap();

        // First commit and push
        std::fs::write(worktree.join("file1.txt"), "content1").unwrap();
        run_git_sync(&worktree, &["add", "."]).unwrap();
        run_git_sync(&worktree, &["commit", "-m", "First commit"]).unwrap();
        push_head_to_branch(&worktree, "feature").unwrap();

        // Second commit and push
        std::fs::write(worktree.join("file2.txt"), "content2").unwrap();
        run_git_sync(&worktree, &["add", "."]).unwrap();
        run_git_sync(&worktree, &["commit", "-m", "Second commit"]).unwrap();

        let result = push_head_to_branch(&worktree, "feature").unwrap();
        assert!(result.is_success());

        // Verify HEAD matches remote
        let head_sha = rev_parse(&worktree, "HEAD").unwrap();
        let remote_sha = get_remote_ref(&worktree, "feature").unwrap().unwrap();
        assert_eq!(head_sha, remote_sha);
    }

    #[test]
    fn push_head_rejected_on_force_needed() {
        let (_temp_dir, config) = create_test_repo();

        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
        run_git_sync(&worktree, &["config", "user.email", "test@test.com"]).unwrap();
        run_git_sync(&worktree, &["config", "user.name", "Test"]).unwrap();

        // Create and push a branch
        std::fs::write(worktree.join("file1.txt"), "content1").unwrap();
        run_git_sync(&worktree, &["add", "."]).unwrap();
        run_git_sync(&worktree, &["commit", "-m", "First commit"]).unwrap();
        push_head_to_branch(&worktree, "feature").unwrap();

        // Reset back and create a different commit
        run_git_sync(&worktree, &["reset", "--hard", "HEAD~1"]).unwrap();
        std::fs::write(worktree.join("different.txt"), "different").unwrap();
        run_git_sync(&worktree, &["add", "."]).unwrap();
        run_git_sync(&worktree, &["commit", "-m", "Different commit"]).unwrap();

        // Try to push - should be rejected
        let result = push_head_to_branch(&worktree, "feature").unwrap();
        assert!(result.is_rejected());
    }

    #[test]
    fn get_remote_ref_returns_none_for_nonexistent() {
        let (_temp_dir, config) = create_test_repo();
        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();

        let result = get_remote_ref(&worktree, "nonexistent-branch").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn get_remote_ref_returns_sha_for_existing() {
        let (_temp_dir, config) = create_test_repo();
        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();

        // main branch should exist
        let result = get_remote_ref(&worktree, "main").unwrap();
        assert!(result.is_some());

        // It should be a valid SHA
        let sha = result.unwrap();
        assert_eq!(sha.as_str().len(), 40);
    }

    #[test]
    fn capture_pre_push_state_captures_tree_and_remote() {
        let (_temp_dir, config) = create_test_repo();
        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
        run_git_sync(&worktree, &["config", "user.email", "test@test.com"]).unwrap();
        run_git_sync(&worktree, &["config", "user.name", "Test"]).unwrap();

        // Create a new commit
        std::fs::write(worktree.join("new.txt"), "content").unwrap();
        run_git_sync(&worktree, &["add", "."]).unwrap();
        run_git_sync(&worktree, &["commit", "-m", "New commit"]).unwrap();

        // Capture state for new branch (doesn't exist yet)
        let (tree_sha, remote_sha) = capture_pre_push_state(&worktree, "new-branch").unwrap();

        // Tree SHA should be valid
        assert_eq!(tree_sha.as_str().len(), 40);

        // Remote should be None (branch doesn't exist)
        assert!(remote_sha.is_none());

        // Capture state for existing branch
        let (_, remote_sha) = capture_pre_push_state(&worktree, "main").unwrap();
        assert!(remote_sha.is_some());
    }

    #[test]
    fn is_push_completed_detects_completed_push() {
        let (_temp_dir, config) = create_test_repo();
        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
        run_git_sync(&worktree, &["config", "user.email", "test@test.com"]).unwrap();
        run_git_sync(&worktree, &["config", "user.name", "Test"]).unwrap();

        // Get pre-push state
        let pre_push_sha = get_remote_ref(&worktree, "main").unwrap().unwrap();

        // Create a commit
        std::fs::write(worktree.join("new.txt"), "content").unwrap();
        run_git_sync(&worktree, &["add", "."]).unwrap();
        run_git_sync(&worktree, &["commit", "-m", "New commit"]).unwrap();

        // Get expected tree
        let expected_tree = get_tree_sha(&worktree, "HEAD").unwrap();

        // Push hasn't happened yet
        assert!(
            !is_push_completed(&worktree, "main", &expected_tree, &pre_push_sha, None).unwrap()
        );

        // Do the push
        push_head_to_branch(&worktree, "main").unwrap();

        // Now it should be detected as completed
        assert!(is_push_completed(&worktree, "main", &expected_tree, &pre_push_sha, None).unwrap());
    }

    #[test]
    fn push_intent_is_completed() {
        let (_temp_dir, config) = create_test_repo();
        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
        run_git_sync(&worktree, &["config", "user.email", "test@test.com"]).unwrap();
        run_git_sync(&worktree, &["config", "user.name", "Test"]).unwrap();

        // Get pre-push state
        let pre_push_sha = get_remote_ref(&worktree, "main").unwrap().unwrap();

        // Create a commit
        std::fs::write(worktree.join("new.txt"), "content").unwrap();
        run_git_sync(&worktree, &["add", "."]).unwrap();
        run_git_sync(&worktree, &["commit", "-m", "New commit"]).unwrap();

        let expected_tree = get_tree_sha(&worktree, "HEAD").unwrap();

        let intent = PushIntent::new("main", expected_tree, pre_push_sha);

        // Not completed yet
        assert!(!intent.is_completed(&worktree).unwrap());

        // Do the push
        push_head_to_branch(&worktree, "main").unwrap();

        // Now completed
        assert!(intent.is_completed(&worktree).unwrap());
    }

    #[test]
    fn is_push_completed_detects_ours_merge() {
        // Test that is_push_completed correctly detects a completed ours-merge push.
        // In an ours-merge, the tree doesn't change (expected_tree == pre_push_tree).
        // The fix checks if remote's first parent is pre_push_sha to confirm our merge made it.
        let (_temp_dir, config) = create_test_repo();
        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
        run_git_sync(&worktree, &["config", "user.email", "test@test.com"]).unwrap();
        run_git_sync(&worktree, &["config", "user.name", "Test"]).unwrap();

        // Get pre-push state
        let pre_push_sha = get_remote_ref(&worktree, "main").unwrap().unwrap();
        let pre_push_tree = get_tree_sha(&worktree, pre_push_sha.as_str()).unwrap();

        // Create a temporary commit with different content (simulates a feature branch)
        std::fs::write(worktree.join("feature.txt"), "feature content").unwrap();
        run_git_sync(&worktree, &["add", "."]).unwrap();
        run_git_sync(&worktree, &["commit", "-m", "Feature commit"]).unwrap();
        let feature_sha = rev_parse(&worktree, "HEAD").unwrap();

        // Go back to main (detached) and create an ours merge
        // This simulates reconciliation after a squash where we keep main's tree
        run_git_sync(&worktree, &["checkout", "--detach", pre_push_sha.as_str()]).unwrap();
        run_git_sync(
            &worktree,
            &[
                "merge",
                "-s",
                "ours",
                feature_sha.as_str(),
                "-m",
                "Ours merge",
            ],
        )
        .unwrap();

        // The tree should be unchanged (ours merge keeps main's tree)
        let expected_tree = get_tree_sha(&worktree, "HEAD").unwrap();
        assert_eq!(
            expected_tree, pre_push_tree,
            "Ours merge should preserve main's tree"
        );

        // Push hasn't happened yet - remote still has pre_push_sha
        assert!(
            !is_push_completed(&worktree, "main", &expected_tree, &pre_push_sha, None).unwrap(),
            "Should not detect completion before push"
        );

        // Do the push
        push_head_to_branch(&worktree, "main").unwrap();

        // Now it should be detected as completed (the fix makes this work)
        // Without expected second parent, it only checks first parent
        assert!(
            is_push_completed(&worktree, "main", &expected_tree, &pre_push_sha, None).unwrap(),
            "Should detect ours-merge push as completed"
        );

        // With expected second parent, should also succeed
        assert!(
            is_push_completed(
                &worktree,
                "main",
                &expected_tree,
                &pre_push_sha,
                Some(&feature_sha)
            )
            .unwrap(),
            "Should detect ours-merge with correct second parent"
        );
    }

    #[test]
    fn is_push_completed_rejects_wrong_second_parent() {
        // Test that is_push_completed rejects an ours-merge with wrong second parent.
        // This prevents misclassifying a different merge commit as our push.
        let (_temp_dir, config) = create_test_repo();
        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
        run_git_sync(&worktree, &["config", "user.email", "test@test.com"]).unwrap();
        run_git_sync(&worktree, &["config", "user.name", "Test"]).unwrap();

        // Get pre-push state
        let pre_push_sha = get_remote_ref(&worktree, "main").unwrap().unwrap();

        // Create a feature commit
        std::fs::write(worktree.join("feature.txt"), "feature content").unwrap();
        run_git_sync(&worktree, &["add", "."]).unwrap();
        run_git_sync(&worktree, &["commit", "-m", "Feature commit"]).unwrap();
        let feature_sha = rev_parse(&worktree, "HEAD").unwrap();

        // Create a different commit (to use as wrong second parent)
        run_git_sync(&worktree, &["checkout", "--detach", pre_push_sha.as_str()]).unwrap();
        std::fs::write(worktree.join("other.txt"), "other content").unwrap();
        run_git_sync(&worktree, &["add", "."]).unwrap();
        run_git_sync(&worktree, &["commit", "-m", "Other commit"]).unwrap();
        let other_sha = rev_parse(&worktree, "HEAD").unwrap();

        // Go back to main (detached) and create an ours merge with feature_sha
        run_git_sync(&worktree, &["checkout", "--detach", pre_push_sha.as_str()]).unwrap();
        run_git_sync(
            &worktree,
            &[
                "merge",
                "-s",
                "ours",
                feature_sha.as_str(),
                "-m",
                "Ours merge",
            ],
        )
        .unwrap();

        let expected_tree = get_tree_sha(&worktree, "HEAD").unwrap();

        // Push the merge
        push_head_to_branch(&worktree, "main").unwrap();

        // With correct second parent - should succeed
        assert!(
            is_push_completed(
                &worktree,
                "main",
                &expected_tree,
                &pre_push_sha,
                Some(&feature_sha)
            )
            .unwrap(),
            "Should succeed with correct second parent"
        );

        // With wrong second parent - should fail
        assert!(
            !is_push_completed(
                &worktree,
                "main",
                &expected_tree,
                &pre_push_sha,
                Some(&other_sha)
            )
            .unwrap(),
            "Should reject wrong second parent"
        );
    }
}
