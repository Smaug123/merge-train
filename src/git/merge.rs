//! Merge operations for cascade (prepare, reconcile, catch-up).
//!
//! These functions implement the core merge operations needed for the cascade:
//!
//! 1. **Preparation** (`prepare_descendant`): Merge predecessor head into descendant.
//!    This happens BEFORE the predecessor is squash-merged into main.
//!
//! 2. **Reconciliation** (`reconcile_descendant`): After squash-merge, incorporate the
//!    squash into the descendant's history. This involves:
//!    - Merging `$SQUASH_SHA^` (parent of squash = main state just before squash)
//!    - Ours-merging `$SQUASH_SHA` (marks squash as ancestor without changing tree)
//!
//! 3. **Catch-up** (`catch_up_descendant`): Merge any commits that landed on main
//!    AFTER the squash. This is a regular merge (not ours).
//!
//! All operations are performed in detached HEAD mode to avoid branch locking issues.
//!
//! **CRITICAL**: Do NOT merge main during preparation! The $SQUASH_SHA^ ordering is
//! essential to prevent lost commits. See "Why merging $SQUASH_SHA^ is essential" in
//! DESIGN.md.

use std::path::Path;

use crate::effects::git::MergeStrategy;
use crate::types::Sha;

use super::{
    GitError, GitResult, MergeResult, checkout_detached, fetch, get_parents, git_command,
    rev_parse, run_git_stdout,
};

/// Prepare a descendant by merging the predecessor's head into it.
///
/// This is called BEFORE the predecessor is squash-merged. It ensures the
/// descendant has all of the predecessor's final content.
///
/// **CRITICAL**: This function does NOT merge main. That happens during
/// reconciliation after the squash, using `$SQUASH_SHA^`. Merging main here
/// would cause lost commits.
///
/// # Arguments
///
/// * `worktree` - Path to the stack's worktree
/// * `descendant_branch` - The descendant's branch name (e.g., "feature-2")
/// * `predecessor_head` - The predecessor's head SHA (from PR refs or cache)
/// * `sign` - Whether to sign the merge commit
///
/// # Returns
///
/// The result of the merge operation.
pub fn prepare_descendant(
    worktree: &Path,
    descendant_branch: &str,
    predecessor_head: &Sha,
    sign: bool,
) -> GitResult<MergeResult> {
    // Fetch the descendant branch and predecessor head
    fetch(worktree, &[descendant_branch, predecessor_head.as_str()]).map_err(|e| {
        if let GitError::CommandFailed { stderr, .. } = &e
            && stderr.contains("couldn't find remote ref")
        {
            return GitError::FetchFailed {
                refspec: descendant_branch.to_string(),
                details: stderr.clone(),
            };
        }
        e
    })?;

    // Checkout the descendant branch in detached HEAD mode
    let remote_branch = format!("origin/{}", descendant_branch);
    checkout_detached(worktree, &remote_branch)?;

    // Merge the predecessor's head
    let message = format!(
        "Merge predecessor into {} (merge train preparation)",
        descendant_branch
    );

    merge_with_message(
        worktree,
        predecessor_head.as_str(),
        &message,
        MergeStrategy::Default,
        sign,
    )
}

/// Reconcile a descendant after the predecessor has been squash-merged.
///
/// This performs two merges:
/// 1. Merge `$SQUASH_SHA^` (parent of squash = main state just before squash)
/// 2. Ours-merge `$SQUASH_SHA` (marks squash as ancestor without changing tree)
///
/// The ours-merge is essential: it records the squash commit as an ancestor in
/// the descendant's history without changing its tree. This allows the descendant
/// to be fast-forward merged later.
///
/// # Arguments
///
/// * `worktree` - Path to the stack's worktree
/// * `descendant_branch` - The descendant's branch name
/// * `squash_sha` - The SHA of the squash commit on main
/// * `sign` - Whether to sign the merge commits
///
/// # Returns
///
/// The result of the merge operation (Success if both merges succeeded).
///
/// # Errors
///
/// Returns an error if:
/// - The squash commit has no parent (shouldn't happen for a valid squash)
/// - Either merge results in a conflict
/// - The squash commit has multiple parents (not a squash merge)
pub fn reconcile_descendant(
    worktree: &Path,
    descendant_branch: &str,
    squash_sha: &Sha,
    sign: bool,
) -> GitResult<MergeResult> {
    // Fetch the squash commit and ensure we have it locally
    fetch(worktree, &[squash_sha.as_str()])?;

    // Get the parent of the squash commit (main state just before squash)
    let parents = get_parents(worktree, squash_sha.as_str())?;

    // Validate this is a squash merge (single parent)
    if parents.is_empty() {
        return Err(GitError::CommandFailed {
            command: "get squash parent".to_string(),
            stderr: format!("Squash commit {} has no parents", squash_sha),
        });
    }
    if parents.len() > 1 {
        return Err(GitError::CommandFailed {
            command: "validate squash commit".to_string(),
            stderr: format!(
                "Commit {} has {} parents, expected 1 for squash merge. \
                 This may indicate a merge commit or rebase was used instead of squash.",
                squash_sha,
                parents.len()
            ),
        });
    }

    let squash_parent = &parents[0];

    // Checkout the descendant branch in detached HEAD mode
    let remote_branch = format!("origin/{}", descendant_branch);
    checkout_detached(worktree, &remote_branch)?;

    // Step 1: Merge the PARENT of the squash commit ($SQUASH_SHA^)
    // This incorporates all main content up to (but not including) the squash
    let message1 = format!(
        "Merge pre-squash main into {} (merge train reconciliation)",
        descendant_branch
    );

    let result1 = merge_with_message(
        worktree,
        squash_parent.as_str(),
        &message1,
        MergeStrategy::Default,
        sign,
    )?;

    if let MergeResult::Conflict { .. } = result1 {
        return Ok(result1);
    }

    // Step 2: Ours-merge the squash commit itself
    // This marks the squash as an ancestor without changing the tree
    let message2 = format!(
        "Relate main history with {} (merge train reconciliation)",
        descendant_branch
    );

    merge_with_message(
        worktree,
        squash_sha.as_str(),
        &message2,
        MergeStrategy::Ours,
        sign,
    )
}

/// Catch up a descendant with any commits that landed on main after the squash.
///
/// This is a regular merge (not ours) to incorporate commits that landed on main
/// AFTER the predecessor was squash-merged.
///
/// If main hasn't advanced past the squash commit, this merge is a no-op
/// (AlreadyUpToDate).
///
/// # Arguments
///
/// * `worktree` - Path to the stack's worktree
/// * `descendant_branch` - The descendant's branch name
/// * `default_branch` - The default branch name (e.g., "main")
/// * `sign` - Whether to sign the merge commit
///
/// # Returns
///
/// The result of the merge operation.
pub fn catch_up_descendant(
    worktree: &Path,
    descendant_branch: &str,
    default_branch: &str,
    sign: bool,
) -> GitResult<MergeResult> {
    // Fetch the latest default branch
    fetch(worktree, &[default_branch])?;

    // Checkout the descendant branch in detached HEAD mode
    let remote_branch = format!("origin/{}", descendant_branch);
    checkout_detached(worktree, &remote_branch)?;

    // Merge origin/main
    let remote_default = format!("origin/{}", default_branch);
    let message = format!(
        "Merge {} into {} (merge train catch-up)",
        default_branch, descendant_branch
    );

    merge_with_message(
        worktree,
        &remote_default,
        &message,
        MergeStrategy::Default,
        sign,
    )
}

/// Perform a merge with a specific message and strategy.
fn merge_with_message(
    worktree: &Path,
    target: &str,
    message: &str,
    strategy: MergeStrategy,
    sign: bool,
) -> GitResult<MergeResult> {
    let mut args = vec!["merge", "--no-edit", "-m", message];

    if let Some(strategy_arg) = strategy.as_git_arg() {
        args.push("-s");
        args.push(strategy_arg);
    }

    if sign {
        args.push("-S");
    }

    args.push(target);

    let output = git_command(worktree).args(&args).output()?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    if output.status.success() {
        // Check if it was already up-to-date
        if stdout.contains("Already up to date") {
            return Ok(MergeResult::AlreadyUpToDate);
        }

        // Get the resulting commit SHA
        let commit_sha = rev_parse(worktree, "HEAD")?;
        return Ok(MergeResult::Success { commit_sha });
    }

    // Check for merge conflict (git outputs CONFLICT to stdout, not stderr)
    let combined = format!("{}{}", stdout, stderr);
    if combined.contains("CONFLICT") || combined.contains("Automatic merge failed") {
        // Get the list of conflicting files
        let conflicting_files = get_conflicting_files(worktree)?;
        return Ok(MergeResult::Conflict { conflicting_files });
    }

    // Other error
    Err(GitError::CommandFailed {
        command: format!("git {}", args.join(" ")),
        stderr: stderr.to_string(),
    })
}

/// Get the list of files with merge conflicts.
fn get_conflicting_files(worktree: &Path) -> GitResult<Vec<String>> {
    // git diff --name-only --diff-filter=U lists unmerged files
    match run_git_stdout(worktree, &["diff", "--name-only", "--diff-filter=U"]) {
        Ok(output) => Ok(output.lines().map(|s| s.to_string()).collect()),
        Err(_) => {
            // Fallback: try to get files from ls-files
            let output = run_git_stdout(worktree, &["ls-files", "-u"])?;
            let files: Vec<_> = output
                .lines()
                .filter_map(|line| line.split('\t').nth(1))
                .map(|s| s.to_string())
                .collect();
            Ok(files)
        }
    }
}

/// Update a descendant for the BEHIND status on the root PR.
///
/// This is used when the root PR (targeting main) has a BEHIND status and needs
/// to be updated. This is different from catch_up_descendant in that:
/// - It's for the ROOT PR, not a descendant
/// - The root has no predecessor to protect
/// - It's safe to merge main directly
///
/// # Arguments
///
/// * `worktree` - Path to the stack's worktree
/// * `branch` - The branch name
/// * `default_branch` - The default branch name (e.g., "main")
/// * `sign` - Whether to sign the merge commit
///
/// # Returns
///
/// The result of the merge operation.
pub fn update_root_for_behind(
    worktree: &Path,
    branch: &str,
    default_branch: &str,
    sign: bool,
) -> GitResult<MergeResult> {
    // Fetch the latest default branch
    fetch(worktree, &[default_branch])?;

    // Checkout the branch in detached HEAD mode
    let remote_branch = format!("origin/{}", branch);
    checkout_detached(worktree, &remote_branch)?;

    // Merge origin/main
    let remote_default = format!("origin/{}", default_branch);
    let message = format!("Merge {} to satisfy branch protection", default_branch);

    merge_with_message(
        worktree,
        &remote_default,
        &message,
        MergeStrategy::Default,
        sign,
    )
}

/// Check if a commit is a valid squash merge.
///
/// A valid squash merge:
/// - Has exactly one parent
/// - That parent is the prior default branch HEAD
///
/// # Arguments
///
/// * `worktree` - Path to the worktree
/// * `commit` - The commit SHA to check
/// * `default_branch` - The default branch name
///
/// # Returns
///
/// `Ok(true)` if it's a valid squash merge, `Ok(false)` otherwise.
pub fn is_valid_squash_merge(
    worktree: &Path,
    commit: &Sha,
    default_branch: &str,
) -> GitResult<bool> {
    let parents = get_parents(worktree, commit.as_str())?;

    // Must have exactly one parent
    if parents.len() != 1 {
        return Ok(false);
    }

    // The parent should be on the default branch
    // We verify this by checking if the commit is an ancestor of the default branch HEAD
    let default_head = rev_parse(worktree, &format!("origin/{}", default_branch))?;
    let is_on_default = super::is_ancestor(worktree, commit, &default_head)?;

    Ok(is_on_default)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::git::worktree::worktree_for_stack;
    use crate::git::{GitConfig, run_git_stdout, run_git_sync};
    use crate::types::PrNumber;
    use std::time::Duration;
    use tempfile::TempDir;

    /// Create a minimal git repo with a main branch and initial commit.
    fn create_test_repo() -> (TempDir, GitConfig, Sha) {
        let temp_dir = TempDir::new().unwrap();
        let base_dir = temp_dir.path().to_path_buf();

        let config = GitConfig {
            base_dir: base_dir.clone(),
            owner: "test".to_string(),
            repo: "repo".to_string(),
            default_branch: "main".to_string(),
            worktree_max_age: Duration::from_secs(24 * 3600),
            sign_commits: false,
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

        // Get the initial commit SHA
        let initial_sha = run_git_stdout(&work_dir, &["rev-parse", "HEAD"]).unwrap();
        let initial_sha = Sha::parse(&initial_sha).unwrap();

        // Push to the bare repo
        run_git_sync(
            &work_dir,
            &["remote", "add", "origin", clone_dir.to_str().unwrap()],
        )
        .unwrap();
        run_git_sync(&work_dir, &["push", "-u", "origin", "HEAD:main"]).unwrap();

        // Update HEAD in the bare repo
        run_git_sync(&clone_dir, &["symbolic-ref", "HEAD", "refs/heads/main"]).unwrap();

        (temp_dir, config, initial_sha)
    }

    /// Create a branch with a file in the test repo.
    fn create_branch_with_file(
        config: &GitConfig,
        branch: &str,
        filename: &str,
        content: &str,
        base_branch: &str,
    ) -> Sha {
        let clone_dir = config.clone_dir();

        // Create a temporary worktree for making the branch
        let temp_work = clone_dir.parent().unwrap().join("temp_work");
        std::fs::create_dir_all(&temp_work).unwrap();
        run_git_sync(
            &clone_dir,
            &[
                "worktree",
                "add",
                "--detach",
                temp_work.to_str().unwrap(),
                &format!("refs/heads/{}", base_branch),
            ],
        )
        .unwrap();

        run_git_sync(&temp_work, &["config", "user.email", "test@test.com"]).unwrap();
        run_git_sync(&temp_work, &["config", "user.name", "Test"]).unwrap();

        // Create the file and commit
        std::fs::write(temp_work.join(filename), content).unwrap();
        run_git_sync(&temp_work, &["add", filename]).unwrap();
        run_git_sync(&temp_work, &["commit", "-m", &format!("Add {}", filename)]).unwrap();

        let sha = run_git_stdout(&temp_work, &["rev-parse", "HEAD"]).unwrap();

        // Push the new branch
        run_git_sync(
            &temp_work,
            &["push", "origin", &format!("HEAD:refs/heads/{}", branch)],
        )
        .unwrap();

        // Cleanup
        run_git_sync(
            &clone_dir,
            &["worktree", "remove", "--force", temp_work.to_str().unwrap()],
        )
        .unwrap();

        Sha::parse(&sha).unwrap()
    }

    #[test]
    fn prepare_descendant_merges_predecessor() {
        let (_temp_dir, config, _initial_sha) = create_test_repo();

        // Create predecessor branch (PR #123)
        let pred_sha =
            create_branch_with_file(&config, "pr-123", "pred.txt", "predecessor content", "main");

        // Create descendant branch (PR #124) from predecessor
        let _desc_sha = create_branch_with_file(
            &config,
            "pr-124",
            "desc.txt",
            "descendant content",
            "pr-123",
        );

        // Get a worktree for the stack
        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();

        // Prepare the descendant
        let result = prepare_descendant(&worktree, "pr-124", &pred_sha, false).unwrap();

        // Should complete without conflict (either Success or AlreadyUpToDate)
        assert!(
            result.is_ok(),
            "Expected merge to succeed, got {:?}",
            result
        );

        // The descendant should now have both files
        assert!(worktree.join("pred.txt").exists());
        assert!(worktree.join("desc.txt").exists());
    }

    #[test]
    fn prepare_descendant_already_up_to_date() {
        let (_temp_dir, config, _initial_sha) = create_test_repo();

        // Create predecessor branch
        let pred_sha =
            create_branch_with_file(&config, "pr-123", "pred.txt", "predecessor content", "main");

        // Create descendant from predecessor (already has predecessor content)
        let _desc_sha = create_branch_with_file(
            &config,
            "pr-124",
            "desc.txt",
            "descendant content",
            "pr-123",
        );

        // Get a worktree for the stack
        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();

        // Prepare the descendant
        let result1 = prepare_descendant(&worktree, "pr-124", &pred_sha, false).unwrap();
        // Since pr-124 was created from pr-123, it might be AlreadyUpToDate
        assert!(
            result1.is_ok(),
            "Expected merge to succeed, got {:?}",
            result1
        );

        // Push the prepared state
        run_git_sync(
            &worktree,
            &["push", "origin", "HEAD:refs/heads/pr-124", "--force"],
        )
        .unwrap();

        // Create a new worktree and prepare again - should be up to date
        let worktree2 = worktree_for_stack(&config, PrNumber(124)).unwrap();
        let result2 = prepare_descendant(&worktree2, "pr-124", &pred_sha, false).unwrap();

        assert!(matches!(result2, MergeResult::AlreadyUpToDate));
    }

    #[test]
    fn reconcile_descendant_incorporates_squash() {
        let (_temp_dir, config, initial_sha) = create_test_repo();

        // Create predecessor branch
        let pred_sha =
            create_branch_with_file(&config, "pr-123", "pred.txt", "predecessor content", "main");

        // Create descendant branch from predecessor
        let _desc_sha = create_branch_with_file(
            &config,
            "pr-124",
            "desc.txt",
            "descendant content",
            "pr-123",
        );

        // Simulate squash-merge of predecessor to main
        // (In real code, GitHub API does this; here we do it manually)
        let clone_dir = config.clone_dir();
        let temp_work = clone_dir.parent().unwrap().join("temp_squash");
        std::fs::create_dir_all(&temp_work).unwrap();
        run_git_sync(
            &clone_dir,
            &[
                "worktree",
                "add",
                "--detach",
                temp_work.to_str().unwrap(),
                "refs/heads/main",
            ],
        )
        .unwrap();
        run_git_sync(&temp_work, &["config", "user.email", "test@test.com"]).unwrap();
        run_git_sync(&temp_work, &["config", "user.name", "Test"]).unwrap();

        // Squash merge (simulate what GitHub does)
        run_git_sync(&temp_work, &["merge", "--squash", &pred_sha.as_str()]).unwrap();
        run_git_sync(&temp_work, &["commit", "-m", "Squash: Add pred.txt"]).unwrap();

        let squash_sha_str = run_git_stdout(&temp_work, &["rev-parse", "HEAD"]).unwrap();
        let squash_sha = Sha::parse(&squash_sha_str).unwrap();

        run_git_sync(&temp_work, &["push", "origin", "HEAD:refs/heads/main"]).unwrap();
        run_git_sync(
            &clone_dir,
            &["worktree", "remove", "--force", temp_work.to_str().unwrap()],
        )
        .unwrap();

        // First, prepare the descendant (normally done before squash)
        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
        let prep_result = prepare_descendant(&worktree, "pr-124", &pred_sha, false).unwrap();
        assert!(
            prep_result.is_ok(),
            "Expected prepare to succeed, got {:?}",
            prep_result
        );
        run_git_sync(
            &worktree,
            &["push", "origin", "HEAD:refs/heads/pr-124", "--force"],
        )
        .unwrap();

        // Now reconcile the descendant
        let result = reconcile_descendant(&worktree, "pr-124", &squash_sha, false).unwrap();

        assert!(
            result.is_ok(),
            "Expected reconcile to succeed, got {:?}",
            result
        );

        // Verify the squash commit is now an ancestor of the descendant
        let head = rev_parse(&worktree, "HEAD").unwrap();
        assert!(super::super::is_ancestor(&worktree, &squash_sha, &head).unwrap());

        // Verify the initial commit is still an ancestor (main history preserved)
        assert!(super::super::is_ancestor(&worktree, &initial_sha, &head).unwrap());
    }

    #[test]
    fn reconcile_rejects_non_squash() {
        let (_temp_dir, config, _initial_sha) = create_test_repo();

        // Create predecessor branch
        let pred_sha =
            create_branch_with_file(&config, "pr-123", "pred.txt", "predecessor content", "main");

        // Create descendant branch
        let _desc_sha = create_branch_with_file(
            &config,
            "pr-124",
            "desc.txt",
            "descendant content",
            "pr-123",
        );

        // Create a merge commit (not a squash) on main
        let clone_dir = config.clone_dir();
        let temp_work = clone_dir.parent().unwrap().join("temp_merge");
        std::fs::create_dir_all(&temp_work).unwrap();
        run_git_sync(
            &clone_dir,
            &[
                "worktree",
                "add",
                "--detach",
                temp_work.to_str().unwrap(),
                "refs/heads/main",
            ],
        )
        .unwrap();
        run_git_sync(&temp_work, &["config", "user.email", "test@test.com"]).unwrap();
        run_git_sync(&temp_work, &["config", "user.name", "Test"]).unwrap();

        // Regular merge (two parents) - use --no-ff to force a merge commit even if fast-forward is possible
        run_git_sync(
            &temp_work,
            &[
                "merge",
                "--no-ff",
                &pred_sha.as_str(),
                "-m",
                "Merge: Add pred.txt",
            ],
        )
        .unwrap();

        let merge_sha_str = run_git_stdout(&temp_work, &["rev-parse", "HEAD"]).unwrap();
        let merge_sha = Sha::parse(&merge_sha_str).unwrap();

        run_git_sync(&temp_work, &["push", "origin", "HEAD:refs/heads/main"]).unwrap();
        run_git_sync(
            &clone_dir,
            &["worktree", "remove", "--force", temp_work.to_str().unwrap()],
        )
        .unwrap();

        // Try to reconcile with the merge commit
        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
        prepare_descendant(&worktree, "pr-124", &pred_sha, false).unwrap();
        run_git_sync(
            &worktree,
            &["push", "origin", "HEAD:refs/heads/pr-124", "--force"],
        )
        .unwrap();

        let result = reconcile_descendant(&worktree, "pr-124", &merge_sha, false);

        // Should fail because it's not a squash merge
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            GitError::CommandFailed { stderr, .. } => {
                assert!(stderr.contains("2 parents"));
            }
            _ => panic!("Expected CommandFailed error"),
        }
    }

    #[test]
    fn catch_up_descendant_merges_new_main_commits() {
        let (_temp_dir, config, _initial_sha) = create_test_repo();

        // Create a branch
        let _branch_sha =
            create_branch_with_file(&config, "pr-123", "feature.txt", "feature content", "main");

        // Add a new commit to main (simulating another PR landing)
        let _new_main_sha =
            create_branch_with_file(&config, "main", "other.txt", "other content", "main");

        // Get worktree and catch up
        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
        let result = catch_up_descendant(&worktree, "pr-123", "main", false).unwrap();

        assert!(result.is_success());

        // Both files should exist
        assert!(worktree.join("feature.txt").exists());
        assert!(worktree.join("other.txt").exists());
    }

    #[test]
    fn catch_up_already_up_to_date() {
        let (_temp_dir, config, _initial_sha) = create_test_repo();

        // Create a branch from main (no new commits on main since)
        let _branch_sha =
            create_branch_with_file(&config, "pr-123", "feature.txt", "feature content", "main");

        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
        let result = catch_up_descendant(&worktree, "pr-123", "main", false).unwrap();

        assert!(matches!(result, MergeResult::AlreadyUpToDate));
    }
}
