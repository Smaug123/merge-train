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
//! Note: Do NOT merge main during preparation. The $SQUASH_SHA^ ordering prevents
//! lost commits. See "Why merging $SQUASH_SHA^ is essential" in DESIGN.md.

use std::path::Path;

use crate::effects::git::MergeStrategy;
use crate::types::Sha;

use super::{
    CommitIdentity, GitError, GitResult, MergeResult, checkout_detached, fetch, get_parents,
    git_command, git_commit_command, rev_parse, run_git_stdout, run_git_sync,
};

/// Result of preparing a descendant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrepareResult {
    /// The result of merging the predecessor into the descendant.
    pub merge: MergeResult,

    /// The exact predecessor commit that was merged (resolved from the PR ref
    /// at fetch time, then pinned). [`reconcile_descendant`] requires this SHA
    /// to (a) equal the head the PR ref says was actually squash-merged, and
    /// (b) be an ancestor of the descendant, before it will perform the
    /// irreversible ours-merge. (a) is what detects a force-push between
    /// preparation and squash; an ancestry check alone cannot, because this
    /// pre-force-push head IS an ancestor of the prepared descendant.
    pub predecessor_head: Sha,
}

/// Prepare a descendant by merging the predecessor's head into it.
///
/// This is called BEFORE the predecessor is squash-merged. It ensures the
/// descendant has all of the predecessor's final content.
///
/// Note: This function does NOT merge main. That happens during reconciliation
/// after the squash, using `$SQUASH_SHA^`. Merging main here would cause lost
/// commits.
///
/// # Arguments
///
/// * `worktree` - Path to the stack's worktree
/// * `descendant_branch` - The descendant's branch name (e.g., "feature-2")
/// * `predecessor_pr` - The predecessor's PR number (used to fetch via refs/pull/<n>/head)
/// * `identity` - Identity for the merge commit (author/committer, optional signing key)
///
/// # Returns
///
/// The merge result plus the pinned predecessor head SHA that was merged.
///
/// # Why PR refs?
///
/// Fetching by raw SHA is unreliable because:
/// - `uploadpack.allowReachableSHA1InWant` may be disabled on the server
/// - After branch deletion, the SHA may not be directly fetchable
///
/// GitHub's PR refs (`refs/pull/<n>/head`) are:
/// - Maintained even after the PR branch is deleted
/// - Reliable for fork-based PRs
/// - Always fetchable via standard git protocols
pub fn prepare_descendant(
    worktree: &Path,
    descendant_branch: &str,
    predecessor_pr: u64,
    identity: &CommitIdentity,
) -> GitResult<PrepareResult> {
    // Construct the PR ref for the predecessor.
    // GitHub maintains refs/pull/<n>/head even after the PR branch is deleted.
    let pr_ref = format!("refs/pull/{}/head", predecessor_pr);
    let local_ref = format!("refs/remotes/origin/pr/{}", predecessor_pr);

    // Fetch the descendant branch and predecessor via PR ref.
    // Format: "+refs/pull/<n>/head:refs/remotes/origin/pr/<n>" creates a local
    // tracking ref that we can merge from.
    // The "+" prefix forces the update even if the PR was force-pushed (rebased).
    // Without "+", a force-pushed PR would cause fetch to fail with "non-fast-forward".
    // Use "--" to prevent branch names starting with "-" from being interpreted as flags.
    let fetch_refspec = format!("+{}:{}", pr_ref, local_ref);
    run_git_sync(
        worktree,
        &["fetch", "origin", "--", descendant_branch, &fetch_refspec],
    )
    .map_err(|e| {
        // String-matching git's output is only sound because git_command pins
        // LC_ALL=C; under other locales this message is translated.
        if let GitError::CommandFailed { stderr, .. } = &e
            && stderr.contains("couldn't find remote ref")
        {
            // Determine which ref failed
            if stderr.contains(&pr_ref) {
                return GitError::FetchFailed {
                    refspec: pr_ref.clone(),
                    details: format!(
                        "Failed to fetch predecessor PR #{}. \
                             This ref may have been garbage-collected on GitHub Enterprise Server.",
                        predecessor_pr
                    ),
                };
            }
            return GitError::FetchFailed {
                refspec: descendant_branch.to_string(),
                details: stderr.clone(),
            };
        }
        e
    })?;

    // Pin the exact predecessor commit: merging the moving ref would race with
    // force-pushes between fetch and merge, and reconciliation later requires
    // exactly this SHA to be an ancestor of the descendant.
    let predecessor_head = rev_parse(worktree, &local_ref)?;

    // Checkout the descendant branch in detached HEAD mode
    let remote_branch = format!("origin/{}", descendant_branch);
    checkout_detached(worktree, &remote_branch)?;

    let message = format!(
        "Merge predecessor into {} (merge train preparation)",
        descendant_branch
    );

    let merge = merge_with_message(
        worktree,
        predecessor_head.as_str(),
        &message,
        MergeStrategy::Default,
        identity,
    )?;

    Ok(PrepareResult {
        merge,
        predecessor_head,
    })
}

/// Inputs to [`reconcile_descendant`].
///
/// Named fields rather than positional arguments: three of these are `Sha`s
/// guarding an irreversible ours-merge, and a transposition at a positional
/// call site would compile silently.
#[derive(Debug, Clone, Copy)]
pub struct ReconcileRequest<'a> {
    /// The descendant's branch name (e.g. "feature-2").
    pub descendant_branch: &'a str,
    /// The predecessor's PR number; its PR ref names the head that was
    /// actually squash-merged.
    pub predecessor_pr: u64,
    /// The squash commit on the default branch.
    pub squash_sha: &'a Sha,
    /// The expected parent of the squash commit (default branch head at
    /// squash time).
    pub expected_squash_parent: &'a Sha,
    /// The predecessor head that preparation merged into the descendant
    /// ([`PrepareResult::predecessor_head`]).
    pub predecessor_pre_squash_head: &'a Sha,
    /// The repository default branch (e.g. "main").
    pub default_branch: &'a str,
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
/// * `expected_squash_parent` - The main HEAD captured just before the squash
/// * `predecessor_pre_squash_head` - The predecessor head that was merged during
///   preparation ([`PrepareResult::predecessor_head`]); must be an ancestor of the
///   descendant's current head
/// * `default_branch` - The default branch name (e.g., "main")
/// * `identity` - Identity for the merge commits (author/committer, optional signing key)
///
/// # Returns
///
/// The result of the merge operation (Success if both merges succeeded).
///
/// # Errors
///
/// Returns an error if:
/// - The descendant does not contain `predecessor_pre_squash_head` (preparation
///   missing or stale, e.g. the predecessor was force-pushed after preparation)
/// - The squash commit has no parent (shouldn't happen for a valid squash)
/// - Either merge results in a conflict
/// - The squash commit has multiple parents (not a squash merge)
/// - The squash parent is not on the default branch (rebase/fast-forward merge detected)
/// - The squash parent does not match the expected parent (multi-commit rebase/FF detected)
/// - The head named by the predecessor's PR ref (the head that was actually
///   squash-merged) differs from `predecessor_pre_squash_head`, or the latter
///   is not an ancestor of the descendant (force-push race; the ours-merge
///   would record content as merged that the descendant does not contain)
pub fn reconcile_descendant(
    worktree: &Path,
    req: &ReconcileRequest<'_>,
    identity: &CommitIdentity,
) -> GitResult<MergeResult> {
    let &ReconcileRequest {
        descendant_branch,
        predecessor_pr,
        squash_sha,
        expected_squash_parent,
        predecessor_pre_squash_head,
        default_branch,
    } = req;
    // Fetch the default branch, the descendant branch, AND the predecessor's
    // PR ref. We fetch the default branch (not the squash SHA) because:
    // 1. The squash commit is on the default branch, so fetching main gets it
    // 2. Some servers disallow fetching by raw SHA (uploadpack.allowReachableSHA1InWant)
    // The descendant branch must be fetched because after a push, our local
    // remote-tracking ref (origin/<branch>) may be stale. Without fetching,
    // we'd check out an old commit and lose the prepared merge state.
    let pr_ref = format!("refs/pull/{}/head", predecessor_pr);
    let pr_local_ref = format!("refs/remotes/origin/pr/{}", predecessor_pr);
    let pr_refspec = format!("+{}:{}", pr_ref, pr_local_ref);
    fetch(worktree, &[default_branch, descendant_branch, &pr_refspec])?;

    let remote_branch = format!("origin/{}", descendant_branch);
    let descendant_head = rev_parse(worktree, &remote_branch)?;

    // The PR ref is frozen once the PR is merged, so it names the head GitHub
    // actually squash-merged. An ancestry check on the caller-supplied
    // prepared head alone cannot detect a force-push between preparation and
    // squash: the prepared head IS an ancestor of the descendant, but the
    // squashed content is not.
    let squashed_head = rev_parse(worktree, &pr_local_ref)?;
    if squashed_head != *predecessor_pre_squash_head {
        return Err(GitError::PredecessorHeadChanged {
            predecessor_pr,
            prepared: predecessor_pre_squash_head.clone(),
            squashed: squashed_head,
        });
    }

    // The ours-merge below records `squash_sha` as an ancestor WITHOUT taking
    // its tree. That is only sound if the descendant already contains the
    // predecessor's pre-squash content (established by prepare_descendant). A
    // force-push to the predecessor between preparation and squash breaks
    // this; proceeding would lose the squashed content with no later phase
    // able to restore it.
    let predecessor_known = git_command(worktree)
        .args([
            "cat-file",
            "-e",
            &format!("{}^{{commit}}", predecessor_pre_squash_head),
        ])
        .output()?
        .status
        .success();
    if !predecessor_known
        || !super::is_ancestor(worktree, predecessor_pre_squash_head, &descendant_head)?
    {
        return Err(GitError::PreparationNotCompleted {
            descendant_branch: descendant_branch.to_string(),
            predecessor_head: predecessor_pre_squash_head.clone(),
            descendant_head,
        });
    }

    // Get the parent of the squash commit (main state just before squash)
    let parents = get_parents(worktree, squash_sha.as_str())?;

    // Validate this is a squash merge (single parent)
    if parents.is_empty() {
        return Err(GitError::InvalidSquashCommit {
            squash_sha: squash_sha.clone(),
            details: "commit has no parents, expected 1 for squash merge".to_string(),
        });
    }
    if parents.len() > 1 {
        return Err(GitError::InvalidSquashCommit {
            squash_sha: squash_sha.clone(),
            details: format!(
                "commit has {} parents, expected 1 for squash merge; \
                 a merge commit or rebase may have been used instead of squash",
                parents.len()
            ),
        });
    }

    let squash_parent = &parents[0];

    // Validate: the squash commit must be on the default branch.
    // This catches cases where a wrong SHA (off-main) is passed.
    // (`merge-base --is-ancestor X X` exits 0, so equality needs no extra arm.)
    let default_head = rev_parse(worktree, &format!("origin/{}", default_branch))?;
    if !super::is_ancestor(worktree, squash_sha, &default_head)? {
        return Err(GitError::InvalidSquashCommit {
            squash_sha: squash_sha.clone(),
            details: format!(
                "commit is not on the {} branch history; \
                 the squash SHA must be a commit on the default branch",
                default_branch
            ),
        });
    }

    // Validate: the parent must be on the default branch history.
    // For a valid squash merge: parent is the prior main HEAD, which is on main.
    // For a multi-commit rebase: parent is the previous rebased commit,
    // which is NOT on the main branch history.
    if !super::is_ancestor(worktree, squash_parent, &default_head)? {
        return Err(GitError::InvalidSquashCommit {
            squash_sha: squash_sha.clone(),
            details: format!(
                "parent {} is not on the {} branch history; \
                 this indicates a rebase or fast-forward merge was used instead of squash \
                 (the merge train only supports squash merges)",
                squash_parent, default_branch
            ),
        });
    }

    // Validate: the parent must be exactly the expected parent (prior main HEAD).
    // This catches multi-commit rebase/FF merges that land on main:
    // After a multi-commit FF, both commits are on main, so the ancestor check passes.
    // But the parent is NOT the prior main HEAD - it's another FF'd commit.
    if squash_parent != expected_squash_parent {
        return Err(GitError::InvalidSquashCommit {
            squash_sha: squash_sha.clone(),
            details: format!(
                "parent {} does not match expected parent {}; \
                 this indicates a multi-commit rebase or fast-forward merge was used \
                 instead of squash (the merge train only supports squash merges)",
                squash_parent, expected_squash_parent
            ),
        });
    }

    // Checkout the descendant branch in detached HEAD mode
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
        identity,
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
        identity,
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
/// * `identity` - Identity for the merge commit (author/committer, optional signing key)
///
/// # Returns
///
/// The result of the merge operation.
pub fn catch_up_descendant(
    worktree: &Path,
    descendant_branch: &str,
    default_branch: &str,
    identity: &CommitIdentity,
) -> GitResult<MergeResult> {
    // Fetch the latest default branch AND the descendant branch.
    // The descendant branch must be fetched because after a push, our local
    // remote-tracking ref (origin/<branch>) may be stale. Without fetching,
    // we'd check out an old commit and lose the reconciled merge state.
    fetch(worktree, &[default_branch, descendant_branch])?;

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
        identity,
    )
}

/// Perform a merge with a specific message and strategy.
///
/// Uses the provided identity for the commit author/committer. If
/// `identity.signing_key` is set, the merge commit will be GPG signed.
///
/// Classification is structural (locale-independent): a no-op merge is
/// detected by HEAD not moving, a conflict by unmerged index entries.
fn merge_with_message(
    worktree: &Path,
    target: &str,
    message: &str,
    strategy: MergeStrategy,
    identity: &CommitIdentity,
) -> GitResult<MergeResult> {
    let head_before = rev_parse(worktree, "HEAD")?;

    let mut args = vec!["merge", "--no-edit", "-m", message];

    if let Some(strategy_arg) = strategy.as_git_arg() {
        args.push("-s");
        args.push(strategy_arg);
    }

    if identity.signing_key.is_some() {
        args.push("-S");
    }

    args.push(target);

    let output = git_commit_command(worktree, identity)
        .args(&args)
        .output()?;

    if output.status.success() {
        let head_after = rev_parse(worktree, "HEAD")?;
        if head_after == head_before {
            return Ok(MergeResult::AlreadyUpToDate);
        }
        return Ok(MergeResult::Success {
            commit_sha: head_after,
        });
    }

    // Unmerged index entries mean the merge started and hit conflicts. If
    // `ls-files -u` itself fails, fall through to the hard error: an
    // unclassifiable merge failure must halt the cascade.
    let unmerged = run_git_stdout(worktree, &["ls-files", "-u"]).unwrap_or_default();
    if !unmerged.is_empty() {
        let conflicting_files = get_conflicting_files(worktree)?;
        return Ok(MergeResult::Conflict { conflicting_files });
    }

    Err(GitError::CommandFailed {
        command: format!("git {}", args.join(" ")),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
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
/// * `identity` - Identity for the merge commit (author/committer, optional signing key)
///
/// # Returns
///
/// The result of the merge operation.
pub fn update_root_for_behind(
    worktree: &Path,
    branch: &str,
    default_branch: &str,
    identity: &CommitIdentity,
) -> GitResult<MergeResult> {
    // Fetch both the default branch AND the branch we're updating.
    // The branch must be fetched because after a push, our local
    // remote-tracking ref (origin/<branch>) may be stale. Without fetching,
    // we'd check out an old commit and lose newer commits when we later push.
    fetch(worktree, &[default_branch, branch])?;

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
        identity,
    )
}

/// Check if a commit is a valid squash merge.
///
/// A valid squash merge:
/// - Has exactly one parent
/// - That parent is on the default branch history (i.e., is an ancestor of the
///   current default branch HEAD)
///
/// This rejects:
/// - Merge commits (two parents)
/// - Multi-commit rebase merges (parent is the previous rebased commit, not on main)
/// - Multi-commit fast-forward merges (same issue as rebase)
///
/// Single-commit rebase/fast-forward are indistinguishable from squash and accepted.
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
    // Fetch the default branch first to ensure we have the latest state
    fetch(worktree, &[default_branch])?;

    let parents = get_parents(worktree, commit.as_str())?;

    // Must have exactly one parent
    if parents.len() != 1 {
        return Ok(false);
    }

    let parent = &parents[0];

    let default_head = rev_parse(worktree, &format!("origin/{}", default_branch))?;

    // The COMMIT must be on the default branch.
    // This catches cases where a wrong SHA (off-main) is passed.
    // (`merge-base --is-ancestor X X` exits 0, so equality needs no extra arm.)
    if !super::is_ancestor(worktree, commit, &default_head)? {
        return Ok(false);
    }

    // The PARENT must be on the default branch.
    // For a valid squash: the parent is the prior main HEAD, which is on main.
    // For a multi-commit rebase: the parent is the previous rebased commit,
    // which is NOT on the main branch history.
    super::is_ancestor(worktree, parent, &default_head)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::git::test_support::{
        create_branch_with_file, create_pr_ref, create_test_repo_with_origin, test_identity,
    };
    use crate::git::worktree::worktree_for_stack;
    use crate::git::{GitConfig, run_git_stdout, run_git_sync};
    use crate::types::PrNumber;

    #[test]
    fn prepare_descendant_merges_predecessor() {
        let (_temp_dir, config, _initial_sha) = create_test_repo_with_origin();

        // Create predecessor branch (PR #123)
        let pred_sha =
            create_branch_with_file(&config, "pr-123", "pred.txt", "predecessor content", "main");
        // Create PR ref for predecessor
        create_pr_ref(&config, 123, &pred_sha);

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

        // Prepare the descendant using PR number
        let identity = test_identity();
        let result = prepare_descendant(&worktree, "pr-124", 123, &identity).unwrap();

        // Should complete without conflict (either Success or AlreadyUpToDate)
        assert!(
            matches!(
                result.merge,
                MergeResult::Success { .. } | MergeResult::AlreadyUpToDate
            ),
            "Expected merge to succeed, got {:?}",
            result.merge
        );

        // The pinned predecessor head must be the SHA the PR ref pointed at
        assert_eq!(result.predecessor_head, pred_sha);

        // The descendant should now have both files
        assert!(worktree.join("pred.txt").exists());
        assert!(worktree.join("desc.txt").exists());
    }

    #[test]
    fn prepare_descendant_already_up_to_date() {
        let (_temp_dir, config, _initial_sha) = create_test_repo_with_origin();

        // Create predecessor branch
        let pred_sha =
            create_branch_with_file(&config, "pr-123", "pred.txt", "predecessor content", "main");
        create_pr_ref(&config, 123, &pred_sha);

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

        // Prepare the descendant using PR number
        let identity = test_identity();
        let result1 = prepare_descendant(&worktree, "pr-124", 123, &identity).unwrap();
        // Since pr-124 was created from pr-123, it might be AlreadyUpToDate
        assert!(
            matches!(
                result1.merge,
                MergeResult::Success { .. } | MergeResult::AlreadyUpToDate
            ),
            "Expected merge to succeed, got {:?}",
            result1.merge
        );

        // Push the prepared state
        run_git_sync(
            &worktree,
            &["push", "origin", "HEAD:refs/heads/pr-124", "--force"],
        )
        .unwrap();

        // Create a new worktree and prepare again - should be up to date
        let worktree2 = worktree_for_stack(&config, PrNumber(124)).unwrap();
        let result2 = prepare_descendant(&worktree2, "pr-124", 123, &identity).unwrap();

        assert!(matches!(result2.merge, MergeResult::AlreadyUpToDate));
    }

    #[test]
    fn reconcile_descendant_incorporates_squash() {
        let (_temp_dir, config, initial_sha) = create_test_repo_with_origin();

        // Create predecessor branch
        let pred_sha =
            create_branch_with_file(&config, "pr-123", "pred.txt", "predecessor content", "main");
        create_pr_ref(&config, 123, &pred_sha);

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

        // Capture main HEAD before squash - this is what the squash parent should be
        let main_before_squash =
            run_git_stdout(&clone_dir, &["rev-parse", "refs/heads/main"]).unwrap();
        let main_before_squash = Sha::parse(&main_before_squash).unwrap();

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
        run_git_sync(&temp_work, &["merge", "--squash", pred_sha.as_str()]).unwrap();
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
        let identity = test_identity();
        let prep_result = prepare_descendant(&worktree, "pr-124", 123, &identity).unwrap();
        assert!(
            matches!(
                prep_result.merge,
                MergeResult::Success { .. } | MergeResult::AlreadyUpToDate
            ),
            "Expected prepare to succeed, got {:?}",
            prep_result.merge
        );
        run_git_sync(
            &worktree,
            &["push", "origin", "HEAD:refs/heads/pr-124", "--force"],
        )
        .unwrap();

        // Now reconcile the descendant
        let result = reconcile_descendant(
            &worktree,
            &ReconcileRequest {
                descendant_branch: "pr-124",
                predecessor_pr: 123,
                squash_sha: &squash_sha,
                expected_squash_parent: &main_before_squash,
                predecessor_pre_squash_head: &prep_result.predecessor_head,
                default_branch: "main",
            },
            &identity,
        )
        .unwrap();

        assert!(
            matches!(
                result,
                MergeResult::Success { .. } | MergeResult::AlreadyUpToDate
            ),
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
        let (_temp_dir, config, _initial_sha) = create_test_repo_with_origin();

        // Create predecessor branch
        let pred_sha =
            create_branch_with_file(&config, "pr-123", "pred.txt", "predecessor content", "main");
        create_pr_ref(&config, 123, &pred_sha);

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

        // Capture main HEAD before merge
        let main_before_merge =
            run_git_stdout(&clone_dir, &["rev-parse", "refs/heads/main"]).unwrap();
        let main_before_merge = Sha::parse(&main_before_merge).unwrap();

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
                pred_sha.as_str(),
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
        let identity = test_identity();
        prepare_descendant(&worktree, "pr-124", 123, &identity).unwrap();
        run_git_sync(
            &worktree,
            &["push", "origin", "HEAD:refs/heads/pr-124", "--force"],
        )
        .unwrap();

        let result = reconcile_descendant(
            &worktree,
            &ReconcileRequest {
                descendant_branch: "pr-124",
                predecessor_pr: 123,
                squash_sha: &merge_sha,
                expected_squash_parent: &main_before_merge,
                predecessor_pre_squash_head: &pred_sha,
                default_branch: "main",
            },
            &identity,
        );

        // Should fail because it's not a squash merge
        assert!(result.is_err());
        let err = result.unwrap_err();
        let err_str = err.to_string();
        assert!(
            err_str.contains("2 parents"),
            "Error should mention parent count, got: {}",
            err_str
        );
    }

    #[test]
    fn catch_up_descendant_merges_new_main_commits() {
        let (_temp_dir, config, _initial_sha) = create_test_repo_with_origin();

        // Create a branch
        let _branch_sha =
            create_branch_with_file(&config, "pr-123", "feature.txt", "feature content", "main");

        // Add a new commit to main (simulating another PR landing)
        let _new_main_sha =
            create_branch_with_file(&config, "main", "other.txt", "other content", "main");

        // Get worktree and catch up
        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
        let identity = test_identity();
        let result = catch_up_descendant(&worktree, "pr-123", "main", &identity).unwrap();

        assert!(matches!(result, MergeResult::Success { .. }));

        // Both files should exist
        assert!(worktree.join("feature.txt").exists());
        assert!(worktree.join("other.txt").exists());
    }

    #[test]
    fn catch_up_already_up_to_date() {
        let (_temp_dir, config, _initial_sha) = create_test_repo_with_origin();

        // Create a branch from main (no new commits on main since)
        let _branch_sha =
            create_branch_with_file(&config, "pr-123", "feature.txt", "feature content", "main");

        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
        let identity = test_identity();
        let result = catch_up_descendant(&worktree, "pr-123", "main", &identity).unwrap();

        assert!(matches!(result, MergeResult::AlreadyUpToDate));
    }

    /// Test that reconcile_descendant fetches the descendant branch before checkout.
    ///
    /// After prepare_descendant pushes, a fresh worktree has stale origin/<branch> refs.
    /// reconcile_descendant must fetch the descendant branch to get the prepared state,
    /// otherwise it checks out the old (unprepared) commit and drops the merge.
    #[test]
    fn reconcile_descendant_fetches_descendant_before_checkout() {
        let (_temp_dir, config, _initial_sha) = create_test_repo_with_origin();

        // Create predecessor and descendant branches
        let pred_sha =
            create_branch_with_file(&config, "pr-123", "pred.txt", "predecessor content", "main");
        create_pr_ref(&config, 123, &pred_sha);
        let _desc_sha = create_branch_with_file(
            &config,
            "pr-124",
            "desc.txt",
            "descendant content",
            "pr-123",
        );

        // Prepare descendant in worktree 1
        let worktree1 = worktree_for_stack(&config, PrNumber(123)).unwrap();
        let identity = test_identity();
        let prep_result = prepare_descendant(&worktree1, "pr-124", 123, &identity).unwrap();
        assert!(
            matches!(
                prep_result.merge,
                MergeResult::Success { .. } | MergeResult::AlreadyUpToDate
            ),
            "Prepare should succeed"
        );

        // Push the prepared state
        run_git_sync(
            &worktree1,
            &["push", "origin", "HEAD:refs/heads/pr-124", "--force"],
        )
        .unwrap();

        // Get the prepared HEAD SHA
        let prepared_sha = run_git_stdout(&worktree1, &["rev-parse", "HEAD"]).unwrap();

        // Squash merge predecessor to main
        let clone_dir = config.clone_dir();

        // Capture main HEAD before squash
        let main_before_squash =
            run_git_stdout(&clone_dir, &["rev-parse", "refs/heads/main"]).unwrap();
        let main_before_squash = Sha::parse(&main_before_squash).unwrap();

        let squash_sha = {
            let temp_work = clone_dir.parent().unwrap().join("temp_squash_test");
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
            run_git_sync(&temp_work, &["merge", "--squash", pred_sha.as_str()]).unwrap();
            run_git_sync(&temp_work, &["commit", "-m", "Squash merge"]).unwrap();
            let sha = run_git_stdout(&temp_work, &["rev-parse", "HEAD"]).unwrap();
            run_git_sync(&temp_work, &["push", "origin", "HEAD:refs/heads/main"]).unwrap();
            run_git_sync(
                &clone_dir,
                &["worktree", "remove", "--force", temp_work.to_str().unwrap()],
            )
            .unwrap();
            Sha::parse(&sha).unwrap()
        };

        // Now get a FRESH worktree (worktree 2) - this simulates recovery or continuation.
        // The fresh worktree has stale origin/pr-124 that doesn't include the prepared merge.
        let worktree2 = worktree_for_stack(&config, PrNumber(200)).unwrap();

        // Reconcile should fetch pr-124 and use the PREPARED state, not the old state
        let result = reconcile_descendant(
            &worktree2,
            &ReconcileRequest {
                descendant_branch: "pr-124",
                predecessor_pr: 123,
                squash_sha: &squash_sha,
                expected_squash_parent: &main_before_squash,
                predecessor_pre_squash_head: &prep_result.predecessor_head,
                default_branch: "main",
            },
            &identity,
        )
        .unwrap();
        assert!(
            matches!(
                result,
                MergeResult::Success { .. } | MergeResult::AlreadyUpToDate
            ),
            "Reconcile should succeed, got {:?}",
            result
        );

        // Verify we reconciled from the prepared state (prepared SHA should be an ancestor)
        let head_sha = run_git_stdout(&worktree2, &["rev-parse", "HEAD"]).unwrap();
        let is_ancestor = run_git_sync(
            &worktree2,
            &["merge-base", "--is-ancestor", &prepared_sha, &head_sha],
        );
        assert!(
            is_ancestor.is_ok(),
            "The prepared state should be an ancestor of the reconciled state"
        );
    }

    /// Test that catch_up_descendant fetches the descendant branch before checkout.
    ///
    /// After reconcile pushes, the local refs are stale. catch_up must fetch first.
    #[test]
    fn catch_up_descendant_fetches_descendant_before_checkout() {
        let (_temp_dir, config, _initial_sha) = create_test_repo_with_origin();

        // Create a branch
        let _branch_sha =
            create_branch_with_file(&config, "pr-123", "feature.txt", "feature content", "main");

        // Get worktree 1, make a change, and push
        let worktree1 = worktree_for_stack(&config, PrNumber(123)).unwrap();
        run_git_sync(&worktree1, &["config", "user.email", "test@test.com"]).unwrap();
        run_git_sync(&worktree1, &["config", "user.name", "Test"]).unwrap();
        run_git_sync(&worktree1, &["fetch", "origin", "pr-123"]).unwrap();
        run_git_sync(&worktree1, &["checkout", "--detach", "origin/pr-123"]).unwrap();

        // Add another file to simulate preparation work
        std::fs::write(worktree1.join("prepared.txt"), "prepared").unwrap();
        run_git_sync(&worktree1, &["add", "."]).unwrap();
        run_git_sync(&worktree1, &["commit", "-m", "Add prepared file"]).unwrap();
        let _prepared_sha = run_git_stdout(&worktree1, &["rev-parse", "HEAD"]).unwrap();

        // Push the prepared state
        run_git_sync(
            &worktree1,
            &["push", "origin", "HEAD:refs/heads/pr-123", "--force"],
        )
        .unwrap();

        // Add a commit to main
        create_branch_with_file(&config, "main", "main_update.txt", "main update", "main");

        // Get a FRESH worktree (worktree 2) - has stale origin/pr-123
        let worktree2 = worktree_for_stack(&config, PrNumber(200)).unwrap();

        // Catch up should fetch pr-123 and use the PUSHED state
        let identity = test_identity();
        let result = catch_up_descendant(&worktree2, "pr-123", "main", &identity).unwrap();
        assert!(
            matches!(
                result,
                MergeResult::Success { .. } | MergeResult::AlreadyUpToDate
            ),
            "Catch up should succeed"
        );

        // Verify the prepared file exists (was not lost due to stale ref)
        assert!(
            worktree2.join("prepared.txt").exists(),
            "The prepared file should exist - catch_up should use the pushed state"
        );
    }

    /// Test that is_valid_squash_merge accepts a legitimate squash merge.
    #[test]
    fn is_valid_squash_merge_accepts_squash() {
        let (_temp_dir, config, _initial_sha) = create_test_repo_with_origin();

        // Create a feature branch
        let branch_sha =
            create_branch_with_file(&config, "feature", "feature.txt", "feature content", "main");

        // Squash merge it
        let clone_dir = config.clone_dir();
        let temp_work = clone_dir.parent().unwrap().join("temp_squash_valid");
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
        run_git_sync(&temp_work, &["merge", "--squash", branch_sha.as_str()]).unwrap();
        run_git_sync(&temp_work, &["commit", "-m", "Squash merge feature"]).unwrap();

        let squash_sha = run_git_stdout(&temp_work, &["rev-parse", "HEAD"]).unwrap();
        let squash_sha = Sha::parse(&squash_sha).unwrap();

        run_git_sync(&temp_work, &["push", "origin", "HEAD:refs/heads/main"]).unwrap();
        run_git_sync(
            &clone_dir,
            &["worktree", "remove", "--force", temp_work.to_str().unwrap()],
        )
        .unwrap();

        // Validate from a worktree
        let worktree = worktree_for_stack(&config, PrNumber(999)).unwrap();
        let is_valid = is_valid_squash_merge(&worktree, &squash_sha, "main").unwrap();

        assert!(is_valid, "Squash merge should be accepted");
    }

    /// Test that is_valid_squash_merge rejects a merge commit (two parents).
    #[test]
    fn is_valid_squash_merge_rejects_merge_commit() {
        let (_temp_dir, config, _initial_sha) = create_test_repo_with_origin();

        // Create a feature branch
        let branch_sha =
            create_branch_with_file(&config, "feature", "feature.txt", "feature content", "main");

        // Create a divergence on main so merge is required
        create_branch_with_file(&config, "main", "main_change.txt", "main change", "main");

        // Do a real merge (not squash) - this creates two parents
        let clone_dir = config.clone_dir();
        let temp_work = clone_dir.parent().unwrap().join("temp_merge_commit");
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
        run_git_sync(
            &temp_work,
            &[
                "merge",
                "--no-ff",
                branch_sha.as_str(),
                "-m",
                "Merge commit",
            ],
        )
        .unwrap();

        let merge_sha = run_git_stdout(&temp_work, &["rev-parse", "HEAD"]).unwrap();
        let merge_sha = Sha::parse(&merge_sha).unwrap();

        run_git_sync(&temp_work, &["push", "origin", "HEAD:refs/heads/main"]).unwrap();
        run_git_sync(
            &clone_dir,
            &["worktree", "remove", "--force", temp_work.to_str().unwrap()],
        )
        .unwrap();

        // Validate from a worktree
        let worktree = worktree_for_stack(&config, PrNumber(999)).unwrap();
        let is_valid = is_valid_squash_merge(&worktree, &merge_sha, "main").unwrap();

        assert!(!is_valid, "Merge commit (two parents) should be rejected");
    }

    /// Test that is_valid_squash_merge rejects a multi-commit rebase merge.
    ///
    /// In a multi-commit rebase, the parent of the last rebased commit is
    /// the previous rebased commit, NOT the prior main HEAD.
    #[test]
    fn is_valid_squash_merge_rejects_multi_commit_rebase() {
        let (_temp_dir, config, _initial_sha) = create_test_repo_with_origin();
        let clone_dir = config.clone_dir();

        // Create a feature branch with TWO commits
        let temp_work = clone_dir.parent().unwrap().join("temp_feature");
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

        // First commit on feature
        std::fs::write(temp_work.join("file1.txt"), "content1").unwrap();
        run_git_sync(&temp_work, &["add", "."]).unwrap();
        run_git_sync(&temp_work, &["commit", "-m", "Feature commit 1"]).unwrap();

        // Second commit on feature
        std::fs::write(temp_work.join("file2.txt"), "content2").unwrap();
        run_git_sync(&temp_work, &["add", "."]).unwrap();
        run_git_sync(&temp_work, &["commit", "-m", "Feature commit 2"]).unwrap();

        run_git_sync(&temp_work, &["push", "origin", "HEAD:refs/heads/feature"]).unwrap();

        // Record the second commit SHA
        let second_commit_sha = run_git_stdout(&temp_work, &["rev-parse", "HEAD"]).unwrap();
        let second_commit_sha = Sha::parse(&second_commit_sha).unwrap();

        // The parent of the second commit is the first commit, NOT main
        let parents = run_git_stdout(&temp_work, &["rev-parse", "HEAD^"]).unwrap();
        let first_commit_parent = Sha::parse(&parents).unwrap();

        // Verify that the parent is NOT an ancestor of main
        // (it's on the feature branch, not on main)
        run_git_sync(&temp_work, &["fetch", "origin", "main"]).unwrap();
        let main_sha = run_git_stdout(&temp_work, &["rev-parse", "origin/main"]).unwrap();

        // The first commit should NOT be an ancestor of main (it's on feature only)
        let _is_ancestor_result = run_git_sync(
            &temp_work,
            &[
                "merge-base",
                "--is-ancestor",
                first_commit_parent.as_str(),
                &main_sha,
            ],
        );

        run_git_sync(
            &clone_dir,
            &["worktree", "remove", "--force", temp_work.to_str().unwrap()],
        )
        .unwrap();

        // Now check is_valid_squash_merge for the second commit
        // It has one parent, but that parent is the first commit, not main
        let worktree = worktree_for_stack(&config, PrNumber(999)).unwrap();
        run_git_sync(&worktree, &["fetch", "origin", "feature"]).unwrap();

        let is_valid = is_valid_squash_merge(&worktree, &second_commit_sha, "main").unwrap();

        assert!(
            !is_valid,
            "Multi-commit rebase (parent not on main) should be rejected"
        );
    }

    /// Test that is_valid_squash_merge accepts a single-commit rebase merge.
    ///
    /// For a single-commit branch, rebase and squash are indistinguishable:
    /// both result in a single commit on main whose parent is the prior main HEAD.
    /// This is expected behavior per DESIGN.md.
    #[test]
    fn is_valid_squash_merge_accepts_single_commit_rebase() {
        let (_temp_dir, config, _initial_sha) = create_test_repo_with_origin();
        let clone_dir = config.clone_dir();

        // Create a feature branch with ONE commit
        let temp_work = clone_dir.parent().unwrap().join("temp_single_rebase");
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

        // Single commit on feature
        std::fs::write(temp_work.join("feature.txt"), "feature content").unwrap();
        run_git_sync(&temp_work, &["add", "."]).unwrap();
        run_git_sync(&temp_work, &["commit", "-m", "Feature commit"]).unwrap();
        run_git_sync(&temp_work, &["push", "origin", "HEAD:refs/heads/feature"]).unwrap();

        // Record the main HEAD before rebase
        let main_head_before =
            run_git_stdout(&clone_dir, &["rev-parse", "refs/heads/main"]).unwrap();
        let main_head_before = Sha::parse(&main_head_before).unwrap();

        // Rebase onto main (trivial since it's already based on main, but let's be explicit)
        run_git_sync(&temp_work, &["rebase", "refs/heads/main"]).unwrap();

        // Fast-forward main to the rebased commit
        let rebased_sha = run_git_stdout(&temp_work, &["rev-parse", "HEAD"]).unwrap();
        run_git_sync(&clone_dir, &["update-ref", "refs/heads/main", &rebased_sha]).unwrap();
        let rebased_sha = Sha::parse(&rebased_sha).unwrap();

        run_git_sync(
            &clone_dir,
            &["worktree", "remove", "--force", temp_work.to_str().unwrap()],
        )
        .unwrap();

        // For a single-commit rebase, the parent is the prior main HEAD
        let worktree = worktree_for_stack(&config, PrNumber(999)).unwrap();
        run_git_sync(&worktree, &["fetch", "origin", "main"]).unwrap();

        // Verify the parent is the prior main HEAD (same as squash behavior)
        let parents = super::get_parents(&worktree, rebased_sha.as_str()).unwrap();
        assert_eq!(
            parents.len(),
            1,
            "Single-commit rebase should have one parent"
        );
        assert_eq!(
            parents[0], main_head_before,
            "Single-commit rebase parent should be prior main HEAD"
        );

        let is_valid = is_valid_squash_merge(&worktree, &rebased_sha, "main").unwrap();
        assert!(
            is_valid,
            "Single-commit rebase should be ACCEPTED (indistinguishable from squash)"
        );
    }

    /// Test that is_valid_squash_merge accepts a single-commit fast-forward merge.
    ///
    /// For a single-commit branch based directly on main, fast-forward merging
    /// produces the same result as squash: a single commit whose parent is the
    /// prior main HEAD. This is expected behavior per DESIGN.md.
    #[test]
    fn is_valid_squash_merge_accepts_single_commit_fast_forward() {
        let (_temp_dir, config, _initial_sha) = create_test_repo_with_origin();
        let clone_dir = config.clone_dir();

        // Record main HEAD before creating feature
        let main_head_before =
            run_git_stdout(&clone_dir, &["rev-parse", "refs/heads/main"]).unwrap();
        let main_head_before = Sha::parse(&main_head_before).unwrap();

        // Create a feature branch with ONE commit
        let temp_work = clone_dir.parent().unwrap().join("temp_single_ff");
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

        // Single commit on feature
        std::fs::write(temp_work.join("feature.txt"), "feature content").unwrap();
        run_git_sync(&temp_work, &["add", "."]).unwrap();
        run_git_sync(&temp_work, &["commit", "-m", "Feature commit"]).unwrap();
        let feature_sha = run_git_stdout(&temp_work, &["rev-parse", "HEAD"]).unwrap();
        run_git_sync(&temp_work, &["push", "origin", "HEAD:refs/heads/feature"]).unwrap();

        // Fast-forward main to feature (git merge --ff-only)
        run_git_sync(&temp_work, &["checkout", "--detach", "refs/heads/main"]).unwrap();
        run_git_sync(&temp_work, &["merge", "--ff-only", &feature_sha]).unwrap();
        run_git_sync(&temp_work, &["push", "origin", "HEAD:refs/heads/main"]).unwrap();

        let ff_sha = Sha::parse(&feature_sha).unwrap();

        run_git_sync(
            &clone_dir,
            &["worktree", "remove", "--force", temp_work.to_str().unwrap()],
        )
        .unwrap();

        // For a single-commit fast-forward, the parent is the prior main HEAD
        let worktree = worktree_for_stack(&config, PrNumber(999)).unwrap();
        run_git_sync(&worktree, &["fetch", "origin", "main"]).unwrap();

        // Verify the parent is the prior main HEAD
        let parents = super::get_parents(&worktree, ff_sha.as_str()).unwrap();
        assert_eq!(
            parents.len(),
            1,
            "Single-commit fast-forward should have one parent"
        );
        assert_eq!(
            parents[0], main_head_before,
            "Single-commit fast-forward parent should be prior main HEAD"
        );

        let is_valid = is_valid_squash_merge(&worktree, &ff_sha, "main").unwrap();
        assert!(
            is_valid,
            "Single-commit fast-forward should be ACCEPTED (indistinguishable from squash)"
        );
    }

    /// Test that is_valid_squash_merge rejects a multi-commit branch as invalid squash.
    ///
    /// This tests the same scenario as multi-commit rebase: a branch with multiple commits
    /// where the tip commit's parent is NOT on main's history (it's the previous feature commit).
    ///
    /// Note: This tests the branch BEFORE it's merged. After a fast-forward merge, the
    /// commits become part of main's history and this check would pass. The primary protection
    /// against multi-commit fast-forward is the preflight check requiring squash-only merges.
    ///
    /// This is complementary to is_valid_squash_merge_rejects_multi_commit_rebase, testing
    /// the same underlying property with a different branch structure.
    #[test]
    fn is_valid_squash_merge_rejects_multi_commit_branch() {
        let (_temp_dir, config, _initial_sha) = create_test_repo_with_origin();
        let clone_dir = config.clone_dir();

        // Record main HEAD before creating feature
        let main_head_before =
            run_git_stdout(&clone_dir, &["rev-parse", "refs/heads/main"]).unwrap();
        let main_head_before = Sha::parse(&main_head_before).unwrap();

        // Create a feature branch with TWO commits
        let temp_work = clone_dir.parent().unwrap().join("temp_multi_branch");
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

        // First commit on feature
        std::fs::write(temp_work.join("file1.txt"), "content1").unwrap();
        run_git_sync(&temp_work, &["add", "."]).unwrap();
        run_git_sync(&temp_work, &["commit", "-m", "Feature commit 1"]).unwrap();
        let first_sha = run_git_stdout(&temp_work, &["rev-parse", "HEAD"]).unwrap();
        let first_sha = Sha::parse(&first_sha).unwrap();

        // Second commit on feature
        std::fs::write(temp_work.join("file2.txt"), "content2").unwrap();
        run_git_sync(&temp_work, &["add", "."]).unwrap();
        run_git_sync(&temp_work, &["commit", "-m", "Feature commit 2"]).unwrap();
        let second_sha = run_git_stdout(&temp_work, &["rev-parse", "HEAD"]).unwrap();
        let second_sha = Sha::parse(&second_sha).unwrap();

        // Push feature branch (but DO NOT merge into main)
        run_git_sync(&temp_work, &["push", "origin", "HEAD:refs/heads/feature"]).unwrap();

        run_git_sync(
            &clone_dir,
            &["worktree", "remove", "--force", temp_work.to_str().unwrap()],
        )
        .unwrap();

        // Check from a worktree - the second commit has first commit as parent
        let worktree = worktree_for_stack(&config, PrNumber(999)).unwrap();
        run_git_sync(&worktree, &["fetch", "origin", "feature"]).unwrap();

        let parents = super::get_parents(&worktree, second_sha.as_str()).unwrap();
        assert_eq!(parents.len(), 1, "Second commit should have one parent");
        assert_eq!(
            parents[0], first_sha,
            "Second commit's parent should be the first commit"
        );
        assert_ne!(
            parents[0], main_head_before,
            "Second commit's parent should NOT be main HEAD"
        );

        // The second commit is NOT on main (feature branch only)
        // This should be rejected because:
        // 1. The commit is not on main's history
        // 2. The parent (first commit) is also not on main's history
        let is_valid = is_valid_squash_merge(&worktree, &second_sha, "main").unwrap();
        assert!(
            !is_valid,
            "Multi-commit branch tip should be REJECTED (commit not on main)"
        );
    }

    /// Helper to add a commit to a branch WITHOUT updating remote-tracking refs.
    /// This simulates the scenario where someone else pushes to the remote.
    fn add_commit_without_fetch(
        config: &GitConfig,
        branch: &str,
        filename: &str,
        content: &str,
    ) -> Sha {
        let clone_dir = config.clone_dir();

        // Create a temporary worktree
        let temp_work = clone_dir.parent().unwrap().join("temp_remote_push");
        let _ = std::fs::remove_dir_all(&temp_work);
        std::fs::create_dir_all(&temp_work).unwrap();
        run_git_sync(
            &clone_dir,
            &[
                "worktree",
                "add",
                "--detach",
                temp_work.to_str().unwrap(),
                &format!("refs/heads/{}", branch),
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

        // Update the branch ref DIRECTLY in the bare repo without using push
        // This avoids the opportunistic remote-tracking ref update
        run_git_sync(
            &clone_dir,
            &["update-ref", &format!("refs/heads/{}", branch), &sha],
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

    /// Test that update_root_for_behind fetches the branch, not just the default branch.
    ///
    /// BUG: update_root_for_behind only fetches the default branch, so origin/<branch> can
    /// be stale. This can cause us to merge main into an outdated root head, potentially
    /// dropping newer commits when we later push.
    #[test]
    fn update_root_for_behind_fetches_branch() {
        let (_temp_dir, config, _initial_sha) = create_test_repo_with_origin();

        // Create a branch
        let _branch_sha = create_branch_with_file(
            &config,
            "pr-123",
            "original.txt",
            "original content",
            "main",
        );

        // Get worktree and fetch pr-123 to establish initial state
        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
        run_git_sync(&worktree, &["fetch", "origin", "pr-123"]).unwrap();
        let original_head = rev_parse(&worktree, "origin/pr-123").unwrap();

        // Simulate someone else pushing a new commit to pr-123 (remote update)
        // Use add_commit_without_fetch to avoid updating remote-tracking refs
        let updated_sha =
            add_commit_without_fetch(&config, "pr-123", "updated.txt", "updated content");

        // Also add a commit to main (to make BEHIND status meaningful)
        let _main_update =
            add_commit_without_fetch(&config, "main", "main_new.txt", "main new content");

        // Verify the remote ref was actually updated
        let clone_dir = config.clone_dir();
        let remote_head = run_git_stdout(&clone_dir, &["rev-parse", "refs/heads/pr-123"]).unwrap();
        let remote_head = Sha::parse(&remote_head).unwrap();
        assert_eq!(
            remote_head, updated_sha,
            "Remote refs/heads/pr-123 should be updated"
        );

        // Now our worktree has stale origin/pr-123 (pointing to original_head)
        // Verify staleness before calling update_root_for_behind
        let stale_head = rev_parse(&worktree, "origin/pr-123").unwrap();
        assert_eq!(
            stale_head, original_head,
            "origin/pr-123 should still point to original_head (stale)"
        );
        assert_ne!(
            stale_head, remote_head,
            "origin/pr-123 should NOT point to remote_head yet (should be stale)"
        );

        // Call update_root_for_behind - it should fetch the branch to get the latest state
        let identity = test_identity();
        let result = update_root_for_behind(&worktree, "pr-123", "main", &identity).unwrap();
        assert!(
            matches!(
                result,
                MergeResult::Success { .. } | MergeResult::AlreadyUpToDate
            ),
            "update_root_for_behind should succeed, got {:?}",
            result
        );

        // The result should include the updated file (updated.txt)
        // If the branch wasn't fetched, this file won't exist because we started
        // from the stale origin/pr-123
        assert!(
            worktree.join("updated.txt").exists(),
            "updated.txt should exist - update_root_for_behind should fetch the branch \
             to get the latest state, not use a stale origin/<branch>"
        );

        // Also verify original file and main_new file exist
        assert!(worktree.join("original.txt").exists());
        assert!(worktree.join("main_new.txt").exists());
    }

    /// Test that reconcile_descendant validates the squash parent is on the default branch.
    ///
    /// This verifies that reconcile_descendant rejects commits whose parent is NOT
    /// reachable from the default branch. This catches cases like:
    /// - Multi-commit rebase where the "squash SHA" is actually the last rebased commit
    /// - Invalid SHA provided by mistake
    ///
    /// Note: The validation only works when the commits are NOT yet on main. Once a
    /// rebase-and-merge is pushed to main, the rebased commits ARE on main, so we
    /// can't distinguish them from a squash at the git level. The merge train relies
    /// on the GitHub API to verify the merge method (squash vs rebase) before calling
    /// reconcile_descendant.
    #[test]
    fn reconcile_descendant_rejects_commit_with_parent_not_on_main() {
        let (_temp_dir, config, _initial_sha) = create_test_repo_with_origin();
        let clone_dir = config.clone_dir();

        // Create predecessor
        let pred_sha =
            create_branch_with_file(&config, "pr-123", "pred.txt", "predecessor", "main");
        create_pr_ref(&config, 123, &pred_sha);

        // Create descendant from predecessor
        let _desc_sha = create_branch_with_file(
            &config,
            "pr-124",
            "desc.txt",
            "descendant content",
            "pr-123",
        );

        // Prepare the descendant
        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
        let identity = test_identity();
        prepare_descendant(&worktree, "pr-124", 123, &identity).unwrap();
        run_git_sync(
            &worktree,
            &["push", "origin", "HEAD:refs/heads/pr-124", "--force"],
        )
        .unwrap();

        // Capture main HEAD - this would be the expected squash parent
        let main_head = run_git_stdout(&clone_dir, &["rev-parse", "refs/heads/main"]).unwrap();
        let main_head = Sha::parse(&main_head).unwrap();

        // Create a multi-commit branch (simulating a rebase scenario)
        // The KEY is: the second commit's parent is NOT on main
        let multi_work = clone_dir.parent().unwrap().join("temp_multi");
        std::fs::create_dir_all(&multi_work).unwrap();
        run_git_sync(
            &clone_dir,
            &[
                "worktree",
                "add",
                "--detach",
                multi_work.to_str().unwrap(),
                "refs/heads/main",
            ],
        )
        .unwrap();
        run_git_sync(&multi_work, &["config", "user.email", "test@test.com"]).unwrap();
        run_git_sync(&multi_work, &["config", "user.name", "Test"]).unwrap();

        // First commit (parent IS on main)
        std::fs::write(multi_work.join("first.txt"), "first commit").unwrap();
        run_git_sync(&multi_work, &["add", "."]).unwrap();
        run_git_sync(&multi_work, &["commit", "-m", "First commit"]).unwrap();

        // Second commit (parent is first commit, NOT on main)
        std::fs::write(multi_work.join("second.txt"), "second commit").unwrap();
        run_git_sync(&multi_work, &["add", "."]).unwrap();
        run_git_sync(&multi_work, &["commit", "-m", "Second commit"]).unwrap();

        let second_sha = run_git_stdout(&multi_work, &["rev-parse", "HEAD"]).unwrap();
        let second_sha = Sha::parse(&second_sha).unwrap();

        // Push to a feature branch, NOT main
        // This simulates a rebase scenario where commits are on a branch but not yet merged
        run_git_sync(
            &multi_work,
            &["push", "origin", "HEAD:refs/heads/feature-multi"],
        )
        .unwrap();
        run_git_sync(
            &clone_dir,
            &[
                "worktree",
                "remove",
                "--force",
                multi_work.to_str().unwrap(),
            ],
        )
        .unwrap();

        // Try to reconcile with the second commit (whose parent is NOT on main)
        // This simulates trying to use a rebased commit as if it were a squash
        let result = reconcile_descendant(
            &worktree,
            &ReconcileRequest {
                descendant_branch: "pr-124",
                predecessor_pr: 123,
                squash_sha: &second_sha,
                expected_squash_parent: &main_head,
                predecessor_pre_squash_head: &pred_sha,
                default_branch: "main",
            },
            &identity,
        );

        // Should fail because second_sha's parent (first commit) is not on main
        assert!(
            result.is_err(),
            "reconcile_descendant should reject commit whose parent is not on main. Result: {:?}",
            result
        );
        let err = result.unwrap_err();
        let err_str = err.to_string();
        assert!(
            err_str.contains("not on the main branch"),
            "Error should mention 'not on the main branch', got: {}",
            err_str
        );
    }

    /// Regression test for the multi-commit FF-to-main bug.
    ///
    /// This test demonstrates the gap identified in the review:
    /// After a multi-commit rebase/FF merge lands on main, the parent check
    /// incorrectly passes because both the "squash" and its parent are now
    /// ancestors of main HEAD.
    ///
    /// The fix requires passing `expected_squash_parent` to verify the parent
    /// is exactly what we expect (the prior main HEAD), not just any ancestor.
    #[test]
    fn reconcile_descendant_rejects_multi_commit_ff_on_main() {
        let (_temp_dir, config, initial_sha) = create_test_repo_with_origin();
        let clone_dir = config.clone_dir();

        // Create predecessor
        let pred_sha =
            create_branch_with_file(&config, "pr-123", "pred.txt", "predecessor", "main");
        create_pr_ref(&config, 123, &pred_sha);

        // Create descendant from predecessor
        let _desc_sha = create_branch_with_file(
            &config,
            "pr-124",
            "desc.txt",
            "descendant content",
            "pr-123",
        );

        // Prepare the descendant
        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
        let identity = test_identity();
        prepare_descendant(&worktree, "pr-124", 123, &identity).unwrap();
        run_git_sync(
            &worktree,
            &["push", "origin", "HEAD:refs/heads/pr-124", "--force"],
        )
        .unwrap();

        // Record main HEAD BEFORE the FF merge - this is what the squash parent should be
        let main_before_ff = run_git_stdout(&clone_dir, &["rev-parse", "refs/heads/main"]).unwrap();
        let main_before_ff = Sha::parse(&main_before_ff).unwrap();

        // Create a multi-commit branch and FF merge it to main
        // This simulates someone doing a rebase-merge or direct FF push
        let multi_work = clone_dir.parent().unwrap().join("temp_multi_ff");
        std::fs::create_dir_all(&multi_work).unwrap();
        run_git_sync(
            &clone_dir,
            &[
                "worktree",
                "add",
                "--detach",
                multi_work.to_str().unwrap(),
                "refs/heads/main",
            ],
        )
        .unwrap();
        run_git_sync(&multi_work, &["config", "user.email", "test@test.com"]).unwrap();
        run_git_sync(&multi_work, &["config", "user.name", "Test"]).unwrap();

        // First commit
        std::fs::write(multi_work.join("first.txt"), "first commit").unwrap();
        run_git_sync(&multi_work, &["add", "."]).unwrap();
        run_git_sync(&multi_work, &["commit", "-m", "First commit"]).unwrap();
        let first_sha = run_git_stdout(&multi_work, &["rev-parse", "HEAD"]).unwrap();
        let first_sha = Sha::parse(&first_sha).unwrap();

        // Second commit (parent is first commit)
        std::fs::write(multi_work.join("second.txt"), "second commit").unwrap();
        run_git_sync(&multi_work, &["add", "."]).unwrap();
        run_git_sync(&multi_work, &["commit", "-m", "Second commit"]).unwrap();
        let second_sha = run_git_stdout(&multi_work, &["rev-parse", "HEAD"]).unwrap();
        let second_sha = Sha::parse(&second_sha).unwrap();

        // KEY DIFFERENCE: Push directly to main (fast-forward merge)
        // This makes BOTH commits ancestors of main HEAD
        run_git_sync(&multi_work, &["push", "origin", "HEAD:refs/heads/main"]).unwrap();

        run_git_sync(
            &clone_dir,
            &[
                "worktree",
                "remove",
                "--force",
                multi_work.to_str().unwrap(),
            ],
        )
        .unwrap();

        // Now try to reconcile with the second commit as if it were a squash
        // The parent of second_sha is first_sha, which is NOW on main (after the FF)
        // But first_sha is NOT the "prior main HEAD" - main_before_ff was!
        //
        // With the new expected_squash_parent parameter, this should fail because
        // first_sha != main_before_ff
        let result = reconcile_descendant(
            &worktree,
            &ReconcileRequest {
                descendant_branch: "pr-124",
                predecessor_pr: 123,
                squash_sha: &second_sha,
                expected_squash_parent: &main_before_ff,
                predecessor_pre_squash_head: // Expected parent: what main was BEFORE the FF
            &pred_sha,
                default_branch: "main",
            },
            &identity,
        );

        // Should fail because second_sha's parent (first_sha) != expected (main_before_ff)
        assert!(
            result.is_err(),
            "reconcile_descendant should reject multi-commit FF where parent != expected. \
             second_sha={}, parent={}, expected={}, result={:?}",
            second_sha,
            first_sha,
            main_before_ff,
            result
        );
        let err = result.unwrap_err();
        let err_str = err.to_string();
        assert!(
            err_str.contains("expected") || err_str.contains("rebase"),
            "Error should mention expected parent mismatch, got: {}",
            err_str
        );

        // Verify initial_sha is not used (silence warning)
        let _ = initial_sha;
    }

    /// `git merge-base --is-ancestor X X` exits 0: every commit is its own
    /// ancestor. The `is_ancestor(x, y)? || x == y` pattern is therefore
    /// redundant; this test pins the git semantics that justify omitting it.
    #[test]
    fn is_ancestor_is_reflexive() {
        let (_temp_dir, config, initial_sha) = create_test_repo_with_origin();
        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
        assert!(super::super::is_ancestor(&worktree, &initial_sha, &initial_sha).unwrap());
    }

    /// Simulates the force-push race: the predecessor is force-pushed to new
    /// content AFTER the descendant was prepared, then squash-merged. The
    /// descendant does not contain the squashed content, so the ours-merge in
    /// reconciliation would record the squash as an ancestor while the tree
    /// lacks its content — permanent, silent content loss. Reconcile must
    /// refuse.
    #[test]
    fn reconcile_refuses_when_predecessor_force_pushed_after_prepare() {
        let (_temp_dir, config, _initial_sha) = create_test_repo_with_origin();
        let clone_dir = config.clone_dir();
        let identity = test_identity();

        // Predecessor at state X
        let pred_x =
            create_branch_with_file(&config, "pr-123", "pred.txt", "original content", "main");
        create_pr_ref(&config, 123, &pred_x);

        // Descendant from X
        let _desc_sha = create_branch_with_file(
            &config,
            "pr-124",
            "desc.txt",
            "descendant content",
            "pr-123",
        );

        // Prepare the descendant (pins X)
        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
        let prep = prepare_descendant(&worktree, "pr-124", 123, &identity).unwrap();
        assert_eq!(prep.predecessor_head, pred_x);
        run_git_sync(
            &worktree,
            &["push", "origin", "HEAD:refs/heads/pr-124", "--force"],
        )
        .unwrap();

        // Force-push the predecessor to state Y: rewritten from main, X is NOT
        // an ancestor of Y and Y's content is absent from the descendant.
        let pred_y = create_branch_with_file(
            &config,
            "pr-123-rewrite",
            "pred.txt",
            "rewritten content",
            "main",
        );
        run_git_sync(
            &clone_dir,
            &["update-ref", "refs/heads/pr-123", pred_y.as_str()],
        )
        .unwrap();
        run_git_sync(
            &clone_dir,
            &["update-ref", "refs/pull/123/head", pred_y.as_str()],
        )
        .unwrap();

        // GitHub squash-merges Y to main
        let main_before_squash =
            run_git_stdout(&clone_dir, &["rev-parse", "refs/heads/main"]).unwrap();
        let main_before_squash = Sha::parse(&main_before_squash).unwrap();

        let temp_work = clone_dir.parent().unwrap().join("temp_squash_race");
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
        run_git_sync(&temp_work, &["merge", "--squash", pred_y.as_str()]).unwrap();
        run_git_sync(&temp_work, &["commit", "-m", "Squash: rewritten pred"]).unwrap();
        let squash_sha = run_git_stdout(&temp_work, &["rev-parse", "HEAD"]).unwrap();
        let squash_sha = Sha::parse(&squash_sha).unwrap();
        run_git_sync(&temp_work, &["push", "origin", "HEAD:refs/heads/main"]).unwrap();
        run_git_sync(
            &clone_dir,
            &["worktree", "remove", "--force", temp_work.to_str().unwrap()],
        )
        .unwrap();

        // The cascade believes the pre-squash predecessor head is Y. Y is not
        // an ancestor of the descendant (only X was merged), so reconciliation
        // must refuse rather than perform the ours-merge.
        let result = reconcile_descendant(
            &worktree,
            &ReconcileRequest {
                descendant_branch: "pr-124",
                predecessor_pr: 123,
                squash_sha: &squash_sha,
                expected_squash_parent: &main_before_squash,
                predecessor_pre_squash_head: &pred_y,
                default_branch: "main",
            },
            &identity,
        );

        assert!(
            result.is_err(),
            "reconcile must refuse when the descendant does not contain the \
             predecessor's pre-squash head (force-push race), got {:?}",
            result
        );
    }

    #[test]
    fn reconcile_refuses_when_caller_passes_the_prepared_head_after_force_push() {
        // The companion test above passes the NEW head Y. A caller following
        // the documentation passes what prepare_descendant RETURNED — the old
        // head X. X is an ancestor of the descendant, so an ancestry check on
        // the passed value alone cannot detect the force-push: reconcile must
        // verify the head that was ACTUALLY squash-merged.
        let (_temp_dir, config, _initial_sha) = create_test_repo_with_origin();
        let clone_dir = config.clone_dir();
        let identity = test_identity();

        let pred_x =
            create_branch_with_file(&config, "pr-123", "pred.txt", "original content", "main");
        create_pr_ref(&config, 123, &pred_x);

        let _desc_sha = create_branch_with_file(
            &config,
            "pr-124",
            "desc.txt",
            "descendant content",
            "pr-123",
        );

        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
        let prep = prepare_descendant(&worktree, "pr-124", 123, &identity).unwrap();
        assert_eq!(prep.predecessor_head, pred_x);
        run_git_sync(
            &worktree,
            &["push", "origin", "HEAD:refs/heads/pr-124", "--force"],
        )
        .unwrap();

        // Force-push the predecessor to Y after preparation.
        let pred_y = create_branch_with_file(
            &config,
            "pr-123-rewrite",
            "pred.txt",
            "rewritten content",
            "main",
        );
        run_git_sync(
            &clone_dir,
            &["update-ref", "refs/heads/pr-123", pred_y.as_str()],
        )
        .unwrap();
        run_git_sync(
            &clone_dir,
            &["update-ref", "refs/pull/123/head", pred_y.as_str()],
        )
        .unwrap();

        // GitHub squash-merges Y to main.
        let main_before_squash =
            run_git_stdout(&clone_dir, &["rev-parse", "refs/heads/main"]).unwrap();
        let main_before_squash = Sha::parse(&main_before_squash).unwrap();

        let temp_work = clone_dir.parent().unwrap().join("temp_squash_race2");
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
        run_git_sync(&temp_work, &["merge", "--squash", pred_y.as_str()]).unwrap();
        run_git_sync(&temp_work, &["commit", "-m", "Squash: rewritten pred"]).unwrap();
        let squash_sha = run_git_stdout(&temp_work, &["rev-parse", "HEAD"]).unwrap();
        let squash_sha = Sha::parse(&squash_sha).unwrap();
        run_git_sync(&temp_work, &["push", "origin", "HEAD:refs/heads/main"]).unwrap();
        run_git_sync(
            &clone_dir,
            &["worktree", "remove", "--force", temp_work.to_str().unwrap()],
        )
        .unwrap();

        // The caller passes exactly what prepare returned: X.
        let result = reconcile_descendant(
            &worktree,
            &ReconcileRequest {
                descendant_branch: "pr-124",
                predecessor_pr: 123,
                squash_sha: &squash_sha,
                expected_squash_parent: &main_before_squash,
                predecessor_pre_squash_head: &prep.predecessor_head,
                default_branch: "main",
            },
            &identity,
        );

        assert!(
            result.is_err(),
            "reconcile must refuse when the squashed head differs from the \
             prepared head, even though the prepared head IS an ancestor of \
             the descendant; got {:?}",
            result
        );
    }

    /// Strips the crate-name prefix from `module_path!()` and appends the test
    /// name, producing the name libtest expects for `--exact` filtering.
    fn child_test_name(test_name: &str) -> String {
        let module = module_path!();
        let module = module
            .split_once("::")
            .map(|(_, rest)| rest)
            .unwrap_or(module);
        format!("{}::{}", module, test_name)
    }

    /// Re-runs the named test in a child process with extra environment
    /// variables, leaving the parent test process's environment untouched
    /// (mutating the process environment would race with parallel tests).
    fn rerun_self_with_env(test_name: &str, envs: &[(&str, &str)]) {
        let exe = std::env::current_exe().unwrap();
        let mut cmd = std::process::Command::new(&exe);
        cmd.args([test_name, "--exact", "--test-threads=1", "--nocapture"]);
        for (key, value) in envs {
            cmd.env(key, value);
        }
        let output = cmd.output().unwrap();
        assert!(
            output.status.success(),
            "child test run failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    const CHILD_MARKER: &str = "MERGE_TRAIN_GIT_ENV_TEST_CHILD";

    /// Merge classification must not depend on the process locale: under a
    /// German locale git prints "Bereits aktuell." and "KONFLIKT", which prose
    /// parsing of the English strings misclassifies (a no-op merge as Success,
    /// a conflict as an unexpected error).
    #[test]
    fn merge_classification_is_locale_independent() {
        if std::env::var_os(CHILD_MARKER).is_none() {
            rerun_self_with_env(
                &child_test_name("merge_classification_is_locale_independent"),
                &[
                    (CHILD_MARKER, "1"),
                    ("LC_ALL", "de_DE.UTF-8"),
                    ("LC_MESSAGES", "de_DE.UTF-8"),
                    ("LANG", "de_DE.UTF-8"),
                    ("LANGUAGE", "de_DE.UTF-8"),
                ],
            );
            return;
        }

        let (_temp_dir, config, _initial_sha) = create_test_repo_with_origin();
        let identity = test_identity();

        // No-op merge: branch already contains main.
        create_branch_with_file(&config, "pr-123", "feature.txt", "feature", "main");
        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
        let result = catch_up_descendant(&worktree, "pr-123", "main", &identity).unwrap();
        assert!(
            matches!(result, MergeResult::AlreadyUpToDate),
            "no-op merge must classify as AlreadyUpToDate regardless of locale, got {:?}",
            result
        );

        // Conflicting merge: branch and main both add conflict.txt.
        create_branch_with_file(&config, "pr-200", "conflict.txt", "branch version", "main");
        create_branch_with_file(&config, "main", "conflict.txt", "main version", "main");
        let worktree2 = worktree_for_stack(&config, PrNumber(200)).unwrap();
        let result = catch_up_descendant(&worktree2, "pr-200", "main", &identity)
            .expect("conflicting merge must classify as Conflict regardless of locale");
        assert!(
            matches!(result, MergeResult::Conflict { .. }),
            "conflicting merge must classify as Conflict regardless of locale, got {:?}",
            result
        );
    }

    /// GIT_DIR / GIT_WORK_TREE / GIT_INDEX_FILE in the parent environment must
    /// not redirect git operations away from the directory each command is run
    /// in.
    #[test]
    fn parent_env_does_not_redirect_git_operations() {
        if std::env::var_os(CHILD_MARKER).is_none() {
            rerun_self_with_env(
                &child_test_name("parent_env_does_not_redirect_git_operations"),
                &[
                    (CHILD_MARKER, "1"),
                    ("GIT_DIR", "/nonexistent/never-a-git-dir"),
                    ("GIT_WORK_TREE", "/nonexistent/never-a-work-tree"),
                    ("GIT_INDEX_FILE", "/nonexistent/never-an-index"),
                ],
            );
            return;
        }

        let (_temp_dir, config, initial_sha) = create_test_repo_with_origin();
        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
        let head = rev_parse(&worktree, "HEAD").unwrap();
        assert_eq!(head, initial_sha);
    }

    /// HOME and SSH_AUTH_SOCK must survive the environment scrub: ssh
    /// resolves ~/.ssh/config, keys, and known_hosts via HOME, and
    /// agent-based auth needs the socket. Without them, every fetch/push to
    /// an SSH remote breaks. (Config injection stays blocked regardless:
    /// GIT_CONFIG_GLOBAL/GIT_CONFIG_NOSYSTEM are pinned.)
    #[test]
    fn transport_auth_env_is_preserved() {
        if std::env::var_os(CHILD_MARKER).is_none() {
            rerun_self_with_env(
                &child_test_name("transport_auth_env_is_preserved"),
                &[
                    (CHILD_MARKER, "1"),
                    ("HOME", "/tmp/test-home"),
                    ("SSH_AUTH_SOCK", "/tmp/test-agent.sock"),
                    ("GIT_DIR", "/nonexistent/still-scrubbed"),
                ],
            );
            return;
        }

        let cmd = git_command(Path::new("."));
        let envs: std::collections::HashMap<_, _> = cmd
            .get_envs()
            .filter_map(|(k, v)| v.map(|v| (k.to_os_string(), v.to_os_string())))
            .collect();

        assert_eq!(
            envs.get(std::ffi::OsStr::new("HOME"))
                .map(|v| v.as_os_str()),
            Some(std::ffi::OsStr::new("/tmp/test-home")),
            "HOME must be preserved for ssh config/keys/known_hosts"
        );
        assert_eq!(
            envs.get(std::ffi::OsStr::new("SSH_AUTH_SOCK"))
                .map(|v| v.as_os_str()),
            Some(std::ffi::OsStr::new("/tmp/test-agent.sock")),
            "SSH_AUTH_SOCK must be preserved for agent auth"
        );
        assert!(
            !envs.contains_key(std::ffi::OsStr::new("GIT_DIR")),
            "GIT_DIR must still be scrubbed"
        );
    }

    /// GIT_AUTHOR_* / GIT_COMMITTER_* in the parent environment must not
    /// override the commit identity passed via `-c user.name` / `-c user.email`.
    #[test]
    fn commit_identity_not_overridden_by_parent_env() {
        if std::env::var_os(CHILD_MARKER).is_none() {
            rerun_self_with_env(
                &child_test_name("commit_identity_not_overridden_by_parent_env"),
                &[
                    (CHILD_MARKER, "1"),
                    ("GIT_AUTHOR_NAME", "Mallory"),
                    ("GIT_AUTHOR_EMAIL", "mallory@evil.example"),
                    ("GIT_COMMITTER_NAME", "Mallory"),
                    ("GIT_COMMITTER_EMAIL", "mallory@evil.example"),
                ],
            );
            return;
        }

        let (_temp_dir, config, _initial_sha) = create_test_repo_with_origin();
        let identity = test_identity();

        // Predecessor and descendant diverge, so preparation creates a real
        // merge commit (not a no-op).
        let pred_sha =
            create_branch_with_file(&config, "pr-123", "pred.txt", "predecessor", "main");
        create_pr_ref(&config, 123, &pred_sha);
        let _desc_sha =
            create_branch_with_file(&config, "pr-124", "desc.txt", "descendant", "main");

        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
        let prep = prepare_descendant(&worktree, "pr-124", 123, &identity).unwrap();
        assert!(
            matches!(prep.merge, MergeResult::Success { .. }),
            "expected a real merge commit"
        );

        let identities =
            run_git_stdout(&worktree, &["log", "-1", "--format=%an <%ae>%n%cn <%ce>"]).unwrap();
        assert_eq!(
            identities, "Test <test@test.com>\nTest <test@test.com>",
            "merge commit identity must come from the configured CommitIdentity, \
             not the parent environment"
        );
    }
}
