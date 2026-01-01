//! Property-based tests with real git operations.
//!
//! These tests verify the correctness of the cascade operations using property-based
//! testing with real git repositories. Each test generates random but valid scenarios
//! and verifies invariants.
//!
//! **Property 1**: Descendant content preserved after cascade
//! **Property 2**: Intervening main commits preserved
//! **Property 3**: Squash parent ordering prevents lost commits
//! **Property 9**: Recovery uses frozen descendants (via worktree state)
//! **Property**: Worktree cleanup on abort leaves no orphans

use proptest::prelude::*;
use proptest::test_runner::Config as ProptestConfig;
use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;

use crate::git::merge::{catch_up_descendant, prepare_descendant, reconcile_descendant};
use crate::git::recovery::{cleanup_worktree_on_abort, is_worktree_dirty};
use crate::git::worktree::{cleanup_stale_worktrees, list_worktrees, worktree_for_stack};
use crate::git::{CommitIdentity, GitConfig, is_ancestor, run_git_stdout, run_git_sync};
use crate::types::{PrNumber, Sha};

/// Test identity for merge commits (no signing).
fn test_identity() -> CommitIdentity {
    CommitIdentity {
        name: "Test".to_string(),
        email: "test@test.com".to_string(),
        signing_key: None,
    }
}

/// Create a test repository with the given configuration.
fn create_test_repo() -> (TempDir, GitConfig) {
    let temp_dir = TempDir::new().unwrap();
    let base_dir = temp_dir.path().to_path_buf();

    let config = GitConfig {
        base_dir: base_dir.clone(),
        owner: "test".to_string(),
        repo: "repo".to_string(),
        default_branch: "main".to_string(),
        worktree_max_age: Duration::from_secs(24 * 3600),
        commit_identity: CommitIdentity {
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

/// Create a branch with a file.
fn create_branch_with_file(
    config: &GitConfig,
    branch: &str,
    filename: &str,
    content: &str,
    base_branch: &str,
) -> Sha {
    let clone_dir = config.clone_dir();

    // Create a temporary worktree for making the branch
    let temp_work = clone_dir.parent().unwrap().join(format!("temp_{}", branch));
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

/// Create a PR ref in the bare repo (simulates GitHub's refs/pull/<n>/head).
///
/// This is needed because prepare_descendant now fetches via PR refs.
fn create_pr_ref(config: &GitConfig, pr_number: u64, sha: &Sha) {
    let clone_dir = config.clone_dir();
    run_git_sync(
        &clone_dir,
        &[
            "update-ref",
            &format!("refs/pull/{}/head", pr_number),
            sha.as_str(),
        ],
    )
    .unwrap();
}

/// Perform a squash merge of a branch into main.
fn squash_merge_to_main(config: &GitConfig, branch_sha: &Sha) -> Sha {
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

    // Squash merge
    run_git_sync(&temp_work, &["merge", "--squash", branch_sha.as_str()]).unwrap();
    run_git_sync(&temp_work, &["commit", "-m", "Squash merge"]).unwrap();

    let squash_sha = run_git_stdout(&temp_work, &["rev-parse", "HEAD"]).unwrap();

    run_git_sync(&temp_work, &["push", "origin", "HEAD:refs/heads/main"]).unwrap();
    run_git_sync(
        &clone_dir,
        &["worktree", "remove", "--force", temp_work.to_str().unwrap()],
    )
    .unwrap();

    Sha::parse(&squash_sha).unwrap()
}

/// Add a commit to main (simulating an independent PR landing).
fn add_commit_to_main(config: &GitConfig, filename: &str, content: &str) -> Sha {
    create_branch_with_file(config, "main", filename, content, "main")
}

/// Read file content from a worktree.
fn read_file(worktree: &PathBuf, filename: &str) -> Option<String> {
    std::fs::read_to_string(worktree.join(filename)).ok()
}

// Configure proptest to run fewer cases since each test creates a fresh git repo.
// 10 cases is enough to catch issues while keeping tests fast.
proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    /// Property 1: Descendant content is preserved after cascade.
    ///
    /// After prepare + reconcile + catch-up, the descendant should contain:
    /// - All content from the predecessor
    /// - All content from the descendant's own work
    /// - All content from main (including the squash)
    #[test]
    fn descendant_content_preserved_after_cascade(
        pred_content in "[a-z]{10,50}",
        desc_content in "[a-z]{10,50}",
    ) {
        let (_temp_dir, config) = create_test_repo();

        // Create predecessor branch with a file
        let pred_sha = create_branch_with_file(&config, "pr-123", "pred.txt", &pred_content, "main");
        create_pr_ref(&config, 123, &pred_sha);

        // Create descendant branch with its own file
        let _desc_sha = create_branch_with_file(&config, "pr-124", "desc.txt", &desc_content, "pr-123");

        // Get worktree and prepare descendant using PR number
        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
        let prep_result = prepare_descendant(&worktree, "pr-124", 123, &test_identity()).unwrap();
        prop_assert!(prep_result.is_ok());
        run_git_sync(&worktree, &["push", "origin", "HEAD:refs/heads/pr-124", "--force"]).unwrap();

        // Squash merge predecessor to main
        let squash_sha = squash_merge_to_main(&config, &pred_sha);

        // Reconcile descendant
        let reconcile_result = reconcile_descendant(&worktree, "pr-124", &squash_sha, "main", &test_identity()).unwrap();
        prop_assert!(reconcile_result.is_ok());

        // Catch up with main (if needed)
        let _ = catch_up_descendant(&worktree, "pr-124", "main", &test_identity()).unwrap();

        // Verify all content is present
        prop_assert!(worktree.join("pred.txt").exists(), "pred.txt should exist");
        prop_assert!(worktree.join("desc.txt").exists(), "desc.txt should exist");
        prop_assert!(worktree.join("README.md").exists(), "README.md should exist");

        // Verify content is correct
        prop_assert_eq!(read_file(&worktree, "pred.txt"), Some(pred_content));
        prop_assert_eq!(read_file(&worktree, "desc.txt"), Some(desc_content));
    }

    /// Property 2: Intervening main commits are preserved.
    ///
    /// If an independent commit lands on main between preparation and reconciliation,
    /// it should be incorporated into the descendant via catch-up.
    #[test]
    fn intervening_main_commits_preserved(
        pred_content in "[a-z]{10,50}",
        desc_content in "[a-z]{10,50}",
        intervening_content in "[a-z]{10,50}",
    ) {
        let (_temp_dir, config) = create_test_repo();

        // Create predecessor and descendant
        let pred_sha = create_branch_with_file(&config, "pr-123", "pred.txt", &pred_content, "main");
        create_pr_ref(&config, 123, &pred_sha);
        let _desc_sha = create_branch_with_file(&config, "pr-124", "desc.txt", &desc_content, "pr-123");

        // Prepare descendant using PR number
        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
        prepare_descendant(&worktree, "pr-124", 123, &test_identity()).unwrap();
        run_git_sync(&worktree, &["push", "origin", "HEAD:refs/heads/pr-124", "--force"]).unwrap();

        // Squash merge predecessor
        let squash_sha = squash_merge_to_main(&config, &pred_sha);

        // An independent commit lands on main AFTER the squash
        let _intervening_sha = add_commit_to_main(&config, "intervening.txt", &intervening_content);

        // Reconcile and catch up
        reconcile_descendant(&worktree, "pr-124", &squash_sha, "main", &test_identity()).unwrap();
        catch_up_descendant(&worktree, "pr-124", "main", &test_identity()).unwrap();

        // The intervening commit should be incorporated
        prop_assert!(worktree.join("intervening.txt").exists(), "intervening.txt should exist");
        prop_assert_eq!(read_file(&worktree, "intervening.txt"), Some(intervening_content));

        // All other content should still be there
        prop_assert!(worktree.join("pred.txt").exists());
        prop_assert!(worktree.join("desc.txt").exists());
    }

    /// Property 1b: Multi-file descendant content is preserved after cascade.
    ///
    /// This extends Property 1 to test multiple disjoint files across predecessor
    /// and descendant branches. All files from both branches must be preserved.
    #[test]
    fn multi_file_content_preserved_after_cascade(
        pred_content1 in "[a-z]{10,30}",
        pred_content2 in "[a-z]{10,30}",
        desc_content1 in "[a-z]{10,30}",
        desc_content2 in "[a-z]{10,30}",
        desc_content3 in "[a-z]{10,30}",
    ) {
        let (_temp_dir, config) = create_test_repo();
        let clone_dir = config.clone_dir();

        // Create predecessor with MULTIPLE files (simulates multi-commit)
        let temp_work = clone_dir.parent().unwrap().join("temp_pred_multi");
        std::fs::create_dir_all(&temp_work).unwrap();
        run_git_sync(
            &clone_dir,
            &["worktree", "add", "--detach", temp_work.to_str().unwrap(), "refs/heads/main"],
        ).unwrap();
        run_git_sync(&temp_work, &["config", "user.email", "test@test.com"]).unwrap();
        run_git_sync(&temp_work, &["config", "user.name", "Test"]).unwrap();

        // Predecessor commit 1
        std::fs::write(temp_work.join("pred1.txt"), &pred_content1).unwrap();
        run_git_sync(&temp_work, &["add", "."]).unwrap();
        run_git_sync(&temp_work, &["commit", "-m", "Pred file 1"]).unwrap();

        // Predecessor commit 2
        std::fs::write(temp_work.join("pred2.txt"), &pred_content2).unwrap();
        run_git_sync(&temp_work, &["add", "."]).unwrap();
        run_git_sync(&temp_work, &["commit", "-m", "Pred file 2"]).unwrap();

        let pred_sha = run_git_stdout(&temp_work, &["rev-parse", "HEAD"]).unwrap();
        run_git_sync(&temp_work, &["push", "origin", "HEAD:refs/heads/pr-123"]).unwrap();
        let pred_sha = Sha::parse(&pred_sha).unwrap();
        run_git_sync(&clone_dir, &["update-ref", "refs/pull/123/head", pred_sha.as_str()]).unwrap();

        // Create descendant from predecessor with MULTIPLE files
        run_git_sync(&temp_work, &["checkout", "--detach", &pred_sha.as_str()]).unwrap();

        // Descendant commit 1
        std::fs::write(temp_work.join("desc1.txt"), &desc_content1).unwrap();
        run_git_sync(&temp_work, &["add", "."]).unwrap();
        run_git_sync(&temp_work, &["commit", "-m", "Desc file 1"]).unwrap();

        // Descendant commit 2
        std::fs::write(temp_work.join("desc2.txt"), &desc_content2).unwrap();
        run_git_sync(&temp_work, &["add", "."]).unwrap();
        run_git_sync(&temp_work, &["commit", "-m", "Desc file 2"]).unwrap();

        // Descendant commit 3
        std::fs::write(temp_work.join("desc3.txt"), &desc_content3).unwrap();
        run_git_sync(&temp_work, &["add", "."]).unwrap();
        run_git_sync(&temp_work, &["commit", "-m", "Desc file 3"]).unwrap();

        run_git_sync(&temp_work, &["push", "origin", "HEAD:refs/heads/pr-124"]).unwrap();
        run_git_sync(&clone_dir, &["worktree", "remove", "--force", temp_work.to_str().unwrap()]).unwrap();

        // Prepare, squash, reconcile, catch-up
        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
        prepare_descendant(&worktree, "pr-124", 123, &test_identity()).unwrap();
        run_git_sync(&worktree, &["push", "origin", "HEAD:refs/heads/pr-124", "--force"]).unwrap();

        let squash_sha = squash_merge_to_main(&config, &pred_sha);
        reconcile_descendant(&worktree, "pr-124", &squash_sha, "main", &test_identity()).unwrap();
        catch_up_descendant(&worktree, "pr-124", "main", &test_identity()).unwrap();

        // Verify ALL predecessor files preserved
        prop_assert!(worktree.join("pred1.txt").exists(), "pred1.txt should exist");
        prop_assert!(worktree.join("pred2.txt").exists(), "pred2.txt should exist");
        prop_assert_eq!(read_file(&worktree, "pred1.txt"), Some(pred_content1));
        prop_assert_eq!(read_file(&worktree, "pred2.txt"), Some(pred_content2));

        // Verify ALL descendant files preserved
        prop_assert!(worktree.join("desc1.txt").exists(), "desc1.txt should exist");
        prop_assert!(worktree.join("desc2.txt").exists(), "desc2.txt should exist");
        prop_assert!(worktree.join("desc3.txt").exists(), "desc3.txt should exist");
        prop_assert_eq!(read_file(&worktree, "desc1.txt"), Some(desc_content1));
        prop_assert_eq!(read_file(&worktree, "desc2.txt"), Some(desc_content2));
        prop_assert_eq!(read_file(&worktree, "desc3.txt"), Some(desc_content3));
    }

    /// Property 2b: Multiple intervening main commits are preserved.
    ///
    /// This extends Property 2 to test:
    /// 1. Intervening commits that land BEFORE the squash (via $SQUASH_SHA^)
    /// 2. Intervening commits that land AFTER the squash (via catch-up)
    /// Both types must be preserved in the final state.
    #[test]
    fn multiple_intervening_commits_preserved(
        pred_content in "[a-z]{10,30}",
        desc_content in "[a-z]{10,30}",
        before_squash_content1 in "[a-z]{10,30}",
        before_squash_content2 in "[a-z]{10,30}",
        after_squash_content in "[a-z]{10,30}",
    ) {
        let (_temp_dir, config) = create_test_repo();

        // Create predecessor and descendant
        let pred_sha = create_branch_with_file(&config, "pr-123", "pred.txt", &pred_content, "main");
        create_pr_ref(&config, 123, &pred_sha);
        let _desc_sha = create_branch_with_file(&config, "pr-124", "desc.txt", &desc_content, "pr-123");

        // Prepare descendant
        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
        prepare_descendant(&worktree, "pr-124", 123, &test_identity()).unwrap();
        run_git_sync(&worktree, &["push", "origin", "HEAD:refs/heads/pr-124", "--force"]).unwrap();

        // === Two commits land on main BEFORE the squash ===
        // These get incorporated via $SQUASH_SHA^ (parent of squash)
        let _before1 = add_commit_to_main(&config, "before1.txt", &before_squash_content1);
        let _before2 = add_commit_to_main(&config, "before2.txt", &before_squash_content2);

        // Squash merge predecessor
        let squash_sha = squash_merge_to_main(&config, &pred_sha);

        // === One commit lands on main AFTER the squash ===
        // This gets incorporated via catch-up
        let _after = add_commit_to_main(&config, "after.txt", &after_squash_content);

        // Reconcile and catch up
        reconcile_descendant(&worktree, "pr-124", &squash_sha, "main", &test_identity()).unwrap();
        catch_up_descendant(&worktree, "pr-124", "main", &test_identity()).unwrap();

        // Verify BEFORE-squash commits are preserved (via $SQUASH_SHA^)
        prop_assert!(worktree.join("before1.txt").exists(), "before1.txt should exist (pre-squash commit via $SQUASH_SHA^)");
        prop_assert!(worktree.join("before2.txt").exists(), "before2.txt should exist (pre-squash commit via $SQUASH_SHA^)");
        prop_assert_eq!(read_file(&worktree, "before1.txt"), Some(before_squash_content1));
        prop_assert_eq!(read_file(&worktree, "before2.txt"), Some(before_squash_content2));

        // Verify AFTER-squash commit is preserved (via catch-up)
        prop_assert!(worktree.join("after.txt").exists(), "after.txt should exist (post-squash commit via catch-up)");
        prop_assert_eq!(read_file(&worktree, "after.txt"), Some(after_squash_content));

        // Verify original content still present
        prop_assert!(worktree.join("pred.txt").exists());
        prop_assert!(worktree.join("desc.txt").exists());
        prop_assert_eq!(read_file(&worktree, "pred.txt"), Some(pred_content));
        prop_assert_eq!(read_file(&worktree, "desc.txt"), Some(desc_content));
    }

    /// Property 3: Squash parent ordering prevents lost commits.
    ///
    /// The reconciliation uses $SQUASH_SHA^ (parent of squash) to ensure that
    /// commits landing on main before the squash are incorporated correctly.
    /// This test verifies:
    /// 1. The squash commit has exactly one parent
    /// 2. That parent is on the default branch (ancestor of current main HEAD)
    #[test]
    fn squash_has_single_parent(
        content in "[a-z]{10,50}",
    ) {
        let (_temp_dir, config) = create_test_repo();

        // Create and squash a branch
        let branch_sha = create_branch_with_file(&config, "pr-123", "file.txt", &content, "main");
        let squash_sha = squash_merge_to_main(&config, &branch_sha);

        // Get a worktree to verify
        let worktree = worktree_for_stack(&config, PrNumber(999)).unwrap();
        run_git_sync(&worktree, &["fetch", "origin", "main"]).unwrap();

        // Verify squash has exactly one parent
        let parents = super::get_parents(&worktree, squash_sha.as_str()).unwrap();
        prop_assert_eq!(parents.len(), 1, "Squash commit should have exactly one parent");

        // The parent should be on the default branch.
        // This is the actual invariant: the parent must be on main's history,
        // not just "an ancestor of the squash" (which is trivially true for any parent).
        let parent = &parents[0];
        let main_head = super::rev_parse(&worktree, "origin/main").unwrap();

        // Parent should be an ancestor of (or equal to) main HEAD
        let parent_on_main = is_ancestor(&worktree, parent, &main_head).unwrap()
            || parent == &main_head;
        prop_assert!(
            parent_on_main,
            "Parent {} should be on main's history (ancestor of main HEAD {})",
            parent,
            main_head
        );
    }

    /// Worktree cleanup on abort leaves no dirty state.
    #[test]
    fn worktree_cleanup_leaves_clean_state(
        file_content in "[a-z]{10,50}",
    ) {
        let (_temp_dir, config) = create_test_repo();

        // Create a worktree and make it dirty
        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
        run_git_sync(&worktree, &["config", "user.email", "test@test.com"]).unwrap();
        run_git_sync(&worktree, &["config", "user.name", "Test"]).unwrap();

        // Add untracked file
        std::fs::write(worktree.join("untracked.txt"), &file_content).unwrap();

        // Add staged file
        std::fs::write(worktree.join("staged.txt"), &file_content).unwrap();
        run_git_sync(&worktree, &["add", "staged.txt"]).unwrap();

        // Verify dirty
        prop_assert!(is_worktree_dirty(&worktree).unwrap());

        // Clean up
        cleanup_worktree_on_abort(&worktree).unwrap();

        // Verify clean
        prop_assert!(!is_worktree_dirty(&worktree).unwrap());
        prop_assert!(!worktree.join("untracked.txt").exists());
        prop_assert!(!worktree.join("staged.txt").exists());
    }

    /// Stale worktree cleanup removes orphaned worktrees.
    #[test]
    fn stale_worktree_cleanup_removes_orphans(
        pr_numbers in prop::collection::vec(1u64..1000, 1..5),
        active_pr in 1u64..1000,
    ) {
        let (_temp_dir, config) = create_test_repo();

        // Create worktrees for the generated PR numbers
        for pr in &pr_numbers {
            worktree_for_stack(&config, PrNumber(*pr)).unwrap();
        }

        // Mark only one as active
        let mut active = HashSet::new();
        active.insert(PrNumber(active_pr));

        // Use zero max age so all are considered "stale"
        let mut config_zero = config.clone();
        config_zero.worktree_max_age = Duration::ZERO;

        let removed = cleanup_stale_worktrees(&config_zero, &active).unwrap();

        // All non-active worktrees should be removed
        let remaining = list_worktrees(&config).unwrap();
        for (pr, _path) in &remaining {
            prop_assert!(
                active.contains(pr) || !pr_numbers.contains(&pr.0),
                "Only active worktrees should remain, but found {:?}",
                pr
            );
        }

        // Removed should contain the non-active PRs from our list
        for pr in &pr_numbers {
            if *pr != active_pr {
                prop_assert!(
                    removed.contains(&PrNumber(*pr)),
                    "PR {} should have been removed",
                    pr
                );
            }
        }
    }
}

/// Property 3 extension: Verify that the correct approach (using $SQUASH_SHA^ during
/// reconciliation) properly incorporates late commits that land on main.
///
/// This is a deterministic test rather than property-based because it's testing
/// a specific scenario from the design doc.
///
/// The test demonstrates:
/// 1. A late commit lands on main between preparation and squash
/// 2. The $SQUASH_SHA^ includes this late commit
/// 3. Reconciliation properly incorporates the late commit's content
///
/// The naive approach (merging main during preparation) would create a more
/// complex merge topology that could cause issues, but git's 3-way merge
/// is robust enough to handle most cases. The correct approach avoids this
/// complexity by not pre-merging main.
#[test]
fn squash_parent_ordering_incorporates_late_commits() {
    let (_temp_dir, config) = create_test_repo();

    // Create predecessor PR
    let pred_sha = create_branch_with_file(&config, "pr-123", "pred.txt", "predecessor", "main");
    create_pr_ref(&config, 123, &pred_sha);

    // Create descendant PR from predecessor
    let _desc_sha = create_branch_with_file(&config, "pr-124", "desc.txt", "descendant", "pr-123");

    // CORRECT APPROACH: Prepare WITHOUT merging main
    let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
    prepare_descendant(&worktree, "pr-124", 123, &test_identity()).unwrap();
    run_git_sync(
        &worktree,
        &["push", "origin", "HEAD:refs/heads/pr-124", "--force"],
    )
    .unwrap();

    // Verify: after preparation, the descendant does NOT have late.txt
    // (because we didn't merge main during preparation)
    assert!(
        !worktree.join("late.txt").exists(),
        "Before late commit, late.txt should NOT exist"
    );

    // A late commit lands on main AFTER preparation
    let _late_sha = add_commit_to_main(&config, "late.txt", "late commit content");

    // Get main state before squash (includes late commit)
    let clone_dir = config.clone_dir();
    let main_before = run_git_stdout(&clone_dir, &["rev-parse", "refs/heads/main"]).unwrap();
    let main_before = Sha::parse(&main_before).unwrap();

    // Squash merge the predecessor
    let squash_sha = squash_merge_to_main(&config, &pred_sha);

    // Verify the squash parent is main_before (which includes the late commit)
    let parents = super::get_parents(&clone_dir, squash_sha.as_str()).unwrap();
    assert_eq!(
        parents.len(),
        1,
        "Squash commit should have exactly one parent"
    );
    assert_eq!(
        parents[0], main_before,
        "Squash parent should be main before squash (includes late commit)"
    );

    // Verify: before reconciliation, the descendant still doesn't have late.txt
    assert!(
        !worktree.join("late.txt").exists(),
        "Before reconciliation, late.txt should NOT exist"
    );

    // Reconcile with $SQUASH_SHA^ (correct approach)
    // This merges the squash parent (which includes late.txt) into the descendant
    reconcile_descendant(&worktree, "pr-124", &squash_sha, "main", &test_identity()).unwrap();

    // Verify: after reconciliation, late.txt exists
    // The reconciliation merged $SQUASH_SHA^ which includes the late commit
    assert!(
        worktree.join("late.txt").exists(),
        "After reconciliation, late.txt MUST exist - the late commit was incorporated via $SQUASH_SHA^"
    );
    assert_eq!(
        read_file(&worktree, "late.txt"),
        Some("late commit content".to_string()),
        "late.txt should have the correct content"
    );

    // Catch up with main
    catch_up_descendant(&worktree, "pr-124", "main", &test_identity()).unwrap();

    // Verify all content is preserved after catch-up
    assert!(
        worktree.join("late.txt").exists(),
        "late.txt should still exist after catch-up"
    );
    assert!(
        worktree.join("pred.txt").exists(),
        "pred.txt should exist (from predecessor)"
    );
    assert!(
        worktree.join("desc.txt").exists(),
        "desc.txt should exist (from descendant)"
    );
    assert!(
        worktree.join("README.md").exists(),
        "README.md should exist (from initial commit)"
    );
}

/// Conflicts during catch-up are detected and reported.
///
/// When the descendant and main both modify the same file, catch-up should
/// detect the conflict.
#[test]
fn catch_up_detects_conflicts_with_main() {
    let (_temp_dir, config) = create_test_repo();

    // Create predecessor
    let pred_sha = create_branch_with_file(&config, "pr-123", "pred.txt", "predecessor", "main");
    create_pr_ref(&config, 123, &pred_sha);

    // Create descendant that modifies a shared file
    let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
    run_git_sync(&worktree, &["config", "user.email", "test@test.com"]).unwrap();
    run_git_sync(&worktree, &["config", "user.name", "Test"]).unwrap();
    run_git_sync(&worktree, &["checkout", "--detach", "refs/heads/pr-123"]).unwrap();

    // Create a file that will conflict
    std::fs::write(worktree.join("conflict.txt"), "descendant version").unwrap();
    run_git_sync(&worktree, &["add", "conflict.txt"]).unwrap();
    run_git_sync(&worktree, &["commit", "-m", "Descendant adds conflict.txt"]).unwrap();
    run_git_sync(&worktree, &["push", "origin", "HEAD:refs/heads/pr-124"]).unwrap();

    // Squash predecessor
    let squash_sha = squash_merge_to_main(&config, &pred_sha);

    // Main gets a conflicting change
    let main_worktree = config.clone_dir().parent().unwrap().join("main_work");
    std::fs::create_dir_all(&main_worktree).unwrap();
    run_git_sync(
        &config.clone_dir(),
        &[
            "worktree",
            "add",
            "--detach",
            main_worktree.to_str().unwrap(),
            "refs/heads/main",
        ],
    )
    .unwrap();
    run_git_sync(&main_worktree, &["config", "user.email", "test@test.com"]).unwrap();
    run_git_sync(&main_worktree, &["config", "user.name", "Test"]).unwrap();
    std::fs::write(main_worktree.join("conflict.txt"), "main version").unwrap();
    run_git_sync(&main_worktree, &["add", "conflict.txt"]).unwrap();
    run_git_sync(&main_worktree, &["commit", "-m", "Main adds conflict.txt"]).unwrap();
    run_git_sync(&main_worktree, &["push", "origin", "HEAD:refs/heads/main"]).unwrap();
    run_git_sync(
        &config.clone_dir(),
        &[
            "worktree",
            "remove",
            "--force",
            main_worktree.to_str().unwrap(),
        ],
    )
    .unwrap();

    // Prepare and reconcile the descendant (should work) using PR number
    run_git_sync(&worktree, &["checkout", "--detach", "refs/heads/pr-124"]).unwrap();
    prepare_descendant(&worktree, "pr-124", 123, &test_identity()).unwrap();
    reconcile_descendant(&worktree, "pr-124", &squash_sha, "main", &test_identity()).unwrap();

    // Catch-up should detect the conflict
    let result = catch_up_descendant(&worktree, "pr-124", "main", &test_identity()).unwrap();
    assert!(
        result.is_conflict(),
        "Expected conflict during catch-up, got {:?}",
        result
    );
}

// =============================================================================
// Stage-8 Required Tests
// =============================================================================

/// Property 9: Recovery uses frozen descendants, not current state.
///
/// When a cascade is in progress and new descendants are added mid-cascade,
/// the processing must use the frozen set (captured at Preparing phase entry),
/// not the current descendants (which may include late additions).
///
/// This test verifies the core invariant by using the actual `remaining_descendants`
/// function with a CascadePhase that has frozen descendants, demonstrating that
/// late additions to the descendants index are NOT included in processing.
#[test]
fn recovery_uses_frozen_descendants() {
    use crate::state::descendants::{build_descendants_index, remaining_descendants};
    use crate::types::{CachedPr, CascadePhase, DescendantProgress, MergeStateStatus, PrState};
    use std::collections::HashMap;

    // Simulate the frozen descendants captured when entering Preparing phase
    let frozen_prs = vec![PrNumber(101), PrNumber(102)];
    let progress = DescendantProgress::new(frozen_prs.clone());
    let phase = CascadePhase::Preparing { progress };

    // Verify remaining_descendants returns ONLY the frozen set
    let remaining = remaining_descendants(&phase);
    assert_eq!(
        remaining.len(),
        2,
        "Should have exactly 2 remaining descendants"
    );
    assert!(remaining.contains(&PrNumber(101)));
    assert!(remaining.contains(&PrNumber(102)));

    // Now simulate a late addition: create a descendants index that includes pr-103
    // (a PR that declared predecessor during cascade but wasn't in frozen set)
    let mut prs: HashMap<PrNumber, CachedPr> = HashMap::new();
    for &pr in &[101, 102, 103] {
        prs.insert(
            PrNumber(pr),
            CachedPr {
                number: PrNumber(pr),
                predecessor: Some(PrNumber(100)), // All are descendants of pr-100
                head_sha: Sha::parse(&"a".repeat(40)).unwrap(),
                head_ref: format!("pr-{}", pr),
                base_ref: "pr-100".to_string(),
                state: PrState::Open,
                merge_state_status: MergeStateStatus::Unknown,
                is_draft: false,
                closed_at: None,
                predecessor_squash_reconciled: None,
            },
        );
    }

    let descendants_index = build_descendants_index(&prs);

    // The descendants index now shows 3 descendants of pr-100
    let current_descendants = descendants_index.get(&PrNumber(100)).unwrap();
    assert_eq!(
        current_descendants.len(),
        3,
        "Current index has 3 descendants (including late addition)"
    );
    assert!(current_descendants.contains(&PrNumber(103)));

    // CRITICAL INVARIANT: remaining_descendants still returns only the frozen set
    // It does NOT use the descendants_index, so pr-103 is NOT included
    let remaining_after_late_addition = remaining_descendants(&phase);
    assert_eq!(
        remaining_after_late_addition.len(),
        2,
        "remaining_descendants must use frozen set, not current index"
    );
    assert!(
        !remaining_after_late_addition.contains(&PrNumber(103)),
        "Late descendant (pr-103) must NOT be in remaining - recovery must use frozen set"
    );

    // Also test with real git operations to show the late descendant is not processed
    let (_temp_dir, config) = create_test_repo();

    // Create predecessor and initial descendants
    let pred_sha = create_branch_with_file(&config, "pr-100", "pred.txt", "predecessor", "main");
    create_pr_ref(&config, 100, &pred_sha);
    let desc1_sha = create_branch_with_file(&config, "pr-101", "desc1.txt", "desc1", "pr-100");
    let desc2_sha = create_branch_with_file(&config, "pr-102", "desc2.txt", "desc2", "pr-100");
    create_pr_ref(&config, 101, &desc1_sha);
    create_pr_ref(&config, 102, &desc2_sha);

    // Prepare the frozen descendants
    let worktree = worktree_for_stack(&config, PrNumber(100)).unwrap();
    for &pr in &[101, 102] {
        let branch = format!("pr-{}", pr);
        prepare_descendant(&worktree, &branch, 100, &test_identity()).unwrap();
        run_git_sync(
            &worktree,
            &[
                "push",
                "origin",
                &format!("HEAD:refs/heads/{}", branch),
                "--force",
            ],
        )
        .unwrap();
    }

    // Late addition arrives (would be via predecessor_declared event)
    let _late_sha = create_branch_with_file(&config, "pr-103", "late.txt", "late", "pr-100");

    // Squash the predecessor
    let squash_sha = squash_merge_to_main(&config, &pred_sha);

    // Recovery: use remaining_descendants to get what to process
    let to_reconcile = remaining_descendants(&phase);
    assert!(
        !to_reconcile.contains(&PrNumber(103)),
        "Late addition must not be in remaining"
    );

    // Reconcile only the frozen descendants
    for &pr in &to_reconcile {
        let branch = format!("pr-{}", pr.0);
        reconcile_descendant(&worktree, &branch, &squash_sha, "main", &test_identity()).unwrap();
    }

    // Verify late descendant was NOT processed
    let worktree2 = worktree_for_stack(&config, PrNumber(103)).unwrap();
    run_git_sync(&worktree2, &["fetch", "origin", "pr-103"]).unwrap();
    run_git_sync(&worktree2, &["checkout", "--detach", "origin/pr-103"]).unwrap();

    let head_sha = super::rev_parse(&worktree2, "HEAD").unwrap();
    let squash_is_ancestor = is_ancestor(&worktree2, &squash_sha, &head_sha).unwrap_or(false);
    assert!(
        !squash_is_ancestor,
        "Late descendant must NOT have squash as ancestor (it was never reconciled)"
    );
}

/// Property 8: Fan-out worktree ordering.
///
/// When a PR with multiple descendants completes (fan-out), the old worktree
/// should be cleaned up and new worktrees created for each descendant that becomes
/// a new root. This test verifies:
/// 1. The old root's worktree can be removed after cascade completion
/// 2. New worktrees can be created for fan-out descendants
/// 3. The cleanup/creation ordering is correct (remove before create to avoid conflicts)
#[test]
fn fanout_worktree_ordering() {
    let (_temp_dir, config) = create_test_repo();

    // Create a root PR that will fan out to multiple descendants
    let root_sha = create_branch_with_file(&config, "pr-100", "root.txt", "root", "main");
    create_pr_ref(&config, 100, &root_sha);

    // Create multiple descendants from the root (fan-out scenario)
    let desc1_sha = create_branch_with_file(&config, "pr-101", "desc1.txt", "desc1", "pr-100");
    let desc2_sha = create_branch_with_file(&config, "pr-102", "desc2.txt", "desc2", "pr-100");
    let desc3_sha = create_branch_with_file(&config, "pr-103", "desc3.txt", "desc3", "pr-100");

    // Get the root worktree
    let root_worktree = worktree_for_stack(&config, PrNumber(100)).unwrap();
    assert!(root_worktree.exists(), "Root worktree should exist");

    // Record root worktree path for later verification
    let root_worktree_path = root_worktree.clone();

    // Prepare all descendants in the root's worktree
    create_pr_ref(&config, 101, &desc1_sha);
    create_pr_ref(&config, 102, &desc2_sha);
    create_pr_ref(&config, 103, &desc3_sha);

    for pr in [101, 102, 103] {
        let branch = format!("pr-{}", pr);
        prepare_descendant(&root_worktree, &branch, 100, &test_identity()).unwrap();
        run_git_sync(
            &root_worktree,
            &[
                "push",
                "origin",
                &format!("HEAD:refs/heads/{}", branch),
                "--force",
            ],
        )
        .unwrap();
    }

    // Squash merge the root
    let squash_sha = squash_merge_to_main(&config, &root_sha);

    // Reconcile all descendants
    for pr in [101, 102, 103] {
        let branch = format!("pr-{}", pr);
        reconcile_descendant(
            &root_worktree,
            &branch,
            &squash_sha,
            "main",
            &test_identity(),
        )
        .unwrap();
        run_git_sync(
            &root_worktree,
            &[
                "push",
                "origin",
                &format!("HEAD:refs/heads/{}", branch),
                "--force",
            ],
        )
        .unwrap();
    }

    // Verify initial worktree list contains root
    let initial_worktrees = list_worktrees(&config).unwrap();
    let initial_prs: HashSet<PrNumber> = initial_worktrees.iter().map(|(pr, _)| *pr).collect();
    assert!(
        initial_prs.contains(&PrNumber(100)),
        "Root worktree (pr-100) should exist before cleanup"
    );

    // === KEY TEST: Remove the old root worktree ===
    // After fan-out completion, the root PR is merged and its worktree should be cleaned up.
    // This simulates the cleanup that happens when a train completes.

    // Mark only the fan-out descendants as active (the root is no longer active)
    let active_after_fanout: HashSet<PrNumber> = [PrNumber(101), PrNumber(102), PrNumber(103)]
        .into_iter()
        .collect();

    // Use zero max_age to ensure the old root worktree is considered stale
    let mut config_cleanup = config.clone();
    config_cleanup.worktree_max_age = Duration::ZERO;

    // Cleanup stale worktrees (the root)
    let removed = cleanup_stale_worktrees(&config_cleanup, &active_after_fanout).unwrap();

    // Verify root worktree was removed
    assert!(
        removed.contains(&PrNumber(100)),
        "Root worktree (pr-100) should be removed during fan-out cleanup"
    );
    assert!(
        !root_worktree_path.exists(),
        "Root worktree directory should no longer exist after cleanup"
    );

    // Verify worktree list no longer contains root
    let after_cleanup_worktrees = list_worktrees(&config).unwrap();
    let after_cleanup_prs: HashSet<PrNumber> =
        after_cleanup_worktrees.iter().map(|(pr, _)| *pr).collect();
    assert!(
        !after_cleanup_prs.contains(&PrNumber(100)),
        "Root worktree should not be in list after cleanup"
    );

    // === Now create worktrees for the fan-out descendants (new roots) ===
    // After cleanup, each descendant can become a new root with its own worktree.

    let desc_worktrees: Vec<PathBuf> = [101, 102, 103]
        .iter()
        .map(|&pr| worktree_for_stack(&config, PrNumber(pr)).unwrap())
        .collect();

    // All new worktrees should exist
    for (i, worktree) in desc_worktrees.iter().enumerate() {
        assert!(
            worktree.exists(),
            "Fan-out worktree {} should exist",
            101 + i
        );
    }

    // Verify each descendant worktree can checkout its branch
    for (i, worktree) in desc_worktrees.iter().enumerate() {
        let branch = format!("pr-{}", 101 + i);
        run_git_sync(worktree, &["fetch", "origin", &branch]).unwrap();
        let result = run_git_sync(
            worktree,
            &["checkout", "--detach", &format!("origin/{}", branch)],
        );
        assert!(
            result.is_ok(),
            "Worktree {} should be able to checkout its branch",
            101 + i
        );
    }

    // Verify final worktree list contains all fan-out descendants
    let final_worktrees = list_worktrees(&config).unwrap();
    let final_prs: HashSet<PrNumber> = final_worktrees.iter().map(|(pr, _)| *pr).collect();
    for pr in [101, 102, 103] {
        assert!(
            final_prs.contains(&PrNumber(pr)),
            "Fan-out descendant pr-{} should have a worktree",
            pr
        );
    }
}

/// Worktree cleanup leaves no orphans.
///
/// After cleanup, there should be no worktrees that are:
/// - Not associated with an active train
/// - Older than max_age
///
/// This test creates multiple worktrees and verifies cleanup removes exactly
/// those that should be removed.
#[test]
fn worktree_cleanup_leaves_no_orphans() {
    let (_temp_dir, config) = create_test_repo();

    // Create several worktrees
    let worktrees_to_create: Vec<u64> = vec![100, 101, 102, 103, 104];
    for pr in &worktrees_to_create {
        worktree_for_stack(&config, PrNumber(*pr)).unwrap();
    }

    // Verify all were created
    let initial_list = list_worktrees(&config).unwrap();
    assert_eq!(
        initial_list.len(),
        worktrees_to_create.len(),
        "All worktrees should be created"
    );

    // Mark some as "active" (simulating active trains)
    let active: HashSet<PrNumber> = vec![PrNumber(101), PrNumber(103)].into_iter().collect();

    // Use zero max_age to mark all non-active as stale
    let mut config_zero = config.clone();
    config_zero.worktree_max_age = Duration::ZERO;

    // Cleanup stale worktrees
    let removed = cleanup_stale_worktrees(&config_zero, &active).unwrap();

    // Verify correct number removed (all except active ones)
    let expected_removed: HashSet<PrNumber> = worktrees_to_create
        .iter()
        .map(|&pr| PrNumber(pr))
        .filter(|pr| !active.contains(pr))
        .collect();

    let removed_set: HashSet<PrNumber> = removed.into_iter().collect();
    assert_eq!(
        removed_set, expected_removed,
        "Should remove exactly non-active worktrees"
    );

    // Verify remaining worktrees are exactly the active ones
    let remaining = list_worktrees(&config).unwrap();
    let remaining_prs: HashSet<PrNumber> = remaining.into_iter().map(|(pr, _)| pr).collect();

    assert_eq!(remaining_prs, active, "Only active worktrees should remain");

    // The invariant: no orphans remain
    // An orphan would be a worktree that is not active but wasn't removed
    let orphans: Vec<_> = remaining_prs
        .iter()
        .filter(|pr| !active.contains(pr))
        .collect();
    assert!(
        orphans.is_empty(),
        "No orphaned worktrees should remain: {:?}",
        orphans
    );
}

/// Regression test: Naive approach (merging main during preparation) loses late commits.
///
/// This test demonstrates WHY we use `$SQUASH_SHA^` during reconciliation rather than
/// merging main during preparation. The naive approach would:
/// 1. During preparation: merge current main into descendant
/// 2. Late commit lands on main
/// 3. Predecessor is squashed
/// 4. Reconciliation doesn't incorporate the late commit
///
/// The correct approach (using `$SQUASH_SHA^`) ensures the late commit is incorporated
/// because `$SQUASH_SHA^` IS main at the time of the squash, which includes the late commit.
#[test]
fn naive_ordering_loses_late_commits() {
    let (_temp_dir, config) = create_test_repo();

    // Create predecessor and descendant
    let pred_sha = create_branch_with_file(&config, "pr-123", "pred.txt", "predecessor", "main");
    create_pr_ref(&config, 123, &pred_sha);
    let _desc_sha = create_branch_with_file(
        &config,
        "pr-124",
        "desc.txt",
        "descendant content",
        "pr-123",
    );

    // Get worktree
    let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();

    // === NAIVE APPROACH ===
    // The naive approach would prepare by merging main into the descendant.
    // Simulate this by explicitly merging main during preparation phase.
    run_git_sync(&worktree, &["checkout", "--detach", "refs/heads/pr-124"]).unwrap();
    run_git_sync(&worktree, &["fetch", "origin", "main"]).unwrap();
    run_git_sync(
        &worktree,
        &[
            "merge",
            "origin/main",
            "-m",
            "Naive: merge main during prep",
        ],
    )
    .unwrap();

    // Save the "naively prepared" state
    let naive_prep_sha = super::rev_parse(&worktree, "HEAD").unwrap();

    // Push naive prep (this would be what the naive approach would do)
    run_git_sync(
        &worktree,
        &[
            "push",
            "origin",
            &format!("{}:refs/heads/pr-124-naive", naive_prep_sha),
            "--force",
        ],
    )
    .unwrap();

    // Late commit lands on main AFTER the naive preparation
    let late_content = "LATE COMMIT - this must not be lost!";
    let _late_sha = add_commit_to_main(&config, "late.txt", late_content);

    // Squash merge predecessor (main now includes late commit)
    let squash_sha = squash_merge_to_main(&config, &pred_sha);

    // === NAIVE RECONCILIATION ===
    // A naive reconciliation would just rebase onto the squash or merge squash^
    // without considering late commits. Let's see if the naive prep has late.txt.
    run_git_sync(
        &worktree,
        &["checkout", "--detach", "refs/heads/pr-124-naive"],
    )
    .unwrap();

    // The naive preparation does NOT have the late commit!
    assert!(
        !worktree.join("late.txt").exists(),
        "NAIVE APPROACH BUG: The naive preparation (merging main during prep) \
         does not have the late commit that landed after preparation"
    );

    // === CORRECT APPROACH ===
    // Now let's do it correctly: prepare without merging main, then reconcile with $SQUASH_SHA^

    // Start fresh from the original descendant
    run_git_sync(&worktree, &["checkout", "--detach", "refs/heads/pr-124"]).unwrap();

    // Correct preparation: just merge predecessor, NOT main
    prepare_descendant(&worktree, "pr-124", 123, &test_identity()).unwrap();

    // At this point, we still don't have late.txt (correct - it hasn't landed on our branch)
    assert!(
        !worktree.join("late.txt").exists(),
        "Before reconciliation, late.txt should not exist (expected behavior)"
    );

    // Correct reconciliation: uses $SQUASH_SHA^ which IS main at squash time (includes late commit)
    reconcile_descendant(&worktree, "pr-124", &squash_sha, "main", &test_identity()).unwrap();

    // The correct approach DOES have the late commit!
    assert!(
        worktree.join("late.txt").exists(),
        "CORRECT APPROACH: Reconciliation with $SQUASH_SHA^ incorporates the late commit"
    );
    assert_eq!(
        read_file(&worktree, "late.txt"),
        Some(late_content.to_string()),
        "late.txt should have correct content"
    );
}
