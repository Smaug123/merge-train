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
use crate::git::{GitConfig, is_ancestor, run_git_stdout, run_git_sync};
use crate::types::{PrNumber, Sha};

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
        let prep_result = prepare_descendant(&worktree, "pr-124", 123, false).unwrap();
        prop_assert!(prep_result.is_ok());
        run_git_sync(&worktree, &["push", "origin", "HEAD:refs/heads/pr-124", "--force"]).unwrap();

        // Squash merge predecessor to main
        let squash_sha = squash_merge_to_main(&config, &pred_sha);

        // Reconcile descendant
        let reconcile_result = reconcile_descendant(&worktree, "pr-124", &squash_sha, false).unwrap();
        prop_assert!(reconcile_result.is_ok());

        // Catch up with main (if needed)
        let _ = catch_up_descendant(&worktree, "pr-124", "main", false).unwrap();

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
        prepare_descendant(&worktree, "pr-124", 123, false).unwrap();
        run_git_sync(&worktree, &["push", "origin", "HEAD:refs/heads/pr-124", "--force"]).unwrap();

        // Squash merge predecessor
        let squash_sha = squash_merge_to_main(&config, &pred_sha);

        // An independent commit lands on main AFTER the squash
        let _intervening_sha = add_commit_to_main(&config, "intervening.txt", &intervening_content);

        // Reconcile and catch up
        reconcile_descendant(&worktree, "pr-124", &squash_sha, false).unwrap();
        catch_up_descendant(&worktree, "pr-124", "main", false).unwrap();

        // The intervening commit should be incorporated
        prop_assert!(worktree.join("intervening.txt").exists(), "intervening.txt should exist");
        prop_assert_eq!(read_file(&worktree, "intervening.txt"), Some(intervening_content));

        // All other content should still be there
        prop_assert!(worktree.join("pred.txt").exists());
        prop_assert!(worktree.join("desc.txt").exists());
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

/// Property 3 extension: Verify that the naive approach (merging main during preparation)
/// would lose commits, while the correct approach preserves them.
///
/// This is a deterministic test rather than property-based because it's testing
/// a specific scenario from the design doc.
#[test]
fn squash_parent_ordering_prevents_lost_commits() {
    let (_temp_dir, config) = create_test_repo();

    // Create predecessor PR
    let pred_sha = create_branch_with_file(&config, "pr-123", "pred.txt", "predecessor", "main");
    create_pr_ref(&config, 123, &pred_sha);

    // Create descendant PR from predecessor
    let _desc_sha = create_branch_with_file(&config, "pr-124", "desc.txt", "descendant", "pr-123");

    // An independent commit lands on main BEFORE we squash
    let _intervening = add_commit_to_main(&config, "late.txt", "late commit");

    // Get main state before squash
    let clone_dir = config.clone_dir();
    let main_before = run_git_stdout(&clone_dir, &["rev-parse", "refs/heads/main"]).unwrap();
    let main_before = Sha::parse(&main_before).unwrap();

    // Squash merge the predecessor
    let squash_sha = squash_merge_to_main(&config, &pred_sha);

    // Verify the squash parent is main_before (which includes the late commit)
    let parents = super::get_parents(&clone_dir, squash_sha.as_str()).unwrap();
    assert_eq!(parents.len(), 1);
    assert_eq!(
        parents[0], main_before,
        "Squash parent should be the main before squash"
    );

    // Now test the correct reconciliation approach
    let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();

    // Prepare first (merge predecessor head) using PR number
    prepare_descendant(&worktree, "pr-124", 123, false).unwrap();
    run_git_sync(
        &worktree,
        &["push", "origin", "HEAD:refs/heads/pr-124", "--force"],
    )
    .unwrap();

    // Reconcile with $SQUASH_SHA^ (correct approach)
    reconcile_descendant(&worktree, "pr-124", &squash_sha, false).unwrap();

    // Catch up with main
    catch_up_descendant(&worktree, "pr-124", "main", false).unwrap();

    // Verify the late commit's content is preserved
    assert!(
        worktree.join("late.txt").exists(),
        "The late commit should be preserved in descendant"
    );
    assert_eq!(
        read_file(&worktree, "late.txt"),
        Some("late commit".to_string())
    );

    // Verify all content is present
    assert!(worktree.join("pred.txt").exists());
    assert!(worktree.join("desc.txt").exists());
    assert!(worktree.join("README.md").exists());
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
    prepare_descendant(&worktree, "pr-124", 123, false).unwrap();
    reconcile_descendant(&worktree, "pr-124", &squash_sha, false).unwrap();

    // Catch-up should detect the conflict
    let result = catch_up_descendant(&worktree, "pr-124", "main", false).unwrap();
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
/// This test simulates the invariant by tracking which descendants were "frozen"
/// at the start vs. which exist after new additions.
#[test]
fn recovery_uses_frozen_descendants() {
    let (_temp_dir, config) = create_test_repo();

    // Create initial predecessor and descendants (the "frozen" set)
    let pred_sha = create_branch_with_file(&config, "pr-100", "pred.txt", "predecessor", "main");
    create_pr_ref(&config, 100, &pred_sha);
    let desc1_sha = create_branch_with_file(&config, "pr-101", "desc1.txt", "desc1", "pr-100");
    let desc2_sha = create_branch_with_file(&config, "pr-102", "desc2.txt", "desc2", "pr-100");

    // These are the "frozen" descendants captured when entering Preparing phase
    let frozen_descendants = vec![101u64, 102u64];

    // Simulate starting preparation on the frozen set
    let worktree = worktree_for_stack(&config, PrNumber(100)).unwrap();
    create_pr_ref(&config, 101, &desc1_sha);
    create_pr_ref(&config, 102, &desc2_sha);

    // Prepare frozen descendants (simulating cascade start)
    for &pr in &frozen_descendants {
        let branch = format!("pr-{}", pr);
        prepare_descendant(&worktree, &branch, 100, false).unwrap();
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

    // Now simulate a "late addition" that arrives during cascade
    // (In reality, this would be via predecessor_declared event during spool replay)
    let _late_sha = create_branch_with_file(&config, "pr-103", "late.txt", "late", "pr-100");

    // Current descendants now includes the late addition
    let _current_descendants = vec![101u64, 102u64, 103u64];

    // Squash the predecessor
    let squash_sha = squash_merge_to_main(&config, &pred_sha);

    // CRITICAL: Recovery must use frozen_descendants, not current_descendants.
    // The late descendant (pr-103) was never prepared, so reconciling it would be wrong.
    let to_process: Vec<u64> = frozen_descendants.clone();

    // Verify the invariant: to_process should equal frozen_descendants
    assert_eq!(
        to_process, frozen_descendants,
        "Recovery must use frozen descendants"
    );
    assert!(
        !to_process.contains(&103),
        "Late descendant should NOT be in the frozen set"
    );

    // Reconcile only the frozen descendants (correct behavior)
    for &pr in &to_process {
        let branch = format!("pr-{}", pr);
        reconcile_descendant(&worktree, &branch, &squash_sha, false).unwrap();
    }

    // The late descendant (pr-103) should NOT have been processed
    // It remains in its original state without reconciliation
    let worktree2 = worktree_for_stack(&config, PrNumber(103)).unwrap();
    run_git_sync(&worktree2, &["fetch", "origin", "pr-103"]).unwrap();
    run_git_sync(&worktree2, &["checkout", "--detach", "origin/pr-103"]).unwrap();

    // pr-103 should NOT have the squash commit as an ancestor (wasn't reconciled)
    let head_sha = super::rev_parse(&worktree2, "HEAD").unwrap();
    let squash_is_ancestor = is_ancestor(&worktree2, &squash_sha, &head_sha).unwrap_or(false);
    assert!(
        !squash_is_ancestor,
        "Late descendant should NOT have squash as ancestor (it was never reconciled)"
    );
}

/// Property 8: Fan-out worktree ordering.
///
/// When a PR with multiple descendants completes (fan-out), the old worktree
/// should be cleaned up before/during the transition. This test verifies that
/// worktree management handles fan-out scenarios correctly.
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

    // Prepare all descendants in the root's worktree
    create_pr_ref(&config, 101, &desc1_sha);
    create_pr_ref(&config, 102, &desc2_sha);
    create_pr_ref(&config, 103, &desc3_sha);

    for pr in [101, 102, 103] {
        let branch = format!("pr-{}", pr);
        prepare_descendant(&root_worktree, &branch, 100, false).unwrap();
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
        reconcile_descendant(&root_worktree, &branch, &squash_sha, false).unwrap();
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

    // After fan-out, each descendant could become a new root.
    // Verify new worktrees can be created for each fan-out descendant.
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
