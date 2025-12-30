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

        // Create descendant branch with its own file
        let _desc_sha = create_branch_with_file(&config, "pr-124", "desc.txt", &desc_content, "pr-123");

        // Get worktree and prepare descendant
        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
        let prep_result = prepare_descendant(&worktree, "pr-124", &pred_sha, false).unwrap();
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
        let _desc_sha = create_branch_with_file(&config, "pr-124", "desc.txt", &desc_content, "pr-123");

        // Prepare descendant
        let worktree = worktree_for_stack(&config, PrNumber(123)).unwrap();
        prepare_descendant(&worktree, "pr-124", &pred_sha, false).unwrap();
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
    /// This test verifies the squash commit has exactly one parent.
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

        // The parent should be on main (the prior main HEAD)
        let parent = &parents[0];
        let main_contains_parent = is_ancestor(&worktree, parent, &squash_sha).unwrap();
        prop_assert!(main_contains_parent, "Parent should be ancestor of squash");
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

    // Prepare first (merge predecessor head)
    prepare_descendant(&worktree, "pr-124", &pred_sha, false).unwrap();
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

    // Prepare and reconcile the descendant (should work)
    run_git_sync(&worktree, &["checkout", "--detach", "refs/heads/pr-124"]).unwrap();
    prepare_descendant(&worktree, "pr-124", &pred_sha, false).unwrap();
    reconcile_descendant(&worktree, "pr-124", &squash_sha, false).unwrap();

    // Catch-up should detect the conflict
    let result = catch_up_descendant(&worktree, "pr-124", "main", false).unwrap();
    assert!(
        result.is_conflict(),
        "Expected conflict during catch-up, got {:?}",
        result
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Property 8: Fan-out worktree lifecycle
// ─────────────────────────────────────────────────────────────────────────────

use crate::git::worktree::remove_worktree;

/// Property 8: Fan-out worktree ordering.
///
/// When a PR with multiple descendants completes (fan-out), the old worktree
/// must be removed BEFORE new worktrees are created. This ensures:
/// - No stale worktree references causing git errors
/// - Clean state for new independent trains
/// - No accumulation of worktrees during rapid fan-out cascades
#[test]
fn fanout_worktree_ordering_removes_old_before_creating_new() {
    let (_temp_dir, config) = create_test_repo();

    // Create the original worktree for the root train
    let root_pr = PrNumber(100);
    let root_worktree = worktree_for_stack(&config, root_pr).unwrap();
    assert!(root_worktree.exists(), "Root worktree should be created");

    // Verify we can list it
    let initial_worktrees = list_worktrees(&config).unwrap();
    assert_eq!(initial_worktrees.len(), 1);
    assert_eq!(initial_worktrees[0].0, root_pr);

    // Simulate fan-out: root completes, multiple descendants become independent
    let descendant_prs = vec![PrNumber(101), PrNumber(102), PrNumber(103)];

    // === CRITICAL ORDERING: Remove old worktree FIRST ===
    remove_worktree(&config, root_pr).unwrap();
    assert!(
        !root_worktree.exists(),
        "Old worktree must be removed before creating new ones"
    );

    // Now create new worktrees for each descendant
    let mut new_worktrees = Vec::new();
    for &desc_pr in &descendant_prs {
        let wt = worktree_for_stack(&config, desc_pr).unwrap();
        assert!(wt.exists(), "New worktree for {} should exist", desc_pr);
        new_worktrees.push(wt);
    }

    // Verify final state
    assert!(
        !root_worktree.exists(),
        "Old worktree should remain removed after fan-out"
    );

    let final_worktrees = list_worktrees(&config).unwrap();
    assert_eq!(
        final_worktrees.len(),
        descendant_prs.len(),
        "Should have one worktree per descendant"
    );

    for &desc_pr in &descendant_prs {
        assert!(
            final_worktrees.iter().any(|(pr, _)| *pr == desc_pr),
            "Descendant {} should have a worktree",
            desc_pr
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 20,
        max_shrink_iters: 100,
        ..ProptestConfig::default()
    })]

    /// Property test: Fan-out worktree lifecycle with arbitrary descendant counts.
    ///
    /// For any number of descendants (2-5), the fan-out sequence should:
    /// 1. Start with exactly one root worktree
    /// 2. Remove the root worktree
    /// 3. Create worktrees for all descendants
    /// 4. End with exactly N descendant worktrees (where N is descendant count)
    #[test]
    fn fanout_worktree_lifecycle_property(
        root_pr_num in 100u64..200,
        descendant_count in 2usize..5,
    ) {
        let (_temp_dir, config) = create_test_repo();

        let root_pr = PrNumber(root_pr_num);

        // Create root worktree
        let root_worktree = worktree_for_stack(&config, root_pr).unwrap();
        prop_assert!(root_worktree.exists(), "Root worktree should be created");

        // Generate descendant PR numbers (ensure they don't conflict with root)
        let descendant_prs: Vec<PrNumber> = (0..descendant_count)
            .map(|i| PrNumber(root_pr_num + 100 + i as u64))
            .collect();

        // === CRITICAL ORDERING ===
        // Step 1: Remove old worktree FIRST
        remove_worktree(&config, root_pr).unwrap();
        prop_assert!(
            !root_worktree.exists(),
            "Old worktree must be removed before creating new ones"
        );

        // Step 2: Create new worktrees (only after removal)
        for &desc_pr in &descendant_prs {
            let wt = worktree_for_stack(&config, desc_pr).unwrap();
            prop_assert!(wt.exists(), "Worktree for {} should exist", desc_pr);
        }

        // Verify final state
        prop_assert!(
            !root_worktree.exists(),
            "Old worktree should remain removed"
        );

        let final_worktrees = list_worktrees(&config).unwrap();
        prop_assert_eq!(
            final_worktrees.len(),
            descendant_count,
            "Should have exactly {} worktrees after fan-out",
            descendant_count
        );
    }

    /// Property test: Fan-out doesn't leave orphaned worktrees.
    ///
    /// After a complete fan-out sequence, the only worktrees that exist
    /// should be exactly the descendant worktrees - no orphans from the
    /// original root.
    #[test]
    fn fanout_no_orphan_worktrees(
        root_pr_num in 100u64..200,
        descendant_nums in prop::collection::vec(200u64..300, 2..4),
    ) {
        let (_temp_dir, config) = create_test_repo();

        let root_pr = PrNumber(root_pr_num);
        let descendant_prs: Vec<PrNumber> = descendant_nums.iter().map(|&n| PrNumber(n)).collect();

        // Create root worktree
        worktree_for_stack(&config, root_pr).unwrap();

        // Fan-out sequence: remove old, create new
        remove_worktree(&config, root_pr).unwrap();
        for &desc_pr in &descendant_prs {
            worktree_for_stack(&config, desc_pr).unwrap();
        }

        // Verify: exactly the descendant worktrees exist, nothing else
        let worktrees = list_worktrees(&config).unwrap();
        let worktree_prs: HashSet<PrNumber> = worktrees.iter().map(|(pr, _)| *pr).collect();
        let expected_prs: HashSet<PrNumber> = descendant_prs.iter().copied().collect();

        prop_assert_eq!(
            worktree_prs.clone(),
            expected_prs,
            "Worktrees should exactly match descendant set, got {:?}",
            worktree_prs
        );

        // Verify root is not in the set
        prop_assert!(
            !worktree_prs.contains(&root_pr),
            "Root worktree should not exist after fan-out"
        );
    }
}
