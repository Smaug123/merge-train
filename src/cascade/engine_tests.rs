//! Unit tests and regression tests for the cascade engine.
//!
//! Property-based specification tests remain in `engine.rs` alongside the implementation.
//! This file contains:
//! - Example-based unit tests for API behavior
//! - Regression tests for specific bugs
//! - Property-based tests for internal helper functions

use super::*;
use crate::types::{CommentId, DescendantProgress, PrState, Sha, TrainState};

// ─────────────────────────────────────────────────────────────────────────────
// Test Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn make_sha(n: u64) -> Sha {
    Sha::parse(format!("{:0>40x}", n)).unwrap()
}

fn make_pr(
    number: u64,
    base_ref: &str,
    predecessor: Option<u64>,
    state: PrState,
    merge_state_status: MergeStateStatus,
) -> CachedPr {
    CachedPr::new(
        PrNumber(number),
        make_sha(number),
        format!("branch-{}", number),
        base_ref.to_string(),
        predecessor.map(PrNumber),
        state,
        merge_state_status,
        false,
    )
}

fn make_open_pr(number: u64, base_ref: &str, predecessor: Option<u64>) -> CachedPr {
    make_pr(
        number,
        base_ref,
        predecessor,
        PrState::Open,
        MergeStateStatus::Clean,
    )
}

// ─────────────────────────────────────────────────────────────────────────────
// Unit Tests
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn start_train_succeeds_for_valid_root() {
    let engine = CascadeEngine::new("main");

    let pr = make_open_pr(1, "main", None);
    let prs = HashMap::from([(PrNumber(1), pr)]);
    let active_trains = HashMap::new();

    let result = engine.start_train(PrNumber(1), &prs, &active_trains);

    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(result.train.original_root_pr, PrNumber(1));
    assert_eq!(result.train.current_pr, PrNumber(1));
    assert_eq!(result.train.state, TrainState::Running);
    assert_eq!(result.stack.root(), Some(PrNumber(1)));
}

#[test]
fn start_train_fails_for_non_root() {
    let engine = CascadeEngine::new("main");

    // PR targets feature branch, not main
    let pr = make_open_pr(1, "feature", None);
    let prs = HashMap::from([(PrNumber(1), pr)]);
    let active_trains = HashMap::new();

    let result = engine.start_train(PrNumber(1), &prs, &active_trains);

    assert!(matches!(result, Err(CascadeError::NotARoot(_, _))));
}

#[test]
fn start_train_fails_for_existing_train() {
    let engine = CascadeEngine::new("main");

    let pr = make_open_pr(1, "main", None);
    let prs = HashMap::from([(PrNumber(1), pr)]);

    let existing_train = TrainRecord::new(PrNumber(1));
    let active_trains = HashMap::from([(PrNumber(1), existing_train)]);

    let result = engine.start_train(PrNumber(1), &prs, &active_trains);

    assert!(matches!(result, Err(CascadeError::TrainAlreadyExists(_))));
}

#[test]
fn start_train_includes_descendants_in_stack() {
    let engine = CascadeEngine::new("main");

    // main <- #1 <- #2 <- #3
    let pr1 = make_open_pr(1, "main", None);
    let mut pr2 = make_open_pr(2, "branch-1", Some(1));
    pr2.head_ref = "branch-2".to_string();
    let mut pr3 = make_open_pr(3, "branch-2", Some(2));
    pr3.head_ref = "branch-3".to_string();

    let prs = HashMap::from([(PrNumber(1), pr1), (PrNumber(2), pr2), (PrNumber(3), pr3)]);
    let active_trains = HashMap::new();

    let result = engine.start_train(PrNumber(1), &prs, &active_trains);

    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(
        result.stack.prs,
        vec![PrNumber(1), PrNumber(2), PrNumber(3)]
    );
}

#[test]
fn stop_train_succeeds_for_active_train() {
    let engine = CascadeEngine::new("main");

    let train = TrainRecord::new(PrNumber(1));
    let result = engine.stop_train(train, false);

    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(result.train.state, TrainState::Stopped);
    assert!(result.train.ended_at.is_some());
}

#[test]
fn stop_train_fails_for_inactive_train_without_force() {
    let engine = CascadeEngine::new("main");

    let mut train = TrainRecord::new(PrNumber(1));
    train.stop();

    let result = engine.stop_train(train, false);

    assert!(matches!(result, Err(CascadeError::InvalidState(_, _, _))));
}

#[test]
fn stop_train_succeeds_for_inactive_train_with_force() {
    let engine = CascadeEngine::new("main");

    let mut train = TrainRecord::new(PrNumber(1));
    train.stop();

    let result = engine.stop_train(train, true);

    assert!(result.is_ok());
}

#[test]
fn evaluate_train_returns_proceed_for_clean_pr() {
    let engine = CascadeEngine::new("main");

    let train = TrainRecord::new(PrNumber(1));
    let pr = make_open_pr(1, "main", None);
    let prs = HashMap::from([(PrNumber(1), pr)]);

    let action = engine.evaluate_train(&train, &prs);

    assert_eq!(action, TrainAction::Proceed);
}

#[test]
fn evaluate_train_returns_block_for_blocked_pr() {
    let engine = CascadeEngine::new("main");

    let train = TrainRecord::new(PrNumber(1));
    let pr = make_pr(1, "main", None, PrState::Open, MergeStateStatus::Blocked);
    let prs = HashMap::from([(PrNumber(1), pr)]);

    let action = engine.evaluate_train(&train, &prs);

    assert!(matches!(
        action,
        TrainAction::Block {
            reason: BlockReason::Blocked
        }
    ));
}

#[test]
fn evaluate_train_returns_abort_for_closed_pr() {
    let engine = CascadeEngine::new("main");

    let train = TrainRecord::new(PrNumber(1));
    let pr = make_pr(1, "main", None, PrState::Closed, MergeStateStatus::Clean);
    let prs = HashMap::from([(PrNumber(1), pr)]);

    let action = engine.evaluate_train(&train, &prs);

    assert!(matches!(
        action,
        TrainAction::Abort {
            reason: AbortReason::PrClosed
        }
    ));
}

#[test]
fn evaluate_train_returns_abort_for_dirty_pr() {
    let engine = CascadeEngine::new("main");

    let train = TrainRecord::new(PrNumber(1));
    let pr = make_pr(1, "main", None, PrState::Open, MergeStateStatus::Dirty);
    let prs = HashMap::from([(PrNumber(1), pr)]);

    let action = engine.evaluate_train(&train, &prs);

    assert!(matches!(
        action,
        TrainAction::Abort {
            reason: AbortReason::MergeConflict { .. }
        }
    ));
}

#[test]
fn evaluate_train_returns_block_for_behind_root() {
    // TODO: BEHIND roots should eventually be updated (merge main, push) instead of blocked.
    // For now, we treat them the same as non-root BEHIND PRs.
    let engine = CascadeEngine::new("main");

    let train = TrainRecord::new(PrNumber(1));
    let pr = make_pr(1, "main", None, PrState::Open, MergeStateStatus::Behind);
    let prs = HashMap::from([(PrNumber(1), pr)]);

    let action = engine.evaluate_train(&train, &prs);

    assert_eq!(
        action,
        TrainAction::Block {
            reason: BlockReason::Behind
        }
    );
}

#[test]
fn compute_frozen_descendants_returns_open_descendants() {
    let engine = CascadeEngine::new("main");

    let pr1 = make_open_pr(1, "main", None);
    let mut pr2 = make_open_pr(2, "branch-1", Some(1));
    pr2.head_ref = "branch-2".to_string();
    // pr3 is closed, should not be included
    let mut pr3 = make_pr(
        3,
        "branch-1",
        Some(1),
        PrState::Closed,
        MergeStateStatus::Clean,
    );
    pr3.head_ref = "branch-3".to_string();

    let prs = HashMap::from([(PrNumber(1), pr1), (PrNumber(2), pr2), (PrNumber(3), pr3)]);

    let descendants = engine.compute_frozen_descendants(PrNumber(1), &prs);

    assert_eq!(descendants.len(), 1);
    assert!(descendants.contains(&PrNumber(2)));
    assert!(!descendants.contains(&PrNumber(3)));
}

#[test]
fn compute_initial_phase_with_descendants() {
    let engine = CascadeEngine::new("main");

    let phase = engine.compute_initial_phase(vec![PrNumber(2), PrNumber(3)]);

    assert!(matches!(phase, CascadePhase::Preparing { .. }));
    if let CascadePhase::Preparing { progress } = phase {
        assert_eq!(progress.frozen_descendants, vec![PrNumber(2), PrNumber(3)]);
    }
}

#[test]
fn compute_initial_phase_without_descendants() {
    let engine = CascadeEngine::new("main");

    let phase = engine.compute_initial_phase(vec![]);

    assert!(matches!(phase, CascadePhase::SquashPending { .. }));
}

/// Regression test: compute_frozen_descendants must return TRANSITIVE descendants,
/// not just direct descendants. This prevents under-freezing in chains like
/// main <- #1 <- #2 <- #3, where #3 would be missed if only direct descendants
/// were returned.
#[test]
fn compute_frozen_descendants_returns_transitive_descendants() {
    let engine = CascadeEngine::new("main");

    // Create a chain: main <- #1 <- #2 <- #3
    let pr1 = make_open_pr(1, "main", None);
    let mut pr2 = make_open_pr(2, "branch-1", Some(1));
    pr2.head_ref = "branch-2".to_string();
    let mut pr3 = make_open_pr(3, "branch-2", Some(2));
    pr3.head_ref = "branch-3".to_string();

    let prs = HashMap::from([(PrNumber(1), pr1), (PrNumber(2), pr2), (PrNumber(3), pr3)]);

    let descendants = engine.compute_frozen_descendants(PrNumber(1), &prs);

    // Should include BOTH #2 (direct) AND #3 (transitive)
    assert_eq!(
        descendants.len(),
        2,
        "Expected 2 transitive descendants, got {:?}",
        descendants
    );
    assert!(
        descendants.contains(&PrNumber(2)),
        "Should contain direct descendant #2"
    );
    assert!(
        descendants.contains(&PrNumber(3)),
        "Should contain transitive descendant #3"
    );
}

/// Verifies that descendants of an active train cannot be started as new trains.
///
/// This test validates an architectural invariant: in a tree-shaped PR graph,
/// a descendant of a train's root PR cannot itself be a valid root because it
/// targets its parent's head branch rather than `main`. The `is_root()` check
/// rejects such attempts before the overlap detection code is reached.
///
/// Note: The Idle-phase overlap detection code (lines 254-265 in start_train)
/// exists as a defensive measure but is architecturally unreachable in normal
/// scenarios because:
/// 1. A PR can only have one base branch
/// 2. To be a valid root, a PR must target `main`
/// 3. To be a descendant of another PR, it must target that PR's head (not main)
/// 4. These are mutually exclusive
///
/// Scenario:
/// - main <- #1 <- #2 (train for #1 in Idle phase)
/// - Attempt to start train for #2 (which is a descendant of #1)
/// - Should fail with NotARoot because #2 targets branch-1, not main
#[test]
fn start_train_rejects_descendant_of_idle_train_as_root() {
    let engine = CascadeEngine::new("main");

    // Create structure: main <- #1 <- #2
    let pr1 = make_open_pr(1, "main", None);
    let mut pr2 = make_open_pr(2, "branch-1", Some(1));
    pr2.head_ref = "branch-2".to_string();

    let prs = HashMap::from([(PrNumber(1), pr1), (PrNumber(2), pr2)]);
    let mut active_trains = HashMap::new();

    // Start train for #1 - it will be in Idle phase (default state)
    let result1 = engine.start_train(PrNumber(1), &prs, &active_trains);
    assert!(result1.is_ok(), "Should start train for #1");
    let train1 = result1.unwrap().train;

    // Verify train is in Idle phase (no frozen descendants)
    assert!(
        matches!(train1.cascade_phase, CascadePhase::Idle),
        "Train should be in Idle phase"
    );
    assert!(
        train1.cascade_phase.progress().is_none(),
        "Idle phase should have no progress"
    );

    // Add train #1 to active trains (in Idle phase)
    active_trains.insert(PrNumber(1), train1);

    // Try to start train for #2 - should fail because #2 targets branch-1, not main
    let result2 = engine.start_train(PrNumber(2), &prs, &active_trains);
    assert!(result2.is_err(), "Starting train for #2 should fail");

    // Verify error is NotARoot because #2 targets branch-1
    if let Err(CascadeError::NotARoot(pr, msg)) = result2 {
        assert_eq!(pr, PrNumber(2));
        assert!(
            msg.contains("branch-1"),
            "Error should mention the invalid base_ref"
        );
    } else {
        panic!("Expected NotARoot error, got {:?}", result2);
    }
}

/// Verifies that Idle-phase overlap detection uses the current PR graph, not a snapshot.
///
/// When a train is in Idle phase, its potential descendants are computed dynamically
/// from the current PR graph. If the graph changes (e.g., a PR retargets to a different
/// branch), the overlap detection should reflect the new structure.
///
/// Scenario:
/// 1. Initially: main <- #1 <- #2 (train for #1 starts in Idle phase)
/// 2. Graph changes: #2 retargets to branch-3, creating main <- #3 <- #2
/// 3. Now #2 is no longer a descendant of #1
/// 4. Starting train for #3 should succeed because there's no overlap
#[test]
fn start_train_allows_after_retarget_removes_overlap() {
    let engine = CascadeEngine::new("main");

    // Initially: main <- #1 <- #2
    let pr1 = make_open_pr(1, "main", None);
    let mut pr2 = make_open_pr(2, "branch-1", Some(1));
    pr2.head_ref = "branch-2".to_string();

    let mut prs = HashMap::from([(PrNumber(1), pr1), (PrNumber(2), pr2)]);
    let mut active_trains = HashMap::new();

    // Start train for #1 - it will be in Idle phase
    let result1 = engine.start_train(PrNumber(1), &prs, &active_trains);
    assert!(result1.is_ok(), "Should start train for #1");
    let train1 = result1.unwrap().train;

    // Verify train is in Idle phase (potential descendants computed dynamically)
    assert!(train1.cascade_phase.progress().is_none());
    active_trains.insert(PrNumber(1), train1);

    // Modify the structure: #2 retargets from branch-1 to branch-3
    // New structure: main <- #1 (no descendants), main <- #3 <- #2
    let pr3 = make_open_pr(3, "main", None);
    prs.insert(PrNumber(3), pr3);

    let mut pr2_modified = make_open_pr(2, "branch-3", Some(3));
    pr2_modified.head_ref = "branch-2".to_string();
    prs.insert(PrNumber(2), pr2_modified);

    // Overlap check for Train #3:
    // - Train #1's potential descendants are computed from CURRENT graph = []
    // - Train #3's PRs are [#3, #2]
    // - No overlap → train #3 should be allowed
    let result3 = engine.start_train(PrNumber(3), &prs, &active_trains);
    assert!(
        result3.is_ok(),
        "Should allow starting train for #3 since #2 is no longer #1's descendant"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Bug Regression Tests
// ─────────────────────────────────────────────────────────────────────────────

/// Regression test: evaluate_train must block draft PRs even if they have CLEAN
/// mergeStateStatus. GitHub's API can report CLEAN for drafts (if CI passes),
/// but the merge API will reject them.
#[test]
fn evaluate_train_blocks_draft_prs() {
    let engine = CascadeEngine::new("main");

    let train = TrainRecord::new(PrNumber(1));

    // Create a draft PR that reports CLEAN status (this can happen per DESIGN.md).
    // We construct directly because CachedPr::new() has a debug_assert preventing
    // this combination, but GitHub's API can actually return it.
    let draft_pr = CachedPr {
        number: PrNumber(1),
        head_sha: make_sha(1),
        head_ref: "branch-1".to_string(),
        base_ref: "main".to_string(),
        predecessor: None,
        state: PrState::Open,
        merge_state_status: MergeStateStatus::Clean, // Draft can still be CLEAN!
        is_draft: true,                              // This is the key: it's a draft
        closed_at: None,
        predecessor_squash_reconciled: None,
    };

    let prs = HashMap::from([(PrNumber(1), draft_pr)]);

    let action = engine.evaluate_train(&train, &prs);

    assert!(
        matches!(
            action,
            TrainAction::Block {
                reason: BlockReason::Draft
            }
        ),
        "evaluate_train must return Block {{ reason: Draft }} for draft PRs. \
         Got: {:?}",
        action
    );
}

/// Regression test: Status comment JSON must use compact serialization,
/// omit local-only fields, and respect the 60KB size limit.
#[test]
fn status_comment_json_has_size_guard() {
    // Create a train with a large number of descendants (within the 50 PR limit)
    let mut train = TrainRecord::new(PrNumber(1));
    train.status_comment_id = Some(CommentId(12345)); // LOCAL-ONLY field

    // Add many descendants to approach size limits
    let descendants: Vec<PrNumber> = (2..=45).map(PrNumber).collect();
    train.cascade_phase = CascadePhase::Preparing {
        progress: DescendantProgress::new(descendants),
    };

    // Generate the status comment JSON
    let json = format_train_json(&train).expect("Should not overflow with moderate input");

    // status_comment_id should be omitted from JSON (it's local-only)
    assert!(
        !json.contains("status_comment_id"),
        "status_comment_id should be omitted from recovery JSON. \
         JSON: {}",
        json
    );

    // Should use compact serialization, not pretty-printed
    let has_excessive_whitespace = json.lines().count() > 10;
    assert!(
        !has_excessive_whitespace,
        "Status comment JSON should use compact format. Lines: {}",
        json.lines().count()
    );

    // Verify size is under 60KB
    assert!(
        json.len() < 60 * 1024,
        "Status comment JSON ({} bytes) exceeds 60KB limit",
        json.len()
    );
}

/// Regression test: format_train_json must truncate error fields per DESIGN.md.
/// error.message: Maximum 4KB, error.stderr: Maximum 2KB
#[test]
fn status_comment_truncates_error_fields() {
    let mut train = TrainRecord::new(PrNumber(1));

    // Create an error with very long message and stderr
    let long_message = "x".repeat(10_000); // 10KB, over 4KB limit
    let long_stderr = "y".repeat(5_000); // 5KB, over 2KB limit

    train.abort(
        TrainError::new("test_error", long_message.clone()).with_stderr(long_stderr.clone()),
    );

    let json = format_train_json(&train).expect("Should not overflow with moderate input");

    // Check that message was truncated (should not contain full 10KB string)
    assert!(
        !json.contains(&long_message),
        "error.message ({} bytes) should be truncated to 4KB limit",
        long_message.len()
    );

    // Check that stderr was truncated (should not contain full 5KB string)
    assert!(
        !json.contains(&long_stderr),
        "error.stderr ({} bytes) should be truncated to 2KB limit",
        long_stderr.len()
    );

    // Verify truncation indicator is present
    assert!(
        json.contains("..."),
        "Truncated fields should have '...' indicator"
    );
}

/// Regression test: External merge during Preparing phase with unprepared descendants
/// must abort (not advance). If preparation isn't complete, descendants haven't
/// had the predecessor's content merged into them, violating the invariant.
#[test]
fn external_merge_during_preparing_with_unprepared_descendants_aborts() {
    let engine = CascadeEngine::new("main");

    // Create a train in Preparing phase with unprepared descendants
    let mut train = TrainRecord::new(PrNumber(1));
    train.cascade_phase = CascadePhase::Preparing {
        progress: DescendantProgress::new(vec![PrNumber(2), PrNumber(3)]),
    };

    // The current PR was externally merged
    let merged_pr = CachedPr {
        number: PrNumber(1),
        head_sha: make_sha(1),
        head_ref: "branch-1".to_string(),
        base_ref: "main".to_string(),
        predecessor: None,
        state: PrState::Merged {
            merge_commit_sha: make_sha(100),
        },
        merge_state_status: MergeStateStatus::Clean,
        is_draft: false,
        closed_at: None,
        predecessor_squash_reconciled: None,
    };

    // Descendants must be open PRs in the cache for the abort check to fire
    let pr2 = CachedPr {
        number: PrNumber(2),
        head_sha: make_sha(2),
        head_ref: "branch-2".to_string(),
        base_ref: "branch-1".to_string(),
        predecessor: Some(PrNumber(1)),
        state: PrState::Open,
        merge_state_status: MergeStateStatus::Clean,
        is_draft: false,
        closed_at: None,
        predecessor_squash_reconciled: None,
    };

    let pr3 = CachedPr {
        number: PrNumber(3),
        head_sha: make_sha(3),
        head_ref: "branch-3".to_string(),
        base_ref: "branch-1".to_string(),
        predecessor: Some(PrNumber(1)),
        state: PrState::Open,
        merge_state_status: MergeStateStatus::Clean,
        is_draft: false,
        closed_at: None,
        predecessor_squash_reconciled: None,
    };

    let prs = HashMap::from([
        (PrNumber(1), merged_pr),
        (PrNumber(2), pr2),
        (PrNumber(3), pr3),
    ]);

    let action = engine.evaluate_train(&train, &prs);

    // Should abort because descendants #2 and #3 weren't prepared
    assert!(
        matches!(
            action,
            TrainAction::Abort {
                reason: AbortReason::PreparationIncomplete { .. }
            }
        ),
        "External merge during Preparing with unprepared descendants should abort. \
         Got: {:?}",
        action
    );

    // Verify the unprepared descendants are mentioned
    if let TrainAction::Abort {
        reason: AbortReason::PreparationIncomplete {
            unprepared_descendants,
        },
    } = action
    {
        assert!(
            unprepared_descendants.contains(&PrNumber(2)),
            "Should mention unprepared descendant #2"
        );
        assert!(
            unprepared_descendants.contains(&PrNumber(3)),
            "Should mention unprepared descendant #3"
        );
    }
}

/// External merge in Preparing phase when all descendants are prepared should advance.
#[test]
fn external_merge_during_preparing_with_all_prepared_advances() {
    let engine = CascadeEngine::new("main");

    // Create a train in Preparing phase with ALL descendants completed
    let mut train = TrainRecord::new(PrNumber(1));
    let mut progress = DescendantProgress::new(vec![PrNumber(2), PrNumber(3)]);
    progress.mark_completed(PrNumber(2));
    progress.mark_completed(PrNumber(3));
    train.cascade_phase = CascadePhase::Preparing { progress };

    // The current PR was externally merged
    let merged_pr = CachedPr {
        number: PrNumber(1),
        head_sha: make_sha(1),
        head_ref: "branch-1".to_string(),
        base_ref: "main".to_string(),
        predecessor: None,
        state: PrState::Merged {
            merge_commit_sha: make_sha(100),
        },
        merge_state_status: MergeStateStatus::Clean,
        is_draft: false,
        closed_at: None,
        predecessor_squash_reconciled: None,
    };

    let prs = HashMap::from([(PrNumber(1), merged_pr)]);

    let action = engine.evaluate_train(&train, &prs);

    // Should advance because all descendants were prepared
    assert!(
        matches!(action, TrainAction::AdvanceAfterExternalMerge { .. }),
        "External merge during Preparing with all descendants prepared should advance. \
         Got: {:?}",
        action
    );
}

/// External merge in Idle phase should always advance (no preparation yet).
#[test]
fn external_merge_during_idle_advances() {
    let engine = CascadeEngine::new("main");

    let train = TrainRecord::new(PrNumber(1)); // Idle phase by default

    // The current PR was externally merged
    let merged_pr = CachedPr {
        number: PrNumber(1),
        head_sha: make_sha(1),
        head_ref: "branch-1".to_string(),
        base_ref: "main".to_string(),
        predecessor: None,
        state: PrState::Merged {
            merge_commit_sha: make_sha(100),
        },
        merge_state_status: MergeStateStatus::Clean,
        is_draft: false,
        closed_at: None,
        predecessor_squash_reconciled: None,
    };

    let prs = HashMap::from([(PrNumber(1), merged_pr)]);

    let action = engine.evaluate_train(&train, &prs);

    // Should advance (Idle has no frozen descendants to check)
    assert!(
        matches!(action, TrainAction::AdvanceAfterExternalMerge { .. }),
        "External merge during Idle should advance. Got: {:?}",
        action
    );
}

/// External merge with unprepared descendants that are all closed should advance.
/// Closed descendants would be skipped anyway, so they shouldn't block progress.
#[test]
fn external_merge_with_only_closed_unprepared_descendants_advances() {
    let engine = CascadeEngine::new("main");

    // Create a train in Preparing phase with unprepared descendants
    let mut train = TrainRecord::new(PrNumber(1));
    train.cascade_phase = CascadePhase::Preparing {
        progress: DescendantProgress::new(vec![PrNumber(2), PrNumber(3)]),
    };

    // The current PR was externally merged
    let merged_pr = CachedPr {
        number: PrNumber(1),
        head_sha: make_sha(1),
        head_ref: "branch-1".to_string(),
        base_ref: "main".to_string(),
        predecessor: None,
        state: PrState::Merged {
            merge_commit_sha: make_sha(100),
        },
        merge_state_status: MergeStateStatus::Clean,
        is_draft: false,
        closed_at: None,
        predecessor_squash_reconciled: None,
    };

    // Descendants #2 and #3 are closed (not in prs map or closed state)
    // They would be skipped anyway, so external merge should advance
    let prs = HashMap::from([(PrNumber(1), merged_pr)]);

    let action = engine.evaluate_train(&train, &prs);

    // Should advance because closed descendants would be skipped
    assert!(
        matches!(action, TrainAction::AdvanceAfterExternalMerge { .. }),
        "External merge with only closed unprepared descendants should advance. \
         Got: {:?}",
        action
    );
}

/// External merge with mix of open and closed unprepared descendants should abort.
/// Only the open ones matter for the "preparation incomplete" check.
#[test]
fn external_merge_with_mixed_open_closed_unprepared_aborts() {
    let engine = CascadeEngine::new("main");

    // Create a train in Preparing phase with unprepared descendants
    let mut train = TrainRecord::new(PrNumber(1));
    train.cascade_phase = CascadePhase::Preparing {
        progress: DescendantProgress::new(vec![PrNumber(2), PrNumber(3)]),
    };

    // The current PR was externally merged
    let merged_pr = CachedPr {
        number: PrNumber(1),
        head_sha: make_sha(1),
        head_ref: "branch-1".to_string(),
        base_ref: "main".to_string(),
        predecessor: None,
        state: PrState::Merged {
            merge_commit_sha: make_sha(100),
        },
        merge_state_status: MergeStateStatus::Clean,
        is_draft: false,
        closed_at: None,
        predecessor_squash_reconciled: None,
    };

    // #2 is open (unprepared), #3 is closed
    let open_pr = CachedPr {
        number: PrNumber(2),
        head_sha: make_sha(2),
        head_ref: "branch-2".to_string(),
        base_ref: "branch-1".to_string(),
        predecessor: Some(PrNumber(1)),
        state: PrState::Open,
        merge_state_status: MergeStateStatus::Clean,
        is_draft: false,
        closed_at: None,
        predecessor_squash_reconciled: None,
    };

    let prs = HashMap::from([(PrNumber(1), merged_pr), (PrNumber(2), open_pr)]);
    // #3 is not in prs (closed/missing)

    let action = engine.evaluate_train(&train, &prs);

    // Should abort because #2 is open and unprepared
    match action {
        TrainAction::Abort {
            reason:
                AbortReason::PreparationIncomplete {
                    unprepared_descendants,
                },
        } => {
            assert_eq!(
                unprepared_descendants,
                vec![PrNumber(2)],
                "Only open unprepared descendants should be listed"
            );
        }
        other => panic!(
            "Expected Abort with PreparationIncomplete, got: {:?}",
            other
        ),
    }
}

/// Regression test: create_step_outcome must filter out closed descendants.
/// If a descendant closes mid-train but isn't marked as skipped yet,
/// it should not appear in the outcome.
#[test]
fn create_step_outcome_filters_closed_descendants() {
    let engine = CascadeEngine::new("main");

    // Create a train with some descendants, one of which is closed
    let mut train = TrainRecord::new(PrNumber(1));
    train.cascade_phase = CascadePhase::Preparing {
        progress: DescendantProgress::new(vec![PrNumber(2), PrNumber(3), PrNumber(4)]),
    };

    // PR #1 is current, #2 is open, #3 is closed, #4 is open
    let pr1 = CachedPr {
        number: PrNumber(1),
        head_sha: make_sha(1),
        head_ref: "branch-1".to_string(),
        base_ref: "main".to_string(),
        predecessor: None,
        state: PrState::Open,
        merge_state_status: MergeStateStatus::Clean,
        is_draft: false,
        closed_at: None,
        predecessor_squash_reconciled: None,
    };

    let pr2 = CachedPr {
        number: PrNumber(2),
        head_sha: make_sha(2),
        head_ref: "branch-2".to_string(),
        base_ref: "branch-1".to_string(),
        predecessor: Some(PrNumber(1)),
        state: PrState::Open,
        merge_state_status: MergeStateStatus::Clean,
        is_draft: false,
        closed_at: None,
        predecessor_squash_reconciled: None,
    };

    let pr3 = CachedPr {
        number: PrNumber(3),
        head_sha: make_sha(3),
        head_ref: "branch-3".to_string(),
        base_ref: "branch-1".to_string(),
        predecessor: Some(PrNumber(1)),
        state: PrState::Closed, // Closed!
        merge_state_status: MergeStateStatus::Clean,
        is_draft: false,
        closed_at: None,
        predecessor_squash_reconciled: None,
    };

    let pr4 = CachedPr {
        number: PrNumber(4),
        head_sha: make_sha(4),
        head_ref: "branch-4".to_string(),
        base_ref: "branch-1".to_string(),
        predecessor: Some(PrNumber(1)),
        state: PrState::Open,
        merge_state_status: MergeStateStatus::Clean,
        is_draft: false,
        closed_at: None,
        predecessor_squash_reconciled: None,
    };

    let prs = HashMap::from([
        (PrNumber(1), pr1),
        (PrNumber(2), pr2),
        (PrNumber(3), pr3),
        (PrNumber(4), pr4),
    ]);

    let outcome = engine.create_step_outcome(&train, &prs);

    // Should be FanOut with only the open descendants (#2 and #4)
    match outcome {
        CascadeStepOutcome::FanOut { descendants } => {
            assert_eq!(descendants.len(), 2, "Should have 2 open descendants");
            assert!(
                descendants.contains(&PrNumber(2)),
                "Should contain open PR #2"
            );
            assert!(
                !descendants.contains(&PrNumber(3)),
                "Should NOT contain closed PR #3"
            );
            assert!(
                descendants.contains(&PrNumber(4)),
                "Should contain open PR #4"
            );
        }
        other => panic!("Expected FanOut with 2 descendants, got {:?}", other),
    }
}

/// create_step_outcome returns Complete when all remaining descendants are closed.
#[test]
fn create_step_outcome_complete_when_all_descendants_closed() {
    let engine = CascadeEngine::new("main");

    let mut train = TrainRecord::new(PrNumber(1));
    train.cascade_phase = CascadePhase::Preparing {
        progress: DescendantProgress::new(vec![PrNumber(2)]),
    };

    // Current PR is open, but the only descendant is closed
    let pr1 = CachedPr {
        number: PrNumber(1),
        head_sha: make_sha(1),
        head_ref: "branch-1".to_string(),
        base_ref: "main".to_string(),
        predecessor: None,
        state: PrState::Open,
        merge_state_status: MergeStateStatus::Clean,
        is_draft: false,
        closed_at: None,
        predecessor_squash_reconciled: None,
    };

    let pr2 = CachedPr {
        number: PrNumber(2),
        head_sha: make_sha(2),
        head_ref: "branch-2".to_string(),
        base_ref: "branch-1".to_string(),
        predecessor: Some(PrNumber(1)),
        state: PrState::Closed,
        merge_state_status: MergeStateStatus::Clean,
        is_draft: false,
        closed_at: None,
        predecessor_squash_reconciled: None,
    };

    let prs = HashMap::from([(PrNumber(1), pr1), (PrNumber(2), pr2)]);

    let outcome = engine.create_step_outcome(&train, &prs);

    // All remaining descendants are closed, so outcome should be Complete
    assert!(
        matches!(outcome, CascadeStepOutcome::Complete),
        "Expected Complete when all descendants are closed, got {:?}",
        outcome
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Property-Based Bug Detection Tests
// ─────────────────────────────────────────────────────────────────────────────

mod property_based_bug_detection {
    use super::*;
    use proptest::prelude::*;

    /// truncate_string handles non-ASCII boundaries safely.
    ///
    /// Property: truncate_string NEVER panics, regardless of input string
    /// content or max_len value. The output is always valid UTF-8.
    #[test]
    fn truncate_string_never_panics() {
        proptest!(|(
            s in "\\PC{0,200}",  // Any Unicode string up to 200 chars
            max_len in 0usize..100
        )| {
            // This should never panic
            let result = truncate_string(&s, max_len);

            // Result should be valid UTF-8 (it's a String, so it is)
            prop_assert!(result.len() <= max_len.max(3), // at least "..."
                "Result length {} exceeds max_len {}",
                result.len(), max_len
            );

            // If truncated, should end with "..."
            if s.len() > max_len && max_len >= 3 {
                prop_assert!(
                    result.ends_with("..."),
                    "Truncated string should end with '...'"
                );
            }
        });
    }

    /// BUG #5 (specific cases): truncate_string with multi-byte UTF-8.
    ///
    /// Property: Truncation at any position within multi-byte characters
    /// should find a valid boundary and not panic.
    #[test]
    fn truncate_string_handles_multibyte_chars() {
        // Test with various multi-byte UTF-8 strings
        let test_cases = [
            "Hello 世界!", // Chinese
            "Привет мир",  // Russian
            "🎉🎊🎁🎈",    // Emojis (4-byte UTF-8)
            "café résumé", // Latin with diacritics
            "αβγδε",       // Greek
            "こんにちは",  // Japanese
        ];

        for s in &test_cases {
            // Try truncating at every possible byte position
            for max_len in 0..=s.len() + 5 {
                let result = truncate_string(s, max_len);

                // Should never panic, and result should be valid UTF-8
                assert!(
                    result.len() <= max_len.max(3),
                    "truncate_string({:?}, {}) = {:?} exceeds max",
                    s,
                    max_len,
                    result
                );
            }
        }
    }

    /// Status comment size is enforced in release builds.
    ///
    /// Property: format_train_json ALWAYS returns a string under 60KB,
    /// even in release builds, even with pathologically large input.
    #[test]
    fn format_train_json_enforces_size_limit() {
        proptest!(|(
            desc_count in 0usize..50,
            error_msg_len in 0usize..20000,
            stderr_len in 0usize..10000
        )| {
            let mut train = TrainRecord::new(PrNumber(1));

            // Add many descendants
            let descendants: Vec<PrNumber> = (2..=(desc_count as u64 + 1))
                .map(PrNumber)
                .collect();
            if !descendants.is_empty() {
                train.cascade_phase = CascadePhase::Preparing {
                    progress: DescendantProgress::new(descendants),
                };
            }

            // Add a large error
            if error_msg_len > 0 {
                let msg = "E".repeat(error_msg_len);
                let mut error = TrainError::new("test", msg);
                if stderr_len > 0 {
                    error = error.with_stderr("S".repeat(stderr_len));
                }
                train.abort(error);
            }

            // format_train_json now returns Result - it should either succeed
            // with a reasonably-sized JSON, or error if oversize.
            match format_train_json(&train) {
                Ok(json) => {
                    // If it succeeds, MUST be under 60KB
                    prop_assert!(
                        json.len() < 60 * 1024,
                        "BUG: format_train_json returned {} bytes, exceeding 60KB limit",
                        json.len()
                    );

                    // Must be valid JSON
                    prop_assert!(
                        serde_json::from_str::<serde_json::Value>(&json).is_ok(),
                        "format_train_json must return valid JSON"
                    );
                }
                Err(CascadeError::StatusCommentOversize { .. }) => {
                    // Expected for extreme inputs - this is correct behavior per DESIGN.md
                }
                Err(e) => {
                    prop_assert!(false, "Unexpected error: {:?}", e);
                }
            }
        });
    }

    /// BUG #4 (extreme case): Even with max-size input, should either succeed
    /// under 60KB OR return StatusCommentOversize error.
    #[test]
    fn format_train_json_handles_extreme_input() {
        let mut train = TrainRecord::new(PrNumber(1));

        // Max descendants (50 is the limit per MAX_TRAIN_SIZE)
        let descendants: Vec<PrNumber> = (2..=50).map(PrNumber).collect();
        train.cascade_phase = CascadePhase::Preparing {
            progress: DescendantProgress::new(descendants),
        };

        // Max error sizes
        let huge_msg = "X".repeat(100_000); // 100KB message
        let huge_stderr = "Y".repeat(50_000); // 50KB stderr
        train.abort(TrainError::new("huge_error", huge_msg).with_stderr(huge_stderr));

        match format_train_json(&train) {
            Ok(json) => {
                // If it succeeds, must stay under 60KB
                assert!(
                    json.len() < 60 * 1024,
                    "BUG: Extreme input produced {} byte JSON, exceeding 60KB limit",
                    json.len()
                );

                // Must still be valid JSON
                assert!(
                    serde_json::from_str::<serde_json::Value>(&json).is_ok(),
                    "Extreme input must still produce valid JSON"
                );
            }
            Err(CascadeError::StatusCommentOversize {
                actual_size,
                max_size,
            }) => {
                // Expected for extreme inputs - verify the error is sensible.
                // The check uses >= (actual_size >= max_size triggers error),
                // so actual_size may equal max_size at the boundary.
                assert!(
                    actual_size >= max_size,
                    "Oversize error should report actual >= max"
                );
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }

    /// Property: escape_for_html_comment prevents --> from terminating comments,
    /// and the escaped JSON still parses correctly, recovering the original value.
    #[test]
    fn escape_for_html_comment_preserves_json_semantics() {
        proptest!(|(
            prefix in "[a-zA-Z0-9 ]{0,20}",
            suffix in "[a-zA-Z0-9 ]{0,20}",
            arrow_count in 1usize..5
        )| {
            // Create a string with --> embedded
            let dangerous_content = format!("{}{}{}", prefix, "-->".repeat(arrow_count), suffix);

            // Create a TrainRecord with this dangerous content in an error
            let mut train = TrainRecord::new(PrNumber(1));
            let error = TrainError::new("git", dangerous_content.clone())
                .with_stderr(format!("stderr also has --> in it: {}", dangerous_content));
            train.abort(error);

            // Serialize and escape
            let json = format_train_json(&train).expect("should serialize");
            let escaped = escape_for_html_comment(&json);

            // Property 1: No --> in escaped output
            prop_assert!(
                !escaped.contains("-->"),
                "Escaped JSON must not contain -->, but found: {}",
                escaped.chars().take(200).collect::<String>()
            );

            // Property 2: Escaped JSON is still valid JSON
            let parsed: serde_json::Value = serde_json::from_str(&escaped)
                .expect("Escaped JSON must still be valid JSON");

            // Property 3: The original content is recoverable after parsing
            let error_obj = parsed.get("error").expect("should have error field");
            let message = error_obj.get("message").and_then(|v| v.as_str()).expect("should have message");
            let stderr = error_obj.get("stderr").and_then(|v| v.as_str()).expect("should have stderr");

            prop_assert!(
                message.contains("-->"),
                "Original --> should be recovered in message after parsing"
            );
            prop_assert!(
                stderr.contains("-->"),
                "Original --> should be recovered in stderr after parsing"
            );
        });
    }

    /// Edge case: Multiple overlapping --> sequences
    #[test]
    fn escape_handles_overlapping_arrows() {
        // Test case: "-->" appears multiple times, including "---->>" (overlapping)
        let input = r#"{"msg":"a-->b---->c"}"#;
        let escaped = escape_for_html_comment(input);

        assert!(
            !escaped.contains("-->"),
            "Should escape all --> occurrences"
        );
        assert_eq!(
            escaped, r#"{"msg":"a--\u003eb----\u003ec"}"#,
            "Each --> should be escaped independently"
        );

        // Verify it round-trips correctly
        let parsed: serde_json::Value = serde_json::from_str(&escaped).unwrap();
        assert_eq!(
            parsed.get("msg").unwrap().as_str().unwrap(),
            "a-->b---->c",
            "Original string should be recovered after parsing"
        );
    }
}
