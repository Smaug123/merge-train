//! Unit tests and regression tests for cascade step execution.
//!
//! Specification-level property tests remain in `step.rs` with the implementation.
//! This file contains:
//! - Basic unit tests for step execution
//! - Bug regression tests (example-based tests for specific bugs)
//! - Property-based bug detection tests

use super::*;
use crate::types::{CommentId, MergeStateStatus, Sha, TrainState};

fn make_sha(n: u64) -> Sha {
    Sha::parse(format!("{:0>40x}", n)).unwrap()
}

fn make_open_pr(number: u64, base_ref: &str, predecessor: Option<u64>) -> CachedPr {
    CachedPr::new(
        PrNumber(number),
        make_sha(number),
        format!("branch-{}", number),
        base_ref.to_string(),
        predecessor.map(PrNumber),
        PrState::Open,
        MergeStateStatus::Clean,
        false,
    )
}

#[test]
fn execute_from_idle_with_no_descendants() {
    let train = TrainRecord::new(PrNumber(1));
    let pr = make_open_pr(1, "main", None);
    let prs = HashMap::from([(PrNumber(1), pr)]);
    let ctx = StepContext::new("main");

    let result = execute_cascade_step(train, &prs, &ctx);

    assert!(matches!(
        result.train.cascade_phase,
        CascadePhase::SquashPending { .. }
    ));
}

#[test]
fn execute_from_idle_with_descendants() {
    let train = TrainRecord::new(PrNumber(1));
    let pr1 = make_open_pr(1, "main", None);
    let mut pr2 = make_open_pr(2, "branch-1", Some(1));
    pr2.head_ref = "branch-2".to_string();

    let prs = HashMap::from([(PrNumber(1), pr1), (PrNumber(2), pr2)]);
    let ctx = StepContext::new("main");

    let result = execute_cascade_step(train, &prs, &ctx);

    assert!(matches!(
        result.train.cascade_phase,
        CascadePhase::Preparing { .. }
    ));
    if let CascadePhase::Preparing { progress } = &result.train.cascade_phase {
        assert_eq!(progress.frozen_descendants.len(), 1);
        assert!(progress.frozen_descendants.contains(&PrNumber(2)));
    }
}

#[test]
fn execute_preparing_generates_merge_effects() {
    let mut train = TrainRecord::new(PrNumber(1));
    train.cascade_phase = CascadePhase::Preparing {
        progress: DescendantProgress::new(vec![PrNumber(2)]),
    };

    let pr1 = make_open_pr(1, "main", None);
    let mut pr2 = make_open_pr(2, "branch-1", Some(1));
    pr2.head_ref = "branch-2".to_string();

    let prs = HashMap::from([(PrNumber(1), pr1), (PrNumber(2), pr2)]);
    let ctx = StepContext::new("main");

    let result = execute_cascade_step(train, &prs, &ctx);

    // Should have merge and push effects
    assert!(!result.effects.is_empty());
    assert!(
        result
            .effects
            .iter()
            .any(|e| matches!(e, Effect::Git(GitEffect::Merge { .. })))
    );
    assert!(
        result
            .effects
            .iter()
            .any(|e| matches!(e, Effect::Git(GitEffect::Push { .. })))
    );
}

#[test]
fn execute_squash_pending_generates_squash_effect() {
    let mut train = TrainRecord::new(PrNumber(1));
    train.cascade_phase = CascadePhase::SquashPending {
        progress: DescendantProgress::new(vec![]),
    };

    let pr = make_open_pr(1, "main", None);
    let prs = HashMap::from([(PrNumber(1), pr)]);
    let ctx = StepContext::new("main");

    let result = execute_cascade_step(train, &prs, &ctx);

    assert!(
        result
            .effects
            .iter()
            .any(|e| matches!(e, Effect::GitHub(GitHubEffect::SquashMerge { .. })))
    );
}

#[test]
fn process_squash_merged_transitions_to_reconciling() {
    let mut train = TrainRecord::new(PrNumber(1));
    train.cascade_phase = CascadePhase::SquashPending {
        progress: DescendantProgress::new(vec![PrNumber(2)]),
    };

    let squash_sha = make_sha(0x123);
    let (updated_train, outcome, _effects) = process_operation_result(
        train,
        OperationResult::SquashMerged {
            pr: PrNumber(1),
            squash_sha: squash_sha.clone(),
        },
    );

    assert!(matches!(
        updated_train.cascade_phase,
        CascadePhase::Reconciling { .. }
    ));
    assert!(matches!(outcome, Some(CascadeStepOutcome::Merged { .. })));
}

#[test]
fn skip_descendant_marks_as_skipped() {
    let progress = DescendantProgress::new(vec![PrNumber(2), PrNumber(3)]);
    let mut train = TrainRecord::new(PrNumber(1));
    train.cascade_phase = CascadePhase::Preparing { progress };

    // Simulate skipping PR #2
    let (updated_train, _, _effects) = process_operation_result(
        train,
        OperationResult::DescendantSkipped {
            pr: PrNumber(2),
            reason: "PR closed".to_string(),
        },
    );

    if let CascadePhase::Preparing { progress } = &updated_train.cascade_phase {
        assert!(progress.skipped.contains(&PrNumber(2)));
    } else {
        panic!("Expected Preparing phase");
    }
}

// ─── Regression Tests ─────────────────────────────────────────────────────

mod bug_regression_tests {
    use super::*;

    /// Regression test: process_operation_result must emit a status comment update
    /// when transitioning to Reconciling after squash. This ensures GitHub-based
    /// recovery can see last_squash_sha, preventing re-squash or recovery failures
    /// if the bot crashes.
    #[test]
    fn squash_merged_emits_status_comment_update() {
        let mut train = TrainRecord::new(PrNumber(1));
        train.cascade_phase = CascadePhase::SquashPending {
            progress: DescendantProgress::new(vec![PrNumber(2)]),
        };
        // Train has a status comment ID
        train.status_comment_id = Some(CommentId(12345));

        let squash_sha = make_sha(0xabc);
        let (updated_train, outcome, effects) = process_operation_result(
            train,
            OperationResult::SquashMerged {
                pr: PrNumber(1),
                squash_sha: squash_sha.clone(),
            },
        );

        // Verify the transition happened
        assert!(matches!(
            updated_train.cascade_phase,
            CascadePhase::Reconciling { .. }
        ));
        assert!(matches!(outcome, Some(CascadeStepOutcome::Merged { .. })));
        assert_eq!(updated_train.last_squash_sha, Some(squash_sha));

        // Verify status comment update effect is emitted
        let has_comment_update = effects.iter().any(|e| {
            matches!(e, Effect::GitHub(GitHubEffect::UpdateComment { comment_id, .. })
                if *comment_id == CommentId(12345))
        });
        assert!(
            has_comment_update,
            "process_operation_result must emit UpdateComment effect when transitioning \
             to Reconciling. Effects: {:?}",
            effects
        );
    }

    /// Regression test: complete_cascade must emit a status comment update when
    /// changing phase to Idle and advancing current_pr.
    #[test]
    fn complete_cascade_emits_status_comment_update() {
        let mut train = TrainRecord::new(PrNumber(1));
        // Status comment exists
        train.status_comment_id = Some(CommentId(12345));

        // Set up Retargeting phase with one completed descendant
        let mut progress = DescendantProgress::new(vec![PrNumber(2)]);
        progress.mark_completed(PrNumber(2));
        train.cascade_phase = CascadePhase::Retargeting {
            progress,
            squash_sha: make_sha(0xabc),
        };

        // Build PR map
        let pr1 = make_open_pr(1, "main", None);
        let mut pr2 = make_open_pr(2, "branch-1", Some(1));
        pr2.head_ref = "branch-2".to_string();
        let prs = HashMap::from([(PrNumber(1), pr1), (PrNumber(2), pr2)]);
        let ctx = StepContext::new("main");

        // Execute cascade step - should complete and advance current_pr
        let result = execute_cascade_step(train, &prs, &ctx);

        // Should have advanced current_pr to the completed descendant
        assert_eq!(result.train.current_pr, PrNumber(2));

        // Verify status comment update is emitted
        let has_comment_update = result.effects.iter().any(|e| {
            matches!(e, Effect::GitHub(GitHubEffect::UpdateComment { comment_id, .. })
                if *comment_id == CommentId(12345))
        });

        assert!(
            has_comment_update,
            "complete_cascade must emit status comment update when advancing current_pr. \
             Effects: {:?}",
            result.effects
        );
    }

    /// Regression test: handle_external_merge must emit a status comment update
    /// when changing phase to Reconciling and storing squash_sha.
    ///
    /// External merge in Preparing with all descendants prepared must
    /// transition to Reconciling (not complete the cascade). The "completed" set
    /// in Preparing tracks "prepared" not "reconciled" - all descendants still
    /// need reconciliation.
    #[test]
    fn external_merge_emits_status_comment_update() {
        let mut train = TrainRecord::new(PrNumber(1));
        // Status comment exists
        train.status_comment_id = Some(CommentId(12345));

        // In Preparing phase with descendants - but ALL COMPLETED (prepared)
        // This is required because handle_external_merge now aborts if
        // descendants weren't prepared.
        let mut progress = DescendantProgress::new(vec![PrNumber(2)]);
        progress.mark_completed(PrNumber(2)); // Descendant was prepared
        train.cascade_phase = CascadePhase::Preparing { progress };

        // Build PR map with root PR merged externally
        let merge_sha = make_sha(0xfff);
        let mut pr1 = make_open_pr(1, "main", None);
        pr1.state = PrState::Merged {
            merge_commit_sha: merge_sha.clone(),
        };
        let mut pr2 = make_open_pr(2, "branch-1", Some(1));
        pr2.head_ref = "branch-2".to_string();

        let prs = HashMap::from([(PrNumber(1), pr1), (PrNumber(2), pr2)]);
        let ctx = StepContext::new("main");

        // Execute cascade step - should transition to Reconciling with reset progress
        let result = execute_cascade_step(train, &prs, &ctx);

        // Must transition to Reconciling (not complete cascade).
        // Current PR stays at #1 because we're reconciling, not advancing.
        assert_eq!(result.train.current_pr, PrNumber(1));
        assert_eq!(result.train.last_squash_sha, Some(merge_sha.clone()));

        // Phase must be Reconciling with reset progress (descendant NOT in completed set)
        match &result.train.cascade_phase {
            CascadePhase::Reconciling {
                progress,
                squash_sha,
            } => {
                assert_eq!(*squash_sha, merge_sha);
                // Progress must be reset - descendant should be in frozen set but NOT completed
                assert!(
                    progress.frozen_descendants.contains(&PrNumber(2)),
                    "Descendant must be in frozen_descendants"
                );
                assert!(
                    !progress.completed.contains(&PrNumber(2)),
                    "Descendant must NOT be in completed set (needs reconciliation)"
                );
            }
            other => panic!(
                "Expected Reconciling phase, got {:?}",
                std::mem::discriminant(other)
            ),
        }

        // Verify status comment update is emitted
        let has_comment_update = result.effects.iter().any(|e| {
            matches!(e, Effect::GitHub(GitHubEffect::UpdateComment { comment_id, .. })
                if *comment_id == CommentId(12345))
        });

        assert!(
            has_comment_update,
            "handle_external_merge must emit status comment update. \
             Effects: {:?}",
            result.effects
        );
    }

    /// Regression test: handle_external_merge must abort if descendants weren't
    /// prepared. Moving to Reconciling with unprepared descendants violates the
    /// "prepare before squash" invariant and can drop predecessor content.
    #[test]
    fn external_merge_verifies_descendants_prepared() {
        let mut train = TrainRecord::new(PrNumber(1));
        // In Preparing phase with TWO descendants, but NONE completed
        train.cascade_phase = CascadePhase::Preparing {
            progress: DescendantProgress::new(vec![PrNumber(2), PrNumber(3)]),
        };

        // Build PR map with root PR merged externally BEFORE preparation completed
        let merge_sha = make_sha(0xfff);
        let mut pr1 = make_open_pr(1, "main", None);
        pr1.state = PrState::Merged {
            merge_commit_sha: merge_sha.clone(),
        };
        let mut pr2 = make_open_pr(2, "branch-1", Some(1));
        pr2.head_ref = "branch-2".to_string();
        let mut pr3 = make_open_pr(3, "branch-2", Some(2));
        pr3.head_ref = "branch-3".to_string();

        let prs = HashMap::from([(PrNumber(1), pr1), (PrNumber(2), pr2), (PrNumber(3), pr3)]);
        let ctx = StepContext::new("main");

        // Execute cascade step
        let result = execute_cascade_step(train, &prs, &ctx);

        // Should abort because descendants weren't prepared
        assert!(
            matches!(result.outcome, CascadeStepOutcome::Aborted { .. }),
            "handle_external_merge must abort when descendants are unprepared. \
             Got outcome: {:?}",
            result.outcome
        );

        // Verify it's a PreparationIncomplete error
        if let CascadeStepOutcome::Aborted { reason, .. } = &result.outcome {
            assert!(
                matches!(reason, AbortReason::PreparationIncomplete { .. }),
                "Abort reason should be PreparationIncomplete, got: {:?}",
                reason
            );
        }
    }

    /// Regression test: external merge with NO descendants must still emit
    /// ValidateSquashCommit effect to enforce squash-only requirement.
    /// Even with no descendants to corrupt, a non-squash merge violates linear history.
    #[test]
    fn external_merge_no_descendants_validates_squash() {
        let mut train = TrainRecord::new(PrNumber(1));
        train.status_comment_id = Some(CommentId(12345));
        // In Idle phase (no descendants)
        train.cascade_phase = CascadePhase::Idle;

        // Build PR map with root PR merged externally
        let merge_sha = make_sha(0xfff);
        let mut pr1 = make_open_pr(1, "main", None);
        pr1.state = PrState::Merged {
            merge_commit_sha: merge_sha.clone(),
        };

        let prs = HashMap::from([(PrNumber(1), pr1)]);
        let ctx = StepContext::new("main");

        // Execute cascade step
        let result = execute_cascade_step(train, &prs, &ctx);

        // Should complete (no descendants to process)
        assert!(
            matches!(result.outcome, CascadeStepOutcome::Complete),
            "Expected Complete outcome, got: {:?}",
            result.outcome
        );

        // Must emit ValidateSquashCommit effect
        let has_validation = result.effects.iter().any(|e| {
            matches!(
                e,
                Effect::Git(GitEffect::ValidateSquashCommit {
                    squash_sha,
                    default_branch
                }) if squash_sha == &merge_sha && default_branch == "main"
            )
        });
        assert!(
            has_validation,
            "External merge with no descendants must emit ValidateSquashCommit. \
             Effects: {:?}",
            result.effects
        );

        // Validation should be first effect (before status update)
        assert!(
            matches!(
                &result.effects[0],
                Effect::Git(GitEffect::ValidateSquashCommit { .. })
            ),
            "ValidateSquashCommit must be first effect. Effects: {:?}",
            result.effects
        );
    }

    /// BUG: skip_descendant returns WaitingOnCi with no effects. Without a poll tick,
    /// the cascade can stall after a closed/missing descendant.
    #[test]
    fn skip_descendant_continues_cascade() {
        let mut train = TrainRecord::new(PrNumber(1));
        // In Preparing phase with two descendants
        train.cascade_phase = CascadePhase::Preparing {
            progress: DescendantProgress::new(vec![PrNumber(2), PrNumber(3)]),
        };

        // Build PR map - PR #2 is CLOSED (will be skipped), PR #3 is open
        let pr1 = make_open_pr(1, "main", None);
        let mut pr2 = CachedPr::new(
            PrNumber(2),
            make_sha(2),
            "branch-2".to_string(),
            "branch-1".to_string(),
            Some(PrNumber(1)),
            PrState::Closed, // CLOSED - will be skipped
            MergeStateStatus::Clean,
            false,
        );
        pr2.head_ref = "branch-2".to_string();
        let mut pr3 = make_open_pr(3, "branch-1", Some(1));
        pr3.head_ref = "branch-3".to_string();

        let prs = HashMap::from([(PrNumber(1), pr1), (PrNumber(2), pr2), (PrNumber(3), pr3)]);
        let ctx = StepContext::new("main");

        // Execute cascade step - should skip PR #2 and continue to PR #3
        let result = execute_cascade_step(train, &prs, &ctx);

        // PR #2 should be skipped
        if let CascadePhase::Preparing { progress } = &result.train.cascade_phase {
            assert!(
                progress.skipped.contains(&PrNumber(2)),
                "PR #2 should be marked as skipped"
            );
        }

        // BUG: skip_descendant returns WaitingOnCi with empty effects.
        // The cascade will stall because there's nothing to trigger the next step.
        //
        // The fix should either:
        // 1. Immediately invoke the next step after skipping (tail recursion), OR
        // 2. Return effects to process the next descendant
        //
        // Option 1 is cleaner: after skipping, call execute_cascade_step again
        // to process the next descendant.

        // Check that we either have effects to continue OR moved past the skipped PR
        let has_effects = !result.effects.is_empty();
        let skipped_and_continued =
            if let CascadePhase::Preparing { progress } = &result.train.cascade_phase {
                // If we skipped PR #2, we should have moved on to process PR #3
                progress.skipped.contains(&PrNumber(2))
                    && (has_effects || progress.completed.contains(&PrNumber(3)))
            } else {
                false
            };

        assert!(
            has_effects || skipped_and_continued,
            "BUG: skip_descendant returns empty effects, causing cascade to stall. \
             Outcome: {:?}, Effects: {:?}",
            result.outcome,
            result.effects
        );
    }

    /// Regression test: predecessor_head_sha must NOT be overwritten when preparing
    /// subsequent descendants. The squash-time guard (line 406) must compare against
    /// the head SHA from cascade START, not from the most recent preparation.
    ///
    /// Bug scenario:
    /// 1. Start cascade with root PR at SHA_A, prepare descendant #2 → stores SHA_A
    /// 2. External push changes root PR to SHA_B
    /// 3. Prepare descendant #3 → INCORRECTLY overwrites predecessor_head_sha to SHA_B
    /// 4. Squash guard compares SHA_B vs SHA_B → passes (should fail!)
    /// 5. Result: Descendant #2 was prepared with SHA_A but squash includes SHA_B content
    #[test]
    fn predecessor_head_sha_not_overwritten_on_subsequent_preparations() {
        let original_sha = make_sha(0x111);
        let new_sha = make_sha(0x222);

        // Setup: train with 2 DIRECT descendants of PR#1 (both based on branch-1)
        // This is the key setup - both descendants have the same predecessor
        let mut train = TrainRecord::new(PrNumber(1));
        train.cascade_phase = CascadePhase::Preparing {
            progress: DescendantProgress::new(vec![PrNumber(2), PrNumber(3)]),
        };

        // PR map with root PR at original SHA
        let mut pr1 = make_open_pr(1, "main", None);
        pr1.head_sha = original_sha.clone();
        // Both PR#2 and PR#3 are direct descendants of PR#1 (base_ref = "branch-1")
        let mut pr2 = make_open_pr(2, "branch-1", Some(1));
        pr2.head_ref = "branch-2".to_string();
        let mut pr3 = make_open_pr(3, "branch-1", Some(1)); // Direct child of PR#1
        pr3.head_ref = "branch-3".to_string();

        let prs = HashMap::from([
            (PrNumber(1), pr1.clone()),
            (PrNumber(2), pr2),
            (PrNumber(3), pr3),
        ]);
        let ctx = StepContext::new("main");

        // Step 1: Prepare first descendant (#2)
        let result = execute_cascade_step(train, &prs, &ctx);
        assert_eq!(
            result.train.predecessor_head_sha,
            Some(original_sha.clone()),
            "First preparation should record original SHA"
        );

        // Step 2: Mark first descendant completed, resume train
        let mut train = result.train;
        if let CascadePhase::Preparing { ref mut progress } = train.cascade_phase {
            progress.mark_completed(PrNumber(2));
        }
        train.state = TrainState::Running; // Resume after CI

        // Step 3: Simulate root PR head change (external push)
        let mut pr1_updated = pr1.clone();
        pr1_updated.head_sha = new_sha.clone();
        let mut pr2_updated = make_open_pr(2, "branch-1", Some(1));
        pr2_updated.head_ref = "branch-2".to_string();
        let mut pr3_updated = make_open_pr(3, "branch-1", Some(1));
        pr3_updated.head_ref = "branch-3".to_string();
        let prs_updated = HashMap::from([
            (PrNumber(1), pr1_updated),
            (PrNumber(2), pr2_updated),
            (PrNumber(3), pr3_updated),
        ]);

        // Step 4: Prepare second descendant (#3) - BUG: overwrites predecessor_head_sha
        let result = execute_cascade_step(train, &prs_updated, &ctx);

        // Assertion:
        // predecessor_head_sha should STILL be original_sha, not new_sha
        assert_eq!(
            result.train.predecessor_head_sha,
            Some(original_sha.clone()),
            "predecessor_head_sha must NOT be overwritten on subsequent preparations. \
             Was {}, expected {} (original). Bug: squash guard would compare {} vs {} \
             and incorrectly pass, allowing unprepared commits to be squashed.",
            result
                .train
                .predecessor_head_sha
                .as_ref()
                .map(|s| s.to_string())
                .unwrap_or_default(),
            original_sha,
            new_sha,
            new_sha
        );
    }

    /// Regression test: trains with NO descendants must record predecessor_head_sha
    /// so the squash-time guard can detect post-start pushes.
    ///
    /// Bug scenario (before fix):
    /// 1. Train starts with no descendants, head SHA is X
    /// 2. Someone pushes to PR, head SHA becomes Y
    /// 3. Bot squash-merges - guard doesn't fire because predecessor_head_sha was never set
    /// 4. Result: Unreviewed commits Y are merged
    #[test]
    fn no_descendants_train_records_head_sha() {
        let original_sha = make_sha(0x111);

        // Create train in Idle phase (no descendants)
        let train = TrainRecord::new(PrNumber(1));

        // Build PR map with single PR (no descendants)
        let pr1 = CachedPr::new(
            PrNumber(1),
            original_sha.clone(),
            "branch-1".to_string(),
            "main".to_string(),
            None,
            PrState::Open,
            MergeStateStatus::Clean,
            false,
        );
        let prs = HashMap::from([(PrNumber(1), pr1)]);
        let ctx = StepContext::new("main");

        // Execute cascade step - should transition to SquashPending
        let result = execute_cascade_step(train, &prs, &ctx);

        // Phase should be SquashPending (no descendants to prepare)
        assert!(
            matches!(
                result.train.cascade_phase,
                CascadePhase::SquashPending { .. }
            ),
            "Should transition to SquashPending with no descendants. Got: {:?}",
            result.train.cascade_phase
        );

        // predecessor_head_sha must be recorded
        assert_eq!(
            result.train.predecessor_head_sha,
            Some(original_sha.clone()),
            "BUG: Trains with no descendants must record predecessor_head_sha \
             for the squash-time guard. Without this, post-start pushes go undetected."
        );

        // predecessor_pr should also be set
        assert_eq!(
            result.train.predecessor_pr,
            Some(PrNumber(1)),
            "predecessor_pr should be set to current_pr"
        );
    }

    /// Trains with no descendants must abort on head SHA change.
    ///
    /// Verifies the guard fires for trains with empty descendant lists.
    #[test]
    fn no_descendants_train_aborts_on_head_change() {
        let original_sha = make_sha(0x111);
        let new_sha = make_sha(0x222);

        // Setup: train already in SquashPending with recorded SHA
        let mut train = TrainRecord::new(PrNumber(1));
        train.cascade_phase = CascadePhase::SquashPending {
            progress: DescendantProgress::new(vec![]),
        };
        train.predecessor_pr = Some(PrNumber(1));
        train.predecessor_head_sha = Some(original_sha.clone());
        train.state = TrainState::Running;

        // Build PR map with CHANGED head SHA (simulates post-start push)
        let pr1 = CachedPr::new(
            PrNumber(1),
            new_sha.clone(), // HEAD CHANGED
            "branch-1".to_string(),
            "main".to_string(),
            None,
            PrState::Open,
            MergeStateStatus::Clean,
            false,
        );
        let prs = HashMap::from([(PrNumber(1), pr1)]);
        let ctx = StepContext::new("main");

        // Execute cascade step - should abort
        let result = execute_cascade_step(train, &prs, &ctx);

        // Must abort due to head SHA change
        assert!(
            matches!(
                result.outcome,
                CascadeStepOutcome::Aborted {
                    reason: AbortReason::HeadShaChanged { .. },
                    ..
                }
            ),
            "Train with no descendants must abort on head SHA change. \
             Original: {}, New: {}, Outcome: {:?}",
            original_sha,
            new_sha,
            result.outcome
        );
    }

    /// Regression test: external merge must preserve the skipped set.
    ///
    /// Bug scenario (before fix):
    /// 1. Train in Preparing phase with descendants #2 (skipped/closed) and #3
    /// 2. Descendant #3 is prepared, #2 was skipped (closed PR)
    /// 3. External merge happens
    /// 4. BUG: DescendantProgress::new() clears skipped set
    /// 5. Result: #2 is reintroduced for reconciliation, causing errors
    #[test]
    fn external_merge_preserves_skipped_descendants() {
        let merge_sha = make_sha(0xfff);

        // Setup: train in SquashPending with one descendant skipped
        let mut train = TrainRecord::new(PrNumber(1));
        let mut progress = DescendantProgress::new(vec![PrNumber(2), PrNumber(3)]);
        progress.mark_completed(PrNumber(3)); // #3 was prepared
        progress.mark_skipped(PrNumber(2)); // #2 was skipped (closed)
        train.cascade_phase = CascadePhase::SquashPending { progress };

        // Build PR map with root PR merged externally
        let mut pr1 = make_open_pr(1, "main", None);
        pr1.state = PrState::Merged {
            merge_commit_sha: merge_sha.clone(),
        };
        // #2 is closed (skipped)
        let mut pr2 = CachedPr::new(
            PrNumber(2),
            make_sha(2),
            "branch-2".to_string(),
            "branch-1".to_string(),
            Some(PrNumber(1)),
            PrState::Closed,
            MergeStateStatus::Clean,
            false,
        );
        pr2.head_ref = "branch-2".to_string();
        // #3 is open
        let mut pr3 = make_open_pr(3, "branch-1", Some(1));
        pr3.head_ref = "branch-3".to_string();

        let prs = HashMap::from([(PrNumber(1), pr1), (PrNumber(2), pr2), (PrNumber(3), pr3)]);
        let ctx = StepContext::new("main");

        // Execute cascade step - should transition to Reconciling
        let result = execute_cascade_step(train, &prs, &ctx);

        // Phase must be Reconciling
        match &result.train.cascade_phase {
            CascadePhase::Reconciling { progress, .. } => {
                // Skipped set must be preserved
                assert!(
                    progress.skipped.contains(&PrNumber(2)),
                    "BUG: Skipped descendant #2 was dropped during external merge handling. \
                     Skipped set: {:?}",
                    progress.skipped
                );

                // Completed set should be reset (prepared != reconciled)
                assert!(
                    !progress.completed.contains(&PrNumber(3)),
                    "Completed set should be reset for reconciliation"
                );

                // Only #3 should be in remaining() (not #2 which is skipped)
                let remaining: Vec<_> = progress.remaining().copied().collect();
                assert_eq!(
                    remaining,
                    vec![PrNumber(3)],
                    "Only unskipped descendants should be in remaining(). \
                     Got: {:?}",
                    remaining
                );
            }
            other => panic!(
                "Expected Reconciling phase, got {:?}",
                std::mem::discriminant(other)
            ),
        }
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Property-based invariant tests
    // ─────────────────────────────────────────────────────────────────────────────

    mod property_based_bug_detection {
        use super::*;
        use proptest::prelude::*;

        fn arb_sha() -> impl Strategy<Value = Sha> {
            "[0-9a-f]{40}".prop_map(|s| Sha::parse(s).unwrap())
        }

        fn arb_pr_number() -> impl Strategy<Value = PrNumber> {
            (2u64..100).prop_map(PrNumber)
        }

        fn arb_unique_descendants(min: usize, max: usize) -> impl Strategy<Value = Vec<PrNumber>> {
            prop::collection::hash_set(arb_pr_number(), min..max)
                .prop_map(|set| set.into_iter().collect())
        }

        /// External merge in any phase with unprepared descendants must abort.
        ///
        /// Property: For ANY phase with unprepared direct descendants, an external
        /// merge MUST abort (not silently proceed to Reconciling).
        #[test]
        fn external_merge_in_idle_with_descendants_must_abort() {
            proptest!(|(
                descendants in arb_unique_descendants(1, 3),
                merge_sha in arb_sha()
            )| {
                // Create train in Idle phase (no preparation started)
                let train = TrainRecord::new(PrNumber(1));
                prop_assert!(matches!(train.cascade_phase, CascadePhase::Idle));

                // Build PR map - root is merged externally, descendants exist
                let pr1 = CachedPr::new(
                    PrNumber(1),
                    merge_sha.clone(),
                    "branch-1".to_string(),
                    "main".to_string(),
                    None,
                    PrState::Merged { merge_commit_sha: merge_sha.clone() },
                    MergeStateStatus::Clean,
                    false,
                );
                let mut prs = HashMap::from([(PrNumber(1), pr1)]);

                // Add descendants (all are DIRECT children of #1 - unprepared)
                for &desc in &descendants {
                    let desc_pr = CachedPr::new(
                        desc,
                        merge_sha.clone(),
                        format!("branch-{}", desc.0),
                        "branch-1".to_string(), // base is root's head branch
                        Some(PrNumber(1)),
                        PrState::Open,
                        MergeStateStatus::Clean,
                        false,
                    );
                    prs.insert(desc, desc_pr);
                }

                let ctx = StepContext::new("main");
                let result = execute_cascade_step(train, &prs, &ctx);

                // MUST abort - descendants were never prepared
                prop_assert!(
                    matches!(result.outcome, CascadeStepOutcome::Aborted { .. }),
                    "External merge in Idle with descendants MUST abort. \
                     Descendants: {:?}, Outcome: {:?}",
                    descendants, result.outcome
                );

                if let CascadeStepOutcome::Aborted { reason, .. } = &result.outcome {
                    prop_assert!(
                        matches!(reason, AbortReason::PreparationIncomplete { .. }),
                        "Should be PreparationIncomplete, got: {:?}",
                        reason
                    );
                }
            });
        }

        /// BUG #1 (continued): External merge in SquashPending must reset progress.
        /// The completed set tracks PREPARED descendants, not RECONCILED ones.
        ///
        /// Property: External merge in SquashPending must start Reconciling with
        /// empty completed set (all descendants need reconciliation).
        #[test]
        fn external_merge_in_squash_pending_resets_completed() {
            proptest!(|(
                descendants in arb_unique_descendants(1, 3),
                merge_sha in arb_sha()
            )| {
                // Create train in SquashPending with ALL descendants "prepared"
                let mut progress = DescendantProgress::new(descendants.clone());
                for &desc in &descendants {
                    progress.mark_completed(desc);
                }
                let mut train = TrainRecord::new(PrNumber(1));
                train.cascade_phase = CascadePhase::SquashPending { progress };

                // Build PR map - root is merged externally
                let pr1 = CachedPr::new(
                    PrNumber(1),
                    merge_sha.clone(),
                    "branch-1".to_string(),
                    "main".to_string(),
                    None,
                    PrState::Merged { merge_commit_sha: merge_sha.clone() },
                    MergeStateStatus::Clean,
                    false,
                );
                let mut prs = HashMap::from([(PrNumber(1), pr1)]);

                for &desc in &descendants {
                    let desc_pr = CachedPr::new(
                        desc,
                        merge_sha.clone(),
                        format!("branch-{}", desc.0),
                        "branch-1".to_string(),
                        Some(PrNumber(1)),
                        PrState::Open,
                        MergeStateStatus::Clean,
                        false,
                    );
                    prs.insert(desc, desc_pr);
                }

                let ctx = StepContext::new("main");
                let result = execute_cascade_step(train, &prs, &ctx);

                // Should transition to Reconciling (not abort - prep is complete)
                prop_assert!(
                    matches!(result.train.cascade_phase, CascadePhase::Reconciling { .. }),
                    "External merge in SquashPending should transition to Reconciling. \
                     Got: {:?}",
                    result.train.cascade_phase
                );

                // The completed set must be RESET, not carried over
                if let CascadePhase::Reconciling { progress, .. } = &result.train.cascade_phase {
                    prop_assert!(
                        progress.completed.is_empty(),
                        "BUG: completed set was carried over from SquashPending! \
                         Completed: {:?}. These descendants were PREPARED, not RECONCILED.",
                        progress.completed
                    );
                    prop_assert_eq!(
                        progress.remaining().count(),
                        descendants.len(),
                        "All descendants should need reconciliation"
                    );
                }
            });
        }

        /// BUG #1b: External merge in Preparing with ALL descendants prepared skips reconciliation.
        /// The completed set in Preparing tracks "prepared" not "reconciled".
        ///
        /// Property: External merge in Preparing (with all prepared) must transition to
        /// Reconciling with reset progress, not skip directly to complete_cascade.
        #[test]
        fn external_merge_in_preparing_all_prepared_resets_completed() {
            proptest!(|(
                descendants in arb_unique_descendants(1, 3),
                merge_sha in arb_sha()
            )| {
                // Create train in Preparing with ALL descendants already "prepared" (completed)
                let mut progress = DescendantProgress::new(descendants.clone());
                for &desc in &descendants {
                    progress.mark_completed(desc);
                }
                let mut train = TrainRecord::new(PrNumber(1));
                train.cascade_phase = CascadePhase::Preparing { progress };

                // Build PR map - root is merged externally
                let pr1 = CachedPr::new(
                    PrNumber(1),
                    merge_sha.clone(),
                    "branch-1".to_string(),
                    "main".to_string(),
                    None,
                    PrState::Merged { merge_commit_sha: merge_sha.clone() },
                    MergeStateStatus::Clean,
                    false,
                );
                let mut prs = HashMap::from([(PrNumber(1), pr1)]);

                for &desc in &descendants {
                    let desc_pr = CachedPr::new(
                        desc,
                        merge_sha.clone(),
                        format!("branch-{}", desc.0),
                        "branch-1".to_string(),
                        Some(PrNumber(1)),
                        PrState::Open,
                        MergeStateStatus::Clean,
                        false,
                    );
                    prs.insert(desc, desc_pr);
                }

                let ctx = StepContext::new("main");
                let result = execute_cascade_step(train, &prs, &ctx);

                // Must transition to Reconciling (not Idle/complete)
                prop_assert!(
                    matches!(result.train.cascade_phase, CascadePhase::Reconciling { .. }),
                    "External merge in Preparing (all prepared) must transition to Reconciling. \
                     Got: {:?}. BUG: skipped reconciliation!",
                    result.train.cascade_phase
                );

                // The completed set must be RESET - "prepared" != "reconciled"
                if let CascadePhase::Reconciling { progress, .. } = &result.train.cascade_phase {
                    prop_assert!(
                        progress.completed.is_empty(),
                        "BUG: completed set was carried over from Preparing! \
                         Completed: {:?}. These descendants were PREPARED, not RECONCILED.",
                        progress.completed
                    );
                    prop_assert_eq!(
                        progress.remaining().count(),
                        descendants.len(),
                        "All descendants should need reconciliation"
                    );
                }
            });
        }

        /// External merges with no descendants must validate squash semantics.
        ///
        /// Property: External merge with NO descendants must emit ValidateSquashCommit.
        /// (With descendants, validation happens during reconciliation in the next step.)
        #[test]
        fn external_merge_no_descendants_emits_validate_squash_effect() {
            proptest!(|(merge_sha in arb_sha())| {
                let mut train = TrainRecord::new(PrNumber(1));
                train.status_comment_id = Some(CommentId(12345));
                train.cascade_phase = CascadePhase::Idle; // No descendants

                // Build PR map - root is merged externally, no descendants
                let pr1 = CachedPr::new(
                    PrNumber(1),
                    merge_sha.clone(),
                    "branch-1".to_string(),
                    "main".to_string(),
                    None,
                    PrState::Merged { merge_commit_sha: merge_sha.clone() },
                    MergeStateStatus::Clean,
                    false,
                );
                let prs = HashMap::from([(PrNumber(1), pr1)]);

                let ctx = StepContext::new("main");
                let result = execute_cascade_step(train, &prs, &ctx);

                // Should complete (no descendants to process)
                prop_assert!(
                    matches!(result.outcome, CascadeStepOutcome::Complete),
                    "External merge with no descendants should Complete. Got: {:?}",
                    result.outcome
                );

                // MUST have ValidateSquashCommit effect
                let has_validate_effect = result.effects.iter().any(|e| {
                    matches!(e, Effect::Git(GitEffect::ValidateSquashCommit { .. }))
                });

                prop_assert!(
                    has_validate_effect,
                    "BUG: External merge with no descendants must emit ValidateSquashCommit. \
                     Effects: {:?}",
                    result.effects
                );
            });
        }

        /// Property: External merge WITH descendants transitions to Reconciling.
        /// Validation then happens during reconciliation (MergeReconcile validates).
        #[test]
        fn external_merge_with_descendants_transitions_to_reconciling() {
            proptest!(|(
                descendants in arb_unique_descendants(1, 3),
                merge_sha in arb_sha()
            )| {
                let mut train = TrainRecord::new(PrNumber(1));
                train.status_comment_id = Some(CommentId(12345));

                // SquashPending with all descendants "prepared"
                let mut progress = DescendantProgress::new(descendants.clone());
                for &desc in &descendants {
                    progress.mark_completed(desc);
                }
                train.cascade_phase = CascadePhase::SquashPending { progress };

                // Build PR map - root is merged externally
                let pr1 = CachedPr::new(
                    PrNumber(1),
                    merge_sha.clone(),
                    "branch-1".to_string(),
                    "main".to_string(),
                    None,
                    PrState::Merged { merge_commit_sha: merge_sha.clone() },
                    MergeStateStatus::Clean,
                    false,
                );
                let mut prs = HashMap::from([(PrNumber(1), pr1)]);

                for &desc in &descendants {
                    let desc_pr = CachedPr::new(
                        desc,
                        merge_sha.clone(),
                        format!("branch-{}", desc.0),
                        "branch-1".to_string(),
                        Some(PrNumber(1)),
                        PrState::Open,
                        MergeStateStatus::Clean,
                        false,
                    );
                    prs.insert(desc, desc_pr);
                }

                let ctx = StepContext::new("main");
                let result = execute_cascade_step(train, &prs, &ctx);

                // Must transition to Reconciling (not complete)
                prop_assert!(
                    matches!(result.train.cascade_phase, CascadePhase::Reconciling { .. }),
                    "External merge with descendants must transition to Reconciling. \
                     Got: {:?}",
                    result.train.cascade_phase
                );

                // The reconciliation step will validate via MergeReconcile
                // (but that's in the NEXT execute_cascade_step call, not this one)
            });
        }

        /// Phase transitions from Idle update status comment.
        ///
        /// Property: Every phase transition MUST emit a status comment update
        /// (when status_comment_id is set).
        #[test]
        fn idle_transition_emits_status_comment_update() {
            proptest!(|(
                descendants in arb_unique_descendants(0, 3),
                sha in arb_sha()
            )| {
                let mut train = TrainRecord::new(PrNumber(1));
                train.status_comment_id = Some(CommentId(12345));

                // Build PR map
                let pr1 = CachedPr::new(
                    PrNumber(1),
                    sha.clone(),
                    "branch-1".to_string(),
                    "main".to_string(),
                    None,
                    PrState::Open,
                    MergeStateStatus::Clean,
                    false,
                );
                let mut prs = HashMap::from([(PrNumber(1), pr1)]);

                for &desc in &descendants {
                    let desc_pr = CachedPr::new(
                        desc,
                        sha.clone(),
                        format!("branch-{}", desc.0),
                        "branch-1".to_string(),
                        Some(PrNumber(1)),
                        PrState::Open,
                        MergeStateStatus::Clean,
                        false,
                    );
                    prs.insert(desc, desc_pr);
                }

                let ctx = StepContext::new("main");
                let result = execute_cascade_step(train, &prs, &ctx);

                // Should have transitioned from Idle
                let transitioned = !matches!(result.train.cascade_phase, CascadePhase::Idle);

                if transitioned {
                    // MUST emit status comment update for GitHub-based recovery
                    let has_comment_update = result.effects.iter().any(|e| {
                        matches!(e, Effect::GitHub(GitHubEffect::UpdateComment { comment_id, .. })
                            if *comment_id == CommentId(12345))
                    });
                    prop_assert!(
                        has_comment_update,
                        "BUG: Idle → {:?} transition did not emit status comment update! \
                         Effects: {:?}",
                        result.train.cascade_phase.name(),
                        result.effects
                    );
                }
            });
        }

        /// Train size check counts transitive descendants.
        ///
        /// Property: Train size check must count ALL transitive descendants,
        /// not just immediate children. Deep chains exceeding the 50-PR limit
        /// must trigger abort.
        #[test]
        fn train_size_check_counts_transitive_descendants() {
            // Create a deep linear chain that exceeds limit
            // Root <- D1 <- D2 <- ... <- DN where N > MAX_TRAIN_SIZE
            const CHAIN_LENGTH: usize = 55; // Exceeds MAX_TRAIN_SIZE (50)

            let sha = Sha::parse("abcd1234abcd1234abcd1234abcd1234abcd1234").unwrap();
            let train = TrainRecord::new(PrNumber(1));

            // Build a DEEP chain: 1 <- 2 <- 3 <- ... <- 55
            let mut prs = HashMap::new();
            let pr1 = CachedPr::new(
                PrNumber(1),
                sha.clone(),
                "branch-1".to_string(),
                "main".to_string(),
                None,
                PrState::Open,
                MergeStateStatus::Clean,
                false,
            );
            prs.insert(PrNumber(1), pr1);

            for i in 2..=(CHAIN_LENGTH as u64) {
                let pr = CachedPr::new(
                    PrNumber(i),
                    sha.clone(),
                    format!("branch-{}", i),
                    format!("branch-{}", i - 1), // base is predecessor's head
                    Some(PrNumber(i - 1)),       // predecessor is previous in chain
                    PrState::Open,
                    MergeStateStatus::Clean,
                    false,
                );
                prs.insert(PrNumber(i), pr);
            }

            let ctx = StepContext::new("main");
            let result = execute_cascade_step(train, &prs, &ctx);

            // MUST abort due to train too large
            // BUG: If only counting direct descendants, would only see 1 (PR #2)
            // and proceed, but the full chain is 55 PRs.
            assert!(
                matches!(
                    result.outcome,
                    CascadeStepOutcome::Aborted {
                        reason: AbortReason::TrainTooLarge { .. },
                        ..
                    }
                ),
                "BUG: Deep chain of {} PRs should trigger TrainTooLarge abort. \
                 Only direct descendants counted? Outcome: {:?}",
                CHAIN_LENGTH,
                result.outcome
            );

            if let CascadeStepOutcome::Aborted {
                reason: AbortReason::TrainTooLarge { pr_count, .. },
                ..
            } = &result.outcome
            {
                assert_eq!(
                    *pr_count, CHAIN_LENGTH,
                    "BUG: pr_count should be {} (full chain), not just direct descendants",
                    CHAIN_LENGTH
                );
            }
        }

        /// BUG #6 (property version): For any chain structure, size check must
        /// count all reachable PRs.
        #[test]
        fn transitive_descendant_count_is_correct() {
            proptest!(|(chain_length in 2usize..10)| {
                let sha = Sha::parse("abcd1234abcd1234abcd1234abcd1234abcd1234").unwrap();

                // Build a linear chain
                let mut prs = HashMap::new();
                let pr1 = CachedPr::new(
                    PrNumber(1),
                    sha.clone(),
                    "branch-1".to_string(),
                    "main".to_string(),
                    None,
                    PrState::Open,
                    MergeStateStatus::Clean,
                    false,
                );
                prs.insert(PrNumber(1), pr1);

                for i in 2..=(chain_length as u64) {
                    let pr = CachedPr::new(
                        PrNumber(i),
                        sha.clone(),
                        format!("branch-{}", i),
                        format!("branch-{}", i - 1),
                        Some(PrNumber(i - 1)),
                        PrState::Open,
                        MergeStateStatus::Clean,
                        false,
                    );
                    prs.insert(PrNumber(i), pr);
                }

                // compute_all_descendants should return chain_length - 1 descendants
                let all_desc = compute_all_descendants(PrNumber(1), &prs);
                prop_assert_eq!(
                    all_desc.len(),
                    chain_length - 1,
                    "compute_all_descendants should find {} transitive descendants, found {}",
                    chain_length - 1,
                    all_desc.len()
                );

                // compute_direct_descendants should return only 1 (PR #2)
                let direct_desc = compute_direct_descendants(PrNumber(1), &prs);
                prop_assert_eq!(
                    direct_desc.len(),
                    1,
                    "compute_direct_descendants should find exactly 1 direct child"
                );
                prop_assert!(direct_desc.contains(&PrNumber(2)));
            });
        }

        // ─────────────────────────────────────────────────────────────────────────────
        // State consistency tests
        // ─────────────────────────────────────────────────────────────────────────────

        /// WaitingOnCi outcomes MUST set train.state to WaitingCi.
        ///
        /// Property: Whenever execute_cascade_step returns CascadeStepOutcome::WaitingOnCi,
        /// the returned train.state MUST be TrainState::WaitingCi.
        #[test]
        fn waiting_on_ci_outcome_sets_train_state() {
            proptest!(|(
                descendants in arb_unique_descendants(1, 3),
                sha in arb_sha()
            )| {
                // Create train in Preparing phase with multiple descendants
                let mut train = TrainRecord::new(PrNumber(1));
                train.status_comment_id = Some(CommentId(12345));
                train.predecessor_head_sha = Some(sha.clone());

                // Set up Preparing phase
                let progress = DescendantProgress::new(descendants.clone());
                train.cascade_phase = CascadePhase::Preparing { progress };

                // Build PR map with root open, descendants exist
                let pr1 = CachedPr::new(
                    PrNumber(1),
                    sha.clone(),
                    "branch-1".to_string(),
                    "main".to_string(),
                    None,
                    PrState::Open,
                    MergeStateStatus::Clean,
                    false,
                );
                let mut prs = HashMap::from([(PrNumber(1), pr1)]);

                // Add descendants as open PRs based on branch-1
                for &desc in &descendants {
                    let desc_pr = CachedPr::new(
                        desc,
                        sha.clone(),
                        format!("branch-{}", desc.0),
                        "branch-1".to_string(),
                        Some(PrNumber(1)),
                        PrState::Open,
                        MergeStateStatus::Clean,
                        false,
                    );
                    prs.insert(desc, desc_pr);
                }

                let ctx = StepContext::new("main");
                let result = execute_cascade_step(train, &prs, &ctx);

                // If outcome is WaitingOnCi, train.state MUST be WaitingCi
                if matches!(result.outcome, CascadeStepOutcome::WaitingOnCi { .. }) {
                    prop_assert!(
                        matches!(result.train.state, TrainState::WaitingCi),
                        "WaitingOnCi outcome returned but train.state is {:?}, not WaitingCi. \
                         Outcome: {:?}",
                        result.train.state,
                        result.outcome
                    );
                }
            });
        }

        /// DescendantRetargeted MUST emit RecordReconciliation.
        ///
        /// Property: When processing OperationResult::DescendantRetargeted with
        /// last_squash_sha set, the step MUST emit Effect::RecordReconciliation.
        #[test]
        fn descendant_retargeted_emits_record_reconciliation() {
            proptest!(|(
                descendant_pr in arb_pr_number(),
                squash_sha in arb_sha(),
                head_sha in arb_sha()
            )| {
                // Create train in Retargeting phase with a descendant to retarget
                let mut train = TrainRecord::new(PrNumber(1));
                train.status_comment_id = Some(CommentId(12345));
                train.last_squash_sha = Some(squash_sha.clone());

                // Set up Retargeting phase with descendant pending
                let progress = DescendantProgress::new(vec![descendant_pr]);
                train.cascade_phase = CascadePhase::Retargeting {
                    progress,
                    squash_sha: squash_sha.clone(),
                };

                // Build PR map
                let pr1 = CachedPr::new(
                    PrNumber(1),
                    head_sha.clone(),
                    "branch-1".to_string(),
                    "main".to_string(),
                    None,
                    PrState::Merged { merge_commit_sha: squash_sha.clone() },
                    MergeStateStatus::Clean,
                    false,
                );
                let desc_pr = CachedPr::new(
                    descendant_pr,
                    head_sha.clone(),
                    format!("branch-{}", descendant_pr.0),
                    "main".to_string(), // Already retargeted to main
                    None,               // Predecessor cleared
                    PrState::Open,
                    MergeStateStatus::Clean,
                    false,
                );
                let _prs = HashMap::from([(PrNumber(1), pr1), (descendant_pr, desc_pr)]);

                // Simulate retarget completion via operation result
                let op_result = OperationResult::DescendantRetargeted { pr: descendant_pr };
                let (updated_train, _, effects) =
                    process_operation_result(train.clone(), op_result);

                // MUST emit RecordReconciliation for the retargeted descendant
                let has_record_reconciliation = effects.iter().any(|e| {
                    match e {
                        Effect::RecordReconciliation { pr, squash_sha: s } =>
                            *pr == descendant_pr && *s == squash_sha,
                        _ => false,
                    }
                });

                prop_assert!(
                    has_record_reconciliation,
                    "DescendantRetargeted with last_squash_sha MUST emit RecordReconciliation. \
                     descendant_pr: {:?}, last_squash_sha: {:?}, effects: {:?}",
                    descendant_pr,
                    train.last_squash_sha,
                    effects
                );

                // Verify the descendant is marked completed in progress
                if let CascadePhase::Retargeting { progress, .. } = &updated_train.cascade_phase {
                    prop_assert!(
                        progress.completed.contains(&descendant_pr),
                        "Descendant should be marked completed after retarget"
                    );
                }
            });
        }

        /// DescendantRetargeted aborts on missing last_squash_sha.
        ///
        /// Property: When last_squash_sha is None, DescendantRetargeted MUST abort
        /// with InternalInvariantViolation. This prevents silent fan-out breakage.
        #[test]
        fn descendant_retargeted_aborts_without_squash_sha() {
            proptest!(|(
                descendant_pr in arb_pr_number(),
                head_sha in arb_sha()
            )| {
                // Create train WITHOUT last_squash_sha set
                let mut train = TrainRecord::new(PrNumber(1));
                train.last_squash_sha = None; // Critical: no squash SHA

                // Set up Retargeting phase (squash_sha is in phase but last_squash_sha is None)
                let progress = DescendantProgress::new(vec![descendant_pr]);
                train.cascade_phase = CascadePhase::Retargeting {
                    progress,
                    squash_sha: head_sha.clone(), // Phase has it, but train.last_squash_sha doesn't
                };

                // Simulate retarget completion
                let op_result = OperationResult::DescendantRetargeted { pr: descendant_pr };
                let (updated_train, outcome, effects) = process_operation_result(train, op_result);

                // MUST abort with InternalInvariantViolation
                let is_invariant_violation_abort = matches!(
                    &outcome,
                    Some(CascadeStepOutcome::Aborted {
                        reason: AbortReason::InternalInvariantViolation { .. },
                        ..
                    })
                );

                prop_assert!(
                    is_invariant_violation_abort,
                    "DescendantRetargeted WITHOUT train.last_squash_sha MUST abort with \
                     InternalInvariantViolation. Got outcome: {:?}",
                    outcome
                );

                // Train should be aborted
                prop_assert!(
                    updated_train.state == TrainState::Aborted,
                    "Train state should be Aborted, got: {:?}",
                    updated_train.state
                );

                // Should NOT emit RecordReconciliation (we're aborting, not continuing)
                let has_record_reconciliation = effects.iter().any(|e| {
                    matches!(e, Effect::RecordReconciliation { .. })
                });

                prop_assert!(
                    !has_record_reconciliation,
                    "Aborting train should NOT emit RecordReconciliation. Effects: {:?}",
                    effects
                );
            });
        }

        /// Draft descendants in Preparing phase should return WaitingOnCi with empty effects.
        ///
        /// Property: When a descendant is a draft (is_draft=true) during Preparing phase,
        /// the cascade step should return WaitingOnCi with no effects, and set train.state
        /// to WaitingCi.
        #[test]
        fn draft_descendant_waits_in_preparing() {
            proptest!(|(
                descendant_pr in arb_pr_number(),
                sha in arb_sha()
            )| {
                // Create train in Preparing phase
                let mut train = TrainRecord::new(PrNumber(1));
                train.status_comment_id = Some(CommentId(12345));
                train.predecessor_head_sha = Some(sha.clone());

                let progress = DescendantProgress::new(vec![descendant_pr]);
                train.cascade_phase = CascadePhase::Preparing { progress };

                // Build PR map with OPEN root and draft descendant
                let pr1 = CachedPr::new(
                    PrNumber(1),
                    sha.clone(),
                    "branch-1".to_string(),
                    "main".to_string(),
                    None,
                    PrState::Open,
                    MergeStateStatus::Clean,
                    false,
                );
                // Descendant is a DRAFT
                let desc_pr = CachedPr::new(
                    descendant_pr,
                    sha.clone(),
                    format!("branch-{}", descendant_pr.0),
                    "branch-1".to_string(),
                    Some(PrNumber(1)),
                    PrState::Open,
                    MergeStateStatus::Draft,
                    true,
                );
                let prs = HashMap::from([
                    (PrNumber(1), pr1),
                    (descendant_pr, desc_pr),
                ]);

                let ctx = StepContext::new("main");
                let result = execute_cascade_step(train, &prs, &ctx);

                // Draft descendants should return WaitingOnCi
                prop_assert!(
                    matches!(result.outcome, CascadeStepOutcome::WaitingOnCi { pr_number } if pr_number == descendant_pr),
                    "Draft descendant should return WaitingOnCi, got {:?}",
                    result.outcome
                );

                // Effects should be empty (no modifications to draft PRs)
                prop_assert!(
                    result.effects.is_empty(),
                    "Draft descendant should have empty effects, got {:?}",
                    result.effects
                );

                // Train state should be WaitingCi
                prop_assert!(
                    matches!(result.train.state, TrainState::WaitingCi),
                    "Draft descendant should set train.state to WaitingCi, got {:?}",
                    result.train.state
                );
            });
        }

        /// Draft descendants in post-squash phases should also wait.
        ///
        /// Tests Reconciling, CatchingUp, and Retargeting phases. These require special
        /// setup since the root PR must be Open (Merged roots trigger external merge handling).
        #[test]
        fn draft_descendant_waits_in_post_squash_phases() {
            proptest!(|(
                descendant_pr in arb_pr_number(),
                sha in arb_sha(),
                phase_selector in 0u8..3
            )| {
                // Create train in a post-squash phase
                let mut train = TrainRecord::new(PrNumber(1));
                train.status_comment_id = Some(CommentId(12345));
                train.last_squash_sha = Some(sha.clone());

                let progress = DescendantProgress::new(vec![descendant_pr]);
                train.cascade_phase = match phase_selector {
                    0 => CascadePhase::Reconciling {
                        progress,
                        squash_sha: sha.clone(),
                    },
                    1 => CascadePhase::CatchingUp {
                        progress,
                        squash_sha: sha.clone(),
                    },
                    _ => CascadePhase::Retargeting {
                        progress,
                        squash_sha: sha.clone(),
                    },
                };

                // Root must be OPEN to avoid external merge handling path.
                // In practice, after squash the root is Merged and external merge
                // handling transitions us to Reconciling. But for testing the phase
                // handlers directly, we use Open to bypass that logic.
                let pr1 = CachedPr::new(
                    PrNumber(1),
                    sha.clone(),
                    "branch-1".to_string(),
                    "main".to_string(),
                    None,
                    PrState::Open,
                    MergeStateStatus::Clean,
                    false,
                );
                // Descendant is a DRAFT
                let desc_pr = CachedPr::new(
                    descendant_pr,
                    sha.clone(),
                    format!("branch-{}", descendant_pr.0),
                    "branch-1".to_string(),
                    Some(PrNumber(1)),
                    PrState::Open,
                    MergeStateStatus::Draft,
                    true,
                );
                let prs = HashMap::from([
                    (PrNumber(1), pr1),
                    (descendant_pr, desc_pr),
                ]);

                let ctx = StepContext::new("main");
                let result = execute_cascade_step(train, &prs, &ctx);

                // Draft descendants should return WaitingOnCi
                prop_assert!(
                    matches!(result.outcome, CascadeStepOutcome::WaitingOnCi { pr_number } if pr_number == descendant_pr),
                    "Draft descendant in phase {} should return WaitingOnCi, got {:?}",
                    phase_selector,
                    result.outcome
                );

                // Effects should be empty (no modifications to draft PRs)
                prop_assert!(
                    result.effects.is_empty(),
                    "Draft descendant should have empty effects, got {:?}",
                    result.effects
                );

                // Train state should be WaitingCi
                prop_assert!(
                    matches!(result.train.state, TrainState::WaitingCi),
                    "Draft descendant should set train.state to WaitingCi, got {:?}",
                    result.train.state
                );
            });
        }

        /// TrainTooLarge abort MUST emit status comment update effect.
        ///
        /// Property: When aborting due to TrainTooLarge, if status_comment_id is set,
        /// the effects MUST include an UpdateComment to inform GitHub-based recovery.
        #[test]
        fn train_too_large_emits_status_comment_update() {
            // Create a chain exceeding the limit (needs >50 PRs for MAX_TRAIN_SIZE)
            const CHAIN_LENGTH: usize = 55;

            let sha = Sha::parse("abcd1234abcd1234abcd1234abcd1234abcd1234").unwrap();
            let mut train = TrainRecord::new(PrNumber(1));
            // Set status_comment_id to test comment update
            train.status_comment_id = Some(CommentId(12345));

            // Build a deep chain
            let mut prs = HashMap::new();
            let pr1 = CachedPr::new(
                PrNumber(1),
                sha.clone(),
                "branch-1".to_string(),
                "main".to_string(),
                None,
                PrState::Open,
                MergeStateStatus::Clean,
                false,
            );
            prs.insert(PrNumber(1), pr1);

            for i in 2..=(CHAIN_LENGTH as u64) {
                let pr = CachedPr::new(
                    PrNumber(i),
                    sha.clone(),
                    format!("branch-{}", i),
                    format!("branch-{}", i - 1),
                    Some(PrNumber(i - 1)),
                    PrState::Open,
                    MergeStateStatus::Clean,
                    false,
                );
                prs.insert(PrNumber(i), pr);
            }

            let ctx = StepContext::new("main");
            let result = execute_cascade_step(train, &prs, &ctx);

            // Should abort due to TrainTooLarge
            assert!(
                matches!(
                    result.outcome,
                    CascadeStepOutcome::Aborted {
                        reason: AbortReason::TrainTooLarge { .. },
                        ..
                    }
                ),
                "Should abort with TrainTooLarge, got: {:?}",
                result.outcome
            );

            // MUST emit status comment update
            let has_update_comment = result
                .effects
                .iter()
                .any(|e| matches!(e, Effect::GitHub(GitHubEffect::UpdateComment { .. })));

            assert!(
                has_update_comment,
                "TrainTooLarge abort with status_comment_id MUST emit UpdateComment effect. \
                 Effects: {:?}",
                result.effects
            );
        }

        /// TrainTooLarge abort without status_comment_id should NOT emit update effect.
        ///
        /// Property: When aborting due to TrainTooLarge without a status comment,
        /// no UpdateComment effect should be emitted (nothing to update).
        #[test]
        fn train_too_large_no_update_without_comment_id() {
            const CHAIN_LENGTH: usize = 55;

            let sha = Sha::parse("abcd1234abcd1234abcd1234abcd1234abcd1234").unwrap();
            let mut train = TrainRecord::new(PrNumber(1));
            // NO status_comment_id set
            train.status_comment_id = None;

            let mut prs = HashMap::new();
            let pr1 = CachedPr::new(
                PrNumber(1),
                sha.clone(),
                "branch-1".to_string(),
                "main".to_string(),
                None,
                PrState::Open,
                MergeStateStatus::Clean,
                false,
            );
            prs.insert(PrNumber(1), pr1);

            for i in 2..=(CHAIN_LENGTH as u64) {
                let pr = CachedPr::new(
                    PrNumber(i),
                    sha.clone(),
                    format!("branch-{}", i),
                    format!("branch-{}", i - 1),
                    Some(PrNumber(i - 1)),
                    PrState::Open,
                    MergeStateStatus::Clean,
                    false,
                );
                prs.insert(PrNumber(i), pr);
            }

            let ctx = StepContext::new("main");
            let result = execute_cascade_step(train, &prs, &ctx);

            // Should abort due to TrainTooLarge
            assert!(
                matches!(
                    result.outcome,
                    CascadeStepOutcome::Aborted {
                        reason: AbortReason::TrainTooLarge { .. },
                        ..
                    }
                ),
                "Should abort with TrainTooLarge, got: {:?}",
                result.outcome
            );

            // Should NOT emit any UpdateComment (no comment to update)
            let has_update_comment = result
                .effects
                .iter()
                .any(|e| matches!(e, Effect::GitHub(GitHubEffect::UpdateComment { .. })));

            assert!(
                !has_update_comment,
                "TrainTooLarge abort WITHOUT status_comment_id should NOT emit UpdateComment. \
                 Effects: {:?}",
                result.effects
            );
        }
    }
}
