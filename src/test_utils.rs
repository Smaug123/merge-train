//! Shared test utilities and arbitrary generators for property-based testing.

use crate::persistence::event::{StateEvent, StateEventPayload};
use crate::types::{
    CascadePhase, CommentId, DescendantProgress, PrNumber, Sha, TrainError, TrainErrorKind,
    TrainRecord, TrainState,
};
use chrono::{DateTime, Utc};
use proptest::prelude::*;

pub fn arb_pr_number() -> impl Strategy<Value = PrNumber> {
    any::<u64>().prop_map(PrNumber)
}

pub fn arb_comment_id() -> impl Strategy<Value = CommentId> {
    any::<u64>().prop_map(CommentId)
}

pub fn arb_sha() -> impl Strategy<Value = Sha> {
    "[0-9a-f]{40}".prop_map(|s| Sha::parse(s).unwrap())
}

/// Timestamps in a sane range (years 2000-2100), second precision (chrono's
/// RFC 3339 serde roundtrips exactly at this precision).
pub fn arb_datetime() -> impl Strategy<Value = DateTime<Utc>> {
    (946_684_800i64..4_102_444_800i64).prop_map(|secs| DateTime::from_timestamp(secs, 0).unwrap())
}

/// A fixed timestamp for tests that need *a* time but don't care which.
pub fn test_timestamp() -> DateTime<Utc> {
    DateTime::from_timestamp(1_700_000_000, 0).unwrap()
}

pub fn arb_train_error_kind() -> impl Strategy<Value = TrainErrorKind> {
    proptest::sample::select(&[
        TrainErrorKind::MergeConflict,
        TrainErrorKind::PushRejected,
        TrainErrorKind::PrClosed,
        TrainErrorKind::CiFailed,
        TrainErrorKind::CycleDetected,
        TrainErrorKind::ReviewDismissed,
        TrainErrorKind::ApprovalWithdrawn,
        TrainErrorKind::ApiError,
        TrainErrorKind::BranchDeleted,
        TrainErrorKind::NonSquashMerge,
        TrainErrorKind::StatusCommentTooLarge,
        TrainErrorKind::TrainTooLarge,
        TrainErrorKind::PreparationMissing,
        TrainErrorKind::MergeHooksEnabled,
        TrainErrorKind::PreparationIncomplete,
        TrainErrorKind::BaseBranchMismatch,
        TrainErrorKind::HeadShaChanged,
        TrainErrorKind::InternalInvariantViolation,
    ])
}

pub fn arb_train_state() -> impl Strategy<Value = TrainState> {
    prop_oneof![
        Just(TrainState::Running),
        Just(TrainState::WaitingCi),
        Just(TrainState::NeedsManualReview),
        arb_datetime().prop_map(|ended_at| TrainState::Stopped { ended_at }),
        (arb_datetime(), arb_train_error())
            .prop_map(|(ended_at, error)| TrainState::Aborted { ended_at, error }),
    ]
}

pub fn arb_train_record() -> impl Strategy<Value = TrainRecord> {
    (
        arb_pr_number(),
        arb_pr_number(),
        arb_train_state(),
        arb_cascade_phase(),
        prop::option::of(arb_pr_number()),
        prop::option::of(arb_sha()),
        any::<u64>(),
        arb_datetime(),
        prop::option::of(any::<u64>().prop_map(CommentId)),
    )
        .prop_map(
            |(
                original_root_pr,
                current_pr,
                state,
                cascade_phase,
                predecessor_pr,
                predecessor_head_sha,
                recovery_seq,
                started_at,
                status_comment_id,
            )| {
                TrainRecord {
                    version: 1,
                    recovery_seq,
                    state,
                    original_root_pr,
                    current_pr,
                    cascade_phase,
                    predecessor_pr,
                    predecessor_head_sha,
                    started_at,
                    status_comment_id,
                }
            },
        )
}

/// Text that exercises the status-comment escaping and truncation paths:
/// HTML comment terminators, JSON metacharacters, newlines, and multi-byte
/// UTF-8. The status comment is the backup state store, so its roundtrip
/// property must hold for arbitrary error text, not just `[a-zA-Z0-9 ]`.
pub fn arb_hostile_text(max_fragments: usize) -> impl Strategy<Value = String> {
    let fragment = prop_oneof![
        4 => "[a-zA-Z0-9 ]{1,40}".prop_map(String::from),
        1 => Just("-->".to_string()),
        1 => Just("--!>".to_string()),
        1 => Just("--->".to_string()),
        1 => Just("<!-- merge-train-state\n".to_string()),
        1 => Just("\n\t".to_string()),
        1 => Just("\"\\\u{8}".to_string()),
        1 => Just("🚂🚃🚄".to_string()),
    ];
    prop::collection::vec(fragment, 0..=max_fragments).prop_map(|v| v.concat())
}

pub fn arb_train_error() -> impl Strategy<Value = TrainError> {
    // The long arms exceed MAX_ERROR_MESSAGE_LEN (4096) / MAX_ERROR_STDERR_LEN
    // (2048) so the truncation paths are actually exercised.
    let message = prop_oneof![
        3 => arb_hostile_text(8),
        1 => arb_hostile_text(8).prop_map(|s| format!("{}{}", "m".repeat(4200), s)),
    ];
    let stderr = prop_oneof![
        3 => arb_hostile_text(6),
        1 => arb_hostile_text(6).prop_map(|s| format!("{}{}", "e".repeat(2200), s)),
    ];
    (arb_train_error_kind(), message, prop::option::of(stderr)).prop_map(|(kind, m, stderr)| {
        let mut err = TrainError::new(kind, m);
        if let Some(s) = stderr {
            err = err.with_stderr(s);
        }
        err
    })
}

pub fn arb_descendant_progress() -> impl Strategy<Value = DescendantProgress> {
    prop::collection::vec(arb_pr_number(), 0..5).prop_map(DescendantProgress::new)
}

pub fn arb_cascade_phase() -> impl Strategy<Value = CascadePhase> {
    prop_oneof![
        Just(CascadePhase::Idle),
        arb_descendant_progress().prop_map(|p| CascadePhase::Preparing { progress: p }),
        arb_descendant_progress().prop_map(|p| CascadePhase::SquashPending { progress: p }),
        (arb_descendant_progress(), arb_sha()).prop_map(|(p, s)| CascadePhase::Reconciling {
            progress: p,
            squash_sha: s
        }),
        (arb_descendant_progress(), arb_sha()).prop_map(|(p, s)| CascadePhase::CatchingUp {
            progress: p,
            squash_sha: s
        }),
        (arb_descendant_progress(), arb_sha()).prop_map(|(p, s)| CascadePhase::Retargeting {
            progress: p,
            squash_sha: s
        }),
    ]
}

pub fn arb_branch_name() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9/-]{0,50}".prop_map(String::from)
}

pub fn arb_state_event_payload() -> impl Strategy<Value = StateEventPayload> {
    prop_oneof![
        // Train lifecycle
        (arb_pr_number(), arb_pr_number()).prop_map(|(r, c)| StateEventPayload::TrainStarted {
            root_pr: r,
            current_pr: c
        }),
        arb_pr_number().prop_map(|r| StateEventPayload::TrainStopped { root_pr: r }),
        arb_pr_number().prop_map(|r| StateEventPayload::TrainCompleted { root_pr: r }),
        (arb_pr_number(), arb_train_error()).prop_map(|(r, e)| {
            StateEventPayload::TrainAborted {
                root_pr: r,
                error: e,
            }
        }),
        // Phase transitions
        (
            arb_pr_number(),
            arb_pr_number(),
            prop::option::of(arb_pr_number()),
            prop::option::of(arb_sha()),
            arb_cascade_phase()
        )
            .prop_map(|(tr, cp, pp, ls, ph)| StateEventPayload::PhaseTransition {
                train_root: tr,
                current_pr: cp,
                predecessor_pr: pp,
                last_squash_sha: ls,
                phase: ph
            }),
        (arb_pr_number(), arb_pr_number(), arb_sha()).prop_map(|(tr, pr, sh)| {
            StateEventPayload::SquashCommitted {
                train_root: tr,
                pr,
                sha: sh,
            }
        }),
        // Intent/done - prep
        (arb_pr_number(), arb_branch_name(), arb_sha(), arb_sha()).prop_map(
            |(tr, br, pre, exp)| StateEventPayload::IntentPushPrep {
                train_root: tr,
                branch: br,
                pre_push_sha: pre,
                expected_tree: exp
            }
        ),
        (arb_pr_number(), arb_branch_name()).prop_map(|(tr, br)| {
            StateEventPayload::DonePushPrep {
                train_root: tr,
                branch: br,
            }
        }),
        // Intent squash
        (arb_pr_number(), arb_pr_number())
            .prop_map(|(tr, pr)| StateEventPayload::IntentSquash { train_root: tr, pr }),
        // Intent/done - reconcile
        (arb_pr_number(), arb_branch_name(), arb_sha(), arb_sha()).prop_map(
            |(tr, br, pre, exp)| StateEventPayload::IntentPushReconcile {
                train_root: tr,
                branch: br,
                pre_push_sha: pre,
                expected_tree: exp
            }
        ),
        (arb_pr_number(), arb_branch_name()).prop_map(|(tr, br)| {
            StateEventPayload::DonePushReconcile {
                train_root: tr,
                branch: br,
            }
        }),
        // Intent/done - catchup
        (arb_pr_number(), arb_branch_name(), arb_sha(), arb_sha()).prop_map(
            |(tr, br, pre, exp)| StateEventPayload::IntentPushCatchup {
                train_root: tr,
                branch: br,
                pre_push_sha: pre,
                expected_tree: exp
            }
        ),
        (arb_pr_number(), arb_branch_name()).prop_map(|(tr, br)| {
            StateEventPayload::DonePushCatchup {
                train_root: tr,
                branch: br,
            }
        }),
        // Intent/done - retarget
        (arb_pr_number(), arb_pr_number(), arb_branch_name()).prop_map(|(tr, pr, nb)| {
            StateEventPayload::IntentRetarget {
                train_root: tr,
                pr,
                new_base: nb,
            }
        }),
        (arb_pr_number(), arb_pr_number())
            .prop_map(|(tr, pr)| StateEventPayload::DoneRetarget { train_root: tr, pr }),
        // Fan-out
        (
            arb_pr_number(),
            prop::collection::vec(arb_pr_number(), 1..5),
            arb_pr_number()
        )
            .prop_map(|(old, new, orig)| StateEventPayload::FanOutCompleted {
                old_root: old,
                new_roots: new,
                original_root_pr: orig
            }),
        // Non-critical - PR state
        (arb_pr_number(), arb_sha())
            .prop_map(|(pr, sha)| StateEventPayload::PrMerged { pr, merge_sha: sha }),
        (arb_pr_number(), "[a-z]{1,10}".prop_map(String::from))
            .prop_map(|(pr, st)| StateEventPayload::PrStateChanged { pr, state: st }),
        (arb_pr_number(), arb_pr_number(), arb_comment_id()).prop_map(|(pr, pred, cid)| {
            StateEventPayload::PredecessorDeclared {
                pr,
                predecessor: pred,
                comment_id: cid,
            }
        }),
        (arb_pr_number(), arb_comment_id()).prop_map(|(pr, cid)| {
            StateEventPayload::PredecessorRemoved {
                pr,
                comment_id: cid,
            }
        }),
        // PR lifecycle events
        (
            arb_pr_number(),
            arb_sha(),
            arb_branch_name(),
            arb_branch_name(),
            any::<bool>()
        )
            .prop_map(|(pr, head_sha, head_ref, base_ref, is_draft)| {
                StateEventPayload::PrOpened {
                    pr,
                    head_sha,
                    head_ref,
                    base_ref,
                    is_draft,
                }
            }),
        arb_pr_number().prop_map(|pr| StateEventPayload::PrClosed { pr }),
        arb_pr_number().prop_map(|pr| StateEventPayload::PrReopened { pr }),
        (arb_pr_number(), arb_branch_name(), arb_branch_name()).prop_map(
            |(pr, old_base, new_base)| {
                StateEventPayload::PrBaseChanged {
                    pr,
                    old_base,
                    new_base,
                }
            }
        ),
        (arb_pr_number(), arb_sha()).prop_map(|(pr, sha)| StateEventPayload::PrSynchronized {
            pr,
            new_head_sha: sha
        }),
        arb_pr_number().prop_map(|pr| StateEventPayload::PrConvertedToDraft { pr }),
        arb_pr_number().prop_map(|pr| StateEventPayload::PrReadyForReview { pr }),
        (
            arb_pr_number(),
            arb_pr_number(),
            "[a-z ]{1,50}".prop_map(String::from)
        )
            .prop_map(|(root_pr, descendant_pr, reason)| {
                StateEventPayload::DescendantSkipped {
                    root_pr,
                    descendant_pr,
                    reason,
                }
            }),
        // CI/Review events
        (arb_sha(), "[a-z_]{1,20}".prop_map(String::from)).prop_map(|(sha, conclusion)| {
            StateEventPayload::CheckSuiteCompleted { sha, conclusion }
        }),
        (
            arb_sha(),
            "[a-z/]{1,30}".prop_map(String::from),
            "[a-z]{1,10}".prop_map(String::from)
        )
            .prop_map(|(sha, context, state)| StateEventPayload::StatusReceived {
                sha,
                context,
                state,
            }),
        (
            arb_pr_number(),
            "[a-z]{1,20}".prop_map(String::from),
            "[a-z_]{1,20}".prop_map(String::from)
        )
            .prop_map(|(pr, reviewer, state)| StateEventPayload::ReviewSubmitted {
                pr,
                reviewer,
                state,
            }),
        (arb_pr_number(), "[a-z]{1,20}".prop_map(String::from))
            .prop_map(|(pr, reviewer)| { StateEventPayload::ReviewDismissed { pr, reviewer } }),
    ]
}

pub fn arb_state_event() -> impl Strategy<Value = StateEvent> {
    (any::<u64>(), arb_state_event_payload()).prop_map(|(seq, payload)| StateEvent {
        seq,
        ts: chrono::Utc::now(),
        payload,
    })
}
