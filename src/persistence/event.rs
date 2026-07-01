//! Event types for the persistence event log.
//!
//! These events are appended to the event log in JSON Lines format.
//! Each event has a monotonic sequence number and timestamp.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::{CascadePhase, CommentId, MergeStateStatus, PrNumber, Sha, TrainError};

/// An event in the event log.
///
/// Events are serialized as JSON Lines (one JSON object per line).
/// The payload is flattened into the event object.
///
/// Example JSON:
/// ```json
/// {"seq":0,"ts":"2024-01-15T10:00:00Z","type":"train_started","root_pr":123,"current_pr":123}
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateEvent {
    /// Monotonic sequence number. Used for replay positioning and ordering.
    pub seq: u64,

    /// Timestamp when the event was created (UTC).
    pub ts: DateTime<Utc>,

    /// The event payload, flattened into the JSON object.
    #[serde(flatten)]
    pub payload: StateEventPayload,
}

impl StateEvent {
    /// Returns true if this event requires immediate fsync.
    ///
    /// Critical events must be durable before the operation proceeds:
    /// - Train lifecycle events (start, stop, complete, abort)
    /// - Phase transitions
    /// - Squash commit recording
    /// - Intent/done events for irreversible operations
    /// - Fan-out completion
    ///
    /// Non-critical events can be batched for performance:
    /// - PR merged, state changed, predecessor declared
    pub fn is_critical(&self) -> bool {
        self.payload.is_critical()
    }
}

/// Event payload types for the event log.
///
/// Serialized with internal tagging: `{"type": "train_started", "root_pr": 123, ...}`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum StateEventPayload {
    // ─── Train lifecycle (always critical) ───
    /// A new train has been started.
    #[serde(rename = "train_started")]
    TrainStarted {
        /// The PR that received `@merge-train start`.
        root_pr: PrNumber,
        /// The PR currently being processed (initially same as root_pr).
        current_pr: PrNumber,
    },

    /// A train has been stopped via `@merge-train stop`.
    #[serde(rename = "train_stopped")]
    TrainStopped {
        /// The original root PR of the train.
        root_pr: PrNumber,
    },

    /// A train has completed successfully (all PRs merged).
    #[serde(rename = "train_completed")]
    TrainCompleted {
        /// The original root PR of the train.
        root_pr: PrNumber,
    },

    /// A train has been aborted due to an error.
    #[serde(rename = "train_aborted")]
    TrainAborted {
        /// The original root PR of the train.
        root_pr: PrNumber,
        /// Details about the error.
        error: TrainError,
    },

    /// A train has parked (`TrainState::WaitingCi`): a transient condition
    /// (checks pending, approval temporarily missing, transient squash
    /// failure) that auto-resolves. It resumes on the next relevant
    /// observation (`train_resumed`). Durable so a restart knows the train is
    /// waiting rather than mid-operation.
    #[serde(rename = "train_parked")]
    TrainParked {
        /// The original root PR of the train.
        root_pr: PrNumber,
        /// Human-readable description of what the train is waiting for.
        reason: String,
    },

    /// A parked train has resumed (`TrainState::Running`).
    #[serde(rename = "train_resumed")]
    TrainResumed {
        /// The original root PR of the train.
        root_pr: PrNumber,
    },

    /// The train's status comment was created on the root PR.
    ///
    /// Durable so replay restores `TrainRecord.status_comment_id` — without
    /// it, a restarted bot would post a duplicate status comment instead of
    /// updating the existing one.
    #[serde(rename = "status_comment_posted")]
    StatusCommentPosted {
        /// The original root PR of the train.
        root_pr: PrNumber,
        /// The comment id GitHub assigned.
        comment_id: CommentId,
    },

    // ─── Phase transitions (always critical) ───
    /// The cascade has transitioned to a new phase.
    ///
    /// Phase transitions include all fields needed for restart-safe recovery:
    /// - current_pr: which PR the train is currently processing
    /// - predecessor_pr: for fetching via refs/pull/<n>/head during recovery
    /// - last_squash_sha: for reconciliation recovery
    /// - last_squash_parent: `$SQUASH_SHA^`, the ancestry proof MergeReconcile needs
    /// - phase: the CascadePhase including completed descendant lists
    #[serde(rename = "phase_transition")]
    PhaseTransition {
        /// The original root PR of the train.
        train_root: PrNumber,
        /// The PR currently being processed.
        current_pr: PrNumber,
        /// The predecessor PR (for recovery).
        predecessor_pr: Option<PrNumber>,
        /// The SHA of the last squash commit (for reconciliation recovery).
        last_squash_sha: Option<Sha>,
        /// The parent of the squash commit (`$SQUASH_SHA^`), captured at squash
        /// observation time (seam e). Recovery reads it from the latest
        /// `PhaseTransition` to restore `TrainRecord.last_squash_parent_sha`,
        /// which `MergeReconcile` needs as `expected_squash_parent`. `#[serde(default)]`
        /// so events written before this field existed still deserialize.
        #[serde(default)]
        last_squash_parent: Option<Sha>,
        /// The pinned predecessor head that preparation merged into the
        /// descendants (`PrepareResult::predecessor_head`), carried from the
        /// `SquashPending` transition through the end of the cascade step.
        /// Replay restores `TrainRecord.predecessor_head_sha` from it: it is
        /// both the squash fencing pin (`SquashMerge.expected_sha`) and
        /// `MergeReconcile.predecessor_pre_squash_head` — losing it across a
        /// crash would disarm the force-push-race guard (M2 amendment 2).
        #[serde(default)]
        predecessor_head_sha: Option<Sha>,
        /// The new cascade phase with descendant tracking.
        phase: CascadePhase,
    },

    /// A PR has been squash-merged to the default branch.
    #[serde(rename = "squash_committed")]
    SquashCommitted {
        /// The original root PR of the train.
        train_root: PrNumber,
        /// The PR that was squash-merged.
        pr: PrNumber,
        /// The SHA of the squash commit on the default branch.
        sha: Sha,
    },

    // ─── Intent/done pairs for irreversible operations ───
    //
    // Push intents record `pre_push_sha` (remote ref before our push) and `expected_tree`
    // (tree SHA we expect after the merge). On recovery:
    // 1. Fetch current remote SHA
    // 2. If remote's tree matches expected_tree AND remote's parent chain includes pre_push_sha:
    //    push already succeeded → write completion event
    // 3. Otherwise: re-run merge operations and push
    //
    // We use tree SHA (not commit SHA) because merge commits aren't reproducible across
    // retries (timestamps, signatures vary), but the tree content is deterministic.
    /// Intent: about to push preparation merge to a descendant branch.
    #[serde(rename = "intent_push_prep")]
    IntentPushPrep {
        /// The original root PR of the train.
        train_root: PrNumber,
        /// The branch being pushed to.
        branch: String,
        /// Remote ref SHA before we push (for verifying push actually happened).
        pre_push_sha: Sha,
        /// Expected tree SHA after merge (deterministic, unlike commit SHA).
        expected_tree: Sha,
        /// The pinned predecessor head this preparation merged
        /// (`PrepareResult::predecessor_head`). Durable from the *first* prep
        /// push: recovery mid-`Preparing` reads it so re-prepared descendants
        /// are checked against the same pin, and the engine's consistency rule
        /// ("any later descendant whose pinned head differs ⇒ abort") survives
        /// a crash (M2 amendment 2). `#[serde(default)]` for events written
        /// before the field existed.
        #[serde(default)]
        predecessor_head: Option<Sha>,
    },

    /// Done: preparation push completed successfully.
    #[serde(rename = "done_push_prep")]
    DonePushPrep {
        /// The original root PR of the train.
        train_root: PrNumber,
        /// The branch that was pushed.
        branch: String,
    },

    /// Intent: about to squash-merge a PR.
    #[serde(rename = "intent_squash")]
    IntentSquash {
        /// The original root PR of the train.
        train_root: PrNumber,
        /// The PR about to be squash-merged.
        pr: PrNumber,
    },

    /// Intent: about to push reconciliation (ours-merge) to a descendant branch.
    #[serde(rename = "intent_push_reconcile")]
    IntentPushReconcile {
        /// The original root PR of the train.
        train_root: PrNumber,
        /// The branch being pushed to.
        branch: String,
        /// Remote ref SHA before we push.
        pre_push_sha: Sha,
        /// Expected tree SHA after merge.
        expected_tree: Sha,
    },

    /// Done: reconciliation push completed successfully.
    #[serde(rename = "done_push_reconcile")]
    DonePushReconcile {
        /// The original root PR of the train.
        train_root: PrNumber,
        /// The branch that was pushed.
        branch: String,
    },

    /// A descendant was reconciled against the squash commit (seam c).
    ///
    /// `apply_event` sets `prs[pr].predecessor_squash_reconciled = Some(squash_sha)`,
    /// which is what lets `is_root` later recognize the descendant as a valid
    /// new train root. Equality with the predecessor's `merge_commit_sha` holds
    /// by construction: both flow from the single squash observation.
    #[serde(rename = "reconciliation_recorded")]
    ReconciliationRecorded {
        /// The descendant PR that was reconciled.
        pr: PrNumber,
        /// The squash commit SHA it was reconciled against.
        squash_sha: Sha,
    },

    /// Intent: about to push catch-up merge to a descendant branch.
    #[serde(rename = "intent_push_catchup")]
    IntentPushCatchup {
        /// The original root PR of the train.
        train_root: PrNumber,
        /// The branch being pushed to.
        branch: String,
        /// Remote ref SHA before we push.
        pre_push_sha: Sha,
        /// Expected tree SHA after catch-up merge.
        expected_tree: Sha,
    },

    /// Done: catch-up push completed successfully.
    #[serde(rename = "done_push_catchup")]
    DonePushCatchup {
        /// The original root PR of the train.
        train_root: PrNumber,
        /// The branch that was pushed.
        branch: String,
    },

    /// Intent: about to retarget a PR to a new base branch.
    #[serde(rename = "intent_retarget")]
    IntentRetarget {
        /// The original root PR of the train.
        train_root: PrNumber,
        /// The PR being retargeted.
        pr: PrNumber,
        /// The new base branch.
        new_base: String,
    },

    /// Done: retarget completed successfully.
    #[serde(rename = "done_retarget")]
    DoneRetarget {
        /// The original root PR of the train.
        train_root: PrNumber,
        /// The PR that was retargeted.
        pr: PrNumber,
        /// The branch the PR was retargeted onto. `apply_event` sets
        /// `prs[pr].base_ref = new_base` so the materialized state matches the
        /// retarget the interpreter performed. Required: `done_retarget` is only
        /// emitted by the cascade (M2+), so no field-less legacy events exist —
        /// representing the target as missing-or-empty would lose a successful
        /// retarget on replay (Codex review #49).
        new_base: String,
    },

    // ─── Fan-out (atomic update of train records) ───
    /// Fan-out completed: original train ended, new trains created for descendants.
    #[serde(rename = "fan_out_completed")]
    FanOutCompleted {
        /// Original train root being retired.
        old_root: PrNumber,
        /// New train roots (the descendants that became independent).
        new_roots: Vec<PrNumber>,
        /// For worktree management.
        original_root_pr: PrNumber,
    },

    // ─── Non-critical state updates (batched fsync) ───
    /// A PR has been merged (outside of cascade context).
    #[serde(rename = "pr_merged")]
    PrMerged {
        /// The PR that was merged.
        pr: PrNumber,
        /// The merge commit SHA.
        #[serde(alias = "sha")]
        merge_sha: Sha,
    },

    /// A PR's state has changed (opened, closed, etc.).
    #[serde(rename = "pr_state_changed")]
    PrStateChanged {
        /// The PR whose state changed.
        pr: PrNumber,
        /// The new state (e.g., "open", "closed", "merged").
        state: String,
    },

    /// A predecessor has been declared via `@merge-train predecessor`.
    ///
    /// The `comment_id` tracks which comment is authoritative for this declaration.
    /// This enables proper handling of comment edits and deletions per DESIGN.md.
    #[serde(rename = "predecessor_declared")]
    PredecessorDeclared {
        /// The PR declaring a predecessor.
        pr: PrNumber,
        /// The declared predecessor PR.
        predecessor: PrNumber,
        /// The comment ID containing the authoritative declaration.
        comment_id: CommentId,
    },

    /// A predecessor declaration has been removed.
    ///
    /// This occurs when the authoritative predecessor comment is deleted or edited
    /// to remove the `@merge-train predecessor` command.
    #[serde(rename = "predecessor_removed")]
    PredecessorRemoved {
        /// The PR whose predecessor declaration was removed.
        pr: PrNumber,
        /// The comment ID that was removed/edited.
        comment_id: CommentId,
    },

    // ─── PR lifecycle events (non-critical, for cache updates) ───
    /// A new PR has been opened.
    #[serde(rename = "pr_opened")]
    PrOpened {
        /// The PR number.
        pr: PrNumber,
        /// The head SHA of the PR.
        head_sha: Sha,
        /// The head branch name.
        head_ref: String,
        /// The base branch name.
        base_ref: String,
        /// Whether the PR is a draft.
        is_draft: bool,
    },

    /// A PR has been closed without merging.
    #[serde(rename = "pr_closed")]
    PrClosed {
        /// The PR that was closed.
        pr: PrNumber,
    },

    /// A PR has been reopened.
    #[serde(rename = "pr_reopened")]
    PrReopened {
        /// The PR that was reopened.
        pr: PrNumber,
    },

    /// A PR's base branch has been changed.
    #[serde(rename = "pr_base_changed")]
    PrBaseChanged {
        /// The PR whose base changed.
        pr: PrNumber,
        /// The old base branch.
        old_base: String,
        /// The new base branch.
        new_base: String,
    },

    /// A PR has been synchronized (new commits pushed).
    #[serde(rename = "pr_synchronized")]
    PrSynchronized {
        /// The PR that was synchronized.
        pr: PrNumber,
        /// The new head SHA.
        new_head_sha: Sha,
    },

    /// A PR has been converted to draft.
    #[serde(rename = "pr_converted_to_draft")]
    PrConvertedToDraft {
        /// The PR that was converted to draft.
        pr: PrNumber,
    },

    /// A PR is ready for review (no longer a draft).
    #[serde(rename = "pr_ready_for_review")]
    PrReadyForReview {
        /// The PR that is ready for review.
        pr: PrNumber,
    },

    /// A fresh mergeability observation for a PR (from `GetMergeState` /
    /// `RefetchPr` responses). Mergeability enters the cache through the log
    /// like every other fact; `apply_event` sets
    /// `prs[pr].merge_state_status`.
    #[serde(rename = "pr_merge_state_changed")]
    PrMergeStateChanged {
        /// The PR observed.
        pr: PrNumber,
        /// GitHub's computed merge state.
        status: MergeStateStatus,
    },

    /// A descendant PR was skipped during cascade.
    #[serde(rename = "descendant_skipped")]
    DescendantSkipped {
        /// The train root.
        root_pr: PrNumber,
        /// The descendant that was skipped.
        descendant_pr: PrNumber,
        /// The reason for skipping.
        reason: String,
    },

    // ─── CI/Review events (non-critical, for cache updates) ───
    /// A check suite has completed.
    #[serde(rename = "check_suite_completed")]
    CheckSuiteCompleted {
        /// The commit SHA the check suite ran on.
        sha: Sha,
        /// The conclusion (success, failure, etc.).
        conclusion: String,
    },

    /// A commit status has been received.
    #[serde(rename = "status_received")]
    StatusReceived {
        /// The commit SHA the status is for.
        sha: Sha,
        /// The context (name) of the status check.
        context: String,
        /// The state of the status.
        state: String,
    },

    /// A review has been submitted.
    #[serde(rename = "review_submitted")]
    ReviewSubmitted {
        /// The PR that was reviewed.
        pr: PrNumber,
        /// The reviewer's login.
        reviewer: String,
        /// The review state (approved, changes_requested, etc.).
        state: String,
    },

    /// A review has been dismissed.
    #[serde(rename = "review_dismissed")]
    ReviewDismissed {
        /// The PR whose review was dismissed.
        pr: PrNumber,
        /// The reviewer whose review was dismissed.
        reviewer: String,
    },
}

impl StateEventPayload {
    /// Returns true if this event type requires immediate fsync.
    pub fn is_critical(&self) -> bool {
        // Exhaustive match ensures new variants force explicit classification.
        match self {
            // Train lifecycle
            StateEventPayload::TrainStarted { .. }
            | StateEventPayload::TrainStopped { .. }
            | StateEventPayload::TrainCompleted { .. }
            | StateEventPayload::TrainAborted { .. }
            | StateEventPayload::TrainParked { .. }
            | StateEventPayload::TrainResumed { .. } => true,

            // Phase transitions
            StateEventPayload::PhaseTransition { .. }
            | StateEventPayload::SquashCommitted { .. }
            // Reconciliation marker (is_root-by-construction depends on it)
            | StateEventPayload::ReconciliationRecorded { .. } => true,

            // Intent events (must be durable before performing operation)
            StateEventPayload::IntentPushPrep { .. }
            | StateEventPayload::IntentSquash { .. }
            | StateEventPayload::IntentPushReconcile { .. }
            | StateEventPayload::IntentPushCatchup { .. }
            | StateEventPayload::IntentRetarget { .. } => true,

            // Done events (must be durable before considering operation complete)
            StateEventPayload::DonePushPrep { .. }
            | StateEventPayload::DonePushReconcile { .. }
            | StateEventPayload::DonePushCatchup { .. }
            | StateEventPayload::DoneRetarget { .. } => true,

            // Fan-out (atomic train record updates)
            StateEventPayload::FanOutCompleted { .. } => true,

            // Observational events (not critical for recovery)
            StateEventPayload::StatusCommentPosted { .. }
            | StateEventPayload::PrMerged { .. }
            | StateEventPayload::PrStateChanged { .. }
            | StateEventPayload::PredecessorDeclared { .. }
            | StateEventPayload::PredecessorRemoved { .. }
            | StateEventPayload::PrOpened { .. }
            | StateEventPayload::PrClosed { .. }
            | StateEventPayload::PrReopened { .. }
            | StateEventPayload::PrBaseChanged { .. }
            | StateEventPayload::PrSynchronized { .. }
            | StateEventPayload::PrConvertedToDraft { .. }
            | StateEventPayload::PrReadyForReview { .. }
            | StateEventPayload::PrMergeStateChanged { .. }
            | StateEventPayload::DescendantSkipped { .. }
            | StateEventPayload::CheckSuiteCompleted { .. }
            | StateEventPayload::StatusReceived { .. }
            | StateEventPayload::ReviewSubmitted { .. }
            | StateEventPayload::ReviewDismissed { .. } => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{arb_state_event, arb_state_event_payload};
    use proptest::prelude::*;

    // ─── Property tests ───

    proptest! {
        /// StateEvent serialization roundtrip.
        #[test]
        fn state_event_serde_roundtrip(event in arb_state_event()) {
            let json = serde_json::to_string(&event).unwrap();
            let parsed: StateEvent = serde_json::from_str(&json).unwrap();
            prop_assert_eq!(event, parsed);
        }

        /// StateEventPayload serialization roundtrip.
        #[test]
        fn payload_serde_roundtrip(payload in arb_state_event_payload()) {
            let json = serde_json::to_string(&payload).unwrap();
            let parsed: StateEventPayload = serde_json::from_str(&json).unwrap();
            prop_assert_eq!(payload, parsed);
        }
    }

    // ─── is_critical tests ───

    #[test]
    fn critical_events_are_critical() {
        let critical_payloads = vec![
            StateEventPayload::TrainStarted {
                root_pr: PrNumber(1),
                current_pr: PrNumber(1),
            },
            StateEventPayload::TrainStopped {
                root_pr: PrNumber(1),
            },
            StateEventPayload::TrainCompleted {
                root_pr: PrNumber(1),
            },
            StateEventPayload::TrainAborted {
                root_pr: PrNumber(1),
                error: TrainError::new(crate::types::TrainErrorKind::ApiError, "test"),
            },
            StateEventPayload::TrainParked {
                root_pr: PrNumber(1),
                reason: "waiting for CI".to_string(),
            },
            StateEventPayload::TrainResumed {
                root_pr: PrNumber(1),
            },
            StateEventPayload::PhaseTransition {
                train_root: PrNumber(1),
                current_pr: PrNumber(1),
                predecessor_pr: None,
                last_squash_sha: None,
                last_squash_parent: None,
                predecessor_head_sha: None,
                phase: CascadePhase::Idle,
            },
            StateEventPayload::SquashCommitted {
                train_root: PrNumber(1),
                pr: PrNumber(1),
                sha: Sha::parse("0".repeat(40)).unwrap(),
            },
            StateEventPayload::ReconciliationRecorded {
                pr: PrNumber(1),
                squash_sha: Sha::parse("0".repeat(40)).unwrap(),
            },
            StateEventPayload::IntentPushPrep {
                train_root: PrNumber(1),
                branch: "test".to_string(),
                pre_push_sha: Sha::parse("0".repeat(40)).unwrap(),
                expected_tree: Sha::parse("0".repeat(40)).unwrap(),
                predecessor_head: None,
            },
            StateEventPayload::DonePushPrep {
                train_root: PrNumber(1),
                branch: "test".to_string(),
            },
            StateEventPayload::IntentSquash {
                train_root: PrNumber(1),
                pr: PrNumber(1),
            },
            StateEventPayload::IntentPushReconcile {
                train_root: PrNumber(1),
                branch: "test".to_string(),
                pre_push_sha: Sha::parse("0".repeat(40)).unwrap(),
                expected_tree: Sha::parse("0".repeat(40)).unwrap(),
            },
            StateEventPayload::DonePushReconcile {
                train_root: PrNumber(1),
                branch: "test".to_string(),
            },
            StateEventPayload::IntentPushCatchup {
                train_root: PrNumber(1),
                branch: "test".to_string(),
                pre_push_sha: Sha::parse("0".repeat(40)).unwrap(),
                expected_tree: Sha::parse("0".repeat(40)).unwrap(),
            },
            StateEventPayload::DonePushCatchup {
                train_root: PrNumber(1),
                branch: "test".to_string(),
            },
            StateEventPayload::IntentRetarget {
                train_root: PrNumber(1),
                pr: PrNumber(1),
                new_base: "main".to_string(),
            },
            StateEventPayload::DoneRetarget {
                train_root: PrNumber(1),
                pr: PrNumber(1),
                new_base: "main".to_string(),
            },
            StateEventPayload::FanOutCompleted {
                old_root: PrNumber(1),
                new_roots: vec![PrNumber(2)],
                original_root_pr: PrNumber(1),
            },
        ];

        for payload in critical_payloads {
            assert!(
                payload.is_critical(),
                "Expected {:?} to be critical",
                payload
            );
        }
    }

    #[test]
    fn non_critical_events_are_not_critical() {
        let non_critical_payloads = vec![
            StateEventPayload::StatusCommentPosted {
                root_pr: PrNumber(1),
                comment_id: CommentId(12345),
            },
            StateEventPayload::PrMergeStateChanged {
                pr: PrNumber(1),
                status: MergeStateStatus::Clean,
            },
            StateEventPayload::PrMerged {
                pr: PrNumber(1),
                merge_sha: Sha::parse("0".repeat(40)).unwrap(),
            },
            StateEventPayload::PrStateChanged {
                pr: PrNumber(1),
                state: "open".to_string(),
            },
            StateEventPayload::PredecessorDeclared {
                pr: PrNumber(2),
                predecessor: PrNumber(1),
                comment_id: CommentId(12345),
            },
            StateEventPayload::PredecessorRemoved {
                pr: PrNumber(2),
                comment_id: CommentId(12345),
            },
        ];

        for payload in non_critical_payloads {
            assert!(
                !payload.is_critical(),
                "Expected {:?} to be non-critical",
                payload
            );
        }
    }

    // ─── Backwards compatibility tests ───

    #[test]
    fn pr_merged_deserializes_with_old_sha_field() {
        // Old format used "sha" field name
        let old_json =
            r#"{"type":"pr_merged","pr":42,"sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}"#;
        let parsed: StateEventPayload = serde_json::from_str(old_json).unwrap();

        assert!(matches!(
            parsed,
            StateEventPayload::PrMerged { pr, merge_sha }
            if pr == PrNumber(42) && merge_sha.as_str() == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));
    }

    #[test]
    fn pr_merged_deserializes_with_new_merge_sha_field() {
        // New format uses "merge_sha" field name
        let new_json = r#"{"type":"pr_merged","pr":42,"merge_sha":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}"#;
        let parsed: StateEventPayload = serde_json::from_str(new_json).unwrap();

        assert!(matches!(
            parsed,
            StateEventPayload::PrMerged { pr, merge_sha }
            if pr == PrNumber(42) && merge_sha.as_str() == "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        ));
    }

    #[test]
    fn predecessor_declared_serde_roundtrip() {
        let json = r#"{"type":"predecessor_declared","pr":2,"predecessor":1,"comment_id":12345}"#;
        let parsed: StateEventPayload = serde_json::from_str(json).unwrap();

        assert!(matches!(
            parsed,
            StateEventPayload::PredecessorDeclared { pr, predecessor, comment_id }
            if pr == PrNumber(2) && predecessor == PrNumber(1) && comment_id == CommentId(12345)
        ));

        // Verify roundtrip
        let reserialized = serde_json::to_string(&parsed).unwrap();
        let reparsed: StateEventPayload = serde_json::from_str(&reserialized).unwrap();
        assert_eq!(parsed, reparsed);
    }
}
