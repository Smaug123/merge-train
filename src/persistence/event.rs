//! Event types for the persistence event log.
//!
//! These events are appended to the event log in JSON Lines format.
//! Each event has a monotonic sequence number and timestamp.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::{CascadePhase, PrNumber, Sha, TrainError};

/// An event in the event log.
///
/// Events are serialized as JSON Lines (one JSON object per line).
/// The payload is flattened into the event object.
///
/// Example JSON:
/// ```json
/// {"seq":1,"ts":"2024-01-15T10:00:00Z","type":"train_started","root_pr":123,"current_pr":123}
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
    /// Creates a new event with the given sequence number and payload.
    /// Timestamp is set to the current time.
    pub fn new(seq: u64, payload: StateEventPayload) -> Self {
        StateEvent {
            seq,
            ts: Utc::now(),
            payload,
        }
    }

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

    // ─── Phase transitions (always critical) ───
    /// The cascade has transitioned to a new phase.
    ///
    /// Phase transitions include all fields needed for restart-safe recovery:
    /// - current_pr: which PR the train is currently processing
    /// - predecessor_pr: for fetching via refs/pull/<n>/head during recovery
    /// - last_squash_sha: for reconciliation recovery
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
        sha: Sha,
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
    #[serde(rename = "predecessor_declared")]
    PredecessorDeclared {
        /// The PR declaring a predecessor.
        pr: PrNumber,
        /// The declared predecessor PR.
        predecessor: PrNumber,
    },
}

impl StateEventPayload {
    /// Returns true if this event type requires immediate fsync.
    pub fn is_critical(&self) -> bool {
        matches!(
            self,
            // Train lifecycle
            StateEventPayload::TrainStarted { .. }
                | StateEventPayload::TrainStopped { .. }
                | StateEventPayload::TrainCompleted { .. }
                | StateEventPayload::TrainAborted { .. }
                // Phase transitions
                | StateEventPayload::PhaseTransition { .. }
                | StateEventPayload::SquashCommitted { .. }
                // Intent events (must be durable before performing operation)
                | StateEventPayload::IntentPushPrep { .. }
                | StateEventPayload::IntentSquash { .. }
                | StateEventPayload::IntentPushReconcile { .. }
                | StateEventPayload::IntentPushCatchup { .. }
                | StateEventPayload::IntentRetarget { .. }
                // Done events (must be durable before considering operation complete)
                | StateEventPayload::DonePushPrep { .. }
                | StateEventPayload::DonePushReconcile { .. }
                | StateEventPayload::DonePushCatchup { .. }
                | StateEventPayload::DoneRetarget { .. }
                // Fan-out (atomic train record updates)
                | StateEventPayload::FanOutCompleted { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DescendantProgress;
    use proptest::prelude::*;

    // ─── Arbitrary implementations for property testing ───

    fn arb_pr_number() -> impl Strategy<Value = PrNumber> {
        any::<u64>().prop_map(PrNumber)
    }

    fn arb_sha() -> impl Strategy<Value = Sha> {
        "[0-9a-f]{40}".prop_map(|s| Sha::parse(s).unwrap())
    }

    fn arb_train_error() -> impl Strategy<Value = TrainError> {
        ("[a-z_]{1,20}", "[a-zA-Z0-9 ]{1,100}").prop_map(|(t, m)| TrainError::new(t, m))
    }

    fn arb_descendant_progress() -> impl Strategy<Value = DescendantProgress> {
        prop::collection::vec(arb_pr_number(), 0..5).prop_map(DescendantProgress::new)
    }

    fn arb_cascade_phase() -> impl Strategy<Value = CascadePhase> {
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

    fn arb_branch_name() -> impl Strategy<Value = String> {
        "[a-z][a-z0-9/-]{0,50}".prop_map(String::from)
    }

    fn arb_state_event_payload() -> impl Strategy<Value = StateEventPayload> {
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
            // Non-critical
            (arb_pr_number(), arb_sha())
                .prop_map(|(pr, sha)| StateEventPayload::PrMerged { pr, sha }),
            (arb_pr_number(), "[a-z]{1,10}".prop_map(String::from))
                .prop_map(|(pr, st)| StateEventPayload::PrStateChanged { pr, state: st }),
            (arb_pr_number(), arb_pr_number()).prop_map(|(pr, pred)| {
                StateEventPayload::PredecessorDeclared {
                    pr,
                    predecessor: pred,
                }
            }),
        ]
    }

    fn arb_state_event() -> impl Strategy<Value = StateEvent> {
        (any::<u64>(), arb_state_event_payload())
            .prop_map(|(seq, payload)| StateEvent::new(seq, payload))
    }

    // ─── Property tests ───

    proptest! {
        /// StateEvent serialization roundtrip.
        #[test]
        fn state_event_serde_roundtrip(event in arb_state_event()) {
            let json = serde_json::to_string(&event).unwrap();
            let parsed: StateEvent = serde_json::from_str(&json).unwrap();
            // Compare everything except timestamp (which is set at creation time)
            prop_assert_eq!(event.seq, parsed.seq);
            prop_assert_eq!(event.payload, parsed.payload);
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
                error: TrainError::new("test", "test"),
            },
            StateEventPayload::PhaseTransition {
                train_root: PrNumber(1),
                current_pr: PrNumber(1),
                predecessor_pr: None,
                last_squash_sha: None,
                phase: CascadePhase::Idle,
            },
            StateEventPayload::SquashCommitted {
                train_root: PrNumber(1),
                pr: PrNumber(1),
                sha: Sha::parse("0".repeat(40)).unwrap(),
            },
            StateEventPayload::IntentPushPrep {
                train_root: PrNumber(1),
                branch: "test".to_string(),
                pre_push_sha: Sha::parse("0".repeat(40)).unwrap(),
                expected_tree: Sha::parse("0".repeat(40)).unwrap(),
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
            StateEventPayload::PrMerged {
                pr: PrNumber(1),
                sha: Sha::parse("0".repeat(40)).unwrap(),
            },
            StateEventPayload::PrStateChanged {
                pr: PrNumber(1),
                state: "open".to_string(),
            },
            StateEventPayload::PredecessorDeclared {
                pr: PrNumber(2),
                predecessor: PrNumber(1),
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
}
