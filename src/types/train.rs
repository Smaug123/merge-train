//! Train record and cascade phase types.
//!
//! These types represent the state of an active merge train and the phases
//! of the cascade operation.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use super::ids::{CommentId, PrNumber, Sha};

/// The high-level state of a train.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrainState {
    /// Train is actively running.
    Running,

    /// Train was explicitly stopped via `@merge-train stop`.
    Stopped,

    /// Train is waiting for CI/checks to pass. Auto-resumes when ready.
    WaitingCi,

    /// Train was aborted due to an error. Requires `@merge-train start` to resume.
    Aborted,

    /// Local state was lost and couldn't be recovered. Manual intervention required.
    NeedsManualReview,
}

impl TrainState {
    /// Returns true if the train is active (running or waiting for CI).
    pub fn is_active(&self) -> bool {
        matches!(self, TrainState::Running | TrainState::WaitingCi)
    }

    /// Returns true if the train requires human intervention to resume.
    pub fn requires_restart(&self) -> bool {
        matches!(
            self,
            TrainState::Stopped | TrainState::Aborted | TrainState::NeedsManualReview
        )
    }
}

/// Data common to all cascade phases that process descendants.
///
/// This is carried through from Preparing all the way to Retargeting,
/// ensuring the frozen descendant set is never re-queried.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescendantProgress {
    /// Descendants that have completed this phase successfully.
    pub completed: HashSet<PrNumber>,

    /// Descendants that were skipped due to errors (PR closed, branch deleted, etc.).
    /// These are not retried.
    pub skipped: HashSet<PrNumber>,

    /// The descendant set captured when entering `Preparing`.
    /// This is frozen and carried through ALL subsequent phases.
    /// CRITICAL: Recovery MUST use this, not re-query the descendants index.
    pub frozen_descendants: Vec<PrNumber>,
}

impl DescendantProgress {
    /// Creates a new DescendantProgress with the given frozen descendants.
    pub fn new(frozen_descendants: Vec<PrNumber>) -> Self {
        DescendantProgress {
            completed: HashSet::new(),
            skipped: HashSet::new(),
            frozen_descendants,
        }
    }

    /// Returns the descendants that still need to be processed.
    pub fn remaining(&self) -> impl Iterator<Item = &PrNumber> {
        self.frozen_descendants
            .iter()
            .filter(|pr| !self.completed.contains(pr) && !self.skipped.contains(pr))
    }

    /// Returns true if all frozen descendants are either completed or skipped.
    pub fn is_complete(&self) -> bool {
        self.remaining().next().is_none()
    }

    /// Marks a descendant as completed.
    pub fn mark_completed(&mut self, pr: PrNumber) {
        self.completed.insert(pr);
    }

    /// Marks a descendant as skipped.
    pub fn mark_skipped(&mut self, pr: PrNumber) {
        self.skipped.insert(pr);
    }
}

/// The cascade phase indicates which operation is currently in progress.
///
/// INVARIANT: `frozen_descendants` is captured once when entering `Preparing`
/// and carried through all subsequent phases. It is NEVER re-queried from the
/// descendants index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum CascadePhase {
    /// Not currently performing any operation; waiting for CI or next event.
    Idle,

    /// Merging predecessor head into descendants (before squash).
    /// NOT main - only predecessor head.
    Preparing(DescendantProgress),

    /// Preparation complete; about to squash-merge current PR.
    SquashPending {
        /// Carried forward from Preparing.
        progress: DescendantProgress,
    },

    /// Squash complete; performing ours-strategy merges into descendants.
    Reconciling {
        /// Carried forward from SquashPending.
        progress: DescendantProgress,

        /// The SHA of the squash commit on main.
        squash_sha: Sha,
    },

    /// Ours-merge complete; performing regular merge of origin/main.
    CatchingUp {
        /// Carried forward from Reconciling.
        progress: DescendantProgress,

        /// The SHA of the squash commit on main.
        squash_sha: Sha,
    },

    /// Catch-up complete; retargeting descendant PRs to default branch.
    Retargeting {
        /// Carried forward from CatchingUp.
        progress: DescendantProgress,

        /// The SHA of the squash commit on main.
        squash_sha: Sha,
    },
}

impl CascadePhase {
    /// Returns the name of this phase for logging/display.
    pub fn name(&self) -> &'static str {
        match self {
            CascadePhase::Idle => "idle",
            CascadePhase::Preparing(_) => "preparing",
            CascadePhase::SquashPending { .. } => "squash_pending",
            CascadePhase::Reconciling { .. } => "reconciling",
            CascadePhase::CatchingUp { .. } => "catching_up",
            CascadePhase::Retargeting { .. } => "retargeting",
        }
    }

    /// Returns the descendant progress if this phase has it.
    pub fn progress(&self) -> Option<&DescendantProgress> {
        match self {
            CascadePhase::Idle => None,
            CascadePhase::Preparing(p) => Some(p),
            CascadePhase::SquashPending { progress } => Some(progress),
            CascadePhase::Reconciling { progress, .. } => Some(progress),
            CascadePhase::CatchingUp { progress, .. } => Some(progress),
            CascadePhase::Retargeting { progress, .. } => Some(progress),
        }
    }

    /// Returns a mutable reference to the descendant progress if this phase has it.
    pub fn progress_mut(&mut self) -> Option<&mut DescendantProgress> {
        match self {
            CascadePhase::Idle => None,
            CascadePhase::Preparing(p) => Some(p),
            CascadePhase::SquashPending { progress } => Some(progress),
            CascadePhase::Reconciling { progress, .. } => Some(progress),
            CascadePhase::CatchingUp { progress, .. } => Some(progress),
            CascadePhase::Retargeting { progress, .. } => Some(progress),
        }
    }

    /// Returns the squash SHA if this phase has it.
    pub fn squash_sha(&self) -> Option<&Sha> {
        match self {
            CascadePhase::Reconciling { squash_sha, .. }
            | CascadePhase::CatchingUp { squash_sha, .. }
            | CascadePhase::Retargeting { squash_sha, .. } => Some(squash_sha),
            _ => None,
        }
    }

    /// Checks if a transition from this phase to the target phase is valid.
    ///
    /// Valid transitions:
    /// - Idle -> Preparing (has descendants)
    /// - Idle -> SquashPending (no descendants)
    /// - Preparing -> SquashPending
    /// - SquashPending -> Reconciling (has descendants)
    /// - SquashPending -> Idle (no descendants, advancing to next PR)
    /// - Reconciling -> CatchingUp
    /// - CatchingUp -> Retargeting
    /// - Retargeting -> Idle
    pub fn can_transition_to(&self, target: &CascadePhase) -> bool {
        matches!(
            (self, target),
            (CascadePhase::Idle, CascadePhase::Preparing(_))
                | (CascadePhase::Idle, CascadePhase::SquashPending { .. })
                | (
                    CascadePhase::Preparing(_),
                    CascadePhase::SquashPending { .. }
                )
                | (
                    CascadePhase::SquashPending { .. },
                    CascadePhase::Reconciling { .. }
                )
                | (CascadePhase::SquashPending { .. }, CascadePhase::Idle)
                | (
                    CascadePhase::Reconciling { .. },
                    CascadePhase::CatchingUp { .. }
                )
                | (
                    CascadePhase::CatchingUp { .. },
                    CascadePhase::Retargeting { .. }
                )
                | (CascadePhase::Retargeting { .. }, CascadePhase::Idle)
        )
    }
}

/// Error details when a train is aborted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrainError {
    /// The type of error.
    pub error_type: String,

    /// Human-readable error message. Truncated to 4KB in status comments.
    pub message: String,

    /// stderr output if applicable. Truncated to 2KB in status comments.
    pub stderr: Option<String>,
}

impl TrainError {
    pub fn new(error_type: impl Into<String>, message: impl Into<String>) -> Self {
        TrainError {
            error_type: error_type.into(),
            message: message.into(),
            stderr: None,
        }
    }

    pub fn with_stderr(mut self, stderr: impl Into<String>) -> Self {
        self.stderr = Some(stderr.into());
        self
    }
}

/// A record of an active merge train.
///
/// IMPORTANT: `original_root_pr` is stable throughout the train's lifetime
/// (the PR that received `@merge-train start`). `current_pr` advances as
/// each PR in the stack is processed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrainRecord {
    /// Schema version for forward compatibility.
    pub version: u32,

    /// Monotonic counter incremented on each state change.
    /// Used to determine which record (local vs GitHub) is "ahead" during recovery.
    pub recovery_seq: u64,

    /// The high-level state of the train.
    pub state: TrainState,

    /// The PR that originated this train.
    /// For the initial train, this is the PR that received `@merge-train start`.
    /// For trains created by fan-out, this is the descendant PR that became a new root.
    /// CONSTANT within a single train's lifetime.
    pub original_root_pr: PrNumber,

    /// PR currently being processed. Advances as each PR merges.
    pub current_pr: PrNumber,

    /// The current cascade phase with descendant tracking.
    pub cascade_phase: CascadePhase,

    /// PR number of predecessor (for fetching via `refs/pull/<n>/head` during recovery).
    pub predecessor_pr: Option<PrNumber>,

    /// Head SHA of predecessor at preparation time (for verifying preparation during recovery).
    pub predecessor_head_sha: Option<Sha>,

    /// SHA of last squash commit (for reconciliation recovery).
    pub last_squash_sha: Option<Sha>,

    /// ISO 8601 timestamp when started.
    pub started_at: DateTime<Utc>,

    /// ISO 8601 timestamp if stopped.
    pub stopped_at: Option<DateTime<Utc>>,

    /// Error details if aborted.
    pub error: Option<TrainError>,

    /// The ID of the status comment on the original root PR.
    /// Used to update the comment as the train progresses.
    pub status_comment_id: Option<CommentId>,
}

impl TrainRecord {
    /// Creates a new train record for a starting train.
    pub fn new(root_pr: PrNumber) -> Self {
        TrainRecord {
            version: 1,
            recovery_seq: 0,
            state: TrainState::Running,
            original_root_pr: root_pr,
            current_pr: root_pr,
            cascade_phase: CascadePhase::Idle,
            predecessor_pr: None,
            predecessor_head_sha: None,
            last_squash_sha: None,
            started_at: Utc::now(),
            stopped_at: None,
            error: None,
            status_comment_id: None,
        }
    }

    /// Increments the recovery sequence number.
    /// Must be called on each state change.
    pub fn increment_seq(&mut self) {
        self.recovery_seq += 1;
    }

    /// Marks the train as stopped.
    pub fn stop(&mut self) {
        self.state = TrainState::Stopped;
        self.stopped_at = Some(Utc::now());
        self.increment_seq();
    }

    /// Marks the train as aborted with an error.
    pub fn abort(&mut self, error: TrainError) {
        self.state = TrainState::Aborted;
        self.stopped_at = Some(Utc::now());
        self.error = Some(error);
        self.increment_seq();
    }

    /// Marks the train as waiting for CI.
    pub fn wait_for_ci(&mut self) {
        self.state = TrainState::WaitingCi;
        self.increment_seq();
    }

    /// Resumes the train after waiting for CI.
    pub fn resume(&mut self) {
        self.state = TrainState::Running;
        self.increment_seq();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use proptest::strategy::Strategy;

    fn arb_sha() -> impl Strategy<Value = Sha> {
        "[0-9a-f]{40}".prop_map(Sha::new)
    }

    fn arb_pr_number() -> impl Strategy<Value = PrNumber> {
        any::<u64>().prop_map(PrNumber)
    }

    fn arb_train_state() -> impl Strategy<Value = TrainState> {
        prop_oneof![
            Just(TrainState::Running),
            Just(TrainState::Stopped),
            Just(TrainState::WaitingCi),
            Just(TrainState::Aborted),
            Just(TrainState::NeedsManualReview),
        ]
    }

    fn arb_descendant_progress() -> impl Strategy<Value = DescendantProgress> {
        prop::collection::vec(arb_pr_number(), 0..10).prop_flat_map(|frozen| {
            let frozen_clone = frozen.clone();
            let frozen_len = frozen.len();

            // Handle empty frozen set specially to avoid 0..0 range
            if frozen_len == 0 {
                return Just(DescendantProgress::new(frozen)).boxed();
            }

            // Pick random subsets for completed and skipped
            prop::collection::vec(any::<usize>(), 0..=frozen_len)
                .prop_flat_map(move |completed_indices| {
                    let frozen = frozen_clone.clone();
                    let frozen_len = frozen.len();
                    prop::collection::vec(any::<usize>(), 0..=frozen_len).prop_map(
                        move |skipped_indices| {
                            let mut progress = DescendantProgress::new(frozen.clone());
                            for i in &completed_indices {
                                if *i < frozen.len() {
                                    progress.mark_completed(frozen[*i]);
                                }
                            }
                            for i in &skipped_indices {
                                if *i < frozen.len() && !progress.completed.contains(&frozen[*i]) {
                                    progress.mark_skipped(frozen[*i]);
                                }
                            }
                            progress
                        },
                    )
                })
                .boxed()
        })
    }

    fn arb_cascade_phase() -> impl Strategy<Value = CascadePhase> {
        prop_oneof![
            Just(CascadePhase::Idle),
            arb_descendant_progress().prop_map(CascadePhase::Preparing),
            arb_descendant_progress().prop_map(|progress| CascadePhase::SquashPending { progress }),
            (arb_descendant_progress(), arb_sha()).prop_map(|(progress, sha)| {
                CascadePhase::Reconciling {
                    progress,
                    squash_sha: sha,
                }
            }),
            (arb_descendant_progress(), arb_sha()).prop_map(|(progress, sha)| {
                CascadePhase::CatchingUp {
                    progress,
                    squash_sha: sha,
                }
            }),
            (arb_descendant_progress(), arb_sha()).prop_map(|(progress, sha)| {
                CascadePhase::Retargeting {
                    progress,
                    squash_sha: sha,
                }
            }),
        ]
    }

    mod train_state {
        use super::*;

        proptest! {
            #[test]
            fn serde_roundtrip(state in arb_train_state()) {
                let json = serde_json::to_string(&state).unwrap();
                let parsed: TrainState = serde_json::from_str(&json).unwrap();
                prop_assert_eq!(state, parsed);
            }
        }

        #[test]
        fn is_active_correct() {
            assert!(TrainState::Running.is_active());
            assert!(TrainState::WaitingCi.is_active());
            assert!(!TrainState::Stopped.is_active());
            assert!(!TrainState::Aborted.is_active());
            assert!(!TrainState::NeedsManualReview.is_active());
        }

        #[test]
        fn requires_restart_correct() {
            assert!(!TrainState::Running.requires_restart());
            assert!(!TrainState::WaitingCi.requires_restart());
            assert!(TrainState::Stopped.requires_restart());
            assert!(TrainState::Aborted.requires_restart());
            assert!(TrainState::NeedsManualReview.requires_restart());
        }
    }

    mod descendant_progress {
        use super::*;

        proptest! {
            #[test]
            fn serde_roundtrip(progress in arb_descendant_progress()) {
                let json = serde_json::to_string(&progress).unwrap();
                let parsed: DescendantProgress = serde_json::from_str(&json).unwrap();
                prop_assert_eq!(progress, parsed);
            }

            #[test]
            fn remaining_excludes_completed_and_skipped(progress in arb_descendant_progress()) {
                let remaining: Vec<_> = progress.remaining().collect();
                for pr in &remaining {
                    prop_assert!(!progress.completed.contains(pr));
                    prop_assert!(!progress.skipped.contains(pr));
                }
            }

            #[test]
            fn is_complete_when_all_processed(frozen in prop::collection::vec(arb_pr_number(), 0..10)) {
                let mut progress = DescendantProgress::new(frozen.clone());

                // Mark all as completed or skipped
                for (i, pr) in frozen.iter().enumerate() {
                    if i % 2 == 0 {
                        progress.mark_completed(*pr);
                    } else {
                        progress.mark_skipped(*pr);
                    }
                }

                prop_assert!(progress.is_complete());
            }
        }
    }

    mod cascade_phase {
        use super::*;

        proptest! {
            #[test]
            fn serde_roundtrip(phase in arb_cascade_phase()) {
                let json = serde_json::to_string(&phase).unwrap();
                let parsed: CascadePhase = serde_json::from_str(&json).unwrap();
                prop_assert_eq!(phase, parsed);
            }

            #[test]
            fn frozen_descendants_preserved_across_transitions(
                frozen in prop::collection::vec(arb_pr_number(), 1..10),
                sha in arb_sha()
            ) {
                let progress = DescendantProgress::new(frozen.clone());

                let preparing = CascadePhase::Preparing(progress.clone());
                let squash_pending = CascadePhase::SquashPending { progress: progress.clone() };
                let reconciling = CascadePhase::Reconciling {
                    progress: progress.clone(),
                    squash_sha: sha.clone(),
                };
                let catching_up = CascadePhase::CatchingUp {
                    progress: progress.clone(),
                    squash_sha: sha.clone(),
                };
                let retargeting = CascadePhase::Retargeting {
                    progress: progress.clone(),
                    squash_sha: sha.clone(),
                };

                // All phases that carry progress should have the same frozen_descendants
                for phase in [preparing, squash_pending, reconciling, catching_up, retargeting] {
                    let phase_progress = phase.progress().unwrap();
                    prop_assert_eq!(&phase_progress.frozen_descendants, &frozen);
                }
            }
        }

        #[test]
        fn valid_transitions() {
            let progress = DescendantProgress::new(vec![PrNumber(1)]);
            let sha = Sha::new("abc123def456789012345678901234567890abcd");

            let idle = CascadePhase::Idle;
            let preparing = CascadePhase::Preparing(progress.clone());
            let squash_pending = CascadePhase::SquashPending {
                progress: progress.clone(),
            };
            let reconciling = CascadePhase::Reconciling {
                progress: progress.clone(),
                squash_sha: sha.clone(),
            };
            let catching_up = CascadePhase::CatchingUp {
                progress: progress.clone(),
                squash_sha: sha.clone(),
            };
            let retargeting = CascadePhase::Retargeting {
                progress: progress.clone(),
                squash_sha: sha.clone(),
            };

            // Valid transitions
            assert!(idle.can_transition_to(&preparing));
            assert!(idle.can_transition_to(&squash_pending));
            assert!(preparing.can_transition_to(&squash_pending));
            assert!(squash_pending.can_transition_to(&reconciling));
            assert!(squash_pending.can_transition_to(&idle));
            assert!(reconciling.can_transition_to(&catching_up));
            assert!(catching_up.can_transition_to(&retargeting));
            assert!(retargeting.can_transition_to(&idle));
        }

        #[test]
        fn invalid_transitions() {
            let progress = DescendantProgress::new(vec![PrNumber(1)]);
            let sha = Sha::new("abc123def456789012345678901234567890abcd");

            let idle = CascadePhase::Idle;
            let preparing = CascadePhase::Preparing(progress.clone());
            let reconciling = CascadePhase::Reconciling {
                progress: progress.clone(),
                squash_sha: sha.clone(),
            };
            let retargeting = CascadePhase::Retargeting {
                progress: progress.clone(),
                squash_sha: sha.clone(),
            };

            // Invalid transitions - skipping phases
            assert!(!idle.can_transition_to(&reconciling));
            assert!(!preparing.can_transition_to(&reconciling));
            assert!(!preparing.can_transition_to(&retargeting));

            // Invalid transitions - going backwards
            assert!(!reconciling.can_transition_to(&preparing));
            assert!(!retargeting.can_transition_to(&reconciling));
        }
    }

    mod train_record {
        use super::*;
        use crate::types::ids::CommentId;

        fn arb_train_error() -> impl Strategy<Value = TrainError> {
            (
                "[a-zA-Z_]{1,20}",
                "[a-zA-Z0-9 ]{1,100}",
                prop::option::of("[a-zA-Z0-9 ]{1,50}".prop_map(|s| s.to_string())),
            )
                .prop_map(|(error_type, message, stderr)| {
                    let mut err = TrainError::new(error_type, message);
                    if let Some(s) = stderr {
                        err = err.with_stderr(s);
                    }
                    err
                })
        }

        fn arb_datetime() -> impl Strategy<Value = DateTime<Utc>> {
            // Generate timestamps in a reasonable range (year 2000-2100)
            (946684800i64..4102444800i64)
                .prop_map(|secs| DateTime::from_timestamp(secs, 0).unwrap())
        }

        fn arb_comment_id() -> impl Strategy<Value = CommentId> {
            any::<u64>().prop_map(CommentId)
        }

        fn arb_train_record() -> impl Strategy<Value = TrainRecord> {
            (
                arb_pr_number(),
                arb_pr_number(),
                arb_train_state(),
                arb_cascade_phase(),
                prop::option::of(arb_pr_number()),
                prop::option::of(arb_sha()),
                prop::option::of(arb_sha()),
                prop::option::of(arb_train_error()),
                any::<u64>(),
                arb_datetime(),
                prop::option::of(arb_datetime()),
                prop::option::of(arb_comment_id()),
            )
                .prop_map(
                    |(
                        original_root_pr,
                        current_pr,
                        state,
                        cascade_phase,
                        predecessor_pr,
                        predecessor_head_sha,
                        last_squash_sha,
                        error,
                        recovery_seq,
                        started_at,
                        stopped_at,
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
                            last_squash_sha,
                            started_at,
                            stopped_at,
                            error,
                            status_comment_id,
                        }
                    },
                )
        }

        proptest! {
            #[test]
            fn serde_roundtrip(record in arb_train_record()) {
                let json = serde_json::to_string(&record).unwrap();
                let parsed: TrainRecord = serde_json::from_str(&json).unwrap();
                // Full equality check - serde roundtrip should be lossless
                prop_assert_eq!(record, parsed);
            }

            #[test]
            fn increment_seq_increases(initial_seq: u64) {
                // Avoid overflow
                if initial_seq < u64::MAX {
                    let mut record = TrainRecord::new(PrNumber(1));
                    record.recovery_seq = initial_seq;
                    record.increment_seq();
                    prop_assert_eq!(record.recovery_seq, initial_seq + 1);
                }
            }
        }

        #[test]
        fn new_creates_valid_record() {
            let record = TrainRecord::new(PrNumber(123));

            assert_eq!(record.version, 1);
            assert_eq!(record.recovery_seq, 0);
            assert_eq!(record.state, TrainState::Running);
            assert_eq!(record.original_root_pr, PrNumber(123));
            assert_eq!(record.current_pr, PrNumber(123));
            assert_eq!(record.cascade_phase, CascadePhase::Idle);
            assert!(record.predecessor_pr.is_none());
            assert!(record.predecessor_head_sha.is_none());
            assert!(record.last_squash_sha.is_none());
            assert!(record.stopped_at.is_none());
            assert!(record.error.is_none());
            assert!(record.status_comment_id.is_none());
        }

        #[test]
        fn stop_sets_state_and_timestamp() {
            let mut record = TrainRecord::new(PrNumber(123));
            let initial_seq = record.recovery_seq;

            record.stop();

            assert_eq!(record.state, TrainState::Stopped);
            assert!(record.stopped_at.is_some());
            assert_eq!(record.recovery_seq, initial_seq + 1);
        }

        #[test]
        fn abort_sets_state_and_error() {
            let mut record = TrainRecord::new(PrNumber(123));
            let initial_seq = record.recovery_seq;

            record.abort(TrainError::new("merge_conflict", "Cannot merge"));

            assert_eq!(record.state, TrainState::Aborted);
            assert!(record.stopped_at.is_some());
            assert!(record.error.is_some());
            assert_eq!(record.error.as_ref().unwrap().error_type, "merge_conflict");
            assert_eq!(record.recovery_seq, initial_seq + 1);
        }
    }
}
