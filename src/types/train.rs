//! Train record and cascade phase types.
//!
//! These types represent the state of an active merge train and the phases
//! of the cascade operation.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use super::ids::{CommentId, PrNumber, Sha};

/// The high-level state of a train.
///
/// Terminal states carry their own data: a train cannot be `Aborted` without
/// an error, nor `Stopped`/`Aborted` without an end timestamp, and an active
/// train cannot carry either.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrainState {
    /// Train is actively running.
    Running,

    /// Train is waiting for CI/checks to pass. Auto-resumes when ready.
    WaitingCi,

    /// Train was explicitly stopped via `@merge-train stop`.
    Stopped {
        /// When the train was stopped.
        ended_at: DateTime<Utc>,
    },

    /// Train finished successfully (every PR merged or fanned out).
    ///
    /// Never present in `active_trains` (completion *removes* the record);
    /// this variant exists for the final status comment, so GitHub-fallback
    /// recovery (M6) reads "completed" rather than a stale "running" and does
    /// not resurrect a finished train.
    Completed {
        /// When the train completed.
        ended_at: DateTime<Utc>,
    },

    /// Train was aborted due to an error. Requires `@merge-train start` to resume.
    Aborted {
        /// When the train was aborted.
        ended_at: DateTime<Utc>,
        /// What went wrong.
        error: TrainError,
    },

    /// Local state was lost and couldn't be recovered. Manual intervention required.
    NeedsManualReview,
}

impl TrainState {
    /// Returns true if the train is active (running or waiting for CI).
    pub fn is_active(&self) -> bool {
        matches!(self, TrainState::Running | TrainState::WaitingCi)
    }

    /// Returns true if the train requires human intervention to resume.
    /// (`Completed` is terminal but needs no intervention: there is nothing
    /// left to resume.)
    pub fn requires_restart(&self) -> bool {
        matches!(
            self,
            TrainState::Stopped { .. } | TrainState::Aborted { .. } | TrainState::NeedsManualReview
        )
    }

    /// The abort error, if the train is aborted.
    pub fn error(&self) -> Option<&TrainError> {
        match self {
            TrainState::Aborted { error, .. } => Some(error),
            _ => None,
        }
    }

    /// When the train ended (stopped, aborted, or completed), if it has.
    pub fn ended_at(&self) -> Option<DateTime<Utc>> {
        match self {
            TrainState::Stopped { ended_at }
            | TrainState::Aborted { ended_at, .. }
            | TrainState::Completed { ended_at } => Some(*ended_at),
            _ => None,
        }
    }
}

/// An invalid marking of a descendant, or invalid persisted progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressError {
    /// The PR is not in the frozen descendant set.
    NotInFrozenSet { pr: PrNumber },

    /// The PR was already skipped; a skipped descendant is never revisited.
    AlreadySkipped { pr: PrNumber },

    /// The PR already completed this phase; completed work cannot be skipped.
    AlreadyCompleted { pr: PrNumber },

    /// The frozen descendant set contains a duplicate (persisted input only;
    /// `DescendantProgress::new` deduplicates).
    DuplicateFrozen { pr: PrNumber },
}

impl std::fmt::Display for ProgressError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProgressError::NotInFrozenSet { pr } => {
                write!(f, "PR {} is not in the frozen descendant set", pr)
            }
            ProgressError::AlreadySkipped { pr } => {
                write!(f, "PR {} was already skipped", pr)
            }
            ProgressError::AlreadyCompleted { pr } => {
                write!(f, "PR {} already completed this phase", pr)
            }
            ProgressError::DuplicateFrozen { pr } => {
                write!(
                    f,
                    "PR {} appears more than once in the frozen descendant set",
                    pr
                )
            }
        }
    }
}

impl std::error::Error for ProgressError {}

/// Data common to all cascade phases that process descendants.
///
/// This is carried through from Preparing all the way to Retargeting,
/// ensuring the frozen descendant set is never re-queried.
///
/// Invariants (enforced by construction, including on deserialization):
/// - `completed ⊆ frozen_descendants` and `skipped ⊆ frozen_descendants`
/// - `completed ∩ skipped = ∅`
/// - `frozen_descendants` contains no duplicates
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RawDescendantProgress")]
pub struct DescendantProgress {
    /// Descendants that have completed this phase successfully.
    completed: HashSet<PrNumber>,

    /// Descendants that were skipped due to errors (PR closed, branch deleted, etc.).
    /// These are not retried.
    skipped: HashSet<PrNumber>,

    /// The descendant set captured when entering `Preparing`.
    /// Frozen and carried through all subsequent phases.
    /// Recovery must use this, not re-query the descendants index.
    frozen_descendants: Vec<PrNumber>,
}

/// Serde mirror of [`DescendantProgress`]. Deserialization goes through
/// `TryFrom` because persisted progress (status-comment JSON, snapshots) is
/// untrusted recovery input: progress violating the invariants is corrupt
/// and must be rejected, not repaired.
#[derive(Deserialize)]
struct RawDescendantProgress {
    completed: HashSet<PrNumber>,
    skipped: HashSet<PrNumber>,
    frozen_descendants: Vec<PrNumber>,
}

impl TryFrom<RawDescendantProgress> for DescendantProgress {
    type Error = ProgressError;

    fn try_from(raw: RawDescendantProgress) -> Result<Self, ProgressError> {
        let mut seen = HashSet::new();
        for pr in &raw.frozen_descendants {
            if !seen.insert(*pr) {
                return Err(ProgressError::DuplicateFrozen { pr: *pr });
            }
        }
        let mut progress = DescendantProgress {
            completed: HashSet::new(),
            skipped: HashSet::new(),
            frozen_descendants: raw.frozen_descendants,
        };
        for pr in raw.completed {
            progress.mark_completed(pr)?;
        }
        for pr in raw.skipped {
            progress.mark_skipped(pr)?;
        }
        Ok(progress)
    }
}

impl DescendantProgress {
    /// Creates a new DescendantProgress with the given frozen descendants.
    ///
    /// Duplicates are removed, preserving first-occurrence order: progress is
    /// tracked per PR, so a PR listed twice is still one unit of work.
    pub fn new(frozen_descendants: Vec<PrNumber>) -> Self {
        let mut seen = HashSet::new();
        let frozen_descendants = frozen_descendants
            .into_iter()
            .filter(|pr| seen.insert(*pr))
            .collect();
        DescendantProgress {
            completed: HashSet::new(),
            skipped: HashSet::new(),
            frozen_descendants,
        }
    }

    /// Descendants that have completed the current phase.
    pub fn completed(&self) -> &HashSet<PrNumber> {
        &self.completed
    }

    /// Descendants that were skipped; never revisited in any later phase.
    pub fn skipped(&self) -> &HashSet<PrNumber> {
        &self.skipped
    }

    /// The descendant set captured when the cascade started.
    pub fn frozen_descendants(&self) -> &[PrNumber] {
        &self.frozen_descendants
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

    /// Marks a descendant as completed. Idempotent for already-completed PRs.
    pub fn mark_completed(&mut self, pr: PrNumber) -> Result<(), ProgressError> {
        if !self.frozen_descendants.contains(&pr) {
            return Err(ProgressError::NotInFrozenSet { pr });
        }
        if self.skipped.contains(&pr) {
            return Err(ProgressError::AlreadySkipped { pr });
        }
        self.completed.insert(pr);
        Ok(())
    }

    /// Marks a descendant as skipped. Idempotent for already-skipped PRs.
    pub fn mark_skipped(&mut self, pr: PrNumber) -> Result<(), ProgressError> {
        if !self.frozen_descendants.contains(&pr) {
            return Err(ProgressError::NotInFrozenSet { pr });
        }
        if self.completed.contains(&pr) {
            return Err(ProgressError::AlreadyCompleted { pr });
        }
        self.skipped.insert(pr);
        Ok(())
    }

    /// Progress for entering the next descendant-processing phase: the frozen
    /// set and skips carry over (a skipped descendant is never revisited);
    /// the per-phase completion ledger is cleared.
    pub fn with_completed_reset(&self) -> Self {
        DescendantProgress {
            completed: HashSet::new(),
            skipped: self.skipped.clone(),
            frozen_descendants: self.frozen_descendants.clone(),
        }
    }
}

/// The cascade phase indicates which operation is currently in progress.
///
/// # Descendant Set Invariant
///
/// `frozen_descendants` is captured when transitioning out of `Idle`:
/// - `Idle -> Preparing`: descendants exist; captured at `Preparing` entry
/// - `Idle -> SquashPending`: no descendants; uses empty `frozen_descendants`
///
/// Once captured, `frozen_descendants` is carried through all subsequent phases
/// and NEVER re-queried from the descendants index. This prevents late-arriving
/// descendants (added during spool replay after a crash) from corrupting the
/// cascade.
///
/// Serializes with external tagging per the design: `{"Preparing": {...}}` for
/// phases with data, `"Idle"` for unit variants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CascadePhase {
    /// Not currently performing any operation; waiting for CI or next event.
    Idle,

    /// Merging predecessor head into descendants (before squash).
    /// NOT main - only predecessor head.
    Preparing {
        /// Descendant tracking carried through all subsequent phases.
        #[serde(flatten)]
        progress: DescendantProgress,
    },

    /// Preparation complete; about to squash-merge current PR.
    SquashPending {
        /// Descendant tracking. When entered via `Preparing`, carries forward
        /// from that phase. When entered directly from `Idle` (no descendants),
        /// contains empty `frozen_descendants`.
        #[serde(flatten)]
        progress: DescendantProgress,
    },

    /// Squash complete; performing ours-strategy merges into descendants.
    Reconciling {
        /// Carried forward from SquashPending.
        #[serde(flatten)]
        progress: DescendantProgress,

        /// The SHA of the squash commit on main.
        squash_sha: Sha,
    },

    /// Ours-merge complete; performing regular merge of origin/main.
    CatchingUp {
        /// Carried forward from Reconciling.
        #[serde(flatten)]
        progress: DescendantProgress,

        /// The SHA of the squash commit on main.
        squash_sha: Sha,
    },

    /// Catch-up complete; retargeting descendant PRs to default branch.
    Retargeting {
        /// Carried forward from CatchingUp.
        #[serde(flatten)]
        progress: DescendantProgress,

        /// The SHA of the squash commit on main.
        squash_sha: Sha,
    },
}

/// The cascade phases, without their data. The constant [`PhaseKind::ORDER`]
/// is the single definition of the cascade sequence; `successor` and
/// `CascadePhase::can_transition_to` are derived from it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PhaseKind {
    Idle,
    Preparing,
    SquashPending,
    Reconciling,
    CatchingUp,
    Retargeting,
}

impl PhaseKind {
    /// The active phases in execution order. `Idle` is both entry and exit of
    /// the cycle and is therefore not listed.
    pub const ORDER: [PhaseKind; 5] = [
        PhaseKind::Preparing,
        PhaseKind::SquashPending,
        PhaseKind::Reconciling,
        PhaseKind::CatchingUp,
        PhaseKind::Retargeting,
    ];

    /// Position in the cascade: `Idle` (entry) is 0, active phases are 1..=5.
    fn position(self) -> usize {
        match self {
            PhaseKind::Idle => 0,
            active => {
                1 + Self::ORDER
                    .iter()
                    .position(|k| *k == active)
                    .expect("every non-Idle kind is in ORDER")
            }
        }
    }

    /// The phase that follows once this phase's work is done.
    /// `None` for `Idle`: leaving `Idle` is `start_preparing`'s job, not a
    /// completion event.
    pub fn successor(self) -> Option<PhaseKind> {
        match self {
            PhaseKind::Idle => None,
            active => Some(
                Self::ORDER
                    .get(active.position())
                    .copied()
                    .unwrap_or(PhaseKind::Idle),
            ),
        }
    }

    /// Phases whose work is per-descendant bookkeeping (mark completed or
    /// skipped until none remain). `SquashPending`'s exit is the squash
    /// itself, not descendant progress; `Idle` has no work.
    pub fn processes_descendants(self) -> bool {
        matches!(
            self,
            PhaseKind::Preparing
                | PhaseKind::Reconciling
                | PhaseKind::CatchingUp
                | PhaseKind::Retargeting
        )
    }

    /// Returns the name of this phase for logging/display.
    pub fn name(self) -> &'static str {
        match self {
            PhaseKind::Idle => "idle",
            PhaseKind::Preparing => "preparing",
            PhaseKind::SquashPending => "squash_pending",
            PhaseKind::Reconciling => "reconciling",
            PhaseKind::CatchingUp => "catching_up",
            PhaseKind::Retargeting => "retargeting",
        }
    }
}

impl CascadePhase {
    /// This phase's kind (the phase without its data).
    pub fn kind(&self) -> PhaseKind {
        match self {
            CascadePhase::Idle => PhaseKind::Idle,
            CascadePhase::Preparing { .. } => PhaseKind::Preparing,
            CascadePhase::SquashPending { .. } => PhaseKind::SquashPending,
            CascadePhase::Reconciling { .. } => PhaseKind::Reconciling,
            CascadePhase::CatchingUp { .. } => PhaseKind::CatchingUp,
            CascadePhase::Retargeting { .. } => PhaseKind::Retargeting,
        }
    }

    /// Returns the name of this phase for logging/display.
    pub fn name(&self) -> &'static str {
        self.kind().name()
    }

    /// Returns the descendant progress if this phase has it.
    pub fn progress(&self) -> Option<&DescendantProgress> {
        match self {
            CascadePhase::Idle => None,
            CascadePhase::Preparing { progress }
            | CascadePhase::SquashPending { progress }
            | CascadePhase::Reconciling { progress, .. }
            | CascadePhase::CatchingUp { progress, .. }
            | CascadePhase::Retargeting { progress, .. } => Some(progress),
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

    /// Checks if a transition from this phase to the target phase is structurally valid.
    ///
    /// This only validates the phase sequence, not contextual constraints like
    /// whether descendants exist. The cascade engine enforces those constraints
    /// when performing the actual transition.
    ///
    /// Derived from [`PhaseKind::ORDER`]: transitions move strictly forward,
    /// and phases with no remaining work are passed through rather than parked
    /// in, so a single observable transition may jump several phases ahead.
    /// The one gate that can never be jumped is `SquashPending`: every path
    /// out of `Idle`/`Preparing` stops there until the squash itself happens.
    pub fn can_transition_to(&self, target: &CascadePhase) -> bool {
        let from = self.kind().position();
        // Idle is the exit as well as the entry: as a target it sits one past
        // the last active phase.
        let to = match target.kind() {
            PhaseKind::Idle => PhaseKind::ORDER.len() + 1,
            kind => kind.position(),
        };
        let squash_gate = PhaseKind::SquashPending.position();
        to > from && (from >= squash_gate || to <= squash_gate)
    }
}

/// The category of error that aborted a train.
///
/// One variant per [`super::stack::AbortReason`] variant;
/// `AbortReason::error_type()` is the mapping. Serialized snake_case in
/// status comments (e.g. `"merge_conflict"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrainErrorKind {
    MergeConflict,
    PushRejected,
    PrClosed,
    CiFailed,
    CycleDetected,
    /// A PR's predecessor relationship was changed or removed while a train
    /// involving it was active, altering the stack out from under the cascade.
    PredecessorChanged,
    ReviewDismissed,
    ApprovalWithdrawn,
    ApiError,
    BranchDeleted,
    NonSquashMerge,
    StatusCommentTooLarge,
    TrainTooLarge,
    PreparationMissing,
    MergeHooksEnabled,
    PreparationIncomplete,
    BaseBranchMismatch,
    HeadShaChanged,
    InternalInvariantViolation,
}

/// Error details when a train is aborted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrainError {
    /// The category of error. Serialized as `error_type` (the status-comment
    /// schema name).
    #[serde(rename = "error_type")]
    pub kind: TrainErrorKind,

    /// Human-readable error message. Truncated to 4KB in status comments.
    pub message: String,

    /// stderr output if applicable. Truncated to 2KB in status comments.
    pub stderr: Option<String>,
}

impl TrainError {
    pub fn new(kind: TrainErrorKind, message: impl Into<String>) -> Self {
        TrainError {
            kind,
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

    /// Parent of the squash commit (`$SQUASH_SHA^`), captured at the moment the
    /// squash is observed (M1 seam e). Threaded into `GitEffect::MergeReconcile`
    /// as `expected_squash_parent` so `reconcile_descendant` can refuse without
    /// an ancestry proof. `None` until a squash has been committed this train;
    /// `#[serde(default)]` so snapshots/status comments written before this
    /// field existed still deserialize.
    #[serde(default)]
    pub last_squash_parent_sha: Option<Sha>,

    /// ISO 8601 timestamp when started.
    pub started_at: DateTime<Utc>,

    /// The ID of the status comment on the original root PR.
    /// Used to update the comment as the train progresses.
    ///
    /// Note: This is local-only data and is excluded from the status comment
    /// JSON via format_train_json() (which sets it to None) to avoid including
    /// redundant/confusing info. The skip_serializing_if ensures it doesn't
    /// appear as "null" in the JSON when set to None.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub status_comment_id: Option<CommentId>,
}

impl TrainRecord {
    /// Creates a new train record for a starting train.
    ///
    /// The timestamp is passed in (not read from the clock): the types layer
    /// is pure, and the shell decides what time it is.
    pub fn new(root_pr: PrNumber, started_at: DateTime<Utc>) -> Self {
        TrainRecord {
            version: 1,
            recovery_seq: 0,
            state: TrainState::Running,
            original_root_pr: root_pr,
            current_pr: root_pr,
            cascade_phase: CascadePhase::Idle,
            predecessor_pr: None,
            predecessor_head_sha: None,
            last_squash_parent_sha: None,
            started_at,
            status_comment_id: None,
        }
    }

    /// Increments the recovery sequence number.
    /// Must be called on each state change.
    pub fn increment_seq(&mut self) {
        self.recovery_seq += 1;
    }

    /// Marks the train as stopped.
    pub fn stop(&mut self, ended_at: DateTime<Utc>) {
        self.state = TrainState::Stopped { ended_at };
        self.increment_seq();
    }

    /// Marks the train as aborted with an error.
    pub fn abort(&mut self, error: TrainError, ended_at: DateTime<Utc>) {
        self.state = TrainState::Aborted { ended_at, error };
        self.increment_seq();
    }

    /// Marks the train as waiting for CI.
    pub fn wait_for_ci(&mut self) {
        self.state = TrainState::WaitingCi;
        self.increment_seq();
    }

    /// Resumes the train after waiting for CI.
    ///
    /// # Panics
    ///
    /// Debug-asserts that the train is in `WaitingCi` state. Restarting from
    /// `Stopped` or `Aborted` requires a new `TrainRecord` via `@merge-train start`,
    /// not calling `resume()` on the existing record.
    pub fn resume(&mut self) {
        debug_assert!(
            self.state == TrainState::WaitingCi,
            "resume() is only valid from WaitingCi state, not {:?}",
            self.state
        );
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
        "[0-9a-f]{40}".prop_map(|s| Sha::parse(s).unwrap())
    }

    fn arb_pr_number() -> impl Strategy<Value = PrNumber> {
        any::<u64>().prop_map(PrNumber)
    }

    use crate::test_utils::{arb_train_state, test_timestamp};

    fn arb_descendant_progress() -> impl Strategy<Value = DescendantProgress> {
        prop::collection::vec(arb_pr_number(), 0..10).prop_flat_map(|frozen| {
            // Per descendant: 0 = pending, 1 = completed, 2 = skipped.
            prop::collection::vec(0u8..3, frozen.len()..=frozen.len()).prop_map(move |marks| {
                let mut progress = DescendantProgress::new(frozen.clone());
                let in_progress_order = progress.frozen_descendants().to_vec();
                for (pr, mark) in in_progress_order.into_iter().zip(marks) {
                    match mark {
                        1 => progress.mark_completed(pr).unwrap(),
                        2 => progress.mark_skipped(pr).unwrap(),
                        _ => {}
                    }
                }
                progress
            })
        })
    }

    fn arb_cascade_phase() -> impl Strategy<Value = CascadePhase> {
        prop_oneof![
            Just(CascadePhase::Idle),
            arb_descendant_progress().prop_map(|progress| CascadePhase::Preparing { progress }),
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
        fn activity_and_restart_are_complementary() {
            let stopped = TrainState::Stopped {
                ended_at: test_timestamp(),
            };
            let aborted = TrainState::Aborted {
                ended_at: test_timestamp(),
                error: TrainError::new(TrainErrorKind::ApiError, "boom"),
            };

            assert!(TrainState::Running.is_active());
            assert!(TrainState::WaitingCi.is_active());
            assert!(!stopped.is_active());
            assert!(!aborted.is_active());
            assert!(!TrainState::NeedsManualReview.is_active());

            assert!(!TrainState::Running.requires_restart());
            assert!(!TrainState::WaitingCi.requires_restart());
            assert!(stopped.requires_restart());
            assert!(aborted.requires_restart());
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
                    prop_assert!(!progress.completed().contains(pr));
                    prop_assert!(!progress.skipped().contains(pr));
                }
            }

            #[test]
            fn new_dedupes_frozen_descendants(
                // Tiny domain so duplicates actually occur.
                frozen in prop::collection::vec((1u64..5).prop_map(PrNumber), 0..10)
            ) {
                let progress = DescendantProgress::new(frozen.clone());
                let unique: std::collections::HashSet<_> = frozen.iter().copied().collect();
                prop_assert_eq!(progress.frozen_descendants().len(), unique.len());
                // First-occurrence order is preserved
                let mut seen = std::collections::HashSet::new();
                let expected: Vec<_> = frozen.iter().copied().filter(|pr| seen.insert(*pr)).collect();
                prop_assert_eq!(progress.frozen_descendants().to_vec(), expected);
            }

            #[test]
            fn is_complete_when_all_processed(frozen in prop::collection::vec(arb_pr_number(), 0..10)) {
                let mut progress = DescendantProgress::new(frozen.clone());

                // Mark all as completed or skipped
                for (i, pr) in frozen.iter().enumerate() {
                    if i % 2 == 0 {
                        progress.mark_completed(*pr).unwrap();
                    } else {
                        progress.mark_skipped(*pr).unwrap();
                    }
                }

                prop_assert!(progress.is_complete());
            }
        }

        #[test]
        fn deserialize_rejects_completed_outside_frozen() {
            let json = r#"{"completed":[5],"skipped":[],"frozen_descendants":[1,2]}"#;
            let result: Result<DescendantProgress, _> = serde_json::from_str(json);
            assert!(
                result.is_err(),
                "completed must be a subset of frozen_descendants; status-comment \
                 JSON violating that is corrupt and recovery must not trust it"
            );
        }

        #[test]
        fn deserialize_rejects_completed_skipped_overlap() {
            let json = r#"{"completed":[1],"skipped":[1],"frozen_descendants":[1,2]}"#;
            let result: Result<DescendantProgress, _> = serde_json::from_str(json);
            assert!(result.is_err());
        }

        #[test]
        fn deserialize_rejects_duplicate_frozen() {
            let json = r#"{"completed":[],"skipped":[],"frozen_descendants":[1,1,2]}"#;
            let result: Result<DescendantProgress, _> = serde_json::from_str(json);
            assert!(result.is_err());
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

                let preparing = CascadePhase::Preparing { progress: progress.clone() };
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
                    prop_assert_eq!(phase_progress.frozen_descendants(), &frozen[..]);
                }
            }
        }

        #[test]
        fn valid_transitions() {
            let progress = DescendantProgress::new(vec![PrNumber(1)]);
            let sha = Sha::parse("abc123def456789012345678901234567890abcd").unwrap();

            let idle = CascadePhase::Idle;
            let preparing = CascadePhase::Preparing {
                progress: progress.clone(),
            };
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

            // Forward jumps: phases with no remaining work are settled
            // through, so observable transitions can skip them entirely
            // (e.g. all descendants skipped during reconciliation).
            assert!(squash_pending.can_transition_to(&catching_up));
            assert!(squash_pending.can_transition_to(&retargeting));
            assert!(reconciling.can_transition_to(&retargeting));
            assert!(reconciling.can_transition_to(&idle));
            assert!(catching_up.can_transition_to(&idle));
        }

        #[test]
        fn invalid_transitions() {
            let progress = DescendantProgress::new(vec![PrNumber(1)]);
            let sha = Sha::parse("abc123def456789012345678901234567890abcd").unwrap();

            let idle = CascadePhase::Idle;
            let preparing = CascadePhase::Preparing {
                progress: progress.clone(),
            };
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
        use crate::test_utils::arb_train_record;

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
                    let mut record = TrainRecord::new(PrNumber(1), test_timestamp());
                    record.recovery_seq = initial_seq;
                    record.increment_seq();
                    prop_assert_eq!(record.recovery_seq, initial_seq + 1);
                }
            }
        }

        #[test]
        fn new_creates_valid_record() {
            let record = TrainRecord::new(PrNumber(123), test_timestamp());

            assert_eq!(record.version, 1);
            assert_eq!(record.recovery_seq, 0);
            assert_eq!(record.state, TrainState::Running);
            assert_eq!(record.original_root_pr, PrNumber(123));
            assert_eq!(record.current_pr, PrNumber(123));
            assert_eq!(record.cascade_phase, CascadePhase::Idle);
            assert!(record.predecessor_pr.is_none());
            assert!(record.predecessor_head_sha.is_none());
            assert!(record.status_comment_id.is_none());
        }

        #[test]
        fn stop_sets_state_and_timestamp() {
            let mut record = TrainRecord::new(PrNumber(123), test_timestamp());
            let initial_seq = record.recovery_seq;

            record.stop(test_timestamp());

            assert_eq!(
                record.state,
                TrainState::Stopped {
                    ended_at: test_timestamp()
                }
            );
            assert_eq!(record.recovery_seq, initial_seq + 1);
        }

        #[test]
        fn abort_sets_state_and_error() {
            let mut record = TrainRecord::new(PrNumber(123), test_timestamp());
            let initial_seq = record.recovery_seq;

            record.abort(
                TrainError::new(TrainErrorKind::MergeConflict, "Cannot merge"),
                test_timestamp(),
            );

            match &record.state {
                TrainState::Aborted { ended_at, error } => {
                    assert_eq!(*ended_at, test_timestamp());
                    assert_eq!(error.kind, TrainErrorKind::MergeConflict);
                }
                other => panic!("expected Aborted, got {:?}", other),
            }
            assert_eq!(record.recovery_seq, initial_seq + 1);
        }
    }
}
