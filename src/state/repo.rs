//! The in-memory materialization of a repository's state (`RepoState`).
//!
//! `RepoState` is the live, in-memory view DESIGN.md calls *RepoState*: the
//! cached PRs, the active trains, the derived descendants index, and the
//! dedupe-key TTL set. It is reconstructed from a [`PersistedRepoSnapshot`]
//! plus a replay of the event log.
//!
//! # The single mutation rule
//!
//! Every state change flows through exactly one entry point:
//! [`RepoState::apply_event`]. The persistence contract is "append the event,
//! then apply it" — nothing mutates `RepoState` any other way. This is what
//! makes recovery tractable: replaying the log through `apply_event` from any
//! snapshot reconstructs the same state (the *compaction-equivalence*
//! property), so a crash at any point is recovered by replay.
//!
//! `apply_event` is **total**: events referencing PRs or trains that aren't in
//! the materialized state are tolerated (skipped), because the log may contain
//! events about PRs pruned from a later snapshot.
//!
//! # Scope (Stage M1)
//!
//! `apply_event` materializes the fields the current event vocabulary carries.
//! Some `TrainRecord` fields the planner (M2) manages — `recovery_seq`,
//! `predecessor_head_sha`, `status_comment_id`, the `Running`/`WaitingCi`
//! distinction — are not yet derivable from events alone; pure-replay
//! materialization is best-effort for those and is completed as later stages
//! widen the vocabulary. The compaction-equivalence property is unaffected: it
//! compares replay against replay, so whatever `apply_event` does
//! deterministically holds on both sides.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};

use crate::persistence::event::{StateEvent, StateEventPayload};
use crate::persistence::snapshot::{PersistedRepoSnapshot, SCHEMA_VERSION};
use crate::types::{CachedPr, MergeStateStatus, PrNumber, PrState, TrainRecord, TrainState};

use super::descendants::build_descendants_index;

/// The materialized state of a single repository.
///
/// `descendants` is a derived index (predecessor → descendants), kept coherent
/// with `prs` by construction: it is rebuilt from `prs` on load and after any
/// event that mutates a PR. The invariant
/// `descendants == build_descendants_index(&prs)` holds after every
/// `apply_event`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoState {
    /// The repository's default branch (e.g. "main").
    pub default_branch: String,

    /// Cached PR state, keyed by PR number.
    pub prs: HashMap<PrNumber, CachedPr>,

    /// Active trains, keyed by their original root PR.
    pub active_trains: HashMap<PrNumber, TrainRecord>,

    /// Reverse predecessor index (predecessor → descendants). Derived from
    /// `prs`; never mutated independently.
    pub descendants: HashMap<PrNumber, HashSet<PrNumber>>,

    /// Seen dedupe keys with the timestamp they were first seen (TTL pruning).
    pub seen_dedupe_keys: HashMap<String, DateTime<Utc>>,
}

impl RepoState {
    /// Materializes a `RepoState` from a persisted snapshot, rebuilding the
    /// derived descendants index from the snapshot's PRs.
    pub fn from_snapshot(snapshot: PersistedRepoSnapshot) -> Self {
        let descendants = build_descendants_index(&snapshot.prs);
        RepoState {
            default_branch: snapshot.default_branch,
            prs: snapshot.prs,
            active_trains: snapshot.active_trains,
            descendants,
            seen_dedupe_keys: snapshot.seen_dedupe_keys,
        }
    }

    /// Captures the current state as a persisted snapshot. The persistence
    /// metadata (`log_generation`/`log_position`/`next_seq`/`snapshot_at`) is
    /// supplied by the caller — `RepoState` does not track it. The derived
    /// `descendants` index is not stored (it is rebuilt by `from_snapshot`).
    pub fn to_snapshot(
        &self,
        log_generation: u64,
        log_position: u64,
        next_seq: u64,
        snapshot_at: DateTime<Utc>,
    ) -> PersistedRepoSnapshot {
        PersistedRepoSnapshot {
            schema_version: SCHEMA_VERSION,
            snapshot_at,
            log_generation,
            log_position,
            next_seq,
            default_branch: self.default_branch.clone(),
            prs: self.prs.clone(),
            active_trains: self.active_trains.clone(),
            seen_dedupe_keys: self.seen_dedupe_keys.clone(),
        }
    }

    /// THE single mutation entry point. Applies one event to the state.
    ///
    /// Total: events about unknown PRs/trains are skipped. Timestamps come from
    /// `event.ts` (the shell stamped it on append), never from the clock, so
    /// replay is deterministic.
    pub fn apply_event(&mut self, event: &StateEvent) {
        // Whether this event mutated `prs` and so may have changed the
        // predecessor relationships the descendants index is derived from.
        let mut prs_mutated = false;

        match &event.payload {
            // ─── Train lifecycle ───
            StateEventPayload::TrainStarted {
                root_pr,
                current_pr,
            } => {
                let mut train = TrainRecord::new(*root_pr, event.ts);
                train.current_pr = *current_pr;
                self.active_trains.insert(*root_pr, train);
            }

            StateEventPayload::TrainStopped { root_pr } => {
                if let Some(train) = self.active_trains.get_mut(root_pr) {
                    train.state = TrainState::Stopped { ended_at: event.ts };
                }
            }

            StateEventPayload::TrainAborted { root_pr, error } => {
                if let Some(train) = self.active_trains.get_mut(root_pr) {
                    train.state = TrainState::Aborted {
                        ended_at: event.ts,
                        error: error.clone(),
                    };
                }
            }

            // A completed train is fully done — it leaves the active set.
            // (Stopped/Aborted are retained, their state reflecting the
            // terminal condition for status display and `@merge-train start`.)
            StateEventPayload::TrainCompleted { root_pr } => {
                self.active_trains.remove(root_pr);
            }

            // ─── Phase transition ───
            StateEventPayload::PhaseTransition {
                train_root,
                current_pr,
                predecessor_pr,
                last_squash_sha: _,
                last_squash_parent,
                phase,
            } => {
                if let Some(train) = self.active_trains.get_mut(train_root) {
                    train.current_pr = *current_pr;
                    train.predecessor_pr = *predecessor_pr;
                    train.last_squash_parent_sha = last_squash_parent.clone();
                    train.cascade_phase = phase.clone();
                }
            }

            // ─── is-root-by-construction (seam c) ───
            StateEventPayload::SquashCommitted { pr, sha, .. } => {
                if let Some(p) = self.prs.get_mut(pr) {
                    p.state = PrState::Merged {
                        merge_commit_sha: sha.clone(),
                    };
                    prs_mutated = true;
                }
            }

            StateEventPayload::ReconciliationRecorded { pr, squash_sha } => {
                if let Some(p) = self.prs.get_mut(pr) {
                    p.predecessor_squash_reconciled = Some(squash_sha.clone());
                    prs_mutated = true;
                }
            }

            StateEventPayload::DoneRetarget { pr, new_base, .. } => {
                // A legacy `done_retarget` written before `new_base` existed
                // deserializes (via `#[serde(default)]`) to an empty string;
                // that means "base unrecorded", not "retarget to no branch", so
                // leave the cached base untouched rather than blanking it
                // (Codex review #49 [P2]).
                if !new_base.is_empty()
                    && let Some(p) = self.prs.get_mut(pr)
                {
                    p.base_ref = new_base.clone();
                    prs_mutated = true;
                }
            }

            // ─── Observational PR updates ───
            StateEventPayload::PrMerged { pr, merge_sha } => {
                if let Some(p) = self.prs.get_mut(pr) {
                    p.state = PrState::Merged {
                        merge_commit_sha: merge_sha.clone(),
                    };
                    prs_mutated = true;
                }
            }

            StateEventPayload::PrStateChanged { pr, state } => {
                if let Some(p) = self.prs.get_mut(pr) {
                    // `Merged` requires a commit SHA the string form can't
                    // carry; merges arrive via `PrMerged`/`SquashCommitted`.
                    match state.as_str() {
                        "open" => {
                            p.state = PrState::Open;
                            prs_mutated = true;
                        }
                        "closed" => {
                            p.state = PrState::Closed;
                            prs_mutated = true;
                        }
                        _ => {}
                    }
                }
            }

            StateEventPayload::PredecessorDeclared {
                pr,
                predecessor,
                comment_id,
            } => {
                if let Some(p) = self.prs.get_mut(pr) {
                    p.predecessor = Some(*predecessor);
                    p.predecessor_comment_id = Some(*comment_id);
                    prs_mutated = true;
                }
            }

            // The authoritative predecessor comment was deleted/edited away;
            // clear the link so the descendants index no longer reflects it.
            StateEventPayload::PredecessorRemoved { pr, comment_id: _ } => {
                if let Some(p) = self.prs.get_mut(pr) {
                    p.predecessor = None;
                    p.predecessor_comment_id = None;
                    prs_mutated = true;
                }
            }

            // ─── PR lifecycle: keep the PR cache current ───
            StateEventPayload::PrOpened {
                pr,
                head_sha,
                head_ref,
                base_ref,
                is_draft,
            } => {
                self.prs.insert(
                    *pr,
                    CachedPr::new(
                        *pr,
                        head_sha.clone(),
                        head_ref.clone(),
                        base_ref.clone(),
                        None,
                        PrState::Open,
                        MergeStateStatus::Unknown,
                        *is_draft,
                    ),
                );
                prs_mutated = true;
            }

            StateEventPayload::PrClosed { pr } => {
                if let Some(p) = self.prs.get_mut(pr) {
                    p.state = PrState::Closed;
                    p.closed_at = Some(event.ts);
                    prs_mutated = true;
                }
            }

            StateEventPayload::PrReopened { pr } => {
                if let Some(p) = self.prs.get_mut(pr) {
                    p.state = PrState::Open;
                    p.closed_at = None;
                    prs_mutated = true;
                }
            }

            StateEventPayload::PrBaseChanged { pr, new_base, .. } => {
                if let Some(p) = self.prs.get_mut(pr) {
                    p.base_ref = new_base.clone();
                    prs_mutated = true;
                }
            }

            StateEventPayload::PrSynchronized { pr, new_head_sha } => {
                if let Some(p) = self.prs.get_mut(pr) {
                    p.head_sha = new_head_sha.clone();
                    // A new head invalidates everything derived from the old
                    // one: cached mergeability is stale (CI hasn't run on the
                    // new head) and any predecessor-squash reconciliation done
                    // against the old head no longer holds. Clear both so
                    // is_root / mergeability decisions force a refetch and
                    // re-reconciliation — the force-push race guard (Codex
                    // review #49 [P1]).
                    p.merge_state_status = MergeStateStatus::Unknown;
                    p.predecessor_squash_reconciled = None;
                    prs_mutated = true;
                }
            }

            StateEventPayload::PrConvertedToDraft { pr } => {
                if let Some(p) = self.prs.get_mut(pr) {
                    p.is_draft = true;
                    // A draft is never mergeable; keep the cached status
                    // consistent so `is_mergeable()` consumers don't see it as
                    // ready (Codex review #49 [P2]).
                    p.merge_state_status = MergeStateStatus::Draft;
                    prs_mutated = true;
                }
            }

            StateEventPayload::PrReadyForReview { pr } => {
                if let Some(p) = self.prs.get_mut(pr) {
                    p.is_draft = false;
                    // The real mergeability is unknown until a refetch; don't
                    // leave the stale `Draft` status looking authoritative.
                    p.merge_state_status = MergeStateStatus::Unknown;
                    prs_mutated = true;
                }
            }

            // ─── Fan-out: old train retires, descendants become new roots ───
            StateEventPayload::FanOutCompleted {
                old_root,
                new_roots,
                ..
            } => {
                self.active_trains.remove(old_root);
                for root in new_roots {
                    self.active_trains
                        .insert(*root, TrainRecord::new(*root, event.ts));
                }
            }

            // ─── Intent/done bracketing: durability only, no materialized
            // state change (the resulting content lands via the following
            // PhaseTransition / SquashCommitted). ───
            StateEventPayload::IntentPushPrep { .. }
            | StateEventPayload::DonePushPrep { .. }
            | StateEventPayload::IntentSquash { .. }
            | StateEventPayload::IntentPushReconcile { .. }
            | StateEventPayload::DonePushReconcile { .. }
            | StateEventPayload::IntentPushCatchup { .. }
            | StateEventPayload::DonePushCatchup { .. }
            | StateEventPayload::IntentRetarget { .. } => {}

            // ─── Engine observations: CI / review / descendant-skip events
            // drive the planner's decisions (M2+) and the train's progress
            // (carried wholesale on `PhaseTransition`), not the materialized PR
            // cache. M1 replay-materialization is best-effort for these. ───
            StateEventPayload::DescendantSkipped { .. }
            | StateEventPayload::CheckSuiteCompleted { .. }
            | StateEventPayload::StatusReceived { .. }
            | StateEventPayload::ReviewSubmitted { .. }
            | StateEventPayload::ReviewDismissed { .. } => {}
        }

        if prs_mutated {
            self.descendants = build_descendants_index(&self.prs);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::event::StateEvent;
    use crate::test_utils::{arb_datetime, arb_sha};
    use crate::types::{CommentId, MergeStateStatus, Sha, TrainError};
    use proptest::prelude::*;

    // ─── Generators ───
    //
    // The crux of a meaningful oracle here: events must actually *reference the
    // PRs and trains that exist in the state*, or `apply_event` just keeps
    // hitting its skip-unknown path and the properties pass vacuously. So every
    // PR number — in the state and in the events — is drawn from one small
    // shared universe (`SMALL_PR`), and the few numbers above the state's range
    // still exercise the totality (skip) path.

    const PR_UNIVERSE: u64 = 10;

    fn arb_small_pr() -> impl Strategy<Value = PrNumber> {
        (1u64..=PR_UNIVERSE).prop_map(PrNumber)
    }

    /// A short branch from a tiny set, so `DoneRetarget`/`base_ref` collisions
    /// with the default branch ("main") happen often enough to matter.
    fn arb_small_branch() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("main".to_string()),
            Just("a".to_string()),
            Just("b".to_string()),
            Just("c".to_string()),
        ]
    }

    fn arb_pr_state() -> impl Strategy<Value = PrState> {
        prop_oneof![
            Just(PrState::Open),
            Just(PrState::Closed),
            arb_sha().prop_map(|sha| PrState::Merged {
                merge_commit_sha: sha
            }),
        ]
    }

    /// A `CachedPr` whose fields exercise the materialization paths
    /// (predecessor links within the universe, merged state, reconciliation
    /// marker, base branch).
    fn arb_cached_pr(number: PrNumber) -> impl Strategy<Value = CachedPr> {
        (
            arb_sha(),
            arb_small_branch(),
            arb_small_branch(),
            prop::option::of(arb_small_pr()),
            arb_pr_state(),
            prop::option::of(arb_sha()),
        )
            .prop_map(
                move |(head_sha, head_ref, base_ref, predecessor, state, reconciled)| {
                    let mut pr = CachedPr::new(
                        number,
                        head_sha,
                        head_ref,
                        base_ref,
                        predecessor,
                        state,
                        MergeStateStatus::Unknown,
                        false,
                    );
                    pr.predecessor_squash_reconciled = reconciled;
                    pr
                },
            )
    }

    /// An arbitrary materialized state over the small PR universe. Built through
    /// `from_snapshot` so the descendants index always starts coherent.
    fn arb_repo_state() -> impl Strategy<Value = RepoState> {
        (
            prop::collection::hash_set(1u64..=PR_UNIVERSE, 0..=PR_UNIVERSE as usize),
            arb_datetime(),
        )
            .prop_flat_map(|(pr_numbers, snapshot_at)| {
                let pr_strats: Vec<_> = pr_numbers
                    .into_iter()
                    .map(|n| arb_cached_pr(PrNumber(n)))
                    .collect();
                (pr_strats, Just(snapshot_at)).prop_map(|(prs_vec, snapshot_at)| {
                    let prs: HashMap<PrNumber, CachedPr> =
                        prs_vec.into_iter().map(|p| (p.number, p)).collect();
                    let snapshot = PersistedRepoSnapshot {
                        schema_version: SCHEMA_VERSION,
                        snapshot_at,
                        log_generation: 0,
                        log_position: 0,
                        next_seq: 0,
                        default_branch: "main".to_string(),
                        prs,
                        active_trains: HashMap::new(),
                        seen_dedupe_keys: HashMap::new(),
                    };
                    RepoState::from_snapshot(snapshot)
                })
            })
    }

    /// An event payload over the small PR universe. Covers every variant that
    /// mutates materialized state, so the properties actually exercise the
    /// mutation paths rather than the skip-unknown path.
    fn arb_pool_event_payload() -> impl Strategy<Value = StateEventPayload> {
        prop_oneof![
            (arb_small_pr(), arb_small_pr()).prop_map(|(r, c)| {
                StateEventPayload::TrainStarted {
                    root_pr: r,
                    current_pr: c,
                }
            }),
            arb_small_pr().prop_map(|r| StateEventPayload::TrainStopped { root_pr: r }),
            arb_small_pr().prop_map(|r| StateEventPayload::TrainCompleted { root_pr: r }),
            (arb_small_pr(), arb_sha()).prop_map(|(r, sha)| StateEventPayload::TrainAborted {
                root_pr: r,
                error: TrainError::new(crate::types::TrainErrorKind::ApiError, sha.as_str()),
            }),
            (
                arb_small_pr(),
                arb_small_pr(),
                prop::option::of(arb_small_pr()),
                prop::option::of(arb_sha()),
                arb_cascade_phase_small(),
            )
                .prop_map(|(tr, cp, pp, lp, ph)| StateEventPayload::PhaseTransition {
                    train_root: tr,
                    current_pr: cp,
                    predecessor_pr: pp,
                    last_squash_sha: None,
                    last_squash_parent: lp,
                    phase: ph,
                }),
            (arb_small_pr(), arb_small_pr(), arb_sha()).prop_map(|(tr, pr, sha)| {
                StateEventPayload::SquashCommitted {
                    train_root: tr,
                    pr,
                    sha,
                }
            }),
            (arb_small_pr(), arb_sha()).prop_map(|(pr, sha)| {
                StateEventPayload::ReconciliationRecorded {
                    pr,
                    squash_sha: sha,
                }
            }),
            (arb_small_pr(), arb_small_pr(), arb_small_branch()).prop_map(|(tr, pr, nb)| {
                StateEventPayload::DoneRetarget {
                    train_root: tr,
                    pr,
                    new_base: nb,
                }
            }),
            (arb_small_pr(), arb_sha())
                .prop_map(|(pr, sha)| StateEventPayload::PrMerged { pr, merge_sha: sha }),
            (arb_small_pr(), arb_pr_state_string())
                .prop_map(|(pr, state)| StateEventPayload::PrStateChanged { pr, state }),
            (arb_small_pr(), arb_small_pr()).prop_map(|(pr, pred)| {
                StateEventPayload::PredecessorDeclared {
                    pr,
                    predecessor: pred,
                    comment_id: CommentId(1),
                }
            }),
            (
                arb_small_pr(),
                prop::collection::vec(arb_small_pr(), 1..4),
                arb_small_pr(),
            )
                .prop_map(|(old, new, orig)| StateEventPayload::FanOutCompleted {
                    old_root: old,
                    new_roots: new,
                    original_root_pr: orig,
                }),
            // PR-lifecycle arms — exercise the cache/index materialization.
            arb_small_pr().prop_map(|pr| StateEventPayload::PredecessorRemoved {
                pr,
                comment_id: CommentId(1),
            }),
            (
                arb_small_pr(),
                arb_sha(),
                arb_small_branch(),
                arb_small_branch(),
                any::<bool>(),
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
            arb_small_pr().prop_map(|pr| StateEventPayload::PrClosed { pr }),
            arb_small_pr().prop_map(|pr| StateEventPayload::PrReopened { pr }),
            (arb_small_pr(), arb_small_branch(), arb_small_branch()).prop_map(
                |(pr, old_base, new_base)| StateEventPayload::PrBaseChanged {
                    pr,
                    old_base,
                    new_base,
                },
            ),
            (arb_small_pr(), arb_sha()).prop_map(|(pr, new_head_sha)| {
                StateEventPayload::PrSynchronized { pr, new_head_sha }
            }),
            arb_small_pr().prop_map(|pr| StateEventPayload::PrConvertedToDraft { pr }),
            arb_small_pr().prop_map(|pr| StateEventPayload::PrReadyForReview { pr }),
        ]
    }

    fn arb_pr_state_string() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("open".to_string()),
            Just("closed".to_string()),
            Just("merged".to_string()),
            Just("draft".to_string()),
        ]
    }

    fn arb_descendant_progress_small() -> impl Strategy<Value = crate::types::DescendantProgress> {
        prop::collection::vec(arb_small_pr(), 0..4).prop_map(crate::types::DescendantProgress::new)
    }

    fn arb_cascade_phase_small() -> impl Strategy<Value = crate::types::CascadePhase> {
        use crate::types::CascadePhase;
        prop_oneof![
            Just(CascadePhase::Idle),
            arb_descendant_progress_small().prop_map(|p| CascadePhase::Preparing { progress: p }),
            (arb_descendant_progress_small(), arb_sha()).prop_map(|(p, s)| {
                CascadePhase::Reconciling {
                    progress: p,
                    squash_sha: s,
                }
            }),
        ]
    }

    fn arb_pool_event() -> impl Strategy<Value = StateEvent> {
        (any::<u64>(), arb_datetime(), arb_pool_event_payload())
            .prop_map(|(seq, ts, payload)| StateEvent { seq, ts, payload })
    }

    proptest! {
        /// Index coherence + totality: from any starting state, applying any
        /// sequence of events never panics, and the derived descendants index
        /// equals `build_descendants_index(&prs)` after every single step.
        #[test]
        fn index_coherent_and_total(
            mut state in arb_repo_state(),
            events in prop::collection::vec(arb_pool_event(), 0..30),
        ) {
            for event in &events {
                state.apply_event(event);
                prop_assert_eq!(
                    &state.descendants,
                    &build_descendants_index(&state.prs),
                    "descendants index drifted from prs after {:?}",
                    event.payload,
                );
            }
        }

        /// Compaction equivalence: snapshotting at any cut `k` and replaying the
        /// tail reconstructs exactly the same state as applying every event.
        /// This is the property recovery rests on.
        #[test]
        fn compaction_equivalence(
            initial in arb_repo_state(),
            events in prop::collection::vec(arb_pool_event(), 0..30),
            cut in 0usize..31,
        ) {
            let k = cut.min(events.len());

            // Full replay.
            let mut full = initial.clone();
            for event in &events {
                full.apply_event(event);
            }

            // Replay to the cut, round-trip through a snapshot, replay the tail.
            let mut cut_state = initial.clone();
            for event in &events[..k] {
                cut_state.apply_event(event);
            }
            let snapshot = cut_state.to_snapshot(7, 42, 99, crate::test_utils::test_timestamp());
            let mut resumed = RepoState::from_snapshot(snapshot);
            for event in &events[k..] {
                resumed.apply_event(event);
            }

            prop_assert_eq!(full, resumed);
        }

    }

    /// Deterministic regression guard for the descendants-index rebuild: a
    /// `PredecessorDeclared` that actually lands on an existing PR must update
    /// the derived index. (This is the exact path a vacuous generator hides —
    /// it fails if `apply_event` stops rebuilding the index on that event.)
    #[test]
    fn predecessor_declared_rebuilds_index() {
        let snapshot = PersistedRepoSnapshot {
            schema_version: SCHEMA_VERSION,
            snapshot_at: crate::test_utils::test_timestamp(),
            log_generation: 0,
            log_position: 0,
            next_seq: 0,
            default_branch: "main".to_string(),
            prs: [PrNumber(1), PrNumber(2)]
                .into_iter()
                .map(|n| {
                    (
                        n,
                        CachedPr::new(
                            n,
                            Sha::parse("0".repeat(40)).unwrap(),
                            format!("branch-{n}"),
                            "main".to_string(),
                            None,
                            PrState::Open,
                            MergeStateStatus::Clean,
                            false,
                        ),
                    )
                })
                .collect(),
            active_trains: HashMap::new(),
            seen_dedupe_keys: HashMap::new(),
        };
        let mut state = RepoState::from_snapshot(snapshot);
        assert!(!state.descendants.contains_key(&PrNumber(1)));

        state.apply_event(&StateEvent {
            seq: 0,
            ts: crate::test_utils::test_timestamp(),
            payload: StateEventPayload::PredecessorDeclared {
                pr: PrNumber(2),
                predecessor: PrNumber(1),
                comment_id: CommentId(1),
            },
        });

        assert_eq!(
            state.descendants.get(&PrNumber(1)),
            Some(&HashSet::from([PrNumber(2)])),
            "PredecessorDeclared must rebuild the descendants index"
        );
        assert_eq!(state.descendants, build_descendants_index(&state.prs));
    }

    /// A `PrSynchronized` (force-)push invalidates everything derived from the
    /// old head: the cached mergeability and the predecessor-squash
    /// reconciliation marker. Leaving either would let a new head look CI-clean
    /// and already-reconciled, bypassing the force-push race guard (Codex #49).
    #[test]
    fn synchronize_invalidates_old_head_derived_state() {
        let new_head = Sha::parse("b".repeat(40)).unwrap();
        let mut pr = CachedPr::new(
            PrNumber(1),
            Sha::parse("a".repeat(40)).unwrap(),
            "feature".to_string(),
            "main".to_string(),
            None,
            PrState::Open,
            MergeStateStatus::Clean,
            false,
        );
        pr.predecessor_squash_reconciled = Some(Sha::parse("c".repeat(40)).unwrap());

        let snapshot = PersistedRepoSnapshot {
            schema_version: SCHEMA_VERSION,
            snapshot_at: crate::test_utils::test_timestamp(),
            log_generation: 0,
            log_position: 0,
            next_seq: 0,
            default_branch: "main".to_string(),
            prs: HashMap::from([(PrNumber(1), pr)]),
            active_trains: HashMap::new(),
            seen_dedupe_keys: HashMap::new(),
        };
        let mut state = RepoState::from_snapshot(snapshot);

        state.apply_event(&StateEvent {
            seq: 0,
            ts: crate::test_utils::test_timestamp(),
            payload: StateEventPayload::PrSynchronized {
                pr: PrNumber(1),
                new_head_sha: new_head.clone(),
            },
        });

        let p = &state.prs[&PrNumber(1)];
        assert_eq!(p.head_sha, new_head, "head SHA must update");
        assert_eq!(
            p.merge_state_status,
            MergeStateStatus::Unknown,
            "stale mergeability must be cleared on a new head"
        );
        assert_eq!(
            p.predecessor_squash_reconciled, None,
            "reconciliation against the old head must be cleared"
        );
    }

    /// is-root-by-construction (seam c): a reconciled, retargeted descendant of
    /// a squash-merged predecessor is a root; a marker against the wrong squash
    /// SHA is not.
    #[test]
    fn is_root_by_construction() {
        use crate::persistence::event::StateEvent;
        use crate::state::topology::is_root;

        let default_branch = "main".to_string();
        let pred = PrNumber(1);
        let desc = PrNumber(2);
        let squash = Sha::parse("a".repeat(40)).unwrap();

        // Predecessor (targets main) and descendant (stacked on predecessor,
        // targeting the predecessor's branch).
        let pred_pr = CachedPr::new(
            pred,
            Sha::parse("b".repeat(40)).unwrap(),
            "pred-branch".to_string(),
            default_branch.clone(),
            None,
            PrState::Open,
            MergeStateStatus::Clean,
            false,
        );
        let desc_pr = CachedPr::new(
            desc,
            Sha::parse("c".repeat(40)).unwrap(),
            "desc-branch".to_string(),
            "pred-branch".to_string(),
            Some(pred),
            PrState::Open,
            MergeStateStatus::Blocked,
            false,
        );
        let mut prs = HashMap::new();
        prs.insert(pred, pred_pr);
        prs.insert(desc, desc_pr);
        let snapshot = PersistedRepoSnapshot {
            schema_version: SCHEMA_VERSION,
            snapshot_at: crate::test_utils::test_timestamp(),
            log_generation: 0,
            log_position: 0,
            next_seq: 0,
            default_branch: default_branch.clone(),
            prs,
            active_trains: HashMap::new(),
            seen_dedupe_keys: HashMap::new(),
        };

        let ts = crate::test_utils::test_timestamp();
        let ev = |payload| StateEvent {
            seq: 0,
            ts,
            payload,
        };

        // Not a root yet: still stacked, targeting pred-branch.
        let mut state = RepoState::from_snapshot(snapshot);
        assert!(!is_root(&state.prs[&desc], &default_branch, &state.prs));

        // Apply the three seam-c events with matching squash SHA.
        state.apply_event(&ev(StateEventPayload::SquashCommitted {
            train_root: pred,
            pr: pred,
            sha: squash.clone(),
        }));
        state.apply_event(&ev(StateEventPayload::ReconciliationRecorded {
            pr: desc,
            squash_sha: squash.clone(),
        }));
        state.apply_event(&ev(StateEventPayload::DoneRetarget {
            train_root: pred,
            pr: desc,
            new_base: default_branch.clone(),
        }));

        assert!(
            is_root(&state.prs[&desc], &default_branch, &state.prs),
            "reconciled+retargeted descendant of a merged predecessor must be a root"
        );

        // A marker against a *different* squash SHA must NOT make it a root.
        let mut wrong = RepoState::from_snapshot(state.to_snapshot(0, 0, 0, ts));
        let other = Sha::parse("d".repeat(40)).unwrap();
        wrong.apply_event(&ev(StateEventPayload::ReconciliationRecorded {
            pr: desc,
            squash_sha: other,
        }));
        assert!(
            !is_root(&wrong.prs[&desc], &default_branch, &wrong.prs),
            "a reconciliation marker against the wrong squash must not confer rootness"
        );
    }
}
