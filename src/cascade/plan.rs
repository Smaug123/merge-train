//! The cascade planner's data vocabulary: plans, observations, and the
//! durable-fact ledger recovery reads.
//!
//! A [`StepPlan`] is one planning step's output. The executor contract
//! (M5) is:
//!
//! 1. Append `events` to the [`crate::store::Store`] (which applies them to
//!    the materialized `RepoState`) — durability strictly BEFORE effects.
//! 2. Execute `effects` in order, collecting an [`EffectOutcome`] per effect;
//!    stop at the first failure.
//! 3. Execute `best_effort` effects, logging (never propagating) failures.
//! 4. If `control` is [`Control::Continue`]: derive the next observation via
//!    [`super::observe`] and call [`super::advance`] again.
//!
//! The engine is stateless between calls: everything it needs to plan the
//! next step is in the materialized state (updated by step 1), the
//! observation, or — on [`Observation::Evaluate`] — the [`ReplayFacts`]
//! ledger derived from the durable event log.

use crate::effects::github::PrData;
use crate::effects::{Effect, GitHubResponse, GitResponse, MergeOutcome, PushOutcome, PushPoint};
use crate::persistence::event::{StateEvent, StateEventPayload};
use crate::state::transitions::TransitionError;
use crate::types::{CommentId, MergeStateStatus, PrNumber, Sha, TrainErrorKind};

/// What the worker should do after executing a plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Control {
    /// Execute the effects, observe the outcomes, and call `advance` again.
    Continue,
    /// The train is waiting (`WaitingCi` or preconditions unmet): stop
    /// looping. The next relevant webhook re-enters via
    /// [`Observation::Evaluate`].
    Park,
    /// This train needs nothing further (completed, stopped, aborted, or the
    /// request was rejected before a train existed).
    Done,
    /// The cascade step fanned out: the old train retired and one new train
    /// was created per listed root (by `FanOutCompleted` in `events`). The
    /// worker may evaluate each new train in turn.
    FanOut {
        /// The new independent train roots.
        new_roots: Vec<PrNumber>,
    },
}

/// One planning step's output.
///
/// `events` are appended (and applied) BEFORE any effect runs — that ordering
/// is the durability contract every intent/done bracket relies on. Events in
/// one plan land in a single `Store::append` batch, so their relative order
/// is atomic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepPlan {
    /// State events to append (then apply) in order, before effects run.
    pub events: Vec<StateEventPayload>,
    /// Effects to execute in order; their outcomes feed [`super::observe`].
    /// A failure of any of these becomes [`Observation::EffectFailed`].
    pub effects: Vec<Effect>,
    /// Effects whose failure must never influence the cascade (status-comment
    /// updates, worktree cleanup after a terminal event, courtesy comments).
    /// The executor logs failures and moves on.
    pub best_effort: Vec<Effect>,
    /// What to do next.
    pub control: Control,
}

impl StepPlan {
    /// A plan that does nothing further.
    pub fn done() -> Self {
        StepPlan {
            events: Vec::new(),
            effects: Vec::new(),
            best_effort: Vec::new(),
            control: Control::Done,
        }
    }
}

/// The response half of an executed effect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EffectResponse {
    /// A git effect's response.
    Git(GitResponse),
    /// A GitHub effect's response.
    GitHub(GitHubResponse),
}

/// A classified effect failure. The interpreters/executor (M4/M5) map raw
/// `GitError`/API errors into these; the engine only branches on the
/// classification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EffectError {
    /// The target branch no longer exists (deleted mid-cascade). During
    /// descendant processing this skips the descendant rather than aborting.
    BranchGone {
        /// The branch that is gone.
        branch: String,
    },
    /// Transient (network, 5xx, rate limit, propagation delay): the train
    /// parks and the work is re-derived from the durable ledger on the next
    /// evaluation.
    Transient {
        /// Human-readable detail.
        detail: String,
    },
    /// Permanent: retrying cannot help. Aborts the train with `kind` —
    /// except `PrClosed`/`BranchDeleted` on a descendant-scoped effect,
    /// which skip the descendant (DESIGN.md §Expected state tracking).
    Permanent {
        /// The abort category.
        kind: TrainErrorKind,
        /// Human-readable detail.
        detail: String,
    },
}

/// One executed effect with its result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectOutcome {
    /// The effect that was executed.
    pub effect: Effect,
    /// What happened.
    pub result: Result<EffectResponse, EffectError>,
}

/// What the engine learns between plans. Constructed from effect outcomes by
/// [`super::observe`] (or [`Observation::Evaluate`] by the worker itself when
/// a trigger or recovery re-enters the engine).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Observation {
    /// (Re-)evaluate the train: a start/trigger/recovery entry point. Carries
    /// the durable-fact ledger for the current phase so resumption plans
    /// idempotently around unmatched intents.
    Evaluate {
        /// Intent/done facts since the train's last phase transition.
        facts: ReplayFacts,
    },
    /// The preflight trio fetched at train start.
    PreflightFetched {
        /// Repository merge-method settings.
        settings: crate::effects::RepoSettingsData,
        /// Raw branch-protection response (`BranchProtection` or
        /// `BranchProtectionUnknown`).
        branch_protection: GitHubResponse,
        /// Raw rulesets response (`Rulesets` or `RulesetsUnknown`).
        rulesets: GitHubResponse,
    },
    /// The train's status comment was created.
    StatusCommentCreated {
        /// The new comment's id.
        id: CommentId,
    },
    /// A descendant was prepared (predecessor head merged into it).
    Prepared {
        /// The descendant's branch.
        branch: String,
        /// The merge outcome.
        merge: MergeOutcome,
        /// The pinned predecessor head that was merged.
        predecessor_head: Sha,
        /// Push point, present iff the merge succeeded.
        push_point: Option<PushPoint>,
    },
    /// A reconciliation merge completed on a descendant (merges only).
    Reconciled {
        /// The descendant's branch.
        branch: String,
        /// The merge outcome (of the two-step reconcile).
        merge: MergeOutcome,
        /// Push point, present iff the merge succeeded.
        push_point: Option<PushPoint>,
    },
    /// A catch-up merge completed on a descendant.
    CaughtUp {
        /// The descendant's branch.
        branch: String,
        /// The merge outcome.
        merge: MergeOutcome,
        /// Push point, present iff the merge succeeded.
        push_point: Option<PushPoint>,
    },
    /// The root branch was caught up with the default branch (`BEHIND` fix).
    RootCaughtUp {
        /// The root PR's branch.
        branch: String,
        /// The merge outcome.
        merge: MergeOutcome,
        /// Push point, present iff the merge succeeded.
        push_point: Option<PushPoint>,
    },
    /// A push completed with a domain outcome.
    Pushed {
        /// The branch that was pushed to.
        branch: String,
        /// The push outcome.
        outcome: PushOutcome,
    },
    /// The squash-merge landed.
    Squashed {
        /// The PR that was squashed.
        pr: PrNumber,
        /// The squash commit on the default branch.
        sha: Sha,
    },
    /// A revision was resolved (e.g. the squash parent `<sha>^`).
    Resolved {
        /// The revision that was parsed.
        rev: String,
        /// The resulting SHA.
        sha: Sha,
    },
    /// An externally-observed merge commit was validated as squash-shaped
    /// (or not).
    SquashValidated {
        /// The commit that was checked.
        sha: Sha,
        /// Whether it is a valid squash commit.
        valid: bool,
    },
    /// Recovery check: whether an intended push had already landed.
    PushChecked {
        /// The branch that was checked.
        branch: String,
        /// Whether the push completed.
        completed: bool,
    },
    /// A PR was retargeted to a new base.
    Retargeted {
        /// The PR that was retargeted.
        pr: PrNumber,
    },
    /// Fresh PR data + mergeability.
    PrRefreshed {
        /// The PR that was fetched.
        pr: PrNumber,
        /// The fetched PR data.
        data: PrData,
        /// The fetched merge state.
        merge_state: MergeStateStatus,
    },
    /// An observed effect failed, with classification.
    EffectFailed {
        /// The effect that failed.
        effect: Effect,
        /// The classified failure.
        error: EffectError,
    },
}

/// Errors from the planner. These are caller/executor bugs or impossible
/// states — every *domain* outcome (conflicts, rejections, closed PRs) is a
/// plan, not an error.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CascadeError {
    /// No train with this root exists in the state.
    #[error("no train with root {root} exists")]
    NoSuchTrain {
        /// The root that was looked up.
        root: PrNumber,
    },
    /// A PR the plan needs is not in the cache (M5 must ensure referenced PRs
    /// are cached before invoking the engine).
    #[error("PR {pr} is not in the cache")]
    UnknownPr {
        /// The missing PR.
        pr: PrNumber,
    },
    /// The observation cannot occur in the train's current phase — an
    /// executor bug or stale observation.
    #[error("observation {observation} is impossible in phase {phase}")]
    UnexpectedObservation {
        /// The train's phase.
        phase: &'static str,
        /// Debug rendering of the observation.
        observation: String,
    },
    /// A phase-transition invariant broke while planning — a bug in the
    /// engine itself.
    #[error("internal transition error: {0}")]
    Transition(#[from] TransitionError),
}

// ─── The durable-fact ledger ───

/// An intent recorded in the current phase, with whether its matching
/// done-event landed. Parsed from the raw event payloads into typed facts so
/// recovery code consumes proof, not promises.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntentFact {
    /// `intent_push_prep` (also used by the root `BEHIND` catch-up push).
    PushPrep {
        /// The branch being pushed.
        branch: String,
        /// Remote ref before the push.
        pre_push_sha: Sha,
        /// Expected tree after the merge.
        expected_tree: Sha,
        /// The prepare pin, if recorded.
        predecessor_head: Option<Sha>,
        /// Whether `done_push_prep` landed.
        done: bool,
    },
    /// `intent_squash`, matched by `squash_committed`.
    Squash {
        /// The PR being squashed.
        pr: PrNumber,
        /// Whether `squash_committed` landed.
        done: bool,
    },
    /// `intent_push_reconcile`.
    PushReconcile {
        /// The branch being pushed.
        branch: String,
        /// Remote ref before the push.
        pre_push_sha: Sha,
        /// Expected tree after the merge.
        expected_tree: Sha,
        /// Whether `done_push_reconcile` landed.
        done: bool,
    },
    /// `intent_push_catchup`.
    PushCatchup {
        /// The branch being pushed.
        branch: String,
        /// Remote ref before the push.
        pre_push_sha: Sha,
        /// Expected tree after the merge.
        expected_tree: Sha,
        /// Whether `done_push_catchup` landed.
        done: bool,
    },
    /// `intent_retarget`.
    Retarget {
        /// The PR being retargeted.
        pr: PrNumber,
        /// The base it is being retargeted to.
        new_base: String,
        /// Whether `done_retarget` landed.
        done: bool,
    },
}

/// The intent/done ledger for a train's *current* phase: every intent event
/// for the train since its last `PhaseTransition`, with done-ness.
///
/// This is bounded by construction: each descendant's intent/done pair is
/// followed by the `PhaseTransition` that marks the descendant complete, so
/// the ledger only ever holds the in-flight unit of work (plus, transiently,
/// the squash intent between `SquashPending` entry and the `Reconciling`
/// transition).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReplayFacts {
    /// The current phase's intents, in event order.
    pub intents: Vec<IntentFact>,
}

impl ReplayFacts {
    /// An empty ledger (for entering the engine on a fresh train).
    pub fn empty() -> Self {
        ReplayFacts::default()
    }

    /// Derives the ledger for `root`'s current phase from the event log.
    ///
    /// `events` must be (a suffix of) the repo's event log in append order,
    /// covering at least everything since the train's last `PhaseTransition`.
    /// Events for other trains are ignored; the ledger resets at each
    /// `PhaseTransition`/lifecycle boundary for this train, so passing more
    /// history than needed is harmless.
    pub fn for_train(events: &[StateEvent], root: PrNumber) -> Self {
        let mut intents: Vec<IntentFact> = Vec::new();
        for event in events {
            match &event.payload {
                // Phase/lifecycle boundaries reset the ledger: every intent
                // belonging to earlier work was settled before the boundary
                // was written.
                StateEventPayload::PhaseTransition { train_root, .. }
                | StateEventPayload::TrainStarted {
                    root_pr: train_root,
                    ..
                }
                | StateEventPayload::TrainStopped {
                    root_pr: train_root,
                }
                | StateEventPayload::TrainCompleted {
                    root_pr: train_root,
                }
                | StateEventPayload::TrainAborted {
                    root_pr: train_root,
                    ..
                }
                | StateEventPayload::FanOutCompleted {
                    old_root: train_root,
                    ..
                } if *train_root == root => intents.clear(),

                StateEventPayload::IntentPushPrep {
                    train_root,
                    branch,
                    pre_push_sha,
                    expected_tree,
                    predecessor_head,
                } if *train_root == root => intents.push(IntentFact::PushPrep {
                    branch: branch.clone(),
                    pre_push_sha: pre_push_sha.clone(),
                    expected_tree: expected_tree.clone(),
                    predecessor_head: predecessor_head.clone(),
                    done: false,
                }),
                StateEventPayload::DonePushPrep { train_root, branch } if *train_root == root => {
                    mark_done(
                        &mut intents,
                        |f| matches!(f, IntentFact::PushPrep { branch: b, done: false, .. } if b == branch),
                    );
                }

                StateEventPayload::IntentSquash { train_root, pr } if *train_root == root => {
                    intents.push(IntentFact::Squash {
                        pr: *pr,
                        done: false,
                    })
                }
                StateEventPayload::SquashCommitted { train_root, pr, .. }
                    if *train_root == root =>
                {
                    mark_done(
                        &mut intents,
                        |f| matches!(f, IntentFact::Squash { pr: p, done: false } if p == pr),
                    );
                }

                StateEventPayload::IntentPushReconcile {
                    train_root,
                    branch,
                    pre_push_sha,
                    expected_tree,
                } if *train_root == root => intents.push(IntentFact::PushReconcile {
                    branch: branch.clone(),
                    pre_push_sha: pre_push_sha.clone(),
                    expected_tree: expected_tree.clone(),
                    done: false,
                }),
                StateEventPayload::DonePushReconcile { train_root, branch }
                    if *train_root == root =>
                {
                    mark_done(
                        &mut intents,
                        |f| matches!(f, IntentFact::PushReconcile { branch: b, done: false, .. } if b == branch),
                    );
                }

                StateEventPayload::IntentPushCatchup {
                    train_root,
                    branch,
                    pre_push_sha,
                    expected_tree,
                } if *train_root == root => intents.push(IntentFact::PushCatchup {
                    branch: branch.clone(),
                    pre_push_sha: pre_push_sha.clone(),
                    expected_tree: expected_tree.clone(),
                    done: false,
                }),
                StateEventPayload::DonePushCatchup { train_root, branch }
                    if *train_root == root =>
                {
                    mark_done(
                        &mut intents,
                        |f| matches!(f, IntentFact::PushCatchup { branch: b, done: false, .. } if b == branch),
                    );
                }

                StateEventPayload::IntentRetarget {
                    train_root,
                    pr,
                    new_base,
                } if *train_root == root => intents.push(IntentFact::Retarget {
                    pr: *pr,
                    new_base: new_base.clone(),
                    done: false,
                }),
                StateEventPayload::DoneRetarget { train_root, pr, .. } if *train_root == root => {
                    mark_done(
                        &mut intents,
                        |f| matches!(f, IntentFact::Retarget { pr: p, done: false, .. } if p == pr),
                    );
                }

                _ => {}
            }
        }
        ReplayFacts { intents }
    }

    /// The unmatched (intent without done) facts, in event order.
    pub fn unmatched(&self) -> impl Iterator<Item = &IntentFact> {
        self.intents.iter().filter(|f| {
            !matches!(
                f,
                IntentFact::PushPrep { done: true, .. }
                    | IntentFact::Squash { done: true, .. }
                    | IntentFact::PushReconcile { done: true, .. }
                    | IntentFact::PushCatchup { done: true, .. }
                    | IntentFact::Retarget { done: true, .. }
            )
        })
    }

    /// The prepare pin recorded by any prep-push intent in the current phase
    /// (the earliest, which is the pin every later prep must match).
    pub fn recorded_pin(&self) -> Option<&Sha> {
        self.intents.iter().find_map(|f| match f {
            IntentFact::PushPrep {
                predecessor_head: Some(pin),
                ..
            } => Some(pin),
            _ => None,
        })
    }
}

/// Marks the earliest fact matching `pred` as done.
fn mark_done(intents: &mut [IntentFact], pred: impl Fn(&IntentFact) -> bool) {
    if let Some(fact) = intents.iter_mut().find(|f| pred(f)) {
        match fact {
            IntentFact::PushPrep { done, .. }
            | IntentFact::Squash { done, .. }
            | IntentFact::PushReconcile { done, .. }
            | IntentFact::PushCatchup { done, .. }
            | IntentFact::Retarget { done, .. } => *done = true,
        }
    }
}
