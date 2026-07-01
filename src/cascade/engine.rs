//! The pure cascade planner (stage M2).
//!
//! Every function here maps `(materialized state, observation)` to a
//! [`StepPlan`] — no I/O, no clock, no mutation. The worker (M5) owns the
//! loop: append the plan's events (durability first), execute its effects,
//! [`super::observe`] the outcomes, and call [`advance`] again until the plan
//! says [`Control::Park`], [`Control::Done`], or [`Control::FanOut`].
//!
//! # The crash contract
//!
//! Irreversible operations (pushes, the squash, retargets) are bracketed by
//! intent/done events per DESIGN.md §Durability and commit points; the intent
//! always travels in the *same plan* as the operation's effect, so it is
//! durable strictly before the operation runs. Recovery IS resumption:
//! [`Observation::Evaluate`] carries the [`ReplayFacts`] ledger of the
//! current phase's intents, and each resumption path plans an idempotency
//! check (`CheckPushCompleted`, a PR refetch) for any unmatched intent before
//! re-running work. Nothing here distinguishes "restart after crash" from
//! "resume after parking" — one code path, verified once.
//!
//! # Termination ordering (M2 amendment 4)
//!
//! Terminal events (`TrainStopped`/`TrainAborted`/`TrainCompleted`/
//! `FanOutCompleted`) land *first*; worktree cleanup and status-comment
//! updates follow as best-effort effects, and every cascade-step entry and
//! resumption defensively re-creates/cleans the worktree. A dirty worktree is
//! re-establishable state; an unrecorded terminal decision is not.

use chrono::{DateTime, Utc};

use crate::effects::github::GitHubEffect;
use crate::effects::{Effect, GitEffect, GitHubResponse, MergeOutcome, PushOutcome, PushPoint};
use crate::effects::{PrData, RepoSettingsData};
use crate::persistence::event::{StateEvent, StateEventPayload};
use crate::preflight::branch_protection::check_dismiss_stale_approvals;
use crate::preflight::{DismissStaleApprovalsCheckInput, check_merge_method_preflight};
use crate::state::RepoState;
use crate::state::descendants::{collect_all_descendants, collect_direct_descendants};
use crate::state::topology::{detect_cycle, is_root};
use crate::state::transitions::{PhaseOutcome, next_phase, start_preparing};
use crate::state::validation::validate_base_branch_matches_predecessor;
use crate::status::format_status_comment;
use crate::types::{
    CascadePhase, CommentId, MergeStateStatus, PhaseKind, PrNumber, PrState, Sha, TrainError,
    TrainErrorKind, TrainRecord, TrainState,
};

use super::plan::{
    CascadeError, Control, EffectError, IntentFact, Observation, ReplayFacts, StepPlan,
};

/// Maximum PRs in one train (root + all transitive descendants). Matches the
/// status-comment sizing assumption ("the 50-PR train cap") in
/// `crate::status::format`.
pub const MAX_TRAIN_SIZE: usize = 50;

/// Immutable planning context for one engine call.
struct Ctx<'a> {
    state: &'a RepoState,
    train: &'a TrainRecord,
    now: DateTime<Utc>,
}

impl Ctx<'_> {
    fn root(&self) -> PrNumber {
        self.train.original_root_pr
    }

    fn current(&self) -> PrNumber {
        self.train.current_pr
    }

    fn default_branch(&self) -> &str {
        &self.state.default_branch
    }

    fn branch_of(&self, pr: PrNumber) -> Result<String, CascadeError> {
        self.state
            .prs
            .get(&pr)
            .map(|p| p.head_ref.clone())
            .ok_or(CascadeError::UnknownPr { pr })
    }

    /// The first descendant of the current phase still needing work.
    fn next_remaining(&self) -> Option<PrNumber> {
        self.train
            .cascade_phase
            .progress()
            .and_then(|p| p.remaining().next().copied())
    }

    /// Resolves a branch to the frozen descendant it belongs to (remaining
    /// work only): observations name branches, plans name PRs.
    fn descendant_for_branch(&self, branch: &str) -> Result<PrNumber, CascadeError> {
        self.train
            .cascade_phase
            .progress()
            .and_then(|p| {
                p.remaining()
                    .find(|d| {
                        self.state
                            .prs
                            .get(d)
                            .is_some_and(|pr| pr.head_ref == branch)
                    })
                    .copied()
            })
            .ok_or_else(|| self.unexpected(format!("push/merge outcome for branch {branch}")))
    }

    fn unexpected(&self, observation: String) -> CascadeError {
        CascadeError::UnexpectedObservation {
            phase: self.train.cascade_phase.name(),
            observation,
        }
    }

    /// `[TrainResumed]` if the train is parked — prefixed onto any plan that
    /// actually proceeds with work.
    fn resumed_events(&self) -> Vec<StateEventPayload> {
        if self.train.state == TrainState::WaitingCi {
            vec![StateEventPayload::TrainResumed {
                root_pr: self.root(),
            }]
        } else {
            Vec::new()
        }
    }

    /// The live (non-skipped) frozen descendants of the current phase.
    fn live_descendants(&self) -> Vec<PrNumber> {
        self.train
            .cascade_phase
            .progress()
            .map(|p| {
                p.frozen_descendants()
                    .iter()
                    .filter(|d| !p.skipped().contains(d))
                    .copied()
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// The name of a train's worktree (`stack-<root>`, parsed back by
/// `git::parse_stack_dir_name`).
fn worktree_name(root: PrNumber) -> String {
    format!("stack-{root}")
}

/// Worktree prelude for plans that (re-)enter git work after a GitHub
/// round-trip or a resumption: create-or-reuse the stack worktree and return
/// it to a clean state (both idempotent).
fn worktree_prelude(root: PrNumber) -> Vec<Effect> {
    vec![
        Effect::Git(GitEffect::CreateWorktree {
            name: worktree_name(root),
        }),
        Effect::Git(GitEffect::CleanupWorktree),
    ]
}

fn push_effect(branch: &str) -> Effect {
    Effect::Git(GitEffect::Push {
        refspec: format!("HEAD:refs/heads/{branch}"),
        force: false,
    })
}

fn comment(pr: PrNumber, body: impl Into<String>) -> Effect {
    Effect::GitHub(GitHubEffect::PostComment {
        pr,
        body: body.into(),
    })
}

// ─── Entry points ───

/// Plans the response to `@merge-train start` on `pr`. Validation failures
/// are rejection comments (plans), not errors; on success the plan fetches
/// the preflight trio and continues via [`Observation::PreflightFetched`].
pub fn start_train(
    state: &RepoState,
    pr: PrNumber,
    _now: DateTime<Utc>,
) -> Result<StepPlan, CascadeError> {
    let Some(cached) = state.prs.get(&pr) else {
        return Err(CascadeError::UnknownPr { pr });
    };

    let reject = |body: String| StepPlan {
        events: Vec::new(),
        effects: Vec::new(),
        best_effort: vec![comment(pr, body)],
        control: Control::Done,
    };

    if !cached.state.is_open() {
        return Ok(reject(format!(
            "Cannot start a merge train: PR #{pr} is not open."
        )));
    }

    if let Some(root) = train_containing(state, pr) {
        return Ok(reject(format!(
            "A merge train involving PR #{pr} is already active (root #{root}). \
             Stop it with `@merge-train stop` before starting a new one."
        )));
    }

    if !is_root(cached, &state.default_branch, &state.prs) {
        return Ok(reject(format!(
            "PR #{pr} is not a stack root (it does not target `{}` with a \
             resolved predecessor). Issue `@merge-train start` on the root of \
             the stack.",
            state.default_branch
        )));
    }

    let members = {
        let mut m = collect_all_descendants(pr, &state.descendants, &state.prs);
        m.push(pr);
        m
    };
    if let Some(cycle) = detect_cycle(&state.prs)
        && cycle.iter().any(|c| members.contains(c))
    {
        return Ok(reject(format!(
            "Cannot start: the predecessor declarations form a cycle ({}).",
            cycle
                .iter()
                .map(|p| format!("#{p}"))
                .collect::<Vec<_>>()
                .join(" → ")
        )));
    }
    if members.len() > MAX_TRAIN_SIZE {
        return Ok(reject(format!(
            "Cannot start: this train would contain {} PRs (maximum {MAX_TRAIN_SIZE}).",
            members.len()
        )));
    }

    Ok(StepPlan {
        events: Vec::new(),
        effects: vec![
            Effect::GitHub(GitHubEffect::GetRepoSettings),
            Effect::GitHub(GitHubEffect::GetBranchProtection {
                branch: state.default_branch.clone(),
            }),
            Effect::GitHub(GitHubEffect::GetRulesets),
        ],
        best_effort: Vec::new(),
        control: Control::Continue,
    })
}

/// Plans the response to `@merge-train stop` on `pr` (any PR in the stack).
///
/// v1 `--force` semantics equal plain stop (stop + worktree removal + final
/// status-comment update — resolved design question 2); the flag is accepted
/// for interface stability.
pub fn stop_train(
    state: &RepoState,
    pr: PrNumber,
    _force: bool,
    now: DateTime<Utc>,
) -> Result<StepPlan, CascadeError> {
    let Some(root) = train_containing(state, pr) else {
        return Ok(StepPlan {
            events: Vec::new(),
            effects: Vec::new(),
            best_effort: vec![comment(pr, "No active merge train involves this PR.")],
            control: Control::Done,
        });
    };
    let train = state
        .active_trains
        .get(&root)
        .expect("train_containing returned an active train");

    let mut stopped = train.clone();
    stopped.stop(now);

    let mut best_effort = vec![Effect::Git(GitEffect::RemoveWorktree {
        name: worktree_name(root),
    })];
    if let Ok(Some(update)) = status_effect(&stopped, "🛑 Merge train stopped by request.") {
        best_effort.push(update);
    }

    Ok(StepPlan {
        events: vec![StateEventPayload::TrainStopped { root_pr: root }],
        effects: Vec::new(),
        best_effort,
        control: Control::Done,
    })
}

/// Plans the next step for `root`'s train given an observation.
pub fn advance(
    state: &RepoState,
    root: PrNumber,
    obs: Observation,
    now: DateTime<Utc>,
) -> Result<StepPlan, CascadeError> {
    // The start flow's first observation arrives before the train exists.
    if let Observation::PreflightFetched {
        settings,
        branch_protection,
        rulesets,
    } = obs
    {
        return on_preflight(state, root, settings, branch_protection, rulesets, now);
    }

    let train = state
        .active_trains
        .get(&root)
        .ok_or(CascadeError::NoSuchTrain { root })?;
    if !train.state.is_active() {
        return Ok(StepPlan::done());
    }
    let ctx = Ctx { state, train, now };

    match obs {
        Observation::PreflightFetched { .. } => unreachable!("handled above"),
        Observation::Evaluate { facts } => evaluate(&ctx, &facts),
        Observation::StatusCommentCreated { id } => on_status_comment_created(&ctx, id),
        Observation::Prepared {
            branch,
            merge,
            predecessor_head,
            push_point,
        } => on_prepared(&ctx, &branch, merge, predecessor_head, push_point),
        Observation::Reconciled {
            branch,
            merge,
            push_point,
        } => on_reconciled(&ctx, &branch, merge, push_point),
        Observation::CaughtUp {
            branch,
            merge,
            push_point,
        } => on_caught_up(&ctx, &branch, merge, push_point),
        Observation::RootCaughtUp {
            branch,
            merge,
            push_point,
        } => on_root_caught_up(&ctx, &branch, merge, push_point),
        Observation::Pushed { branch, outcome } => on_pushed(&ctx, &branch, outcome),
        Observation::Squashed { pr, sha } => on_squashed(&ctx, pr, sha),
        Observation::Resolved { rev, sha } => on_resolved(&ctx, &rev, sha),
        Observation::SquashValidated { sha, valid } => on_squash_validated(&ctx, sha, valid),
        Observation::PushChecked { branch, completed } => on_push_checked(&ctx, &branch, completed),
        Observation::Retargeted { pr } => on_retargeted(&ctx, pr),
        Observation::PrRefreshed {
            pr,
            data,
            merge_state,
        } => on_pr_refreshed(&ctx, pr, data, merge_state),
        Observation::EffectFailed { effect, error } => on_effect_failed(&ctx, &effect, error),
    }
}

/// Recovery entry point: identical to `advance(.., Evaluate { facts }, ..)`.
/// Recovery IS resumption (M2 amendment 5).
pub fn recover_train(
    state: &RepoState,
    root: PrNumber,
    facts: ReplayFacts,
    now: DateTime<Utc>,
) -> Result<StepPlan, CascadeError> {
    advance(state, root, Observation::Evaluate { facts }, now)
}

// ─── Start flow ───

fn on_preflight(
    state: &RepoState,
    root: PrNumber,
    settings: RepoSettingsData,
    branch_protection: GitHubResponse,
    rulesets: GitHubResponse,
    now: DateTime<Utc>,
) -> Result<StepPlan, CascadeError> {
    if let Err(e) = check_merge_method_preflight(&settings) {
        return Ok(StepPlan {
            events: Vec::new(),
            effects: Vec::new(),
            best_effort: vec![comment(root, e.format_comment())],
            control: Control::Done,
        });
    }

    let input = DismissStaleApprovalsCheckInput::from_responses(
        state.default_branch.clone(),
        branch_protection,
        rulesets,
    )
    .map_err(|e| CascadeError::UnexpectedObservation {
        phase: "start",
        observation: e.to_string(),
    })?;
    let mut best_effort = Vec::new();
    if let Some(warning) = check_dismiss_stale_approvals(&input).warning() {
        best_effort.push(comment(root, warning.format_comment()));
    }

    // The status comment is created from the record TrainStarted will
    // materialize (same modulo the store-stamped timestamp).
    let record = TrainRecord::new(root, now);
    let body = match format_status_comment(&record, "🚂 Merge train starting.") {
        Ok(body) => body,
        Err(_) => {
            // Unreachable for a fresh record; refuse to start rather than run
            // without the off-disk backup.
            best_effort.push(comment(
                root,
                "Cannot start: the status comment could not be formatted.",
            ));
            return Ok(StepPlan {
                events: Vec::new(),
                effects: Vec::new(),
                best_effort,
                control: Control::Done,
            });
        }
    };

    Ok(StepPlan {
        events: vec![StateEventPayload::TrainStarted {
            root_pr: root,
            current_pr: root,
        }],
        effects: vec![comment(root, body)],
        best_effort,
        control: Control::Continue,
    })
}

fn on_status_comment_created(ctx: &Ctx<'_>, id: CommentId) -> Result<StepPlan, CascadeError> {
    Ok(StepPlan {
        events: vec![StateEventPayload::StatusCommentPosted {
            root_pr: ctx.root(),
            comment_id: id,
        }],
        effects: vec![Effect::GitHub(GitHubEffect::RefetchPr {
            pr: ctx.current(),
        })],
        best_effort: Vec::new(),
        control: Control::Continue,
    })
}

// ─── Evaluation / resumption (one path for resume and recovery) ───

fn evaluate(ctx: &Ctx<'_>, facts: &ReplayFacts) -> Result<StepPlan, CascadeError> {
    match ctx.train.cascade_phase.kind() {
        PhaseKind::Idle => evaluate_idle(ctx, facts),
        PhaseKind::Preparing => resume_preparing(ctx, facts),
        PhaseKind::SquashPending => resume_squash(ctx, facts),
        PhaseKind::Reconciling => resume_descendant_phase(ctx, facts, PhaseKind::Reconciling),
        PhaseKind::CatchingUp => resume_descendant_phase(ctx, facts, PhaseKind::CatchingUp),
        PhaseKind::Retargeting => resume_retargeting(ctx, facts),
    }
}

fn evaluate_idle(ctx: &Ctx<'_>, facts: &ReplayFacts) -> Result<StepPlan, CascadeError> {
    // An unmatched prep-push intent at Idle is the root BEHIND catch-up push
    // interrupted mid-flight: settle it before re-evaluating.
    if let Some(check) = check_unmatched_push(ctx, facts) {
        return Ok(check);
    }

    // Self-healing: the status comment is the off-disk state backup; make
    // sure it exists before any step makes progress.
    if ctx.train.status_comment_id.is_none() {
        let body = match format_status_comment(ctx.train, "🚂 Merge train starting.") {
            Ok(body) => body,
            Err(_) => return Ok(abort_too_large(ctx)),
        };
        return Ok(StepPlan {
            events: Vec::new(),
            effects: vec![comment(ctx.root(), body)],
            best_effort: Vec::new(),
            control: Control::Continue,
        });
    }

    Ok(refetch_current(ctx))
}

fn resume_preparing(ctx: &Ctx<'_>, facts: &ReplayFacts) -> Result<StepPlan, CascadeError> {
    if let Some(check) = check_unmatched_push(ctx, facts) {
        return Ok(check);
    }
    let Some(d) = ctx.next_remaining() else {
        return Ok(abort_internal(
            ctx,
            "Preparing phase parked with no remaining work",
        ));
    };
    let branch = ctx.branch_of(d)?;
    let mut effects = worktree_prelude(ctx.root());
    effects.push(Effect::Git(GitEffect::PrepareDescendant {
        descendant_branch: branch,
        predecessor_pr: ctx.current(),
    }));
    Ok(StepPlan {
        events: ctx.resumed_events(),
        effects,
        best_effort: Vec::new(),
        control: Control::Continue,
    })
}

fn resume_squash(ctx: &Ctx<'_>, facts: &ReplayFacts) -> Result<StepPlan, CascadeError> {
    if let Some(check) = check_unmatched_push(ctx, facts) {
        return Ok(check);
    }
    // Memoryless: refetch the current PR; the refreshed data distinguishes
    // "squash landed (adopt after validation)" from "squash didn't land
    // (re-attempt behind the mergeability gate)". An unmatched IntentSquash
    // needs no special casing — the refetch answers it.
    Ok(refetch_current(ctx))
}

fn resume_descendant_phase(
    ctx: &Ctx<'_>,
    facts: &ReplayFacts,
    kind: PhaseKind,
) -> Result<StepPlan, CascadeError> {
    let Some(d) = ctx.next_remaining() else {
        return Ok(abort_internal(
            ctx,
            "descendant phase parked with no remaining work",
        ));
    };
    let branch = ctx.branch_of(d)?;

    let unmatched = facts.unmatched().find_map(|f| match (kind, f) {
        (
            PhaseKind::Reconciling,
            IntentFact::PushReconcile {
                branch: b,
                pre_push_sha,
                expected_tree,
                ..
            },
        )
        | (
            PhaseKind::CatchingUp,
            IntentFact::PushCatchup {
                branch: b,
                pre_push_sha,
                expected_tree,
                ..
            },
        ) if *b == branch => Some((pre_push_sha.clone(), expected_tree.clone())),
        _ => None,
    });

    let mut effects = worktree_prelude(ctx.root());
    if let Some((pre_push_sha, expected_tree)) = unmatched {
        // An ours-merge push doesn't change the tree: identify our commit by
        // its second parent (the squash) as well.
        let second_parent = match kind {
            PhaseKind::Reconciling => ctx.train.cascade_phase.squash_sha().cloned(),
            _ => None,
        };
        effects.push(Effect::Git(GitEffect::CheckPushCompleted {
            branch,
            expected_tree,
            pre_push_sha,
            second_parent,
        }));
    } else {
        effects.push(descendant_merge_effect(ctx, kind, &branch)?);
    }
    Ok(StepPlan {
        events: ctx.resumed_events(),
        effects,
        best_effort: Vec::new(),
        control: Control::Continue,
    })
}

fn resume_retargeting(ctx: &Ctx<'_>, facts: &ReplayFacts) -> Result<StepPlan, CascadeError> {
    let Some(d) = ctx.next_remaining() else {
        return Ok(abort_internal(
            ctx,
            "Retargeting phase parked with no remaining work",
        ));
    };
    let unmatched = facts
        .unmatched()
        .any(|f| matches!(f, IntentFact::Retarget { pr, .. } if *pr == d));
    if unmatched {
        // Did the retarget land? The refreshed base answers.
        return Ok(StepPlan {
            events: Vec::new(),
            effects: vec![Effect::GitHub(GitHubEffect::RefetchPr { pr: d })],
            best_effort: Vec::new(),
            control: Control::Continue,
        });
    }
    let mut events = ctx.resumed_events();
    events.push(StateEventPayload::IntentRetarget {
        train_root: ctx.root(),
        pr: d,
        new_base: ctx.default_branch().to_string(),
    });
    Ok(StepPlan {
        events,
        effects: vec![Effect::GitHub(GitHubEffect::RetargetPr {
            pr: d,
            new_base: ctx.default_branch().to_string(),
        })],
        best_effort: Vec::new(),
        control: Control::Continue,
    })
}

/// If the ledger holds an unmatched prep-push intent (a descendant prep push
/// or the root BEHIND push), plan its idempotency check first.
fn check_unmatched_push(ctx: &Ctx<'_>, facts: &ReplayFacts) -> Option<StepPlan> {
    facts.unmatched().find_map(|f| match f {
        IntentFact::PushPrep {
            branch,
            pre_push_sha,
            expected_tree,
            ..
        } => {
            let mut effects = worktree_prelude(ctx.root());
            effects.push(Effect::Git(GitEffect::CheckPushCompleted {
                branch: branch.clone(),
                expected_tree: expected_tree.clone(),
                pre_push_sha: pre_push_sha.clone(),
                second_parent: None,
            }));
            Some(StepPlan {
                events: Vec::new(),
                effects,
                best_effort: Vec::new(),
                control: Control::Continue,
            })
        }
        _ => None,
    })
}

fn refetch_current(ctx: &Ctx<'_>) -> StepPlan {
    StepPlan {
        events: Vec::new(),
        effects: vec![Effect::GitHub(GitHubEffect::RefetchPr {
            pr: ctx.current(),
        })],
        best_effort: Vec::new(),
        control: Control::Continue,
    }
}

// ─── The refreshed-PR decision points ───

fn on_pr_refreshed(
    ctx: &Ctx<'_>,
    pr: PrNumber,
    data: PrData,
    merge_state: MergeStateStatus,
) -> Result<StepPlan, CascadeError> {
    match ctx.train.cascade_phase.kind() {
        PhaseKind::Idle => current_pr_decision(ctx, pr, data, merge_state, true),
        PhaseKind::SquashPending => current_pr_decision(ctx, pr, data, merge_state, false),
        PhaseKind::Retargeting => retarget_check(ctx, pr, data, merge_state),
        _ => Err(ctx.unexpected(format!("PrRefreshed for #{pr}"))),
    }
}

/// The shared Idle / SquashPending decision: given fresh data + mergeability
/// for the current PR, start (or continue) the step, fix BEHIND, park, or
/// abort. `starting` is true at Idle (step not yet entered).
fn current_pr_decision(
    ctx: &Ctx<'_>,
    pr: PrNumber,
    data: PrData,
    merge_state: MergeStateStatus,
    starting: bool,
) -> Result<StepPlan, CascadeError> {
    if pr != ctx.current() {
        return Err(ctx.unexpected(format!(
            "PrRefreshed for #{pr} (current is #{}",
            ctx.current()
        )));
    }
    let mut events = vec![StateEventPayload::PrMergeStateChanged {
        pr,
        status: merge_state,
    }];

    match &data.state {
        PrState::Merged { merge_commit_sha } => {
            if starting {
                // Merged outside the cascade before this step began: its
                // descendants were never prepared. With none, the train is
                // simply finished; with some, this needs the (deferred)
                // late-addition flow — abort loudly.
                let open_descendants =
                    collect_direct_descendants(pr, &ctx.state.descendants, &ctx.state.prs);
                if open_descendants.is_empty() {
                    return complete_train(ctx, events);
                }
                return Ok(abort_plan(
                    ctx,
                    events,
                    TrainErrorKind::PreparationIncomplete,
                    format!(
                        "PR #{pr} was merged outside the cascade before its descendants \
                         were prepared. Late additions are not yet supported: rebase the \
                         descendants onto `{}` or restart their trains manually.",
                        ctx.default_branch()
                    ),
                ));
            }
            // SquashPending: the squash landed (ours, recovered, or an
            // external merge). Validate its shape before adopting it.
            let mut effects = worktree_prelude(ctx.root());
            effects.push(Effect::Git(GitEffect::ValidateSquashCommit {
                squash_sha: merge_commit_sha.clone(),
                default_branch: ctx.default_branch().to_string(),
            }));
            let mut all_events = ctx.resumed_events();
            all_events.append(&mut events);
            Ok(StepPlan {
                events: all_events,
                effects,
                best_effort: Vec::new(),
                control: Control::Continue,
            })
        }
        PrState::Closed => Ok(abort_plan(
            ctx,
            events,
            TrainErrorKind::PrClosed,
            format!("PR #{pr} was closed without merging; the train cannot continue."),
        )),
        PrState::Open => match merge_state {
            MergeStateStatus::Clean | MergeStateStatus::Unstable => {
                let mut all_events = ctx.resumed_events();
                all_events.append(&mut events);
                if starting {
                    begin_step(ctx, all_events, &data)
                } else {
                    plan_squash(ctx, all_events, &data)
                }
            }
            MergeStateStatus::Behind => {
                // Fix BEHIND only while it cannot invalidate preparations:
                // at Idle (nothing prepared) or a pin-less empty SquashPending.
                if starting
                    || (ctx.train.predecessor_head_sha.is_none()
                        && ctx.live_descendants().is_empty())
                {
                    let mut all_events = ctx.resumed_events();
                    all_events.append(&mut events);
                    let mut effects = worktree_prelude(ctx.root());
                    effects.push(Effect::Git(GitEffect::UpdateRootForBehind {
                        branch: data.head_ref.clone(),
                        default_branch: ctx.default_branch().to_string(),
                    }));
                    Ok(StepPlan {
                        events: all_events,
                        effects,
                        best_effort: Vec::new(),
                        control: Control::Continue,
                    })
                } else {
                    // Updating the root now would change its head and break
                    // the prepare pin (the force-push guard would refuse
                    // reconciliation). Restarting re-runs the BEHIND fix at
                    // Idle, then re-prepares against the new head.
                    Ok(abort_plan(
                        ctx,
                        events,
                        TrainErrorKind::ApiError,
                        format!(
                            "`{}` advanced during the cascade step and branch protection \
                             requires up-to-date branches; updating #{pr} now would \
                             invalidate the prepared descendants. Restart the train.",
                            ctx.default_branch()
                        ),
                    ))
                }
            }
            MergeStateStatus::Dirty => Ok(abort_plan(
                ctx,
                events,
                TrainErrorKind::MergeConflict,
                format!(
                    "PR #{pr} has merge conflicts with `{}`.",
                    ctx.default_branch()
                ),
            )),
            MergeStateStatus::HasHooks => Ok(abort_plan(
                ctx,
                events,
                TrainErrorKind::MergeHooksEnabled,
                "The repository has merge hooks or a merge queue enabled; \
                 merge-train cannot operate safely alongside them."
                    .to_string(),
            )),
            MergeStateStatus::Blocked | MergeStateStatus::Unknown | MergeStateStatus::Draft => {
                Ok(park_plan(
                    ctx,
                    events,
                    format!("waiting for PR #{pr} to become mergeable ({merge_state:?})"),
                ))
            }
        },
    }
}

/// Enters a cascade step for the current PR: freeze the open direct
/// descendants (sorted for determinism), validate their bases, and start
/// preparing — or squash directly when there is nothing to prepare.
fn begin_step(
    ctx: &Ctx<'_>,
    mut events: Vec<StateEventPayload>,
    data: &PrData,
) -> Result<StepPlan, CascadeError> {
    let current = ctx.current();
    let mut frozen = collect_direct_descendants(current, &ctx.state.descendants, &ctx.state.prs);
    frozen.sort();

    for d in &frozen {
        let Some(cached) = ctx.state.prs.get(d) else {
            continue;
        };
        if let Err(e) = validate_base_branch_matches_predecessor(cached, &ctx.state.prs) {
            return Ok(abort_plan(
                ctx,
                events,
                TrainErrorKind::BaseBranchMismatch,
                format!("descendant #{d} no longer targets its predecessor's branch: {e}"),
            ));
        }
    }

    let phase = start_preparing(frozen.clone());
    let entering = phase.kind();
    events.push(phase_transition(ctx, phase.clone(), None, None, None));

    let plan = match entering {
        PhaseKind::Preparing => {
            let first = *frozen.first().expect("Preparing implies descendants");
            let branch = ctx.branch_of(first)?;
            let mut effects = worktree_prelude(ctx.root());
            effects.push(Effect::Git(GitEffect::PrepareDescendant {
                descendant_branch: branch,
                predecessor_pr: current,
            }));
            StepPlan {
                events,
                effects,
                best_effort: Vec::new(),
                control: Control::Continue,
            }
        }
        PhaseKind::SquashPending => {
            // No descendants: squash immediately, pinned to the head we just
            // fetched.
            events.push(StateEventPayload::IntentSquash {
                train_root: ctx.root(),
                pr: current,
            });
            StepPlan {
                events,
                effects: vec![Effect::GitHub(GitHubEffect::SquashMerge {
                    pr: current,
                    expected_sha: data.head_sha.clone(),
                })],
                best_effort: Vec::new(),
                control: Control::Continue,
            }
        }
        other => {
            return Err(CascadeError::UnexpectedObservation {
                phase: other.name(),
                observation: "start_preparing produced a non-entry phase".to_string(),
            });
        }
    };
    Ok(finish_with_status(
        ctx,
        plan,
        &format!(
            "🚂 Cascade step started for #{current} ({} descendant(s) frozen).",
            frozen.len()
        ),
    ))
}

/// Plans the squash itself (SquashPending, gate passed).
fn plan_squash(
    ctx: &Ctx<'_>,
    mut events: Vec<StateEventPayload>,
    data: &PrData,
) -> Result<StepPlan, CascadeError> {
    // The fencing pin: the prepared head when descendants exist, else the
    // head fetched moments ago. GitHub 409s if the head moved past it.
    let expected_sha = ctx
        .train
        .predecessor_head_sha
        .clone()
        .unwrap_or_else(|| data.head_sha.clone());
    events.push(StateEventPayload::IntentSquash {
        train_root: ctx.root(),
        pr: ctx.current(),
    });
    Ok(StepPlan {
        events,
        effects: vec![Effect::GitHub(GitHubEffect::SquashMerge {
            pr: ctx.current(),
            expected_sha,
        })],
        best_effort: Vec::new(),
        control: Control::Continue,
    })
}

/// Retargeting recovery: an unmatched retarget intent resolves by looking at
/// the refreshed base.
fn retarget_check(
    ctx: &Ctx<'_>,
    pr: PrNumber,
    data: PrData,
    merge_state: MergeStateStatus,
) -> Result<StepPlan, CascadeError> {
    let Some(d) = ctx.next_remaining() else {
        return Ok(abort_internal(
            ctx,
            "Retargeting refresh with no remaining work",
        ));
    };
    if pr != d {
        return Err(ctx.unexpected(format!("PrRefreshed for #{pr} (expected #{d})")));
    }
    let mut events = ctx.resumed_events();
    events.push(StateEventPayload::PrMergeStateChanged {
        pr,
        status: merge_state,
    });
    if data.base_ref == ctx.default_branch() {
        events.push(StateEventPayload::DoneRetarget {
            train_root: ctx.root(),
            pr: d,
            new_base: ctx.default_branch().to_string(),
        });
        after_descendant_marked(
            ctx,
            events,
            PhaseOutcome::DescendantCompleted { pr: d },
            None,
        )
    } else {
        // The intent is durable and unmatched; re-issue the (idempotent)
        // retarget without a second intent.
        Ok(StepPlan {
            events,
            effects: vec![Effect::GitHub(GitHubEffect::RetargetPr {
                pr: d,
                new_base: ctx.default_branch().to_string(),
            })],
            best_effort: Vec::new(),
            control: Control::Continue,
        })
    }
}

// ─── Preparing ───

fn on_prepared(
    ctx: &Ctx<'_>,
    branch: &str,
    merge: MergeOutcome,
    predecessor_head: Sha,
    push_point: Option<PushPoint>,
) -> Result<StepPlan, CascadeError> {
    if ctx.train.cascade_phase.kind() != PhaseKind::Preparing {
        return Err(ctx.unexpected(format!("Prepared for {branch}")));
    }
    let d = ctx.descendant_for_branch(branch)?;

    // The pin rule: the first successful prep pins the predecessor head; any
    // later descendant seeing a different head means the predecessor was
    // force-pushed mid-preparation.
    let pin = match &ctx.train.predecessor_head_sha {
        Some(pin) if *pin != predecessor_head => {
            return Ok(abort_plan(
                ctx,
                Vec::new(),
                TrainErrorKind::HeadShaChanged,
                format!(
                    "PR #{} was force-pushed mid-preparation (prepared {pin}, now \
                     {predecessor_head}); the prepared descendants no longer match.",
                    ctx.current()
                ),
            ));
        }
        _ => predecessor_head,
    };

    match merge {
        MergeOutcome::Conflict { conflicting_files } => Ok(abort_plan(
            ctx,
            Vec::new(),
            TrainErrorKind::MergeConflict,
            format!(
                "preparing #{d} (merging #{}'s head into `{branch}`) conflicted: {}",
                ctx.current(),
                conflicting_files.join(", ")
            ),
        )),
        // The descendant already contains the predecessor head: nothing to
        // push, but the pin must still be recorded on the completion
        // transition.
        MergeOutcome::AlreadyUpToDate => after_descendant_marked(
            ctx,
            Vec::new(),
            PhaseOutcome::DescendantCompleted { pr: d },
            Some(pin),
        ),
        MergeOutcome::Success { .. } => match push_point {
            Some(PushPoint {
                expected_tree,
                pre_push_sha: Some(pre_push_sha),
            }) => Ok(StepPlan {
                events: vec![StateEventPayload::IntentPushPrep {
                    train_root: ctx.root(),
                    branch: branch.to_string(),
                    pre_push_sha,
                    expected_tree,
                    predecessor_head: Some(pin),
                }],
                effects: vec![push_effect(branch)],
                best_effort: Vec::new(),
                control: Control::Continue,
            }),
            _ => skip_descendant(ctx, d, "its branch disappeared before the preparation push"),
        },
    }
}

// ─── Reconciling / CatchingUp ───

fn on_reconciled(
    ctx: &Ctx<'_>,
    branch: &str,
    merge: MergeOutcome,
    push_point: Option<PushPoint>,
) -> Result<StepPlan, CascadeError> {
    if ctx.train.cascade_phase.kind() != PhaseKind::Reconciling {
        return Err(ctx.unexpected(format!("Reconciled for {branch}")));
    }
    let d = ctx.descendant_for_branch(branch)?;
    let squash_sha = expect_squash_sha(ctx)?;

    match merge {
        MergeOutcome::Conflict { conflicting_files } => Ok(abort_plan(
            ctx,
            Vec::new(),
            TrainErrorKind::MergeConflict,
            format!(
                "reconciling #{d} against the squash of #{} conflicted \
                 (commits that landed on `{}` before the squash): {}",
                ctx.current(),
                ctx.default_branch(),
                conflicting_files.join(", ")
            ),
        )),
        // Both reconcile merges were no-ops: the squash is already an
        // ancestor — a previous (crashed) reconcile landed.
        MergeOutcome::AlreadyUpToDate => after_descendant_marked(
            ctx,
            vec![StateEventPayload::ReconciliationRecorded { pr: d, squash_sha }],
            PhaseOutcome::DescendantCompleted { pr: d },
            None,
        ),
        MergeOutcome::Success { .. } => match push_point {
            Some(PushPoint {
                expected_tree,
                pre_push_sha: Some(pre_push_sha),
            }) => Ok(StepPlan {
                events: vec![StateEventPayload::IntentPushReconcile {
                    train_root: ctx.root(),
                    branch: branch.to_string(),
                    pre_push_sha,
                    expected_tree,
                }],
                effects: vec![push_effect(branch)],
                best_effort: Vec::new(),
                control: Control::Continue,
            }),
            _ => skip_descendant(ctx, d, "its branch disappeared before the reconcile push"),
        },
    }
}

fn on_caught_up(
    ctx: &Ctx<'_>,
    branch: &str,
    merge: MergeOutcome,
    push_point: Option<PushPoint>,
) -> Result<StepPlan, CascadeError> {
    if ctx.train.cascade_phase.kind() != PhaseKind::CatchingUp {
        return Err(ctx.unexpected(format!("CaughtUp for {branch}")));
    }
    let d = ctx.descendant_for_branch(branch)?;

    match merge {
        MergeOutcome::Conflict { conflicting_files } => Ok(abort_plan(
            ctx,
            Vec::new(),
            TrainErrorKind::MergeConflict,
            format!(
                "catching #{d} up with `{}` conflicted (commits that landed after \
                 the squash): {}",
                ctx.default_branch(),
                conflicting_files.join(", ")
            ),
        )),
        MergeOutcome::AlreadyUpToDate => after_descendant_marked(
            ctx,
            Vec::new(),
            PhaseOutcome::DescendantCompleted { pr: d },
            None,
        ),
        MergeOutcome::Success { .. } => match push_point {
            Some(PushPoint {
                expected_tree,
                pre_push_sha: Some(pre_push_sha),
            }) => Ok(StepPlan {
                events: vec![StateEventPayload::IntentPushCatchup {
                    train_root: ctx.root(),
                    branch: branch.to_string(),
                    pre_push_sha,
                    expected_tree,
                }],
                effects: vec![push_effect(branch)],
                best_effort: Vec::new(),
                control: Control::Continue,
            }),
            _ => skip_descendant(ctx, d, "its branch disappeared before the catch-up push"),
        },
    }
}

// ─── The root BEHIND flow ───

fn on_root_caught_up(
    ctx: &Ctx<'_>,
    branch: &str,
    merge: MergeOutcome,
    push_point: Option<PushPoint>,
) -> Result<StepPlan, CascadeError> {
    match merge {
        MergeOutcome::Conflict { conflicting_files } => Ok(abort_plan(
            ctx,
            Vec::new(),
            TrainErrorKind::MergeConflict,
            format!(
                "updating #{} with `{}` conflicted: {}",
                ctx.current(),
                ctx.default_branch(),
                conflicting_files.join(", ")
            ),
        )),
        // Not actually behind (raced with a manual update): re-evaluate.
        MergeOutcome::AlreadyUpToDate => Ok(refetch_current(ctx)),
        MergeOutcome::Success { .. } => match push_point {
            Some(PushPoint {
                expected_tree,
                pre_push_sha: Some(pre_push_sha),
            }) => Ok(StepPlan {
                events: vec![StateEventPayload::IntentPushPrep {
                    train_root: ctx.root(),
                    branch: branch.to_string(),
                    pre_push_sha,
                    expected_tree,
                    predecessor_head: None,
                }],
                effects: vec![push_effect(branch)],
                best_effort: Vec::new(),
                control: Control::Continue,
            }),
            _ => Ok(abort_plan(
                ctx,
                Vec::new(),
                TrainErrorKind::BranchDeleted,
                format!("the root branch `{branch}` disappeared during the update"),
            )),
        },
    }
}

// ─── Pushes ───

fn on_pushed(ctx: &Ctx<'_>, branch: &str, outcome: PushOutcome) -> Result<StepPlan, CascadeError> {
    let phase = ctx.train.cascade_phase.kind();
    match outcome {
        PushOutcome::Pushed { .. } | PushOutcome::AlreadyUpToDate => match phase {
            // The root BEHIND catch-up push: CI reruns on the new head.
            PhaseKind::Idle | PhaseKind::SquashPending => {
                let events = vec![StateEventPayload::DonePushPrep {
                    train_root: ctx.root(),
                    branch: branch.to_string(),
                }];
                Ok(park_plan(
                    ctx,
                    events,
                    format!(
                        "updated `{branch}` with `{}`; waiting for CI on the new head",
                        ctx.default_branch()
                    ),
                ))
            }
            PhaseKind::Preparing => {
                let d = ctx.descendant_for_branch(branch)?;
                after_descendant_marked(
                    ctx,
                    vec![StateEventPayload::DonePushPrep {
                        train_root: ctx.root(),
                        branch: branch.to_string(),
                    }],
                    PhaseOutcome::DescendantCompleted { pr: d },
                    None,
                )
            }
            PhaseKind::Reconciling => {
                let d = ctx.descendant_for_branch(branch)?;
                let squash_sha = expect_squash_sha(ctx)?;
                after_descendant_marked(
                    ctx,
                    vec![
                        StateEventPayload::DonePushReconcile {
                            train_root: ctx.root(),
                            branch: branch.to_string(),
                        },
                        StateEventPayload::ReconciliationRecorded { pr: d, squash_sha },
                    ],
                    PhaseOutcome::DescendantCompleted { pr: d },
                    None,
                )
            }
            PhaseKind::CatchingUp => {
                let d = ctx.descendant_for_branch(branch)?;
                after_descendant_marked(
                    ctx,
                    vec![StateEventPayload::DonePushCatchup {
                        train_root: ctx.root(),
                        branch: branch.to_string(),
                    }],
                    PhaseOutcome::DescendantCompleted { pr: d },
                    None,
                )
            }
            PhaseKind::Retargeting => Err(ctx.unexpected(format!("Pushed for {branch}"))),
        },
        PushOutcome::Rejected { details } => match phase {
            // Someone pushed the branch mid-preparation: re-prepare against
            // the new remote state (the merge incorporates their commits); a
            // genuine conflict aborts via the Prepared observation
            // (DESIGN.md §Concurrent push handling).
            PhaseKind::Preparing => {
                let _d = ctx.descendant_for_branch(branch)?;
                Ok(StepPlan {
                    events: Vec::new(),
                    effects: vec![Effect::Git(GitEffect::PrepareDescendant {
                        descendant_branch: branch.to_string(),
                        predecessor_pr: ctx.current(),
                    })],
                    best_effort: Vec::new(),
                    control: Control::Continue,
                })
            }
            // Same rule for the root BEHIND push (nothing irreversible yet).
            PhaseKind::Idle | PhaseKind::SquashPending => Ok(StepPlan {
                events: Vec::new(),
                effects: {
                    let mut effects = worktree_prelude(ctx.root());
                    effects.push(Effect::Git(GitEffect::UpdateRootForBehind {
                        branch: branch.to_string(),
                        default_branch: ctx.default_branch().to_string(),
                    }));
                    effects
                },
                best_effort: Vec::new(),
                control: Control::Continue,
            }),
            // After reconciliation the ours-merge was computed against a
            // specific base: a foreign push means the branch diverged —
            // always abort (DESIGN.md §Concurrent push handling).
            PhaseKind::Reconciling | PhaseKind::CatchingUp => Ok(abort_plan(
                ctx,
                Vec::new(),
                TrainErrorKind::PushRejected,
                format!("push to `{branch}` was rejected after reconciliation: {details}"),
            )),
            PhaseKind::Retargeting => Err(ctx.unexpected(format!("Pushed for {branch}"))),
        },
    }
}

fn on_push_checked(ctx: &Ctx<'_>, branch: &str, completed: bool) -> Result<StepPlan, CascadeError> {
    let phase = ctx.train.cascade_phase.kind();
    match phase {
        // The root BEHIND push: settle the bracket, then re-evaluate.
        PhaseKind::Idle | PhaseKind::SquashPending => {
            let mut plan = if completed {
                refetch_current(ctx)
            } else {
                let mut effects = worktree_prelude(ctx.root());
                effects.push(Effect::Git(GitEffect::UpdateRootForBehind {
                    branch: branch.to_string(),
                    default_branch: ctx.default_branch().to_string(),
                }));
                StepPlan {
                    events: Vec::new(),
                    effects,
                    best_effort: Vec::new(),
                    control: Control::Continue,
                }
            };
            if completed {
                plan.events.push(StateEventPayload::DonePushPrep {
                    train_root: ctx.root(),
                    branch: branch.to_string(),
                });
            }
            Ok(plan)
        }
        PhaseKind::Preparing => {
            let d = ctx.descendant_for_branch(branch)?;
            if completed {
                after_descendant_marked(
                    ctx,
                    vec![StateEventPayload::DonePushPrep {
                        train_root: ctx.root(),
                        branch: branch.to_string(),
                    }],
                    PhaseOutcome::DescendantCompleted { pr: d },
                    None,
                )
            } else {
                // The push provably didn't land: re-preparing is safe.
                Ok(StepPlan {
                    events: Vec::new(),
                    effects: vec![Effect::Git(GitEffect::PrepareDescendant {
                        descendant_branch: branch.to_string(),
                        predecessor_pr: ctx.current(),
                    })],
                    best_effort: Vec::new(),
                    control: Control::Continue,
                })
            }
        }
        PhaseKind::Reconciling => {
            let d = ctx.descendant_for_branch(branch)?;
            if completed {
                let squash_sha = expect_squash_sha(ctx)?;
                after_descendant_marked(
                    ctx,
                    vec![
                        StateEventPayload::DonePushReconcile {
                            train_root: ctx.root(),
                            branch: branch.to_string(),
                        },
                        StateEventPayload::ReconciliationRecorded { pr: d, squash_sha },
                    ],
                    PhaseOutcome::DescendantCompleted { pr: d },
                    None,
                )
            } else {
                Ok(StepPlan {
                    events: Vec::new(),
                    effects: vec![descendant_merge_effect(
                        ctx,
                        PhaseKind::Reconciling,
                        branch,
                    )?],
                    best_effort: Vec::new(),
                    control: Control::Continue,
                })
            }
        }
        PhaseKind::CatchingUp => {
            let d = ctx.descendant_for_branch(branch)?;
            if completed {
                after_descendant_marked(
                    ctx,
                    vec![StateEventPayload::DonePushCatchup {
                        train_root: ctx.root(),
                        branch: branch.to_string(),
                    }],
                    PhaseOutcome::DescendantCompleted { pr: d },
                    None,
                )
            } else {
                Ok(StepPlan {
                    events: Vec::new(),
                    effects: vec![descendant_merge_effect(ctx, PhaseKind::CatchingUp, branch)?],
                    best_effort: Vec::new(),
                    control: Control::Continue,
                })
            }
        }
        PhaseKind::Retargeting => Err(ctx.unexpected(format!("PushChecked for {branch}"))),
    }
}

// ─── The squash ───

fn on_squashed(ctx: &Ctx<'_>, pr: PrNumber, sha: Sha) -> Result<StepPlan, CascadeError> {
    if ctx.train.cascade_phase.kind() != PhaseKind::SquashPending || pr != ctx.current() {
        return Err(ctx.unexpected(format!("Squashed #{pr}")));
    }
    squashed_continuation(
        ctx,
        vec![StateEventPayload::SquashCommitted {
            train_root: ctx.root(),
            pr,
            sha: sha.clone(),
        }],
        sha,
    )
}

fn on_squash_validated(ctx: &Ctx<'_>, sha: Sha, valid: bool) -> Result<StepPlan, CascadeError> {
    if ctx.train.cascade_phase.kind() != PhaseKind::SquashPending {
        return Err(ctx.unexpected(format!("SquashValidated {sha}")));
    }
    if !valid {
        return Ok(abort_plan(
            ctx,
            Vec::new(),
            TrainErrorKind::NonSquashMerge,
            format!(
                "PR #{} was merged outside the cascade and its merge commit {sha} is \
                 not a valid squash (merge/rebase method, or history rewritten); \
                 the descendants cannot be reconciled safely.",
                ctx.current()
            ),
        ));
    }
    squashed_continuation(
        ctx,
        vec![StateEventPayload::SquashCommitted {
            train_root: ctx.root(),
            pr: ctx.current(),
            sha: sha.clone(),
        }],
        sha,
    )
}

/// After the squash is durable: capture its parent if descendants remain,
/// otherwise the train is complete.
fn squashed_continuation(
    ctx: &Ctx<'_>,
    events: Vec<StateEventPayload>,
    sha: Sha,
) -> Result<StepPlan, CascadeError> {
    if ctx.live_descendants().is_empty() {
        return complete_train(ctx, events);
    }
    let mut effects = worktree_prelude(ctx.root());
    effects.push(Effect::Git(GitEffect::Fetch {
        refspecs: vec![ctx.default_branch().to_string()],
    }));
    effects.push(Effect::Git(GitEffect::RevParse {
        rev: format!("{sha}^"),
    }));
    Ok(StepPlan {
        events,
        effects,
        best_effort: Vec::new(),
        control: Control::Continue,
    })
}

/// The squash parent observed: transition to Reconciling and start the first
/// reconcile.
fn on_resolved(ctx: &Ctx<'_>, rev: &str, parent: Sha) -> Result<StepPlan, CascadeError> {
    if ctx.train.cascade_phase.kind() != PhaseKind::SquashPending {
        return Err(ctx.unexpected(format!("Resolved {rev}")));
    }
    let squash_sha = match &ctx.state.prs.get(&ctx.current()).map(|p| &p.state) {
        Some(PrState::Merged { merge_commit_sha }) => merge_commit_sha.clone(),
        _ => {
            return Err(ctx.unexpected(format!(
                "Resolved {rev} but #{} is not merged in the cache",
                ctx.current()
            )));
        }
    };
    if rev != format!("{squash_sha}^") {
        return Err(ctx.unexpected(format!("Resolved {rev} (expected {squash_sha}^)")));
    }

    let Some(pin) = ctx.train.predecessor_head_sha.clone() else {
        // Descendants exist (live check happened before parent capture), so
        // preparation must have pinned the head. Without it the force-push
        // guard cannot run — refuse to reconcile.
        return Ok(abort_internal(
            ctx,
            "the prepare pin is missing at reconciliation time",
        ));
    };

    let phase = ctx.train.cascade_phase.clone();
    let next = next_phase(
        &phase,
        PhaseOutcome::SquashComplete {
            squash_sha: squash_sha.clone(),
        },
    )?;
    if next.kind() != PhaseKind::Reconciling {
        // Live descendants exist, so the settle target must be Reconciling.
        return Ok(abort_internal(ctx, "squash settle skipped Reconciling"));
    }
    let first = next
        .progress()
        .and_then(|p| p.remaining().next().copied())
        .expect("live descendants imply reconcile work");
    let branch = ctx.branch_of(first)?;

    let events = vec![phase_transition(
        ctx,
        next,
        Some(squash_sha.clone()),
        Some(parent.clone()),
        Some(pin.clone()),
    )];
    let effects = vec![Effect::Git(GitEffect::MergeReconcile {
        descendant_branch: branch,
        predecessor_pr: ctx.current(),
        squash_sha: squash_sha.clone(),
        expected_squash_parent: parent,
        predecessor_pre_squash_head: pin,
        default_branch: ctx.default_branch().to_string(),
    })];
    let plan = StepPlan {
        events,
        effects,
        best_effort: Vec::new(),
        control: Control::Continue,
    };
    Ok(finish_with_status(
        ctx,
        plan,
        &format!(
            "✅ Squashed #{} as {}; reconciling descendants.",
            ctx.current(),
            squash_sha
        ),
    ))
}

// ─── Retargeting ───

fn on_retargeted(ctx: &Ctx<'_>, pr: PrNumber) -> Result<StepPlan, CascadeError> {
    if ctx.train.cascade_phase.kind() != PhaseKind::Retargeting {
        return Err(ctx.unexpected(format!("Retargeted #{pr}")));
    }
    let in_remaining = ctx
        .train
        .cascade_phase
        .progress()
        .is_some_and(|p| p.remaining().any(|d| *d == pr));
    if !in_remaining {
        return Err(ctx.unexpected(format!("Retargeted #{pr}")));
    }
    after_descendant_marked(
        ctx,
        vec![StateEventPayload::DoneRetarget {
            train_root: ctx.root(),
            pr,
            new_base: ctx.default_branch().to_string(),
        }],
        PhaseOutcome::DescendantCompleted { pr },
        None,
    )
}

// ─── Failures ───

fn on_effect_failed(
    ctx: &Ctx<'_>,
    effect: &Effect,
    error: EffectError,
) -> Result<StepPlan, CascadeError> {
    match error {
        EffectError::Transient { detail } => Ok(park_plan(
            ctx,
            Vec::new(),
            format!("transient failure ({detail}); will re-derive on the next event"),
        )),
        EffectError::BranchGone { branch } => {
            if let Ok(d) = ctx.descendant_for_branch(&branch) {
                skip_descendant(ctx, d, "its branch was deleted")
            } else {
                Ok(abort_plan(
                    ctx,
                    Vec::new(),
                    TrainErrorKind::BranchDeleted,
                    format!("branch `{branch}` was deleted during the cascade"),
                ))
            }
        }
        EffectError::Permanent { kind, detail } => {
            // A closed/deleted *descendant* is skipped, not fatal (DESIGN.md
            // §Expected state tracking); everything else aborts.
            if matches!(
                kind,
                TrainErrorKind::PrClosed | TrainErrorKind::BranchDeleted
            ) && let Some(d) = descendant_target(ctx, effect)
            {
                return skip_descendant(ctx, d, &detail);
            }
            Ok(abort_plan(ctx, Vec::new(), kind, detail))
        }
    }
}

/// The remaining descendant an effect targets, if any (for skip-vs-abort).
fn descendant_target(ctx: &Ctx<'_>, effect: &Effect) -> Option<PrNumber> {
    let branch = match effect {
        Effect::Git(GitEffect::PrepareDescendant {
            descendant_branch, ..
        })
        | Effect::Git(GitEffect::MergeReconcile {
            descendant_branch, ..
        })
        | Effect::Git(GitEffect::CatchUpDescendant {
            descendant_branch, ..
        })
        | Effect::Git(GitEffect::CheckPushCompleted {
            branch: descendant_branch,
            ..
        }) => descendant_branch.clone(),
        Effect::Git(GitEffect::Push { refspec, .. }) => refspec
            .strip_prefix("HEAD:refs/heads/")
            .unwrap_or(refspec)
            .to_string(),
        Effect::GitHub(GitHubEffect::RetargetPr { pr, .. }) => {
            return ctx
                .train
                .cascade_phase
                .progress()
                .and_then(|p| p.remaining().find(|d| *d == pr).copied());
        }
        _ => return None,
    };
    ctx.descendant_for_branch(&branch).ok()
}

// ─── Progress bookkeeping and step endings ───

/// After a descendant is marked completed/skipped: write the transition and
/// plan the next unit of work — the next descendant, the next phase's entry
/// work, or the end of the step (continue / complete / fan out).
fn after_descendant_marked(
    ctx: &Ctx<'_>,
    mut events: Vec<StateEventPayload>,
    outcome: PhaseOutcome,
    pin_override: Option<Sha>,
) -> Result<StepPlan, CascadeError> {
    let phase = &ctx.train.cascade_phase;
    let next = next_phase(phase, outcome)?;
    let pin = pin_override.or_else(|| ctx.train.predecessor_head_sha.clone());
    let phase_changed = next.kind() != phase.kind();

    if next.kind() == PhaseKind::Idle {
        return end_of_step(ctx, events, &next);
    }

    events.push(phase_transition(
        ctx,
        next.clone(),
        phase.squash_sha().cloned(),
        ctx.train.last_squash_parent_sha.clone(),
        pin.clone(),
    ));

    let plan = match next.kind() {
        PhaseKind::Preparing => {
            let d = first_remaining(&next)?;
            StepPlan {
                events,
                effects: vec![Effect::Git(GitEffect::PrepareDescendant {
                    descendant_branch: ctx.branch_of(d)?,
                    predecessor_pr: ctx.current(),
                })],
                best_effort: Vec::new(),
                control: Control::Continue,
            }
        }
        // Preparation finished: gate the squash on fresh mergeability.
        PhaseKind::SquashPending => StepPlan {
            events,
            effects: vec![Effect::GitHub(GitHubEffect::RefetchPr {
                pr: ctx.current(),
            })],
            best_effort: Vec::new(),
            control: Control::Continue,
        },
        PhaseKind::Reconciling => {
            let d = first_remaining(&next)?;
            let branch = ctx.branch_of(d)?;
            let effect = reconcile_effect(ctx, &next, &branch, pin)?;
            StepPlan {
                events,
                effects: vec![effect],
                best_effort: Vec::new(),
                control: Control::Continue,
            }
        }
        PhaseKind::CatchingUp => {
            let d = first_remaining(&next)?;
            StepPlan {
                events,
                effects: vec![Effect::Git(GitEffect::CatchUpDescendant {
                    descendant_branch: ctx.branch_of(d)?,
                    default_branch: ctx.default_branch().to_string(),
                })],
                best_effort: Vec::new(),
                control: Control::Continue,
            }
        }
        PhaseKind::Retargeting => {
            let d = first_remaining(&next)?;
            let mut events = events;
            events.push(StateEventPayload::IntentRetarget {
                train_root: ctx.root(),
                pr: d,
                new_base: ctx.default_branch().to_string(),
            });
            StepPlan {
                events,
                effects: vec![Effect::GitHub(GitHubEffect::RetargetPr {
                    pr: d,
                    new_base: ctx.default_branch().to_string(),
                })],
                best_effort: Vec::new(),
                control: Control::Continue,
            }
        }
        PhaseKind::Idle => unreachable!("handled above"),
    };

    Ok(if phase_changed {
        finish_with_status(
            ctx,
            plan,
            &format!("➡️ Entering {} for #{}.", next.name(), ctx.current()),
        )
    } else {
        plan
    })
}

/// The step's work is done (settled to Idle): continue with a single
/// descendant, fan out to several, or complete.
fn end_of_step(
    ctx: &Ctx<'_>,
    mut events: Vec<StateEventPayload>,
    _settled: &CascadePhase,
) -> Result<StepPlan, CascadeError> {
    // The settle target (Idle) drops the progress, so read the live set from
    // the pre-transition phase plus the events just recorded (a skip event in
    // `events` means the marked descendant is NOT live).
    let skipped_now: Vec<PrNumber> = events
        .iter()
        .filter_map(|e| match e {
            StateEventPayload::DescendantSkipped { descendant_pr, .. } => Some(*descendant_pr),
            _ => None,
        })
        .collect();
    let live: Vec<PrNumber> = ctx
        .live_descendants()
        .into_iter()
        .filter(|d| !skipped_now.contains(d))
        .collect();

    match live.as_slice() {
        [] => complete_train(ctx, events),
        [only] => {
            let next_pr = *only;
            events.push(StateEventPayload::PhaseTransition {
                train_root: ctx.root(),
                current_pr: next_pr,
                predecessor_pr: Some(ctx.current()),
                last_squash_sha: ctx.train.cascade_phase.squash_sha().cloned(),
                last_squash_parent: ctx.train.last_squash_parent_sha.clone(),
                predecessor_head_sha: None,
                phase: CascadePhase::Idle,
            });
            let plan = StepPlan {
                events,
                effects: vec![Effect::GitHub(GitHubEffect::RefetchPr { pr: next_pr })],
                best_effort: Vec::new(),
                control: Control::Continue,
            };
            Ok(finish_with_status(
                ctx,
                plan,
                &format!(
                    "✅ #{} merged; #{next_pr} is the new head of the train.",
                    ctx.current()
                ),
            ))
        }
        several => {
            let new_roots: Vec<PrNumber> = several.to_vec();
            events.push(StateEventPayload::FanOutCompleted {
                old_root: ctx.root(),
                new_roots: new_roots.clone(),
                original_root_pr: ctx.root(),
            });
            let mut record = ctx.train.clone();
            record.state = TrainState::Completed { ended_at: ctx.now };
            record.increment_seq();
            let mut best_effort = vec![Effect::Git(GitEffect::RemoveWorktree {
                name: worktree_name(ctx.root()),
            })];
            let msg = format!(
                "🎉 Train completed. Descendants {} are now independent trains.",
                new_roots
                    .iter()
                    .map(|p| format!("#{p}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            if let Ok(Some(update)) = status_effect(&record, &msg) {
                best_effort.push(update);
            }
            Ok(StepPlan {
                events,
                effects: Vec::new(),
                best_effort,
                control: Control::FanOut { new_roots },
            })
        }
    }
}

/// Terminal success: the current PR merged and nothing remains to cascade.
fn complete_train(
    ctx: &Ctx<'_>,
    mut events: Vec<StateEventPayload>,
) -> Result<StepPlan, CascadeError> {
    events.push(StateEventPayload::TrainCompleted {
        root_pr: ctx.root(),
    });
    let mut record = ctx.train.clone();
    record.state = TrainState::Completed { ended_at: ctx.now };
    record.increment_seq();
    let mut best_effort = vec![Effect::Git(GitEffect::RemoveWorktree {
        name: worktree_name(ctx.root()),
    })];
    if let Ok(Some(update)) = status_effect(&record, "🎉 Merge train completed.") {
        best_effort.push(update);
    }
    Ok(StepPlan {
        events,
        effects: Vec::new(),
        best_effort,
        control: Control::Done,
    })
}

/// Skips a descendant (closed / branch deleted) and continues the step.
fn skip_descendant(ctx: &Ctx<'_>, d: PrNumber, reason: &str) -> Result<StepPlan, CascadeError> {
    let events = vec![StateEventPayload::DescendantSkipped {
        root_pr: ctx.root(),
        descendant_pr: d,
        reason: reason.to_string(),
    }];
    let mut plan =
        after_descendant_marked(ctx, events, PhaseOutcome::DescendantSkipped { pr: d }, None)?;
    plan.best_effort.push(comment(
        d,
        format!(
            "⚠️ PR #{d} was skipped by the merge train (root #{}): {reason}. \
             It will not be retried; resolve the issue and restart if needed.",
            ctx.root()
        ),
    ));
    Ok(plan)
}

// ─── Small helpers ───

fn first_remaining(phase: &CascadePhase) -> Result<PrNumber, CascadeError> {
    phase
        .progress()
        .and_then(|p| p.remaining().next().copied())
        .ok_or(CascadeError::UnexpectedObservation {
            phase: phase.name(),
            observation: "active phase with no remaining work".to_string(),
        })
}

fn descendant_merge_effect(
    ctx: &Ctx<'_>,
    kind: PhaseKind,
    branch: &str,
) -> Result<Effect, CascadeError> {
    match kind {
        PhaseKind::Reconciling => {
            let pin = ctx.train.predecessor_head_sha.clone();
            reconcile_effect(ctx, &ctx.train.cascade_phase, branch, pin)
        }
        PhaseKind::CatchingUp => Ok(Effect::Git(GitEffect::CatchUpDescendant {
            descendant_branch: branch.to_string(),
            default_branch: ctx.default_branch().to_string(),
        })),
        other => Err(CascadeError::UnexpectedObservation {
            phase: other.name(),
            observation: "no descendant merge effect for this phase".to_string(),
        }),
    }
}

/// Builds the reconcile effect, requiring every guard input to be present —
/// a missing pin or squash parent means the durable state cannot support the
/// force-push guard, which is an internal invariant break.
fn reconcile_effect(
    ctx: &Ctx<'_>,
    phase: &CascadePhase,
    branch: &str,
    pin: Option<Sha>,
) -> Result<Effect, CascadeError> {
    let squash_sha = phase
        .squash_sha()
        .cloned()
        .ok_or_else(|| ctx.unexpected("Reconciling without a squash sha".to_string()))?;
    let parent = ctx
        .train
        .last_squash_parent_sha
        .clone()
        .ok_or_else(|| ctx.unexpected("Reconciling without a squash parent".to_string()))?;
    let pin =
        pin.ok_or_else(|| ctx.unexpected("Reconciling without the prepare pin".to_string()))?;
    // The predecessor whose PR ref names the squashed head: the PR that was
    // just squashed, i.e. the train's current PR.
    Ok(Effect::Git(GitEffect::MergeReconcile {
        descendant_branch: branch.to_string(),
        predecessor_pr: ctx.current(),
        squash_sha,
        expected_squash_parent: parent,
        predecessor_pre_squash_head: pin,
        default_branch: ctx.default_branch().to_string(),
    }))
}

fn expect_squash_sha(ctx: &Ctx<'_>) -> Result<Sha, CascadeError> {
    ctx.train
        .cascade_phase
        .squash_sha()
        .cloned()
        .ok_or_else(|| ctx.unexpected("phase carries no squash sha".to_string()))
}

/// A `PhaseTransition` event carrying the train's threading context.
fn phase_transition(
    ctx: &Ctx<'_>,
    phase: CascadePhase,
    last_squash_sha: Option<Sha>,
    last_squash_parent: Option<Sha>,
    predecessor_head_sha: Option<Sha>,
) -> StateEventPayload {
    StateEventPayload::PhaseTransition {
        train_root: ctx.root(),
        current_pr: ctx.current(),
        predecessor_pr: ctx.train.predecessor_pr,
        last_squash_sha,
        last_squash_parent,
        predecessor_head_sha,
        phase,
    }
}

// ─── Terminal / parking plan builders ───

fn park_plan(ctx: &Ctx<'_>, mut events: Vec<StateEventPayload>, reason: String) -> StepPlan {
    let was_running = ctx.train.state == TrainState::Running;
    if was_running {
        events.push(StateEventPayload::TrainParked {
            root_pr: ctx.root(),
            reason: reason.clone(),
        });
    }
    let mut best_effort = Vec::new();
    if was_running {
        let mut record = ctx.train.clone();
        record.wait_for_ci();
        if let Ok(Some(update)) = status_effect(&record, &format!("⏸️ Waiting: {reason}")) {
            best_effort.push(update);
        }
    }
    StepPlan {
        events,
        effects: Vec::new(),
        best_effort,
        control: Control::Park,
    }
}

fn abort_plan(
    ctx: &Ctx<'_>,
    mut events: Vec<StateEventPayload>,
    kind: TrainErrorKind,
    message: String,
) -> StepPlan {
    let error = TrainError::new(kind, message.clone());
    events.push(StateEventPayload::TrainAborted {
        root_pr: ctx.root(),
        error: error.clone(),
    });

    let mut record = ctx.train.clone();
    record.abort(error, ctx.now);

    let mut best_effort = vec![
        Effect::Git(GitEffect::CleanupWorktree),
        comment(
            ctx.current(),
            format!(
                "🛑 Merge train aborted: {message}\n\nFix the issue and re-issue \
                 `@merge-train start` on #{} to retry.",
                ctx.root()
            ),
        ),
    ];
    if let Ok(Some(update)) = status_effect(&record, &format!("🛑 Aborted: {message}")) {
        best_effort.push(update);
    }

    StepPlan {
        events,
        effects: Vec::new(),
        best_effort,
        control: Control::Done,
    }
}

fn abort_internal(ctx: &Ctx<'_>, detail: &str) -> StepPlan {
    abort_plan(
        ctx,
        Vec::new(),
        TrainErrorKind::InternalInvariantViolation,
        format!("internal invariant violated: {detail}. This is a merge-train bug."),
    )
}

/// Aborts because the status comment cannot be formatted: the train must not
/// run without its off-disk state backup. (No status update on this path —
/// it is the thing that failed.)
fn abort_too_large(ctx: &Ctx<'_>) -> StepPlan {
    let error = TrainError::new(
        TrainErrorKind::StatusCommentTooLarge,
        "the status comment exceeds GitHub's size limits even after truncation",
    );
    StepPlan {
        events: vec![StateEventPayload::TrainAborted {
            root_pr: ctx.root(),
            error,
        }],
        effects: Vec::new(),
        best_effort: vec![
            Effect::Git(GitEffect::CleanupWorktree),
            comment(
                ctx.current(),
                "🛑 Merge train aborted: the status comment exceeds GitHub's size \
                 limits, so the train cannot maintain its recovery backup.",
            ),
        ],
        control: Control::Done,
    }
}

/// The status-comment update effect for a record, if the train has a comment.
fn status_effect(
    record: &TrainRecord,
    message: &str,
) -> Result<Option<Effect>, crate::status::format::StatusCommentTooLarge> {
    let Some(comment_id) = record.status_comment_id else {
        return Ok(None);
    };
    let body = format_status_comment(record, message)?;
    Ok(Some(Effect::GitHub(GitHubEffect::UpdateComment {
        comment_id,
        body,
    })))
}

/// Attaches a status-comment update reflecting the plan's events; if the
/// comment cannot be formatted, the whole plan becomes an abort (the plan's
/// non-terminal events are dropped — the train must not advance without its
/// backup).
fn finish_with_status(ctx: &Ctx<'_>, mut plan: StepPlan, message: &str) -> StepPlan {
    let mut projected = ctx.state.clone();
    for payload in &plan.events {
        projected.apply_event(&StateEvent {
            seq: 0,
            ts: ctx.now,
            payload: payload.clone(),
        });
    }
    let Some(record) = projected.active_trains.get(&ctx.root()) else {
        return plan;
    };
    match status_effect(record, message) {
        Ok(Some(update)) => {
            plan.best_effort.push(update);
            plan
        }
        Ok(None) => plan,
        Err(_) => abort_too_large(ctx),
    }
}

/// The root of the active train whose stack involves `pr` (root, current PR,
/// frozen descendant, or transitive descendant of the root).
fn train_containing(state: &RepoState, pr: PrNumber) -> Option<PrNumber> {
    state
        .active_trains
        .values()
        .find(|t| {
            t.state.is_active()
                && (t.original_root_pr == pr
                    || t.current_pr == pr
                    || t.cascade_phase
                        .progress()
                        .is_some_and(|p| p.frozen_descendants().contains(&pr))
                    || collect_all_descendants(t.original_root_pr, &state.descendants, &state.prs)
                        .contains(&pr))
        })
        .map(|t| t.original_root_pr)
}
