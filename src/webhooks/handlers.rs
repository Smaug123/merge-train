//! Pure event handlers (cascade engine stage M3).
//!
//! [`handle_event`] maps a parsed [`GitHubEvent`] + the current [`RepoState`]
//! to a [`HandlerOutput`]: the state events to append, the side effects to run
//! (acks / rejection comments / cache refreshes), and the [`Trigger`]s the
//! worker feeds to the cascade engine (M2). Handlers are **pure and total** —
//! validation failures become rejection-comment effects, never errors, and
//! events for unknown PRs are tolerated by `apply_event`.
//!
//! The engine triggers ([`Trigger`]) are *emitted* here — including
//! `EvaluateTrain` (an observation that should re-drive an active/parked train)
//! and `LateAddition` (a predecessor declared on an already-merged PR). M2
//! consumes them once it exists; nothing depends on them landing first.
//!
//! # Preconditions the worker (M5) establishes — not these pure handlers
//!
//! - **Dedupe**: handlers assume deduped input.
//! - **Referenced PRs are cached**: a command/observation for a PR the bot has
//!   never seen needs a `GetPr` crawl first. M5 bootstraps unknown PRs before
//!   invoking the handler (and re-processes the original delivery once cached);
//!   the `GetPr` effect a handler may emit is a cold-start fallback, not the
//!   primary path. So a `predecessor` command is validated against a cached PR,
//!   not silently dropped.
//! - **Command authorization**: a command reaching command handling is assumed
//!   already authorized. DESIGN §Command authorization gates commands on the
//!   commenter's identity — `predecessor`/`start` require the PR author,
//!   `stop` the author or a repo admin/maintainer, `stop --force` an admin —
//!   which needs a GitHub `…/collaborators/{user}/permission` query and the PR
//!   author's id, i.e. **effects a pure function cannot perform**. So M5
//!   authorizes a parsed command (rejecting an unauthorized commenter with a
//!   comment) *before* invoking the command path here. The one purely-checkable
//!   rule — the comment must be on a PR, not an issue — *is* enforced here
//!   (a non-PR comment yields no output).

use chrono::{DateTime, Utc};

use crate::commands::{Command, parse_command};
use crate::effects::Effect;
use crate::effects::github::{GitHubEffect, Reaction};
use crate::persistence::event::StateEventPayload;
use crate::state::validation::validate_predecessor_update;
use crate::state::{PredecessorValidationError, RepoState, validate_predecessor_declaration};
use crate::types::{CommentId, PrNumber, PrState, Sha, TrainError, TrainErrorKind};
use crate::webhooks::events::{
    CheckSuiteAction, CommentAction, GitHubEvent, IssueCommentEvent, MergeStatus, PrAction,
    PullRequestEvent, PullRequestReviewEvent, ReviewAction,
};

/// Ambient context a handler needs but the event doesn't carry.
#[derive(Debug, Clone)]
pub struct HandlerCtx {
    /// The bot's GitHub user id — used to ignore the bot's own comments.
    pub bot_user_id: u64,
    /// The bot's mention name (without `@`), e.g. `"merge-train"`.
    pub bot_name: String,
    /// The shell-supplied wall clock (handlers never read the clock themselves).
    pub now: DateTime<Utc>,
}

/// A request the worker routes to the cascade engine (M2). Handlers never run
/// cascades themselves; they only decide *that* one should be (re-)evaluated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Trigger {
    /// `@bot start` on `pr`.
    StartTrain { pr: PrNumber },
    /// `@bot stop[ --force]` on `pr`.
    StopTrain { pr: PrNumber, force: bool },
    /// An observation (CI/status/review/ready/merge) arrived for a PR in this
    /// active train; the engine should re-evaluate it (e.g. resume a `WaitingCi`
    /// train). M2 consumes this once its parking model exists.
    EvaluateTrain { root: PrNumber },
    /// A `predecessor` declaration targets an **already-merged** PR — a "late
    /// addition" that needs its own (squash-vs-rebase) flow. M3 only detects it;
    /// M5 responds (currently: an explicit "not yet supported" comment).
    LateAddition {
        pr: PrNumber,
        merged_predecessor: PrNumber,
    },
}

/// Everything a handler produces from one event.
#[derive(Debug, Default)]
pub struct HandlerOutput {
    /// State events to append (then apply) in order.
    pub events: Vec<StateEventPayload>,
    /// Side effects to run (acks, rejection comments, cache refreshes).
    pub effects: Vec<Effect>,
    /// Engine triggers for the worker to route to M2.
    pub triggers: Vec<Trigger>,
}

impl HandlerOutput {
    fn event(payload: StateEventPayload) -> Self {
        HandlerOutput {
            events: vec![payload],
            ..Default::default()
        }
    }
}

/// Handles one parsed webhook event against the current state.
pub fn handle_event(event: &GitHubEvent, state: &RepoState, ctx: &HandlerCtx) -> HandlerOutput {
    match event {
        GitHubEvent::IssueComment(e) => handle_issue_comment(e, state, ctx),
        GitHubEvent::PullRequest(e) => handle_pull_request(e, state),
        // Only a *completed* suite carries a conclusion the engine can act on;
        // created/(re)requested suites are not "completed" and must not be
        // recorded as such (that would falsely re-trigger CI re-evaluation).
        GitHubEvent::CheckSuite(e) => match e.action {
            CheckSuiteAction::Completed => {
                let mut out = HandlerOutput::event(StateEventPayload::CheckSuiteCompleted {
                    sha: e.head_sha.clone(),
                    conclusion: e
                        .conclusion
                        .as_ref()
                        .map(canonical)
                        .unwrap_or_else(|| "none".to_owned()),
                });
                out.triggers = evaluate_for_sha(state, &e.head_sha);
                out
            }
            _ => HandlerOutput::default(),
        },
        GitHubEvent::Status(e) => {
            let mut out = HandlerOutput::event(StateEventPayload::StatusReceived {
                sha: e.sha.clone(),
                context: e.context.clone(),
                state: canonical(&e.state),
            });
            out.triggers = evaluate_for_sha(state, &e.sha);
            out
        }
        GitHubEvent::PullRequestReview(e) => handle_review(e, state),
    }
}

// ─── issue_comment: the command surface ───

fn handle_issue_comment(
    event: &IssueCommentEvent,
    state: &RepoState,
    ctx: &HandlerCtx,
) -> HandlerOutput {
    // The bot's own comments (e.g. status updates) are never commands.
    if event.author_id == ctx.bot_user_id {
        return HandlerOutput::default();
    }

    // Commands only make sense on a PR.
    let Some(pr) = event.pr_number else {
        return HandlerOutput::default();
    };

    match event.action {
        // A deleted comment that declared a predecessor retracts the declaration.
        CommentAction::Deleted => match find_pr_by_predecessor_comment(state, event.comment_id) {
            Some(declaring_pr) => {
                let mut out = HandlerOutput::event(StateEventPayload::PredecessorRemoved {
                    pr: declaring_pr,
                    comment_id: event.comment_id,
                });
                out.events
                    .extend(topology_change_abort(state, declaring_pr));
                out
            }
            None => HandlerOutput::default(),
        },
        // Created or edited: re-interpret the body, honoring a predecessor
        // declaration this very comment may already own (Codex review #55).
        CommentAction::Created | CommentAction::Edited => {
            handle_comment_command(pr, event.comment_id, &event.body, state, &ctx.bot_name)
        }
    }
}

/// Interprets a created/edited comment's body as a command. **Assumes the
/// command (if any) is already authorized** — see the module docs: M5 enforces
/// DESIGN §Command authorization before calling here. An *edit* to the comment
/// that currently owns `pr`'s predecessor declaration is handled specially:
/// editing the declaration away retracts it, and editing it to a different
/// predecessor updates it (rather than rejecting as already-declared).
fn handle_comment_command(
    pr: PrNumber,
    comment_id: CommentId,
    body: &str,
    state: &RepoState,
    bot_name: &str,
) -> HandlerOutput {
    // Does this comment currently own pr's predecessor declaration?
    let declared = state
        .prs
        .get(&pr)
        .is_some_and(|c| c.predecessor_comment_id == Some(comment_id));

    let command = parse_command(body, bot_name);

    if let Some(Command::Predecessor(predecessor)) = command {
        return handle_predecessor_command(pr, predecessor, comment_id, declared, state);
    }

    // Any non-predecessor outcome: if this comment had declared a predecessor,
    // the edit no longer does, so retract it first (and abort any active train
    // whose stack this changes).
    let mut out = if declared {
        let mut o = HandlerOutput::event(StateEventPayload::PredecessorRemoved { pr, comment_id });
        o.events.extend(topology_change_abort(state, pr));
        o
    } else {
        HandlerOutput::default()
    };
    match command {
        Some(Command::Start) => {
            out.effects.push(ack(comment_id));
            out.triggers.push(Trigger::StartTrain { pr });
        }
        Some(Command::Stop) => {
            out.effects.push(ack(comment_id));
            out.triggers.push(Trigger::StopTrain { pr, force: false });
        }
        Some(Command::StopForce) => {
            out.effects.push(ack(comment_id));
            out.triggers.push(Trigger::StopTrain { pr, force: true });
        }
        Some(Command::Predecessor(_)) => unreachable!("handled above"),
        None => {}
    }
    out
}

fn handle_predecessor_command(
    pr: PrNumber,
    predecessor: PrNumber,
    comment_id: CommentId,
    declared_by_this_comment: bool,
    state: &RepoState,
) -> HandlerOutput {
    // We can only validate a predecessor declaration once the PR is cached. If
    // it isn't, refresh it; the redelivered/re-evaluated command then validates.
    let Some(cached) = state.prs.get(&pr) else {
        return HandlerOutput {
            events: vec![],
            effects: vec![Effect::GitHub(GitHubEffect::GetPr { pr })],
            triggers: vec![],
        };
    };

    // Idempotent re-statement of the same declaration.
    if declared_by_this_comment && cached.predecessor == Some(predecessor) {
        return HandlerOutput::default();
    }

    // Late addition: declaring a predecessor that has *already merged* is a
    // distinct (squash-vs-rebase) flow M5 handles; M3 only detects it and hands
    // it off, so the user's command isn't silently rejected.
    if let Some(pred) = state.prs.get(&predecessor)
        && matches!(pred.state, PrState::Merged { .. })
    {
        return HandlerOutput {
            events: vec![],
            effects: vec![],
            triggers: vec![Trigger::LateAddition {
                pr,
                merged_predecessor: predecessor,
            }],
        };
    }

    // If this comment already owns the declaration, an edit *updates* it (the
    // update validator tolerates an existing predecessor). Otherwise it's a
    // fresh declaration, which rejects if a predecessor already exists — first
    // declaration wins.
    let validation = if declared_by_this_comment {
        validate_predecessor_update(cached, predecessor, &state.prs, &state.default_branch)
    } else {
        validate_predecessor_declaration(cached, predecessor, &state.prs, &state.default_branch)
    };

    match validation {
        Ok(()) => {
            let mut out = HandlerOutput {
                events: vec![StateEventPayload::PredecessorDeclared {
                    pr,
                    predecessor,
                    comment_id,
                }],
                effects: vec![ack(comment_id)],
                triggers: vec![],
            };
            // Changing the stack under an active train is unsafe to cascade.
            out.events.extend(topology_change_abort(state, pr));
            out
        }
        Err(e) => HandlerOutput {
            events: vec![],
            effects: vec![reject(pr, &predecessor_rejection_message(&e))],
            triggers: vec![],
        },
    }
}

// ─── pull_request: state materialization ───

fn handle_pull_request(event: &PullRequestEvent, state: &RepoState) -> HandlerOutput {
    let pr = event.pr_number;
    // An edit can also break a stack (a retarget away from the predecessor's
    // head), so it produces an effect as well as an event — handled separately.
    if let PrAction::Edited = event.action {
        return handle_pr_edited(pr, &event.base_branch, state);
    }

    let payload = match event.action {
        PrAction::Opened => StateEventPayload::PrOpened {
            pr,
            head_sha: event.head_sha.clone(),
            head_ref: event.head_branch.clone(),
            base_ref: event.base_branch.clone(),
            is_draft: event.is_draft,
        },
        PrAction::Closed => match &event.merge_status {
            MergeStatus::Merged { merge_commit_sha } => StateEventPayload::PrMerged {
                pr,
                merge_sha: merge_commit_sha.clone(),
            },
            MergeStatus::NotMerged => StateEventPayload::PrClosed { pr },
        },
        PrAction::Reopened => StateEventPayload::PrReopened { pr },
        PrAction::Synchronize => StateEventPayload::PrSynchronized {
            pr,
            new_head_sha: event.head_sha.clone(),
        },
        PrAction::ConvertedToDraft => StateEventPayload::PrConvertedToDraft { pr },
        PrAction::ReadyForReview => StateEventPayload::PrReadyForReview { pr },
        PrAction::Edited => unreachable!("handled above"),
    };

    let mut out = HandlerOutput::event(payload);
    // A merge or a draft becoming ready may let an active train make progress.
    let may_unblock = match event.action {
        PrAction::ReadyForReview => true,
        PrAction::Closed => matches!(event.merge_status, MergeStatus::Merged { .. }),
        _ => false,
    };
    if may_unblock {
        out.triggers.extend(evaluate_for_pr(state, pr));
    }
    out
}

/// A PR edit matters only when it changes the base branch (and we know the old
/// base — `apply_event` would skip an unknown PR anyway). A retarget that breaks
/// the stack (the new base no longer matches an *open* predecessor's head) also
/// gets a loud warning comment so it's fixed before a cascade trips on it
/// (Codex review #55).
fn handle_pr_edited(pr: PrNumber, new_base: &str, state: &RepoState) -> HandlerOutput {
    let Some(cached) = state.prs.get(&pr) else {
        return HandlerOutput::default();
    };
    if cached.base_ref == new_base {
        return HandlerOutput::default();
    }

    let mut out = HandlerOutput::event(StateEventPayload::PrBaseChanged {
        pr,
        old_base: cached.base_ref.clone(),
        new_base: new_base.to_owned(),
    });

    if let Some(pred_num) = cached.predecessor
        && let Some(pred) = state.prs.get(&pred_num)
        && pred.state == PrState::Open
        && pred.head_ref != new_base
    {
        out.effects.push(reject(
            pr,
            &format!(
                "Heads up: retargeting PR #{pr} to `{new_base}` no longer matches its \
                 predecessor #{pred_num}'s head `{}`, so they are no longer stacked. \
                 Re-declare the predecessor or restart the train.",
                pred.head_ref
            ),
        ));
    }
    out
}

// ─── pull_request_review ───

fn handle_review(event: &PullRequestReviewEvent, state: &RepoState) -> HandlerOutput {
    match event.action {
        ReviewAction::Submitted => {
            let mut out = HandlerOutput::event(StateEventPayload::ReviewSubmitted {
                pr: event.pr_number,
                reviewer: event.reviewer_login.clone(),
                state: canonical(&event.state),
            });
            // An approval may unblock a parked train waiting on review.
            out.triggers.extend(evaluate_for_pr(state, event.pr_number));
            out
        }
        ReviewAction::Dismissed => {
            let mut out = HandlerOutput::event(StateEventPayload::ReviewDismissed {
                pr: event.pr_number,
                reviewer: event.reviewer_login.clone(),
            });
            // A dismissed approval on an active train is unrecoverable without
            // re-approval, so the train aborts immediately (DESIGN: "Exception —
            // review dismissal events").
            if let Some(root) = active_train_root_for(state, event.pr_number) {
                out.events.push(StateEventPayload::TrainAborted {
                    root_pr: root,
                    error: TrainError::new(
                        TrainErrorKind::ReviewDismissed,
                        format!(
                            "review on PR #{} was dismissed; the train cannot continue without \
                             re-approval",
                            event.pr_number
                        ),
                    ),
                });
            }
            out
        }
        // An edited review body changes nothing the bot tracks.
        ReviewAction::Edited => HandlerOutput::default(),
    }
}

// ─── helpers ───

/// The canonical (serde, snake_case) wire value of a webhook state enum, so the
/// persisted event strings match the rest of the model rather than Rust `Debug`
/// names like `Success` / `Other(..)` (Codex review #55). These enums always
/// serialize to a JSON string.
fn canonical<T: serde::Serialize>(value: &T) -> String {
    match serde_json::to_value(value) {
        Ok(serde_json::Value::String(s)) => s,
        other => format!("{other:?}"),
    }
}

/// If an active train involves `pr`, the event that aborts it because its stack
/// topology changed. A predecessor change/removal mid-train is unsafe to
/// cascade — the frozen descendant set no longer reflects the declared stack
/// (DESIGN; Codex review #55).
fn topology_change_abort(state: &RepoState, pr: PrNumber) -> Option<StateEventPayload> {
    active_train_root_for(state, pr).map(|root| StateEventPayload::TrainAborted {
        root_pr: root,
        error: TrainError::new(
            TrainErrorKind::PredecessorChanged,
            format!("PR #{pr}'s predecessor relationship changed while a train was active"),
        ),
    })
}

/// An `EvaluateTrain` trigger for the active train involving `pr`, if any.
fn evaluate_for_pr(state: &RepoState, pr: PrNumber) -> Option<Trigger> {
    active_train_root_for(state, pr).map(|root| Trigger::EvaluateTrain { root })
}

/// `EvaluateTrain` triggers for every active train whose PR currently has head
/// `sha` — CI/status observations land on a commit, not a PR. Roots are
/// deduplicated and ordered for determinism.
fn evaluate_for_sha(state: &RepoState, sha: &Sha) -> Vec<Trigger> {
    let mut roots: Vec<PrNumber> = state
        .prs
        .values()
        .filter(|pr| pr.head_sha == *sha)
        .filter_map(|pr| active_train_root_for(state, pr.number))
        .collect();
    roots.sort_unstable_by_key(|p| p.0);
    roots.dedup();
    roots
        .into_iter()
        .map(|root| Trigger::EvaluateTrain { root })
        .collect()
}

fn ack(comment_id: CommentId) -> Effect {
    Effect::GitHub(GitHubEffect::AddReaction {
        comment_id,
        reaction: Reaction::ThumbsUp,
    })
}

fn reject(pr: PrNumber, body: &str) -> Effect {
    Effect::GitHub(GitHubEffect::PostComment {
        pr,
        body: body.to_owned(),
    })
}

/// The PR whose predecessor declaration came from `comment_id`, if any.
fn find_pr_by_predecessor_comment(state: &RepoState, comment_id: CommentId) -> Option<PrNumber> {
    state
        .prs
        .values()
        .find(|pr| pr.predecessor_comment_id == Some(comment_id))
        .map(|pr| pr.number)
}

/// The root of an *active* train that `pr` participates in — as the root, the
/// current PR, or a frozen descendant being cascaded — if any. Terminal trains
/// (stopped/aborted) still linger in `active_trains` but are excluded, so a late
/// review dismissal can't re-abort one (Codex review #55).
fn active_train_root_for(state: &RepoState, pr: PrNumber) -> Option<PrNumber> {
    state
        .active_trains
        .values()
        .find(|t| {
            t.state.is_active()
                && (t.original_root_pr == pr
                    || t.current_pr == pr
                    || t.cascade_phase
                        .progress()
                        .is_some_and(|p| p.frozen_descendants().contains(&pr)))
        })
        .map(|t| t.original_root_pr)
}

fn predecessor_rejection_message(error: &PredecessorValidationError) -> String {
    use PredecessorValidationError::*;
    match error {
        PredecessorNotFound { predecessor } => {
            format!("Cannot declare predecessor: PR #{predecessor} was not found.")
        }
        PredecessorNotInStack {
            predecessor,
            predecessor_base,
            default_branch,
        } => format!(
            "Cannot declare predecessor: PR #{predecessor} targets `{predecessor_base}`, which is \
             neither `{default_branch}` nor itself a stacked PR."
        ),
        BaseBranchMismatch {
            pr,
            pr_base,
            predecessor,
            predecessor_head,
        } => format!(
            "Cannot declare predecessor: PR #{pr}'s base `{pr_base}` does not match PR \
             #{predecessor}'s head `{predecessor_head}`, so it is not actually stacked on it."
        ),
        PredecessorClosed { predecessor } => {
            format!("Cannot declare predecessor: PR #{predecessor} is closed.")
        }
        CycleDetected { cycle } => {
            let path = cycle
                .iter()
                .map(|p| format!("#{p}"))
                .collect::<Vec<_>>()
                .join(" → ");
            format!("Cannot declare predecessor: this would create a cycle ({path}).")
        }
        AlreadyHasPredecessor {
            pr,
            existing_predecessor,
        } => format!("PR #{pr} already has predecessor #{existing_predecessor}."),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::event::StateEvent;
    use crate::persistence::snapshot::PersistedRepoSnapshot;
    use crate::types::{CachedPr, MergeStateStatus, PrState, RepoId, Sha, TrainRecord};

    // ─── builders ───

    fn ts() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn ctx() -> HandlerCtx {
        HandlerCtx {
            bot_user_id: 999,
            bot_name: "merge-train".to_owned(),
            now: ts(),
        }
    }

    fn sha() -> Sha {
        Sha::parse("a".repeat(40)).unwrap()
    }

    fn repo() -> RepoId {
        RepoId::new("octocat", "hello")
    }

    fn open_pr(number: u64, base: &str, predecessor: Option<u64>) -> CachedPr {
        CachedPr::new(
            PrNumber(number),
            sha(),
            format!("feature-{number}"),
            base.to_owned(),
            predecessor.map(PrNumber),
            PrState::Open,
            MergeStateStatus::Unknown,
            false,
        )
    }

    fn state_with(prs: Vec<CachedPr>) -> RepoState {
        let mut snap = PersistedRepoSnapshot::new("main");
        for pr in prs {
            snap.prs.insert(pr.number, pr);
        }
        RepoState::from_snapshot(snap)
    }

    fn state_with_train(prs: Vec<CachedPr>, root: u64) -> RepoState {
        let mut snap = PersistedRepoSnapshot::new("main");
        for pr in prs {
            snap.prs.insert(pr.number, pr);
        }
        snap.active_trains
            .insert(PrNumber(root), TrainRecord::new(PrNumber(root), ts()));
        RepoState::from_snapshot(snap)
    }

    fn comment(
        action: CommentAction,
        pr_number: Option<u64>,
        body: &str,
        author: u64,
    ) -> GitHubEvent {
        GitHubEvent::IssueComment(IssueCommentEvent {
            repo: repo(),
            action,
            pr_number: pr_number.map(PrNumber),
            comment_id: CommentId(7),
            body: body.to_owned(),
            author_id: author,
            author_login: "alice".to_owned(),
        })
    }

    fn pr_event(action: PrAction, number: u64, base: &str, merge: MergeStatus) -> GitHubEvent {
        GitHubEvent::PullRequest(PullRequestEvent {
            repo: repo(),
            action,
            pr_number: PrNumber(number),
            merge_status: merge,
            head_sha: sha(),
            base_branch: base.to_owned(),
            head_branch: format!("feature-{number}"),
            is_draft: false,
            author_id: 1,
        })
    }

    fn review(action: ReviewAction, pr: u64) -> GitHubEvent {
        GitHubEvent::PullRequestReview(PullRequestReviewEvent {
            repo: repo(),
            action,
            pr_number: PrNumber(pr),
            state: crate::webhooks::events::ReviewState::Approved,
            reviewer_id: 1,
            reviewer_login: "bob".to_owned(),
            body: String::new(),
        })
    }

    /// Applies a handler's events through `apply_event` — the M3 oracle that
    /// every emitted event is well-formed for the state machine (total, no panic).
    fn apply(state: &mut RepoState, out: &HandlerOutput) {
        for (i, payload) in out.events.iter().enumerate() {
            state.apply_event(&StateEvent {
                seq: i as u64,
                ts: ts(),
                payload: payload.clone(),
            });
        }
    }

    fn effects_debug(out: &HandlerOutput) -> Vec<String> {
        out.effects.iter().map(|e| format!("{e:?}")).collect()
    }

    // ─── issue_comment / commands ───

    #[test]
    fn start_command_triggers_start_and_acks() {
        let state = state_with(vec![]);
        let out = handle_event(
            &comment(CommentAction::Created, Some(5), "@merge-train start", 1),
            &state,
            &ctx(),
        );
        assert_eq!(out.triggers, vec![Trigger::StartTrain { pr: PrNumber(5) }]);
        assert!(matches!(
            out.effects.as_slice(),
            [Effect::GitHub(GitHubEffect::AddReaction {
                reaction: Reaction::ThumbsUp,
                ..
            })]
        ));
        assert!(out.events.is_empty());
    }

    #[test]
    fn stop_commands_trigger_stop_with_force_flag() {
        let state = state_with(vec![]);
        let plain = handle_event(
            &comment(CommentAction::Created, Some(5), "@merge-train stop", 1),
            &state,
            &ctx(),
        );
        assert_eq!(
            plain.triggers,
            vec![Trigger::StopTrain {
                pr: PrNumber(5),
                force: false
            }]
        );
        let forced = handle_event(
            &comment(
                CommentAction::Created,
                Some(5),
                "@merge-train stop --force",
                1,
            ),
            &state,
            &ctx(),
        );
        assert_eq!(
            forced.triggers,
            vec![Trigger::StopTrain {
                pr: PrNumber(5),
                force: true
            }]
        );
    }

    #[test]
    fn valid_predecessor_declares_and_acks() {
        // PR #2 (base = #1's head) declares predecessor #1 (targets main).
        let pred = open_pr(1, "main", None);
        let pr2 = {
            let mut p = open_pr(2, "feature-1", None);
            p.base_ref = pred.head_ref.clone();
            p
        };
        let mut state = state_with(vec![pred, pr2]);

        let out = handle_event(
            &comment(
                CommentAction::Created,
                Some(2),
                "@merge-train predecessor #1",
                1,
            ),
            &state,
            &ctx(),
        );
        assert_eq!(
            out.events,
            vec![StateEventPayload::PredecessorDeclared {
                pr: PrNumber(2),
                predecessor: PrNumber(1),
                comment_id: CommentId(7),
            }]
        );
        assert!(matches!(
            out.effects.as_slice(),
            [Effect::GitHub(GitHubEffect::AddReaction { .. })]
        ));
        apply(&mut state, &out); // applies cleanly
        assert_eq!(state.prs[&PrNumber(2)].predecessor, Some(PrNumber(1)));
    }

    #[test]
    fn invalid_predecessor_rejects_with_comment() {
        // Predecessor #1 does not exist.
        let pr2 = open_pr(2, "feature-1", None);
        let state = state_with(vec![pr2]);
        let out = handle_event(
            &comment(
                CommentAction::Created,
                Some(2),
                "@merge-train predecessor #1",
                1,
            ),
            &state,
            &ctx(),
        );
        assert!(out.events.is_empty());
        assert!(matches!(
            out.effects.as_slice(),
            [Effect::GitHub(GitHubEffect::PostComment {
                pr: PrNumber(2),
                ..
            })]
        ));
    }

    #[test]
    fn predecessor_on_unknown_pr_refreshes() {
        let state = state_with(vec![]);
        let out = handle_event(
            &comment(
                CommentAction::Created,
                Some(2),
                "@merge-train predecessor #1",
                1,
            ),
            &state,
            &ctx(),
        );
        assert!(out.events.is_empty());
        assert!(matches!(
            out.effects.as_slice(),
            [Effect::GitHub(GitHubEffect::GetPr { pr: PrNumber(2) })]
        ));
    }

    #[test]
    fn bot_own_comment_is_ignored() {
        let state = state_with(vec![]);
        let out = handle_event(
            &comment(CommentAction::Created, Some(5), "@merge-train start", 999),
            &state,
            &ctx(),
        );
        assert!(out.events.is_empty() && out.effects.is_empty() && out.triggers.is_empty());
    }

    #[test]
    fn non_command_and_issue_comments_are_ignored() {
        let state = state_with(vec![]);
        let chat = handle_event(
            &comment(CommentAction::Created, Some(5), "lgtm, nice work", 1),
            &state,
            &ctx(),
        );
        assert!(chat.events.is_empty() && chat.effects.is_empty() && chat.triggers.is_empty());

        let on_issue = handle_event(
            &comment(CommentAction::Created, None, "@merge-train start", 1),
            &state,
            &ctx(),
        );
        assert!(on_issue.triggers.is_empty() && on_issue.effects.is_empty());
    }

    #[test]
    fn deleted_predecessor_comment_removes_declaration() {
        let mut pr2 = open_pr(2, "main", Some(1));
        pr2.predecessor_comment_id = Some(CommentId(7));
        let state = state_with(vec![open_pr(1, "main", None), pr2]);
        let out = handle_event(
            &comment(CommentAction::Deleted, Some(2), "", 1),
            &state,
            &ctx(),
        );
        assert_eq!(
            out.events,
            vec![StateEventPayload::PredecessorRemoved {
                pr: PrNumber(2),
                comment_id: CommentId(7),
            }]
        );
    }

    // ─── pull_request ───

    #[test]
    fn pr_lifecycle_events() {
        let mut state = state_with(vec![]);

        let opened = handle_event(
            &pr_event(PrAction::Opened, 3, "main", MergeStatus::NotMerged),
            &state,
            &ctx(),
        );
        assert!(matches!(
            opened.events.as_slice(),
            [StateEventPayload::PrOpened {
                pr: PrNumber(3),
                ..
            }]
        ));

        let merged = handle_event(
            &pr_event(
                PrAction::Closed,
                3,
                "main",
                MergeStatus::Merged {
                    merge_commit_sha: sha(),
                },
            ),
            &state,
            &ctx(),
        );
        assert!(matches!(
            merged.events.as_slice(),
            [StateEventPayload::PrMerged {
                pr: PrNumber(3),
                ..
            }]
        ));

        let closed = handle_event(
            &pr_event(PrAction::Closed, 3, "main", MergeStatus::NotMerged),
            &state,
            &ctx(),
        );
        assert_eq!(
            closed.events,
            vec![StateEventPayload::PrClosed { pr: PrNumber(3) }]
        );

        let synced = handle_event(
            &pr_event(PrAction::Synchronize, 3, "main", MergeStatus::NotMerged),
            &state,
            &ctx(),
        );
        assert!(matches!(
            synced.events.as_slice(),
            [StateEventPayload::PrSynchronized {
                pr: PrNumber(3),
                ..
            }]
        ));

        // Each applies cleanly.
        for out in [&opened, &merged, &closed, &synced] {
            apply(&mut state, out);
        }
    }

    #[test]
    fn pr_edited_base_change_emits_event_else_noop() {
        let state = state_with(vec![open_pr(4, "main", None)]);
        let changed = handle_event(
            &pr_event(PrAction::Edited, 4, "develop", MergeStatus::NotMerged),
            &state,
            &ctx(),
        );
        assert_eq!(
            changed.events,
            vec![StateEventPayload::PrBaseChanged {
                pr: PrNumber(4),
                old_base: "main".to_owned(),
                new_base: "develop".to_owned(),
            }]
        );

        let unchanged = handle_event(
            &pr_event(PrAction::Edited, 4, "main", MergeStatus::NotMerged),
            &state,
            &ctx(),
        );
        assert!(unchanged.events.is_empty());
    }

    // ─── review / observations ───

    #[test]
    fn review_dismissed_on_active_train_aborts() {
        let state = state_with_train(vec![open_pr(1, "main", None)], 1);
        let out = handle_event(&review(ReviewAction::Dismissed, 1), &state, &ctx());
        assert!(matches!(
            out.events.as_slice(),
            [
                StateEventPayload::ReviewDismissed {
                    pr: PrNumber(1),
                    ..
                },
                StateEventPayload::TrainAborted {
                    root_pr: PrNumber(1),
                    ..
                }
            ]
        ));
    }

    #[test]
    fn review_dismissed_without_train_only_records() {
        let state = state_with(vec![open_pr(1, "main", None)]);
        let out = handle_event(&review(ReviewAction::Dismissed, 1), &state, &ctx());
        assert!(matches!(
            out.events.as_slice(),
            [StateEventPayload::ReviewDismissed {
                pr: PrNumber(1),
                ..
            }]
        ));
    }

    #[test]
    fn observation_events_are_recorded() {
        let state = state_with(vec![]);
        let submitted = handle_event(&review(ReviewAction::Submitted, 1), &state, &ctx());
        assert!(matches!(
            submitted.events.as_slice(),
            [StateEventPayload::ReviewSubmitted { .. }]
        ));

        let status = handle_event(
            &GitHubEvent::Status(crate::webhooks::events::StatusEvent {
                repo: repo(),
                sha: sha(),
                state: crate::webhooks::events::StatusState::Success,
                context: "ci/test".to_owned(),
                description: None,
                target_url: None,
            }),
            &state,
            &ctx(),
        );
        assert!(matches!(
            status.events.as_slice(),
            [StateEventPayload::StatusReceived { .. }]
        ));
    }

    #[test]
    fn handlers_are_deterministic() {
        let pred = open_pr(1, "main", None);
        let mut pr2 = open_pr(2, "main", None);
        pr2.base_ref = pred.head_ref.clone();
        let state = state_with(vec![pred, pr2]);
        let event = comment(
            CommentAction::Created,
            Some(2),
            "@merge-train predecessor #1",
            1,
        );

        let a = handle_event(&event, &state, &ctx());
        let b = handle_event(&event, &state, &ctx());
        assert_eq!(a.events, b.events);
        assert_eq!(a.triggers, b.triggers);
        assert_eq!(effects_debug(&a), effects_debug(&b));
    }

    // ─── Codex review #55 fixes ───

    #[test]
    fn editing_a_predecessor_comment_retracts_or_updates() {
        // PR #2 already declares predecessor #1 via comment 7; its base matches
        // #3's head so an update to #3 is valid.
        let mut pr2 = open_pr(2, "feature-3", Some(1));
        pr2.predecessor_comment_id = Some(CommentId(7));
        let state = state_with(vec![
            open_pr(1, "main", None),
            open_pr(3, "main", None),
            pr2,
        ]);

        // Edit drops the command → retract the declaration.
        let retract = handle_event(
            &comment(CommentAction::Edited, Some(2), "oops, never mind", 1),
            &state,
            &ctx(),
        );
        assert_eq!(
            retract.events,
            vec![StateEventPayload::PredecessorRemoved {
                pr: PrNumber(2),
                comment_id: CommentId(7),
            }]
        );

        // Edit changes the predecessor → update (not an AlreadyHasPredecessor reject).
        let updated = handle_event(
            &comment(
                CommentAction::Edited,
                Some(2),
                "@merge-train predecessor #3",
                1,
            ),
            &state,
            &ctx(),
        );
        assert_eq!(
            updated.events,
            vec![StateEventPayload::PredecessorDeclared {
                pr: PrNumber(2),
                predecessor: PrNumber(3),
                comment_id: CommentId(7),
            }]
        );
    }

    #[test]
    fn check_suite_only_records_completed() {
        use crate::webhooks::events::{CheckSuiteConclusion, CheckSuiteEvent};
        let state = state_with(vec![]);
        let mk = |action| {
            GitHubEvent::CheckSuite(CheckSuiteEvent {
                repo: repo(),
                action,
                head_sha: sha(),
                conclusion: Some(CheckSuiteConclusion::Success),
                pull_requests: vec![],
            })
        };
        assert!(
            handle_event(&mk(CheckSuiteAction::Created), &state, &ctx())
                .events
                .is_empty()
        );
        assert!(matches!(
            handle_event(&mk(CheckSuiteAction::Completed), &state, &ctx())
                .events
                .as_slice(),
            [StateEventPayload::CheckSuiteCompleted { .. }]
        ));
    }

    #[test]
    fn review_dismissed_on_frozen_descendant_aborts() {
        use crate::types::{CascadePhase, DescendantProgress, TrainState};
        let mut train = TrainRecord::new(PrNumber(1), ts());
        train.state = TrainState::Running;
        train.cascade_phase = CascadePhase::Preparing {
            progress: DescendantProgress::new(vec![PrNumber(2)]),
        };
        let mut snap = PersistedRepoSnapshot::new("main");
        snap.prs.insert(PrNumber(1), open_pr(1, "main", None));
        snap.prs.insert(PrNumber(2), open_pr(2, "main", Some(1)));
        snap.active_trains.insert(PrNumber(1), train);
        let state = RepoState::from_snapshot(snap);

        let out = handle_event(&review(ReviewAction::Dismissed, 2), &state, &ctx());
        assert!(out.events.iter().any(|e| matches!(
            e,
            StateEventPayload::TrainAborted {
                root_pr: PrNumber(1),
                ..
            }
        )));
    }

    #[test]
    fn review_dismissed_on_terminal_train_does_not_abort() {
        use crate::types::TrainState;
        let mut train = TrainRecord::new(PrNumber(1), ts());
        train.state = TrainState::Stopped { ended_at: ts() };
        let mut snap = PersistedRepoSnapshot::new("main");
        snap.prs.insert(PrNumber(1), open_pr(1, "main", None));
        snap.active_trains.insert(PrNumber(1), train);
        let state = RepoState::from_snapshot(snap);

        let out = handle_event(&review(ReviewAction::Dismissed, 1), &state, &ctx());
        assert!(
            !out.events
                .iter()
                .any(|e| matches!(e, StateEventPayload::TrainAborted { .. }))
        );
    }

    #[test]
    fn retarget_breaking_the_stack_warns() {
        // PR #2 is stacked on #1 (base = #1's head); retargeting it to main breaks
        // the stack, so a warning comment accompanies the base-change event.
        let pr2 = open_pr(2, "feature-1", Some(1));
        let state = state_with(vec![open_pr(1, "main", None), pr2]);
        let out = handle_event(
            &pr_event(PrAction::Edited, 2, "main", MergeStatus::NotMerged),
            &state,
            &ctx(),
        );
        assert!(matches!(
            out.events.as_slice(),
            [StateEventPayload::PrBaseChanged { .. }]
        ));
        assert!(matches!(
            out.effects.as_slice(),
            [Effect::GitHub(GitHubEffect::PostComment {
                pr: PrNumber(2),
                ..
            })]
        ));
    }

    #[test]
    fn webhook_states_are_canonical_not_debug() {
        use crate::webhooks::events::{
            CheckSuiteConclusion, CheckSuiteEvent, StatusEvent, StatusState,
        };
        let state = state_with(vec![]);

        let status = handle_event(
            &GitHubEvent::Status(StatusEvent {
                repo: repo(),
                sha: sha(),
                state: StatusState::Success,
                context: "ci".to_owned(),
                description: None,
                target_url: None,
            }),
            &state,
            &ctx(),
        );
        match status.events.as_slice() {
            [StateEventPayload::StatusReceived { state, .. }] => {
                assert_eq!(state.as_str(), "success")
            }
            other => panic!("{other:?}"),
        }

        let cs = handle_event(
            &GitHubEvent::CheckSuite(CheckSuiteEvent {
                repo: repo(),
                action: CheckSuiteAction::Completed,
                head_sha: sha(),
                conclusion: Some(CheckSuiteConclusion::TimedOut),
                pull_requests: vec![],
            }),
            &state,
            &ctx(),
        );
        match cs.events.as_slice() {
            [StateEventPayload::CheckSuiteCompleted { conclusion, .. }] => {
                assert_eq!(conclusion.as_str(), "timed_out")
            }
            other => panic!("{other:?}"),
        }

        // review() uses ReviewState::Approved. `ReviewState` is serialized
        // SCREAMING_SNAKE_CASE in the model, so the canonical value is "APPROVED"
        // — the point is that it matches the model's serde, not Rust `Debug`.
        let rev = handle_event(&review(ReviewAction::Submitted, 1), &state, &ctx());
        match rev.events.as_slice() {
            [StateEventPayload::ReviewSubmitted { state, .. }] => {
                assert_eq!(state.as_str(), "APPROVED")
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn changing_a_predecessor_under_an_active_train_aborts_it() {
        use crate::types::{CascadePhase, DescendantProgress, TrainState};
        // PR #2 declares predecessor #1 via comment 7; an active train freezes #2.
        let mut pr2 = open_pr(2, "feature-3", Some(1));
        pr2.predecessor_comment_id = Some(CommentId(7));
        let mut train = TrainRecord::new(PrNumber(1), ts());
        train.state = TrainState::Running;
        train.cascade_phase = CascadePhase::Preparing {
            progress: DescendantProgress::new(vec![PrNumber(2)]),
        };
        let mut snap = PersistedRepoSnapshot::new("main");
        for p in [open_pr(1, "main", None), open_pr(3, "main", None), pr2] {
            snap.prs.insert(p.number, p);
        }
        snap.active_trains.insert(PrNumber(1), train);
        let state = RepoState::from_snapshot(snap);

        let out = handle_event(
            &comment(
                CommentAction::Edited,
                Some(2),
                "@merge-train predecessor #3",
                1,
            ),
            &state,
            &ctx(),
        );
        assert!(
            out.events
                .iter()
                .any(|e| matches!(e, StateEventPayload::PredecessorDeclared { .. }))
        );
        assert!(out.events.iter().any(|e| matches!(
            e,
            StateEventPayload::TrainAborted { error, .. }
                if error.kind == TrainErrorKind::PredecessorChanged
        )));
    }

    #[test]
    fn removing_a_predecessor_under_an_active_train_aborts_it() {
        let mut pr2 = open_pr(2, "main", Some(1));
        pr2.predecessor_comment_id = Some(CommentId(7));
        let train = TrainRecord::new(PrNumber(2), ts()); // root/current = #2
        let mut snap = PersistedRepoSnapshot::new("main");
        snap.prs.insert(PrNumber(1), open_pr(1, "main", None));
        snap.prs.insert(PrNumber(2), pr2);
        snap.active_trains.insert(PrNumber(2), train);
        let state = RepoState::from_snapshot(snap);

        let out = handle_event(
            &comment(CommentAction::Deleted, Some(2), "", 1),
            &state,
            &ctx(),
        );
        assert!(
            out.events
                .iter()
                .any(|e| matches!(e, StateEventPayload::PredecessorRemoved { .. }))
        );
        assert!(
            out.events
                .iter()
                .any(|e| matches!(e, StateEventPayload::TrainAborted { .. }))
        );
    }

    #[test]
    fn observations_evaluate_the_active_train() {
        use crate::types::TrainState;
        use crate::webhooks::events::{CheckSuiteConclusion, CheckSuiteEvent};
        // PR #1 is the current PR of an active (WaitingCi) train; its head is sha().
        let mut train = TrainRecord::new(PrNumber(1), ts());
        train.state = TrainState::WaitingCi;
        let mut snap = PersistedRepoSnapshot::new("main");
        snap.prs.insert(PrNumber(1), open_pr(1, "main", None));
        snap.active_trains.insert(PrNumber(1), train);
        let state = RepoState::from_snapshot(snap);

        let want = Trigger::EvaluateTrain { root: PrNumber(1) };

        // CI completion on the PR's head re-drives the train.
        let cs = handle_event(
            &GitHubEvent::CheckSuite(CheckSuiteEvent {
                repo: repo(),
                action: CheckSuiteAction::Completed,
                head_sha: sha(),
                conclusion: Some(CheckSuiteConclusion::Success),
                pull_requests: vec![],
            }),
            &state,
            &ctx(),
        );
        assert!(cs.triggers.contains(&want));

        // An approval re-drives it.
        let rev = handle_event(&review(ReviewAction::Submitted, 1), &state, &ctx());
        assert!(rev.triggers.contains(&want));

        // A merge re-drives it.
        let merged = handle_event(
            &pr_event(
                PrAction::Closed,
                1,
                "main",
                MergeStatus::Merged {
                    merge_commit_sha: sha(),
                },
            ),
            &state,
            &ctx(),
        );
        assert!(merged.triggers.contains(&want));

        // An observation for a PR in *no* train produces no trigger.
        let idle = state_with(vec![open_pr(9, "main", None)]);
        let none = handle_event(&review(ReviewAction::Submitted, 9), &idle, &ctx());
        assert!(none.triggers.is_empty());
    }

    #[test]
    fn predecessor_on_a_merged_pr_is_a_late_addition() {
        let mut pred = open_pr(1, "main", None);
        pred.state = PrState::Merged {
            merge_commit_sha: sha(),
        };
        let state = state_with(vec![pred, open_pr(2, "feature-1", None)]);
        let out = handle_event(
            &comment(
                CommentAction::Created,
                Some(2),
                "@merge-train predecessor #1",
                1,
            ),
            &state,
            &ctx(),
        );
        assert_eq!(
            out.triggers,
            vec![Trigger::LateAddition {
                pr: PrNumber(2),
                merged_predecessor: PrNumber(1),
            }]
        );
        assert!(out.events.is_empty());
    }
}
