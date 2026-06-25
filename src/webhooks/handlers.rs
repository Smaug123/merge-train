//! Pure event handlers (cascade engine stage M3).
//!
//! [`handle_event`] maps a parsed [`GitHubEvent`] + the current [`RepoState`]
//! to a [`HandlerOutput`]: the state events to append, the side effects to run
//! (acks / rejection comments / cache refreshes), and the [`Trigger`]s the
//! worker feeds to the cascade engine (M2). Handlers are **pure and total** —
//! validation failures become rejection-comment effects, never errors, and
//! events for unknown PRs are tolerated by `apply_event`.
//!
//! What's deferred to later stages: the `EvaluateTrain`/`LateAddition` triggers
//! that re-drive a *parked* train on CI/review/merge events need M2's parking
//! model, so the observation handlers here emit their record events but not yet
//! those triggers. Dedupe is the worker's job (M5); handlers assume deduped input.

use chrono::{DateTime, Utc};

use crate::commands::{Command, parse_command};
use crate::effects::Effect;
use crate::effects::github::{GitHubEffect, Reaction};
use crate::persistence::event::StateEventPayload;
use crate::state::{PredecessorValidationError, RepoState, validate_predecessor_declaration};
use crate::types::{CommentId, PrNumber, TrainError, TrainErrorKind};
use crate::webhooks::events::{
    CommentAction, GitHubEvent, IssueCommentEvent, MergeStatus, PrAction, PullRequestEvent,
    PullRequestReviewEvent, ReviewAction,
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
        GitHubEvent::CheckSuite(e) => {
            HandlerOutput::event(StateEventPayload::CheckSuiteCompleted {
                sha: e.head_sha.clone(),
                conclusion: e
                    .conclusion
                    .as_ref()
                    .map(|c| format!("{c:?}"))
                    .unwrap_or_else(|| "none".to_owned()),
            })
        }
        GitHubEvent::Status(e) => HandlerOutput::event(StateEventPayload::StatusReceived {
            sha: e.sha.clone(),
            context: e.context.clone(),
            state: format!("{:?}", e.state),
        }),
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
            Some(declaring_pr) => HandlerOutput::event(StateEventPayload::PredecessorRemoved {
                pr: declaring_pr,
                comment_id: event.comment_id,
            }),
            None => HandlerOutput::default(),
        },
        // Created or edited: (re-)parse the body as a command.
        CommentAction::Created | CommentAction::Edited => {
            match parse_command(&event.body, &ctx.bot_name) {
                Some(command) => dispatch_command(command, pr, event.comment_id, state),
                None => HandlerOutput::default(),
            }
        }
    }
}

fn dispatch_command(
    command: Command,
    pr: PrNumber,
    comment_id: CommentId,
    state: &RepoState,
) -> HandlerOutput {
    match command {
        Command::Start => acked_trigger(comment_id, Trigger::StartTrain { pr }),
        Command::Stop => acked_trigger(comment_id, Trigger::StopTrain { pr, force: false }),
        Command::StopForce => acked_trigger(comment_id, Trigger::StopTrain { pr, force: true }),
        Command::Predecessor(predecessor) => {
            handle_predecessor_command(pr, predecessor, comment_id, state)
        }
    }
}

/// Acknowledge a command with a 👍 and emit its engine trigger.
fn acked_trigger(comment_id: CommentId, trigger: Trigger) -> HandlerOutput {
    HandlerOutput {
        events: vec![],
        effects: vec![ack(comment_id)],
        triggers: vec![trigger],
    }
}

fn handle_predecessor_command(
    pr: PrNumber,
    predecessor: PrNumber,
    comment_id: CommentId,
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

    match validate_predecessor_declaration(cached, predecessor, &state.prs, &state.default_branch) {
        Ok(()) => HandlerOutput {
            events: vec![StateEventPayload::PredecessorDeclared {
                pr,
                predecessor,
                comment_id,
            }],
            effects: vec![ack(comment_id)],
            triggers: vec![],
        },
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
    let payload = match event.action {
        PrAction::Opened => Some(StateEventPayload::PrOpened {
            pr,
            head_sha: event.head_sha.clone(),
            head_ref: event.head_branch.clone(),
            base_ref: event.base_branch.clone(),
            is_draft: event.is_draft,
        }),
        PrAction::Closed => Some(match &event.merge_status {
            MergeStatus::Merged { merge_commit_sha } => StateEventPayload::PrMerged {
                pr,
                merge_sha: merge_commit_sha.clone(),
            },
            MergeStatus::NotMerged => StateEventPayload::PrClosed { pr },
        }),
        PrAction::Reopened => Some(StateEventPayload::PrReopened { pr }),
        PrAction::Synchronize => Some(StateEventPayload::PrSynchronized {
            pr,
            new_head_sha: event.head_sha.clone(),
        }),
        PrAction::ConvertedToDraft => Some(StateEventPayload::PrConvertedToDraft { pr }),
        PrAction::ReadyForReview => Some(StateEventPayload::PrReadyForReview { pr }),
        // An edit only matters when it changes the base branch, and only then if
        // we know the previous base (apply_event would skip an unknown PR anyway).
        PrAction::Edited => match state.prs.get(&pr) {
            Some(cached) if cached.base_ref != event.base_branch => {
                Some(StateEventPayload::PrBaseChanged {
                    pr,
                    old_base: cached.base_ref.clone(),
                    new_base: event.base_branch.clone(),
                })
            }
            _ => None,
        },
    };

    match payload {
        Some(p) => HandlerOutput::event(p),
        None => HandlerOutput::default(),
    }
}

// ─── pull_request_review ───

fn handle_review(event: &PullRequestReviewEvent, state: &RepoState) -> HandlerOutput {
    match event.action {
        ReviewAction::Submitted => HandlerOutput::event(StateEventPayload::ReviewSubmitted {
            pr: event.pr_number,
            reviewer: event.reviewer_login.clone(),
            state: format!("{:?}", event.state),
        }),
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

/// The root of an active train that `pr` participates in (as the current PR or
/// a frozen descendant), if any.
fn active_train_root_for(state: &RepoState, pr: PrNumber) -> Option<PrNumber> {
    state
        .active_trains
        .values()
        .find(|t| t.current_pr == pr || t.original_root_pr == pr)
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
}
