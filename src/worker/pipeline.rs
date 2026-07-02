//! The per-delivery pipeline and the saga state machine — the "missing
//! middle" wiring (M5): webhook deliveries in, engine plans out.
//!
//! [`Processor`] owns the repo's [`Store`] and drives everything that is
//! *decision*, while effect *execution* stays outside (the worker loop runs
//! [`super::executor::execute_batch`] on a spawned executor thread; tests run
//! it inline). The split makes every durability boundary a method boundary,
//! so the crash-point tests can drop the store between any two steps.
//!
//! # The per-delivery pipeline ([`Processor::process_claimed`])
//!
//! parse → dedupe-check → default-branch discovery (first contact) →
//! command authorization → referenced-PR precache → `handle_event` →
//! **atomic close** (`commit_delivery`: handler events + dedupe key + done in
//! one transaction) → handler effects (best-effort) → queue triggers.
//!
//! Everything before the atomic close is effect-free on the repo state
//! except *observations* (cache fills, default-branch discovery), so a crash
//! or a [`PipelineOutcome::Released`] retry replays harmlessly. Everything
//! irreversible the engine later does is guarded by its own intent/done
//! ledger, not by delivery accounting.
//!
//! # The saga machine
//!
//! One saga (train) executes at a time per repo. While its effects run
//! off-thread, the worker keeps enqueuing *and processing* deliveries —
//! handler events land mid-saga by design (that is how reality updates; the
//! engine re-reads state at every observation). Engine work discovered
//! mid-saga queues as [`PendingWork`] and starts when the saga parks or
//! completes. Queued **stops run at every observation boundary**
//! ([`Processor::on_outcomes`]), so a human's `stop` takes effect after at
//! most one effect batch (DESIGN: bounded staleness).
//!
//! # GitHub unavailability
//!
//! Pipeline-level GitHub calls (discovery, authorization, precache) are
//! answers the pipeline cannot proceed without. On a transient failure the
//! delivery is **released** back to `pending` and the worker waits for the
//! next mailbox message — in-order processing pauses rather than guessing
//! (correctness over availability; the durable queue bounds the loss to
//! latency).

use std::collections::HashSet;
use std::collections::VecDeque;
use std::path::PathBuf;

use chrono::Utc;
use tracing::{error, info, warn};

use crate::cascade::{self, Control, EffectError, Observation, ReplayFacts, StepPlan, observe};
use crate::commands::{Command, parse_command};
use crate::effects::github::GitHubEffect;
use crate::effects::{Effect, GitHubResponse, PrData};
use crate::git::{CommitIdentity, GitConfig};
use crate::persistence::event::StateEventPayload;
use crate::store::{Delivery, Store, StoreError};
use crate::types::{MergeStateStatus, PhaseKind, PrNumber, PrState};
use crate::webhooks::dedupe::DedupeKey;
use crate::webhooks::events::CommentAction;
use crate::webhooks::handlers::{HandlerCtx, Trigger, handle_event};
use crate::webhooks::{GitHubEvent, parse_webhook};

use super::authz::{
    AuthorDecision, RoleDecision, authorize_by_author, authorize_by_role, authorize_retraction,
};
use super::executor::{GitHubExec, SagaBatch};

/// Per-repo dependencies the processor needs beyond the `Store`.
pub struct WorkerDeps {
    /// How to reach GitHub.
    pub github: GitHubExec,
    /// Where this repo's clone/worktrees live and how to create commits.
    pub git: GitSettings,
    /// The bot's user id (its own comments are never commands).
    pub bot_user_id: u64,
    /// The bot's mention name, without `@`.
    pub bot_name: String,
    /// How long the worker waits before retrying a released delivery.
    pub stall_retry_delay: std::time::Duration,
}

/// The git-side settings a [`GitConfig`] is derived from per saga (the
/// `default_branch` half lives in `RepoState` and may change at runtime).
pub struct GitSettings {
    /// Base directory for all repos (e.g. `/var/lib/merge-train/repos`).
    pub base_dir: PathBuf,
    /// Repository owner.
    pub owner: String,
    /// Repository name.
    pub repo: String,
    /// Identity for the cascade's merge commits.
    pub commit_identity: CommitIdentity,
    /// Maximum age for stale worktree cleanup.
    pub worktree_max_age: std::time::Duration,
    /// URL to clone from on first git use. `None` means the clone is managed
    /// externally (tests pre-create it); a missing clone is then an error.
    pub clone_url: Option<String>,
}

/// How a delivery's pipeline run ended.
#[derive(Debug, PartialEq, Eq)]
pub enum PipelineOutcome {
    /// The delivery was fully processed and closed.
    Processed,
    /// GitHub was unavailable for a pre-close pipeline step; the delivery was
    /// released back to `pending`. The worker should wait for the next
    /// mailbox message rather than re-claiming in a hot loop.
    Released,
}

/// Engine work waiting for the (single) saga slot.
#[derive(Debug, PartialEq, Eq)]
enum PendingWork {
    /// A handler-emitted trigger (`LateAddition` is answered inline and never
    /// queued; `StopTrain` becomes the durable [`PendingWork::Stop`]).
    Trigger(Trigger),
    /// An authorized stop command, persisted in the `pending_stops` table by
    /// the delivery's close (an acknowledged stop must survive a crash while
    /// it waits out an in-flight saga — Codex M5 round 2, P1). `id` is the
    /// durable row, deleted when the stop applies.
    Stop { id: i64, pr: PrNumber, force: bool },
    /// Best-effort cleanup for a train the handlers aborted directly
    /// (worktree + final comment/status), run once the saga slot frees.
    AbortCleanup { root: PrNumber },
    /// A handler `TrainAborted` for the root whose saga is in flight,
    /// deferred to the observation boundary: committed mid-saga it would
    /// make `advance` see an inactive train and discard the outcomes of
    /// effects that already ran (Codex M5 round 2, P1 — the same ordering
    /// queued stops get). Volatile until applied; a crash loses it, and the
    /// train is then recovered like any inherited active train (the abort's
    /// *cause* events are durable, so evaluation re-derives it under M6).
    DeferredAbort {
        root: PrNumber,
        error: crate::types::TrainError,
    },
}

/// A stop extracted at an observation boundary, with the durable row id and
/// whether it consumed a start that was still queued (never started).
struct QueuedStop {
    id: i64,
    pr: PrNumber,
    force: bool,
    cancelled_queued_start: bool,
}

/// The per-repo decision core: the Store, the pending-work queue, and the
/// in-flight saga marker. Execution of effects happens outside.
pub(crate) struct Processor {
    store: Store,
    deps: WorkerDeps,
    pending: VecDeque<PendingWork>,
    /// The root whose plan's effects are currently executing off-thread.
    in_flight: Option<PrNumber>,
    /// Active non-`Idle` trains inherited from a previous process: resuming
    /// them needs M6's recovery (worktree restart cleanup + supplementary
    /// GitHub recovery), so evaluation is refused loudly until then. `stop`
    /// still works.
    inherited_mid_flight: HashSet<PrNumber>,
    /// Active-train evaluations owed at startup, queued when the durable
    /// backlog first drains (`Some` until then; see [`Processor::claim`]).
    startup_evaluates: Option<Vec<PrNumber>>,
}

impl Processor {
    pub fn new(store: Store, deps: WorkerDeps) -> Result<Processor, StoreError> {
        let inherited_mid_flight: HashSet<PrNumber> = store
            .state()
            .active_trains
            .values()
            .filter(|t| t.state.is_active() && t.cascade_phase.kind() != PhaseKind::Idle)
            .map(|t| t.original_root_pr)
            .collect();
        for root in &inherited_mid_flight {
            error!(
                %root,
                "train was mid-cascade when the previous process died; \
                 resuming it requires recovery (M6), which is not implemented \
                 yet — the train will not advance. `@merge-train stop` it and \
                 re-issue `start`."
            );
        }

        // Startup work (Codex M5 round 2): persisted stops that never
        // reached their observation boundary apply first. Startup
        // *evaluations* of active trains (so an acknowledged CI success
        // whose trigger died with the process still resumes a parked train)
        // are computed here but queue only once the durable backlog first
        // drains — see [`Processor::claim`] — or their effects would run
        // against state that predates already-acked deliveries (Codex M5
        // round 6, P1: e.g. a queued topology-change abort overtaken by a
        // squash).
        let mut pending = VecDeque::new();
        for (id, pr, force) in store.pending_stops()? {
            pending.push_back(PendingWork::Stop { id, pr, force });
        }
        let startup_evaluates = store
            .state()
            .active_trains
            .values()
            .filter(|t| t.state.is_active())
            .map(|t| t.original_root_pr)
            .collect();

        Ok(Processor {
            store,
            deps,
            pending,
            in_flight: None,
            inherited_mid_flight,
            startup_evaluates: Some(startup_evaluates),
        })
    }

    pub fn store_mut(&mut self) -> &mut Store {
        &mut self.store
    }

    /// The materialized repo state.
    #[cfg(test)]
    pub fn state(&self) -> &crate::state::RepoState {
        self.store.state()
    }

    /// The roots still refused evaluation as inherited-mid-cascade.
    #[cfg(test)]
    pub fn inherited_markers(&self) -> &HashSet<PrNumber> {
        &self.inherited_mid_flight
    }

    pub fn github(&self) -> &GitHubExec {
        &self.deps.github
    }

    pub fn stall_retry_delay(&self) -> std::time::Duration {
        self.deps.stall_retry_delay
    }

    pub fn git_settings(&self) -> &GitSettings {
        &self.deps.git
    }

    /// The per-saga [`GitConfig`], carrying the *current* default branch.
    pub fn git_config(&self) -> GitConfig {
        let git = &self.deps.git;
        GitConfig {
            base_dir: git.base_dir.clone(),
            owner: git.owner.clone(),
            repo: git.repo.clone(),
            default_branch: self.store.state().default_branch.clone(),
            worktree_max_age: git.worktree_max_age,
            commit_identity: git.commit_identity.clone(),
        }
    }

    pub fn saga_in_flight(&self) -> bool {
        self.in_flight.is_some()
    }

    /// Whether queued engine work is waiting for the saga slot. The worker
    /// loop must not block on its mailbox while this is true and the slot is
    /// free: a turn whose empty `claim` just queued the startup evaluations
    /// would otherwise strand them until unrelated traffic arrives (Codex M5
    /// round 7, P1).
    pub fn has_queued_work(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Claims the next pending delivery, if any.
    ///
    /// The first time the backlog turns up empty, the startup evaluations
    /// queue: every already-acked delivery has now been applied, so the
    /// evaluations plan against current state rather than overtaking the
    /// backlog (Codex M5 round 6, P1).
    pub fn claim(&mut self) -> Result<Option<Delivery>, StoreError> {
        let claimed = self.store.claim_next_delivery()?;
        if claimed.is_none()
            && let Some(roots) = self.startup_evaluates.take()
        {
            for root in roots {
                self.queue(PendingWork::Trigger(Trigger::EvaluateTrain { root }));
            }
        }
        Ok(claimed)
    }

    // ─── The per-delivery pipeline ───

    /// Runs the full pipeline for one claimed delivery.
    pub fn process_claimed(&mut self, delivery: Delivery) -> Result<PipelineOutcome, StoreError> {
        let id = delivery.delivery_id.clone();

        let event = match parse_webhook(&delivery.event_type, &delivery.body) {
            Ok(Some(event)) => event,
            Ok(None) => return self.close(&id, None, "ignored event type"),
            Err(e) => {
                warn!(delivery_id = %id, error = %e, "malformed webhook; closing");
                return self.close(&id, None, "malformed");
            }
        };

        // Dedupe: identical content already handled under a different
        // delivery id (GitHub redelivers with fresh ids).
        let key = DedupeKey::for_event(&event);
        if let Some(k) = &key
            && self.store.is_duplicate(k)?
        {
            return self.close(&id, None, "duplicate content");
        }

        // First-contact default-branch discovery: a fresh store has an empty
        // default branch, and root detection / base validation read it.
        if self.store.state().default_branch.is_empty() {
            match self.deps.github.execute(GitHubEffect::GetRepoSettings) {
                Ok(GitHubResponse::RepoSettings(settings)) => {
                    info!(default_branch = %settings.default_branch, "discovered default branch");
                    self.store.append_batch(
                        &[StateEventPayload::DefaultBranchSet {
                            branch: settings.default_branch,
                        }],
                        Utc::now(),
                    )?;
                }
                Ok(other) => {
                    error!(?other, "GetRepoSettings answered the wrong variant");
                    return self.release(&id);
                }
                Err(e @ EffectError::Transient { .. }) => {
                    warn!(error = ?e, "cannot discover default branch; releasing delivery");
                    return self.release(&id);
                }
                // Permanent (token lacks access, repo deleted/renamed):
                // release too — DELIBERATELY, unlike role lookups. There a
                // denial is a safe answer; here there is none: without the
                // default branch nothing can be processed, and closing the
                // delivery would silently drop webhooks GitHub will never
                // resend. The repo's queue pauses (retrying at the stall
                // cadence, which also heals "permanent" auth errors the
                // moment the operator fixes the token) and this error says
                // so as loudly as we can.
                Err(e) => {
                    error!(
                        error = ?e,
                        "cannot discover the default branch and the failure is \
                         permanent; the repo's queue is PAUSED until discovery \
                         succeeds — operator action likely required (token \
                         scopes? repo moved?)"
                    );
                    return self.release(&id);
                }
            }
        }

        // Command authorization + referenced-PR precache (commands only).
        if let Some((pr, command)) = command_in(&event, &self.deps) {
            match self.authorize(&command, &event) {
                Ok(None) => {}
                Ok(Some(rejection)) => {
                    // Handled-by-rejection: close (recording the key) and
                    // tell the commenter. The handler never sees it.
                    self.store
                        .commit_delivery(&id, &[], key.as_ref(), &[], Utc::now())?;
                    self.best_effort_github(GitHubEffect::PostComment {
                        pr,
                        body: rejection,
                    });
                    return Ok(PipelineOutcome::Processed);
                }
                Err(ReleaseDelivery) => return self.release(&id),
            }

            let optional = match &command {
                Command::Predecessor(target) => vec![*target],
                _ => Vec::new(),
            };
            match self.precache(pr, &optional)? {
                PrecacheOutcome::Ready => {}
                PrecacheOutcome::Release => return self.release(&id),
                PrecacheOutcome::Deny(reason) => {
                    self.store
                        .commit_delivery(&id, &[], key.as_ref(), &[], Utc::now())?;
                    self.best_effort_github(GitHubEffect::PostComment { pr, body: reason });
                    return Ok(PipelineOutcome::Processed);
                }
            }
        }

        // Predecessor *retractions* — an edit that no longer declares, or a
        // deletion of the declaring comment — are topology changes and are
        // author-only, exactly like declarations (Codex M5 round 3, P1).
        // Unauthorized: close the delivery without running the handler, so
        // the declaration stands.
        if let Some(owner_pr) = retraction_in(&event, self.store.state(), &self.deps)
            && let GitHubEvent::IssueComment(comment) = &event
            && let AuthorDecision::Denied { reason } =
                authorize_retraction(comment.sender_id, comment.pr_author_id)
        {
            self.store
                .commit_delivery(&id, &[], key.as_ref(), &[], Utc::now())?;
            self.best_effort_github(GitHubEffect::PostComment {
                pr: owner_pr,
                body: reason,
            });
            return Ok(PipelineOutcome::Processed);
        }

        // The pure handler, then the atomic close: events + dedupe key +
        // `done` commit together.
        let ctx = HandlerCtx {
            bot_user_id: self.deps.bot_user_id,
            bot_name: self.deps.bot_name.clone(),
            now: Utc::now(),
        };
        let output = handle_event(&event, self.store.state(), &ctx);

        // Handler aborts for the *in-flight* saga root defer to the
        // observation boundary: committed now, `advance` would see an
        // inactive train and discard the outcomes of effects that already
        // ran (Codex M5 round 2, P1). Aborts for other roots (nothing of
        // theirs is in flight) commit with the delivery as usual.
        let mut events = output.events;
        if let Some(saga_root) = self.in_flight {
            let mut deferred = Vec::new();
            events.retain(|e| match e {
                StateEventPayload::TrainAborted { root_pr, error } if *root_pr == saga_root => {
                    deferred.push((*root_pr, error.clone()));
                    false
                }
                _ => true,
            });
            for (root, error) in deferred {
                info!(%root, "deferring handler abort to the saga's observation boundary");
                self.queue(PendingWork::DeferredAbort { root, error });
            }
        }

        // Authorized stops persist in the close transaction: the stop may
        // wait out a multi-minute saga before its observation boundary, and
        // an acknowledged stop must survive a crash in that window (Codex M5
        // round 2, P1).
        let stops: Vec<(PrNumber, bool)> = output
            .triggers
            .iter()
            .filter_map(|t| match t {
                Trigger::StopTrain { pr, force } => Some((*pr, *force)),
                _ => None,
            })
            .collect();

        let stop_ids =
            self.store
                .commit_delivery(&id, &events, key.as_ref(), &stops, Utc::now())?;
        self.clear_inherited_markers(&events);
        for (&(pr, force), id) in stops.iter().zip(stop_ids) {
            self.queue(PendingWork::Stop { id, pr, force });
        }

        // Handler-terminated trains need worker-side cleanup (the engine's
        // own aborts carry cleanup in their plans; handler aborts have no
        // plan).
        for payload in &events {
            if let StateEventPayload::TrainAborted { root_pr, .. } = payload {
                self.queue(PendingWork::AbortCleanup { root: *root_pr });
            }
        }

        // Handler effects are cosmetic-or-cache: ack reactions, rejection
        // comments, and the cold-start `GetPr` fallback (whose response is
        // persisted as cache-fill events).
        for effect in output.effects {
            self.run_handler_effect(effect)?;
        }

        for trigger in output.triggers {
            match trigger {
                Trigger::LateAddition {
                    pr,
                    merged_predecessor,
                } => {
                    // Detected by M3, answered here: the reconciliation flow
                    // for late additions is explicitly deferred.
                    self.best_effort_github(GitHubEffect::PostComment {
                        pr,
                        body: format!(
                            "PR #{merged_predecessor} is already merged. Adding a PR onto a \
                             merged predecessor (\"late addition\") is not supported yet — \
                             rebase onto the default branch, or restart the train."
                        ),
                    });
                }
                // Persisted (and queued) above.
                Trigger::StopTrain { .. } => {}
                other => self.queue(PendingWork::Trigger(other)),
            }
        }

        Ok(PipelineOutcome::Processed)
    }

    /// Closes a delivery with no state events.
    fn close(
        &mut self,
        id: &str,
        key: Option<&DedupeKey>,
        reason: &str,
    ) -> Result<PipelineOutcome, StoreError> {
        self.store.commit_delivery(id, &[], key, &[], Utc::now())?;
        info!(delivery_id = %id, reason, "delivery closed without events");
        Ok(PipelineOutcome::Processed)
    }

    /// Releases a delivery for a later retry (GitHub unavailable).
    fn release(&mut self, id: &str) -> Result<PipelineOutcome, StoreError> {
        self.store.release_delivery(id)?;
        Ok(PipelineOutcome::Released)
    }

    /// Decides a command's authorization. `Ok(None)` = allowed; `Ok(Some)` =
    /// denied with the rejection comment; `Err` = GitHub unavailable.
    fn authorize(
        &mut self,
        command: &Command,
        event: &GitHubEvent,
    ) -> Result<Option<String>, ReleaseDelivery> {
        let GitHubEvent::IssueComment(comment) = event else {
            return Ok(None);
        };
        // Authorize the *actor* (`sender`): on `created` that is the comment
        // author; on `edited` it is the editor, who may not be the author.
        match authorize_by_author(command, comment.sender_id, comment.pr_author_id) {
            AuthorDecision::Allowed => Ok(None),
            AuthorDecision::Denied { reason } => Ok(Some(reason)),
            AuthorDecision::NeedsRole => {
                let response = self
                    .deps
                    .github
                    .execute(GitHubEffect::GetCollaboratorPermission {
                        username: comment.sender_login.clone(),
                    });
                match response {
                    Ok(GitHubResponse::CollaboratorPermission { role }) => {
                        match authorize_by_role(command, &role) {
                            RoleDecision::Allowed => Ok(None),
                            RoleDecision::Denied { reason } => Ok(Some(reason)),
                        }
                    }
                    // Transient (outage): release for the stall-retry loop —
                    // an outage must neither grant admin rights nor
                    // permanently swallow a stop.
                    Err(e @ EffectError::Transient { .. }) => {
                        warn!(error = ?e, "cannot verify commenter role; releasing delivery");
                        Err(ReleaseDelivery)
                    }
                    // Permanent (bad token scope, API change) or a wrong
                    // response variant: retrying cannot help, and a released
                    // delivery would retry at the front of the queue forever,
                    // wedging the repo (Codex M5 round 4). Fail closed: deny
                    // the command.
                    Err(e) => {
                        error!(error = ?e, "permission lookup failed permanently; denying");
                        Ok(Some(
                            "The bot cannot verify your repository permissions \
                             (permission lookup failed permanently — check the bot \
                             token's scopes); refusing the command."
                                .to_owned(),
                        ))
                    }
                    Ok(other) => {
                        error!(
                            ?other,
                            "permission lookup answered the wrong variant; denying"
                        );
                        Ok(Some(
                            "The bot cannot verify your repository permissions; \
                             refusing the command."
                                .to_owned(),
                        ))
                    }
                }
            }
        }
    }

    /// Fetches and caches the command's own PR and any optionally referenced
    /// PRs the bot has never seen, so commands validate against facts instead
    /// of being dropped.
    ///
    /// A permanent fetch failure is answered differently by role (Codex M5
    /// round 12): the *command PR* is required — proceeding uncached turns
    /// the acknowledged command into a silently-logged engine error, so the
    /// command is denied with an explanation instead. An *optional* referent
    /// (a predecessor target) proceeds uncached: validation rejects it
    /// loudly with a comment of its own.
    ///
    /// The outer `Result` is the store: an append failure is a broken Store,
    /// not GitHub unavailability, and must take the worker's fatal path (drop
    /// → reopen → recover), never the release-and-retry path (Codex M5
    /// round 6).
    fn precache(
        &mut self,
        command_pr: PrNumber,
        optional: &[PrNumber],
    ) -> Result<PrecacheOutcome, StoreError> {
        for (pr, required) in
            std::iter::once((command_pr, true)).chain(optional.iter().map(|&pr| (pr, false)))
        {
            if self.store.state().prs.contains_key(&pr) {
                continue;
            }
            match self.deps.github.execute(GitHubEffect::RefetchPr { pr }) {
                Ok(GitHubResponse::PrRefetched {
                    pr: data,
                    merge_state,
                }) => {
                    let events = cache_fill_events(pr, &data, merge_state);
                    self.store.append_batch(&events, Utc::now())?;
                }
                Ok(other) => {
                    error!(?other, "RefetchPr answered the wrong variant");
                    return Ok(PrecacheOutcome::Release);
                }
                Err(EffectError::Transient { detail }) => {
                    warn!(%pr, detail, "cannot fetch referenced PR; releasing delivery");
                    return Ok(PrecacheOutcome::Release);
                }
                Err(e) if required => {
                    error!(%pr, error = ?e, "the command's PR is permanently unfetchable; denying");
                    return Ok(PrecacheOutcome::Deny(format!(
                        "The bot cannot fetch PR #{pr} (permanent API failure — does the                          bot's token have access to this repository?); refusing the command."
                    )));
                }
                Err(e) => {
                    // Permanent on an optional referent (e.g. the number does
                    // not exist): proceed uncached — validation rejects it
                    // loudly.
                    warn!(%pr, error = ?e, "referenced PR unfetchable; proceeding uncached");
                }
            }
        }
        Ok(PrecacheOutcome::Ready)
    }

    /// Runs one handler-emitted effect. Failures are logged, never fed back —
    /// except that a `GetPr` *response* is persisted as cache-fill events
    /// (the handler's cold-start fallback exists to fill the cache).
    fn run_handler_effect(&mut self, effect: Effect) -> Result<(), StoreError> {
        match effect {
            Effect::GitHub(GitHubEffect::GetPr { pr }) => {
                match self.deps.github.execute(GitHubEffect::GetPr { pr }) {
                    Ok(GitHubResponse::Pr(data)) if !self.store.state().prs.contains_key(&pr) => {
                        let events = cache_fill_events(pr, &data, MergeStateStatus::Unknown);
                        self.store.append_batch(&events, Utc::now())?;
                    }
                    Ok(_) => {}
                    Err(e) => warn!(%pr, error = ?e, "handler GetPr fallback failed (ignored)"),
                }
            }
            Effect::GitHub(effect) => self.best_effort_github(effect),
            Effect::Git(effect) => {
                // Handlers are API-only; a git effect here is a handler bug.
                error!(?effect, "handler emitted a git effect (ignored)");
            }
        }
        Ok(())
    }

    /// Executes a fire-and-forget GitHub effect (ack reactions, rejection
    /// comments) on a detached thread: nothing reads the response, and a
    /// slow or unavailable GitHub must not stall the worker loop — intake
    /// acks and observation boundaries run on this thread (Codex M5
    /// round 4). Failures are logged, never propagated.
    ///
    /// The fake runs inline: it is in-memory (nothing to block on), and the
    /// synchronous test harness asserts on its state right after the call.
    fn best_effort_github(&self, effect: GitHubEffect) {
        let github = self.deps.github.clone();
        #[cfg(test)]
        if matches!(github, GitHubExec::Fake(_)) {
            if let Err(e) = github.execute(effect) {
                warn!(error = ?e, "best-effort GitHub effect failed (ignored)");
            }
            return;
        }
        let spawned = std::thread::Builder::new()
            .name("best-effort-github".to_owned())
            .spawn(move || {
                if let Err(e) = github.execute(effect) {
                    warn!(error = ?e, "best-effort GitHub effect failed (ignored)");
                }
            });
        if spawned.is_err() {
            warn!("failed to spawn the best-effort effect thread; effect dropped");
        }
    }

    /// Queues engine work, coalescing duplicates.
    fn queue(&mut self, work: PendingWork) {
        // Coalesce only *idempotent* work (re-evaluating or re-cleaning the
        // same root twice is one evaluation). User commands are utterances:
        // deduping a second `start` while an earlier identical one is queued
        // collapses `start -> stop -> start` into `start -> stop`, silently
        // losing the final start (Codex M5 round 8).
        let idempotent = matches!(
            work,
            PendingWork::Trigger(Trigger::EvaluateTrain { .. })
                | PendingWork::AbortCleanup { .. }
                | PendingWork::DeferredAbort { .. }
        );
        if idempotent && self.pending.contains(&work) {
            return;
        }
        self.pending.push_back(work);
    }

    /// Inherited-marker upkeep: a root stays refused only while the inherited
    /// mid-cascade record is the live one. Any train-lifecycle event for the
    /// root supersedes that record — most importantly the documented recovery
    /// path, stop → fresh `start` (Codex M5 review: without this, the marker
    /// refused the restarted train's evaluation until the process restarted).
    fn clear_inherited_markers(&mut self, events: &[StateEventPayload]) {
        if self.inherited_mid_flight.is_empty() {
            return;
        }
        for event in events {
            if let StateEventPayload::TrainStarted { root_pr, .. }
            | StateEventPayload::TrainStopped { root_pr }
            | StateEventPayload::TrainAborted { root_pr, .. }
            | StateEventPayload::TrainCompleted { root_pr, .. } = event
            {
                self.inherited_mid_flight.remove(root_pr);
            }
        }
    }

    // ─── The saga machine ───

    /// Starts the next queued saga if the slot is free. Returns the effect
    /// batch to execute, if any work produced one.
    pub fn pump(&mut self) -> Result<Option<SagaBatch>, StoreError> {
        if self.in_flight.is_some() {
            return Ok(None);
        }
        while let Some(work) = self.pending.pop_front() {
            let now = Utc::now();
            let state = self.store.state();
            let (root, plan) = match work {
                PendingWork::Trigger(Trigger::StartTrain { pr }) => {
                    (pr, cascade::start_train(state, pr, now))
                }
                PendingWork::Trigger(Trigger::StopTrain { .. }) => {
                    unreachable!("stops are persisted and queued as PendingWork::Stop")
                }
                // No saga is in flight here, so the stop applies now; its
                // durable row goes with it (delete-after-append: a crash in
                // between replays the stop harmlessly).
                PendingWork::Stop { id, pr, force } => {
                    let root = state.train_involving(pr).unwrap_or(pr);
                    let plan = cascade::stop_train(state, pr, force, now);
                    let integrated = match plan {
                        Ok(plan) => self.integrate_plan(root, plan)?,
                        Err(e) => {
                            error!(%root, error = %e, "engine refused to plan");
                            None
                        }
                    };
                    self.store.delete_pending_stop(id)?;
                    match integrated {
                        Some(batch) => return Ok(Some(batch)),
                        None => continue,
                    }
                }
                PendingWork::Trigger(Trigger::EvaluateTrain { root }) => {
                    if !state
                        .active_trains
                        .get(&root)
                        .is_some_and(|t| t.state.is_active())
                    {
                        continue;
                    }
                    if self.inherited_mid_flight.contains(&root) {
                        error!(
                            %root,
                            "refusing to advance a train inherited mid-cascade from a \
                             previous process (recovery lands in M6); stop and restart it"
                        );
                        continue;
                    }
                    let facts = ReplayFacts::for_train(&self.store.events()?, root);
                    (
                        root,
                        cascade::advance(state, root, Observation::Evaluate { facts }, now),
                    )
                }
                PendingWork::Trigger(Trigger::LateAddition { .. }) => {
                    unreachable!("LateAddition is answered in the pipeline, never queued")
                }
                PendingWork::AbortCleanup { root } => {
                    let effects = cascade::handler_abort_cleanup(state, root);
                    if effects.is_empty() {
                        continue;
                    }
                    self.in_flight = Some(root);
                    return Ok(Some(SagaBatch {
                        root,
                        effects: Vec::new(),
                        best_effort: effects,
                        feedback: false,
                    }));
                }
                // A deferred abort whose saga boundary never consumed it
                // (e.g. the saga ended on a best-effort batch): no effects
                // are in flight now, so it applies immediately.
                PendingWork::DeferredAbort { root, error } => {
                    let (cleanup, _) = self.apply_deferred_aborts(vec![(root, error)])?;
                    if cleanup.is_empty() {
                        continue;
                    }
                    self.in_flight = Some(root);
                    return Ok(Some(SagaBatch {
                        root,
                        effects: Vec::new(),
                        best_effort: cleanup,
                        feedback: false,
                    }));
                }
            };
            match plan {
                Ok(plan) => {
                    if let Some(batch) = self.integrate_plan(root, plan)? {
                        return Ok(Some(batch));
                    }
                    // Plan finished without effects; keep pumping.
                }
                Err(e) => {
                    // Engine errors are caller/executor bugs, not domain
                    // outcomes; nothing to do but be loud.
                    error!(%root, error = %e, "engine refused to plan");
                }
            }
        }
        Ok(None)
    }

    /// Feeds a completed batch's outcomes back. Returns the next batch, which
    /// may continue this saga or start the next queued one.
    pub fn on_outcomes(
        &mut self,
        root: PrNumber,
        outcomes: Vec<crate::cascade::EffectOutcome>,
        feedback: bool,
    ) -> Result<Option<SagaBatch>, StoreError> {
        debug_assert_eq!(self.in_flight, Some(root), "outcomes for a foreign saga");
        self.in_flight = None;

        if !feedback {
            return self.pump();
        }

        // Observation boundary: queued stops and deferred handler aborts act
        // here, so a human's stop (or a handler's abort) preempts whatever
        // this saga would do next.
        let stops = self.take_queued_stops();
        let aborts = self.take_deferred_aborts();

        // The start-cancel window: a stop naming this saga's root while no
        // train record exists yet means the outcomes in flight are the
        // start's preflight *reads* — discarding them loses nothing durable,
        // and letting `advance` run would start the train the user just
        // refused. (A stop naming a *descendant* in that sub-second window
        // cannot be resolved to the stack — there is no train record yet —
        // and takes the normal path's "no active train" answer; the
        // commenter re-issues once the status comment appears.)
        if stops.iter().any(|s| s.pr == root) && self.store.state().train_involving(root).is_none()
        {
            let (cancels, others): (Vec<_>, Vec<_>) = stops.into_iter().partition(|s| s.pr == root);
            let (mut cleanup, _) = self.apply_stops(others)?;
            let (mut abort_cleanup, _) = self.apply_deferred_aborts(aborts)?;
            cleanup.append(&mut abort_cleanup);
            // Cancelling the start IS these stops' application.
            for stop in cancels {
                self.store.delete_pending_stop(stop.id)?;
            }
            cleanup.push(Effect::GitHub(GitHubEffect::PostComment {
                pr: root,
                body: "🛑 Merge train start cancelled.".to_owned(),
            }));
            return self.finish_or_pump(root, cleanup);
        }

        // Integrate the completed outcomes FIRST: these effects already ran
        // (a squash may have merged a PR on GitHub), so their records must
        // land before any stop retires the train — a stopped train ignores
        // observations, and the store would diverge from reality (Codex M5
        // review, P1). Stops then suppress the *continuation*, which has not
        // run yet and is therefore safe to drop.
        let planned = match observe(&outcomes) {
            Ok(obs) => match cascade::advance(self.store.state(), root, obs, Utc::now()) {
                Ok(plan) => self.integrate_plan(root, plan)?,
                Err(e) => {
                    error!(%root, error = %e, "engine rejected an observation; abandoning step");
                    None
                }
            },
            Err(e) => {
                error!(%root, error = %e, "cannot observe executed outcomes; abandoning step");
                None
            }
        };

        let (mut cleanup, mut retired) = self.apply_stops(stops)?;
        let (mut abort_cleanup, aborted) = self.apply_deferred_aborts(aborts)?;
        cleanup.append(&mut abort_cleanup);
        retired.extend(aborted);
        if retired.contains(&root) {
            // A stop or deferred abort retired this train at the boundary:
            // its planned continuation must not run. The plan's events are
            // already durable; an intent among them whose effect never ran
            // is the same state a crash before dispatch leaves, which the
            // recovery contract already covers — and a retired train is
            // never evaluated anyway.
            self.in_flight = None;
            return self.finish_or_pump(root, cleanup);
        }
        match planned {
            Some(mut batch) => {
                batch.best_effort.append(&mut cleanup);
                Ok(Some(batch))
            }
            None => self.finish_or_pump(root, cleanup),
        }
    }

    /// After a saga step produced no follow-up batch: run leftover boundary
    /// cleanup as its own batch, or pump the next queued work.
    fn finish_or_pump(
        &mut self,
        root: PrNumber,
        cleanup: Vec<Effect>,
    ) -> Result<Option<SagaBatch>, StoreError> {
        if cleanup.is_empty() {
            return self.pump();
        }
        self.in_flight = Some(root);
        Ok(Some(SagaBatch {
            root,
            effects: Vec::new(),
            best_effort: cleanup,
            feedback: false,
        }))
    }

    /// Extracts every queued stop, preserving other pending work in order.
    ///
    /// Each stop also consumes any not-yet-started `StartTrain` for its PR
    /// queued *before* it — the stop arrived after that start and must
    /// suppress it, or the train starts anyway once the slot frees (Codex M5
    /// round 6). Starts queued *after* a stop are the user starting anew and
    /// survive (Codex M5 round 8).
    fn take_queued_stops(&mut self) -> Vec<QueuedStop> {
        let mut stops: Vec<QueuedStop> = Vec::new();
        let mut kept: VecDeque<PendingWork> = VecDeque::new();
        for work in std::mem::take(&mut self.pending) {
            match work {
                PendingWork::Stop { id, pr, force } => {
                    let before = kept.len();
                    kept.retain(|w| {
                        !matches!(w, PendingWork::Trigger(Trigger::StartTrain { pr: p }) if *p == pr)
                    });
                    stops.push(QueuedStop {
                        id,
                        pr,
                        force,
                        cancelled_queued_start: kept.len() != before,
                    });
                }
                other => kept.push_back(other),
            }
        }
        self.pending = kept;
        stops
    }

    /// Extracts every queued deferred handler abort.
    fn take_deferred_aborts(&mut self) -> Vec<(PrNumber, crate::types::TrainError)> {
        let mut aborts = Vec::new();
        let mut remaining = VecDeque::new();
        while let Some(work) = self.pending.pop_front() {
            match work {
                PendingWork::DeferredAbort { root, error } => aborts.push((root, error)),
                other => remaining.push_back(other),
            }
        }
        self.pending = remaining;
        aborts
    }

    /// Applies deferred handler aborts now, returning their best-effort
    /// cleanup and the roots actually retired. Idempotent: a train that
    /// completed, stopped, or aborted in the meantime is skipped.
    fn apply_deferred_aborts(
        &mut self,
        aborts: Vec<(PrNumber, crate::types::TrainError)>,
    ) -> Result<(Vec<Effect>, HashSet<PrNumber>), StoreError> {
        let mut cleanup = Vec::new();
        let mut retired = HashSet::new();
        for (root, error) in aborts {
            let still_active = self
                .store
                .state()
                .active_trains
                .get(&root)
                .is_some_and(|t| t.state.is_active());
            if !still_active {
                continue;
            }
            let events = vec![StateEventPayload::TrainAborted {
                root_pr: root,
                error,
            }];
            self.store.append_batch(&events, Utc::now())?;
            self.clear_inherited_markers(&events);
            cleanup.extend(cascade::handler_abort_cleanup(self.store.state(), root));
            retired.insert(root);
        }
        Ok((cleanup, retired))
    }

    /// Applies stops now (terminal events only — cheap and safe at an
    /// observation boundary), returning their best-effort cleanup and the
    /// roots whose trains were actually retired. Each stop's durable row is
    /// deleted after its append (a crash in between replays it harmlessly).
    fn apply_stops(
        &mut self,
        stops: Vec<QueuedStop>,
    ) -> Result<(Vec<Effect>, HashSet<PrNumber>), StoreError> {
        let mut cleanup = Vec::new();
        let mut stopped_roots = HashSet::new();
        for QueuedStop {
            id,
            pr,
            force,
            cancelled_queued_start,
        } in stops
        {
            // A stop that consumed a queued (not-yet-started) start for a PR
            // with no train record is fully answered by that cancellation.
            if cancelled_queued_start && self.store.state().train_involving(pr).is_none() {
                cleanup.push(Effect::GitHub(GitHubEffect::PostComment {
                    pr,
                    body: "\u{1f6d1} Merge train start cancelled.".to_owned(),
                }));
                self.store.delete_pending_stop(id)?;
                continue;
            }
            let now = Utc::now();
            match cascade::stop_train(self.store.state(), pr, force, now) {
                Ok(plan) => {
                    debug_assert!(
                        plan.effects.is_empty(),
                        "stop plans have no observed effects"
                    );
                    for event in &plan.events {
                        if let StateEventPayload::TrainStopped { root_pr }
                        | StateEventPayload::TrainAborted { root_pr, .. } = event
                        {
                            stopped_roots.insert(*root_pr);
                        }
                    }
                    self.store.append_batch(&plan.events, now)?;
                    self.clear_inherited_markers(&plan.events);
                    cleanup.extend(plan.best_effort);
                }
                Err(e) => error!(%pr, error = %e, "stop_train refused"),
            }
            self.store.delete_pending_stop(id)?;
        }
        Ok((cleanup, stopped_roots))
    }

    /// Appends a plan's events and turns its effects into a batch. `None`
    /// means the plan completed immediately (no effects to run).
    fn integrate_plan(
        &mut self,
        root: PrNumber,
        plan: StepPlan,
    ) -> Result<Option<SagaBatch>, StoreError> {
        let StepPlan {
            events,
            effects,
            best_effort,
            control,
        } = plan;
        self.store.append_batch(&events, Utc::now())?;
        self.clear_inherited_markers(&events);

        let feedback = matches!(control, Control::Continue);
        if let Control::FanOut { new_roots } = control {
            for new_root in new_roots {
                self.queue(PendingWork::Trigger(Trigger::EvaluateTrain {
                    root: new_root,
                }));
            }
        }

        if effects.is_empty() && best_effort.is_empty() {
            if feedback {
                // A Continue plan with nothing to observe cannot advance.
                error!(%root, "engine bug: Continue plan with no effects; abandoning saga");
            }
            return Ok(None);
        }
        self.in_flight = Some(root);
        Ok(Some(SagaBatch {
            root,
            effects,
            best_effort,
            feedback,
        }))
    }
}

/// Marker: the delivery must be released and retried later.
struct ReleaseDelivery;

/// How the referenced-PR precache ended.
enum PrecacheOutcome {
    /// Everything needed is cached (or acceptably uncached); proceed.
    Ready,
    /// GitHub was transiently unavailable: release the delivery.
    Release,
    /// The command's own PR is permanently unfetchable: refuse the command
    /// with this explanation.
    Deny(String),
}

/// The command a delivery carries, if the pipeline must authorize one.
///
/// Start/stop commands fire only from *created* comments — a command is an
/// utterance, not a state, and honoring edits would both re-run the command
/// on every unrelated edit (edits re-key dedupe by `updated_at`) and widen
/// the editor-impersonation hole (Codex M5 round 2, P1). Predecessor
/// declarations DO live in the comment (editing the declaring comment
/// legitimately updates or retracts it — the handler's semantics), so those
/// are authorized on edits too — against the *sender*, who on an edit is
/// the editor, not the original comment author.
fn command_in(event: &GitHubEvent, deps: &WorkerDeps) -> Option<(PrNumber, Command)> {
    let GitHubEvent::IssueComment(comment) = event else {
        return None;
    };
    if comment.sender_id == deps.bot_user_id {
        return None;
    }
    let pr = comment.pr_number?;
    let command = parse_command(&comment.body, &deps.bot_name)?;
    match comment.action {
        CommentAction::Created => Some((pr, command)),
        CommentAction::Edited => {
            matches!(command, Command::Predecessor(_)).then_some((pr, command))
        }
        CommentAction::Deleted => None,
    }
}

/// The PR whose predecessor declaration this event would *retract*, if any:
/// a deletion of the declaring comment, or an edit whose new body no longer
/// declares a predecessor. Mirrors the handler's retraction conditions
/// exactly (`handle_issue_comment`), so authorization gates precisely what
/// the handler would do.
fn retraction_in(
    event: &GitHubEvent,
    state: &crate::state::RepoState,
    deps: &WorkerDeps,
) -> Option<PrNumber> {
    let GitHubEvent::IssueComment(comment) = event else {
        return None;
    };
    if comment.sender_id == deps.bot_user_id {
        return None;
    }
    match comment.action {
        CommentAction::Created => None,
        // The handler retracts when the *owning* comment's body no longer
        // parses as a predecessor declaration.
        CommentAction::Edited => {
            let pr = comment.pr_number?;
            let owns = state
                .prs
                .get(&pr)
                .is_some_and(|c| c.predecessor_comment_id == Some(comment.comment_id));
            let still_declares = matches!(
                parse_command(&comment.body, &deps.bot_name),
                Some(Command::Predecessor(_))
            );
            (owns && !still_declares).then_some(pr)
        }
        // The handler looks the owning PR up by comment id.
        CommentAction::Deleted => state.prs.iter().find_map(|(pr, cached)| {
            (cached.predecessor_comment_id == Some(comment.comment_id)).then_some(*pr)
        }),
    }
}

/// The events that upsert a PR the bot has never seen into the cache, from a
/// fresh fetch. (The engine's own `refresh_events` diffs against a cached
/// entry; this is the cache-miss counterpart.)
pub(crate) fn cache_fill_events(
    pr: PrNumber,
    data: &PrData,
    merge_state: MergeStateStatus,
) -> Vec<StateEventPayload> {
    let mut events = vec![StateEventPayload::PrOpened {
        pr,
        head_sha: data.head_sha.clone(),
        head_ref: data.head_ref.clone(),
        base_ref: data.base_ref.clone(),
        is_draft: data.is_draft,
    }];
    match &data.state {
        PrState::Open => {}
        PrState::Merged { merge_commit_sha } => events.push(StateEventPayload::PrMerged {
            pr,
            merge_sha: merge_commit_sha.clone(),
        }),
        PrState::Closed => events.push(StateEventPayload::PrClosed { pr }),
    }
    events.push(StateEventPayload::PrMergeStateChanged {
        pr,
        status: merge_state,
    });
    events
}
