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

use super::authz::{AuthorDecision, RoleDecision, authorize_by_author, authorize_by_role};
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
    /// queued).
    Trigger(Trigger),
    /// Best-effort cleanup for a train the handlers aborted directly
    /// (worktree + final comment/status), run once the saga slot frees.
    AbortCleanup { root: PrNumber },
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
}

impl Processor {
    pub fn new(store: Store, deps: WorkerDeps) -> Processor {
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
        Processor {
            store,
            deps,
            pending: VecDeque::new(),
            in_flight: None,
            inherited_mid_flight,
        }
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

    /// Claims the next pending delivery, if any.
    pub fn claim(&mut self) -> Result<Option<Delivery>, StoreError> {
        self.store.claim_next_delivery()
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
                Err(e) => {
                    warn!(error = ?e, "cannot discover default branch; releasing delivery");
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
                        .commit_delivery(&id, &[], key.as_ref(), Utc::now())?;
                    self.best_effort_github(GitHubEffect::PostComment {
                        pr,
                        body: rejection,
                    });
                    return Ok(PipelineOutcome::Processed);
                }
                Err(ReleaseDelivery) => return self.release(&id),
            }

            let mut referenced = vec![pr];
            if let Command::Predecessor(target) = &command {
                referenced.push(*target);
            }
            if let Err(ReleaseDelivery) = self.precache(&referenced) {
                return self.release(&id);
            }
        }

        // The pure handler, then the atomic close: events + dedupe key +
        // `done` commit together.
        let ctx = HandlerCtx {
            bot_user_id: self.deps.bot_user_id,
            bot_name: self.deps.bot_name.clone(),
            now: Utc::now(),
        };
        let output = handle_event(&event, self.store.state(), &ctx);
        self.store
            .commit_delivery(&id, &output.events, key.as_ref(), Utc::now())?;
        self.clear_inherited_markers(&output.events);

        // Handler-terminated trains need worker-side cleanup (the engine's
        // own aborts carry cleanup in their plans; handler aborts have no
        // plan).
        for payload in &output.events {
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
        self.store.commit_delivery(id, &[], key, Utc::now())?;
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
        match authorize_by_author(command, comment.author_id, comment.pr_author_id) {
            AuthorDecision::Allowed => Ok(None),
            AuthorDecision::Denied { reason } => Ok(Some(reason)),
            AuthorDecision::NeedsRole => {
                let response = self
                    .deps
                    .github
                    .execute(GitHubEffect::GetCollaboratorPermission {
                        username: comment.author_login.clone(),
                    });
                match response {
                    Ok(GitHubResponse::CollaboratorPermission { role }) => {
                        match authorize_by_role(command, &role) {
                            RoleDecision::Allowed => Ok(None),
                            RoleDecision::Denied { reason } => Ok(Some(reason)),
                        }
                    }
                    Ok(other) => {
                        error!(?other, "permission lookup answered the wrong variant");
                        Err(ReleaseDelivery)
                    }
                    Err(e) => {
                        // Fail closed but retriable: an outage must neither
                        // grant admin rights nor permanently swallow a stop.
                        warn!(error = ?e, "cannot verify commenter role; releasing delivery");
                        Err(ReleaseDelivery)
                    }
                }
            }
        }
    }

    /// Fetches and caches referenced PRs the bot has never seen, so commands
    /// validate against facts instead of being dropped.
    fn precache(&mut self, referenced: &[PrNumber]) -> Result<(), ReleaseDelivery> {
        for &pr in referenced {
            if self.store.state().prs.contains_key(&pr) {
                continue;
            }
            match self.deps.github.execute(GitHubEffect::RefetchPr { pr }) {
                Ok(GitHubResponse::PrRefetched {
                    pr: data,
                    merge_state,
                }) => {
                    let events = cache_fill_events(pr, &data, merge_state);
                    if self.store.append_batch(&events, Utc::now()).is_err() {
                        return Err(ReleaseDelivery);
                    }
                }
                Ok(other) => {
                    error!(?other, "RefetchPr answered the wrong variant");
                    return Err(ReleaseDelivery);
                }
                Err(EffectError::Transient { detail }) => {
                    warn!(%pr, detail, "cannot fetch referenced PR; releasing delivery");
                    return Err(ReleaseDelivery);
                }
                Err(e) => {
                    // Permanent (e.g. the number does not exist): proceed
                    // uncached — validation rejects it loudly.
                    warn!(%pr, error = ?e, "referenced PR unfetchable; proceeding uncached");
                }
            }
        }
        Ok(())
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

    /// Executes a GitHub effect, logging (never propagating) failures.
    fn best_effort_github(&self, effect: GitHubEffect) {
        if let Err(e) = self.deps.github.execute(effect) {
            warn!(error = ?e, "best-effort GitHub effect failed (ignored)");
        }
    }

    /// Queues engine work, coalescing duplicates.
    fn queue(&mut self, work: PendingWork) {
        if !self.pending.contains(&work) {
            self.pending.push_back(work);
        }
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
                PendingWork::Trigger(Trigger::StopTrain { pr, force }) => {
                    let root = state.train_involving(pr).unwrap_or(pr);
                    (root, cascade::stop_train(state, pr, force, now))
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

        // Observation boundary: queued stops act here, so a human's stop
        // preempts whatever this saga would do next.
        let stops = self.take_queued_stops();

        // The start-cancel window: a stop naming this saga's root while no
        // train record exists yet means the outcomes in flight are the
        // start's preflight *reads* — discarding them loses nothing durable,
        // and letting `advance` run would start the train the user just
        // refused. (A stop naming a *descendant* in that sub-second window
        // cannot be resolved to the stack — there is no train record yet —
        // and takes the normal path's "no active train" answer; the
        // commenter re-issues once the status comment appears.)
        if stops.iter().any(|(pr, _)| *pr == root)
            && self.store.state().train_involving(root).is_none()
        {
            let others = stops.into_iter().filter(|(pr, _)| *pr != root).collect();
            let (mut cleanup, _) = self.apply_stops(others)?;
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

        let (mut cleanup, stopped_roots) = self.apply_stops(stops)?;
        if stopped_roots.contains(&root) {
            // A stop retired this train at the boundary: its planned
            // continuation must not run. The plan's events are already
            // durable; an intent among them whose effect never ran is the
            // same state a crash before dispatch leaves, which the recovery
            // contract already covers — and a stopped train is never
            // evaluated anyway.
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

    /// Extracts every queued `StopTrain` trigger, preserving other pending
    /// work in order.
    fn take_queued_stops(&mut self) -> Vec<(PrNumber, bool)> {
        let mut stops = Vec::new();
        let mut remaining = VecDeque::new();
        while let Some(work) = self.pending.pop_front() {
            match work {
                PendingWork::Trigger(Trigger::StopTrain { pr, force }) => stops.push((pr, force)),
                other => remaining.push_back(other),
            }
        }
        self.pending = remaining;
        stops
    }

    /// Applies stops now (terminal events only — cheap and safe at an
    /// observation boundary), returning their best-effort cleanup and the
    /// roots whose trains were actually retired.
    fn apply_stops(
        &mut self,
        stops: Vec<(PrNumber, bool)>,
    ) -> Result<(Vec<Effect>, HashSet<PrNumber>), StoreError> {
        let mut cleanup = Vec::new();
        let mut stopped_roots = HashSet::new();
        for (pr, force) in stops {
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

/// The command a delivery carries, if the pipeline must authorize one: a
/// created/edited non-bot comment on a PR whose body parses as a command.
fn command_in(event: &GitHubEvent, deps: &WorkerDeps) -> Option<(PrNumber, Command)> {
    let GitHubEvent::IssueComment(comment) = event else {
        return None;
    };
    if comment.author_id == deps.bot_user_id || comment.action == CommentAction::Deleted {
        return None;
    }
    let pr = comment.pr_number?;
    let command = parse_command(&comment.body, &deps.bot_name)?;
    Some((pr, command))
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
