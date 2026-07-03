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

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::path::PathBuf;

use chrono::Utc;
use tracing::{error, info, warn};

use crate::cascade::{self, Control, EffectError, Observation, ReplayFacts, StepPlan, observe};
use crate::commands::{Command, parse_command};
use crate::effects::github::{CommentData, GitHubEffect};
use crate::effects::{Effect, GitHubResponse, PrData};
use crate::git::{CommitIdentity, GitConfig};
use crate::persistence::event::StateEventPayload;
use crate::store::{Delivery, Store, StoreError};
use crate::types::{MergeStateStatus, PrNumber, PrState};
use crate::webhooks::dedupe::DedupeKey;
use crate::webhooks::events::{CommentAction, IssueCommentEvent};
use crate::webhooks::handlers::{HandlerCtx, Trigger, handle_event};
use crate::webhooks::{GitHubEvent, parse_webhook};

use super::authz::{
    AuthorDecision, RoleDecision, authorize_by_author, authorize_by_role, authorize_retraction,
};
use super::bootstrap::{CrawlInput, CrawlOutcome, TriggerComment};
use super::executor::{GitHubExec, SagaBatch};
use super::recovery::{CommentRecovery, decide_comment_recovery};

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
    /// How often the poll timer re-evaluates active trains (the
    /// missed-webhook fallback). Zero disables polling.
    pub poll_interval: std::time::Duration,
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
    /// A handler-emitted trigger (`LateAddition` is answered inline and
    /// never queued; `StartTrain`/`StopTrain` become the durable
    /// [`PendingWork::Start`]/[`PendingWork::Stop`]).
    Trigger(Trigger),
    /// An authorized start command, persisted in the `pending_commands`
    /// table by the delivery's close (an acknowledged start must survive a
    /// crash while it waits out an in-flight saga — Codex M5 round 19, P1;
    /// the mirror of round 2's stops). `id` is the durable row, deleted when
    /// the start is answered: `TrainStarted` appended, rejected, cancelled
    /// by a stop, or its preflight failed with a told-the-user comment.
    Start { id: i64, pr: PrNumber },
    /// An authorized stop command, persisted in the `pending_commands` table
    /// by the delivery's close (an acknowledged stop must survive a crash
    /// while it waits out an in-flight saga — Codex M5 round 2, P1). `id` is
    /// the durable row, deleted when the stop applies.
    Stop { id: i64, pr: PrNumber, force: bool },
    /// Best-effort cleanup for a train the handlers aborted directly
    /// (worktree + final comment/status), run once the saga slot frees.
    AbortCleanup { root: PrNumber },
    /// A TERMINAL train owes its status comment the final word: the
    /// comment is the only recovery source if the state DB is later lost,
    /// and left saying "active" it would resurrect a train the user
    /// stopped (monolith review, P1). The obligation is written by the
    /// store in the terminal event's own transaction and cleared only once
    /// an update is confirmed to have landed or the comment confirmed
    /// gone; this work item is the retry — a probe of the PR's comments,
    /// then the rewrite — queued at startup and at the stall cadence.
    StatusSync {
        root: PrNumber,
        /// The train INCARNATION whose comment is owed the final word: the
        /// stored comment id can be absent (posted, id not yet committed)
        /// or stale (a repost), so the retry resolves the live comment by
        /// the incarnation embedded in it.
        started_at: chrono::DateTime<Utc>,
    },
    /// A handler `TrainAborted` for the root whose saga is in flight,
    /// deferred to the observation boundary: committed mid-saga it would
    /// make `advance` see an inactive train and discard the outcomes of
    /// effects that already ran (Codex M5 round 2, P1 — the same ordering
    /// queued stops get). Volatile until applied; a crash loses it, and the
    /// train is then recovered like any inherited active train (the abort's
    /// *cause* events are durable, so the startup evaluation re-derives it).
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
    /// Active non-`Idle` trains inherited from a previous process, awaiting
    /// recovery (DESIGN §Restart safety): supplementary GitHub recovery
    /// runs at the root's first evaluation, worktree restart cleanup with
    /// its first effect batch. Roots leave the set when recovery succeeds
    /// or a lifecycle event ends the train (a stop needs no recovery).
    inherited_mid_flight: HashSet<PrNumber>,
    /// Recovered roots whose worktree still needs the one-time restart
    /// cleanup (`cleanup_worktree_on_restart`, run on the executor thread
    /// with the root's next batch — a mid-git-op crash leaves in-progress
    /// merges and dirt no later operation may see).
    needs_restart_cleanup: HashSet<PrNumber>,
    /// Set when supplementary recovery found GitHub unavailable: the worker
    /// loop arms the stall-retry timer, whose firing re-queues evaluations
    /// for the still-marked roots (nothing else may wake a traffic-less
    /// repo).
    retry_requested: bool,
    /// Deliveries released once because their triggering comment was not in
    /// the crawl's listing (GitHub is not read-after-write consistent). A
    /// second absence is believed. In-memory: a restart re-crawls anyway,
    /// by which point the listing has long caught up.
    absent_triggers: HashSet<String>,
    /// The incarnation whose obligation the in-flight batch is PROBING
    /// (`ListComments` on the root), keyed by that root: the probe's
    /// outcome decides between the rewrite and clearing the obligation.
    sync_probes: HashMap<PrNumber, chrono::DateTime<Utc>>,
    /// Active-train evaluations owed at startup, queued when the durable
    /// backlog first drains (`Some` until then; see [`Processor::claim`]).
    startup_evaluates: Option<Vec<PrNumber>>,
    /// The durable row of the start whose preflight saga is in flight; the
    /// row is deleted when the start is answered (see [`PendingWork::Start`]).
    active_start: Option<(i64, PrNumber)>,
}

impl Processor {
    pub fn new(store: Store, deps: WorkerDeps) -> Result<Processor, StoreError> {
        // Every inherited active train runs recovery at its first
        // evaluation (DESIGN §Restart safety: worktree cleanup +
        // supplementary GitHub recovery) — including Idle-phase trains: no
        // git operation was mid-flight for those, but their status comment
        // may still be ahead of a restored-from-backup store, or may have
        // been deleted while the process was down.
        let inherited_mid_flight: HashSet<PrNumber> = store
            .state()
            .active_trains
            .values()
            .filter(|t| t.state.is_active())
            .map(|t| t.original_root_pr)
            .collect();
        for root in &inherited_mid_flight {
            info!(
                %root,
                "train was active when the previous process died; it will \
                 be recovered at its first evaluation"
            );
        }

        // Reloaded not-yet-answered commands (Codex M5 rounds 2 and 19)
        // queue immediately, in `pending_commands` id order: a reloaded
        // command's delivery closed before every backlog delivery arrived,
        // so front-of-queue IS the user's utterance order — both among the
        // reloaded commands and against any command the backlog still
        // carries. Deferring them behind the backlog drain (round 19's
        // first shape) inverted that order: a post-restart `stop` was
        // answered "no active merge train" and the older reloaded start
        // then started the train the user had just refused (round 20).
        // Unlike the startup *evaluations* below, immediate application
        // cannot outrun acked train-terminating deliveries: a start's plan
        // is read-only preflight whose TrainStarted lands only at the
        // observation boundary (which the worker runs against the drained
        // backlog), and a stop appends terminal events valid at any
        // staleness.
        let mut pending: VecDeque<PendingWork> = store
            .pending_commands()?
            .into_iter()
            .map(|(id, command)| match command {
                crate::store::DurableCommand::Start { pr } => PendingWork::Start { id, pr },
                crate::store::DurableCommand::Stop { pr, force } => {
                    PendingWork::Stop { id, pr, force }
                }
            })
            .collect();
        // Terminal status-comment updates a previous process never
        // confirmed: owed until they land (cheap, GitHub-only,
        // order-independent).
        pending.extend(store.owed_status_syncs()?.into_iter().map(|owed| {
            PendingWork::StatusSync {
                root: owed.root,
                started_at: owed.started_at,
            }
        }));
        // Startup *evaluations* of active trains (so an acknowledged CI
        // success whose trigger died with the process still resumes a
        // parked train) are computed here but queue only once the durable
        // backlog first drains — see [`Processor::claim`] — or their
        // effects would run against state that predates already-acked
        // deliveries (Codex M5 round 6, P1: e.g. a queued topology-change
        // abort overtaken by a squash).
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
            needs_restart_cleanup: HashSet::new(),
            retry_requested: false,
            absent_triggers: HashSet::new(),
            sync_probes: HashMap::new(),
            startup_evaluates: Some(startup_evaluates),
            active_start: None,
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

    pub fn github(&self) -> &GitHubExec {
        &self.deps.github
    }

    pub fn stall_retry_delay(&self) -> std::time::Duration {
        self.deps.stall_retry_delay
    }

    /// How often the poll timer fires (zero disables it).
    pub fn poll_interval(&self) -> std::time::Duration {
        self.deps.poll_interval
    }

    /// A deterministic per-repo initial poll delay in `[0, poll_interval)`,
    /// derived from the repo identity, so many repos restarting together do
    /// not poll in lockstep (DESIGN §Distributed polling). No RNG — the
    /// stagger is a pure function of `owner/repo`, stable across restarts.
    pub fn poll_stagger(&self) -> std::time::Duration {
        let interval = self.deps.poll_interval;
        if interval.is_zero() {
            return std::time::Duration::ZERO;
        }
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.deps.git.owner.hash(&mut hasher);
        self.deps.git.repo.hash(&mut hasher);
        let frac = hasher.finish() % interval.as_millis().max(1) as u64;
        std::time::Duration::from_millis(frac)
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

        // First contact — a fresh store (brand-new repo, or a LOST state
        // db) has an empty default branch. Webhooks only describe the
        // future, so bootstrap crawls the present: settings, open and
        // recently merged PRs, their comments (predecessor topology,
        // author-gated), and the bot's own status comments (train recovery
        // — DESIGN §Bootstrap Phase 2). Any failure releases the delivery:
        // there is no safe way to process anything without the bootstrap,
        // and closing would silently drop webhooks GitHub will never
        // resend — the repo's queue pauses at the stall cadence (which
        // also heals "permanent" auth errors the moment the operator fixes
        // the token).
        //
        // The crawl must NOT consume THIS delivery's own comment as a
        // historical declaration: it is live input the command handler
        // below is about to process, and pre-recording it (round 9 keeps
        // merged-predecessor edges) would let the handler see it as
        // already-owned and skip the `LateAddition` answer a genuine
        // late-addition command deserves (Codex crawl review round 10).
        //
        // Two more things about the trigger only the crawl can act on
        // (Codex crawl review, P1s): an authorized RETRACTION carried by
        // the trigger is applied as a tombstone (the deleted comment may
        // have owned the declaration, and with it gone the crawl would
        // promote an older surviving comment the handler can no longer see
        // as retracted), and a `created`/`edited` trigger whose comment is
        // no longer in the PR's listing — or a `created` one whose comment
        // has since been edited — is a stale redelivery: handling it would
        // recreate a retracted declaration, or record a body the comment no
        // longer has, so it is closed instead.
        if self.store.state().default_branch.is_empty() {
            let trigger = match &event {
                GitHubEvent::IssueComment(c) => Some(TriggerComment {
                    id: c.comment_id,
                    retraction: trigger_retraction(c, &self.deps),
                    authorized: c.sender_id != self.deps.bot_user_id
                        && c.sender_id == c.pr_author_id,
                }),
                _ => None,
            };
            let freshness = TriggerFreshness::of(&event);
            let retried = self.absent_triggers.contains(&id);
            match self.bootstrap_crawl(&event.referenced_prs(), trigger, freshness, retried)? {
                Bootstrap::Unavailable => return self.release(&id),
                Bootstrap::TriggerAbsent => {
                    self.absent_triggers.insert(id.clone());
                    return self.release(&id);
                }
                Bootstrap::Landed { outcome, stale } => {
                    if stale {
                        // The crawl and the close of the stale delivery
                        // commit TOGETHER: were the crawl to land alone and
                        // the process die before the close, the retried
                        // delivery would find a bootstrapped store, skip
                        // this check, and be handled after all (Codex crawl
                        // review round 2, P1). The crawl was recomputed
                        // without the stale trigger, so no receipt is owed
                        // for it either (round 4, P2).
                        self.store.commit_delivery(
                            &id,
                            &outcome.events,
                            key.as_ref(),
                            &[],
                            Utc::now(),
                        )?;
                        self.after_bootstrap(outcome, None)?;
                        info!(delivery = %id, "closed: the trigger is stale against the crawl");
                        return Ok(PipelineOutcome::Processed);
                    }
                    self.store.append_batch(&outcome.events, Utc::now())?;
                    self.after_bootstrap(outcome, trigger)?;
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

        // Any comment event *performed by the bot* is the bot's own action
        // (its own comments have author == bot and the handler ignores them,
        // but an edit/deletion the bot performs on a USER's comment carries
        // author == user, sender == bot — and the handler's self-guard keys
        // off the author). The bot never issues commands, so close before
        // handling: otherwise a bot-actor edit could declare or retract a
        // predecessor with no authorization gate (Codex M5 round 18).
        if let GitHubEvent::IssueComment(comment) = &event
            && comment.sender_id == self.deps.bot_user_id
        {
            return self.close(&id, key.as_ref(), "bot-actor comment event");
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

        // Authorized commands persist in the close transaction: a command
        // may wait out a multi-minute saga before the slot frees, and an
        // acknowledged command must survive a crash in that window — the
        // delivery is `done` and deduped, so nothing external replays it
        // (Codex M5 rounds 2 and 19, both P1).
        let commands: Vec<crate::store::DurableCommand> = output
            .triggers
            .iter()
            .filter_map(|t| match t {
                Trigger::StartTrain { pr } => Some(crate::store::DurableCommand::Start { pr: *pr }),
                Trigger::StopTrain { pr, force } => Some(crate::store::DurableCommand::Stop {
                    pr: *pr,
                    force: *force,
                }),
                _ => None,
            })
            .collect();

        // A retraction's durable tombstone, captured against the PRE-commit
        // state (the commit clears the edge). `PredecessorRemoved` is
        // emitted only when an AUTHORIZED retraction is applied, and the
        // deletion behind it leaves no trace in GitHub's present — an older
        // declaration comment on the PR would resurrect the edge in a
        // lost-DB crawl, whose recovered train could then DRIVE the
        // descendant the user unstacked (owner ruling 2026-07-18). The
        // receipt comment outlives the DB; the crawl reads it as a
        // tombstone for every declaration on the PR up to the RETRACTED
        // comment's id (its anchor — the receipt's own id would race a
        // re-declaration posted while this delivery sat in the backlog).
        // Posted best-effort after the close, like every status update — a
        // receipt lost to an outage re-opens the window for that one
        // retraction (documented residual).
        let retraction_receipts: Vec<(PrNumber, crate::types::CommentId, Option<PrNumber>)> =
            events
                .iter()
                .filter_map(|e| match e {
                    StateEventPayload::PredecessorRemoved { pr, comment_id } => Some((
                        *pr,
                        *comment_id,
                        self.store.state().prs.get(pr).and_then(|c| c.predecessor),
                    )),
                    _ => None,
                })
                .collect();

        let command_ids =
            self.store
                .commit_delivery(&id, &events, key.as_ref(), &commands, Utc::now())?;
        self.clear_inherited_markers(&events);
        for (&command, id) in commands.iter().zip(command_ids) {
            self.queue(match command {
                crate::store::DurableCommand::Start { pr } => PendingWork::Start { id, pr },
                crate::store::DurableCommand::Stop { pr, force } => {
                    PendingWork::Stop { id, pr, force }
                }
            });
        }

        // Handler-terminated trains need worker-side cleanup (the engine's
        // own aborts carry cleanup in their plans; handler aborts have no
        // plan).
        for payload in &events {
            if let StateEventPayload::TrainAborted { root_pr, .. } = payload {
                self.queue(PendingWork::AbortCleanup { root: *root_pr });
            }
        }

        for (pr, retracted, predecessor) in retraction_receipts {
            self.best_effort_github(GitHubEffect::PostComment {
                pr,
                body: crate::status::format_retraction_receipt(pr, retracted, predecessor),
            });
        }

        // A handler-committed terminal event (a topology abort) may owe a
        // status sync that no later outcome refers to.
        self.queue_owed_status_syncs()?;

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
                    comment_id,
                } => {
                    // Detected by M3, answered here: the reconciliation flow
                    // for late additions is explicitly deferred. The answer
                    // is a rejection RECEIPT for the declaring comment: live
                    // recorded nothing for it, and a lost-DB crawl must not
                    // replay it as a declaration.
                    self.best_effort_github(GitHubEffect::PostComment {
                        pr,
                        body: crate::status::format_rejection_receipt(
                            pr,
                            comment_id,
                            &format!(
                                "PR #{merged_predecessor} is already merged. Adding a PR onto a \
                                 merged predecessor (\"late addition\") is not supported yet — \
                                 rebase onto the default branch, or restart the train."
                            ),
                        ),
                    });
                }
                // Persisted (and queued) above.
                Trigger::StartTrain { .. } | Trigger::StopTrain { .. } => {}
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
                | PendingWork::StatusSync { .. }
        );
        if idempotent && self.pending.contains(&work) {
            return;
        }
        self.pending.push_back(work);
    }

    /// The first-contact crawl (DESIGN §Bootstrap algorithm, Phase 2):
    /// fetches settings, open + recently merged PRs, and every crawled
    /// PR's comments, then appends [`super::bootstrap::crawl_events`]'s
    /// result as ONE atomic batch — a crash or a released retry re-crawls
    /// from nothing (idempotent reads, no partial state). Adopted ACTIVE
    /// trains are marked for M6 recovery, deferred behind the backlog
    /// drain like every other recovery. Returns `false` when GitHub was
    /// unavailable (any failure: transient, permanent, or a wrong
    /// variant): the caller releases the delivery and the queue pauses at
    /// the stall cadence — there is no safe degraded answer at bootstrap.
    fn bootstrap_crawl(
        &mut self,
        seed_prs: &[PrNumber],
        trigger: Option<TriggerComment>,
        freshness: Option<TriggerFreshness>,
        retried: bool,
    ) -> Result<Bootstrap, StoreError> {
        /// How many days of merged PRs the crawl considers: predecessor
        /// targets and mid-cascade roots older than this are treated as
        /// history (DESIGN bounds the resurrection window the same way).
        const MERGED_SINCE_DAYS: u32 = 30;
        /// How many PRs one bootstrap will list comments for. Each listing
        /// is a paginated API call, and nothing is committed until the
        /// whole crawl finishes, so an unbounded crawl of a very large
        /// repository can exhaust the rate-limit window, discard all its
        /// progress, and never bootstrap at all (Codex crawl review round
        /// 6, P2). Beyond the cap the crawl proceeds with the PRs it has:
        /// their cache entries stand, and topology it could not read is
        /// reported as loudly as we can.
        const MAX_COMMENT_LISTINGS: usize = 1000;
        /// How many individually-fetched PRs one bootstrap will pay for.
        /// Referenced-but-uncrawled PRs cost one `GetPr` each, and a PR
        /// author can leave arbitrarily many distinct declaration targets;
        /// unbounded, a long junk history could exhaust the API quota, and
        /// the crawl abandons all progress on a rate limit and starts over
        /// — pausing the repo's queue indefinitely (Codex crawl review
        /// round 3, P1). Beyond the cap the remaining PRs are treated as
        /// UNFETCHABLE: declaration edges onto them drop, and a train that
        /// references one aborts loudly rather than recovering broken.
        const MAX_REFERENCED_FETCHES: usize = 200;

        macro_rules! fetch {
            ($effect:expr, $expected:pat => $value:expr) => {
                match self.deps.github.execute($effect) {
                    Ok($expected) => $value,
                    Ok(other) => {
                        error!(?other, "bootstrap fetch answered the wrong variant");
                        return Ok(Bootstrap::Unavailable);
                    }
                    Err(e) => {
                        warn!(error = ?e, "bootstrap crawl failed; the repo's queue \
                               pauses until it succeeds");
                        return Ok(Bootstrap::Unavailable);
                    }
                }
            };
        }

        let settings = fetch!(
            GitHubEffect::GetRepoSettings,
            GitHubResponse::RepoSettings(s) => s
        );
        if settings.default_branch.is_empty() {
            error!("repository settings carry an empty default branch");
            return Ok(Bootstrap::Unavailable);
        }
        let open = fetch!(GitHubEffect::ListOpenPrs, GitHubResponse::PrList(prs) => prs);
        let (merged, may_be_incomplete) = fetch!(
            GitHubEffect::ListRecentlyMergedPrs { since_days: MERGED_SINCE_DAYS },
            GitHubResponse::RecentlyMergedPrList { prs, may_be_incomplete } => (prs, may_be_incomplete)
        );
        if may_be_incomplete {
            warn!(
                "the recently-merged crawl hit its pagination limit; trains \
                 rooted at older merged PRs will not be recovered"
            );
        }
        // Discover PRs to a fixpoint. The list endpoints miss a PR closed
        // *unmerged* during the gap, but the wake-up webhook names some PRs
        // (`seed_prs`) and the crawl surfaces more — declaration targets and
        // adopted-train members it referenced but did not fetch. A closed
        // root reachable only through its descendants' declarations is found
        // this way: fetch the referenced PRs, list their comments, re-crawl,
        // repeat until nothing new is referenced (Codex crawl review rounds
        // 6–7). `attempted` bounds it — every PR is fetched at most once
        // (a 404 counts), and the PR universe is finite — so it terminates.
        let mut crawled: Vec<PrData> = open;
        crawled.extend(merged);
        let mut attempted: HashSet<PrNumber> = crawled.iter().map(|p| p.number).collect();
        // Referenced PRs that a permanent `GetPr` failure could not fetch
        // (deleted, or the token lost access). `crawl_events` aborts a train
        // that references one rather than recover it into an `UnknownPr`
        // stall (Codex crawl review round 12).
        let mut unfetchable: HashSet<PrNumber> = HashSet::new();
        let mut comments: Vec<(PrNumber, Vec<CommentData>)> = Vec::new();
        let mut listed: HashSet<PrNumber> = HashSet::new();
        let mut pending: Vec<PrNumber> = seed_prs
            .iter()
            .copied()
            .filter(|pr| !attempted.contains(pr))
            .collect();

        let mut fetched = 0usize;
        let outcome = loop {
            for pr in std::mem::take(&mut pending) {
                if !attempted.insert(pr) {
                    continue;
                }
                if fetched >= MAX_REFERENCED_FETCHES {
                    warn!(
                        %pr,
                        "the bootstrap crawl hit its referenced-PR fetch cap; \
                         treating the rest as unfetchable"
                    );
                    unfetchable.insert(pr);
                    continue;
                }
                fetched += 1;
                match self.deps.github.execute(GitHubEffect::GetPr { pr }) {
                    Ok(GitHubResponse::Pr(data)) => crawled.push(data),
                    Err(e @ EffectError::Transient { .. }) => {
                        warn!(%pr, error = ?e, "cannot fetch a referenced PR; bootstrap paused");
                        return Ok(Bootstrap::Unavailable);
                    }
                    other => {
                        warn!(%pr, ?other, "referenced PR unfetchable; skipping it in the crawl");
                        unfetchable.insert(pr);
                    }
                }
            }
            let unlisted: Vec<PrNumber> = crawled
                .iter()
                .map(|p| p.number)
                .filter(|pr| !listed.contains(pr))
                .collect();
            for pr in unlisted {
                if listed.len() >= MAX_COMMENT_LISTINGS {
                    error!(
                        %pr, listed = listed.len(),
                        "the bootstrap crawl hit its comment-listing cap; predecessor \
                         topology and trains on the remaining PRs cannot be recovered \
                         — operator action likely required (split the repository, or \
                         raise the cap)"
                    );
                    break;
                }
                listed.insert(pr);
                let pr_comments = fetch!(
                    GitHubEffect::ListComments { pr },
                    GitHubResponse::Comments(c) => c
                );
                comments.push((pr, pr_comments));
            }
            let outcome = super::bootstrap::crawl_events(&CrawlInput {
                default_branch: &settings.default_branch,
                crawled_prs: &crawled,
                comments: &comments,
                bot_name: &self.deps.bot_name,
                bot_user_id: self.deps.bot_user_id,
                trigger,
                unfetchable: &unfetchable,
                now: Utc::now(),
            });
            let fresh: Vec<PrNumber> = outcome
                .referenced_uncrawled
                .iter()
                .copied()
                .filter(|pr| !attempted.contains(pr))
                .collect();
            if fresh.is_empty() {
                break outcome;
            }
            pending = fresh;
        };
        info!(
            default_branch = %settings.default_branch,
            crawled_prs = crawled.len(),
            recovered_trains = outcome.recovered_roots.len(),
            "bootstrapped the repo from a crawl"
        );
        // Is the triggering delivery CURRENT against the present the crawl
        // just fetched? A comment trigger must still be listed, unedited if
        // the payload is a `created`, and its body must match the listed
        // body (an `edited` payload superseded by a later edit is as stale
        // as a deleted one). A PR trigger must agree with the crawled PR:
        // an old `closed` redelivered after the PR was reopened would
        // otherwise close — and abort the recovered train of — a PR the
        // crawl just cached as open (Codex crawl review round 2, P1s). The
        // crawl itself stands either way; only the delivery is stale.
        let stale = match &freshness {
            Some(TriggerFreshness::Comment {
                pr,
                id: trigger_id,
                body,
                created,
            }) => {
                let listed_comments: Vec<&CommentData> = comments
                    .iter()
                    .filter(|(p, _)| p == pr)
                    .flat_map(|(_, cs)| cs.iter())
                    .collect();
                match listed_comments.iter().find(|c| c.id == *trigger_id) {
                    // Present but MOVED ON: an edit superseded this
                    // payload, or a `created` payload's comment has since
                    // been edited. Stale.
                    Some(c) => (*created && c.edited) || c.body != *body,
                    // ABSENT. GitHub is not read-after-write consistent, so
                    // a just-created comment can be missing from a listing
                    // taken moments later — absence alone is not proof the
                    // comment was deleted, and closing on it would silently
                    // lose a valid command (Codex crawl review round 6,
                    // P1). Release instead: the delivery is retried at the
                    // stall cadence, by which point the listing has caught
                    // up or the comment is genuinely gone (the retry runs
                    // after the crawl landed, so the handler decides).
                    // ONCE: the listing may simply not have caught up, so
                    // the delivery is released and re-crawled. If it is
                    // still absent on that retry, the comment really is
                    // gone and the delivery is stale.
                    None if listed.contains(pr) && !retried => {
                        info!(
                            %pr, comment = %trigger_id,
                            "the triggering comment is not in the listing yet; retrying once"
                        );
                        return Ok(Bootstrap::TriggerAbsent);
                    }
                    None => listed.contains(pr),
                }
            }
            Some(pr_trigger @ TriggerFreshness::PullRequest { pr, .. }) => crawled
                .iter()
                .find(|p| p.number == *pr)
                .is_some_and(|present| pr_trigger.disagrees_with(present)),
            None => false,
        };
        // A trigger a receipt already killed is stale too: the crawl
        // recorded nothing for it, and the handler must not resurrect it.
        let stale = stale || outcome.trigger_tombstoned;
        // A stale delivery shapes NOTHING: re-crawl without it, so its
        // retraction does not tombstone surviving declarations and its own
        // comment — live input no handler will now process — is recorded
        // like any other (Codex crawl review round 4, P2).
        let outcome = if stale && trigger.is_some() {
            super::bootstrap::crawl_events(&CrawlInput {
                default_branch: &settings.default_branch,
                crawled_prs: &crawled,
                comments: &comments,
                bot_name: &self.deps.bot_name,
                bot_user_id: self.deps.bot_user_id,
                trigger: None,
                unfetchable: &unfetchable,
                now: Utc::now(),
            })
        } else {
            outcome
        };
        Ok(Bootstrap::Landed { outcome, stale })
    }

    /// What every landed crawl owes, after its events are committed: the
    /// inherited markers, the abort cleanups, the trigger retraction's
    /// receipt (only for a trigger that was actually applied), and the
    /// recovery requeue.
    fn after_bootstrap(
        &mut self,
        outcome: CrawlOutcome,
        trigger: Option<TriggerComment>,
    ) -> Result<(), StoreError> {
        self.clear_inherited_markers(&outcome.events);
        // The trigger's own retraction was applied as a tombstone by the
        // crawl; make it durable like every other retraction, so the next
        // lost-DB crawl — which will not see this delivery — applies it too.
        if let Some(TriggerComment {
            id,
            retraction: Some(pr),
            ..
        }) = trigger
        {
            self.best_effort_github(GitHubEffect::PostComment {
                pr,
                body: crate::status::format_retraction_receipt(pr, id, None),
            });
        }
        // A train the crawl aborted (its stack was extended during the gap)
        // needs the same worker-side cleanup a handler abort gets — stale
        // worktree removal + a final status comment (the engine's own
        // aborts carry cleanup in their plans; this one has no plan).
        for payload in &outcome.events {
            if let StateEventPayload::TrainAborted { root_pr, .. } = payload {
                self.queue(PendingWork::AbortCleanup { root: *root_pr });
            }
        }
        for root in outcome.recovered_roots {
            self.inherited_mid_flight.insert(root);
        }
        // The recoveries defer behind the backlog drain, exactly like
        // startup evaluations (the round-6/round-20 gating); owed status
        // syncs (a synthesized completion) queue too.
        self.requeue_marked_recoveries()?;
        Ok(())
    }

    /// Supplementary GitHub recovery for a train inherited mid-cascade
    /// (DESIGN §Recovery precedence, §Supplementary GitHub recovery), run
    /// once at the root's first post-restart evaluation. Fetches the root
    /// PR's comments and lets [`decide_comment_recovery`] rule:
    ///
    /// - the bot's status comment is AHEAD (`recovery_seq`) — only possible
    ///   when local durable state regressed, i.e. the DB was restored from
    ///   a backup — its record is adopted (`TrainRecordAdopted`), which is
    ///   what stops the cascade re-running a squash the world already saw;
    /// - the comment is gone — the dangling id is cleared (same event) so
    ///   the engine re-posts its off-disk backup;
    /// - otherwise local state stands.
    ///
    /// On success the root is unmarked and flagged for the one-time
    /// worktree restart cleanup (executed with its next batch). Returns
    /// `false` when GitHub was unavailable: the root stays marked, the
    /// caller parks the evaluation, and `retry_requested` asks the worker
    /// loop to arm the stall-retry timer. A *permanent* failure is treated
    /// the same way, deliberately: proceeding unverified risks exactly the
    /// double-squash this check exists to prevent, and the stall cadence
    /// heals "permanent" auth errors the moment the operator fixes the
    /// token (`stop` remains available throughout).
    fn recover_inherited(&mut self, root: PrNumber) -> Result<bool, StoreError> {
        let local = self
            .store
            .state()
            .active_trains
            .get(&root)
            .expect("caller checked the train is active")
            .clone();
        let comments = match self
            .deps
            .github
            .execute(GitHubEffect::ListComments { pr: root })
        {
            Ok(GitHubResponse::Comments(comments)) => comments,
            Ok(other) => {
                error!(?other, "ListComments answered the wrong variant");
                self.retry_requested = true;
                return Ok(false);
            }
            Err(e @ EffectError::Transient { .. }) => {
                warn!(%root, error = ?e, "cannot fetch status comments; recovery parked");
                self.retry_requested = true;
                return Ok(false);
            }
            Err(e) => {
                error!(
                    %root, error = ?e,
                    "cannot fetch status comments and the failure is permanent; \
                     recovery is PARKED at the stall cadence — operator action \
                     likely required (token scopes?). `@merge-train stop` still \
                     works."
                );
                self.retry_requested = true;
                return Ok(false);
            }
        };
        match decide_comment_recovery(&local, &comments, self.deps.bot_user_id) {
            CommentRecovery::Adopt(record) => {
                info!(
                    %root,
                    local_seq = local.recovery_seq,
                    remote_seq = record.recovery_seq,
                    "status comment is ahead of the store (restored from \
                     backup?); adopting its record"
                );
                self.store.append_batch(
                    &[StateEventPayload::TrainRecordAdopted {
                        root_pr: root,
                        record: *record,
                    }],
                    Utc::now(),
                )?;
            }
            CommentRecovery::RefreshComment(comment_id) => {
                // The recorded comment is live but behind the store (the
                // common crash shape: events commit before the best-effort
                // update runs) or mangled. Rewrite it BEFORE resuming: the
                // comment is the only recovery source if the DB is lost in
                // the resume window (Codex M6 review round 3).
                info!(%root, %comment_id, "status comment is behind the store; refreshing");
                if !self.refresh_status_comment(root, &local, comment_id)? {
                    return Ok(false);
                }
            }
            CommentRecovery::RepairCommentId(comment_id) => {
                info!(%root, %comment_id, "status comment moved; repairing the id");
                self.store.append_batch(
                    &[StateEventPayload::StatusCommentPosted {
                        root_pr: root,
                        comment_id,
                    }],
                    Utc::now(),
                )?;
                // The found comment's content is at best as old as the
                // local record: bring the backup current before resuming
                // (a crash between the append above and this refresh
                // re-decides as RefreshComment — same repair, converges).
                if !self.refresh_status_comment(root, &local, comment_id)? {
                    return Ok(false);
                }
            }
            CommentRecovery::RepostBackup => {
                // Re-establish the off-disk backup NOW: the engine's own
                // self-heal runs only at idle evaluations, and a cascade
                // resumed mid-phase may chain to completion without passing
                // one. The fresh id lands via `StatusCommentPosted` — NOT
                // `TrainRecordAdopted`, which is a ledger boundary: here
                // the local intent ledger is genuine and recovery needs it.
                info!(%root, "status comment is gone; re-posting the backup");
                let mut record = local;
                record.status_comment_id = None;
                match crate::status::format::format_status_comment(
                    &record,
                    "🚂 Merge train recovered after a restart.",
                ) {
                    Ok(body) => {
                        match self
                            .deps
                            .github
                            .execute(GitHubEffect::PostComment { pr: root, body })
                        {
                            Ok(GitHubResponse::CommentPosted { id }) => {
                                self.store.append_batch(
                                    &[StateEventPayload::StatusCommentPosted {
                                        root_pr: root,
                                        comment_id: id,
                                    }],
                                    Utc::now(),
                                )?;
                            }
                            // Transient AND permanent park identically:
                            // nothing durable happened yet, and resuming
                            // without the disaster-recovery backup when
                            // GitHub already refuses writes only means the
                            // cascade's own effects fail next. The stall
                            // cadence heals "permanent" auth errors the
                            // moment the operator fixes the token; `stop`
                            // works throughout.
                            Err(e) => {
                                warn!(%root, error = ?e, "cannot re-post the status comment; \
                                       recovery parked");
                                self.retry_requested = true;
                                return Ok(false);
                            }
                            Ok(other) => {
                                error!(?other, "PostComment answered the wrong variant");
                                self.retry_requested = true;
                                return Ok(false);
                            }
                        }
                    }
                    Err(e) => {
                        // Unreachable in practice (a record that formatted
                        // before fits now — truncation keeps it bounded):
                        // resume on local state, loudly and without the
                        // backup, rather than wedge recovery forever on a
                        // record no retry can fix.
                        error!(
                            %root, error = %e,
                            "recovered record cannot be formatted as a status \
                             comment; resuming WITHOUT the off-disk backup"
                        );
                    }
                }
            }
            CommentRecovery::KeepLocal => {}
        }
        self.inherited_mid_flight.remove(&root);
        self.needs_restart_cleanup.insert(root);
        info!(%root, "recovered an inherited mid-cascade train; resuming");
        Ok(true)
    }

    /// Rewrites the live status comment from `record` so the off-disk
    /// backup is current before a recovered train resumes. Returns `false`
    /// (with the retry timer requested) when GitHub was unavailable —
    /// parking the whole recovery, exactly like the re-post path. A record
    /// that cannot be formatted resumes without the refresh, loudly
    /// (unreachable in practice: a record that formatted before fits now).
    fn refresh_status_comment(
        &mut self,
        root: PrNumber,
        record: &crate::types::TrainRecord,
        comment_id: crate::types::CommentId,
    ) -> Result<bool, StoreError> {
        let mut record = record.clone();
        record.status_comment_id = Some(comment_id);
        match crate::status::format::format_status_comment(
            &record,
            "🚂 Merge train recovered after a restart.",
        ) {
            Ok(body) => match self
                .deps
                .github
                .execute(GitHubEffect::UpdateComment { comment_id, body })
            {
                Ok(_) => Ok(true),
                Err(e) => {
                    // Transient and permanent park identically (see the
                    // re-post path). A permanent 404 — deleted between the
                    // list and this update — re-decides as RepostBackup on
                    // the retry.
                    warn!(%root, error = ?e, "cannot refresh the status comment; recovery parked");
                    self.retry_requested = true;
                    Ok(false)
                }
            },
            Err(e) => {
                error!(
                    %root, error = %e,
                    "recovered record cannot be formatted as a status \
                     comment; resuming WITHOUT refreshing the backup"
                );
                Ok(true)
            }
        }
    }

    /// Whether the worker loop should arm the stall-retry timer (set when
    /// supplementary recovery found GitHub unavailable, or a terminal
    /// status-comment sync is owed). Clears on read.
    pub fn take_retry_request(&mut self) -> bool {
        std::mem::take(&mut self.retry_requested)
    }

    /// Bookkeeping over a batch's best-effort outcomes: whether an OWED
    /// terminal status-comment update landed. Status updates are
    /// best-effort by design (a slow GitHub must not stall the cascade),
    /// but for a terminal train the comment is the only recovery source
    /// should the state DB be lost, and a comment left saying "active"
    /// would resurrect a train the user stopped or that already finished
    /// (monolith review, P1). The store wrote the obligation with the
    /// terminal event; a confirmed success clears it, and any failure —
    /// transient, or permanent such as revoked credentials — leaves it
    /// owed and arms the stall-retry timer. Only the retry's probe may
    /// conclude the comment is gone. The residual is a DB loss during the
    /// same outage: documented, bounded by the outage.
    pub fn note_best_effort(
        &mut self,
        outcomes: &[crate::cascade::EffectOutcome],
    ) -> Result<(), StoreError> {
        // Matched by COMMENT, never by the batch's root: an observation
        // boundary appends another train's terminal cleanup to whichever
        // batch is in flight, so its update rides a foreign root (Codex
        // terminal-sync review round 3, P1).
        let owed = self.store.owed_status_syncs()?;
        if owed.is_empty() {
            return Ok(());
        }
        for outcome in outcomes {
            let Effect::GitHub(GitHubEffect::UpdateComment { comment_id, .. }) = &outcome.effect
            else {
                continue;
            };
            let Some(sync) = owed.iter().find(|o| o.comment_id == Some(*comment_id)) else {
                continue;
            };
            match &outcome.result {
                Ok(_) => self
                    .store
                    .delete_owed_status_sync(sync.root, sync.started_at)?,
                Err(e) => {
                    warn!(
                        root = %sync.root, comment = %comment_id,
                        error = ?e, "terminal status comment update failed; sync owed"
                    );
                    self.retry_requested = true;
                }
            }
        }
        Ok(())
    }

    /// The outcome of a status-sync probe (`ListComments` on the root): the
    /// comment is there — rewrite it from the owed record, as a batch whose
    /// best-effort outcome `note_best_effort` confirms; gone — nothing
    /// stale survives, clear the obligation; unknown (the listing failed)
    /// — keep it and retry at the stall cadence.
    fn on_sync_probe(
        &mut self,
        root: PrNumber,
        started_at: chrono::DateTime<Utc>,
        outcomes: Vec<crate::cascade::EffectOutcome>,
    ) -> Result<Option<SagaBatch>, StoreError> {
        let Some(owed) = self
            .store
            .owed_status_syncs()?
            .into_iter()
            .find(|o| o.root == root && o.started_at == started_at)
        else {
            return self.pump();
        };
        let listing = outcomes.into_iter().find_map(|o| match o.result {
            Ok(crate::cascade::EffectResponse::GitHub(GitHubResponse::Comments(comments))) => {
                Some(comments)
            }
            _ => None,
        });
        let Some(comments) = listing else {
            warn!(%root, "status-sync probe failed; retrying at the stall cadence");
            self.retry_requested = true;
            return self.pump();
        };
        // The stored id FIRST, whatever the body says: a live comment at
        // that id is this train's backup, and one whose body is mangled or
        // behind needs the rewrite most (the M6 recovery path calls that
        // `RefreshComment`) — reading it as "gone" would skip the final
        // update forever (Codex terminal-sync review round 6, P2). Only an
        // absent or stale id falls back to matching the INCARNATION
        // embedded in the bot's comments, which covers a post whose
        // `StatusCommentPosted` never committed (round 4, P1).
        let live = owed
            .comment_id
            .and_then(|id| comments.iter().find(|c| c.id == id))
            .or_else(|| {
                comments.iter().find(|c| {
                    c.author_id == self.deps.bot_user_id
                        && crate::status::parse_status_comment(&c.body).is_ok_and(|r| {
                            r.original_root_pr == root && r.started_at == owed.started_at
                        })
                })
            });
        let Some(live) = live else {
            info!(%root, "no live status comment for this incarnation; nothing stale survives");
            self.store.delete_owed_status_sync(root, started_at)?;
            return self.pump();
        };
        let comment_id = live.id;
        // Pin the resolved id so the rewrite's outcome matches this
        // obligation (it may have been owed with no id at all).
        self.store
            .set_owed_status_comment(root, started_at, comment_id)?;
        let message = terminal_message(&owed.record);
        let body = match crate::status::format_status_comment(&owed.record, &message) {
            Ok(body) => body,
            Err(e) => {
                error!(%root, error = %e, "cannot format the terminal status comment; sync dropped");
                self.store.delete_owed_status_sync(root, started_at)?;
                return self.pump();
            }
        };
        self.in_flight = Some(root);
        Ok(Some(SagaBatch {
            root,
            effects: Vec::new(),
            best_effort: vec![Effect::GitHub(GitHubEffect::UpdateComment {
                comment_id,
                body,
            })],
            feedback: false,
            restart_cleanup: false,
        }))
    }

    /// The comments whose terminal sync is owed, in obligation order.
    #[cfg(test)]
    pub fn owed_status_comments(&self) -> Vec<Option<crate::types::CommentId>> {
        self.store
            .owed_status_syncs()
            .unwrap()
            .into_iter()
            .map(|o| o.comment_id)
            .collect()
    }

    /// Queues a probe for every terminal status-comment sync the store
    /// owes. Called wherever a terminal event may have just created one:
    /// the obligation is written by the EVENT, and the cleanup that
    /// follows carries an `UpdateComment` only when the record knew its
    /// comment id — otherwise nothing would ever refer to it and only a
    /// restart would pick it up (Codex terminal-sync review round 5, P1).
    /// Idempotent: `queue` coalesces status syncs.
    fn queue_owed_status_syncs(&mut self) -> Result<(), StoreError> {
        for sync in self.store.owed_status_syncs()? {
            self.queue(PendingWork::StatusSync {
                root: sync.root,
                started_at: sync.started_at,
            });
        }
        Ok(())
    }

    /// Roots whose terminal status-comment sync is owed.
    #[cfg(test)]
    pub fn owed_status_syncs(&self) -> Vec<PrNumber> {
        self.store
            .owed_status_syncs()
            .unwrap()
            .into_iter()
            .map(|o| o.root)
            .collect()
    }

    /// Re-owes an evaluation for every root still awaiting recovery —
    /// called when the stall-retry timer fires, so a parked recovery is
    /// retried even on a repo with no other traffic.
    ///
    /// The retry routes through the SAME backlog-drain gate as the startup
    /// evaluations ([`Processor::claim`]): the timer may have been armed
    /// for a *released delivery* (the same outage), and the worker loop
    /// pumps before it claims — queueing the evaluation directly would let
    /// recovery act (and push) ahead of an acked stop or topology change
    /// still sitting in the backlog (Codex M6 review, P1; the round-6 rule
    /// again).
    pub fn requeue_marked_recoveries(&mut self) -> Result<(), StoreError> {
        // A store error here would otherwise strand every owed sync
        // silently: this is the only timer that re-arms them (Codex
        // terminal-sync review round 6, P2).
        self.queue_owed_status_syncs()?;
        if self.inherited_mid_flight.is_empty() {
            return Ok(());
        }
        let owed = self.startup_evaluates.get_or_insert_with(Vec::new);
        for root in &self.inherited_mid_flight {
            if !owed.contains(root) {
                owed.push(*root);
            }
        }
        Ok(())
    }

    /// The polling fallback (DESIGN §Polling fallback): owes a re-evaluation
    /// of every active train, gated behind the backlog drain exactly like
    /// the startup evaluations. Webhooks are the primary trigger, but a lost
    /// `check_suite`/`status`/`review` delivery would strand a parked train
    /// forever; the worker's poll timer calls this periodically. The engine's
    /// `evaluate` re-fetches the frontier PR's merge state as it resumes, so
    /// a train whose readiness changed while its webhook went missing makes
    /// progress on the next poll. Evaluations coalesce in the queue
    /// ([`Processor::queue`]), so a poll overlapping owed work adds nothing,
    /// and a poll with no active trains is a no-op.
    pub fn poll_active_trains(&mut self) {
        let active: Vec<PrNumber> = self
            .store
            .state()
            .active_trains
            .values()
            .filter(|t| t.state.is_active())
            .map(|t| t.original_root_pr)
            .collect();
        if active.is_empty() {
            return;
        }
        let owed = self.startup_evaluates.get_or_insert_with(Vec::new);
        for root in active {
            if !owed.contains(&root) {
                owed.push(root);
            }
        }
    }

    /// Inherited-marker upkeep: a root stays marked for recovery only while
    /// the inherited mid-cascade record is the live one. Any train-lifecycle
    /// event for the root supersedes that record — most importantly a stop
    /// (a stopped train needs no recovery, and before M6 the stale marker
    /// refused the restarted train's evaluation until the process
    /// restarted; Codex M5 review).
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
                PendingWork::Trigger(Trigger::StartTrain { .. }) => {
                    unreachable!("starts are persisted and queued as PendingWork::Start")
                }
                // A start applying from the queue: its durable row lives
                // until the start is *answered* (TrainStarted, a rejection,
                // a preflight failure that told the user, or cancellation by
                // a stop), so a crash anywhere in between reloads and
                // re-plans it — start_train is a pure decision and preflight
                // is read-only, so replay is safe.
                PendingWork::Start { id, pr } => {
                    match cascade::start_train(state, pr, now) {
                        Ok(plan) => {
                            let decided = !matches!(plan.control, Control::Continue);
                            let batch = self.integrate_plan(pr, plan, now)?;
                            if decided {
                                // Rejected (or otherwise settled) with no
                                // preflight in flight: answered.
                                self.store.delete_pending_command(id)?;
                            } else {
                                self.active_start = Some((id, pr));
                            }
                            match batch {
                                Some(batch) => return Ok(Some(batch)),
                                None => continue,
                            }
                        }
                        Err(e) => {
                            error!(%pr, error = %e, "engine refused to plan the start");
                            self.store.delete_pending_command(id)?;
                            continue;
                        }
                    }
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
                        Ok(plan) => self.integrate_plan(root, plan, now)?,
                        Err(e) => {
                            error!(%root, error = %e, "engine refused to plan");
                            None
                        }
                    };
                    self.store.delete_pending_command(id)?;
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
                    // An inherited train recovers at its first evaluation:
                    // supplementary GitHub recovery now (the status comment
                    // may be AHEAD of a restored-from-backup store),
                    // worktree restart cleanup with its next batch. GitHub
                    // unavailable: the evaluation is deliberately DROPPED —
                    // the root stays marked and the stall-retry timer
                    // re-queues it (`requeue_marked_recoveries`); keeping
                    // it queued would spin the idle check into a hot loop
                    // against a down GitHub.
                    if self.inherited_mid_flight.contains(&root) && !self.recover_inherited(root)? {
                        continue;
                    }
                    let facts = ReplayFacts::for_train(&self.store.events()?, root);
                    (
                        root,
                        cascade::advance(
                            self.store.state(),
                            root,
                            Observation::Evaluate { facts },
                            now,
                        ),
                    )
                }
                PendingWork::Trigger(Trigger::LateAddition { .. }) => {
                    unreachable!("LateAddition is answered in the pipeline, never queued")
                }
                PendingWork::StatusSync { root, started_at } => {
                    if !self
                        .store
                        .owed_status_syncs()?
                        .iter()
                        .any(|o| o.root == root && o.started_at == started_at)
                    {
                        continue; // confirmed meanwhile
                    }
                    // The probe: does this incarnation's comment still
                    // exist? Its outcome (`on_outcomes`) rewrites or clears.
                    self.sync_probes.insert(root, started_at);
                    self.in_flight = Some(root);
                    return Ok(Some(SagaBatch {
                        root,
                        effects: vec![Effect::GitHub(GitHubEffect::ListComments { pr: root })],
                        best_effort: Vec::new(),
                        feedback: false,
                        restart_cleanup: false,
                    }));
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
                        restart_cleanup: self.needs_restart_cleanup.remove(&root),
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
                        restart_cleanup: self.needs_restart_cleanup.remove(&root),
                    }));
                }
            };
            match plan {
                Ok(plan) => {
                    if let Some(batch) = self.integrate_plan(root, plan, now)? {
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

        if let Some(started_at) = self.sync_probes.remove(&root) {
            return self.on_sync_probe(root, started_at, outcomes);
        }
        if !feedback {
            return self.pump();
        }

        // Observation boundary: queued stops and deferred handler aborts act
        // here, so a human's stop (or a handler's abort) preempts whatever
        // this saga would do next.
        let stops = self.take_queued_stops()?;
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
            // Cancelling the start IS these stops' application — and the
            // start's own answer, so its row goes too.
            for stop in cancels {
                self.store.delete_pending_command(stop.id)?;
            }
            if let Some((id, pr)) = self.active_start
                && pr == root
            {
                self.store.delete_pending_command(id)?;
                self.active_start = None;
            }
            cleanup.push(Effect::GitHub(GitHubEffect::PostComment {
                pr: root,
                body: "🛑 Merge train start cancelled.".to_owned(),
            }));
            return self.finish_or_pump(root, cleanup);
        }

        // A FAILED batch before the train exists is the start's preflight
        // dying (5xx on settings/protection): the engine has nowhere to feed
        // the failure — `advance` pre-train is specified only for the
        // success observation — and the acked, deduped start would vanish
        // with a log line (Codex M5 round 14, P1). Answer the user instead;
        // re-issuing is the retry (preflight performed nothing irreversible).
        if self.store.state().train_involving(root).is_none()
            && let Some(failed) = outcomes.iter().find(|o| o.result.is_err())
        {
            warn!(%root, failure = ?failed.result, "start preflight failed; answering the user");
            // The re-issue answer IS the start's resolution.
            if let Some((id, pr)) = self.active_start
                && pr == root
            {
                self.store.delete_pending_command(id)?;
                self.active_start = None;
            }
            let (mut cleanup, _) = self.apply_stops(stops)?;
            let (mut abort_cleanup, _) = self.apply_deferred_aborts(aborts)?;
            cleanup.append(&mut abort_cleanup);
            cleanup.push(Effect::GitHub(GitHubEffect::PostComment {
                pr: root,
                body: "⚠️ Could not start the merge train: GitHub was unavailable \
                       while checking repository settings. Please re-issue \
                       `start`."
                    .to_owned(),
            }));
            return self.finish_or_pump(root, cleanup);
        }

        // Integrate the completed outcomes FIRST: these effects already ran
        // (a squash may have merged a PR on GitHub), so their records must
        // land before any stop retires the train — a stopped train ignores
        // observations, and the store would diverge from reality (Codex M5
        // review, P1). Stops then suppress the *continuation*, which has not
        // run yet and is therefore safe to drop.
        let mut stops = stops;
        // One clock read for the plan AND its append: the engine bakes `now`
        // into effect payloads that `apply_event` must reproduce from the
        // event stamp (see `integrate_plan`).
        let now = Utc::now();
        let planned = match observe(&outcomes) {
            Ok(obs) => match cascade::advance(self.store.state(), root, obs, now) {
                Ok(plan) => {
                    // A fan-out retires `root` and spawns new roots; a stop
                    // for `root` queued during this batch must retire those
                    // too, or it resolves to "no active train" and the
                    // continuation the user refused runs anyway (Codex M5
                    // round 13). The expansion is made durable BEFORE the
                    // fan-out integrates (Codex M5 round 15): a crash
                    // between the two leaves rows naming the new-root PRs,
                    // which pre-fan-out still resolve to the old train — so
                    // every window recovers to a stopped cascade.
                    let fanned_into: Vec<PrNumber> = plan
                        .events
                        .iter()
                        .find_map(|e| match e {
                            StateEventPayload::FanOutCompleted {
                                old_root,
                                new_roots,
                                ..
                            } if *old_root == root => Some(new_roots.clone()),
                            _ => None,
                        })
                        .unwrap_or_default();
                    if !fanned_into.is_empty() {
                        let mut expanded = Vec::with_capacity(stops.len());
                        for stop in stops {
                            if stop.pr != root {
                                expanded.push(stop);
                                continue;
                            }
                            let replacements: Vec<(PrNumber, bool)> =
                                fanned_into.iter().map(|&pr| (pr, stop.force)).collect();
                            let ids = self.store.replace_pending_stop(stop.id, &replacements)?;
                            for (&(pr, force), id) in replacements.iter().zip(ids) {
                                expanded.push(QueuedStop {
                                    id,
                                    pr,
                                    force,
                                    cancelled_queued_start: false,
                                });
                            }
                        }
                        stops = expanded;
                    }
                    self.integrate_plan(root, plan, now)?
                }
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
        // Terminal events applied at this boundary (a stop, an abort, a
        // completion) may have created obligations nothing else refers to.
        self.queue_owed_status_syncs()?;
        if cleanup.is_empty() {
            return self.pump();
        }
        self.in_flight = Some(root);
        Ok(Some(SagaBatch {
            root,
            effects: Vec::new(),
            best_effort: cleanup,
            feedback: false,
            restart_cleanup: self.needs_restart_cleanup.remove(&root),
        }))
    }

    /// Extracts every queued stop, preserving other pending work in order.
    ///
    /// Each stop also consumes any not-yet-started `StartTrain` for its PR
    /// queued *before* it — the stop arrived after that start and must
    /// suppress it, or the train starts anyway once the slot frees (Codex M5
    /// round 6). Starts queued *after* a stop are the user starting anew and
    /// survive (Codex M5 round 8).
    fn take_queued_stops(&mut self) -> Result<Vec<QueuedStop>, StoreError> {
        let mut stops: Vec<QueuedStop> = Vec::new();
        let mut kept: VecDeque<PendingWork> = VecDeque::new();
        let mut consumed_start_rows: Vec<i64> = Vec::new();
        for work in std::mem::take(&mut self.pending) {
            match work {
                PendingWork::Stop { id, pr, force } => {
                    let before = kept.len();
                    kept.retain(|w| match w {
                        PendingWork::Start { id, pr: p } if *p == pr => {
                            consumed_start_rows.push(*id);
                            false
                        }
                        _ => true,
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
        // A consumed start is answered (cancelled): its row goes with it.
        for id in consumed_start_rows {
            self.store.delete_pending_command(id)?;
        }
        Ok(stops)
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
        // Like stops: the terminal event may owe a status sync that no
        // later outcome refers to, and this path can bypass both
        // `integrate_plan` and `finish_or_pump` (Codex terminal-sync
        // review round 6, P1).
        self.queue_owed_status_syncs()?;
        Ok((cleanup, retired))
    }

    /// Applies stops now (terminal events only — cheap and safe at an
    /// observation boundary), returning their best-effort cleanup and the
    /// roots whose trains were actually retired. Each stop's durable row is
    /// deleted after its append (a crash in between replays it harmlessly).
    /// Applies queued stops. Terminal events here may create status-sync
    /// obligations nothing else refers to, so they are scheduled before
    /// returning (Codex terminal-sync review round 5, P1).
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
                self.store.delete_pending_command(id)?;
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
            self.store.delete_pending_command(id)?;
        }
        self.queue_owed_status_syncs()?;
        Ok((cleanup, stopped_roots))
    }

    /// Appends a plan's events and turns its effects into a batch. `None`
    /// means the plan completed immediately (no effects to run).
    ///
    /// `now` MUST be the timestamp the plan was computed with: the engine
    /// bakes it into effect payloads (the preflight status comment embeds
    /// `TrainRecord::started_at = now`) and `apply_event` derives the same
    /// fields from the event's stamp — a second clock read here would make
    /// the comment and the store disagree about the train's identity, and
    /// recovery's incarnation match would reject the bot's own comment
    /// (Codex M6 review round 2, P3).
    fn integrate_plan(
        &mut self,
        root: PrNumber,
        plan: StepPlan,
        now: chrono::DateTime<Utc>,
    ) -> Result<Option<SagaBatch>, StoreError> {
        let StepPlan {
            events,
            effects,
            best_effort,
            control,
        } = plan;
        self.store.append_batch(&events, now)?;
        self.clear_inherited_markers(&events);
        // A terminal plan (a stop, an abort, a completion) may have just
        // created a status-sync obligation nothing else refers to.
        self.queue_owed_status_syncs()?;

        // TrainStarted answers the in-flight start: its durable row is
        // consumed. (Delete-after-append: a crash in between reloads the
        // start, which re-plans against the now-active train and is
        // rejected as already running — harmless.)
        if let Some((id, pr)) = self.active_start
            && events.iter().any(|e| {
                matches!(
                    e,
                    StateEventPayload::TrainStarted { root_pr, .. } if *root_pr == pr
                )
            })
        {
            self.store.delete_pending_command(id)?;
            self.active_start = None;
        }

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
            restart_cleanup: self.needs_restart_cleanup.remove(&root),
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

/// The human line of a terminal train's final status comment, by state.
fn terminal_message(record: &crate::types::TrainRecord) -> String {
    match &record.state {
        crate::types::TrainState::Stopped { .. } => "🛑 Merge train stopped by request.".to_owned(),
        crate::types::TrainState::Aborted { error, .. } => format!(
            "🛑 Merge train aborted: {}\n\nFix the issue and re-issue `@merge-train start`.",
            error.message
        ),
        crate::types::TrainState::Completed { .. } => "🎉 Merge train completed.".to_owned(),
        crate::types::TrainState::NeedsManualReview => {
            "⚠️ Merge train needs manual review.".to_owned()
        }
        crate::types::TrainState::Running | crate::types::TrainState::WaitingCi => {
            "Merge train status.".to_owned()
        }
    }
}

/// What the first-contact crawl decided about the delivery that woke it.
enum Bootstrap {
    /// GitHub was unavailable (any failure): release the delivery; the
    /// repo's queue pauses at the stall cadence.
    Unavailable,
    /// The triggering comment was not in the listing, seen for the first
    /// time: GitHub is not read-after-write consistent, so the delivery is
    /// released and re-crawled once before being believed.
    TriggerAbsent,
    /// The crawl computed its events (not yet committed — the caller
    /// commits them, atomically with the delivery's close when the trigger
    /// is `stale`: a redelivery the present has overtaken).
    Landed { outcome: CrawlOutcome, stale: bool },
}

/// The facts a first-contact trigger asserts about the present, checked
/// against what the crawl fetched: a stale redelivery must not be handled
/// on top of a fresher crawl.
enum TriggerFreshness {
    /// A `created`/`edited` comment: must still be listed with this body,
    /// and unedited if `created`.
    Comment {
        pr: PrNumber,
        id: crate::types::CommentId,
        body: String,
        created: bool,
    },
    /// A pull_request event: its claims must match the crawled PR.
    PullRequest {
        pr: PrNumber,
        action: crate::webhooks::events::PrAction,
        head_sha: crate::types::Sha,
        head_branch: String,
        base_branch: String,
        is_draft: bool,
        merged: bool,
    },
}

impl TriggerFreshness {
    fn of(event: &GitHubEvent) -> Option<Self> {
        match event {
            GitHubEvent::IssueComment(c) => match c.action {
                CommentAction::Deleted => None,
                CommentAction::Created | CommentAction::Edited => {
                    c.pr_number.map(|pr| TriggerFreshness::Comment {
                        pr,
                        id: c.comment_id,
                        body: c.body.clone(),
                        created: c.action == CommentAction::Created,
                    })
                }
            },
            GitHubEvent::PullRequest(p) => Some(TriggerFreshness::PullRequest {
                pr: p.pr_number,
                action: p.action,
                head_sha: p.head_sha.clone(),
                head_branch: p.head_branch.clone(),
                base_branch: p.base_branch.clone(),
                is_draft: p.is_draft,
                merged: matches!(
                    p.merge_status,
                    crate::webhooks::events::MergeStatus::Merged { .. }
                ),
            }),
            _ => None,
        }
    }

    /// Whether a PR trigger's claims contradict the crawled PR.
    fn disagrees_with(&self, present: &PrData) -> bool {
        use crate::webhooks::events::PrAction;
        let TriggerFreshness::PullRequest {
            action,
            head_sha,
            head_branch,
            base_branch,
            is_draft,
            merged,
            ..
        } = self
        else {
            return false;
        };
        match action {
            PrAction::Closed => {
                if *merged {
                    !matches!(present.state, PrState::Merged { .. })
                } else {
                    present.state != PrState::Closed
                }
            }
            // `PrOpened` (and the refresh a reopen drives) writes the
            // payload's head, base and draft over what the crawl just
            // fetched, so an old delivery must agree with all three or it
            // would regress authoritative data (Codex crawl review round
            // 3, P1).
            PrAction::Opened | PrAction::Reopened => {
                present.state != PrState::Open
                    || present.head_sha != *head_sha
                    // `PrOpened` writes the payload's head REF over the
                    // crawled one, so a delayed delivery from before a
                    // branch rename would restore a branch that no longer
                    // exists (Codex crawl review round 6, P2).
                    || present.head_ref != *head_branch
                    || present.base_ref != *base_branch
                    || present.is_draft != *is_draft
            }
            PrAction::Synchronize => {
                present.state != PrState::Open || present.head_sha != *head_sha
            }
            PrAction::Edited => present.state != PrState::Open || present.base_ref != *base_branch,
            PrAction::ConvertedToDraft | PrAction::ReadyForReview => {
                present.state != PrState::Open || present.is_draft != *is_draft
            }
        }
    }
}

/// The PR on which a first-contact trigger is an AUTHORIZED retraction —
/// the PR author deleted the comment, or edited it to no longer declare a
/// predecessor — decided without state (the crawl has none yet). The
/// handler's own retraction check (`retraction_in`) needs the owning
/// comment, which the crawl is about to reconstruct; this is the
/// state-free half the crawl applies as a tombstone.
fn trigger_retraction(comment: &IssueCommentEvent, deps: &WorkerDeps) -> Option<PrNumber> {
    if comment.sender_id == deps.bot_user_id || comment.sender_id != comment.pr_author_id {
        return None;
    }
    let pr = comment.pr_number?;
    match comment.action {
        CommentAction::Created => None,
        CommentAction::Deleted => Some(pr),
        CommentAction::Edited => {
            let still_declares = matches!(
                parse_command(&comment.body, &deps.bot_name),
                Some(Command::Predecessor(_))
            );
            (!still_declares).then_some(pr)
        }
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
