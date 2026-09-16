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
use crate::effects::github::{CommentData, CommentListing, GitHubEffect};
use crate::effects::{Effect, GitHubResponse, PrData};
use crate::git::{CommitIdentity, GitConfig};
use crate::persistence::event::StateEventPayload;
use crate::store::{Delivery, Store, StoreError};
use crate::types::{CommentId, MergeStateStatus, PrNumber, PrState, TrainRecord};
use crate::webhooks::dedupe::DedupeKey;
use crate::webhooks::events::CommentAction;
use crate::webhooks::handlers::{HandlerCtx, Trigger, handle_event};
use crate::webhooks::{GitHubEvent, parse_webhook};

use super::authz::{
    AuthorDecision, RoleDecision, authorize_by_author, authorize_by_role, authorize_retraction,
};
use super::bootstrap::{CrawlInput, CrawlOutcome};
use super::executor::{GitHubExec, SagaBatch};
use super::recovery::{CommentRecovery, decide_comment_recovery};

/// Per-repo dependencies the processor needs beyond the `Store`.
/// Where the processor reads the time its cooldowns are measured against
/// — "two listings at least a stall cadence apart are independent
/// evidence of an absence". Production reads the system clock. Tests hold
/// a clock they advance by hand, so "spaced past the cooldown" is a fact
/// a test states, never a race it wins by sleeping.
#[derive(Clone)]
pub enum Clock {
    System,
    #[cfg(test)]
    Manual(std::sync::Arc<std::sync::Mutex<chrono::DateTime<Utc>>>),
}

impl Clock {
    pub fn now(&self) -> chrono::DateTime<Utc> {
        match self {
            Clock::System => Utc::now(),
            #[cfg(test)]
            Clock::Manual(held) => *held.lock().unwrap(),
        }
    }
}

impl std::fmt::Debug for Clock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Clock::System => f.write_str("System"),
            #[cfg(test)]
            Clock::Manual(held) => write!(f, "Manual({})", held.lock().unwrap()),
        }
    }
}

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
    /// The clock cooldowns are measured against.
    pub clock: Clock,
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
    /// Best-effort cleanup for a train the handlers aborted (worktree +
    /// final comment/status), computed WHEN the abort applied and carried
    /// here rather than recomputed later: by the time it runs, a queued
    /// `Start` for the same root may have replaced the aborted record,
    /// and `handler_abort_cleanup` would then return nothing at all —
    /// losing the worktree removal and the user's abort notice (Codex
    /// terminal-sync review rounds 13 and 14, both P2).
    CapturedCleanup {
        root: PrNumber,
        effects: Vec<Effect>,
    },
    /// A TERMINAL train owes its status comment the final word: the
    /// comment is the only recovery source if the state DB is later lost,
    /// and left saying "active" it would resurrect a train the user
    /// stopped (monolith review, P1). The obligation is written by the
    /// store in the terminal event's own transaction and cleared only once
    /// an update is confirmed to have landed or the comment confirmed
    /// gone; this work item is the retry — a probe of the PR's comments,
    /// then the rewrite — queued at startup and at the stall cadence.
    /// A PR the stack-ledger machinery has work left on: its ledger
    /// comment no longer matches the declaration the store holds, a bot
    /// comment forged into a ledger is owed neutralizing, or a comment
    /// that may exist unrecorded is owed a look. The ledger is the
    /// topology's off-disk backup — what a crawl READS instead of
    /// re-deriving the graph from the users' comments — so all three are
    /// owed until settled. This is the retry: a listing of the PR's
    /// comments, with the writes the store can already address riding
    /// along, then whatever the listing decides.
    LedgerSync { pr: PrNumber },
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
    /// A stall-retry timer the worker armed that has not yet landed. While
    /// one is out, further retry requests coalesce into it instead of
    /// arming more (see [`Self::take_retry_request`]).
    retry_timer_outstanding: bool,
    /// Deliveries released because a listing DOUBTED their triggering
    /// comment — absent from it, or present with another body (GitHub is
    /// not read-after-write consistent, and the listing may be the older
    /// of the two) — with WHEN the doubt was first raised. A doubt is
    /// believed only once it has stood for the stall cadence: another
    /// webhook can wake the worker and retry a released delivery at once,
    /// and two reads within one propagation window prove nothing (Codex
    /// topology review, P2 and P1). In-memory: a restart re-crawls anyway,
    /// by which point the listing has long caught up.
    doubted_triggers: HashMap<String, chrono::DateTime<Utc>>,
    /// The incarnation whose obligation the in-flight batch is PROBING
    /// (`ListComments` on the root), keyed by that root: the probe's
    /// outcome decides between the rewrite and clearing the obligation.
    sync_probes: HashMap<PrNumber, chrono::DateTime<Utc>>,
    /// Which retired incarnations a durable start row has already been
    /// deferred behind — the row id with the incarnation's key. A start
    /// waits for each obligation once and no more: several incarnations of
    /// one root can owe their comments (Codex terminal-sync review round
    /// 12, P2), and an obligation that can never be discharged must not
    /// stall the start forever.
    deferred_for_sync: HashSet<(i64, chrono::DateTime<Utc>)>,
    /// PRs whose ledger probe (`ListComments`) is in flight, each with
    /// the time it was dispatched: absence evidence is dated by the
    /// listing, not by the end of the batch it rode in — the writes
    /// beside it can take longer than the cooldown.
    ledger_probes: HashMap<PrNumber, chrono::DateTime<Utc>>,
    /// The obligation generation each in-flight ledger write was made
    /// for. The write clears THAT generation and no other, so anything
    /// that dirtied the ledger while it was out survives it. In memory
    /// only: a restart loses it, the obligation stands, and the next sync
    /// writes again — which is idempotent.
    ledger_write_gen: HashMap<PrNumber, u64>,
    /// The repair each in-flight neutralization was dispatched for: the
    /// comment's PR and the generation read. In memory for the same
    /// reason: the repair is durable, the binding need not be.
    repair_write_gen: HashMap<crate::types::CommentId, (PrNumber, u64)>,
    /// Active-train evaluations owed at startup, queued when the durable
    /// backlog first drains (`Some` until then; see [`Processor::claim`]).
    startup_evaluates: Option<Vec<PrNumber>>,
    /// The durable row of the start whose preflight saga is in flight; the
    /// row is deleted when the start is answered (see [`PendingWork::Start`]).
    active_start: Option<(i64, PrNumber)>,
}

/// How many probes must miss a status comment, at least one stall-retry
/// delay apart, before its absence is believed and the obligation cleared.
/// GitHub's listings are eventually consistent, so a comment created
/// moments earlier can be missing from one and present in the next. The
/// separation is what makes a second miss evidence rather than a repeat of
/// the first, and the store enforces it: an unrelated delivery re-queues
/// the owed syncs, so probes are not otherwise spread out in time at all.
/// Bounded at two: an obligation nobody can ever discharge is worse than a
/// rare wasted listing.
const ABSENT_PROBES_BEFORE_BELIEVED: u32 = 2;

/// What a forged or stale-duplicate stack-ledger comment is rewritten
/// to. Inert on purpose: it must parse as neither a ledger nor a command.
const NEUTRALIZED_LEDGER_BODY: &str = "This comment matched the format of the bot's stack-ledger records without \
     being one — a copy, a stale duplicate, or an edit — and has been \
     cleared. The authoritative ledger lives in its own comment.";

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
        // Stack-ledger work a previous process never finished: owed until
        // it lands, and on a quiet repository nothing else would ever ask
        // (Codex ledger review round 1, P1).
        pending.extend(
            store
                .ledger_pending_prs()?
                .into_iter()
                .map(|pr| PendingWork::LedgerSync { pr }),
        );
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
            retry_timer_outstanding: false,
            doubted_triggers: HashMap::new(),
            sync_probes: HashMap::new(),
            deferred_for_sync: HashSet::new(),
            ledger_probes: HashMap::new(),
            ledger_write_gen: HashMap::new(),
            repair_write_gen: HashMap::new(),
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

    /// The time, as the deps' clock tells it. Every comparison against a
    /// cooldown reads THIS, so a test can state the passage of time.
    fn now(&self) -> chrono::DateTime<Utc> {
        self.deps.clock.now()
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

        // The dedupe key, before anything can close the delivery: a close
        // records it, so a redelivery under a fresh id — received after
        // the crawl landed, and so unmarked — is a duplicate, not a fresh
        // payload (Codex first-contact review, P1).
        // It is consulted BEFORE a crawled delivery is judged for freshness,
        // below: a second copy of a stale webhook, queued under its own id
        // when the crawl landed, is a duplicate of the first copy's close,
        // discarded at once rather than doubted for another whole stall
        // cadence with every delivery behind it waiting (Codex first-contact
        // review, P2).
        let key = DedupeKey::for_event(&event);

        // First contact — a fresh store (brand-new repo, or a LOST state
        // db) has an empty default branch. It comes before anything that
        // could WRITE: a delayed edit webhook for a ledger the bot has
        // already restored would otherwise queue a repair before the crawl
        // has landed, and a crawl that fails would leave that repair to
        // neutralize the genuine ledger against an empty cache (Codex
        // topology review, P1). Webhooks only describe the
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
        // The crawl reads the bot's own records — status comments and
        // stack ledgers — so the triggering comment shapes nothing in it:
        // a fresh declaration has no ledger yet, and the handler below
        // processes it against the crawled present as it would any other.
        // What the crawl still decides about the trigger is whether the
        // DELIVERY is current: a `created`/`edited` trigger whose comment
        // is no longer in the PR's listing — or a `created` one whose
        // comment has since been edited — is a stale redelivery, and
        // handling it would record a body the comment no longer has.
        let mut crawl_context = delivery.crawled;
        if self.store.state().default_branch.is_empty() {
            let freshness = TriggerFreshness::of(&event);
            let retried = self.doubt_has_stood(&id);
            match self.bootstrap_crawl(&event.referenced_prs(), freshness, retried)? {
                Bootstrap::Unavailable => return self.release(&id),
                Bootstrap::TriggerDoubted => {
                    self.doubt(&id);
                    return self.release(&id);
                }
                Bootstrap::Landed { outcome, stale } => {
                    if stale {
                        // The crawl and the close of the stale delivery
                        // commit TOGETHER: were the crawl to land alone and
                        // the process die before the close, the retried
                        // delivery would find a bootstrapped store, skip
                        // this check, and be handled after all (Codex crawl
                        // review round 2, P1).
                        // The trigger's own suppressed creation may
                        // transfer ownership too — judged against the
                        // state the crawl's events produce, and committed
                        // with them.
                        let mut events = outcome.events.clone();
                        let mut preview = self.store.state().clone();
                        for payload in &events {
                            preview.apply_event(&crate::persistence::event::StateEvent {
                                seq: 0,
                                ts: Utc::now(),
                                payload: payload.clone(),
                            });
                        }
                        if let Some(t) = restatement_transfer(&preview, &event, &self.deps) {
                            info!(delivery = %id, event = ?t, "the suppressed trigger takes ownership");
                            events.push(t);
                        }
                        self.store.commit_delivery_closing_crawl(
                            &id,
                            &events,
                            key.as_ref(),
                            Utc::now(),
                            &outcome.stale_ledgers,
                            outcome.topology_incomplete,
                        )?;
                        self.after_bootstrap(outcome)?;
                        info!(delivery = %id, "closed: the trigger is stale against the crawl");
                        return Ok(PipelineOutcome::Processed);
                    }
                    // The crawl's events and the mark on THIS delivery
                    // commit together: the crawl judged the delivery
                    // current by reading GitHub, and if that judgement
                    // does not survive to the close, the retry must not
                    // act on it (Codex crawl review round 14, P1).
                    self.store.append_batch_marking(
                        &outcome.events,
                        Utc::now(),
                        Some(&id),
                        &outcome.stale_ledgers,
                        outcome.topology_incomplete,
                    )?;
                    self.after_bootstrap(outcome)?;
                    // The trigger is judged against the crawled present
                    // exactly as a crash-marked delivery is: the founding
                    // gate below must see it (the crawl may just have
                    // dropped an uncorroborated ledger edge whose
                    // retraction still sits unacked behind this very
                    // delivery).
                    crawl_context = true;
                }
            }
        }

        // A change to one of the bot's own stack-ledger comments is noted
        // BEFORE the duplicate-content check: the note is idempotent (it
        // owes a rewrite that states the truth, however often), while
        // the dedupe key is second-resolution — an editor reapplying the
        // same tampered body within a second of the repair would
        // otherwise be dropped as a redelivery, corruption and all.
        self.note_bot_comment_change(&event)?;

        // Dedupe: identical content already handled under a different
        // delivery id (GitHub redelivers with fresh ids).
        if let Some(k) = &key
            && self.store.is_duplicate(k)?
        {
            return self.close(&id, None, "duplicate content");
        }

        // A crawl already landed for this delivery — in an earlier process,
        // or in this one before an unrelated transient failure released
        // it. That crawl checked whether the delivery was still current by
        // reading GitHub, and the comment may have been edited or deleted
        // since; the store is now bootstrapped, so the crawl will not run
        // again. What the check guarded decides what happens now:
        // - a COMMENT may have been edited or deleted while we were down,
        //   and an edited-away `start` has no retraction path at all:
        //   closed unhandled, the user re-issues (Codex crawl review round
        //   14, P1);
        // - a PULL REQUEST event is re-checked against the present the
        //   crawl cached: one that disagrees (an old `closed` after a
        //   reopen) is stale and closed; one that agrees is handled;
        // - anything else (a review dismissal, a check suite) carries a
        //   consequence the crawl cannot reconstruct — an adopted train's
        //   required abort, say — and is handled as it would have been.
        // The store is bootstrapped: the crawl's events and the mark on
        // the delivery committed together, so first contact above did
        // not run again, and the note and the dedupe check have had
        // their say.
        if delivery.crawled {
            debug_assert!(
                !self.store.state().default_branch.is_empty(),
                "a crawled delivery finds a bootstrapped store"
            );
            let stale = match &event {
                // A comment is re-checked against GitHub's present, exactly
                // as the crawl checked it: listed with this body, in the
                // sender's own bytes. A maintainer's `stop` that
                // survived the gap unchanged is handled — an adopted train
                // it was meant to stop would otherwise resume; one edited
                // or gone is closed unheard. GitHub unavailable: released,
                // as the crawl itself would be.
                GitHubEvent::IssueComment(_) => match TriggerFreshness::of(&event) {
                    Some(TriggerFreshness::Comment {
                        pr,
                        id: trigger_id,
                        body,
                        sender,
                    }) => match self.deps.github.execute(GitHubEffect::ListComments { pr }) {
                        Ok(GitHubResponse::Comments(CommentListing::Complete(listed))) => {
                            match listed.iter().find(|c| c.id == trigger_id) {
                                Some(c) if c.body == body && written_by_the_sender(c, sender) => {
                                    false
                                }
                                // Another body, or absent: doubted, as at
                                // first contact — the listing may be the
                                // older of the two. Stale only once the doubt
                                // has stood for the stall cadence.
                                _ => {
                                    if self.doubt_has_stood(&id) {
                                        true
                                    } else {
                                        self.doubt(&id);
                                        return self.release(&id);
                                    }
                                }
                            }
                        }
                        // GitHub unavailable: released, as the crawl itself
                        // would be. A PERMANENT failure — the PR gone, or
                        // the token without access — is not: released, the
                        // delivery would sit at the head of the queue for
                        // ever; handled, the handler's own precache refuses
                        // a command on an unfetchable PR with an answer
                        // (Codex first-contact review, P2).
                        Err(e @ EffectError::Transient { .. }) => {
                            warn!(
                                delivery_id = %id, error = ?e,
                                "cannot re-check a crawled comment delivery; releasing it"
                            );
                            return self.release(&id);
                        }
                        other => {
                            warn!(
                                delivery_id = %id, outcome = ?other.err(),
                                "cannot list the PR of a crawled comment delivery; refusing it"
                            );
                            if let Some((pr, _)) = command_in(&event, &self.deps) {
                                self.best_effort_github(GitHubEffect::PostComment {
                                    pr,
                                    body: format!(
                                        "The bot cannot list PR {pr}'s comments (permanent API \
                                         failure — does the bot's token have access to this \
                                         repository?); refusing the command."
                                    ),
                                });
                            }
                            return self.close(&id, key.as_ref(), "crawled but unverifiable");
                        }
                    },
                    // A DELETION is not re-checked: the comment is gone, and
                    // the retraction handler is author-gated and removes an
                    // edge only if the store holds one — which it may, from
                    // a listing that still showed the deleted comment (GitHub
                    // is not read-after-write consistent), or from an older
                    // comment the deleted one would have superseded. Closing
                    // it unheard would leave that edge standing (Codex
                    // topology review, P1; first-contact review, P1). A
                    // comment on an issue rather than a PR has no freshness
                    // either, and the handler ignores it.
                    None => false,
                    Some(TriggerFreshness::PullRequest { .. }) => {
                        unreachable!("a comment event has comment freshness")
                    }
                },
                GitHubEvent::PullRequest(_) => match TriggerFreshness::of(&event) {
                    Some(trigger) if trigger.disagrees_with_cache(self.store.state()) => {
                        // Doubted, as a comment is: the cache is the
                        // snapshot the crawl took, and GitHub's listing
                        // can lag a `closed` it has already delivered.
                        // Once the doubt has stood for the stall cadence,
                        // the PRESENT decides — the PR fetched afresh, not
                        // the cache (Codex topology review, P2).
                        if !self.doubt_has_stood(&id) {
                            self.doubt(&id);
                            return self.release(&id);
                        }
                        let TriggerFreshness::PullRequest { pr, .. } = &trigger else {
                            unreachable!("a pull-request event has pull-request freshness")
                        };
                        match self.deps.github.execute(GitHubEffect::GetPr { pr: *pr }) {
                            Ok(GitHubResponse::Pr(present)) => {
                                // The fresh snapshot IS the present: the
                                // cache is reconciled to it whatever the
                                // verdict on the delivery, or a stale
                                // reopen closed unheard would leave the PR
                                // cached closed while GitHub has it open
                                // (Codex first-contact review, P1).
                                let reconcile =
                                    reconcile_cache_events(self.store.state(), *pr, &present);
                                self.store.append_batch(&reconcile, Utc::now())?;
                                trigger.disagrees_with(&present)
                            }
                            // GitHub unavailable: released. A PERMANENT
                            // failure — the PR gone, or the token without
                            // access — is not: released, the delivery would
                            // sit at the head of the repository's queue for
                            // ever, every unrelated delivery behind it; and
                            // a payload the present cannot confirm is not
                            // believed, so the cache keeps the crawl's
                            // snapshot (Codex first-contact review, P2).
                            Err(e @ EffectError::Transient { .. }) => {
                                warn!(
                                    delivery_id = %id, error = ?e,
                                    "cannot re-fetch a crawled pull-request delivery's PR; \
                                     releasing it"
                                );
                                return self.release(&id);
                            }
                            other => {
                                warn!(
                                    delivery_id = %id, outcome = ?other.err(),
                                    "cannot re-fetch a crawled pull-request delivery's PR for \
                                     good; refusing the delivery"
                                );
                                return self.close(&id, key.as_ref(), "crawled but unverifiable");
                            }
                        }
                    }
                    _ => false,
                },
                _ => false,
            };
            if stale {
                warn!(
                    delivery_id = %id,
                    "a crawl landed for this delivery before the process died; its \
                     freshness check did not survive, so it is not acted on"
                );
                // ...but the ownership a suppressed creation would have
                // transferred is recorded with the close.
                let transfer = restatement_transfer(self.store.state(), &event, &self.deps);
                if let Some(t) = &transfer {
                    info!(delivery_id = %id, event = ?t, "a suppressed restatement takes ownership");
                }
                self.store.commit_delivery(
                    &id,
                    transfer.as_slice(),
                    key.as_ref(),
                    &[],
                    Utc::now(),
                )?;
                // The transfer changed the owner, so the ledger is owed a
                // rewrite — queued HERE, as after a handled delivery: this
                // close may be a cooldown retry with nothing else queued
                // and no timer outstanding, and the ledger would name the
                // old owner until a restart (Codex first-contact review,
                // P2).
                self.queue_owed_status_syncs()?;
                info!(delivery_id = %id, "delivery closed: crawled but never closed");
                return Ok(PipelineOutcome::Processed);
            }
        }

        // A crawled EDIT may not FOUND an edge on a comment its author
        // did not create; it may only move an existing one. The same
        // final bytes — a stranger's comment the author edited into a
        // declaration — arise both from the author genuinely founding
        // that way and from the author RETRACTING elsewhere after a
        // restating edit (their own declaration deleted, its deliveries
        // unacked): no point-in-time check can tell the two apart, and
        // acting on the edit would resurrect the retracted stack. The
        // ruling picks the direction — recovery is stricter than live,
        // and a false drop costs one re-declaration (the ledger rewrite
        // states the truth either way).

        if crawl_context
            && let GitHubEvent::IssueComment(c) = &event
            && c.action == crate::webhooks::events::CommentAction::Edited
            && let Some(pr) = c.pr_number
            && c.author_id != c.pr_author_id
            && matches!(
                parse_command(&c.body, &self.deps.bot_name),
                Some(Command::Predecessor(_))
            )
            && self
                .store
                .state()
                .prs
                .get(&pr)
                .is_none_or(|p| p.predecessor.is_none())
        {
            warn!(
                delivery_id = %id, %pr, comment = %c.comment_id,
                "a crawled edit of a comment the PR author did not create cannot \
                 FOUND a predecessor edge (a retraction may hide behind it); the \
                 author re-declares if the edge is wanted"
            );
            return self.close(&id, key.as_ref(), "crawled edit cannot found an edge");
        }

        // Command authorization + referenced-PR precache (commands only).
        if let Some((pr, command)) = command_in(&event, &self.deps) {
            // A topology the first-contact crawl could not read in full
            // must not be driven: a train started over it would freeze a
            // stack missing the descendants whose ledgers went unread, and
            // squash the root without preparing them. Refused, loudly,
            // until an operator resolves it.
            if matches!(command, Command::Start) && self.store.topology_incomplete()? {
                error!(
                    %pr,
                    "start refused: the first-contact crawl could not read every PR's \
                     comments, so the topology is incomplete — operator action required"
                );
                self.store
                    .commit_delivery(&id, &[], key.as_ref(), &[], Utc::now())?;
                self.best_effort_github(GitHubEffect::PostComment {
                    pr,
                    body: "Cannot start: this repository's first-contact crawl could not \
                           read every pull request's comments, so the stack topology is \
                           incomplete. An operator must resolve this before trains can run."
                        .to_owned(),
                });
                return Ok(PipelineOutcome::Processed);
            }
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
        // plan). Captured NOW and queued AHEAD: a `start` for the same
        // root already waiting out an in-flight saga would otherwise run
        // first, replace the aborted record, and leave a later recompute
        // nothing to find — no worktree removal, no abort notice (Codex
        // terminal-sync review round 14, P2).
        for payload in &events {
            if let StateEventPayload::TrainAborted { root_pr, .. } = payload {
                let effects = cascade::handler_abort_cleanup(self.store.state(), *root_pr);
                if !effects.is_empty() {
                    self.pending.push_front(PendingWork::CapturedCleanup {
                        root: *root_pr,
                        effects,
                    });
                }
            }
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
                    // Detected by M3, answered here: the reconciliation
                    // flow for late additions is explicitly deferred. The
                    // answer records nothing, and needs to record nothing:
                    // a declaration with no stack ledger behind it is one
                    // a crawl will never turn into an edge.
                    let _ = comment_id;
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
                Trigger::StartTrain { .. } | Trigger::StopTrain { .. } => {}
                other => self.queue(PendingWork::Trigger(other)),
            }
        }

        Ok(PipelineOutcome::Processed)
    }

    /// A change to one of the bot's OWN stack-ledger comments — an edit
    /// or a deletion by anyone with rights — invalidates the topology's
    /// off-disk backup for that PR, and no topology event will say so. It
    /// is noted before anything in the pipeline can return early: the
    /// duplicate-content check (second-resolution keys), command
    /// authorization and precaching all can, and a doctored ledger
    /// carrying a `predecessor` line would otherwise be rejected as an
    /// unauthorized command with the tampering left in place (Codex
    /// ledger review round 13, P1). The bot's own writes are excluded by
    /// the sender, or every write would re-dirty what it just wrote.
    fn note_bot_comment_change(&mut self, event: &GitHubEvent) -> Result<(), StoreError> {
        if let GitHubEvent::IssueComment(comment) = event
            && comment.sender_id != self.deps.bot_user_id
            && comment.author_id == self.deps.bot_user_id
            && comment.author_id != 0
            && matches!(
                comment.action,
                crate::webhooks::events::CommentAction::Deleted
                    | crate::webhooks::events::CommentAction::Edited
            )
            && let Some(pr) = comment.pr_number
        {
            // The recorded ledger, edited or deleted, is owed a rewrite (or
            // a fresh post). Any OTHER bot comment now reading as this
            // PR's ledger is a forgery, owed neutralizing by id — the
            // webhook names it, and a listing need never be consulted to
            // act on it (Codex ledger review round 21). A deletion is
            // proof the comment is gone for ever: nothing can be owed to
            // it, and a listing that still shows it is showing a ghost
            // (round 22).
            //
            // And a change to ANY bot comment on a PR whose ledger is
            // owed may be a change to the ledger: a post can have landed
            // and not yet acknowledged, so its id is not recorded — and a
            // change in that window, ignored, would let the
            // acknowledgement record a dead or doctored comment and clear
            // the obligation for good. Re-owing bumps the generation,
            // which is exactly what an in-flight write's acknowledgement
            // cannot clear; the next sync then writes to the recorded id.
            let recorded = self
                .store
                .state()
                .prs
                .get(&pr)
                .and_then(|cached| cached.ledger_comment_id);
            let is_recorded = recorded == Some(comment.comment_id);
            let reads_as_ledger = crate::status::parse_stack_ledger(&comment.body)
                .is_some_and(|ledger| ledger.pr == pr);
            let owed = self.store.owed_stack_ledgers()?.iter().any(|o| o.pr == pr);
            match comment.action {
                crate::webhooks::events::CommentAction::Deleted => {
                    self.store
                        .mark_ledger_comment_dead(pr, comment.comment_id)?;
                    if is_recorded {
                        info!(
                            %pr, comment = %comment.comment_id,
                            "the stack ledger comment was deleted; owed again"
                        );
                        // Forget the id and owe the ledger in ONE
                        // transaction, so the sync posts a fresh comment
                        // instead of writing to nothing, and a crash
                        // cannot leave the id forgotten with nothing owed.
                        self.store
                            .retire_stack_ledger(pr, comment.comment_id, Utc::now())?;
                    } else if owed {
                        info!(
                            %pr, comment = %comment.comment_id,
                            "a bot comment was deleted while the stack ledger was owed; owed afresh"
                        );
                        self.store.mark_ledger_owed(pr)?;
                    }
                }
                crate::webhooks::events::CommentAction::Edited if is_recorded => {
                    info!(
                        %pr, comment = %comment.comment_id,
                        "the stack ledger comment was edited; owed again"
                    );
                    self.store.mark_ledger_owed(pr)?;
                }
                crate::webhooks::events::CommentAction::Edited => {
                    // Whatever the store had settled about this comment's
                    // content, an edit reopens it.
                    self.store.unsettle_ledger_comment(comment.comment_id)?;
                    if reads_as_ledger && self.store.is_status_comment(comment.comment_id)? {
                        // A train's status comment, edited into a ledger:
                        // the ledger machinery leaves it alone. Rewriting
                        // it into inert text would erase the train's
                        // durable record — and a delayed webhook may
                        // describe an edit the terminal update has since
                        // restored.
                        warn!(
                            %pr, comment = %comment.comment_id,
                            "a status comment was edited into a stack ledger; not touched"
                        );
                    } else if reads_as_ledger {
                        info!(
                            %pr, comment = %comment.comment_id,
                            "a bot comment was edited into a stack ledger; owed neutralizing"
                        );
                        self.store.add_ledger_repair(pr, comment.comment_id)?;
                    }
                    if owed {
                        self.store.mark_ledger_owed(pr)?;
                    }
                }
                _ => {}
            }
            // Scheduled HERE, where the work is created: this delivery
            // may return early (an apparent command that authorization
            // refuses), and nothing later would queue it. A deletion may
            // also have settled an unresolved comment, which is what a
            // waiting post needs — so anything pending gets its look.
            if self.store.ledger_pending(pr)? {
                self.queue(PendingWork::LedgerSync { pr });
            }
        }

        Ok(())
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
    /// Records that a listing doubted delivery `id`'s trigger, from now if
    /// this is the first time.
    fn doubt(&mut self, id: &str) {
        let now = self.deps.clock.now();
        self.doubted_triggers.entry(id.to_owned()).or_insert(now);
    }

    /// Whether a doubt about delivery `id` has stood for the stall cadence:
    /// GitHub is not read-after-write consistent, so one read proves
    /// nothing, and a doubt confirmed within the same propagation window
    /// proves nothing more.
    fn doubt_has_stood(&self, id: &str) -> bool {
        let now = self.deps.clock.now();
        self.doubted_triggers.get(id).is_some_and(|first| {
            now.signed_duration_since(*first)
                >= chrono::Duration::from_std(self.deps.stall_retry_delay)
                    .unwrap_or(chrono::Duration::MAX)
        })
    }

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
                        "The bot cannot fetch PR {pr} (permanent API failure — does the bot's \
                         token have access to this repository?); refusing the command."
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
        if matches!(github, GitHubExec::Fake(..)) {
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
                | PendingWork::DeferredAbort { .. }
                | PendingWork::StatusSync { .. }
                | PendingWork::LedgerSync { .. }
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
        // Their comments were never listed because the PR could not be
        // reached, not because the cap bit: a command on one is answered by
        // the handler's own refusal rather than closed unheard.
        let mut unfetchable: HashSet<PrNumber> = HashSet::new();
        let mut comments: Vec<(PrNumber, Vec<CommentData>)> = Vec::new();
        let mut listed: HashSet<PrNumber> = HashSet::new();
        // Set when the cap stops us reading a crawled PR's comments: a
        // ledger, or a whole train member, may then be missing from the
        // crawl, and no train can be recovered from a partial read.
        let mut comments_truncated = false;
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
            // The SEED PRs first. The trigger's freshness is decided by
            // whether its comment is in its PR's listing, so a cap reached
            // before that PR would leave the delivery unverifiable — and
            // the arm below cannot tell "not there" from "never looked"
            // (Codex crawl review round 10, P1).
            let mut unlisted: Vec<PrNumber> = crawled
                .iter()
                .map(|p| p.number)
                .filter(|pr| !listed.contains(pr))
                .collect();
            unlisted.sort_by_key(|pr| !seed_prs.contains(pr));
            for pr in unlisted {
                if listed.len() >= MAX_COMMENT_LISTINGS {
                    comments_truncated = true;
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
                    GitHubResponse::Comments(CommentListing::Complete(c)) => c
                );
                comments.push((pr, pr_comments));
            }
            let outcome = super::bootstrap::crawl_events(&CrawlInput {
                default_branch: &settings.default_branch,
                crawled_prs: &crawled,
                comments: &comments,
                bot_name: &self.deps.bot_name,
                bot_user_id: self.deps.bot_user_id,
                unfetchable: &unfetchable,
                comments_truncated,
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
                sender,
            }) => {
                let listed_comments: Vec<&CommentData> = comments
                    .iter()
                    .filter(|(p, _)| p == pr)
                    .flat_map(|(_, cs)| cs.iter())
                    .collect();
                match listed_comments.iter().find(|c| c.id == *trigger_id) {
                    // Present with ANOTHER body. Either an edit superseded
                    // this payload, or the listing is the older of the two
                    // — an author's retraction edit can trigger the crawl
                    // while the listing still serves the declaration, and
                    // closing on that would keep the withdrawn edge (Codex
                    // topology review, P1). Doubted like an absence: stale
                    // only if it still disagrees after the stall cadence.
                    Some(c) if c.body != *body || !written_by_the_sender(c, *sender) => {
                        if retried {
                            true
                        } else {
                            info!(
                                %pr, comment = %trigger_id,
                                "the triggering comment's listed body differs from the \
                                 payload's; retrying after the stall cadence"
                            );
                            return Ok(Bootstrap::TriggerDoubted);
                        }
                    }
                    Some(_) => false,
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
                    // The PR itself could not be fetched, so its comments
                    // were never listed: not stale — the handler refuses a
                    // command on an unfetchable PR with an answer, as it
                    // would after bootstrap (Codex first-contact review,
                    // P2).
                    None if unfetchable.contains(pr) => false,
                    None if !listed.contains(pr) => {
                        // The cap bit before this PR's comments were read,
                        // so the trigger cannot be verified at all. Seeds
                        // are listed first, so reaching this means the
                        // repository is past what the bot supports;
                        // executing an unverifiable command after a DB
                        // loss is the one thing recovery must not do
                        // (Codex crawl review round 10, P1).
                        error!(
                            %pr, comment = %trigger_id,
                            "the triggering PR's comments were never listed; refusing to \
                             act on a delivery the crawl cannot verify"
                        );
                        true
                    }
                    None if !retried => {
                        info!(
                            %pr, comment = %trigger_id,
                            "the triggering comment is not in the listing yet; retrying after \
                             the stall cadence"
                        );
                        return Ok(Bootstrap::TriggerDoubted);
                    }
                    None => true,
                }
            }
            Some(pr_trigger @ TriggerFreshness::PullRequest { pr, .. }) => {
                let disagrees = crawled
                    .iter()
                    .find(|p| p.number == *pr)
                    .is_some_and(|present| pr_trigger.disagrees_with(present));
                // Doubted like a comment the listing disagrees with: the
                // PR listing can lag a `closed` it has already delivered
                // (Codex topology review, P2). The retry re-crawls, and is
                // stale only if it still disagrees after the stall cadence.
                if disagrees && !retried {
                    info!(
                        %pr,
                        "the triggering pull-request event disagrees with the crawled \
                         snapshot; retrying after the stall cadence"
                    );
                    return Ok(Bootstrap::TriggerDoubted);
                }
                disagrees
            }
            None => false,
        };
        // The crawl itself does not depend on the trigger — it reads the
        // bot's own records — so a stale delivery needs no re-crawl: the
        // same events stand, and only the delivery is closed unhandled.
        Ok(Bootstrap::Landed { outcome, stale })
    }

    /// What every landed crawl owes, after its events are committed: the
    /// inherited markers, the rewrite of any ledger it disbelieved, the
    /// abort cleanups, and the recovery requeue.
    fn after_bootstrap(&mut self, outcome: CrawlOutcome) -> Result<(), StoreError> {
        self.clear_inherited_markers(&outcome.events);
        // (The ledgers the crawl disbelieved were marked owed in the same
        // transaction as its events; the sync below picks them up.)
        // A train the crawl aborted (its stack was extended during the gap)
        // needs the same worker-side cleanup a handler abort gets — stale
        // worktree removal + a final status comment (the engine's own
        // aborts carry cleanup in their plans; this one has no plan).
        // Captured NOW and queued AHEAD, as everywhere: run later, a
        // reloaded `start` could replace the aborted record first and
        // leave a recompute nothing to find (Codex terminal-sync review
        // round 14, P2).
        for payload in &outcome.events {
            if let StateEventPayload::TrainAborted { root_pr, .. } = payload {
                let effects = cascade::handler_abort_cleanup(self.store.state(), *root_pr);
                if !effects.is_empty() {
                    self.pending.push_front(PendingWork::CapturedCleanup {
                        root: *root_pr,
                        effects,
                    });
                }
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
    /// Adopts `record` in place of the store's for `root` — a record ahead
    /// of the store's, or a newer incarnation, that supplementary recovery
    /// found — judged as the crawl judges. Returns whether the train
    /// resumes; a terminal verdict retires it here.
    fn adopt_replacement(
        &mut self,
        root: PrNumber,
        record: TrainRecord,
        barred: Option<CommentId>,
    ) -> Result<bool, StoreError> {
        // Judged as the crawl judges (`adoption::judge_replacement`),
        // against the store as it stands: the record replacing the
        // one the crawl read may name a member the author unstacked
        // during the gap, or one another active train owns, and
        // resumed unjudged the cascade would merge it (Codex trains
        // review, P1). The store's picture of every PR the record
        // names is brought to GitHub's present FIRST: a restored
        // backup's cache can still list a member the train has since
        // merged as open, and that would read as an extension (Codex
        // trains review, P2). A PR that cannot be fetched is unknown
        // here — the crawl's discovery is over.
        let footprint = super::adoption::Footprint::of(self.store.state(), &record);
        // Every PR the judgement looks at: what the record names,
        // and everything the cache hangs off its root and current
        // PR — that is where a merged member the backup still lists
        // as open sits.
        let mut to_refresh: Vec<PrNumber> = footprint.stack.iter().copied().collect();
        to_refresh.sort_unstable();
        let mut unknown: HashSet<PrNumber> = HashSet::new();
        for pr in to_refresh {
            match self.deps.github.execute(GitHubEffect::GetPr { pr }) {
                Ok(GitHubResponse::Pr(present)) => {
                    let reconcile = reconcile_cache_events(self.store.state(), pr, &present);
                    self.store.append_batch(&reconcile, Utc::now())?;
                }
                Err(e @ EffectError::Transient { .. }) => {
                    warn!(%root, %pr, error = ?e, "cannot fetch a named PR; recovery parked");
                    self.retry_requested = true;
                    return Ok(false);
                }
                other => {
                    warn!(%root, %pr, ?other, "a PR of the train's cannot be fetched");
                    if footprint.named.contains(&pr) {
                        unknown.insert(pr);
                    }
                }
            }
        }
        let verdict =
            super::adoption::judge_replacement(self.store.state(), root, &record, &unknown, barred);
        let mut events = vec![StateEventPayload::TrainRecordAdopted {
            root_pr: root,
            record,
        }];
        let resumes = match verdict {
            super::adoption::Verdict::Recover => true,
            // A retired record (stopped, completed, aborted on GitHub's own
            // say-so) retires the train here: nothing resumes.
            super::adoption::Verdict::Retired => false,
            super::adoption::Verdict::Complete => {
                events.push(StateEventPayload::TrainCompleted { root_pr: root });
                false
            }
            super::adoption::Verdict::Abort(why) => {
                warn!(%root, ?why, "the record adopted after the crawl is aborted");
                let default_branch = self.store.state().default_branch.clone();
                events.push(StateEventPayload::TrainAborted {
                    root_pr: root,
                    error: why.error(root, &default_branch),
                });
                false
            }
        };
        self.store.append_batch(&events, Utc::now())?;
        if !resumes {
            // A terminal verdict retires the train here: the
            // inherited marker goes with it (or later ledger repairs
            // on the root would stay deferred behind it), an abort
            // gets the worker-side cleanup a handler abort gets,
            // and the final word the record is now owed is queued —
            // nothing else would, once completion has removed the
            // train from what polling looks at (Codex trains
            // review, P2 twice).
            self.clear_inherited_markers(&events);
            // ...for a retired record too, whose batch carries no terminal
            // event of its own (Codex trains review, P2).
            self.inherited_mid_flight.remove(&root);
            if events
                .iter()
                .any(|e| matches!(e, StateEventPayload::TrainAborted { .. }))
            {
                let effects = cascade::handler_abort_cleanup(self.store.state(), root);
                if !effects.is_empty() {
                    self.pending
                        .push_front(PendingWork::CapturedCleanup { root, effects });
                }
            }
            self.queue_owed_status_syncs()?;
            return Ok(false);
        }
        Ok(true)
    }

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
            Ok(GitHubResponse::Comments(CommentListing::Complete(comments))) => comments,
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
        // The incarnation chosen AGAIN on this listing, as the crawl chose
        // it (`adoption::choose`): the crawl's listing can have missed a
        // newer incarnation's record — a stop and a fresh start it had not
        // caught up with — and `decide_comment_recovery` would keep the
        // older running record as this train's own (Codex trains review,
        // P1). A newer incarnation, or any comment of the bot's on the
        // root that is not its own bytes (a bar, as for the crawl), is
        // adopted as a replacement and judged as one.
        let bot = self.deps.bot_user_id;
        let trusted: Vec<(CommentId, TrainRecord)> = comments
            .iter()
            .filter(|c| c.author_id == bot && c.author_id != 0)
            .filter_map(|c| {
                let record = c
                    .body_written_by(bot)
                    .and_then(|body| crate::status::parse_status_comment(body).ok())?;
                (record.original_root_pr == root).then_some((c.id, record))
            })
            .collect();
        let barred = comments
            .iter()
            .filter(|c| c.author_id == bot && c.body_written_by(bot).is_none())
            .map(|c| c.id)
            .max();
        // A newer incarnation is one whose record sits ABOVE the local
        // train's own: a different identity alone is no evidence — an
        // intact database restarting with its train's comment deleted
        // and a previous incarnation's stop still on the root must repost
        // its backup, not adopt the old stop (Codex trains review, P2).
        // ABOVE is by watermark, the incarnation's FIRST comment id: the
        // local record can be a backup restored after its own repost,
        // whose comment id sits above every genuine newcomer's for ever
        // (Codex trains review, P1).
        let newer = super::adoption::choose(&trusted).filter(|(comment_id, chosen)| {
            chosen.started_at != local.started_at
                && local
                    .watermark
                    .or(local.status_comment_id)
                    .is_some_and(|own| chosen.watermark.unwrap_or(*comment_id) > own)
        });
        // The bar holds whether or not a trusted record remains: with the
        // sole status comment edited by somebody else, nothing is left to
        // choose, and the stale record must not refresh the comment and
        // resume (Codex trains review, P1).
        let replaced = match (newer, barred) {
            (Some((comment_id, mut chosen)), bar) => {
                info!(
                    %root, %comment_id, barred = ?bar,
                    "a newer incarnation's record is on the root; adopting it as a replacement"
                );
                chosen.status_comment_id = Some(comment_id);
                Some(self.adopt_replacement(root, chosen, bar)?)
            }
            (None, Some(bar)) => {
                info!(%root, %bar, "a comment of the bot's on the root is not its own bytes");
                // The abort this bar forces must outrank every trusted
                // duplicate of the incarnation: the local record can be
                // BEHIND them (a database restored from backup), and an
                // abort written from it alone would lose the next loss's
                // `choose` to a surviving duplicate — resuming the train
                // the bar stopped (Codex trains review, P1).
                let mut record = local.clone();
                if let Some(peak) = trusted
                    .iter()
                    .filter(|(_, r)| r.started_at == local.started_at)
                    .map(|(_, r)| r.recovery_seq)
                    .max()
                {
                    record.recovery_seq = record.recovery_seq.max(peak);
                }
                Some(self.adopt_replacement(root, record, Some(bar))?)
            }
            (None, None) => None,
        };
        match replaced {
            Some(false) => return Ok(false),
            // A live replacement continues through the recovery tail below
            // — the inherited marker cleared, restart cleanup owed — as
            // the crawl-adopted record would have (Codex trains review,
            // P2).
            Some(true) => {}
            None => {
                if !self.recover_inherited_comment(root, &local, &comments)? {
                    return Ok(false);
                }
            }
        }
        self.inherited_mid_flight.remove(&root);
        self.needs_restart_cleanup.insert(root);
        info!(%root, "recovered an inherited mid-cascade train; resuming");
        Ok(true)
    }

    /// The comment-recovery half of [`Processor::recover_inherited`]:
    /// [`decide_comment_recovery`]'s verdict, applied. `Ok(false)` parks
    /// the recovery.
    fn recover_inherited_comment(
        &mut self,
        root: PrNumber,
        local: &TrainRecord,
        comments: &[CommentData],
    ) -> Result<bool, StoreError> {
        let local = local.clone();
        let comments = comments.to_vec();
        match decide_comment_recovery(&local, &comments, self.deps.bot_user_id) {
            CommentRecovery::Adopt(record) => {
                info!(
                    %root,
                    local_seq = local.recovery_seq,
                    remote_seq = record.recovery_seq,
                    "status comment is ahead of the store (restored from \
                     backup?); adopting its record"
                );
                if !self.adopt_replacement(root, *record, None)? {
                    return Ok(false);
                }
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

    /// Asks for a stall-retry wake-up (a released delivery: GitHub was
    /// unreachable for a pre-close step, and nothing external retries an
    /// acked webhook). Routed through the same gate as every other retry
    /// so the timers coalesce.
    pub fn request_retry(&mut self) {
        self.retry_requested = true;
    }

    /// Whether the worker loop should arm the stall-retry timer (set when
    /// supplementary recovery found GitHub unavailable, a delivery was
    /// released, or a terminal status-comment sync is owed). At most one
    /// timer is ever outstanding: while one is, further requests coalesce
    /// into it — its `RetryStalled` requeues EVERY owed obligation, so
    /// nothing is lost. Without the gate, each of N failing probes armed
    /// its own timer, every firing requeued all N obligations, and the
    /// probe traffic grew with the number of timers in flight instead of
    /// respecting `stall_retry_delay` (Codex terminal-sync review round
    /// 15, P2). The gate re-opens when the timer lands
    /// ([`Self::requeue_marked_recoveries`]).
    pub fn take_retry_request(&mut self) -> bool {
        let requested = std::mem::take(&mut self.retry_requested);
        if !requested || self.retry_timer_outstanding {
            return false;
        }
        self.retry_timer_outstanding = true;
        true
    }

    /// The worker could not spawn the timer thread it took
    /// [`Self::take_retry_request`] for: re-open the gate, so the next
    /// request tries again rather than a traffic-less repo's obligations
    /// waiting on a timer that does not exist.
    pub fn retry_timer_lost(&mut self) {
        self.retry_timer_outstanding = false;
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
        self.note_ledger_writes(outcomes)?;
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

    /// Bookkeeping over a batch's best-effort outcomes: whether an owed
    /// STACK LEDGER write landed, and whether a neutralization did.
    ///
    /// A ledger write identifies itself — its body is a ledger, naming
    /// the PR it is about and the sequence number it states — so an
    /// outcome needs no side table to be matched back. A neutralization's
    /// body deliberately parses as nothing; it is matched by the comment
    /// id it was dispatched for.
    fn note_ledger_writes(
        &mut self,
        outcomes: &[crate::cascade::EffectOutcome],
    ) -> Result<(), StoreError> {
        for outcome in outcomes {
            let Effect::GitHub(GitHubEffect::UpdateComment { comment_id, .. }) = &outcome.effect
            else {
                continue;
            };
            let Some((pr, generation)) = self.repair_write_gen.remove(comment_id) else {
                continue;
            };
            match &outcome.result {
                Ok(_) => {
                    // Clears the repair this write was dispatched for and,
                    // only then, settles the comment: proof that outlives
                    // the listing — it reads as nothing now, whatever a
                    // lagging listing (even this batch's own) still shows.
                    // A repair re-raised since the dispatch (the comment
                    // re-forged after the write landed) survives both.
                    if !self
                        .store
                        .acknowledge_ledger_repair(pr, *comment_id, generation)?
                    {
                        self.retry_requested = true;
                    }
                }
                Err(crate::cascade::EffectError::Permanent {
                    kind: crate::types::TrainErrorKind::NotFound,
                    ..
                }) => {
                    // Gone, or a passing 404 (losing repository access
                    // 404s comments that exist — Codex ledger review
                    // round 23). Either way there is nothing to write to
                    // NOW: the repair becomes a WATCHED comment for the
                    // next listings — shown, it is raised again; absent
                    // across spaced listings, concluded gone. Never
                    // concluded dead here: only a deletion webhook is
                    // proof.
                    self.store
                        .watch_ledger_comment_after_404(pr, *comment_id, generation)?;
                    self.retry_requested = true;
                }
                Err(e) => {
                    warn!(
                        %pr, comment = %comment_id, error = ?e,
                        "ledger neutralization failed; still owed"
                    );
                    self.retry_requested = true;
                }
            }
        }
        for outcome in outcomes {
            let (posted_to, body) = match &outcome.effect {
                Effect::GitHub(GitHubEffect::UpdateComment { body, .. }) => (None, body),
                Effect::GitHub(GitHubEffect::PostComment { pr, body }) => (Some(*pr), body),
                _ => continue,
            };
            let Some(ledger) = crate::status::parse_stack_ledger(body) else {
                continue;
            };
            if posted_to.is_some_and(|pr| pr != ledger.pr) {
                continue; // not a ledger of ours, whatever it looks like
            }
            match &outcome.result {
                Ok(crate::cascade::EffectResponse::GitHub(GitHubResponse::CommentPosted {
                    id,
                })) => {
                    // Record where the ledger lives BEFORE clearing the
                    // obligation: a crash in between re-writes the ledger,
                    // which is idempotent, while the reverse order could
                    // post a second one. The same transaction settles the
                    // unresolved row this post was dispatched under.
                    self.store.record_stack_ledger_posted(
                        ledger.pr,
                        *id,
                        ledger.seq,
                        Utc::now(),
                    )?;
                    self.clear_written_ledger(ledger.pr)?;
                }
                Ok(_) => self.clear_written_ledger(ledger.pr)?,
                Err(e) => {
                    warn!(
                        pr = %ledger.pr, error = ?e,
                        "stack ledger write failed; the ledger is still owed"
                    );
                    // A rewrite that found nothing at the recorded id:
                    // deleted with the webhook still to come (or lost),
                    // or a passing 404. The id is forgotten either way
                    // (the obligation is still owed — this write failed —
                    // so re-owing it is idempotent), and the comment
                    // becomes a QUESTION for the next listing — shown, it
                    // is adopted back; absent for long enough, a fresh
                    // one is posted. A failed POST needs nothing here:
                    // its unresolved row was written before dispatch, and
                    // stands until a listing settles it.
                    if let (
                        Effect::GitHub(GitHubEffect::UpdateComment { comment_id, .. }),
                        crate::cascade::EffectError::Permanent {
                            kind: crate::types::TrainErrorKind::NotFound,
                            ..
                        },
                    ) = (&outcome.effect, e)
                    {
                        self.store.retire_stack_ledger_watching(
                            ledger.pr,
                            *comment_id,
                            ledger.seq,
                            Utc::now(),
                        )?;
                    }
                    // The binding dies with the attempt: the next write
                    // binds the generation IT reads.
                    self.ledger_write_gen.remove(&ledger.pr);
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
        let mut cleanup = self.boundary_cleanup(root)?;
        let Some(owed) = self
            .store
            .owed_status_syncs()?
            .into_iter()
            .find(|o| o.root == root && o.started_at == started_at)
        else {
            return self.finish_boundary(root, cleanup);
        };
        let listing = outcomes.into_iter().find_map(|o| match o.result {
            Ok(crate::cascade::EffectResponse::GitHub(GitHubResponse::Comments(
                CommentListing::Complete(comments),
            ))) => Some(comments),
            _ => None,
        });
        let Some(comments) = listing else {
            warn!(%root, "status-sync probe failed; retrying at the stall cadence");
            self.retry_requested = true;
            return self.finish_boundary(root, cleanup);
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
            // GitHub is not read-after-write consistent: a comment posted
            // moments ago can be missing from a listing that later
            // returns it, and an obligation with no id was created in
            // exactly that window. Reading one absence as deletion drops
            // the obligation, and the comment then says ACTIVE forever —
            // the resurrection this whole mechanism exists to prevent
            // (Codex terminal-sync review round 9, P1). So absence must
            // be STABLE before it is believed; the count is durable, and
            // seeing the comment resets it.
            let cooldown = chrono::Duration::from_std(self.deps.stall_retry_delay)
                .unwrap_or_else(|_| chrono::Duration::seconds(30));
            let absences = self
                .store
                .note_absent_probe(root, started_at, self.now(), cooldown)?;
            if absences < ABSENT_PROBES_BEFORE_BELIEVED {
                warn!(
                    %root, absences,
                    "status comment missing from the listing; probing again before believing it"
                );
                self.retry_requested = true;
                return self.finish_boundary(root, cleanup);
            }
            info!(
                %root, absences,
                "no live status comment for this incarnation; nothing stale survives"
            );
            self.store.delete_owed_status_sync(root, started_at)?;
            return self.finish_boundary(root, cleanup);
        };
        let comment_id = live.id;
        // Pin the resolved id so the rewrite's outcome matches this
        // obligation (it may have been owed with no id at all).
        self.store
            .set_owed_status_comment(root, started_at, comment_id)?;
        let body = match crate::status::format_status_comment(&owed.record, &owed.message) {
            Ok(body) => body,
            Err(e) => {
                error!(%root, error = %e, "cannot format the terminal status comment; sync dropped");
                self.store.delete_owed_status_sync(root, started_at)?;
                return self.finish_boundary(root, cleanup);
            }
        };
        // The update may ALREADY have landed: the process can die between
        // the write and its outcomes, leaving an obligation against a
        // comment that says exactly what it owes. Writing again would be
        // pointless, and against a token that has lost the right to edit
        // comments it fails forever, retrying an obligation that is
        // already satisfied (Codex terminal-sync review round 9, P2).
        // The comparison is between the PARSED records: the live path's
        // prose need not match this reconstruction, and the machine block
        // is the whole of what recovery reads.
        if let (Ok(live_record), Ok(owed_record)) = (
            crate::status::parse_status_comment(&live.body),
            crate::status::parse_status_comment(&body),
        ) && live_record == owed_record
        {
            info!(%root, "the status comment already carries the terminal record; sync satisfied");
            self.store.delete_owed_status_sync(root, started_at)?;
            return self.finish_boundary(root, cleanup);
        }
        self.in_flight = Some(root);
        let mut best_effort = vec![Effect::GitHub(GitHubEffect::UpdateComment {
            comment_id,
            body,
        })];
        best_effort.append(&mut cleanup);
        Ok(Some(SagaBatch {
            root,
            effects: Vec::new(),
            best_effort,
            feedback: false,
            // As any other batch for this root: a rewrite may be the only
            // batch it gets, and the worktree cleanup an inherited train
            // owes must not be dropped because a sync went first.
            restart_cleanup: self.needs_restart_cleanup.remove(&root),
        }))
    }

    /// Applies queued stops and deferred handler aborts at an observation
    /// boundary — a completed batch, nothing in flight behind it — and
    /// returns the cleanup that belongs in a batch rooted at `batch_root`.
    /// A queued stop (or a deferred abort) acts HERE: letting queued work
    /// jump ahead leaves an acknowledged stop unapplied across however
    /// many comment-only batches a slow GitHub makes us retry (Codex
    /// terminal-sync review round 9, P2). The boundary is taken only when
    /// there is something to apply: `apply_stops` re-queues the owed
    /// syncs, and doing that unconditionally would re-probe the very
    /// obligation a probe boundary is settling, forever.
    ///
    /// A stop's cleanup names the worktree it removes, so it is safe in
    /// any batch. An abort's is root-relative, so `cleanup_for_batch`
    /// keeps `batch_root`'s share in the boundary's own batch and queues
    /// the rest AHEAD of whatever waits — a `start` queued for an aborted
    /// root would otherwise run first, replace the record, and leave a
    /// later recompute nothing to find: no worktree removal, no abort
    /// notice (Codex terminal-sync review round 14, P2).
    fn boundary_cleanup(&mut self, batch_root: PrNumber) -> Result<Vec<Effect>, StoreError> {
        let stops = self.take_queued_stops()?;
        let aborts = self.take_deferred_aborts();
        if stops.is_empty() && aborts.is_empty() {
            return Ok(Vec::new());
        }
        let (mut cleanup, _) = self.apply_stops(stops)?;
        let groups = self.apply_deferred_aborts(aborts)?;
        let (mine, _) = self.cleanup_for_batch(groups, batch_root);
        cleanup.extend(mine);
        Ok(cleanup)
    }

    /// Clears the obligation the in-flight write for `pr` was made for.
    /// Nothing is cleared when the generation is unknown (a restart lost
    /// it) or has moved on (the ledger was dirtied again while the write
    /// was out): the obligation stands, and the next probe settles it.
    fn clear_written_ledger(&mut self, pr: PrNumber) -> Result<(), StoreError> {
        if let Some(generation) = self.ledger_write_gen.remove(&pr) {
            self.store.clear_owed_stack_ledger(pr, generation)?;
        }
        Ok(())
    }

    /// What `pr`'s ledger comment should say right now, or `None` when
    /// the PR left the cache and there is nothing to state. The sequence
    /// number is the state's own: an event appended while the write is in
    /// flight carries a higher one, so it survives this write.
    fn desired_ledger(&self, pr: PrNumber) -> Option<crate::status::StackLedger> {
        let cached = self.store.state().prs.get(&pr)?;
        Some(crate::status::StackLedger {
            pr,
            declared: cached
                .predecessor
                .zip(cached.predecessor_comment_id)
                .map(|(predecessor, owner)| crate::status::Declaration { predecessor, owner }),
            seq: self.store.next_seq().saturating_sub(1),
            settled_through: cached.declarations_settled_through,
        })
    }

    /// How far apart two listings must be for their agreement on an
    /// absence to count as evidence rather than one stale read twice.
    fn absence_cooldown(&self) -> chrono::Duration {
        chrono::Duration::from_std(self.deps.stall_retry_delay)
            .unwrap_or_else(|_| chrono::Duration::seconds(30))
    }

    /// The neutralizations owed on `pr`, each bound to the repair it is
    /// dispatched for.
    fn ledger_repair_writes(&mut self, pr: PrNumber) -> Result<Vec<Effect>, StoreError> {
        let mut writes = Vec::new();
        // A train rooted here whose status comment id the store does not
        // know yet — its post landed and the process died before the
        // event that records it — may own ANY bot comment on this PR,
        // including one an edit webhook named as a forgery. The same
        // goes for a train inherited mid-flight whose recovery has not
        // run yet: its recorded id may be stale (a recovery repost landed
        // before the crash). Nothing is neutralized here until the status
        // machinery has resolved the id (it probes by incarnation at
        // startup and at the stall cadence); registering the id cancels
        // the repair if it was the one.
        let status_id_unresolved = self.inherited_mid_flight.contains(&pr)
            || self
                .store
                .state()
                .active_trains
                .get(&pr)
                .is_some_and(|train| train.state.is_active() && train.status_comment_id.is_none())
            // ...and a terminal sync still owed here means the comment's
            // identity is unverified whatever id it carries: a recovery
            // repost can have landed before its id committed, leaving
            // the sync naming a comment that no longer exists while the
            // live one goes unrecognised until the sync's probe finds it.
            || self
                .store
                .owed_status_syncs()?
                .iter()
                .any(|owed| owed.root == pr);
        if status_id_unresolved && !self.store.ledger_repairs(pr)?.is_empty() {
            info!(%pr, "ledger repairs deferred: a train's status comment here is unresolved");
            self.retry_requested = true;
            return Ok(writes);
        }
        // Never the recorded ledger: a post's comment can be named a
        // forgery (edited under us) before its acknowledgement records
        // it. Recording drops that repair; the obligation the edit
        // re-owed rewrites the comment instead.
        let recorded = self
            .store
            .state()
            .prs
            .get(&pr)
            .and_then(|cached| cached.ledger_comment_id);
        for repair in self.store.ledger_repairs(pr)? {
            if Some(repair.comment_id) == recorded {
                continue;
            }
            // Nor a train's status comment, learned to be one since the
            // repair was raised: registering it cancelled the repair,
            // but the ownership is checked again here, at dispatch.
            if self.store.is_status_comment(repair.comment_id)? {
                continue;
            }
            self.repair_write_gen
                .insert(repair.comment_id, (pr, repair.generation));
            writes.push(Effect::GitHub(GitHubEffect::UpdateComment {
                comment_id: repair.comment_id,
                body: NEUTRALIZED_LEDGER_BODY.to_owned(),
            }));
        }
        Ok(writes)
    }

    /// The outcome of a ledger probe (`ListComments` on the PR). The
    /// listing never discharges anything — only an acknowledged write
    /// does — it DISCOVERS: the comment to adopt when the store knows no
    /// id (a crash between a post and the event recording it leaves one
    /// unrecorded); forgeries and stale duplicates to neutralize (every
    /// other bot comment reading as this PR's ledger); and the fate of
    /// comments that may exist unrecorded — shown, they join the above;
    /// absent across spaced listings, they are concluded never to have
    /// landed. Only then may a fresh ledger be posted.
    fn on_ledger_probe(
        &mut self,
        pr: PrNumber,
        listed_at: chrono::DateTime<Utc>,
        outcomes: Vec<crate::cascade::EffectOutcome>,
    ) -> Result<Option<SagaBatch>, StoreError> {
        let mut cleanup = self.boundary_cleanup(pr)?;
        let listing = outcomes.into_iter().find_map(|o| match o.result {
            Ok(crate::cascade::EffectResponse::GitHub(GitHubResponse::Comments(
                CommentListing::Complete(comments),
            ))) => Some(comments),
            _ => None,
        });
        let Some(comments) = listing else {
            // The writes that rode along may have landed — but a sync is
            // a rewrite AND a discovery, and this one discovered nothing:
            // the discovery stays owed, and the next look lists again
            // (the extra rewrite is idempotent).
            warn!(%pr, "stack-ledger probe failed; retrying at the stall cadence");
            self.retry_requested = true;
            return self.finish_boundary(pr, cleanup);
        };
        // What the store knows better than the listing: a comment whose
        // deletion webhook arrived is a ghost here (Codex ledger review
        // round 22), and one whose neutralization acknowledged reads as
        // nothing whatever body the listing lags behind with — including
        // the listing taken in this very batch, before the write ran.
        let settled = self.store.settled_ledger_comments(pr)?;
        let dead: Vec<crate::types::CommentId> = settled
            .iter()
            .filter(|(_, dead)| *dead)
            .map(|(id, _)| *id)
            .collect();
        // ...and a train's status comment is never a ledger candidate,
        // whatever it reads as: neither adopted nor neutralized.
        let mut ours: Vec<(crate::types::CommentId, crate::status::StackLedger)> = Vec::new();
        for c in comments
            .iter()
            .filter(|c| !settled.iter().any(|(id, _)| *id == c.id))
            .filter(|c| c.author_id == self.deps.bot_user_id)
        {
            if let Some(l) = crate::status::parse_stack_ledger(&c.body).filter(|l| l.pr == pr)
                && !self.store.is_status_comment(c.id)?
            {
                ours.push((c.id, l));
            }
        }
        // The unresolved comments this listing settles. Shown — by id, or
        // by the sequence number a lost-response post carried — the
        // question is answered. A comment the store WROTE is what may be
        // adopted below (the only proof of authorship there is; anything
        // else reading as a ledger is a forgery); its row is settled by
        // the adoption, or as a duplicate below. A WATCHED forgery seen
        // again is owed neutralizing again, whatever body the listing
        // lags behind with: the webhook that named it is the evidence,
        // not the listing. Not shown, absence counts once per cooldown; a
        // post that never landed is concluded so only when that absence
        // is stable (round 1, P2).
        let mut unsettled = false;
        // A comment the store WROTE, shown, is adoptable WHATEVER body the
        // listing shows for it — a body is the listing's word, and the
        // rewrite that follows adoption states the truth by id. Its
        // sequence number is the one the store wrote it with.
        let mut adoptable: Vec<(crate::types::CommentId, u64)> = Vec::new();
        for row in self.store.unresolved_ledgers(pr)? {
            let shown = match row.comment_id {
                Some(id) => comments
                    .iter()
                    .any(|c| c.id == id && !dead.contains(&id))
                    .then_some(id),
                None => ours
                    .iter()
                    .find(|(_, l)| l.seq == row.seq)
                    .map(|(id, _)| *id),
            };
            if let Some(id) = shown {
                if row.ours {
                    adoptable.push((id, row.seq));
                } else if self.store.is_status_comment(id)? {
                    // Learned to be a train's status comment since it was
                    // watched: nothing is owed to it.
                    self.store.resolve_unresolved_ledger(row.row)?;
                } else {
                    self.store
                        .resolve_unresolved_ledger_as_repair(row.row, pr, id)?;
                }
                continue;
            }
            let absences = self.store.note_absent_unresolved_ledger(
                row.row,
                listed_at,
                self.now(),
                self.absence_cooldown(),
            )?;
            if absences >= ABSENT_PROBES_BEFORE_BELIEVED {
                // Concluded gone. For a comment of the store's own that
                // is the end of it: nothing stands. For a WATCHED forgery
                // it is a bounded bet — its deletion webhook may still be
                // on its way, or it may reappear once the listings catch
                // up, and then it is found and neutralized at the next
                // sync on this PR (a topology change, or any change to a
                // bot comment here). Watching it for ever instead would
                // mean listing this PR at every stall cadence until a
                // webhook that may never come: the bound is deliberate.
                self.store.resolve_unresolved_ledger(row.row)?;
            } else {
                unsettled = true;
            }
        }
        // The write, if one is owed and the listing decides its target.
        // The rewrite of a RECORDED comment rode along with the probe (the
        // pump dispatched it best-effort); its acknowledgement was noted
        // before this boundary.
        let owed = self
            .store
            .owed_stack_ledgers()?
            .into_iter()
            .find(|o| o.pr == pr);
        let recorded = self
            .store
            .state()
            .prs
            .get(&pr)
            .and_then(|cached| cached.ledger_comment_id);
        let desired = self.desired_ledger(pr);
        let mut write: Option<Effect> = None;
        let mut post_seq: Option<u64> = None;
        let live: Option<crate::types::CommentId> = match (desired, recorded) {
            (None, _) => {
                // The PR left the cache, or never entered it (the bot
                // replies on PRs it never cached: authorization rejections
                // land before precaching). There is no declaration to
                // RECORD — but anything of ours reading as a ledger here
                // is a forgery a crawl would believe (round 17), so
                // nothing is live and everything is neutralized.
                if let Some(owed) = owed {
                    info!(%pr, "no cached PR for an owed stack ledger; dropping the obligation");
                    self.store.clear_owed_stack_ledger(pr, owed.generation)?;
                }
                None
            }
            (Some(_), Some(id)) => Some(id),
            (Some(desired), None) => {
                // Adopt the comment the store wrote, if the listing shows
                // it — the newest by sequence number, should a crash have
                // left several — rather than post a second (round 1).
                // Recorded BEFORE it is written to, or the next crash
                // leaves it unrecorded again. Nothing else is adopted: a
                // ledger-shaped comment the store cannot prove it wrote
                // is a forgery, however it got there.
                let adoptable = adoptable
                    .iter()
                    .max_by_key(|(id, seq)| (*seq, *id))
                    .copied();
                match adoptable {
                    Some((comment_id, seq)) => {
                        info!(%pr, %comment_id, "adopting an unrecorded stack ledger comment");
                        self.store
                            .record_stack_ledger_posted(pr, comment_id, seq, Utc::now())?;
                        if owed.is_some() {
                            write = Some(Effect::GitHub(GitHubEffect::UpdateComment {
                                comment_id,
                                body: crate::status::format_stack_ledger(&desired),
                            }));
                        }
                        Some(comment_id)
                    }
                    None if owed.is_some() && !unsettled => {
                        // Nothing of ours, and no comment that MAY be
                        // ours is left in question: post. Its unresolved
                        // row is written below, AFTER the discovery over
                        // this listing — before dispatch still, so a lost
                        // response or a crash before the acknowledgement
                        // leaves the question, not a duplicate (round
                        // 24) — because a listed forgery carrying this
                        // very sequence number would otherwise claim the
                        // row as its own and settle it before the post
                        // was ever made.
                        post_seq = Some(desired.seq);
                        write = Some(Effect::GitHub(GitHubEffect::PostComment {
                            pr,
                            body: crate::status::format_stack_ledger(&desired),
                        }));
                        None
                    }
                    None => None,
                }
            }
        };
        // Every OTHER comment of ours reading as this PR's ledger is a
        // forgery or a stale duplicate. Left alone, one with a doctored
        // sequence number wins a crawl's duplicate arbitration over the
        // truth for ever (round 16) — so each is owed neutralizing,
        // durably, before its write is dispatched (round 21). One the
        // store wrote and did not adopt (a second orphan, say) has its
        // row settled in the same transaction as its repair is raised.
        let repairs = self.store.ledger_repairs(pr)?;
        let rows = self.store.unresolved_ledgers(pr)?;
        for (id, ledger) in ours.iter().filter(|(id, _)| Some(*id) != live) {
            let row = rows.iter().find(|r| match r.comment_id {
                Some(named) => named == *id,
                None => r.seq == ledger.seq,
            });
            match row {
                Some(row) => {
                    self.store
                        .resolve_unresolved_ledger_as_repair(row.row, pr, *id)?;
                }
                None if !repairs.iter().any(|r| r.comment_id == *id) => {
                    self.store.add_ledger_repair(pr, *id)?;
                }
                None => {}
            }
        }
        // The post decided above gets its unresolved row now that the
        // discovery over this listing is complete.
        if let Some(seq) = post_seq {
            self.store.add_unresolved_ledger(pr, None, seq, true)?;
        }
        // Discovery done: whatever this listing showed is on the books.
        self.store.clear_ledger_discovery(pr)?;
        let mut best_effort: Vec<Effect> = Vec::new();
        if let Some(write) = write {
            if let Some(owed) = owed {
                self.ledger_write_gen.insert(pr, owed.generation);
            }
            best_effort.push(write);
        }
        best_effort.append(&mut self.ledger_repair_writes(pr)?);
        if unsettled || self.store.ledger_pending(pr)? {
            // Something is still owed after this batch at best: an
            // unresolved comment awaiting a spaced look, a neutralization
            // whose acknowledgement is yet to come, or a write that may
            // fail. The stall cadence brings the next look.
            self.retry_requested = true;
        }
        if best_effort.is_empty() {
            return self.finish_boundary(pr, cleanup);
        }
        best_effort.append(&mut cleanup);
        self.in_flight = Some(pr);
        Ok(Some(SagaBatch {
            root: pr,
            effects: Vec::new(),
            best_effort,
            feedback: false,
            restart_cleanup: self.needs_restart_cleanup.remove(&pr),
        }))
    }

    /// Leaves an observation boundary: runs whatever the stops and aborts
    /// applied there left behind, or moves on to the next work. Unlike
    /// `finish_or_pump` this does NOT re-queue the owed syncs — the
    /// obligation a probe boundary settles is often still owed, and
    /// re-queueing it here would probe it again immediately, forever.
    fn finish_boundary(
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
            restart_cleanup: self.needs_restart_cleanup.remove(&root),
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
        for pr in self.store.ledger_pending_prs()? {
            self.queue(PendingWork::LedgerSync { pr });
        }
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
        // The timer this call answers has landed: the gate re-opens, and
        // the next failure arms the next one.
        self.retry_timer_outstanding = false;
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
                    // A previous incarnation of THIS root may still owe
                    // its comment the final word. Discharge that first:
                    // once the successor's saga starts, its feedback
                    // batches chain without returning here, and the
                    // retired incarnation's comment can sit saying ACTIVE
                    // for the whole of it (Codex terminal-sync review
                    // round 11, P1). Deferred at most ONCE per start —
                    // the obligation may be undischargeable (a token that
                    // cannot edit comments), and a start that waits for
                    // it forever is a worse failure than a stale comment.
                    if let Some(owed) = self.store.owed_status_syncs()?.into_iter().find(|o| {
                        o.root == pr && !self.deferred_for_sync.contains(&(id, o.started_at))
                    }) {
                        self.deferred_for_sync.insert((id, owed.started_at));
                        info!(
                            %pr,
                            "discharging a retired incarnation's owed sync before its \
                             successor starts"
                        );
                        self.pending.push_front(PendingWork::Start { id, pr });
                        self.sync_probes.insert(owed.root, owed.started_at);
                        self.in_flight = Some(owed.root);
                        return Ok(Some(SagaBatch {
                            root: owed.root,
                            effects: vec![Effect::GitHub(GitHubEffect::ListComments {
                                pr: owed.root,
                            })],
                            best_effort: Vec::new(),
                            feedback: false,
                            restart_cleanup: false,
                        }));
                    }
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
                PendingWork::LedgerSync { pr } => {
                    if !self.store.ledger_pending(pr)? {
                        continue; // settled meanwhile
                    }
                    // Nothing touches a ledger before the crawl has landed:
                    // an obligation reloaded into an unbootstrapped store
                    // would be discharged against an empty cache. It is
                    // durable, and re-queued once the crawl commits
                    // (`after_bootstrap`).
                    if state.default_branch.is_empty() {
                        continue;
                    }
                    // The probe, always: the listing is where forgeries,
                    // orphans and lost posts are discovered, and topology
                    // changes are rare next to the cascade's own traffic.
                    // The writes the store can already address ride along
                    // best-effort — the rewrite of a recorded ledger, and
                    // the neutralizations owed — so a sync that knows its
                    // target costs one round trip, not two.
                    let mut best_effort = Vec::new();
                    let owed = self
                        .store
                        .owed_stack_ledgers()?
                        .into_iter()
                        .find(|o| o.pr == pr);
                    let recorded = self
                        .store
                        .state()
                        .prs
                        .get(&pr)
                        .and_then(|cached| cached.ledger_comment_id);
                    if let (Some(owed), Some(comment_id), Some(desired)) =
                        (owed, recorded, self.desired_ledger(pr))
                    {
                        self.ledger_write_gen.insert(pr, owed.generation);
                        best_effort.push(Effect::GitHub(GitHubEffect::UpdateComment {
                            comment_id,
                            body: crate::status::format_stack_ledger(&desired),
                        }));
                    }
                    best_effort.append(&mut self.ledger_repair_writes(pr)?);
                    // The discovery this listing makes is owed until it
                    // is processed — durably, so a crash between the two
                    // (or a listing that fails beside writes that land)
                    // leaves the look owed, not lost.
                    self.store.mark_ledger_discovery_owed(pr)?;
                    self.ledger_probes.insert(pr, self.now());
                    self.in_flight = Some(pr);
                    return Ok(Some(SagaBatch {
                        root: pr,
                        effects: vec![Effect::GitHub(GitHubEffect::ListComments { pr })],
                        best_effort,
                        feedback: false,
                        restart_cleanup: false,
                    }));
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
                PendingWork::CapturedCleanup { root, effects } => {
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
                    let groups = self.apply_deferred_aborts(vec![(root, error)])?;
                    let (cleanup, _) = self.cleanup_for_batch(groups, root);
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
        if let Some(listed_at) = self.ledger_probes.remove(&root) {
            return self.on_ledger_probe(root, listed_at, outcomes);
        }
        if !feedback {
            // A completed best-effort batch is an observation boundary
            // like any other: nothing is in flight behind it, so queued
            // stops and deferred aborts act HERE. Pumping straight past it
            // let a queued restart run against a successor train a review
            // dismissal had already doomed — durably rejected as "already
            // running", and the deferred abort then left no train at all
            // (Codex terminal-sync review round 16, P2).
            let cleanup = self.boundary_cleanup(root)?;
            return self.finish_boundary(root, cleanup);
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
            let groups = self.apply_deferred_aborts(aborts)?;
            let (mut abort_cleanup, _) = self.cleanup_for_batch(groups, root);
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
            let groups = self.apply_deferred_aborts(aborts)?;
            let (mut abort_cleanup, _) = self.cleanup_for_batch(groups, root);
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
        let groups = self.apply_deferred_aborts(aborts)?;
        let (mut abort_cleanup, aborted) = self.cleanup_for_batch(groups, root);
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

    /// Takes the cleanup an abort produced and returns only what belongs
    /// in a batch rooted at `batch_root`. Everything else is queued as
    /// [`PendingWork::CapturedCleanup`] under its own root, AHEAD of the
    /// queue: `GitEffect::CleanupWorktree` is ROOT-RELATIVE — the executor
    /// resolves it against the batch's root — so running one train's
    /// cleanup inside another's batch cleans the wrong worktree and leaves
    /// the right one dirty (Codex terminal-sync review round 12, P2).
    pub(super) fn cleanup_for_batch(
        &mut self,
        groups: Vec<(PrNumber, Vec<Effect>)>,
        batch_root: PrNumber,
    ) -> (Vec<Effect>, HashSet<PrNumber>) {
        let mut mine = Vec::new();
        let mut roots = HashSet::new();
        for (root, effects) in groups {
            roots.insert(root);
            if root == batch_root {
                mine.extend(effects);
            } else {
                // AHEAD of whatever is queued: a `Start` for this root
                // waiting in the queue would otherwise replace the record
                // this cleanup belongs to.
                self.pending
                    .push_front(PendingWork::CapturedCleanup { root, effects });
            }
        }
        (mine, roots)
    }

    /// Applies deferred handler aborts now, returning each root's
    /// best-effort cleanup. Idempotent: a train that completed, stopped,
    /// or aborted in the meantime is skipped.
    fn apply_deferred_aborts(
        &mut self,
        aborts: Vec<(PrNumber, crate::types::TrainError)>,
    ) -> Result<Vec<(PrNumber, Vec<Effect>)>, StoreError> {
        let mut cleanup: Vec<(PrNumber, Vec<Effect>)> = Vec::new();
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
            cleanup.push((
                root,
                cascade::handler_abort_cleanup(self.store.state(), root),
            ));
        }
        // Like stops: the terminal event may owe a status sync that no
        // later outcome refers to, and this path can bypass both
        // `integrate_plan` and `finish_or_pump` (Codex terminal-sync
        // review round 6, P1).
        self.queue_owed_status_syncs()?;
        Ok(cleanup)
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

/// What the first-contact crawl decided about the delivery that woke it.
enum Bootstrap {
    /// GitHub was unavailable (any failure): release the delivery; the
    /// repo's queue pauses at the stall cadence.
    Unavailable,
    /// The triggering comment was not in the listing, seen for the first
    /// time: GitHub is not read-after-write consistent, so the delivery is
    /// released and re-crawled once before being believed.
    TriggerDoubted,
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
    /// and with the payload's sender — the creator, or the editor — as the
    /// writer of its current bytes: equal bodies alone prove nothing about
    /// who wrote them, and a non-author restoring the author's withdrawn
    /// text must not pass as the author's own utterance.
    Comment {
        pr: PrNumber,
        id: crate::types::CommentId,
        body: String,
        sender: u64,
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
                        sender: c.sender_id,
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

impl TriggerFreshness {
    /// [`TriggerFreshness::disagrees_with`], against the state a crawl
    /// already cached rather than the PR it fetched: after a restart the
    /// cache IS the present the crawl saw.
    fn disagrees_with_cache(&self, state: &crate::state::RepoState) -> bool {
        let TriggerFreshness::PullRequest { pr, .. } = self else {
            return false;
        };
        let Some(cached) = state.prs.get(pr) else {
            return false;
        };
        let present = PrData {
            number: cached.number,
            head_sha: cached.head_sha.clone(),
            head_ref: cached.head_ref.clone(),
            base_ref: cached.base_ref.clone(),
            state: cached.state.clone(),
            is_draft: cached.is_draft,
            author_id: 0,
        };
        self.disagrees_with(&present)
    }
}

/// The ownership a comment delivery the crawl SUPPRESSES as stale would
/// have transferred, decided once, here, and recorded durably with the
/// close. A creation or an edit is suppressed when the comment is gone,
/// or reads otherwise, by the time the crawl judges it: handling it would
/// record a body the comment no longer has. But if what it SAID was the
/// PR author's own restatement of the very predecessor the PR holds, from
/// a comment newer than the current owner, then live it took ownership of
/// that edge — a creation and an edit alike, the restatement branch of
/// the handler — and every later deletion or edit of that comment was the
/// owner's, a retraction or an update. The transfer records no body, only
/// that this comment owns the edge from here on, so every later event on
/// it — the final one or any in between, a stranger's or the author's —
/// goes through the owner's own logic and the sender gate. Anything else
/// a suppressed delivery said — prose, a declaration the store would have
/// refused — is no ownership, and a verdict this store hands down on a
/// later edit of the comment stands (Codex first-contact review, P1 and
/// P2, twice; the recovery model found the edit case: an author who
/// restated by editing a stranger's comment, then edited it away, was
/// left stacked). The handler's own exclusions apply first: one of the
/// BOT's comments is never a command, whatever anyone edits it into, so
/// live it took nothing — and handing it the edge would hand it to a
/// comment every later event on which is ignored, with the original
/// declaration's deletion no longer retracting anything (Codex
/// first-contact review, P2).
fn restatement_transfer(
    state: &crate::state::RepoState,
    event: &GitHubEvent,
    deps: &WorkerDeps,
) -> Option<StateEventPayload> {
    let GitHubEvent::IssueComment(c) = event else {
        return None;
    };
    if c.author_id == deps.bot_user_id {
        return None;
    }
    // The utterance is the sender's: the creator of a created payload, the
    // editor of an edited one. Only the PR author's own declares.
    if c.action == CommentAction::Deleted || c.sender_id == 0 || c.sender_id != c.pr_author_id {
        return None;
    }
    let pr = c.pr_number?;
    let cached = state.prs.get(&pr)?;
    let (predecessor, owner) = (cached.predecessor?, cached.predecessor_comment_id?);
    let restates = matches!(
        parse_command(&c.body, &deps.bot_name),
        Some(Command::Predecessor(target)) if target == predecessor
    );
    (restates && c.comment_id > owner).then_some(StateEventPayload::PredecessorDeclared {
        pr,
        predecessor,
        comment_id: c.comment_id,
    })
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
/// The events that bring the cache's picture of `pr` to `present`: a fill
/// (which refreshes head, base and draft state, and closes or merges), and
/// a REOPEN when the cache has the PR closed and GitHub has it open — a
/// fill alone preserves a cached terminal state.
/// Whether the listed comment's current bytes are the payload's sender's:
/// its creator's for a `created` payload, its editor's for an `edited`
/// one, by GitHub's own provenance (`body_written_by`). An equal body
/// alone proves nothing about who put it there: the author may have
/// created the declaration, retracted it by edit, and a stranger restored
/// its text — believing the creation would revive the withdrawn edge
/// (Codex first-contact review, P1, twice: the edit case, then the
/// creation case). The price is an edge live keeps and recovery drops —
/// a stranger's edit to the SAME text — which is the permitted direction:
/// the author re-declares. A listing that lags shows an older writer,
/// which is a doubt like any other.
fn written_by_the_sender(listed: &CommentData, sender: u64) -> bool {
    listed.body_written_by(sender).is_some()
}

pub(crate) fn reconcile_cache_events(
    state: &crate::state::RepoState,
    pr: PrNumber,
    present: &PrData,
) -> Vec<StateEventPayload> {
    let mut events = cache_fill_events(pr, present, MergeStateStatus::Unknown);
    let cached_closed = state
        .prs
        .get(&pr)
        .is_some_and(|c| matches!(c.state, PrState::Closed));
    if cached_closed && present.state.is_open() {
        events.push(StateEventPayload::PrReopened { pr });
    }
    events
}

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
