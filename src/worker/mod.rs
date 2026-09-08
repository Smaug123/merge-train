//! The per-repo worker: the single writer that owns a repo's [`Store`] and
//! drives the cascade engine (M5).
//!
//! # Why a thread per repo
//!
//! `rusqlite` is synchronous and the [`Store`] is a single-writer object (it
//! holds an exclusive per-repo `flock` and an in-memory `RepoState` it mutates
//! in lockstep with the durable log). So each repo gets one dedicated OS thread
//! that owns its `Store` for the process's lifetime and is the *only* code that
//! ever touches it. Concurrency exists *between* repos (independent workers),
//! never *within* one (each worker processes its deliveries serially). This is
//! the actor model: a bounded mailbox ([`mpsc`]) feeds a serial consumer.
//!
//! # The async/sync boundary (plan P1-H)
//!
//! The axum server is async and must **never** open a `Store`: a second
//! `Store::open` on a repo whose worker already holds the `flock` would block
//! forever. Instead the handler routes a raw delivery to the repo's worker over
//! the mailbox and awaits a durable-enqueue ack before replying 200. All
//! `Store` ownership — including the `Store::open` that acquires the lock —
//! happens *on the worker thread*, so the lock is born and dies with its owner.
//!
//! # Where the work happens (M5)
//!
//! Decisions live in [`pipeline::Processor`] (the per-delivery pipeline and
//! the saga machine); effect execution lives in [`executor`]. The worker
//! thread never blocks on effects: each engine plan's effects run on a
//! spawned **executor thread** whose outcomes come back through this same
//! mailbox ([`WorkerMsg::SagaOutcomes`]), so intake acks and delivery
//! processing continue while a multi-minute git saga runs. One saga executes
//! at a time per repo; deliveries keep processing mid-saga (that is how stop
//! commands and head-moved observations reach the engine), and the engine
//! work they trigger queues for the saga slot.

pub mod authz;
pub mod executor;
mod pipeline;
pub mod recovery;
#[cfg(test)]
mod tests;

pub use pipeline::{GitSettings, PipelineOutcome, WorkerDeps};

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::Utc;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tracing::{error, info, warn};

use crate::cascade::{EffectError, EffectOutcome};
use crate::effects::Effect;
use crate::git::CommitIdentity;
use crate::git::interpreter::WorktreeGitInterpreter;
use crate::github::OctocrabClient;
use crate::store::{Store, StoreError};
use crate::types::{PrNumber, RepoId};

use executor::{GitHubExec, SagaBatch, execute_batch};
use pipeline::Processor;

/// Per-repo mailbox depth (message *count*). The byte budget
/// ([`MAX_INFLIGHT_INTAKE_BYTES`]) — not this — bounds intake *memory*; this
/// caps the number of queued messages and the per-turn intake batch.
const MAILBOX_CAPACITY: usize = 1024;

/// Process-wide ceiling on in-flight intake bytes: the summed body size of
/// deliveries accepted but not yet durably enqueued. The axum handler reserves
/// `body.len()` here before handing a delivery to a worker, so a burst of large
/// (signature-verified) payloads applies backpressure — the handler waits —
/// instead of accumulating in mailboxes and risking OOM (a count-only bound
/// could hold `MAILBOX_CAPACITY` × 25 MiB per repo; Codex review #53). Must
/// exceed the 25 MiB body limit so a single max-size body can always be admitted.
const MAX_INFLIGHT_INTAKE_BYTES: usize = 256 * 1024 * 1024;

/// A raw webhook delivery handed from the async server to a repo's worker.
///
/// These are exactly the columns [`Store::enqueue`] persists; the worker owns
/// the connection, so the server never touches the DB.
#[derive(Debug, Clone)]
pub struct IntakeDelivery {
    /// `X-GitHub-Delivery` — the unique intake idempotency key.
    pub delivery_id: String,
    /// `X-GitHub-Event` — the event type, dispatched by `parse_webhook`.
    pub event_type: String,
    /// Captured request headers as a JSON object (audit/replay).
    pub headers: String,
    /// The raw webhook payload bytes (parsed at drain time).
    pub body: Vec<u8>,
}

/// The outcome of a durable enqueue, reported back to the handler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueueOutcome {
    /// The delivery was newly durably queued.
    Enqueued,
    /// A delivery with this id was already present (GitHub redelivered).
    Duplicate,
}

/// A message on a worker's mailbox.
pub enum WorkerMsg {
    /// Durably enqueue a delivery, then ack the result so the handler can reply.
    Enqueue {
        /// The raw delivery.
        delivery: IntakeDelivery,
        /// Where to report the enqueue outcome.
        ack: oneshot::Sender<Result<EnqueueOutcome, StoreError>>,
        /// Backpressure reservation for `delivery.body`'s bytes (see
        /// [`WorkerRegistry::reserve_intake`]); released when this message is
        /// dropped, i.e. after the durable enqueue.
        permit: OwnedSemaphorePermit,
    },
    /// An executor thread finished a saga batch; feed the outcomes back.
    SagaOutcomes {
        /// The batch's train root.
        root: PrNumber,
        /// The observed effects' outcomes (empty for best-effort-only batches).
        outcomes: Vec<EffectOutcome>,
        /// The best-effort effects' outcomes: bookkeeping only (a terminal
        /// train's status-comment sync), never fed to the engine.
        best_effort: Vec<EffectOutcome>,
        /// Whether the outcomes feed `observe` → `advance`.
        feedback: bool,
    },
    /// Self-message from the stall-retry timer: a released delivery (GitHub
    /// was unavailable) is due for another attempt. Carries nothing — waking
    /// the loop clears the stall, and the next turn re-claims.
    RetryStalled,
}

/// Why routing a delivery to a worker failed (distinct from a successful
/// enqueue that returned [`EnqueueOutcome`]).
#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    /// Opening the repo's `Store` failed — most importantly
    /// [`StoreError::Locked`] (another process owns the repo), which the
    /// handler maps to 503.
    #[error("store open failed: {0}")]
    Open(StoreError),

    /// The worker thread is gone (mailbox closed or it dropped the ack without
    /// answering). A liveness failure, not a request error.
    #[error("worker unavailable")]
    Unavailable,
}

/// How workers reach GitHub, shared process-wide.
pub enum GitHubBackend {
    /// The production octocrab client (repo-scoped per worker) plus the
    /// server's runtime handle for `block_on` from worker/executor threads.
    Octocrab {
        /// The authenticated octocrab instance.
        client: octocrab::Octocrab,
        /// The async runtime that owns the client's connections.
        handle: tokio::runtime::Handle,
    },
    /// The real-git-backed fake, shared across workers and the test.
    #[cfg(test)]
    Fake(std::sync::Arc<std::sync::Mutex<crate::github::test_support::FakeGitHub>>),
}

/// Process-wide worker dependencies; per-repo [`WorkerDeps`] derive from it.
pub struct SharedDeps {
    /// How to reach GitHub.
    pub github: GitHubBackend,
    /// Base directory for repo clones/worktrees.
    pub repos_dir: PathBuf,
    /// Identity for the cascade's merge commits.
    pub commit_identity: CommitIdentity,
    /// Maximum age for stale worktree cleanup.
    pub worktree_max_age: std::time::Duration,
    /// Base URL for clone-on-first-use, e.g. `https://github.com`; the
    /// per-repo URL appends `/{owner}/{repo}.git`. Must carry NO credentials —
    /// clones authenticate via a credential helper reading `GITHUB_TOKEN`
    /// from the process environment (see [`executor::ensure_clone`]), keeping
    /// the token out of command lines and error output. `None` means clones
    /// must already exist.
    pub clone_url_base: Option<String>,
    /// The bot's GitHub user id (fetched at startup, resolved question 3).
    pub bot_user_id: u64,
    /// The bot's mention name, without `@`.
    pub bot_name: String,
    /// How long a worker waits before retrying a released delivery (GitHub
    /// was unavailable for a pipeline step). Bounds the recovery latency
    /// when no other webhook traffic arrives to wake the worker.
    pub stall_retry_delay: std::time::Duration,
    /// How often each worker re-evaluates its active trains as a fallback
    /// for missed webhooks (DESIGN §Polling fallback). Zero disables it.
    pub poll_interval: std::time::Duration,
}

impl SharedDeps {
    /// Derives one repo's worker dependencies.
    fn for_repo(&self, owner: &str, repo: &str) -> WorkerDeps {
        WorkerDeps {
            github: match &self.github {
                GitHubBackend::Octocrab { client, handle } => GitHubExec::Real {
                    client: OctocrabClient::new(client.clone(), RepoId::new(owner, repo)),
                    handle: handle.clone(),
                },
                #[cfg(test)]
                GitHubBackend::Fake(fake) => GitHubExec::Fake(fake.clone()),
            },
            git: GitSettings {
                base_dir: self.repos_dir.clone(),
                owner: owner.to_owned(),
                repo: repo.to_owned(),
                commit_identity: self.commit_identity.clone(),
                worktree_max_age: self.worktree_max_age,
                clone_url: self
                    .clone_url_base
                    .as_ref()
                    .map(|base| format!("{base}/{owner}/{repo}.git")),
            },
            bot_user_id: self.bot_user_id,
            bot_name: self.bot_name.clone(),
            stall_retry_delay: self.stall_retry_delay,
            poll_interval: self.poll_interval,
        }
    }
}

/// `(owner, repo)`.
type RepoKey = (String, String);
/// Per-repo creation guards (an async lock per repo); see [`WorkerRegistry`].
type CreationGuards = HashMap<RepoKey, Arc<Mutex<()>>>;

/// The set of live per-repo workers, keyed by `(owner, repo)`.
///
/// Workers are spawned lazily on the first delivery for a repo and live for the
/// process's lifetime. The registry hands out clones of each worker's mailbox
/// sender; it never holds a `Store`.
pub struct WorkerRegistry {
    state_dir: PathBuf,
    deps: Arc<SharedDeps>,
    /// Live per-repo mailbox senders. Guarded by a *sync* mutex held only for the
    /// brief get/insert — never across an `.await` — so a slow worker startup for
    /// one repo can't block routing to another (Codex review #53).
    workers: std::sync::Mutex<HashMap<RepoKey, mpsc::Sender<WorkerMsg>>>,
    /// Per-repo creation guards. Holding a repo's guard across its `Store::open`
    /// single-flights that repo's worker creation (so two concurrent
    /// first-deliveries can't race two opens into a spurious lock conflict)
    /// *without* serializing creation across different repos.
    creating: std::sync::Mutex<CreationGuards>,
    /// Process-wide in-flight intake-byte budget (see `MAX_INFLIGHT_INTAKE_BYTES`).
    intake_permits: Arc<Semaphore>,
}

impl WorkerRegistry {
    /// Creates a registry rooted at `state_dir`; each repo's DB lives at
    /// `<state_dir>/<owner>/<repo>/state.db`.
    pub fn new(state_dir: impl Into<PathBuf>, deps: SharedDeps) -> Self {
        WorkerRegistry {
            state_dir: state_dir.into(),
            deps: Arc::new(deps),
            workers: std::sync::Mutex::new(HashMap::new()),
            creating: std::sync::Mutex::new(HashMap::new()),
            intake_permits: Arc::new(Semaphore::new(MAX_INFLIGHT_INTAKE_BYTES)),
        }
    }

    /// The state directory (the `/state` endpoint still reads from it).
    pub fn state_dir(&self) -> &PathBuf {
        &self.state_dir
    }

    /// Reserves intake budget for a `body_len`-byte delivery, waiting if the
    /// process-wide in-flight budget ([`MAX_INFLIGHT_INTAKE_BYTES`]) is
    /// exhausted (backpressure). The returned permit must travel with the
    /// delivery and drop once it is durably enqueued.
    pub async fn reserve_intake(&self, body_len: usize) -> OwnedSemaphorePermit {
        // Clamp to [1, cap]: never request 0 (a no-op reservation) or more than
        // the budget (which the body limit guarantees we never do, but a clamp
        // keeps it deadlock-free regardless).
        let permits = body_len.clamp(1, MAX_INFLIGHT_INTAKE_BYTES) as u32;
        self.intake_permits
            .clone()
            .acquire_many_owned(permits)
            .await
            .expect("intake semaphore is never closed")
    }

    /// Spawns a worker for every repo that already has a state DB under
    /// `state_dir`, draining whatever a previous run left queued.
    ///
    /// Call once at startup. Without it, a delivery that was acked (200) but not
    /// yet processed before a crash would sit unprocessed — its `processing`/
    /// `pending` row is only requeued/drained when a worker opens the DB, and
    /// workers otherwise spawn lazily on the *next* webhook for that repo, which
    /// may never come (Codex review #53). The repo's own worker lock still
    /// guarantees one writer.
    pub async fn recover_existing(&self) {
        let Ok(owners) = std::fs::read_dir(&self.state_dir) else {
            return; // no state dir yet: nothing to recover
        };
        let mut recovered = 0u64;
        for owner in owners.flatten().filter(is_dir) {
            let Ok(owner_name) = owner.file_name().into_string() else {
                continue;
            };
            let Ok(repos) = std::fs::read_dir(owner.path()) else {
                continue;
            };
            for repo in repos.flatten().filter(is_dir) {
                if !repo.path().join("state.db").is_file() {
                    continue;
                }
                let Ok(repo_name) = repo.file_name().into_string() else {
                    continue;
                };
                match self.sender_for(&owner_name, &repo_name).await {
                    Ok(_) => recovered += 1,
                    Err(e) => {
                        warn!(owner = %owner_name, repo = %repo_name, error = %e,
                            "failed to recover worker at startup")
                    }
                }
            }
        }
        if recovered > 0 {
            info!(recovered, "spawned workers for repos with existing state");
        }
    }

    /// Returns the mailbox sender for `(owner, repo)`, spawning the worker on
    /// first use. Opening the `Store` (and acquiring the per-repo lock) happens
    /// on the new worker thread; a failure there surfaces as [`WorkerError::Open`].
    pub async fn sender_for(
        &self,
        owner: &str,
        repo: &str,
    ) -> Result<mpsc::Sender<WorkerMsg>, WorkerError> {
        let key = (owner.to_owned(), repo.to_owned());

        // Fast path: a live worker already owns this repo.
        if let Some(tx) = self.live_sender(&key) {
            return Ok(tx);
        }

        // Slow path: single-flight creation under *this repo's* guard (an async
        // lock, safe to hold across the `Store::open` await). Different repos use
        // different guards, so a slow open never blocks routing to another repo.
        let guard = self.creation_guard(&key);
        let _creating = guard.lock().await;

        // Re-check: another task may have finished creating it while we waited.
        if let Some(tx) = self.live_sender(&key) {
            return Ok(tx);
        }

        let tx = self.spawn_worker(&key).await?;
        self.workers.lock().unwrap().insert(key, tx.clone());
        Ok(tx)
    }

    /// Returns a live sender for `key`, evicting a stale (dead-worker) entry.
    fn live_sender(&self, key: &RepoKey) -> Option<mpsc::Sender<WorkerMsg>> {
        let mut workers = self.workers.lock().unwrap();
        match workers.get(key) {
            Some(tx) if !tx.is_closed() => Some(tx.clone()),
            // The worker died; drop the stale sender (its `Store` was dropped, so
            // the lock is free) and let the caller respawn.
            Some(_) => {
                workers.remove(key);
                None
            }
            None => None,
        }
    }

    /// The per-repo creation guard, created on first need.
    fn creation_guard(&self, key: &RepoKey) -> Arc<Mutex<()>> {
        self.creating
            .lock()
            .unwrap()
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Spawns a worker thread that opens the repo's `Store` (acquiring the lock)
    /// on its own thread and reports readiness; returns its mailbox sender.
    async fn spawn_worker(&self, key: &RepoKey) -> Result<mpsc::Sender<WorkerMsg>, WorkerError> {
        let db_path = self.state_dir.join(&key.0).join(&key.1).join("state.db");
        let (tx, rx) = mpsc::channel(MAILBOX_CAPACITY);
        let (ready_tx, ready_rx) = oneshot::channel();
        let deps = self.deps.for_repo(&key.0, &key.1);
        let saga_tx = tx.clone();

        std::thread::Builder::new()
            .name(format!("worker-{}-{}", key.0, key.1))
            .spawn(move || match Store::open(&db_path) {
                Ok(store) => {
                    // Tell the registrar we're live before we start draining.
                    if ready_tx.send(Ok(())).is_err() {
                        return; // registrar gave up; drop the Store (unlocks).
                    }
                    run(store, deps, rx, saga_tx);
                }
                Err(e) => {
                    let _ = ready_tx.send(Err(e));
                }
            })
            .map_err(|_| WorkerError::Unavailable)?;

        match ready_rx.await {
            Ok(Ok(())) => Ok(tx),
            Ok(Err(e)) => Err(WorkerError::Open(e)),
            // The thread vanished before reporting (e.g. it panicked in open).
            Err(_) => Err(WorkerError::Unavailable),
        }
    }
}

/// The worker thread body. Returns when every external sender has dropped
/// (process shutdown) or on a fatal `Store` error, releasing the `Store` (and
/// its lock).
///
/// Each turn: service **every** waiting mailbox message (intake acks stay
/// prompt; saga outcomes advance the engine), start the next queued saga if
/// the slot is free, then process at most **one** backlog delivery before
/// looking again. Effects never run on this thread — [`dispatch`] hands each
/// [`SagaBatch`] to a spawned executor thread that reports back through the
/// mailbox — so a webhook's 200 never waits on git.
///
/// A [`PipelineOutcome::Released`] delivery (GitHub unreachable) *stalls*
/// backlog processing until the next mailbox message, preserving in-order
/// processing without a hot claim/release loop.
fn run(
    store: Store,
    deps: WorkerDeps,
    mut rx: mpsc::Receiver<WorkerMsg>,
    tx: mpsc::Sender<WorkerMsg>,
) {
    let mut processor = match Processor::new(store, deps) {
        Ok(processor) => processor,
        Err(e) => return fatal(e),
    };
    let mut stalled = false;
    // Outcomes whose observation boundary is waiting out a stall: the
    // backlog drain before the boundary hit GitHub-unavailable, and
    // advancing would dispatch effects past an unprocessed acked delivery
    // (Codex M5 round 8). Fed back once the stall clears and the backlog
    // drains. At most one saga runs per repo, so one slot suffices.
    let mut parked: Option<(PrNumber, Vec<EffectOutcome>, bool)> = None;

    // Prune once at startup, then again at every idle boundary below, so the
    // intake bookkeeping stays bounded however long the process lives.
    if let Err(e) = prune_expired_intake(&mut processor) {
        return fatal(e);
    }

    // The recurring poll (the missed-webhook fallback) is a DEADLINE on the
    // idle wait below, not a timer thread: a sleeping thread per repo —
    // including every historical repo `recover_existing` wakes — would
    // double the process's thread count for nothing (Codex polling review,
    // P2). A per-repo stagger spreads the load so many repos restarting
    // together do not poll in lockstep (DESIGN §Distributed polling).
    let poll_interval = processor.poll_interval();
    let mut next_poll: Option<std::time::Instant> =
        (!poll_interval.is_zero()).then(|| std::time::Instant::now() + processor.poll_stagger());

    loop {
        // (1) Service waiting messages, capped at one mailbox's worth so
        // sustained intake cannot starve backlog processing (Codex review #53).
        let mut serviced = 0;
        for _ in 0..MAILBOX_CAPACITY {
            match rx.try_recv() {
                Ok(msg) => {
                    serviced += 1;
                    stalled = false;
                    match handle_msg(&mut processor, msg, &mut parked) {
                        Ok(Some(batch)) => {
                            if !dispatch(&processor, batch, tx.clone()) {
                                return fatal_spawn();
                            }
                        }
                        Ok(None) => {}
                        Err(e) => return fatal(e),
                    }
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => return,
            }
        }

        // (2) Start the next queued saga if the slot is free.
        if !processor.saga_in_flight() {
            match processor.pump() {
                Ok(Some(batch)) => {
                    if !dispatch(&processor, batch, tx.clone()) {
                        return fatal_spawn();
                    }
                }
                Ok(None) => {}
                Err(e) => return fatal(e),
            }
        }

        // (3) Process one unit of backlog, then loop back to re-check intake.
        let mut processed = false;
        if !stalled {
            match processor.claim() {
                Ok(Some(delivery)) => {
                    processed = true;
                    match processor.process_claimed(delivery) {
                        Ok(PipelineOutcome::Processed) => {}
                        Ok(PipelineOutcome::Released) => {
                            // The webhook is already acked, so nothing external
                            // retries this delivery: schedule our own wake-up
                            // (Codex M5 review) rather than waiting for
                            // unrelated traffic that may never come.
                            stalled = true;
                            schedule_stall_retry(processor.stall_retry_delay(), tx.clone());
                        }
                        Err(e) => return fatal(e),
                    }
                }
                Ok(None) => {
                    // Backlog drained with no stall: a parked observation
                    // boundary can now run against fully-applied state.
                    if let Some((root, outcomes, feedback)) = parked.take() {
                        processed = true;
                        match processor.on_outcomes(root, outcomes, feedback) {
                            Ok(Some(batch)) => {
                                if !dispatch(&processor, batch, tx.clone()) {
                                    return fatal_spawn();
                                }
                            }
                            Ok(None) => {}
                            Err(e) => return fatal(e),
                        }
                    }
                }
                Err(e) => return fatal(e),
            }
        }

        // (3a) A poll that has come DUE takes its place in the stream here,
        // whatever else the turn did: nested under the idle branch below it
        // would never run on a repo with sustained traffic, and the missed
        // webhook it exists to cover would strand a parked train forever
        // (Codex polling review round 5, P2).
        if let Some(deadline) = next_poll
            && std::time::Instant::now() >= deadline
        {
            processor.poll_active_trains();
            next_poll = Some(std::time::Instant::now() + poll_interval);
            // Round again rather than falling into the idle wait: the owed
            // evaluations sit behind the backlog-drain gate, which only a
            // fresh `claim` opens, and blocking now would hold them for
            // another full interval (Codex polling review round 6, P2).
            continue;
        }

        // (3b) Supplementary recovery found GitHub unavailable this turn:
        // arm the stall-retry timer, whose message re-queues the parked
        // recovery — nothing else wakes a traffic-less repo.
        if processor.take_retry_request() {
            schedule_stall_retry(processor.stall_retry_delay(), tx.clone());
        }

        // (4) Nothing to do: prune expired intake bookkeeping, then block
        // until the next message (or shutdown). Never block while queued
        // engine work could pump into a free saga slot — an empty `claim`
        // may have just queued the owed startup evaluations, and nothing
        // else would ever run them (Codex M5 round 7, P1).
        if serviced == 0
            && !processed
            && (processor.saga_in_flight() || !processor.has_queued_work())
        {
            if let Err(e) = prune_expired_intake(&mut processor) {
                return fatal(e);
            }
            // Wait for the next message, or until the poll deadline. The
            // timed wait needs the server's runtime; without one (tests,
            // which drive polls directly) the wait is untimed.
            let received = match (next_poll, processor.github().runtime()) {
                (Some(deadline), Some(handle)) => {
                    let wait = deadline.saturating_duration_since(std::time::Instant::now());
                    match timed_recv(handle, &mut rx, wait) {
                        Ok(received) => received,
                        Err(_elapsed) => {
                            // Owe a re-evaluation of every active train
                            // (gated behind the backlog drain).
                            processor.poll_active_trains();
                            next_poll = Some(std::time::Instant::now() + poll_interval);
                            continue;
                        }
                    }
                }
                _ => rx.blocking_recv(),
            };
            match received {
                Some(msg) => {
                    stalled = false;
                    match handle_msg(&mut processor, msg, &mut parked) {
                        Ok(Some(batch)) => {
                            if !dispatch(&processor, batch, tx.clone()) {
                                return fatal_spawn();
                            }
                        }
                        Ok(None) => {}
                        Err(e) => return fatal(e),
                    }
                }
                None => return,
            }
        }
    }
}

/// Retention for intake bookkeeping: `done` delivery rows (the delivery-id
/// idempotency guard) and dedupe keys (the logical-event guard) expire
/// together after this window, so both duplicate defences agree on how far
/// back they reach. GitHub's redeliveries (automatic or one-click manual)
/// happen well within a week; a manual redelivery of something older is
/// treated as a fresh event.
const INTAKE_RETENTION_DAYS: i64 = 7;

/// The event-log length above which idle maintenance compacts the log into
/// a checkpoint. Well above any single train's event count (a train writes
/// a few dozen), so compaction typically summarizes many completed trains
/// at once; well below where reading the whole log per evaluation hurts.
const EVENTS_COMPACTION_THRESHOLD: u64 = 1024;

/// Idle-boundary maintenance: prunes `done` deliveries and dedupe keys
/// older than [`INTAKE_RETENTION_DAYS`], and compacts the event log into a
/// checkpoint once it exceeds [`EVENTS_COMPACTION_THRESHOLD`] (a no-op
/// while any train is active — `Store::compact` refuses, preserving the
/// intent-ledger history recovery reads). Called at startup and at every
/// idle boundary; everything here is bounded, so it is cheap to run often.
fn prune_expired_intake(processor: &mut Processor) -> Result<(), StoreError> {
    let cutoff = Utc::now() - chrono::Duration::days(INTAKE_RETENTION_DAYS);
    let store = processor.store_mut();
    let keys = store.prune_dedupe(cutoff)?;
    let deliveries = store.prune_deliveries(cutoff)?;
    if keys > 0 || deliveries > 0 {
        info!(keys, deliveries, "pruned expired intake bookkeeping");
    }
    if let Some(events) = store.compact(EVENTS_COMPACTION_THRESHOLD, Utc::now())? {
        info!(events, "compacted the event log into a checkpoint");
    }
    Ok(())
}

/// Executor-thread spawn failure: the saga slot is marked in-flight and no
/// outcome will ever arrive, so the worker stops (dropping the Store) and
/// the next delivery respawns it with `processing`→`pending` recovery —
/// wedging forever is the one unacceptable outcome (Codex M5 round 6).
fn fatal_spawn() {
    error!("failed to spawn a saga executor thread; stopping worker (it will respawn and recover)");
}

/// A Store error means we can no longer characterize this repo's state. Stop
/// the worker: dropping the `Store` releases the lock, and the next delivery
/// respawns a worker whose `Store::open` re-runs `processing`→`pending`
/// recovery, requeueing whatever was claimed (Codex review #53). Failing fast
/// beats holding a wedged Store open for the process lifetime — which would
/// strand the claimed delivery — and beats hot-looping a retry against a
/// persistently broken store.
fn fatal(e: StoreError) {
    error!(error = %e, "fatal store error; stopping worker (it will respawn and recover)");
}

/// Handles one mailbox message.
fn handle_msg(
    processor: &mut Processor,
    msg: WorkerMsg,
    parked: &mut Option<(PrNumber, Vec<EffectOutcome>, bool)>,
) -> Result<Option<SagaBatch>, StoreError> {
    match msg {
        // `_permit` is held until this arm returns — i.e. until after the
        // enqueue and the `delivery` body have been consumed — then dropped,
        // releasing the reserved intake bytes.
        WorkerMsg::Enqueue {
            delivery,
            ack,
            permit: _permit,
        } => {
            let outcome = processor
                .store_mut()
                .enqueue(
                    &delivery.delivery_id,
                    &delivery.event_type,
                    &delivery.headers,
                    &delivery.body,
                    Utc::now(),
                )
                .map(|inserted| {
                    if inserted {
                        EnqueueOutcome::Enqueued
                    } else {
                        EnqueueOutcome::Duplicate
                    }
                });
            // Store failures are fatal to the worker (see `run`), but the
            // handler learns the outcome first (it maps the error to a 5xx so
            // GitHub redelivers). The original error moves into the ack, so
            // the fatal path carries its rendering.
            let error_text = outcome.as_ref().err().map(ToString::to_string);
            let _ = ack.send(outcome);
            match error_text {
                Some(text) => Err(StoreError::Io(std::io::Error::other(format!(
                    "durable enqueue failed: {text}"
                )))),
                None => Ok(None),
            }
        }
        // Outcomes always PARK; the observation boundary runs only once the
        // acked backlog has been applied (a waiting stop or topology change
        // is still a raw delivery row here — rounds 7/8). The main loop's
        // one-delivery-per-turn cadence does the draining, so intake acks
        // stay prompt however deep the backlog is (round 10) — the loop's
        // step (3) resumes the boundary when its claim finds the backlog
        // empty. The saga slot stays occupied meanwhile: exactly the
        // in-order pause the release/stall mechanism promises.
        WorkerMsg::SagaOutcomes {
            root,
            outcomes,
            best_effort,
            feedback,
        } => {
            debug_assert!(parked.is_none(), "one saga, one parked slot");
            // Bookkeeping, not an observation: it needs no boundary.
            processor.note_best_effort(&best_effort)?;
            *parked = Some((root, outcomes, feedback));
            Ok(None)
        }
        // Receiving any message clears the stall in `run`. The timer also
        // retries any recovery that parked on GitHub unavailability: the
        // re-queued evaluations pump on the next turn.
        WorkerMsg::RetryStalled => {
            processor.requeue_marked_recoveries()?;
            Ok(None)
        }
    }
}

/// Waits for the next mailbox message for at most `wait`, from a plain OS
/// thread, via the runtime that owns the timer. The timeout future must be
/// CONSTRUCTED inside the runtime — `tokio::time::timeout` registers its
/// timer with the current runtime at construction, and built outside
/// `block_on` it panics with "there is no reactor running" on the worker's
/// first idle wait (Codex polling review round 3, P1).
pub(crate) fn timed_recv<T>(
    handle: &tokio::runtime::Handle,
    rx: &mut mpsc::Receiver<T>,
    wait: std::time::Duration,
) -> Result<Option<T>, tokio::time::error::Elapsed> {
    handle.block_on(async { tokio::time::timeout(wait, rx.recv()).await })
}

/// Arms a one-shot timer that wakes the worker to retry a released delivery.
/// One timer per release: a retry that releases again arms the next one, so
/// the retry cadence is bounded by `delay`.
fn schedule_stall_retry(delay: std::time::Duration, tx: mpsc::Sender<WorkerMsg>) {
    let spawned = std::thread::Builder::new()
        .name("stall-retry-timer".to_owned())
        .spawn(move || {
            std::thread::sleep(delay);
            let _ = tx.blocking_send(WorkerMsg::RetryStalled);
        });
    if spawned.is_err() {
        // The stall then lasts until the next unrelated message; loud but
        // not fatal.
        error!("failed to spawn the stall-retry timer thread");
    }
}

/// Hands a batch to a fresh executor thread. The thread ensures the clone
/// exists (first git use), builds the per-saga interpreter, executes, and
/// reports back through the worker's own mailbox.
///
/// Returns `false` when the executor thread could not be spawned: the saga
/// slot is already marked in-flight and no outcome will ever arrive, so the
/// worker must die (drop the Store → respawn recovers) rather than sit
/// wedged forever (Codex M5 round 6).
#[must_use]
fn dispatch(processor: &Processor, batch: SagaBatch, tx: mpsc::Sender<WorkerMsg>) -> bool {
    let github = processor.github().clone();
    let config = processor.git_config();
    let clone_url = processor.git_settings().clone_url.clone();

    let spawned = std::thread::Builder::new()
        .name(format!("saga-{}-{}", config.owner, config.repo))
        .spawn(move || {
            let needs_git = batch
                .effects
                .iter()
                .chain(batch.best_effort.iter())
                .any(|e| matches!(e, Effect::Git(_)));
            if needs_git && let Err(e) = executor::ensure_clone(&config, clone_url.as_deref()) {
                warn!(error = %e, "repo clone unavailable; failing the batch as transient");
                // Best-effort effects fail independently: the API-only ones
                // (comments, status updates) do not need the clone, so a
                // clone failure must not suppress them (Codex M5 round 10) —
                // and their outcomes are reported like any other batch's,
                // or a terminal status update failing in the same outage
                // would go unnoticed (Codex terminal-sync review, P1).
                let mut best_effort = Vec::new();
                for effect in &batch.best_effort {
                    let Effect::GitHub(api) = effect else {
                        continue;
                    };
                    let result = github
                        .execute(api.clone())
                        .map(crate::cascade::EffectResponse::GitHub);
                    if let Err(e) = &result {
                        warn!(?effect, error = ?e, "best-effort effect failed (ignored)");
                    }
                    best_effort.push(EffectOutcome {
                        effect: effect.clone(),
                        result,
                    });
                }
                // Fail the first observed effect so the engine parks and
                // re-derives; a best-effort-only batch just reports empty.
                let outcomes = batch
                    .effects
                    .first()
                    .map(|first| {
                        vec![EffectOutcome {
                            effect: first.clone(),
                            result: Err(EffectError::Transient {
                                detail: format!("repo clone unavailable: {e}"),
                            }),
                        }]
                    })
                    .unwrap_or_default();
                let _ = tx.blocking_send(WorkerMsg::SagaOutcomes {
                    root: batch.root,
                    outcomes,
                    best_effort,
                    feedback: batch.feedback,
                });
                return;
            }
            let interpreter = WorktreeGitInterpreter::new(config, batch.root);
            let result = execute_batch(&interpreter, &github, &batch);
            let _ = tx.blocking_send(WorkerMsg::SagaOutcomes {
                root: batch.root,
                outcomes: result.observed,
                best_effort: result.best_effort,
                feedback: batch.feedback,
            });
        });
    spawned.is_ok()
}

/// Whether a directory entry is itself a directory (used to walk
/// `state_dir/<owner>/<repo>`).
fn is_dir(entry: &std::fs::DirEntry) -> bool {
    entry.file_type().map(|t| t.is_dir()).unwrap_or(false)
}

#[cfg(test)]
pub(crate) mod test_support {
    //! Fake-backed dependency constructors for worker/server tests.

    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::{Arc, Mutex};

    use crate::git::GitConfig;
    use crate::git::test_support::test_identity;
    use crate::github::test_support::{FakeGitHub, FakePr};
    use crate::types::PrNumber;

    use super::{GitHubBackend, SharedDeps};

    /// The bot's user id in fake-backed tests.
    pub(crate) const TEST_BOT_ID: u64 = 424_242;

    /// A `GitConfig` rooted under `dir` for the canonical test repo `o/r`.
    pub(crate) fn test_git_config(dir: &Path) -> GitConfig {
        GitConfig {
            base_dir: dir.join("repos"),
            owner: "o".to_owned(),
            repo: "r".to_owned(),
            default_branch: "main".to_owned(),
            worktree_max_age: std::time::Duration::from_secs(24 * 60 * 60),
            commit_identity: test_identity(),
        }
    }

    /// `SharedDeps` backed by a [`FakeGitHub`] (returned for seeding and
    /// inspection). The fake serves *all* repos the registry spawns.
    pub(crate) fn fake_shared_deps(
        dir: &Path,
        prs: HashMap<PrNumber, FakePr>,
    ) -> (SharedDeps, Arc<Mutex<FakeGitHub>>) {
        let config = test_git_config(dir);
        let mut fake = FakeGitHub::new(config, prs);
        fake.comment_author = TEST_BOT_ID;
        let fake = Arc::new(Mutex::new(fake));
        let deps = SharedDeps {
            github: GitHubBackend::Fake(fake.clone()),
            repos_dir: dir.join("repos"),
            commit_identity: test_identity(),
            worktree_max_age: std::time::Duration::from_secs(24 * 60 * 60),
            clone_url_base: None,
            bot_user_id: TEST_BOT_ID,
            bot_name: "merge-train".to_owned(),
            stall_retry_delay: std::time::Duration::from_millis(25),
            // Tests drive polls directly (`Processor::poll_active_trains`);
            // the background timer stays off for determinism.
            poll_interval: std::time::Duration::ZERO,
        };
        (deps, fake)
    }
}
