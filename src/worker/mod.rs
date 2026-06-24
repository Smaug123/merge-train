//! The per-repo worker: the single writer that owns a repo's [`Store`].
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
//! # The loop body is a stub
//!
//! Processing currently parses the webhook and closes the delivery without
//! emitting state events: the cascade engine that turns a [`GitHubEvent`] into
//! `StateEventPayload`s does not exist yet. [`process_one`] marks the seam where
//! it drops in (see `CASCADE_ENGINE_PLAN.md` M5). Until then the worker is the
//! durable, deduplicated *intake substrate* and nothing more.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::Utc;
use thiserror::Error;
use tokio::sync::{Mutex, mpsc, oneshot};
use tracing::{error, info, warn};

use crate::store::{Delivery, Store, StoreError};
use crate::webhooks::parse_webhook;

/// Mailbox depth per repo. Backpressure: when a worker is busy and its mailbox
/// is full, the axum handler's `send().await` waits rather than unbounded-buffering.
const MAILBOX_CAPACITY: usize = 1024;

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
        delivery: IntakeDelivery,
        ack: oneshot::Sender<Result<EnqueueOutcome, StoreError>>,
    },
}

/// Why routing a delivery to a worker failed (distinct from a successful
/// enqueue that returned [`EnqueueOutcome`]).
#[derive(Debug, Error)]
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
    /// Live per-repo mailbox senders. Guarded by a *sync* mutex held only for the
    /// brief get/insert — never across an `.await` — so a slow worker startup for
    /// one repo can't block routing to another (Codex review #53).
    workers: std::sync::Mutex<HashMap<RepoKey, mpsc::Sender<WorkerMsg>>>,
    /// Per-repo creation guards. Holding a repo's guard across its `Store::open`
    /// single-flights that repo's worker creation (so two concurrent
    /// first-deliveries can't race two opens into a spurious lock conflict)
    /// *without* serializing creation across different repos.
    creating: std::sync::Mutex<CreationGuards>,
}

impl WorkerRegistry {
    /// Creates a registry rooted at `state_dir`; each repo's DB lives at
    /// `<state_dir>/<owner>/<repo>/state.db`.
    pub fn new(state_dir: impl Into<PathBuf>) -> Self {
        WorkerRegistry {
            state_dir: state_dir.into(),
            workers: std::sync::Mutex::new(HashMap::new()),
            creating: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// The state directory (the `/state` endpoint still reads from it).
    pub fn state_dir(&self) -> &PathBuf {
        &self.state_dir
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

        std::thread::Builder::new()
            .name(format!("worker-{}-{}", key.0, key.1))
            .spawn(move || match Store::open(&db_path) {
                Ok(store) => {
                    // Tell the registrar we're live before we start draining.
                    if ready_tx.send(Ok(())).is_err() {
                        return; // registrar gave up; drop the Store (unlocks).
                    }
                    run(store, rx);
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

/// The worker thread body. Returns when every sender has dropped (process
/// shutdown), releasing the `Store` (and its lock).
///
/// Intake is prioritized over processing: each iteration first durably enqueues
/// and acks **every** delivery already waiting in the mailbox, then processes at
/// most **one** backlog delivery before looking again. So a webhook's 200 waits
/// at most one `process_one`, never the whole backlog — without this, a delivery
/// arriving during a long drain could exceed GitHub's redelivery timeout even
/// though its enqueue was trivial. (With the M5 engine a single saga may still
/// be slow; running effects off-thread so intake never waits is M5's concern.)
fn run(mut store: Store, mut rx: mpsc::Receiver<WorkerMsg>) {
    loop {
        // Service all waiting intake first. The mailbox is bounded, so this
        // burst is bounded too.
        loop {
            match rx.try_recv() {
                Ok(msg) => handle_msg(&mut store, msg),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => return,
            }
        }
        // Process one unit of backlog, then loop back to re-check intake.
        match process_next(&mut store) {
            // Did work: re-check intake, then process the next.
            Ok(true) => {}
            // Nothing to process: block until the next message (or shutdown).
            Ok(false) => match rx.blocking_recv() {
                Some(msg) => handle_msg(&mut store, msg),
                None => return,
            },
            // A Store error means we can no longer characterize this repo's
            // state. Stop the worker: dropping the `Store` releases the lock, and
            // the next delivery respawns a worker whose `Store::open` re-runs the
            // `processing`->`pending` recovery, requeueing whatever was claimed
            // (Codex review #53). Failing fast beats holding a wedged Store open
            // for the process lifetime — which would strand the claimed delivery,
            // since open-recovery never re-runs while this worker lives — and
            // beats hot-looping a retry against a persistently broken store.
            Err(e) => {
                error!(error = %e, "fatal store error; stopping worker (it will respawn and recover)");
                return;
            }
        }
    }
}

/// Durably enqueues a delivery and acks the outcome to the handler.
fn handle_msg(store: &mut Store, msg: WorkerMsg) {
    match msg {
        WorkerMsg::Enqueue { delivery, ack } => {
            let outcome = store
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
            // The handler may have timed out and dropped the receiver; the
            // delivery is durable regardless, so ignore a send failure.
            let _ = ack.send(outcome);
        }
    }
}

/// Claims and processes one pending delivery (oldest first). Returns `Ok(true)`
/// if it did work, `Ok(false)` if the queue was empty, or `Err` if the Store
/// failed (fatal — see [`run`]).
fn process_next(store: &mut Store) -> Result<bool, StoreError> {
    match store.claim_next_delivery()? {
        Some(delivery) => {
            process_one(store, delivery)?;
            Ok(true)
        }
        None => Ok(false),
    }
}

/// Processes one claimed delivery and closes it.
///
/// ENGINE SEAM (M5): a parsed event currently produces **no** state events and
/// **no** dedupe mark — the cascade engine that maps a [`crate::webhooks::GitHubEvent`]
/// to `StateEventPayload`s does not exist yet. When it lands, the `Ok(Some)` arm
/// becomes: compute the [`crate::spool::DedupeKey`]; skip if `store.is_duplicate`;
/// else `handle_event` → `commit_delivery(id, events, Some(key))`. The other two
/// arms (ignored type / malformed) stay as effect-free closes.
fn process_one(store: &mut Store, delivery: Delivery) -> Result<(), StoreError> {
    let id = delivery.delivery_id.clone();
    match parse_webhook(&delivery.event_type, &delivery.body) {
        // ENGINE SEAM: no events, no dedupe key yet — just close.
        Ok(Some(_event)) => close(store, &id, "parsed"),
        Ok(None) => close(store, &id, "ignored event type"),
        Err(e) => {
            warn!(delivery_id = %id, error = %e, "malformed webhook; closing");
            close(store, &id, "malformed")
        }
    }
}

/// Closes a delivery with no state events (the effect-free path). A failure here
/// is fatal to the worker (see [`run`]): on respawn, `Store::open` requeues the
/// still-`processing` row.
fn close(store: &mut Store, delivery_id: &str, reason: &str) -> Result<(), StoreError> {
    store.commit_delivery(delivery_id, &[], None, Utc::now())?;
    info!(delivery_id, reason, "delivery closed");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn open_store(dir: &std::path::Path) -> Store {
        Store::open(&dir.join("state.db")).unwrap()
    }

    // A minimal valid `pull_request` payload that `parse_webhook` accepts.
    fn pull_request_body() -> Vec<u8> {
        br#"{
            "action": "synchronize",
            "number": 7,
            "pull_request": {
                "number": 7,
                "state": "open",
                "draft": false,
                "merged": false,
                "head": { "sha": "deadbeef", "ref": "feature" },
                "base": { "sha": "cafef00d", "ref": "main" }
            }
        }"#
        .to_vec()
    }

    #[test]
    fn drain_closes_parsed_ignored_and_malformed_deliveries() {
        let dir = tempdir().unwrap();
        let mut store = open_store(dir.path());
        let ts = Utc::now();

        // A parseable event, an unknown (ignored) type, and a malformed body.
        store
            .enqueue("d-parsed", "pull_request", "{}", &pull_request_body(), ts)
            .unwrap();
        store
            .enqueue("d-ignored", "membership", "{}", b"{}", ts)
            .unwrap();
        store
            .enqueue("d-bad", "pull_request", "{}", b"not json", ts)
            .unwrap();

        while process_next(&mut store).unwrap() {}

        // All three are closed: nothing remains claimable.
        assert!(store.claim_next_delivery().unwrap().is_none());
    }

    #[test]
    fn process_next_is_false_on_empty_queue() {
        let dir = tempdir().unwrap();
        let mut store = open_store(dir.path());
        assert!(!process_next(&mut store).unwrap());
        assert!(store.claim_next_delivery().unwrap().is_none());
    }

    #[test]
    fn delivery_left_processing_is_recovered_on_reopen() {
        // The recovery the fatal-error path relies on (Codex review #53): a
        // delivery claimed but never closed (a worker that stopped mid-process)
        // is requeued by the next `Store::open` and then drained.
        let dir = tempdir().unwrap();
        let path = dir.path().join("state.db");
        {
            let mut store = Store::open(&path).unwrap();
            store
                .enqueue("d1", "pull_request", "{}", &pull_request_body(), Utc::now())
                .unwrap();
            // Claim (-> processing), then drop without closing (the worker died).
            assert!(store.claim_next_delivery().unwrap().is_some());
        }

        // A fresh worker: open requeues the stranded row, the loop drains it.
        let mut store = Store::open(&path).unwrap();
        while process_next(&mut store).unwrap() {}
        assert!(
            store.claim_next_delivery().unwrap().is_none(),
            "the stranded delivery was recovered and closed"
        );
    }

    #[test]
    fn enqueued_body_is_stored_verbatim() {
        // The exact signed bytes must survive for the worker's `parse_webhook`:
        // no serde round-trip that would collapse duplicate keys or whitespace.
        let dir = tempdir().unwrap();
        let mut store = open_store(dir.path());
        let raw: &[u8] = br#"{ "action": "opened",  "action": "closed",
            "repository": { "name": "r", "owner": { "login": "o" } } }"#;

        store
            .enqueue("d1", "pull_request", "{}", raw, Utc::now())
            .unwrap();
        let claimed = store.claim_next_delivery().unwrap().unwrap();
        assert_eq!(claimed.body, raw, "stored body must be byte-identical");
    }

    #[tokio::test]
    async fn registry_routes_enqueue_and_processes() {
        let dir = tempdir().unwrap();
        let registry = WorkerRegistry::new(dir.path());

        let sender = registry.sender_for("octocat", "hello").await.unwrap();
        let (ack_tx, ack_rx) = oneshot::channel();
        sender
            .send(WorkerMsg::Enqueue {
                delivery: IntakeDelivery {
                    delivery_id: "d1".into(),
                    event_type: "pull_request".into(),
                    headers: "{}".into(),
                    body: pull_request_body(),
                },
                ack: ack_tx,
            })
            .await
            .unwrap();

        assert_eq!(ack_rx.await.unwrap().unwrap(), EnqueueOutcome::Enqueued);

        // Redelivery of the same id is reported as a duplicate.
        let (ack_tx, ack_rx) = oneshot::channel();
        sender
            .send(WorkerMsg::Enqueue {
                delivery: IntakeDelivery {
                    delivery_id: "d1".into(),
                    event_type: "pull_request".into(),
                    headers: "{}".into(),
                    body: pull_request_body(),
                },
                ack: ack_tx,
            })
            .await
            .unwrap();
        assert_eq!(ack_rx.await.unwrap().unwrap(), EnqueueOutcome::Duplicate);
    }

    #[tokio::test]
    async fn services_new_intake_with_backlog_present() {
        let dir = tempdir().unwrap();
        let db_dir = dir.path().join("o").join("r");
        std::fs::create_dir_all(&db_dir).unwrap();
        // Pre-seed a backlog of pending deliveries, then release the lock so the
        // worker can open the DB and find them waiting.
        {
            let mut store = Store::open(&db_dir.join("state.db")).unwrap();
            for i in 0..50 {
                store
                    .enqueue(
                        &format!("backlog-{i}"),
                        "pull_request",
                        "{}",
                        &pull_request_body(),
                        Utc::now(),
                    )
                    .unwrap();
            }
        }

        let registry = WorkerRegistry::new(dir.path());
        let sender = registry.sender_for("o", "r").await.unwrap();

        // A fresh delivery is still durably enqueued and acked despite the
        // backlog: intake is serviced ahead of backlog draining, so this ack
        // does not wait for all 50 to process.
        let (ack_tx, ack_rx) = oneshot::channel();
        sender
            .send(WorkerMsg::Enqueue {
                delivery: IntakeDelivery {
                    delivery_id: "fresh".into(),
                    event_type: "pull_request".into(),
                    headers: "{}".into(),
                    body: pull_request_body(),
                },
                ack: ack_tx,
            })
            .await
            .unwrap();
        assert_eq!(ack_rx.await.unwrap().unwrap(), EnqueueOutcome::Enqueued);
    }

    #[tokio::test]
    async fn registry_returns_same_sender_for_same_repo() {
        let dir = tempdir().unwrap();
        let registry = WorkerRegistry::new(dir.path());
        let a = registry.sender_for("o", "r").await.unwrap();
        let b = registry.sender_for("o", "r").await.unwrap();
        assert!(a.same_channel(&b), "one worker (one Store) per repo");
    }

    #[tokio::test]
    async fn open_failure_surfaces_as_open_error() {
        // Hold the repo lock with a Store, then a worker open must fail Locked.
        let dir = tempdir().unwrap();
        let db_dir = dir.path().join("o").join("r");
        std::fs::create_dir_all(&db_dir).unwrap();
        let _held = Store::open(&db_dir.join("state.db")).unwrap();

        let registry = WorkerRegistry::new(dir.path());
        let err = registry.sender_for("o", "r").await.unwrap_err();
        assert!(
            matches!(err, WorkerError::Open(StoreError::Locked(_))),
            "expected Open(Locked), got {err:?}"
        );
    }
}
