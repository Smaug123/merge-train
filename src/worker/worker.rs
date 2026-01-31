//! Per-repo event loop for processing webhook events.
//!
//! Each repository gets a dedicated worker that processes events serially.
//! Workers drain the spool, apply handlers, persist state, and drive cascades.
//!
//! # Event Processing Flow
//!
//! From DESIGN.md:
//! 1. Drain pending deliveries from spool (with cleanup of interrupted processing)
//! 2. Pull highest-priority event from queue
//! 3. Create `.proc` marker (claiming delivery for processing)
//! 4. Apply incremental state update
//! 5. Append event to log
//! 6. Create `.done` marker after state is durably persisted
//! 7. Loop
//!
//! # Critical Invariant
//!
//! A delivery's `.done` marker is only created after all state effects from
//! that delivery are durably persisted (fsynced). This ensures correct replay
//! on restart.
//!
//! # Async Event Loop
//!
//! The worker runs as a tokio task with an async event loop that handles:
//! - Incoming webhook deliveries via message channel
//! - Timer-based re-evaluation for non-blocking waits
//! - Periodic polling for active trains (fallback for missed webhooks)
//! - Graceful shutdown via cancellation token
//!
//! # Stop Cancellation Semantics
//!
//! Stop commands (`@merge-train stop`) cancel cascade operations via token
//! cancellation. There are two paths:
//!
//! ## Normal Stop Flow (via webhook)
//!
//! 1. Stop command arrives as webhook, gets spooled and dispatched
//! 2. Handler validates the command and produces `TrainStopped` event
//! 3. In `apply_handler_result`, when `TrainStopped` is processed, the token
//!    is cancelled synchronously
//! 4. Effects are executed AFTER step 3, so they see the cancelled token
//!    and skip execution
//!
//! This ensures authorization before cancellation - unauthorized commenters
//! cannot interrupt operations.
//!
//! ## External Cancellation (via cancel channel)
//!
//! A separate high-priority channel (`cancel_rx`) exists for out-of-band
//! cancellation. When a request arrives on this channel:
//! 1. The worker immediately cancels the stack's `CancellationToken`
//! 2. No validation is performed (caller is trusted)
//!
//! This channel is currently unused in the normal webhook flow but is
//! available for:
//! - Admin-level emergency cancellation
//! - Testing scenarios
//! - Future features requiring immediate cancellation
//!
//! ## Cancellation Behavior
//!
//! - Effects check `is_cancelled()` before each effect, not mid-effect
//! - An effect that's executing will complete before the next check
//! - Each stack has its own token, so stopping one train doesn't affect others
//!
//! **Known limitation**: When effects are executing, the worker cannot poll
//! messages until the effect batch completes. Individual effects (like git push)
//! run to completion once started.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use thiserror::Error;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, trace, warn};

use crate::effects::{Effect, GitHubResponse, PrData};
use crate::github::OctocrabClient;
use crate::persistence::event::StateEvent;
use crate::persistence::log::{EventLog, EventLogError};
use crate::persistence::snapshot::{
    PersistedRepoSnapshot, SnapshotError, load_snapshot, save_snapshot_atomic,
};
use crate::spool::delivery::{SpoolError, SpooledDelivery, mark_done, mark_processing};
use crate::spool::drain::{cleanup_interrupted_processing, drain_pending};
use crate::types::{DeliveryId, PrNumber, PrState, RepoId};
use crate::webhooks::events::GitHubEvent;
use crate::webhooks::handlers::{HandlerError, HandlerResult, handle_event};
use crate::webhooks::parser::{ParseError, parse_webhook};
use crate::webhooks::priority::{EventPriority, classify_priority_with_bot_name};

use super::effects::EffectError;

use super::message::WorkerMessage;
use super::poll::PollConfig;
use super::queue::{EventQueue, WaitCondition};

/// Default bot name for command parsing.
const DEFAULT_BOT_NAME: &str = "merge-train";

/// Errors that can occur during worker operations.
#[derive(Debug, Error)]
pub enum WorkerError {
    /// Spool operation failed.
    #[error("spool error: {0}")]
    Spool(#[from] SpoolError),

    /// Event log operation failed.
    #[error("event log error: {0}")]
    EventLog(#[from] EventLogError),

    /// Snapshot operation failed.
    #[error("snapshot error: {0}")]
    Snapshot(#[from] SnapshotError),

    /// Event handler failed.
    #[error("handler error: {0}")]
    Handler(#[from] HandlerError),

    /// Webhook parsing failed.
    #[error("parse error: {0}")]
    Parse(#[from] ParseError),

    /// Effect execution failed.
    #[error("effect error: {0}")]
    Effect(#[from] EffectError),

    /// IO error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

impl WorkerError {
    /// Returns true if this is a persistence failure that requires fail-stop.
    ///
    /// Persistence failures (event log, snapshot) are fatal because continuing
    /// without durable state could lead to incorrect behavior on restart.
    /// The worker should stop and leave `.proc` markers in place so the
    /// delivery is re-processed after restart.
    ///
    /// Non-persistence failures (handler logic, parsing, effects) are
    /// recoverable - the delivery is marked done and processing continues.
    pub fn is_persistence_failure(&self) -> bool {
        matches!(
            self,
            WorkerError::EventLog(_) | WorkerError::Snapshot(_) | WorkerError::Io(_)
        )
    }
}

/// Result type for worker operations.
pub type Result<T> = std::result::Result<T, WorkerError>;

/// Result of processing a single event.
///
/// Contains information about what was processed and what effects need to be executed.
#[derive(Debug)]
pub struct ProcessResult {
    /// Whether an event was processed.
    pub processed: bool,
    /// Effects that need to be executed (GitHub API calls, git operations).
    pub effects: Vec<Effect>,
    /// The delivery ID that was processed, if any.
    pub delivery_id: Option<DeliveryId>,
}

/// Configuration for a worker.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// The repository this worker handles.
    pub repo: RepoId,

    /// Directory where webhook deliveries are spooled for this repo.
    pub spool_dir: PathBuf,

    /// Directory where state is persisted for this repo.
    pub state_dir: PathBuf,

    /// Bot name for command parsing (e.g., "merge-train").
    pub bot_name: String,
}

impl WorkerConfig {
    /// Creates a new worker configuration.
    pub fn new(repo: RepoId, spool_dir: impl Into<PathBuf>, state_dir: impl Into<PathBuf>) -> Self {
        WorkerConfig {
            repo,
            spool_dir: spool_dir.into(),
            state_dir: state_dir.into(),
            bot_name: DEFAULT_BOT_NAME.to_string(),
        }
    }

    /// Sets a custom bot name for command parsing.
    pub fn with_bot_name(mut self, bot_name: impl Into<String>) -> Self {
        self.bot_name = bot_name.into();
        self
    }
}

/// A pending timer for non-blocking waits.
#[derive(Debug)]
pub(crate) struct PendingTimer {
    /// When the timer fires.
    pub(crate) fires_at: Instant,
    /// The train this timer is for.
    pub(crate) train_root: PrNumber,
    /// The condition being waited for.
    pub(crate) condition: WaitCondition,
}

impl PendingTimer {
    fn new(fires_at: Instant, train_root: PrNumber, condition: WaitCondition) -> Self {
        PendingTimer {
            fires_at,
            train_root,
            condition,
        }
    }
}

/// Per-repo worker state.
///
/// This struct encapsulates the state and resources needed to process events
/// for a single repository. It runs as a tokio task with an async event loop.
pub struct RepoWorker {
    /// Worker configuration.
    pub(crate) config: WorkerConfig,

    /// Priority queue for pending events.
    pub(crate) queue: EventQueue,

    /// Repository state snapshot.
    pub(crate) state: PersistedRepoSnapshot,

    /// Event log for durability.
    pub(crate) event_log: EventLog,

    /// Set of delivery IDs with pending fsync (for batched writes).
    pub(crate) pending_batch: Vec<DeliveryId>,

    /// Polling configuration.
    pub(crate) poll_config: PollConfig,

    /// Cancellation tokens for active stacks, keyed by root PR.
    pub(crate) stack_tokens: HashMap<PrNumber, CancellationToken>,

    /// Pending timers for non-blocking waits.
    pub(crate) pending_timers: Vec<PendingTimer>,

    /// Time of last poll for active trains.
    pub(crate) last_poll: Option<Instant>,

    /// Time of last periodic spool drain.
    /// This runs independently of active trains to catch deliveries that
    /// were spooled but not dispatched (e.g., dispatcher failure).
    pub(crate) last_spool_drain: Option<Instant>,

    /// Optional GitHub client for executing GitHub effects.
    /// If None, effects are logged but not executed (testing/dry-run mode).
    pub(crate) github_client: Option<OctocrabClient>,

    /// PRs that have been warned about manual retargeting.
    /// Used to dedupe warning comments - we only post once per PR.
    /// In-memory only; cleared on restart (acceptable to re-warn once).
    pub(crate) warned_manual_retargets: HashSet<PrNumber>,
}

impl RepoWorker {
    /// Creates a new worker for a repository.
    ///
    /// This loads or creates the repository state and event log.
    ///
    /// # Startup Sequence
    ///
    /// 1. Clean up any interrupted processing from previous run
    /// 2. Load snapshot (or create empty state)
    /// 3. Replay event log from snapshot's log_position
    /// 4. Open event log for appending
    /// 5. Drain spool and enqueue pending deliveries
    #[instrument(skip(config), fields(repo = %config.repo))]
    pub fn new(config: WorkerConfig) -> Result<Self> {
        info!("Starting worker for {}", config.repo);

        // Ensure state directory exists
        if !config.state_dir.exists() {
            std::fs::create_dir_all(&config.state_dir)?;
        }

        // Step 1: Clean up interrupted processing from previous run.
        // This MUST happen before draining the spool to prevent double-processing.
        debug!("Cleaning up interrupted processing");
        cleanup_interrupted_processing(&config.spool_dir)?;

        // Step 2: Load or create snapshot
        let snapshot_path = find_latest_snapshot(&config.state_dir)?;
        let mut state = if let Some(path) = snapshot_path {
            debug!(path = %path.display(), "Loading snapshot");
            load_snapshot(&path)?
        } else {
            debug!("No snapshot found, creating empty state");
            PersistedRepoSnapshot::new("main")
        };

        // Step 3: Replay event log from snapshot's log_position
        let log_path = config
            .state_dir
            .join(format!("events.{}.log", state.log_generation));
        let (events, next_seq) = EventLog::replay_from(&log_path, state.log_position)?;
        debug!(
            replayed = events.len(),
            next_seq = next_seq,
            "Replayed event log"
        );

        // Apply replayed events to state
        for event in events {
            apply_event_to_state(&mut state, &event);
        }
        state.next_seq = next_seq;

        // Step 4: Open event log for appending
        let event_log = EventLog::open_with_seq(&log_path, next_seq)?;

        // Step 5: Drain spool and create queue
        let mut queue = EventQueue::new();
        let pending = drain_pending(&config.spool_dir)?;
        debug!(pending = pending.len(), "Drained spool");

        // Enqueue pending deliveries.
        // Parse errors are handled gracefully to prevent a single malformed delivery
        // from wedging the worker. Malformed deliveries are marked as done and skipped.
        for delivery in pending {
            match parse_and_classify(&delivery, &config.bot_name) {
                Ok(Some((event, priority))) => {
                    queue.push(event, delivery.delivery_id, priority);
                }
                Ok(None) => {
                    // Event type not recognized or not relevant - mark as done
                    mark_done(&delivery)?;
                }
                Err(e) => {
                    // Malformed delivery: log error and mark as done to prevent
                    // this delivery from blocking startup or being retried forever.
                    // This is a recoverable error - the delivery is corrupt and
                    // retrying won't help.
                    warn!(
                        delivery_id = %delivery.delivery_id,
                        error = %e,
                        "Malformed delivery at startup, marking as done and skipping"
                    );
                    mark_done(&delivery)?;
                }
            }
        }

        // Step 6: Create cancellation tokens for any existing active trains.
        // This ensures stop commands can interrupt in-flight operations for trains
        // that were active when the previous worker instance stopped.
        let mut stack_tokens = HashMap::new();
        for root_pr in state.active_trains.keys() {
            debug!(root_pr = %root_pr, "Creating cancellation token for existing train");
            stack_tokens.insert(*root_pr, CancellationToken::new());
        }

        // Step 7: Restore wait timers from persisted wait conditions.
        // Trains that were waiting when the previous instance stopped need
        // their timers restored so they can resume waiting.
        let poll_config = PollConfig::from_env();
        let mut pending_timers = Vec::new();
        for (root_pr, train) in &state.active_trains {
            if let Some(ref wait_condition) = train.wait_condition {
                use crate::types::PersistedWaitCondition;
                let condition = match wait_condition {
                    PersistedWaitCondition::HeadRefOid {
                        pr,
                        expected,
                        retry_count,
                    } => WaitCondition::HeadRefOid {
                        pr: *pr,
                        expected: expected.clone(),
                        retry_count: *retry_count,
                    },
                    PersistedWaitCondition::MergeStateStatus {
                        pr,
                        not,
                        retry_count,
                    } => WaitCondition::MergeStateStatus {
                        pr: *pr,
                        not: *not,
                        retry_count: *retry_count,
                    },
                    PersistedWaitCondition::CheckSuiteCompleted { sha, retry_count } => {
                        WaitCondition::CheckSuiteCompleted {
                            sha: sha.clone(),
                            retry_count: *retry_count,
                        }
                    }
                };
                // Schedule timer to fire immediately on restart to re-check condition
                let fires_at = Instant::now() + poll_config.recheck_interval;
                debug!(
                    root_pr = %root_pr,
                    condition = ?condition,
                    "Restoring wait timer from persisted condition"
                );
                pending_timers.push(PendingTimer::new(fires_at, *root_pr, condition));
            }
        }

        Ok(RepoWorker {
            config,
            queue,
            state,
            event_log,
            pending_batch: Vec::new(),
            poll_config,
            stack_tokens,
            pending_timers,
            last_poll: None,
            last_spool_drain: None,
            github_client: None,
            warned_manual_retargets: HashSet::new(),
        })
    }

    /// Sets the GitHub client for executing GitHub effects.
    ///
    /// If not set, GitHub effects are logged but not executed (useful for testing).
    /// When set, effects are executed against the real GitHub API and responses
    /// are used to update cached state.
    pub fn with_github_client(mut self, client: OctocrabClient) -> Self {
        self.github_client = Some(client);
        self
    }

    /// Returns the repository this worker handles.
    pub fn repo(&self) -> &RepoId {
        &self.config.repo
    }

    /// Returns the current state snapshot.
    pub fn state(&self) -> &PersistedRepoSnapshot {
        &self.state
    }

    /// Returns the number of events in the queue.
    pub fn queue_len(&self) -> usize {
        self.queue.len()
    }

    /// Returns true if the queue is empty.
    pub fn queue_is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Enqueues a new event for processing.
    ///
    /// This is called by the dispatch layer when a new webhook arrives
    /// while the worker is running.
    ///
    /// # Deduplication
    ///
    /// The delivery is NOT enqueued if:
    /// - It's already marked as done (`.done` marker exists)
    /// - It's already in the queue (same delivery ID)
    /// - The logical event was already processed (dedupe key in `seen_dedupe_keys`)
    ///
    /// This prevents duplicate processing when:
    /// - A redelivery arrives for an already-processed webhook
    /// - The same delivery is dispatched multiple times (e.g., on restart)
    /// - GitHub redelivers a webhook with a new `X-GitHub-Delivery` ID
    pub fn enqueue(&mut self, delivery: SpooledDelivery) -> Result<()> {
        use crate::spool::dedupe::is_duplicate;

        // Skip if already done
        if delivery.is_done() {
            trace!(
                delivery_id = %delivery.delivery_id,
                "Skipping already-done delivery"
            );
            return Ok(());
        }

        // Skip if currently being processed (.proc marker exists).
        // This prevents double-processing when a redelivery arrives while the
        // original is in-flight. The .proc marker indicates an in-flight delivery,
        // so we must not enqueue again until it completes or is cleaned up on restart.
        if delivery.is_processing() {
            trace!(
                delivery_id = %delivery.delivery_id,
                "Skipping in-flight delivery (proc marker exists)"
            );
            return Ok(());
        }

        // Skip if already in queue
        if self.queue.contains(&delivery.delivery_id) {
            trace!(
                delivery_id = %delivery.delivery_id,
                "Skipping already-queued delivery"
            );
            return Ok(());
        }

        if let Some((event, priority)) = parse_and_classify(&delivery, &self.config.bot_name)? {
            // Check for logical duplicate using dedupe key from snapshot.
            // This catches GitHub redeliveries that have a new X-GitHub-Delivery ID
            // but represent the same logical event.
            if let Some(dedupe_key) = event.dedupe_key()
                && is_duplicate(&self.state.seen_dedupe_keys, &dedupe_key)
            {
                trace!(
                    delivery_id = %delivery.delivery_id,
                    dedupe_key = %dedupe_key,
                    "Skipping duplicate event (dedupe key seen)"
                );
                mark_done(&delivery)?;
                return Ok(());
            }

            self.queue.push(event, delivery.delivery_id, priority);
        } else {
            // Event type not recognized - mark as done immediately
            mark_done(&delivery)?;
        }
        Ok(())
    }

    /// Processes the next event in the queue.
    ///
    /// Returns a `ProcessResult` indicating:
    /// - Whether an event was processed
    /// - Effects that need to be executed asynchronously
    /// - The delivery ID that was processed
    ///
    /// # Processing Steps
    ///
    /// 1. Pop highest-priority event from queue
    /// 2. Create `.proc` marker (claim delivery)
    /// 3. Call event handler to get state events and effects
    /// 4. Append state events to log
    /// 5. Apply state events to snapshot
    /// 6. If any event is critical: fsync log immediately, create `.done` marker
    /// 7. Otherwise: add to pending batch (fsync deferred)
    /// 8. Return effects for async execution
    #[instrument(skip(self), fields(repo = %self.config.repo))]
    pub fn process_next(&mut self) -> Result<ProcessResult> {
        // Pop next event
        let queued = match self.queue.pop() {
            Some(q) => q,
            None => {
                return Ok(ProcessResult {
                    processed: false,
                    effects: Vec::new(),
                    delivery_id: None,
                });
            }
        };

        let delivery_id = queued.delivery_id.clone();
        debug!(
            delivery_id = %delivery_id,
            priority = ?queued.priority,
            "Processing event"
        );

        // Create SpooledDelivery for marker operations
        let delivery = SpooledDelivery::new(&self.config.spool_dir, delivery_id.clone());

        // Step 2: Create `.proc` marker
        mark_processing(&delivery)?;

        // After creating the `.proc` marker, we must handle errors carefully:
        // - Persistence failures: propagate error, leave `.proc` for restart recovery
        // - Recoverable failures (handler, parse): mark as done, return success
        //
        // This prevents recoverable errors from leaving deliveries stuck with
        // a `.proc` marker but no `.done` marker (which would prevent retries
        // until restart).
        match self.process_event_after_proc_marker(&queued, &delivery) {
            Ok(result) => Ok(result),
            Err(e) if e.is_persistence_failure() => {
                // Fatal: persistence failure. Leave `.proc` marker so the
                // delivery is re-processed after restart.
                Err(e)
            }
            Err(e) => {
                // Recoverable error (handler logic, parsing).
                // Mark as done to prevent the delivery from being stuck.
                // Log the error but don't propagate it - the delivery is "handled"
                // even though it failed, because retrying won't help.
                warn!(
                    delivery_id = %delivery_id,
                    error = %e,
                    "Recoverable error processing event, marking as done"
                );
                mark_done(&delivery)?;
                Ok(ProcessResult {
                    processed: true,
                    effects: Vec::new(),
                    delivery_id: Some(delivery_id),
                })
            }
        }
    }

    /// Inner processing logic after `.proc` marker is created.
    ///
    /// Separated from `process_next` to enable proper error handling:
    /// persistence failures propagate, recoverable failures mark done.
    fn process_event_after_proc_marker(
        &mut self,
        queued: &super::queue::QueuedEvent,
        delivery: &SpooledDelivery,
    ) -> Result<ProcessResult> {
        let delivery_id = queued.delivery_id.clone();

        // Step 3: Call event handler
        let result = handle_event(&queued.event, &self.state, &self.config.bot_name)?;

        // Step 4 & 5: Append state events and apply to state
        let has_critical = self.apply_handler_result(&result)?;

        // Mark dedupe key as seen to prevent reprocessing of GitHub redeliveries.
        // This is done after apply_handler_result so the key is only recorded
        // for successfully processed events.
        if let Some(dedupe_key) = queued.event.dedupe_key() {
            crate::spool::dedupe::mark_seen(&mut self.state.seen_dedupe_keys, &dedupe_key);
            trace!(dedupe_key = %dedupe_key, "Marked dedupe key as seen");
        }

        // Step 6 & 7: Handle fsync based on criticality
        if has_critical {
            // Critical event: fsync immediately and create done marker.
            // We must sync the log even if pending_batch is empty, because
            // this critical event was just appended to the log.
            self.event_log.sync()?;
            self.flush_pending_batch_done_markers()?;
            mark_done(delivery)?;
            trace!(delivery_id = %delivery_id, "Marked done (critical)");
        } else {
            // Non-critical: add to pending batch
            self.pending_batch.push(delivery_id.clone());
            trace!(
                delivery_id = %delivery_id,
                batch_size = self.pending_batch.len(),
                "Added to pending batch"
            );
        }

        // Step 8: Return effects for async execution
        Ok(ProcessResult {
            processed: true,
            effects: result.effects.clone(),
            delivery_id: Some(delivery_id),
        })
    }

    /// Processes all events currently in the queue.
    ///
    /// Returns the number of events processed and all collected effects.
    /// Effects are NOT executed by this method - use `process_all_with_effects`
    /// for async effect execution.
    pub fn process_all(&mut self) -> Result<(usize, Vec<Effect>)> {
        let mut count = 0;
        let mut all_effects = Vec::new();

        loop {
            let result = self.process_next()?;
            if !result.processed {
                break;
            }
            count += 1;
            all_effects.extend(result.effects);
        }

        // Flush any remaining batched events
        if !self.pending_batch.is_empty() {
            self.flush_pending_batch()?;
        }

        Ok((count, all_effects))
    }

    /// Flushes the pending batch: fsync log and create done markers.
    ///
    /// This should be called:
    /// - Before any irreversible operation (e.g., GitHub API call)
    /// - Periodically to limit data loss window
    /// - On shutdown
    ///
    /// # Performance Note
    ///
    /// Each `mark_done` call currently fsyncs the spool directory individually
    /// (via `create_marker_file`). This is correct but suboptimal for batches.
    ///
    /// A future optimization could batch the fsync: create all `.done.tmp` files,
    /// fsync each, rename all to `.done`, then fsync the directory once. However,
    /// this requires careful handling of crash semantics (partial batch completion)
    /// and refactoring the atomic marker creation pattern in `delivery.rs`.
    ///
    /// For now, the per-marker fsync ensures correctness at the cost of extra
    /// syscalls. In practice, the batch size is bounded by the event loop's
    /// processing rate, so the overhead is modest.
    pub fn flush_pending_batch(&mut self) -> Result<()> {
        if self.pending_batch.is_empty() {
            return Ok(());
        }

        // fsync the event log
        self.event_log.sync()?;

        // Create done markers for all pending deliveries.
        // Note: Each mark_done currently fsyncs the spool directory individually.
        // See doc comment above for optimization discussion.
        for delivery_id in &self.pending_batch {
            let delivery = SpooledDelivery::new(&self.config.spool_dir, delivery_id.clone());
            mark_done(&delivery)?;
        }

        debug!(count = self.pending_batch.len(), "Flushed pending batch");

        self.pending_batch.clear();
        Ok(())
    }

    /// Creates done markers for pending batch items without syncing the log.
    ///
    /// This is used when the log has already been synced (e.g., for critical events)
    /// and we just need to mark the pending batch items as done.
    fn flush_pending_batch_done_markers(&mut self) -> Result<()> {
        if self.pending_batch.is_empty() {
            return Ok(());
        }

        // Create done markers for all pending deliveries.
        for delivery_id in &self.pending_batch {
            let delivery = SpooledDelivery::new(&self.config.spool_dir, delivery_id.clone());
            mark_done(&delivery)?;
        }

        debug!(
            count = self.pending_batch.len(),
            "Flushed pending batch done markers (log already synced)"
        );

        self.pending_batch.clear();
        Ok(())
    }

    /// Saves the current state to a snapshot file.
    ///
    /// Call this periodically to enable faster recovery.
    pub fn save_snapshot(&mut self) -> Result<()> {
        // Update snapshot metadata
        self.state.log_position = self.event_log.position()?;
        self.state.next_seq = self.event_log.next_seq();
        self.state.touch();

        // Save atomically
        let snapshot_path = self
            .config
            .state_dir
            .join(format!("snapshot.{}.json", self.state.log_generation));
        save_snapshot_atomic(&snapshot_path, &self.state)?;

        debug!(
            path = %snapshot_path.display(),
            log_position = self.state.log_position,
            "Saved snapshot"
        );

        Ok(())
    }

    // ─── Async Event Loop ─────────────────────────────────────────────────────────

    /// Runs the worker event loop.
    ///
    /// This is the main entry point for the async worker. It processes messages
    /// from the channel, handles timers, and responds to shutdown signals.
    ///
    /// # Arguments
    ///
    /// * `rx` - Channel receiver for incoming messages
    /// * `cancel_rx` - High-priority channel for cancellation requests
    /// * `shutdown` - Cancellation token for graceful shutdown
    #[instrument(skip(self, rx, cancel_rx, shutdown), fields(repo = %self.config.repo))]
    pub async fn run(
        mut self,
        mut rx: mpsc::Receiver<WorkerMessage>,
        mut cancel_rx: mpsc::UnboundedReceiver<PrNumber>,
        shutdown: CancellationToken,
    ) -> Result<()> {
        info!("Worker event loop started");

        // Process any events that were already in the queue from startup.
        // This handles the case where drain_pending() found deliveries but
        // no new webhooks arrive to trigger processing.
        if !self.queue.is_empty() {
            debug!(queued = self.queue.len(), "Processing startup backlog");
            let (count, effects) = self.process_all()?;
            debug!(
                processed = count,
                effects = effects.len(),
                "Processed startup backlog"
            );

            // Execute effects from startup backlog
            if !effects.is_empty() {
                self.execute_effects(effects, &shutdown).await?;
            }

            // Re-evaluate active cascades after processing startup backlog.
            // This ensures that replayed deliveries (e.g., PR merged, CI passed)
            // advance trains without waiting for a later poll or webhook.
            self.re_evaluate_cascades_with_persistence(&shutdown, "startup")
                .await?;
        }

        // Schedule initial poll with jitter
        let initial_delay = self.poll_config.initial_poll_delay(&self.config.repo);
        self.last_poll = Some(Instant::now() - self.poll_config.poll_interval + initial_delay);

        loop {
            // Calculate time until next poll (only if active trains)
            let poll_delay = self.time_until_next_poll();

            // Calculate time until next timer
            let timer_delay = self.time_until_next_timer();

            // Calculate time until next spool drain (always active)
            let spool_drain_delay = self.time_until_next_spool_drain();

            // Use the minimum of all delays
            let next_wakeup = [poll_delay, timer_delay, spool_drain_delay]
                .into_iter()
                .flatten()
                .min();

            tokio::select! {
                // Use biased mode to prioritize cancellation and shutdown
                biased;

                // Graceful shutdown (highest priority)
                _ = shutdown.cancelled() => {
                    info!("Shutdown signal received, stopping worker");
                    break;
                }

                // High-priority cancellation requests from dispatcher.
                // Per DESIGN.md: "Sends a CancelStackRequest (containing the PR number)
                // to the worker via a separate channel" for prompt stop handling.
                Some(pr) = cancel_rx.recv() => {
                    self.handle_cancel_stack(pr);
                }

                // Incoming message
                msg = rx.recv() => {
                    match msg {
                        Some(WorkerMessage::Shutdown) => {
                            info!("Shutdown message received");
                            break;
                        }
                        Some(msg) => {
                            if let Err(e) = self.handle_message(msg, &shutdown).await {
                                if e.is_persistence_failure() {
                                    // Fatal: persistence failure means we can't guarantee
                                    // correctness after restart. Stop the worker and leave
                                    // .proc markers so deliveries are re-processed.
                                    error!(error = %e, "Persistence failure, stopping worker");
                                    return Err(e);
                                }
                                error!(error = %e, "Error handling message (continuing)");
                            }
                        }
                        None => {
                            // Channel closed, all senders dropped
                            info!("Message channel closed");
                            break;
                        }
                    }
                }

                // Timer wakeup (poll, spool drain, or pending timer)
                _ = async {
                    match next_wakeup {
                        Some(delay) => tokio::time::sleep(delay).await,
                        None => std::future::pending().await,
                    }
                } => {
                    // Check if it's time for a poll (requires active trains)
                    if self.should_poll()
                        && let Err(e) = self.handle_poll_active_trains().await
                    {
                        if e.is_persistence_failure() {
                            error!(error = %e, "Persistence failure during poll, stopping worker");
                            return Err(e);
                        }
                        error!(error = %e, "Error during poll (continuing)");
                    }

                    // Check if it's time for periodic spool drain (always active)
                    if self.should_drain_spool()
                        && let Err(e) = self.handle_periodic_spool_drain(&shutdown).await
                    {
                        if e.is_persistence_failure() {
                            error!(error = %e, "Persistence failure during spool drain, stopping worker");
                            return Err(e);
                        }
                        error!(error = %e, "Error during periodic spool drain (continuing)");
                    }

                    // Fire any expired timers
                    if let Err(e) = self.fire_expired_timers(&shutdown).await {
                        if e.is_persistence_failure() {
                            error!(error = %e, "Persistence failure firing timers, stopping worker");
                            return Err(e);
                        }
                        error!(error = %e, "Error firing timers (continuing)");
                    }
                }
            }
        }

        // Graceful shutdown: flush pending batch
        if let Err(e) = self.flush_pending_batch() {
            error!(error = %e, "Error flushing pending batch on shutdown");
        }

        info!("Worker event loop stopped");
        Ok(())
    }

    /// Handles an incoming worker message.
    async fn handle_message(
        &mut self,
        msg: WorkerMessage,
        shutdown: &CancellationToken,
    ) -> Result<()> {
        match msg {
            WorkerMessage::Delivery(delivery) => {
                self.handle_delivery(delivery, shutdown).await?;
            }
            WorkerMessage::CancelStack(_pr) => {
                // CancelStack is now handled via the separate high-priority cancel channel.
                // This variant is kept for backwards compatibility with tests but should
                // not be used in production - use the cancel_rx channel instead.
                unreachable!(
                    "CancelStack should be sent via the cancel_rx channel, not the main message channel"
                );
            }
            WorkerMessage::PollActiveTrains => {
                self.handle_poll_active_trains().await?;
            }
            WorkerMessage::TimerFired {
                train_root,
                condition,
            } => {
                self.handle_timer_fired(train_root, condition, shutdown)
                    .await?;
            }
            WorkerMessage::Shutdown => {
                // Handled in run() loop
            }
        }
        Ok(())
    }

    /// Handles an incoming delivery.
    ///
    /// For stop commands, immediately cancels in-flight operations before processing.
    /// Then processes events synchronously, executes handler effects inline, and
    /// re-evaluates active cascades to advance trains.
    ///
    /// # Effect Execution Model
    ///
    /// Effects are executed inline (sequentially within this function). This means:
    /// - Long-running effects will block message handling for this worker
    /// - New webhook deliveries queue up in the channel but aren't processed until
    ///   effect execution completes
    /// - Stop commands received during effect execution won't interrupt until the
    ///   current effect batch completes (though cancellation tokens allow individual
    ///   effects to check for cancellation before starting)
    ///
    /// This design is intentional for Stage 17: it ensures correctness by preventing
    /// interleaving of event processing.
    ///
    /// # Cancellation Guarantees
    ///
    /// Stop commands get high priority in the queue and trigger token cancellation
    /// AFTER the handler validates the command and produces a TrainStopped event.
    /// This prevents unauthorized commenters from cancelling in-flight operations.
    ///
    /// At effect boundaries:
    /// - Cancellation is checked before each effect, so pending effects are skipped
    /// - Individual effects (like `git push`) run to completion once started
    ///
    /// This means latency between stop command and actual interruption is bounded by
    /// queue processing time plus the duration of a single effect (typically seconds
    /// for git operations). This tradeoff prioritizes correctness (authorization
    /// before cancellation, no partial effects) over absolute promptness.
    ///
    /// # Cascade Evaluation
    ///
    /// After processing webhook events and their handler effects, we re-evaluate
    /// active cascades. This is essential because webhooks trigger state changes
    /// (e.g., PR merged, CI passed, review approved) that may allow trains to advance.
    /// Without this step, trains would only advance during polling, causing delays.
    async fn handle_delivery(
        &mut self,
        delivery: SpooledDelivery,
        shutdown: &CancellationToken,
    ) -> Result<()> {
        // Note: We intentionally do NOT cancel stacks immediately when detecting stop
        // commands. Cancellation only happens AFTER the handler validates the stop
        // command and produces a TrainStopped event. This prevents unauthorized
        // commenters from cancelling in-flight operations.
        //
        // Stop commands still get high priority in the queue, ensuring they're
        // processed promptly after validation.

        // Enqueue the delivery
        self.enqueue(delivery)?;

        // Process all pending events and collect effects
        let (count, effects) = self.process_all()?;
        trace!(
            processed = count,
            effects = effects.len(),
            "Processed events"
        );

        // Execute effects from event handlers (e.g., reactions, comments)
        if !effects.is_empty() {
            self.execute_effects(effects, shutdown).await?;
        }

        // Re-evaluate active cascades to advance trains based on the new state.
        // This is the key step that allows webhooks (PR merged, CI passed, etc.)
        // to trigger cascade advancement without waiting for the next poll.
        self.re_evaluate_cascades_with_persistence(shutdown, "webhook")
            .await?;

        Ok(())
    }

    /// Executes a list of effects, respecting cancellation.
    ///
    /// This method processes effects **inline** (sequentially within the caller's
    /// execution context). While effects are executing, no other messages can be
    /// processed by this worker. Cancellation is checked before each effect, so
    /// stop commands can interrupt at effect boundaries, but not mid-effect.
    ///
    /// Effect execution uses the `EffectExecutor` which integrates with the
    /// GitHub interpreter (or a logging stub in Stage 17).
    ///
    /// # Effect Types
    ///
    /// - `RecordReconciliation`: Handled locally by updating the cached PR state.
    ///   These effects are pure state updates and don't require external API calls.
    ///
    /// - `GitHub(*)`: Executed via the GitHub interpreter when available.
    ///   In Stage 17, effects are logged but not executed (awaiting Stage 18 integration).
    ///   The logging ensures effects are not silently ignored and aids debugging.
    ///
    /// - `Git(*)`: Executed via the Git interpreter when available.
    ///   In Stage 17, effects are logged but not executed.
    ///
    /// # Cancellation Design
    ///
    /// Two levels of cancellation are checked **before** each effect:
    /// 1. Global shutdown - stops all effect processing
    /// 2. Stack-scoped cancellation - stops effects for a specific train
    ///
    /// Stack-scoped cancellation allows stop commands to interrupt a batch at
    /// effect boundaries without affecting other trains. The key constraint is:
    /// - Cancellation is **cooperative**, not preemptive
    /// - Once an effect starts (e.g., `git push`, `GitHub API call`), it runs to completion
    /// - The next effect in the batch will see the cancellation and be skipped
    ///
    /// This design ensures no partial effects (which could leave inconsistent state)
    /// while still providing prompt interruption. Individual effects should be fast
    /// (seconds), so worst-case latency is bounded by single-effect duration.
    ///
    /// # Stage 18 Integration
    ///
    /// When Stage 18 provides the GitHub client, this method will:
    /// 1. Create an `EffectExecutor` with the GitHub interpreter
    /// 2. Execute each effect and handle the response
    /// 3. Update cached state based on GitHub responses (e.g., PrRefetched)
    pub(crate) async fn execute_effects(
        &mut self,
        effects: Vec<Effect>,
        shutdown: &CancellationToken,
    ) -> Result<()> {
        use crate::worker::effects::{EffectExecutor, LoggingGitHubInterpreter};

        if effects.is_empty() {
            return Ok(());
        }

        debug!(count = effects.len(), "Executing effects");

        // Execute effects with either the real GitHub client or a logging interpreter.
        // When a real client is available, responses are processed to update state.
        // In dry-run mode (no GitHub client), we must NOT apply state updates since
        // the logging interpreter returns placeholder/fake data that would corrupt state.
        if let Some(ref github_client) = self.github_client {
            let executor = EffectExecutor::new(github_client.clone(), shutdown.clone());
            self.execute_effects_with_executor(effects, shutdown, &executor, true)
                .await?;
        } else {
            // No GitHub client configured - use logging interpreter (for testing/dry-run).
            // CRITICAL: Pass apply_state_updates=false to avoid corrupting state with
            // placeholder data from the logging interpreter.
            let logging_interpreter = LoggingGitHubInterpreter::new();
            let executor = EffectExecutor::new(logging_interpreter, shutdown.clone());
            self.execute_effects_with_executor(effects, shutdown, &executor, false)
                .await?;
        }

        Ok(())
    }

    /// Executes effects using the given executor and optionally processes responses.
    ///
    /// # Parameters
    ///
    /// - `apply_state_updates`: If true, GitHub responses update cached state.
    ///   Should be false in dry-run mode to avoid corrupting state with placeholder data.
    async fn execute_effects_with_executor<G>(
        &mut self,
        effects: Vec<Effect>,
        shutdown: &CancellationToken,
        executor: &crate::worker::effects::EffectExecutor<G>,
        apply_state_updates: bool,
    ) -> Result<()>
    where
        G: crate::effects::GitHubInterpreter + Clone,
        G::Error: std::fmt::Display,
    {
        use crate::worker::effects::{EffectError, EffectResult};

        for effect in effects {
            // Check for global shutdown before each effect
            if shutdown.is_cancelled() {
                debug!("Shutdown requested, skipping remaining effects");
                break;
            }

            // Look up the stack token for this effect's PR (if any).
            // This allows stop commands to interrupt in-flight effects for a specific train.
            let stack_token = effect
                .pr_number()
                .and_then(|pr| self.find_train_root(pr))
                .and_then(|train_root| self.stack_tokens.get(&train_root));

            // Check for stack-scoped cancellation before starting.
            // If this effect is for a PR that belongs to a cancelled train, skip it.
            if let Some(token) = stack_token
                && token.is_cancelled()
            {
                if let Some(pr) = effect.pr_number() {
                    debug!(
                        pr = %pr,
                        "Stack cancelled, skipping effect"
                    );
                }
                continue;
            }

            // Handle RecordReconciliation effects locally (no external calls needed)
            if let Effect::RecordReconciliation {
                ref pr,
                ref squash_sha,
            } = effect
            {
                if let Some(cached_pr) = self.state.prs.get_mut(pr) {
                    cached_pr.predecessor_squash_reconciled = Some(squash_sha.clone());
                    trace!(?pr, ?squash_sha, "Recorded reconciliation in state");
                }
                continue;
            }

            // Execute the effect using the executor, passing the stack token
            // so that stop commands can interrupt in-flight GitHub API calls.
            match executor
                .execute_with_stack_token(effect, &mut self.state, stack_token)
                .await
            {
                Ok(result) => {
                    trace!(?result, "Effect executed successfully");

                    // Process responses to update cached state.
                    // This is essential for poll-based state refresh.
                    // Skip in dry-run mode to avoid corrupting state with placeholder data.
                    if apply_state_updates && let EffectResult::GitHub(response) = result {
                        self.process_github_response(response);
                    }
                }
                Err(EffectError::Cancelled) => {
                    debug!("Effect execution cancelled");
                    break;
                }
                Err(e) => {
                    // Log error but continue with remaining effects.
                    // Individual effect failures shouldn't stop the whole batch.
                    warn!(error = %e, "Effect execution failed");
                }
            }
        }

        Ok(())
    }

    /// Processes a GitHub response to update cached state.
    ///
    /// This is called after executing each GitHub effect to ensure the local
    /// cache stays in sync with GitHub. This is essential for:
    /// - Poll-based state refresh (`RefetchPr` effects)
    /// - Keeping merge state status up-to-date
    pub(crate) fn process_github_response(&mut self, response: GitHubResponse) {
        match response {
            GitHubResponse::PrRefetched { pr, merge_state } => {
                // Update the cached PR with fresh data from GitHub
                self.update_cached_pr(&pr, merge_state);
                debug!(
                    pr = %pr.number,
                    merge_state = ?merge_state,
                    "Updated cached PR from RefetchPr response"
                );
            }
            GitHubResponse::Pr(pr) => {
                // Update the cached PR (merge state is Unknown for basic Pr response)
                self.update_cached_pr(&pr, crate::types::MergeStateStatus::Unknown);
            }
            GitHubResponse::MergeState(merge_state) => {
                // MergeState alone doesn't tell us which PR - logged but not cached
                trace!(merge_state = ?merge_state, "Received MergeState response");
            }
            GitHubResponse::Merged { sha } => {
                trace!(sha = ?sha, "PR merged successfully");
                // The PR's state update will come via webhook or subsequent RefetchPr
            }
            GitHubResponse::Retargeted => {
                trace!("PR retargeted successfully");
            }
            GitHubResponse::CommentPosted { id } => {
                trace!(comment_id = %id.0, "Comment posted");
            }
            GitHubResponse::CommentUpdated => {
                trace!("Comment updated");
            }
            GitHubResponse::ReactionAdded => {
                trace!("Reaction added");
            }
            // List responses are typically used for bootstrap, not incremental updates
            GitHubResponse::PrList(_)
            | GitHubResponse::RecentlyMergedPrList { .. }
            | GitHubResponse::Comments(_)
            | GitHubResponse::BranchProtection(_)
            | GitHubResponse::BranchProtectionUnknown
            | GitHubResponse::Rulesets(_)
            | GitHubResponse::RulesetsUnknown
            | GitHubResponse::RepoSettings(_) => {
                trace!("Received list/settings response (no incremental state update)");
            }
        }
    }

    /// Updates a cached PR with fresh data from GitHub.
    fn update_cached_pr(&mut self, pr_data: &PrData, merge_state: crate::types::MergeStateStatus) {
        use crate::types::CachedPr;

        let pr_number = pr_data.number;

        // Get existing PR or create a new one
        if let Some(cached_pr) = self.state.prs.get_mut(&pr_number) {
            // Update existing PR
            cached_pr.head_sha = pr_data.head_sha.clone();
            cached_pr.head_ref = pr_data.head_ref.clone();
            cached_pr.base_ref = pr_data.base_ref.clone();
            cached_pr.state = pr_data.state.clone();
            cached_pr.is_draft = pr_data.is_draft;
            cached_pr.merge_state_status = merge_state;
        } else {
            // Create new cached PR entry
            let cached_pr = CachedPr::new(
                pr_number,
                pr_data.head_sha.clone(),
                pr_data.head_ref.clone(),
                pr_data.base_ref.clone(),
                None, // predecessor is discovered via commands, not API
                pr_data.state.clone(),
                merge_state,
                pr_data.is_draft,
            );
            self.state.prs.insert(pr_number, cached_pr);
        }
    }

    /// Handles a stack cancellation request from external sources (e.g., `cancel_rx`).
    ///
    /// # Token Lifecycle
    ///
    /// When cancelling, we mark the token as cancelled but **do not remove it**.
    /// This is critical because effects may still be in-flight or pending:
    ///
    /// 1. External request or TrainStopped event triggers cancellation
    /// 2. `execute_effects` checks `token.is_cancelled()` before each effect
    /// 3. Remaining effects for this train are skipped
    ///
    /// The token is only removed when the train termination event is processed
    /// (TrainStopped, TrainCompleted, or TrainAborted in `apply_handler_result`).
    fn handle_cancel_stack(&mut self, pr: PrNumber) {
        // Find the train root for this PR
        let train_root = self.find_train_root(pr).unwrap_or(pr);

        // Cancel the token if it exists, but DO NOT remove it.
        // The token must remain so that in-flight/pending effects can check
        // is_cancelled() and skip execution.
        if let Some(token) = self.stack_tokens.get(&train_root) {
            info!(train_root = %train_root, "Cancelling stack operations");
            token.cancel();
        }

        // Remove any pending timers for this train
        self.pending_timers.retain(|t| t.train_root != train_root);
    }

    /// Handles the poll timer for active trains.
    ///
    /// This performs several important recovery tasks:
    ///
    /// 1. Drains the spool to pick up any deliveries that may have been missed
    ///    (e.g., due to dispatcher failures).
    ///
    /// 2. For each active train, generates a `RefetchPr` effect to check the
    ///    current PR's merge state. If the merge state has changed (e.g., CI
    ///    completed, review approved), the cascade will be re-evaluated.
    ///
    /// 3. Scans for "late additions" - PRs whose predecessor has merged but
    ///    that haven't been retargeted yet. These need reconciliation.
    ///
    /// 4. Scans for "manually retargeted" PRs - PRs that were manually
    ///    retargeted to the default branch before the bot could reconcile them.
    ///    These are dangerous because the ours-merge was never done.
    ///
    /// # Design Notes
    ///
    /// The late-addition and manual-retarget scans are described in DESIGN.md
    /// §"Late additions recovery" and §"Manually-retargeted PR detection".
    /// These scans ensure that:
    /// - Late additions are caught during polling if the predecessor_declared
    ///   event was missed
    /// - Manually-retargeted PRs are detected and warned about before they
    ///   can be incorrectly merged
    async fn handle_poll_active_trains(&mut self) -> Result<()> {
        use crate::effects::{Effect, GitHubEffect};
        use crate::spool::dedupe::prune_expired_keys_default;

        self.last_poll = Some(Instant::now());

        // Prune expired dedupe keys to prevent unbounded growth.
        // This runs during each poll cycle (every 10 minutes by default).
        let pruned = prune_expired_keys_default(&mut self.state.seen_dedupe_keys);
        if pruned > 0 {
            debug!(pruned = pruned, "Pruned expired dedupe keys");
        }

        // Create a cancellation token for effect execution in this poll cycle.
        // This is a local token; for stack-scoped cancellation, execute_effects
        // checks the individual train tokens as well.
        let shutdown = CancellationToken::new();

        // First, drain any pending deliveries from the spool.
        // This catches deliveries that may have been spooled but not dispatched
        // (e.g., dispatcher failure, network issue, race condition).
        let drained = self.drain_spool_periodic(&shutdown).await?;
        if drained > 0 {
            debug!(
                drained = drained,
                "Drained pending deliveries from spool during poll"
            );
        }

        // Collect effects to execute
        let mut effects = Vec::new();

        // ─── Active Train Polling ─────────────────────────────────────────────
        //
        // Generate RefetchPr effects for each active train's current PR.
        // This checks the GitHub state in case we missed webhooks.
        let active_trains: Vec<_> = self.state.active_trains.values().cloned().collect();

        if !active_trains.is_empty() {
            debug!(
                count = active_trains.len(),
                "Polling active trains for missed webhooks"
            );

            for train in &active_trains {
                let current_pr = train.current_pr;
                trace!(
                    train_root = %train.original_root_pr,
                    current_pr = %current_pr,
                    trigger = "poll",
                    "Generating RefetchPr effect for active train"
                );
                effects.push(Effect::GitHub(GitHubEffect::RefetchPr { pr: current_pr }));
            }
        }

        // ─── Late Addition Scan ───────────────────────────────────────────────
        //
        // Find PRs whose predecessor is merged but base_ref != default_branch.
        // These are "late additions" that need reconciliation.
        // Per DESIGN.md: "scan for PRs whose predecessor is merged but whose
        // base_ref hasn't been retargeted"
        //
        // NOTE(Stage 17): This scan DETECTS late additions and updates cached
        // state via RefetchPr, but does NOT perform reconciliation (git merge
        // with ours strategy) or retargeting (GitHub API call). Full handling
        // requires git worktree operations implemented in Stage 18's bootstrap/
        // recovery module. The detection infrastructure is in place; the recovery
        // path will be completed when bootstrap recovery is implemented.
        let late_additions = self.find_late_additions();

        if !late_additions.is_empty() {
            debug!(
                count = late_additions.len(),
                "Found late additions during poll scan"
            );

            for (pr_number, pred_number) in &late_additions {
                // Late addition detected - log and generate RefetchPr to update state.
                // Full handling (reconciliation) requires git operations and is
                // deferred to Stage 18. For now, we ensure we have fresh state.
                info!(
                    pr = %pr_number,
                    predecessor = %pred_number,
                    trigger = "poll_late_addition_scan",
                    "Late addition detected: predecessor merged but PR not retargeted"
                );
                effects.push(Effect::GitHub(GitHubEffect::RefetchPr { pr: *pr_number }));
            }
        }

        // ─── Manually Retargeted PR Scan ──────────────────────────────────────
        //
        // Find PRs that were manually retargeted to default_branch but NOT reconciled.
        // These are dangerous: the ours-merge was never done, so merging them
        // would lose the predecessor's changes.
        // Per DESIGN.md: "scan catches PRs that were manually retargeted before
        // reconciliation could run"
        //
        // NOTE(Stage 17): This scan DETECTS and WARNS about dangerous manual
        // retargets, but does NOT perform automatic reconciliation. The warning
        // comment alerts users to the danger. Full reconciliation requires:
        // 1. Finding the predecessor's squash_sha (from train state or git history)
        // 2. Running git merge with ours strategy in a worktree
        // 3. Pushing the reconciled branch
        // These git operations are implemented in Stage 18's bootstrap/recovery.
        let manually_retargeted = self.find_manually_retargeted();

        // Filter out PRs we've already warned about to avoid spamming.
        // The set is cleared on restart, so we may re-warn once per restart.
        let new_warnings: Vec<_> = manually_retargeted
            .into_iter()
            .filter(|(pr, _)| !self.warned_manual_retargets.contains(pr))
            .collect();

        if !new_warnings.is_empty() {
            warn!(
                count = new_warnings.len(),
                "Found manually retargeted PRs that need reconciliation"
            );

            for (pr_number, pred_number) in &new_warnings {
                // Manually retargeted PR detected - this is dangerous!
                // Per DESIGN.md: "Post warning and trigger reconciliation."
                warn!(
                    pr = %pr_number,
                    predecessor = %pred_number,
                    trigger = "poll_manual_retarget_scan",
                    "DANGEROUS: PR was manually retargeted to {} but not reconciled with predecessor's squash commit. \
                     This PR cannot be safely merged until reconciliation completes.",
                    self.state.default_branch
                );

                // Refetch to get current state
                effects.push(Effect::GitHub(GitHubEffect::RefetchPr { pr: *pr_number }));

                // Post warning comment per DESIGN.md:
                // "Post warning and trigger reconciliation."
                effects.push(Effect::GitHub(GitHubEffect::PostComment {
                    pr: *pr_number,
                    body: format!(
                        "⚠️ **Warning**: This PR was retargeted to `{}` but has not been \
                         reconciled with predecessor #{}'s squash commit.\n\n\
                         **This PR cannot be safely merged** until reconciliation completes. \
                         Merging now could lose changes from the predecessor.\n\n\
                         The bot will attempt reconciliation automatically. If this warning \
                         persists, manual intervention may be required.",
                        self.state.default_branch, pred_number
                    ),
                }));

                // Mark as warned to avoid repeating on next poll
                self.warned_manual_retargets.insert(*pr_number);

                // Note: Full reconciliation (git operations) is deferred to Stage 18.
                // The warning comment alerts the user to the danger while reconciliation
                // infrastructure is being built.
            }
        }

        // Execute all collected effects
        if !effects.is_empty() {
            self.execute_effects(effects, &shutdown).await?;
        }

        // After updating the cache, re-evaluate cascades to check if any trains
        // can now advance (e.g., CI completed while we weren't receiving webhooks).
        // Use the persistence version to ensure phase transitions are durably recorded.
        self.re_evaluate_cascades_with_persistence(&shutdown, "poll")
            .await?;

        Ok(())
    }

    /// Re-evaluates active cascades with proper persistence of phase transitions.
    ///
    /// This is called after processing events that may change cascade state:
    /// - After webhook delivery (trigger="webhook")
    /// - After poll updates the cache (trigger="poll")
    /// - After timer fires (trigger="timer")
    /// - After spool drain (trigger="spool_drain")
    /// - After startup backlog processing (trigger="startup")
    ///
    /// For each active train, we execute a cascade step and run any resulting
    /// effects. The cascade step function handles all merge states:
    /// - Clean/Unstable: can proceed to merge
    /// - Blocked: sets up waiting state and updates status
    /// - Unknown: sets up waiting state for state propagation
    /// - Draft: waits for PR to be marked ready
    ///
    /// This is the key mechanism that allows webhooks and poll fallback to
    /// advance trains, detect blocking conditions, and update status comments.
    ///
    /// The method:
    /// 1. Executes cascade steps for active trains
    /// 2. Persists phase transitions via the event log
    /// 3. Executes resulting effects (status updates, git operations, etc.)
    ///
    /// This ensures that cascade progress survives restarts.
    pub(crate) async fn re_evaluate_cascades_with_persistence(
        &mut self,
        shutdown: &CancellationToken,
        trigger: &str,
    ) -> Result<()> {
        use crate::cascade::step::{StepContext, execute_cascade_step};
        use crate::persistence::event::StateEventPayload;
        use crate::types::{CascadeStepOutcome, TrainError};

        // Collect train roots that may need re-evaluation.
        // We only re-evaluate trains that are WaitingCi or Running - stopped/aborted
        // trains require explicit restart.
        let trains_to_evaluate: Vec<_> = self
            .state
            .active_trains
            .iter()
            .filter(|(_, train)| train.state.is_active())
            .map(|(root, train)| {
                (
                    *root,
                    train.current_pr,
                    train.cascade_phase.clone(),
                    train.state,
                )
            })
            .collect();

        if trains_to_evaluate.is_empty() {
            return Ok(());
        }

        for (train_root, current_pr, phase_before, state_before) in trains_to_evaluate {
            // Check for cancellation
            if shutdown.is_cancelled() {
                break;
            }

            // Check if this train's stack is cancelled
            if let Some(token) = self.stack_tokens.get(&train_root)
                && token.is_cancelled()
            {
                continue;
            }

            // Get fresh PR state from cache for logging
            let pr_state = self.state.prs.get(&current_pr);

            debug!(
                train_root = %train_root,
                current_pr = %current_pr,
                merge_state = ?pr_state.map(|p| &p.merge_state_status),
                trigger = trigger,
                "Re-evaluating cascade"
            );

            // Get the train (clone to avoid borrow issues)
            let Some(train) = self.state.active_trains.get(&train_root).cloned() else {
                continue;
            };

            // Execute a cascade step
            let ctx = StepContext::new(&self.state.default_branch);
            let step_result = execute_cascade_step(train, &self.state.prs, &ctx);

            // Check if phase changed - if so, persist the transition.
            // Use full equality (not just name) to catch progress changes within
            // phases (e.g., marking a descendant as completed in Preparing).
            // Without this, a restart would lose progress and re-run completed work.
            let phase_changed = step_result.train.cascade_phase != phase_before;
            let train_completed = matches!(step_result.outcome, CascadeStepOutcome::Complete);
            let train_aborted = matches!(step_result.outcome, CascadeStepOutcome::Aborted { .. });
            let train_fan_out = matches!(step_result.outcome, CascadeStepOutcome::FanOut { .. });

            // Persist state changes via the event log
            if train_completed {
                // Train completed - persist and remove from active trains
                let event = self.event_log.append(StateEventPayload::TrainCompleted {
                    root_pr: train_root,
                })?;
                apply_event_to_state(&mut self.state, &event);
                self.event_log.sync()?;
                debug!(train_root = %train_root, "Train completed (persisted)");

                // Remove cancellation token and timers for completed train
                self.stack_tokens.remove(&train_root);
                self.pending_timers.retain(|t| t.train_root != train_root);
            } else if train_aborted {
                // Train aborted - persist with error
                // The error should be present when train is aborted; use a default if somehow missing.
                let error = step_result.train.error.clone().unwrap_or_else(|| {
                    TrainError::new("unknown", "Cascade aborted for unknown reason")
                });
                let event = self.event_log.append(StateEventPayload::TrainAborted {
                    root_pr: train_root,
                    error,
                })?;
                apply_event_to_state(&mut self.state, &event);
                self.event_log.sync()?;
                debug!(train_root = %train_root, "Train aborted (persisted)");

                // Remove cancellation token and timers for aborted train
                self.stack_tokens.remove(&train_root);
                self.pending_timers.retain(|t| t.train_root != train_root);
            } else if train_fan_out {
                // Fan-out: original train ends, descendants become new independent trains.
                // Extract descendants from the outcome.
                let descendants =
                    if let CascadeStepOutcome::FanOut { descendants } = &step_result.outcome {
                        descendants.clone()
                    } else {
                        // Should never happen given train_fan_out check, but be defensive
                        vec![]
                    };

                if !descendants.is_empty() {
                    // Persist the fan-out atomically: old train removed, new trains created
                    let event = self.event_log.append(StateEventPayload::FanOutCompleted {
                        old_root: train_root,
                        new_roots: descendants.clone(),
                        original_root_pr: step_result.train.original_root_pr,
                    })?;
                    apply_event_to_state(&mut self.state, &event);
                    self.event_log.sync()?;
                    debug!(
                        train_root = %train_root,
                        descendants = ?descendants,
                        "Fan-out completed (persisted)"
                    );

                    // Clean up old train's resources
                    self.stack_tokens.remove(&train_root);
                    self.pending_timers.retain(|t| t.train_root != train_root);

                    // Create cancellation tokens for new trains
                    for &new_root in &descendants {
                        self.stack_tokens.entry(new_root).or_default();
                    }
                }
            } else if phase_changed
                || step_result.train.current_pr != current_pr
                || step_result.train.state != state_before
            {
                // Phase transition, PR advancement, or state change - persist.
                // We persist state changes (e.g., Running -> WaitingCi) so that
                // after restart we know we were waiting and can restore timers.

                // Compute wait_condition based on the step outcome.
                // This must happen BEFORE persistence so it's captured in the event.
                //
                // Wait conditions enable non-blocking polling: instead of relying solely
                // on webhooks or the 10-minute poll interval, we schedule short-interval
                // rechecks to detect GitHub state propagation.
                //
                // - CheckSuiteCompleted: Scheduled when entering WaitingCi state
                // - MergeStateStatus: Scheduled when blocked with Unknown merge state
                // - HeadRefOid: Not yet implemented (needs Stage 18 git integration)
                //
                // See queue.rs WaitCondition docs for implementation status.
                use crate::types::{BlockReason, MergeStateStatus};
                let wait_condition = match &step_result.outcome {
                    CascadeStepOutcome::WaitingOnCi { pr_number } => {
                        self.state.prs.get(pr_number).map(|pr| {
                            crate::types::PersistedWaitCondition::CheckSuiteCompleted {
                                sha: pr.head_sha.clone(),
                                retry_count: 0,
                            }
                        })
                    }
                    CascadeStepOutcome::Blocked {
                        pr_number,
                        reason: BlockReason::Unknown,
                    } => Some(crate::types::PersistedWaitCondition::MergeStateStatus {
                        pr: *pr_number,
                        not: MergeStateStatus::Unknown,
                        retry_count: 0,
                    }),
                    _ => None,
                };

                let event = self.event_log.append(StateEventPayload::PhaseTransition {
                    train_root,
                    current_pr: step_result.train.current_pr,
                    predecessor_pr: step_result.train.predecessor_pr,
                    last_squash_sha: step_result.train.last_squash_sha.clone(),
                    phase: step_result.train.cascade_phase.clone(),
                    state: Some(step_result.train.state),
                    wait_condition: wait_condition.clone(),
                })?;
                apply_event_to_state(&mut self.state, &event);
                self.event_log.sync()?;
                debug!(
                    train_root = %train_root,
                    new_phase = ?step_result.train.cascade_phase.name(),
                    new_current_pr = %step_result.train.current_pr,
                    new_state = ?step_result.train.state,
                    "Phase transition (persisted)"
                );

                // Schedule wait timer if we computed a wait condition.
                // This enables non-blocking polling: instead of relying solely on
                // webhooks or the 10-minute poll interval, we schedule a 5-second
                // recheck to detect GitHub state propagation.
                if let Some(wc) = wait_condition {
                    use super::queue::WaitCondition;
                    let condition = match wc {
                        crate::types::PersistedWaitCondition::CheckSuiteCompleted {
                            sha,
                            retry_count,
                        } => WaitCondition::CheckSuiteCompleted { sha, retry_count },
                        crate::types::PersistedWaitCondition::HeadRefOid {
                            pr,
                            expected,
                            retry_count,
                        } => WaitCondition::HeadRefOid {
                            pr,
                            expected,
                            retry_count,
                        },
                        crate::types::PersistedWaitCondition::MergeStateStatus {
                            pr,
                            not,
                            retry_count,
                        } => WaitCondition::MergeStateStatus {
                            pr,
                            not,
                            retry_count,
                        },
                    };
                    self.schedule_wait_timer_without_persist(train_root, condition);
                }
            } else {
                // No phase change, but still update in-memory state
                self.state
                    .active_trains
                    .insert(train_root, step_result.train.clone());
            }

            // Execute the resulting effects.
            // These include status comment updates and GitHub/git operations.
            if !step_result.effects.is_empty() {
                debug!(
                    train_root = %train_root,
                    effects_count = step_result.effects.len(),
                    "Executing cascade effects"
                );
                self.execute_effects(step_result.effects, shutdown).await?;
            }
        }

        Ok(())
    }

    /// Drains pending deliveries from the spool during periodic polling.
    ///
    /// This catches deliveries that may have been spooled but not dispatched.
    /// Unlike startup drain, this doesn't clean up .proc markers since a concurrent
    /// process might legitimately own them.
    ///
    /// Any effects generated from processing recovered deliveries are executed,
    /// ensuring that GitHub/git operations are not silently dropped.
    pub(crate) async fn drain_spool_periodic(
        &mut self,
        shutdown: &CancellationToken,
    ) -> Result<usize> {
        let deliveries = drain_pending(&self.config.spool_dir)?;
        let mut enqueued = 0;

        for delivery in deliveries {
            // Try to enqueue; skip if already queued or done
            if self.enqueue(delivery).is_ok() {
                enqueued += 1;
            }
        }

        // Process any newly enqueued deliveries
        if enqueued > 0 {
            let (processed, effects) = self.process_all()?;
            trace!(
                enqueued = enqueued,
                processed = processed,
                effects_count = effects.len(),
                "Processed drained deliveries"
            );

            // Execute the effects (not silently discard them!)
            if !effects.is_empty() {
                self.execute_effects(effects, shutdown).await?;
            }
        }

        Ok(enqueued)
    }

    /// Handles a timer firing for non-blocking wait re-evaluation.
    ///
    /// Generates a `RefetchPr` effect to get the latest state from GitHub,
    /// then re-evaluates the cascade. If the train advanced, the timer is not
    /// rescheduled. If still waiting, reschedules with exponential backoff.
    ///
    /// # Generic Re-evaluation
    ///
    /// This method uses `RefetchPr` to update the PR's cached state regardless
    /// of the specific wait condition type. This works because:
    /// - `RefetchPr` returns the PR's current `mergeStateStatus` from GitHub
    /// - The cascade re-evaluation checks this status to decide if the train can advance
    /// - Whether we're waiting for CI, reviews, or merge state, the decision is based
    ///   on the same `mergeStateStatus` check
    ///
    /// The condition type is preserved for retry counting and logging, but doesn't
    /// change what data is fetched. This is intentional: GitHub's `mergeStateStatus`
    /// is the single source of truth for whether a PR is ready to merge.
    async fn handle_timer_fired(
        &mut self,
        train_root: PrNumber,
        condition: WaitCondition,
        shutdown: &CancellationToken,
    ) -> Result<()> {
        use crate::effects::{Effect, GitHubEffect};

        trace!(
            train_root = %train_root,
            condition = ?condition,
            "Timer fired for wait condition"
        );

        // Check if the train is still active and get its current PR
        let train = match self.state.active_trains.get(&train_root) {
            Some(t) => t.clone(),
            None => {
                debug!(
                    train_root = %train_root,
                    "Train no longer active, ignoring timer"
                );
                return Ok(());
            }
        };

        // Capture current state to detect advancement after re-evaluation.
        // We check both phase name and train state to catch state-only changes
        // (e.g., Running -> WaitingCi might not change phase name but indicates progress).
        let phase_before = train.cascade_phase.name();
        let state_before = train.state;
        let current_pr_before = train.current_pr;

        debug!(
            train_root = %train_root,
            current_pr = %train.current_pr,
            condition = ?condition,
            "Re-evaluating wait condition"
        );

        // Generate a RefetchPr effect to get the latest state from GitHub.
        let effects = vec![Effect::GitHub(GitHubEffect::RefetchPr {
            pr: train.current_pr,
        })];

        // Execute the effect to update cached state.
        // Use the passed shutdown token so global shutdown can cancel execution.
        if let Err(e) = self.execute_effects(effects, shutdown).await {
            warn!(
                train_root = %train_root,
                error = %e,
                "Failed to execute effects for timer re-evaluation"
            );
        }

        // Re-evaluate the cascade with updated state and persist phase transitions.
        // This uses the persistent version to ensure phase transitions are durably
        // recorded, preventing replay of side effects after a crash.
        self.re_evaluate_cascades_with_persistence(shutdown, "timer")
            .await?;

        // Check if the train advanced.
        // Train is considered "advanced" if:
        // - Phase name changed
        // - Train state changed
        // - Current PR changed (moved to next in stack)
        // - Train is no longer active (completed/stopped/aborted)
        let train_advanced = match self.state.active_trains.get(&train_root) {
            Some(train) => {
                train.cascade_phase.name() != phase_before
                    || train.state != state_before
                    || train.current_pr != current_pr_before
            }
            None => true, // Train no longer active = it advanced (completed/stopped)
        };

        if train_advanced {
            debug!(
                train_root = %train_root,
                "Train advanced after timer re-evaluation, not rescheduling"
            );
            // Clear the wait condition since we're no longer waiting
            if let Some(train) = self.state.active_trains.get_mut(&train_root) {
                train.wait_condition = None;
            }
        } else {
            // Still waiting - reschedule with backoff
            self.schedule_wait_retry(train_root, condition)?;
        }

        Ok(())
    }

    /// Schedules a retry timer for a wait condition with exponential backoff.
    ///
    /// Returns an error if max retries have been exceeded.
    ///
    /// # Timeout Calculation
    ///
    /// The max retries is computed from `poll_config.wait_timeout` to ensure
    /// we don't wait longer than configured. With exponential backoff starting
    /// at `recheck_interval` (5s) and capping at `wait_timeout`, we compute
    /// how many retries fit within the timeout period.
    fn schedule_wait_retry(
        &mut self,
        train_root: PrNumber,
        condition: WaitCondition,
    ) -> Result<()> {
        // Find the retry count from the condition's metadata
        let retry_count = condition.retry_count();

        // Compute max retries from wait_timeout and recheck_interval.
        // With exponential backoff: total_time = sum(min(base * 2^i, cap))
        // We approximate by computing how many base intervals fit in timeout.
        let wait_timeout = self.poll_config.wait_timeout;
        let base_interval = self.poll_config.recheck_interval;

        // Calculate max retries that fit within wait_timeout
        // With exponential backoff (5s, 10s, 20s, 40s, 80s, 160s, 300s...):
        // We estimate by dividing timeout by base interval and taking log2.
        // For 5min timeout with 5s base: ~6 retries before hitting cap.
        let max_retries = if base_interval.as_secs() > 0 {
            // Approximate: timeout / base_interval gives linear count,
            // but with exponential backoff we need fewer retries.
            // log2(timeout/base) + 1 is a reasonable approximation.
            let ratio = wait_timeout.as_secs_f64() / base_interval.as_secs_f64();
            (ratio.log2().ceil() as u32).clamp(1, 20)
        } else {
            10 // Fallback if base_interval is 0
        };

        if retry_count >= max_retries {
            warn!(
                train_root = %train_root,
                condition = ?condition,
                retry_count = retry_count,
                max_retries = max_retries,
                wait_timeout_secs = wait_timeout.as_secs(),
                "Wait condition timed out after max retries"
            );
            // For now, just log the timeout. Stage 18 will handle cascade failure.
            // TODO(Stage 18): Transition cascade to Failed state with timeout error
            return Ok(());
        }

        // Calculate backoff delay: starts at recheck_interval, caps at wait_timeout
        // This ensures individual delays don't exceed the overall timeout.
        let base_delay = base_interval;
        let backoff_factor = 2u32.saturating_pow(retry_count);
        let delay = base_delay * backoff_factor;
        let max_single_delay = wait_timeout; // Cap at wait_timeout for any single retry
        let delay = delay.min(max_single_delay);

        debug!(
            train_root = %train_root,
            condition = ?condition,
            retry = retry_count + 1,
            max_retries = max_retries,
            delay_ms = delay.as_millis(),
            "Scheduling wait retry timer"
        );

        let fires_at = Instant::now() + delay;
        let new_condition = condition.with_incremented_retry();
        self.pending_timers
            .push(PendingTimer::new(fires_at, train_root, new_condition));

        Ok(())
    }

    // ─── Timer Management ─────────────────────────────────────────────────────────

    /// Schedules a timer for non-blocking wait re-evaluation.
    ///
    /// This is called when a cascade step returns `WaitingOnCi`, indicating
    /// that the train needs to wait for CI completion. Instead of blocking or
    /// relying solely on webhooks/poll interval, we schedule a timer to recheck
    /// after `recheck_interval` (default 5 seconds).
    ///
    /// The timer infrastructure:
    /// - Timers are stored in `pending_timers`
    /// - The event loop checks for expired timers via `fire_expired_timers()`
    /// - When fired, `handle_timer_fired()` refetches PR state and re-evaluates
    ///
    /// # Persistence
    ///
    /// Wait conditions are persisted via the PhaseTransition event when
    /// entering WaitingCi state. This method also updates the in-memory train
    /// record with the wait condition for consistency.
    pub fn schedule_wait_timer(&mut self, train_root: PrNumber, condition: WaitCondition) {
        // Update in-memory train record with the wait condition.
        // This keeps the in-memory state consistent with what we're waiting for.
        if let Some(train) = self.state.active_trains.get_mut(&train_root) {
            use crate::types::PersistedWaitCondition;
            let persisted = match &condition {
                WaitCondition::HeadRefOid {
                    pr,
                    expected,
                    retry_count,
                } => PersistedWaitCondition::HeadRefOid {
                    pr: *pr,
                    expected: expected.clone(),
                    retry_count: *retry_count,
                },
                WaitCondition::MergeStateStatus {
                    pr,
                    not,
                    retry_count,
                } => PersistedWaitCondition::MergeStateStatus {
                    pr: *pr,
                    not: *not,
                    retry_count: *retry_count,
                },
                WaitCondition::CheckSuiteCompleted { sha, retry_count } => {
                    PersistedWaitCondition::CheckSuiteCompleted {
                        sha: sha.clone(),
                        retry_count: *retry_count,
                    }
                }
            };
            train.wait_condition = Some(persisted);
        }

        self.schedule_wait_timer_without_persist(train_root, condition);
    }

    /// Schedules a wait timer without updating the in-memory wait_condition.
    ///
    /// Use this when the wait_condition has already been set (e.g., via event
    /// replay or when it was just persisted in a PhaseTransition event).
    fn schedule_wait_timer_without_persist(
        &mut self,
        train_root: PrNumber,
        condition: WaitCondition,
    ) {
        let delay = self.poll_config.recheck_interval;
        let fires_at = Instant::now() + delay;

        debug!(
            train_root = %train_root,
            delay_ms = delay.as_millis(),
            "Scheduling wait timer"
        );

        self.pending_timers
            .push(PendingTimer::new(fires_at, train_root, condition));
    }

    /// Returns time until the next timer fires, if any.
    fn time_until_next_timer(&self) -> Option<Duration> {
        self.pending_timers
            .iter()
            .map(|t| t.fires_at)
            .min()
            .map(|fires_at| {
                let now = Instant::now();
                if fires_at > now {
                    fires_at - now
                } else {
                    Duration::ZERO
                }
            })
    }

    /// Fires all expired timers.
    ///
    /// Uses the provided shutdown token so global shutdown can cancel timer effect execution.
    async fn fire_expired_timers(&mut self, shutdown: &CancellationToken) -> Result<()> {
        let now = Instant::now();

        // Extract expired timers
        let expired: Vec<_> = self
            .pending_timers
            .iter()
            .filter(|t| t.fires_at <= now)
            .map(|t| (t.train_root, t.condition.clone()))
            .collect();

        // Remove expired timers
        self.pending_timers.retain(|t| t.fires_at > now);

        // Fire each expired timer
        for (train_root, condition) in expired {
            self.handle_timer_fired(train_root, condition, shutdown)
                .await?;
        }

        Ok(())
    }

    /// Returns time until next poll.
    pub(crate) fn time_until_next_poll(&self) -> Option<Duration> {
        // Only poll if we have active trains
        if self.state.active_trains.is_empty() {
            return None;
        }

        let interval = self
            .poll_config
            .poll_interval_with_jitter(&self.config.repo);

        match self.last_poll {
            Some(last) => {
                let elapsed = last.elapsed();
                if elapsed >= interval {
                    Some(Duration::ZERO)
                } else {
                    Some(interval - elapsed)
                }
            }
            None => Some(Duration::ZERO),
        }
    }

    /// Returns true if it's time to poll.
    fn should_poll(&self) -> bool {
        if self.state.active_trains.is_empty() {
            return false;
        }

        let interval = self
            .poll_config
            .poll_interval_with_jitter(&self.config.repo);

        match self.last_poll {
            Some(last) => last.elapsed() >= interval,
            None => true,
        }
    }

    // ─── Spool Drain Timer Management ─────────────────────────────────────────────

    /// Interval for periodic spool draining (independent of active train polling).
    const SPOOL_DRAIN_INTERVAL: Duration = Duration::from_secs(60);

    /// Returns time until next spool drain.
    ///
    /// This runs independently of active trains to catch deliveries that were
    /// spooled but not dispatched (e.g., due to dispatcher failure or race conditions).
    fn time_until_next_spool_drain(&self) -> Option<Duration> {
        let interval = Self::SPOOL_DRAIN_INTERVAL;

        match self.last_spool_drain {
            Some(last) => {
                let elapsed = last.elapsed();
                if elapsed >= interval {
                    Some(Duration::ZERO)
                } else {
                    Some(interval - elapsed)
                }
            }
            // On first call, delay briefly to avoid immediate drain (startup already drained)
            None => Some(interval),
        }
    }

    /// Returns true if it's time to drain the spool.
    fn should_drain_spool(&self) -> bool {
        match self.last_spool_drain {
            Some(last) => last.elapsed() >= Self::SPOOL_DRAIN_INTERVAL,
            // On first call, don't drain immediately (startup already drained)
            None => false,
        }
    }

    /// Handles periodic spool draining.
    ///
    /// This runs independently of active trains to catch deliveries that were
    /// spooled but not dispatched. Unlike `drain_spool_periodic` (called during
    /// poll), this method runs even when there are no active trains.
    pub(crate) async fn handle_periodic_spool_drain(
        &mut self,
        shutdown: &CancellationToken,
    ) -> Result<()> {
        self.last_spool_drain = Some(Instant::now());

        let deliveries = drain_pending(&self.config.spool_dir)?;
        if deliveries.is_empty() {
            trace!("No pending deliveries during periodic drain");
            return Ok(());
        }

        debug!(
            count = deliveries.len(),
            "Draining spool during periodic drain"
        );

        let mut enqueued = 0;
        for delivery in deliveries {
            if self.enqueue(delivery).is_ok() {
                enqueued += 1;
            }
        }

        if enqueued > 0 {
            // Process newly enqueued deliveries
            let (processed, effects) = self.process_all()?;
            debug!(
                enqueued = enqueued,
                processed = processed,
                effects_count = effects.len(),
                "Processed deliveries from periodic spool drain"
            );

            // Execute effects
            if !effects.is_empty() {
                self.execute_effects(effects, shutdown).await?;
            }

            // Re-evaluate active cascades after processing recovered deliveries.
            // This ensures that missed webhooks (e.g., PR merged, CI passed) caught
            // during periodic spool drain advance trains without waiting for a later poll.
            self.re_evaluate_cascades_with_persistence(shutdown, "spool_drain")
                .await?;
        }

        Ok(())
    }

    // ─── Cancellation Token Management ────────────────────────────────────────────

    /// Gets or creates a cancellation token for a stack.
    ///
    /// Tokens are automatically created when trains start (in `apply_handler_result`)
    /// and when loading existing trains from snapshots (in `new`). This method is
    /// useful for tests and for getting a token reference when one is expected to exist.
    pub fn get_or_create_stack_token(&mut self, train_root: PrNumber) -> CancellationToken {
        self.stack_tokens.entry(train_root).or_default().clone()
    }

    /// Finds the train root for a PR.
    ///
    /// Checks if the PR is:
    /// 1. A train root itself
    /// 2. The current PR being processed
    /// 3. In the frozen_descendants of an active train
    pub(crate) fn find_train_root(&self, pr: PrNumber) -> Option<PrNumber> {
        // Check if PR is itself a train root
        if self.state.active_trains.contains_key(&pr) {
            return Some(pr);
        }

        // Search active trains to find one containing this PR
        for (root, train) in &self.state.active_trains {
            // Check if it's the current PR being processed
            if train.current_pr == pr {
                return Some(*root);
            }

            // Check if PR is in the frozen_descendants of this train
            if let Some(progress) = train.cascade_phase.progress()
                && progress.frozen_descendants.contains(&pr)
            {
                return Some(*root);
            }
        }

        None
    }

    // ─── Internal Helpers ─────────────────────────────────────────────────────────

    /// Applies handler result to state and event log.
    ///
    /// Returns `true` if any event was critical (requires immediate fsync).
    fn apply_handler_result(&mut self, result: &HandlerResult) -> Result<bool> {
        use crate::persistence::event::StateEventPayload;

        let mut has_critical = false;

        for payload in &result.state_events {
            // Create cancellation token when a train starts.
            // This allows stop commands to interrupt in-flight operations for this train.
            if let StateEventPayload::TrainStarted { root_pr, .. } = payload {
                debug!(root_pr = %root_pr, "Creating cancellation token for new train");
                // Insert a fresh token for this train (or get existing if somehow already present)
                self.stack_tokens.entry(*root_pr).or_default();
            }

            // Before applying train termination events, cancel and clean up resources.
            // This must happen BEFORE we apply to state so we can still find the train.
            match payload {
                StateEventPayload::TrainStopped { root_pr }
                | StateEventPayload::TrainAborted { root_pr, .. }
                | StateEventPayload::TrainCompleted { root_pr } => {
                    // Cancel in-flight operations for this train (idempotent if already cancelled)
                    if let Some(token) = self.stack_tokens.get(root_pr) {
                        debug!(
                            root_pr = %root_pr,
                            "Cleaning up token for terminated train"
                        );
                        token.cancel();
                    }
                    // Remove the token - train is done
                    self.stack_tokens.remove(root_pr);
                    // Remove any pending timers for this train
                    self.pending_timers.retain(|t| &t.train_root != root_pr);
                }
                _ => {}
            }

            // Append to event log
            let event = self.event_log.append(payload.clone())?;

            // Check if critical
            if event.is_critical() {
                has_critical = true;
            }

            // Apply to state
            apply_event_to_state(&mut self.state, &event);
        }

        Ok(has_critical)
    }

    // ─── Poll Scan Helper Functions ──────────────────────────────────────────────

    /// Finds late additions: PRs whose predecessor is merged but haven't been retargeted.
    ///
    /// Per DESIGN.md: "scan for PRs whose predecessor is merged but whose
    /// base_ref hasn't been retargeted"
    ///
    /// Returns Vec<(pr_number, predecessor_number)>.
    pub fn find_late_additions(&self) -> Vec<(PrNumber, PrNumber)> {
        self.state
            .prs
            .values()
            .filter(|pr| {
                // Must be open
                if !pr.state.is_open() {
                    return false;
                }
                // Must not already target default branch
                if pr.base_ref == self.state.default_branch {
                    return false;
                }
                // Must have a predecessor
                let Some(pred_number) = pr.predecessor else {
                    return false;
                };
                // Predecessor must be merged
                if let Some(pred) = self.state.prs.get(&pred_number) {
                    matches!(pred.state, PrState::Merged { .. })
                } else {
                    false
                }
            })
            .map(|pr| (pr.number, pr.predecessor.unwrap()))
            .collect()
    }

    /// Finds manually retargeted PRs: PRs that target default branch but weren't reconciled.
    ///
    /// Per DESIGN.md: "scan catches PRs that were manually retargeted before
    /// reconciliation could run"
    ///
    /// These are DANGEROUS: the ours-merge was never done, so merging them
    /// would lose the predecessor's changes.
    ///
    /// Returns Vec<(pr_number, predecessor_number)>.
    pub fn find_manually_retargeted(&self) -> Vec<(PrNumber, PrNumber)> {
        self.state
            .prs
            .values()
            .filter(|pr| {
                // Must be open
                if !pr.state.is_open() {
                    return false;
                }
                // Must already target default branch (manually retargeted)
                if pr.base_ref != self.state.default_branch {
                    return false;
                }
                // Must have a predecessor
                let Some(pred_number) = pr.predecessor else {
                    return false;
                };
                // Predecessor must be merged
                let pred_merged = if let Some(pred) = self.state.prs.get(&pred_number) {
                    matches!(pred.state, PrState::Merged { .. })
                } else {
                    false
                };
                if !pred_merged {
                    return false;
                }
                // Must NOT have been reconciled (this is the dangerous case)
                pr.predecessor_squash_reconciled.is_none()
            })
            .map(|pr| (pr.number, pr.predecessor.unwrap()))
            .collect()
    }

    /// Cancels operations for a stack by root PR.
    ///
    /// This cancels the token and removes any pending timers. The token itself
    /// is NOT removed here - it remains in `stack_tokens` so that subsequent
    /// effects for this train will see `is_cancelled()` and skip execution.
    /// The token is removed later when `TrainStopped` is processed in
    /// `apply_handler_result`.
    pub fn cancel_stack(&mut self, root_pr: PrNumber) {
        if let Some(token) = self.stack_tokens.get(&root_pr) {
            info!(root_pr = %root_pr, "Cancelling stack operations");
            token.cancel();
        }
        // Note: Do NOT remove the token here. It must remain so that
        // subsequent effects can check is_cancelled(). The token is
        // removed when TrainStopped/TrainCompleted/TrainAborted is processed.
        self.pending_timers.retain(|t| t.train_root != root_pr);
    }
}

/// Finds the latest snapshot file in a directory using the generation-based
/// compaction scheme.
///
/// This function properly handles crash recovery by:
/// 1. Calling `cleanup_stale_generations` to ensure consistency (handles crashes
///    during compaction where the generation file may be stale)
/// 2. Reading the generation file to get the authoritative generation number
/// 3. Returning the snapshot path for that generation
///
/// This is the correct way to find the snapshot, as opposed to just picking
/// the highest-numbered snapshot file (which could pick the wrong generation
/// after a compaction crash).
pub(crate) fn find_latest_snapshot(state_dir: &Path) -> Result<Option<PathBuf>> {
    use crate::persistence::{cleanup_stale_generations, read_generation, snapshot_path};

    if !state_dir.exists() {
        return Ok(None);
    }

    // First, clean up any stale generation files from interrupted compaction.
    // This ensures the generation file is consistent with existing snapshots.
    if let Err(e) = cleanup_stale_generations(state_dir) {
        warn!(
            state_dir = %state_dir.display(),
            error = %e,
            "Failed to cleanup stale generations, falling back to scan"
        );
        // Fall back to scanning for highest snapshot if cleanup fails
        return find_latest_snapshot_by_scan(state_dir);
    }

    // Read the authoritative generation from the generation file.
    let generation = match read_generation(state_dir) {
        Ok(g) => g,
        Err(e) => {
            warn!(
                state_dir = %state_dir.display(),
                error = %e,
                "Failed to read generation file, falling back to scan"
            );
            return find_latest_snapshot_by_scan(state_dir);
        }
    };

    // Check if the snapshot for this generation exists
    let path = snapshot_path(state_dir, generation);
    if path.exists() {
        Ok(Some(path))
    } else if generation == 0 {
        // Generation 0 with no snapshot means fresh state
        Ok(None)
    } else {
        // Snapshot should exist for non-zero generation - fall back to scan
        warn!(
            state_dir = %state_dir.display(),
            generation = generation,
            "Snapshot missing for generation, falling back to scan"
        );
        find_latest_snapshot_by_scan(state_dir)
    }
}

/// Fallback: finds the latest snapshot by scanning the directory.
///
/// This is used when the generation file is unavailable or inconsistent.
/// It picks the highest-numbered snapshot file.
pub(crate) fn find_latest_snapshot_by_scan(state_dir: &Path) -> Result<Option<PathBuf>> {
    if !state_dir.exists() {
        return Ok(None);
    }

    let mut latest: Option<(u64, PathBuf)> = None;

    for entry in std::fs::read_dir(state_dir)? {
        let entry = entry?;
        let path = entry.path();

        if let Some(generation) = parse_snapshot_generation(&path)
            && (latest.is_none() || generation > latest.as_ref().unwrap().0)
        {
            latest = Some((generation, path));
        }
    }

    Ok(latest.map(|(_, path)| path))
}

/// Parses the generation number from a snapshot filename.
///
/// Expected format: `snapshot.<gen>.json`
pub(crate) fn parse_snapshot_generation(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    if !name.starts_with("snapshot.") || !name.ends_with(".json") {
        return None;
    }

    // Check that there's content between "snapshot." and ".json"
    let prefix_len = "snapshot.".len();
    let suffix_len = ".json".len();
    if name.len() <= prefix_len + suffix_len {
        return None;
    }

    let middle = &name[prefix_len..name.len() - suffix_len];
    middle.parse().ok()
}

/// Parses a spooled delivery and classifies its priority.
///
/// Returns `None` if the event type is not recognized or not relevant.
fn parse_and_classify(
    delivery: &SpooledDelivery,
    bot_name: &str,
) -> Result<Option<(GitHubEvent, EventPriority)>> {
    // Read the webhook envelope
    let envelope = delivery.read_webhook()?;

    // Serialize body to bytes for parsing
    let body_bytes = serde_json::to_vec(&envelope.body)
        .map_err(|e| WorkerError::Parse(crate::webhooks::parser::ParseError::JsonError(e)))?;

    // Parse the event
    let event = match parse_webhook(&envelope.event_type, &body_bytes) {
        Ok(Some(event)) => event,
        Ok(None) => return Ok(None), // Unknown/irrelevant event type
        Err(e) => {
            warn!(
                delivery_id = %delivery.delivery_id,
                event_type = %envelope.event_type,
                error = %e,
                "Failed to parse webhook"
            );
            return Err(e.into());
        }
    };

    // Classify priority
    let priority = classify_priority_with_bot_name(&event, bot_name);

    Ok(Some((event, priority)))
}

/// Applies a state event to the snapshot.
///
/// This updates the in-memory state based on the event payload.
pub(crate) fn apply_event_to_state(state: &mut PersistedRepoSnapshot, event: &StateEvent) {
    use crate::persistence::event::StateEventPayload;
    use crate::types::{CachedPr, MergeStateStatus, PrState};

    match &event.payload {
        StateEventPayload::TrainStarted {
            root_pr,
            current_pr,
        } => {
            let train = crate::types::TrainRecord::new(*root_pr);
            state.active_trains.insert(*root_pr, train);
            trace!(root_pr = %root_pr, current_pr = %current_pr, "Train started");
        }

        StateEventPayload::TrainStopped { root_pr } => {
            // Update train state to Stopped. Unlike completed trains, stopped trains
            // remain in active_trains so that:
            // 1. The stopped state is persisted in snapshots for recovery
            // 2. The status comment can reflect the stopped state
            // 3. The system knows a restart (`@merge-train start`) is needed
            //
            // Per DESIGN.md: stopped trains "require explicit restart" and the status
            // comment should "reflect `stopped` state (required for recovery consistency)".
            if let Some(train) = state.active_trains.get_mut(root_pr) {
                train.stop();
            }
            // Note: Do NOT remove stopped trains from active_trains.
            // They remain until explicitly restarted via `@merge-train start`.
            trace!(root_pr = %root_pr, "Train stopped");
        }

        StateEventPayload::TrainCompleted { root_pr } => {
            state.active_trains.remove(root_pr);
            trace!(root_pr = %root_pr, "Train completed");
        }

        StateEventPayload::TrainAborted { root_pr, error } => {
            // Update train state to aborted, including state, ended_at, and error
            if let Some(train) = state.active_trains.get_mut(root_pr) {
                train.abort(error.clone());
            }
            trace!(root_pr = %root_pr, error = ?error, "Train aborted");
        }

        StateEventPayload::PhaseTransition {
            train_root,
            current_pr,
            predecessor_pr: _,
            last_squash_sha,
            phase,
            state: train_state,
            wait_condition,
        } => {
            if let Some(train) = state.active_trains.get_mut(train_root) {
                train.current_pr = *current_pr;
                train.last_squash_sha = last_squash_sha.clone();
                train.cascade_phase = phase.clone();
                // Restore train state if present (backwards compat: defaults to Running)
                if let Some(ts) = train_state {
                    train.state = *ts;
                }
                // Restore wait condition for timer recovery
                train.wait_condition = wait_condition.clone();
            }
            trace!(
                train_root = %train_root,
                current_pr = %current_pr,
                phase = ?phase,
                train_state = ?train_state,
                "Phase transition"
            );
        }

        StateEventPayload::SquashCommitted {
            train_root,
            pr,
            sha,
        } => {
            if let Some(train) = state.active_trains.get_mut(train_root) {
                train.last_squash_sha = Some(sha.clone());
            }
            trace!(train_root = %train_root, pr = %pr, sha = %sha, "Squash committed");
        }

        StateEventPayload::PrMerged { pr, merge_sha } => {
            if let Some(cached_pr) = state.prs.get_mut(pr) {
                cached_pr.state = PrState::Merged {
                    merge_commit_sha: merge_sha.clone(),
                };
            }
            trace!(pr = %pr, merge_sha = %merge_sha, "PR merged");
        }

        StateEventPayload::PrStateChanged {
            pr,
            state: new_state,
        } => {
            trace!(pr = %pr, state = %new_state, "PR state changed");
            // The state field is a string representation; we'd need more info to update properly
        }

        StateEventPayload::PredecessorDeclared {
            pr,
            predecessor,
            comment_id,
        } => {
            if let Some(cached_pr) = state.prs.get_mut(pr) {
                cached_pr.predecessor = Some(*predecessor);
                cached_pr.predecessor_comment_id = Some(*comment_id);
            }
            trace!(pr = %pr, predecessor = %predecessor, "Predecessor declared");
        }

        StateEventPayload::PredecessorRemoved { pr, comment_id: _ } => {
            if let Some(cached_pr) = state.prs.get_mut(pr) {
                cached_pr.predecessor = None;
                cached_pr.predecessor_comment_id = None;
            }
            trace!(pr = %pr, "Predecessor removed");
        }

        StateEventPayload::PrOpened {
            pr,
            head_sha,
            head_ref,
            base_ref,
            is_draft,
        } => {
            let cached = CachedPr::new(
                *pr,
                head_sha.clone(),
                head_ref.clone(),
                base_ref.clone(),
                None,
                PrState::Open,
                if *is_draft {
                    MergeStateStatus::Draft
                } else {
                    MergeStateStatus::Unknown
                },
                *is_draft,
            );
            state.prs.insert(*pr, cached);
            trace!(pr = %pr, "PR opened");
        }

        StateEventPayload::PrClosed { pr } => {
            if let Some(cached_pr) = state.prs.get_mut(pr) {
                cached_pr.state = PrState::Closed;
            }
            trace!(pr = %pr, "PR closed");
        }

        StateEventPayload::PrReopened { pr } => {
            if let Some(cached_pr) = state.prs.get_mut(pr) {
                cached_pr.state = PrState::Open;
            }
            trace!(pr = %pr, "PR reopened");
        }

        StateEventPayload::PrBaseChanged {
            pr,
            old_base: _,
            new_base,
        } => {
            if let Some(cached_pr) = state.prs.get_mut(pr) {
                cached_pr.base_ref = new_base.clone();
            }
            trace!(pr = %pr, new_base = %new_base, "PR base changed");
        }

        StateEventPayload::PrSynchronized { pr, new_head_sha } => {
            if let Some(cached_pr) = state.prs.get_mut(pr) {
                cached_pr.head_sha = new_head_sha.clone();
            }
            trace!(pr = %pr, new_head_sha = %new_head_sha, "PR synchronized");
        }

        StateEventPayload::PrConvertedToDraft { pr } => {
            if let Some(cached_pr) = state.prs.get_mut(pr) {
                cached_pr.is_draft = true;
                cached_pr.merge_state_status = MergeStateStatus::Draft;
            }
            trace!(pr = %pr, "PR converted to draft");
        }

        StateEventPayload::PrReadyForReview { pr } => {
            if let Some(cached_pr) = state.prs.get_mut(pr) {
                cached_pr.is_draft = false;
                // Don't change merge_state_status - it will be updated by GitHub event
            }
            trace!(pr = %pr, "PR ready for review");
        }

        StateEventPayload::FanOutCompleted {
            old_root,
            new_roots,
            original_root_pr: _,
        } => {
            state.active_trains.remove(old_root);
            for new_root in new_roots {
                let train = crate::types::TrainRecord::new(*new_root);
                state.active_trains.insert(*new_root, train);
            }
            trace!(old_root = %old_root, new_roots = ?new_roots, "Fan-out completed");
        }

        // Intent/done events don't update state (they're for crash recovery)
        StateEventPayload::IntentPushPrep { .. }
        | StateEventPayload::DonePushPrep { .. }
        | StateEventPayload::IntentSquash { .. }
        | StateEventPayload::IntentPushReconcile { .. }
        | StateEventPayload::DonePushReconcile { .. }
        | StateEventPayload::IntentPushCatchup { .. }
        | StateEventPayload::DonePushCatchup { .. }
        | StateEventPayload::IntentRetarget { .. }
        | StateEventPayload::DoneRetarget { .. } => {
            // These events are for crash recovery tracking, not state updates
        }

        // Descendant skipped is informational
        StateEventPayload::DescendantSkipped {
            root_pr,
            descendant_pr,
            reason,
        } => {
            trace!(
                root_pr = %root_pr,
                descendant_pr = %descendant_pr,
                reason = %reason,
                "Descendant skipped"
            );
        }

        // CI/Review events update merge state (simplified for now)
        StateEventPayload::CheckSuiteCompleted { sha, conclusion } => {
            trace!(sha = %sha, conclusion = %conclusion, "Check suite completed");
            // Would need to find PRs with this SHA and update merge_state_status
        }

        StateEventPayload::StatusReceived {
            sha,
            context,
            state: status_state,
        } => {
            trace!(sha = %sha, context = %context, state = %status_state, "Status received");
        }

        StateEventPayload::ReviewSubmitted {
            pr,
            reviewer,
            state: review_state,
        } => {
            trace!(pr = %pr, reviewer = %reviewer, state = %review_state, "Review submitted");
        }

        StateEventPayload::ReviewDismissed { pr, reviewer } => {
            trace!(pr = %pr, reviewer = %reviewer, "Review dismissed");
        }
    }
}

/// Core spec tests for worker behavior.
///
/// These tests verify the fundamental invariants and behaviors of the worker.
/// Edge-case and regression tests are in the separate `tests` module.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::spool::delivery::{WebhookEnvelope, spool_webhook};
    use crate::webhooks::priority::EventPriority;
    use std::collections::HashMap;
    use tempfile::tempdir;

    pub(crate) fn make_pr_opened_envelope(pr_number: u64) -> WebhookEnvelope {
        let body = serde_json::json!({
            "action": "opened",
            "number": pr_number,
            "pull_request": {
                "number": pr_number,
                "head": {
                    "sha": "a".repeat(40),
                    "ref": "feature-branch"
                },
                "base": {
                    "sha": "b".repeat(40),
                    "ref": "main"
                },
                "draft": false,
                "merged": false,
                "user": {
                    "id": 12345,
                    "login": "testuser"
                }
            },
            "repository": {
                "name": "repo",
                "owner": {
                    "login": "owner"
                }
            }
        });

        WebhookEnvelope {
            event_type: "pull_request".to_string(),
            headers: HashMap::new(),
            body,
        }
    }

    pub(crate) fn make_comment_envelope(pr_number: u64, body_text: &str) -> WebhookEnvelope {
        let body = serde_json::json!({
            "action": "created",
            "issue": {
                "number": pr_number,
                "pull_request": {}
            },
            "comment": {
                "id": 98765,
                "body": body_text,
                "user": {
                    "id": 12345,
                    "login": "testuser"
                }
            },
            "repository": {
                "name": "repo",
                "owner": {
                    "login": "owner"
                }
            }
        });

        WebhookEnvelope {
            event_type: "issue_comment".to_string(),
            headers: HashMap::new(),
            body,
        }
    }

    // ─── Core behavior spec tests ───

    #[test]
    fn worker_new_creates_empty_state() {
        let dir = tempdir().unwrap();
        let spool_dir = dir.path().join("spool");
        let state_dir = dir.path().join("state");

        let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);

        let worker = RepoWorker::new(config).unwrap();

        assert!(worker.queue_is_empty());
        assert!(worker.state.prs.is_empty());
        assert!(worker.state.active_trains.is_empty());
    }

    #[test]
    fn worker_drains_spool_on_startup() {
        let dir = tempdir().unwrap();
        let spool_dir = dir.path().join("spool");
        let state_dir = dir.path().join("state");

        // Spool a delivery before creating worker
        let envelope = make_pr_opened_envelope(42);
        let delivery_id = DeliveryId::new("test-delivery-1");
        spool_webhook(&spool_dir, &delivery_id, &envelope).unwrap();

        let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);

        let worker = RepoWorker::new(config).unwrap();

        // Queue should have the pending delivery
        assert_eq!(worker.queue_len(), 1);
    }

    #[test]
    fn worker_processes_event() {
        let dir = tempdir().unwrap();
        let spool_dir = dir.path().join("spool");
        let state_dir = dir.path().join("state");

        // Spool a PR opened event
        let envelope = make_pr_opened_envelope(42);
        let delivery_id = DeliveryId::new("test-delivery-1");
        spool_webhook(&spool_dir, &delivery_id, &envelope).unwrap();

        let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);

        let mut worker = RepoWorker::new(config).unwrap();

        // Process the event
        let result = worker.process_next().unwrap();
        assert!(result.processed);

        // Queue should be empty now
        assert!(worker.queue_is_empty());

        // Flush pending batch to create .done markers
        worker.flush_pending_batch().unwrap();

        // Delivery should be marked as done
        let delivery = SpooledDelivery::new(&spool_dir, delivery_id);
        assert!(delivery.is_done());
    }

    #[test]
    fn stop_command_processed_before_normal_events() {
        let dir = tempdir().unwrap();
        let spool_dir = dir.path().join("spool");
        let state_dir = dir.path().join("state");

        // Spool a normal PR event first
        let pr_envelope = make_pr_opened_envelope(42);
        spool_webhook(&spool_dir, &DeliveryId::new("pr-delivery"), &pr_envelope).unwrap();

        // Then spool a stop command (should be processed first)
        let stop_envelope = make_comment_envelope(42, "@merge-train stop");
        spool_webhook(
            &spool_dir,
            &DeliveryId::new("stop-delivery"),
            &stop_envelope,
        )
        .unwrap();

        let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);

        let mut worker = RepoWorker::new(config).unwrap();

        // Verify stop command is first in queue
        assert_eq!(worker.queue_len(), 2);

        // The first event popped should be the stop command (high priority)
        let first = worker.queue.pop().unwrap();
        assert_eq!(first.priority, EventPriority::High);
        assert_eq!(first.delivery_id.as_str(), "stop-delivery");
    }

    #[test]
    fn train_started_creates_cancellation_token() {
        // End-to-end test: TrainStarted event creates a cancellation token
        // that can be cancelled by a subsequent TrainStopped event.
        use crate::persistence::event::StateEventPayload;
        use crate::webhooks::handlers::HandlerResult;

        let dir = tempdir().unwrap();
        let spool_dir = dir.path().join("spool");
        let state_dir = dir.path().join("state");

        let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
        let mut worker = RepoWorker::new(config).unwrap();

        // Verify no token exists initially
        assert!(
            !worker.stack_tokens.contains_key(&PrNumber(42)),
            "Token should not exist before train starts"
        );

        // Simulate processing a TrainStarted event
        let handler_result = HandlerResult {
            state_events: vec![StateEventPayload::TrainStarted {
                root_pr: PrNumber(42),
                current_pr: PrNumber(42),
            }],
            effects: vec![],
        };
        worker.apply_handler_result(&handler_result).unwrap();

        // Verify token was created
        assert!(
            worker.stack_tokens.contains_key(&PrNumber(42)),
            "Token should exist after train starts"
        );
        let token = worker.stack_tokens.get(&PrNumber(42)).unwrap().clone();
        assert!(
            !token.is_cancelled(),
            "Token should not be cancelled after train starts"
        );

        // Simulate processing a TrainStopped event
        let stop_result = HandlerResult {
            state_events: vec![StateEventPayload::TrainStopped {
                root_pr: PrNumber(42),
            }],
            effects: vec![],
        };
        worker.apply_handler_result(&stop_result).unwrap();

        // Verify token was cancelled and removed
        assert!(
            token.is_cancelled(),
            "Token should be cancelled after train stops"
        );
        assert!(
            !worker.stack_tokens.contains_key(&PrNumber(42)),
            "Token should be removed after train stops"
        );
    }
}
