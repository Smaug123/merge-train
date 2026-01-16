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

use std::collections::HashMap;
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
use crate::types::{DeliveryId, PrNumber, RepoId};
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
struct PendingTimer {
    /// When the timer fires.
    fires_at: Instant,
    /// The train this timer is for.
    train_root: PrNumber,
    /// The condition being waited for.
    condition: WaitCondition,
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
    config: WorkerConfig,

    /// Priority queue for pending events.
    queue: EventQueue,

    /// Repository state snapshot.
    state: PersistedRepoSnapshot,

    /// Event log for durability.
    event_log: EventLog,

    /// Set of delivery IDs with pending fsync (for batched writes).
    pending_batch: Vec<DeliveryId>,

    /// Polling configuration.
    poll_config: PollConfig,

    /// Cancellation tokens for active stacks, keyed by root PR.
    stack_tokens: HashMap<PrNumber, CancellationToken>,

    /// Pending timers for non-blocking waits.
    pending_timers: Vec<PendingTimer>,

    /// Time of last poll for active trains.
    last_poll: Option<Instant>,

    /// Optional GitHub client for executing GitHub effects.
    /// If None, effects are logged but not executed (testing/dry-run mode).
    github_client: Option<OctocrabClient>,
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

        // Enqueue pending deliveries
        for delivery in pending {
            if let Some((event, priority)) = parse_and_classify(&delivery, &config.bot_name)? {
                queue.push(event, delivery.delivery_id, priority);
            } else {
                // Event type not recognized or not relevant - mark as done
                mark_done(&delivery)?;
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

        Ok(RepoWorker {
            config,
            queue,
            state,
            event_log,
            pending_batch: Vec::new(),
            poll_config: PollConfig::from_env(),
            stack_tokens,
            pending_timers: Vec::new(),
            last_poll: None,
            github_client: None,
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
    ///
    /// This prevents duplicate processing when:
    /// - A redelivery arrives for an already-processed webhook
    /// - The same delivery is dispatched multiple times (e.g., on restart)
    pub fn enqueue(&mut self, delivery: SpooledDelivery) -> Result<()> {
        // Skip if already done
        if delivery.is_done() {
            trace!(
                delivery_id = %delivery.delivery_id,
                "Skipping already-done delivery"
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

        // Step 3: Call event handler
        let result = handle_event(&queued.event, &self.state, &self.config.bot_name)?;

        // Step 4 & 5: Append state events and apply to state
        let has_critical = self.apply_handler_result(&result)?;

        // Step 6 & 7: Handle fsync based on criticality
        if has_critical {
            // Critical event: fsync immediately and create done marker.
            // We must sync the log even if pending_batch is empty, because
            // this critical event was just appended to the log.
            self.event_log.sync()?;
            self.flush_pending_batch_done_markers()?;
            mark_done(&delivery)?;
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
    /// * `shutdown` - Cancellation token for graceful shutdown
    #[instrument(skip(self, rx, shutdown), fields(repo = %self.config.repo))]
    pub async fn run(
        mut self,
        mut rx: mpsc::Receiver<WorkerMessage>,
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
        }

        // Schedule initial poll with jitter
        let initial_delay = self.poll_config.initial_poll_delay(&self.config.repo);
        self.last_poll = Some(Instant::now() - self.poll_config.poll_interval + initial_delay);

        loop {
            // Calculate time until next poll
            let poll_delay = self.time_until_next_poll();

            // Calculate time until next timer
            let timer_delay = self.time_until_next_timer();

            // Use the minimum of poll and timer delays
            let next_wakeup = match (poll_delay, timer_delay) {
                (Some(p), Some(t)) => Some(p.min(t)),
                (Some(p), None) => Some(p),
                (None, Some(t)) => Some(t),
                (None, None) => None,
            };

            tokio::select! {
                // Graceful shutdown
                _ = shutdown.cancelled() => {
                    info!("Shutdown signal received, stopping worker");
                    break;
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
                                error!(error = %e, "Error handling message");
                            }
                        }
                        None => {
                            // Channel closed, all senders dropped
                            info!("Message channel closed");
                            break;
                        }
                    }
                }

                // Timer wakeup (poll or pending timer)
                _ = async {
                    match next_wakeup {
                        Some(delay) => tokio::time::sleep(delay).await,
                        None => std::future::pending().await,
                    }
                } => {
                    // Check if it's time for a poll
                    if self.should_poll()
                        && let Err(e) = self.handle_poll_active_trains().await
                    {
                        error!(error = %e, "Error during poll");
                    }

                    // Fire any expired timers
                    if let Err(e) = self.fire_expired_timers().await {
                        error!(error = %e, "Error firing timers");
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
            WorkerMessage::CancelStack(pr) => {
                self.handle_cancel_stack(pr);
            }
            WorkerMessage::PollActiveTrains => {
                self.handle_poll_active_trains().await?;
            }
            WorkerMessage::TimerFired {
                train_root,
                condition,
            } => {
                self.handle_timer_fired(train_root, condition).await?;
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
    /// Then processes events synchronously, and executes effects inline.
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
    /// interleaving of event processing. The cancellation token mechanism allows
    /// stop commands to interrupt at effect boundaries.
    ///
    /// # Stage 18 Consideration
    ///
    /// Stage 18 may introduce chunked effect execution (process N effects, then check
    /// messages) for better stop-command responsiveness. This requires careful handling
    /// of partial effect completion.
    async fn handle_delivery(
        &mut self,
        delivery: SpooledDelivery,
        shutdown: &CancellationToken,
    ) -> Result<()> {
        // Before enqueuing, check if this is a stop command.
        // If so, immediately cancel the relevant stack to interrupt in-flight operations.
        self.handle_immediate_cancellation_if_stop(&delivery)?;

        // Enqueue the delivery
        self.enqueue(delivery)?;

        // Process all pending events and collect effects
        let (count, effects) = self.process_all()?;
        trace!(
            processed = count,
            effects = effects.len(),
            "Processed events"
        );

        // Execute effects asynchronously
        if !effects.is_empty() {
            self.execute_effects(effects, shutdown).await?;
        }

        Ok(())
    }

    /// If the delivery is a stop command, immediately cancel the relevant stack.
    ///
    /// This ensures in-flight operations are interrupted before the stop command
    /// is even enqueued, providing faster response to stop requests.
    fn handle_immediate_cancellation_if_stop(&mut self, delivery: &SpooledDelivery) -> Result<()> {
        use crate::commands::{Command, parse_command};
        use crate::webhooks::parser::parse_webhook;

        // Try to load and parse the webhook
        let envelope = match delivery.read_webhook() {
            Ok(env) => env,
            Err(_) => return Ok(()), // Can't load, skip immediate cancellation
        };

        // Parse the webhook event
        let body_bytes = serde_json::to_vec(&envelope.body).unwrap_or_default();
        let event = match parse_webhook(&envelope.event_type, &body_bytes) {
            Ok(Some(ev)) => ev,
            Ok(None) => return Ok(()), // Not a supported event type
            Err(_) => return Ok(()),   // Can't parse, skip immediate cancellation
        };

        // Check if it's an issue comment with a stop command
        if let GitHubEvent::IssueComment(comment) = event
            && let Some(pr_number) = comment.pr_number
            && let Some(cmd) = parse_command(&comment.body, &self.config.bot_name)
            && matches!(cmd, Command::Stop | Command::StopForce)
        {
            // Find the train root for this PR and cancel immediately
            if let Some(train_root) = self.find_train_root(pr_number) {
                info!(
                    train_root = %train_root,
                    pr = %pr_number,
                    "Stop command detected, immediately cancelling stack"
                );
                self.cancel_stack(train_root);
            }
        }

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
    /// # Cancellation
    ///
    /// Two levels of cancellation are checked before each effect:
    /// 1. Global shutdown - stops all effect processing
    /// 2. Stack-scoped cancellation - stops effects for a specific train
    ///
    /// Stack-scoped cancellation allows stop commands to immediately interrupt
    /// in-flight operations for a specific train without affecting other trains.
    /// The stack token is looked up based on the PR number in the effect.
    ///
    /// # Stage 18 Integration
    ///
    /// When Stage 18 provides the GitHub client, this method will:
    /// 1. Create an `EffectExecutor` with the GitHub interpreter
    /// 2. Execute each effect and handle the response
    /// 3. Update cached state based on GitHub responses (e.g., PrRefetched)
    async fn execute_effects(
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
        if let Some(ref github_client) = self.github_client {
            let executor = EffectExecutor::new(github_client.clone(), shutdown.clone());
            self.execute_effects_with_executor(effects, shutdown, &executor)
                .await?;
        } else {
            // No GitHub client configured - use logging interpreter (for testing/dry-run).
            let logging_interpreter = LoggingGitHubInterpreter::new();
            let executor = EffectExecutor::new(logging_interpreter, shutdown.clone());
            self.execute_effects_with_executor(effects, shutdown, &executor)
                .await?;
        }

        Ok(())
    }

    /// Executes effects using the given executor and processes responses.
    async fn execute_effects_with_executor<G>(
        &mut self,
        effects: Vec<Effect>,
        shutdown: &CancellationToken,
        executor: &crate::worker::effects::EffectExecutor<G>,
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
                    if let EffectResult::GitHub(response) = result {
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
    fn process_github_response(&mut self, response: GitHubResponse) {
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

    /// Handles a stack cancellation request.
    fn handle_cancel_stack(&mut self, pr: PrNumber) {
        // Find the train root for this PR
        let train_root = self.find_train_root(pr).unwrap_or(pr);

        // Cancel the token if it exists
        if let Some(token) = self.stack_tokens.get(&train_root) {
            info!(train_root = %train_root, "Cancelling stack operations");
            token.cancel();
        }

        // Remove the token (will be recreated if train restarts)
        self.stack_tokens.remove(&train_root);

        // Remove any pending timers for this train
        self.pending_timers.retain(|t| t.train_root != train_root);
    }

    /// Handles the poll timer for active trains.
    ///
    /// This also drains the spool to pick up any deliveries that may have been
    /// missed (e.g., due to dispatcher failures).
    ///
    /// For each active train, generates a `RefetchPr` effect to check the current
    /// PR's merge state. If the merge state has changed (e.g., CI completed,
    /// review approved), the cascade will be re-evaluated on the next event.
    ///
    /// # Stage 18 Limitation
    ///
    /// Currently, poll fallback does NOT advance trains because:
    /// - Effects are executed via `LoggingGitHubInterpreter` which returns placeholders
    /// - Placeholder responses don't update `self.state.prs` with real GitHub data
    /// - Without real data, the cascade engine can't detect state changes
    ///
    /// This means the bot relies solely on webhooks for cascade progression in Stage 17.
    /// The poll mechanism is infrastructure for Stage 18, which will:
    /// - Use a real GitHub client to execute RefetchPr effects
    /// - Update cached PR state from API responses
    /// - Trigger cascade re-evaluation when state changes are detected
    async fn handle_poll_active_trains(&mut self) -> Result<()> {
        use crate::effects::{Effect, GitHubEffect};

        self.last_poll = Some(Instant::now());

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

        // Now handle active train polling
        let active_trains: Vec<_> = self.state.active_trains.values().collect();

        if active_trains.is_empty() {
            trace!("No active trains to poll");
            return Ok(());
        }

        debug!(
            count = active_trains.len(),
            "Polling active trains for missed webhooks"
        );

        // Generate RefetchPr effects for each active train's current PR.
        // This checks the GitHub state in case we missed webhooks.
        let mut effects = Vec::new();
        for train in active_trains {
            let current_pr = train.current_pr;
            trace!(
                train_root = %train.original_root_pr,
                current_pr = %current_pr,
                trigger = "poll",
                "Generating RefetchPr effect for active train"
            );
            effects.push(Effect::GitHub(GitHubEffect::RefetchPr { pr: current_pr }));
        }

        // Execute effects (requires GitHub interpreter from Stage 18).
        // For now, effects are logged but not executed against GitHub.
        // The RefetchPr responses would be processed by the event handlers
        // to update cached state and trigger cascade re-evaluation.
        if !effects.is_empty() {
            self.execute_effects(effects, &shutdown).await?;
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
    async fn drain_spool_periodic(&mut self, shutdown: &CancellationToken) -> Result<usize> {
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
    /// then checks if the wait condition is satisfied. If not satisfied,
    /// reschedules the timer with exponential backoff.
    async fn handle_timer_fired(
        &mut self,
        train_root: PrNumber,
        condition: WaitCondition,
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

        debug!(
            train_root = %train_root,
            current_pr = %train.current_pr,
            condition = ?condition,
            "Re-evaluating wait condition"
        );

        // Generate a RefetchPr effect to get the latest state from GitHub.
        // The wait condition typically involves checking if a previous operation
        // (push, merge) has propagated to GitHub's API.
        let effects = vec![Effect::GitHub(GitHubEffect::RefetchPr {
            pr: train.current_pr,
        })];

        // Execute the effect (requires GitHub interpreter from Stage 18).
        // The response would update cached PR state and allow us to check the condition.
        let shutdown = CancellationToken::new();
        if let Err(e) = self.execute_effects(effects, &shutdown).await {
            warn!(
                train_root = %train_root,
                error = %e,
                "Failed to execute effects for timer re-evaluation"
            );
        }

        // Check if the wait condition is now satisfied based on updated state.
        //
        // TODO(Stage 18): Process RefetchPr response and check condition:
        // - HeadShaMatches: Compare PR headRefOid with expected SHA
        // - MergeCompleted: Check if PR state is Merged
        // - CiPassing: Check mergeStateStatus == Clean
        //
        // For now, the condition cannot be checked without GitHub responses.
        // In practice, webhooks should arrive to trigger cascade progression.
        // The timer is a fallback mechanism in case webhooks are missed.
        trace!(
            train_root = %train_root,
            condition = ?condition,
            "Condition check requires GitHub interpreter (Stage 18)"
        );

        // Reschedule timer for retry (with backoff) if condition not satisfied.
        // Without Stage 18 GitHub integration, we can't verify the condition,
        // so we reschedule to continue polling.
        self.schedule_wait_retry(train_root, condition)?;

        Ok(())
    }

    /// Schedules a retry timer for a wait condition with exponential backoff.
    ///
    /// Returns an error if max retries have been exceeded.
    fn schedule_wait_retry(
        &mut self,
        train_root: PrNumber,
        condition: WaitCondition,
    ) -> Result<()> {
        // Find the retry count from the condition's metadata
        let retry_count = condition.retry_count();

        // Max retries before giving up (10 retries with backoff = ~17 minutes total)
        const MAX_RETRIES: u32 = 10;
        if retry_count >= MAX_RETRIES {
            warn!(
                train_root = %train_root,
                condition = ?condition,
                retry_count = retry_count,
                "Wait condition timed out after max retries"
            );
            // For now, just log the timeout. Stage 18 will handle cascade failure.
            // TODO(Stage 18): Transition cascade to Failed state with timeout error
            return Ok(());
        }

        // Calculate backoff delay: starts at recheck_interval, caps at 5 minutes
        let base_delay = self.poll_config.recheck_interval;
        let backoff_factor = 2u32.saturating_pow(retry_count);
        let delay = base_delay * backoff_factor;
        let max_delay = Duration::from_secs(300); // 5 minutes
        let delay = delay.min(max_delay);

        debug!(
            train_root = %train_root,
            condition = ?condition,
            retry = retry_count + 1,
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
    /// # Stage 18 Work Required
    ///
    /// This function is currently dead code because the non-blocking polling
    /// infrastructure is not yet integrated:
    ///
    /// 1. **No callers**: The cascade engine doesn't yet emit "wait for condition"
    ///    effects that would trigger timer scheduling. Currently, the bot relies
    ///    solely on webhooks to drive cascade progression.
    ///
    /// 2. **In-memory only**: Wait conditions are stored in `pending_timers` which
    ///    is not persisted. On crash recovery, all pending waits would be lost.
    ///    Stage 18 should either:
    ///    - Persist wait conditions in `TrainState` (adds complexity)
    ///    - OR rely on the periodic poll fallback to recover (simpler but slower)
    ///
    /// 3. **No event loop integration**: The main event loop in `run()` doesn't
    ///    check for timer expiration. `fire_expired_timers()` exists but isn't
    ///    called from the event loop.
    ///
    /// For Stage 17, the periodic poll (`handle_poll_active_trains`) serves as a
    /// coarse-grained fallback that re-checks all active trains on a timer.
    /// Stage 18 should wire up the more precise per-condition timers.
    #[allow(dead_code)]
    pub fn schedule_wait_timer(&mut self, train_root: PrNumber, condition: WaitCondition) {
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
    async fn fire_expired_timers(&mut self) -> Result<()> {
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
            self.handle_timer_fired(train_root, condition).await?;
        }

        Ok(())
    }

    /// Returns time until next poll.
    fn time_until_next_poll(&self) -> Option<Duration> {
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
    fn find_train_root(&self, pr: PrNumber) -> Option<PrNumber> {
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

            // Before applying TrainStopped/TrainAborted, cancel any in-flight operations.
            // This must happen BEFORE we apply to state so we can still find the train.
            match payload {
                StateEventPayload::TrainStopped { root_pr }
                | StateEventPayload::TrainAborted { root_pr, .. } => {
                    // Cancel in-flight operations for this train
                    if let Some(token) = self.stack_tokens.get(root_pr) {
                        debug!(
                            root_pr = %root_pr,
                            "Cancelling in-flight operations for stopped/aborted train"
                        );
                        token.cancel();
                    }
                    // Remove the token
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

    /// Cancels operations for a stack by root PR.
    ///
    /// This cancels the token and removes any pending timers.
    pub fn cancel_stack(&mut self, root_pr: PrNumber) {
        if let Some(token) = self.stack_tokens.get(&root_pr) {
            info!(root_pr = %root_pr, "Cancelling stack operations");
            token.cancel();
        }
        self.stack_tokens.remove(&root_pr);
        self.pending_timers.retain(|t| t.train_root != root_pr);
    }
}

/// Finds the latest snapshot file in a directory.
///
/// Looks for files matching `snapshot.<gen>.json` and returns the path
/// with the highest generation number.
fn find_latest_snapshot(state_dir: &Path) -> Result<Option<PathBuf>> {
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
fn parse_snapshot_generation(path: &Path) -> Option<u64> {
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
fn apply_event_to_state(state: &mut PersistedRepoSnapshot, event: &StateEvent) {
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
            // Update train state before removing so that if this event is
            // replayed during recovery, the state is consistent.
            if let Some(train) = state.active_trains.get_mut(root_pr) {
                train.stop();
            }
            // Stopped trains are removed from active_trains (like completed trains)
            state.active_trains.remove(root_pr);
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
        } => {
            if let Some(train) = state.active_trains.get_mut(train_root) {
                train.current_pr = *current_pr;
                train.last_squash_sha = last_squash_sha.clone();
                train.cascade_phase = phase.clone();
            }
            trace!(
                train_root = %train_root,
                current_pr = %current_pr,
                phase = ?phase,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spool::delivery::{WebhookEnvelope, spool_webhook};
    use crate::webhooks::priority::EventPriority;
    use std::collections::HashMap;
    use tempfile::tempdir;

    fn make_pr_opened_envelope(pr_number: u64) -> WebhookEnvelope {
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

    fn make_comment_envelope(pr_number: u64, body_text: &str) -> WebhookEnvelope {
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

    // ─── Basic worker tests ───

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

    // ─── Priority ordering tests ───

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

    // ─── Snapshot generation parsing tests ───

    #[test]
    fn parse_snapshot_generation_valid() {
        let path = PathBuf::from("snapshot.0.json");
        assert_eq!(parse_snapshot_generation(&path), Some(0));

        let path = PathBuf::from("snapshot.42.json");
        assert_eq!(parse_snapshot_generation(&path), Some(42));

        let path = PathBuf::from("snapshot.12345.json");
        assert_eq!(parse_snapshot_generation(&path), Some(12345));
    }

    #[test]
    fn parse_snapshot_generation_invalid() {
        assert_eq!(
            parse_snapshot_generation(&PathBuf::from("events.0.log")),
            None
        );
        assert_eq!(
            parse_snapshot_generation(&PathBuf::from("snapshot.json")),
            None
        );
        assert_eq!(
            parse_snapshot_generation(&PathBuf::from("snapshot.abc.json")),
            None
        );
        assert_eq!(
            parse_snapshot_generation(&PathBuf::from("other.0.json")),
            None
        );
    }

    #[test]
    fn find_latest_snapshot_returns_highest_generation() {
        let dir = tempdir().unwrap();
        let state_dir = dir.path();

        // Create multiple snapshots
        let snapshot = PersistedRepoSnapshot::new("main");
        save_snapshot_atomic(&state_dir.join("snapshot.0.json"), &snapshot).unwrap();
        save_snapshot_atomic(&state_dir.join("snapshot.5.json"), &snapshot).unwrap();
        save_snapshot_atomic(&state_dir.join("snapshot.2.json"), &snapshot).unwrap();

        let latest = find_latest_snapshot(state_dir).unwrap().unwrap();
        assert!(latest.ends_with("snapshot.5.json"));
    }

    #[test]
    fn find_latest_snapshot_nonexistent_dir() {
        let dir = tempdir().unwrap();
        let state_dir = dir.path().join("nonexistent");

        let result = find_latest_snapshot(&state_dir).unwrap();
        assert!(result.is_none());
    }

    // ─── Stage 17 Integration Tests ───
    //
    // These tests verify the key oracles for the per-repo worker system:
    // - Spool → worker processing
    // - Stop priority ordering
    // - Serial processing within a repo
    // - .done only after fsync
    // - Deduplication of already-done deliveries

    #[test]
    fn done_marker_only_created_after_flush() {
        // Test oracle: .done marker is only created AFTER state is durably persisted.
        // The durability guarantee is:
        // 1. Events are appended to the log
        // 2. flush_pending_batch() calls event_log.sync() to fsync the log
        // 3. Only THEN are .done markers created
        //
        // This ensures that on crash recovery, any delivery without a .done marker
        // will be reprocessed, and its events will be re-applied to the log.
        //
        // # Limitation
        //
        // This test verifies the SEQUENCE of operations (log write → flush → done
        // marker) but does NOT verify actual durability guarantees:
        //
        // - We cannot inject fsync failures to verify the code handles them
        // - We cannot simulate power loss to verify data survives
        // - The test runs on a normal filesystem, not a failure-injecting one
        //
        // Truly testing durability would require either:
        // 1. Fault injection (libfiu, custom VFS, or mocking the fsync syscall)
        // 2. Crash recovery tests (spawn subprocess, kill it, verify state)
        // 3. Hardware-level testing (actual power cycling)
        //
        // These are impractical for standard unit tests. Instead, we rely on:
        // - Code review to verify fsync is called at the right points
        // - The sequence test here to verify the ordering invariant
        // - Integration tests with actual crash scenarios (manual or CI)

        let dir = tempdir().unwrap();
        let spool_dir = dir.path().join("spool");
        let state_dir = dir.path().join("state");

        // Spool an event
        let envelope = make_pr_opened_envelope(42);
        let delivery_id = DeliveryId::new("test-delivery");
        spool_webhook(&spool_dir, &delivery_id, &envelope).unwrap();

        let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
        let mut worker = RepoWorker::new(config).unwrap();

        // Record the initial log position
        let initial_position = worker.event_log.position().unwrap();

        // Process the event
        let result = worker.process_next().unwrap();
        assert!(result.processed, "Event should be processed");

        // After processing: log should have advanced (events written)
        let post_process_position = worker.event_log.position().unwrap();
        assert!(
            post_process_position > initial_position,
            "Event log should have events written after processing"
        );

        // Before flush: .done marker should NOT exist
        let delivery = SpooledDelivery::new(&spool_dir, delivery_id.clone());
        assert!(
            !delivery.is_done(),
            "Done marker should not exist before flush (events written but not synced)"
        );

        // After flush: event_log.sync() is called, THEN .done marker is created
        worker.flush_pending_batch().unwrap();
        assert!(
            delivery.is_done(),
            "Done marker should exist after flush (log synced)"
        );

        // Verify the event log file actually exists and has content
        let log_path = state_dir.join(format!("events.{}.log", worker.state.log_generation));
        assert!(log_path.exists(), "Event log file should exist");
        let log_content = std::fs::read_to_string(&log_path).unwrap();
        assert!(
            !log_content.is_empty(),
            "Event log should contain the persisted event"
        );
        assert!(
            log_content.contains("pr_opened"),
            "Event log should contain the PrOpened event"
        );
    }

    #[test]
    fn enqueue_skips_already_done_deliveries() {
        // Test oracle: Deduplication - already-done deliveries are skipped
        let dir = tempdir().unwrap();
        let spool_dir = dir.path().join("spool");
        let state_dir = dir.path().join("state");

        // Spool and process an event
        let envelope = make_pr_opened_envelope(42);
        let delivery_id = DeliveryId::new("already-processed");
        spool_webhook(&spool_dir, &delivery_id, &envelope).unwrap();

        let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
        let mut worker = RepoWorker::new(config).unwrap();

        // Process and flush
        let (count, _effects) = worker.process_all().unwrap();
        assert_eq!(count, 1);
        assert!(worker.queue_is_empty());

        // Manually construct a delivery for the same ID
        let delivery = SpooledDelivery::new(&spool_dir, delivery_id.clone());
        assert!(delivery.is_done(), "Delivery should be marked done");

        // Try to enqueue it again - should be skipped
        worker.enqueue(delivery).unwrap();
        assert!(
            worker.queue_is_empty(),
            "Already-done delivery should not be enqueued"
        );
    }

    #[test]
    fn enqueue_skips_duplicate_deliveries_in_queue() {
        // Test oracle: Deduplication - deliveries already in queue are skipped
        let dir = tempdir().unwrap();
        let spool_dir = dir.path().join("spool");
        let state_dir = dir.path().join("state");

        // Spool an event
        let envelope = make_pr_opened_envelope(42);
        let delivery_id = DeliveryId::new("test-delivery");
        spool_webhook(&spool_dir, &delivery_id, &envelope).unwrap();

        let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
        let mut worker = RepoWorker::new(config).unwrap();

        // Queue should have 1 item from startup drain
        assert_eq!(worker.queue_len(), 1);

        // Try to enqueue the same delivery again
        let delivery = SpooledDelivery::new(&spool_dir, delivery_id);
        worker.enqueue(delivery).unwrap();

        // Queue should still have only 1 item (duplicate was skipped)
        assert_eq!(
            worker.queue_len(),
            1,
            "Duplicate delivery should be skipped"
        );
    }

    #[test]
    fn multiple_stop_commands_maintain_fifo_order() {
        // Test oracle: Multiple high-priority events maintain FIFO order
        let dir = tempdir().unwrap();
        let spool_dir = dir.path().join("spool");
        let state_dir = dir.path().join("state");

        // Spool multiple stop commands
        for i in 0..3 {
            let envelope = make_comment_envelope(42, "@merge-train stop");
            let delivery_id = DeliveryId::new(format!("stop-{}", i));
            spool_webhook(&spool_dir, &delivery_id, &envelope).unwrap();
        }

        let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
        let mut worker = RepoWorker::new(config).unwrap();

        assert_eq!(worker.queue_len(), 3);

        // All should be high priority and in FIFO order
        let first = worker.queue.pop().unwrap();
        assert_eq!(first.priority, EventPriority::High);
        assert_eq!(first.delivery_id.as_str(), "stop-0");

        let second = worker.queue.pop().unwrap();
        assert_eq!(second.priority, EventPriority::High);
        assert_eq!(second.delivery_id.as_str(), "stop-1");

        let third = worker.queue.pop().unwrap();
        assert_eq!(third.priority, EventPriority::High);
        assert_eq!(third.delivery_id.as_str(), "stop-2");
    }

    #[test]
    fn worker_processes_backlog_serially() {
        // Test oracle: Events are processed serially (one at a time)
        let dir = tempdir().unwrap();
        let spool_dir = dir.path().join("spool");
        let state_dir = dir.path().join("state");

        // Spool multiple events
        for i in 0..5 {
            let envelope = make_pr_opened_envelope(i);
            let delivery_id = DeliveryId::new(format!("delivery-{}", i));
            spool_webhook(&spool_dir, &delivery_id, &envelope).unwrap();
        }

        let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
        let mut worker = RepoWorker::new(config).unwrap();

        assert_eq!(worker.queue_len(), 5);

        // Process one at a time and verify queue decreases
        for remaining in (0..5).rev() {
            let result = worker.process_next().unwrap();
            assert!(result.processed);
            assert_eq!(worker.queue_len(), remaining);
        }

        // Queue should be empty
        assert!(worker.queue_is_empty());

        // All deliveries should be marked done after flush
        worker.flush_pending_batch().unwrap();

        for i in 0..5 {
            let delivery =
                SpooledDelivery::new(&spool_dir, DeliveryId::new(format!("delivery-{}", i)));
            assert!(delivery.is_done(), "Delivery {} should be marked done", i);
        }
    }

    #[test]
    fn process_all_handles_empty_queue() {
        // Test that process_all gracefully handles empty queue
        let dir = tempdir().unwrap();
        let spool_dir = dir.path().join("spool");
        let state_dir = dir.path().join("state");

        let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
        let mut worker = RepoWorker::new(config).unwrap();

        // Queue should be empty
        assert!(worker.queue_is_empty());

        // process_all should return (0, empty effects) without error
        let (count, effects) = worker.process_all().unwrap();
        assert_eq!(count, 0);
        assert!(effects.is_empty());
    }

    #[test]
    fn queue_push_returns_false_for_duplicates() {
        // Test the EventQueue deduplication at the queue level
        use crate::types::Sha;
        use crate::webhooks::events::{GitHubEvent, PrAction, PullRequestEvent};

        let mut queue = super::super::queue::EventQueue::new();

        let event = GitHubEvent::PullRequest(PullRequestEvent {
            repo: RepoId::new("owner", "repo"),
            action: PrAction::Opened,
            pr_number: PrNumber(42),
            merged: false,
            merge_commit_sha: None,
            head_sha: Sha::parse("a".repeat(40)).unwrap(),
            base_branch: "main".to_string(),
            head_branch: "feature".to_string(),
            is_draft: false,
            author_id: 1,
        });

        // First push should succeed
        let first = queue.push(
            event.clone(),
            DeliveryId::new("test-id"),
            EventPriority::Normal,
        );
        assert!(first, "First push should succeed");

        // Second push with same ID should fail
        let second = queue.push(event, DeliveryId::new("test-id"), EventPriority::Normal);
        assert!(!second, "Second push with same ID should fail");

        // Queue should only have 1 item
        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn queue_contains_tracks_queued_ids() {
        use crate::types::Sha;
        use crate::webhooks::events::{GitHubEvent, PrAction, PullRequestEvent};

        let mut queue = super::super::queue::EventQueue::new();

        let event = GitHubEvent::PullRequest(PullRequestEvent {
            repo: RepoId::new("owner", "repo"),
            action: PrAction::Opened,
            pr_number: PrNumber(42),
            merged: false,
            merge_commit_sha: None,
            head_sha: Sha::parse("a".repeat(40)).unwrap(),
            base_branch: "main".to_string(),
            head_branch: "feature".to_string(),
            is_draft: false,
            author_id: 1,
        });

        let delivery_id = DeliveryId::new("test-id");

        // Before push: should not contain
        assert!(!queue.contains(&delivery_id));

        // After push: should contain
        queue.push(event, delivery_id.clone(), EventPriority::Normal);
        assert!(queue.contains(&delivery_id));

        // After pop: should not contain
        let _ = queue.pop();
        assert!(!queue.contains(&delivery_id));
    }

    // ─── Stage 17: Polling and Timer Tests ───

    #[test]
    fn poll_config_has_sane_defaults() {
        let config = super::super::poll::PollConfig::new();

        // 10 minute poll interval
        assert_eq!(config.poll_interval.as_secs(), 600);
        // 5 minute wait timeout
        assert_eq!(config.wait_timeout.as_secs(), 300);
        // 5 second recheck interval
        assert_eq!(config.recheck_interval.as_secs(), 5);
        // 20% jitter
        assert_eq!(config.jitter_percent, 20);
    }

    #[test]
    fn time_until_next_poll_returns_none_when_no_trains() {
        // When there are no active trains, polling shouldn't be scheduled
        let dir = tempdir().unwrap();
        let spool_dir = dir.path().join("spool");
        let state_dir = dir.path().join("state");

        let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
        let worker = RepoWorker::new(config).unwrap();

        // No active trains means no polling needed
        assert!(worker.state.active_trains.is_empty());

        // time_until_next_poll should return None when there are no active trains
        assert!(
            worker.time_until_next_poll().is_none(),
            "time_until_next_poll should return None when no trains are active"
        );
    }

    #[test]
    fn time_until_next_poll_returns_some_when_trains_active() {
        // When there are active trains, polling should be scheduled
        let dir = tempdir().unwrap();
        let spool_dir = dir.path().join("spool");
        let state_dir = dir.path().join("state");

        let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
        let mut worker = RepoWorker::new(config).unwrap();

        // Add an active train
        let train = crate::types::TrainRecord::new(PrNumber(10));
        worker.state.active_trains.insert(PrNumber(10), train);

        // time_until_next_poll should return Some duration when trains are active
        assert!(
            worker.time_until_next_poll().is_some(),
            "time_until_next_poll should return Some when trains are active"
        );
    }

    #[test]
    fn find_train_root_finds_root_by_current_pr() {
        // Test that find_train_root can find a train by its current_pr
        let dir = tempdir().unwrap();
        let spool_dir = dir.path().join("spool");
        let state_dir = dir.path().join("state");

        let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
        let mut worker = RepoWorker::new(config).unwrap();

        // Add an active train
        let train = crate::types::TrainRecord::new(PrNumber(10));
        worker.state.active_trains.insert(PrNumber(10), train);

        // Train root should be found by root PR
        assert_eq!(worker.find_train_root(PrNumber(10)), Some(PrNumber(10)));

        // Non-existent PR should not be found
        assert_eq!(worker.find_train_root(PrNumber(99)), None);
    }

    // ─── Stage 17: Spool Drain During Operation Tests ───

    #[tokio::test]
    async fn drain_spool_periodic_picks_up_missed_deliveries() {
        // Test that drain_spool_periodic can recover missed deliveries
        let dir = tempdir().unwrap();
        let spool_dir = dir.path().join("spool");
        let state_dir = dir.path().join("state");

        let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
        let mut worker = RepoWorker::new(config).unwrap();

        // Process any startup deliveries
        let (initial_count, _) = worker.process_all().unwrap();
        assert_eq!(initial_count, 0, "Should start empty");

        // Now spool a new delivery (simulating one that was missed)
        let envelope = make_pr_opened_envelope(99);
        let delivery_id = DeliveryId::new("missed-delivery");
        spool_webhook(&spool_dir, &delivery_id, &envelope).unwrap();

        // Run periodic drain
        let shutdown = CancellationToken::new();
        let drained = worker.drain_spool_periodic(&shutdown).await.unwrap();
        assert_eq!(drained, 1, "Should have drained the missed delivery");

        // Delivery should be processed and marked done after flush
        worker.flush_pending_batch().unwrap();
        let delivery = SpooledDelivery::new(&spool_dir, delivery_id);
        assert!(delivery.is_done(), "Drained delivery should be marked done");
    }

    #[tokio::test]
    async fn drain_spool_periodic_skips_already_done() {
        // Test that drain_spool_periodic doesn't re-process done deliveries
        let dir = tempdir().unwrap();
        let spool_dir = dir.path().join("spool");
        let state_dir = dir.path().join("state");

        // Spool and process a delivery first
        let envelope = make_pr_opened_envelope(42);
        let delivery_id = DeliveryId::new("already-done");
        spool_webhook(&spool_dir, &delivery_id, &envelope).unwrap();

        let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
        let mut worker = RepoWorker::new(config).unwrap();

        // Process it
        let (count, _) = worker.process_all().unwrap();
        assert_eq!(count, 1);

        // Now run periodic drain - should not pick it up again
        let shutdown = CancellationToken::new();
        let drained = worker.drain_spool_periodic(&shutdown).await.unwrap();
        assert_eq!(drained, 0, "Already-done delivery should not be re-drained");
    }

    #[test]
    fn stop_command_immediate_cancellation() {
        // Test that stop commands trigger immediate cancellation
        // by checking token is cancelled after handle_immediate_cancellation_if_stop
        let dir = tempdir().unwrap();
        let spool_dir = dir.path().join("spool");
        let state_dir = dir.path().join("state");

        // Create a stop command delivery
        let stop_envelope = make_comment_envelope(42, "@merge-train stop");
        let delivery_id = DeliveryId::new("stop-command");
        spool_webhook(&spool_dir, &delivery_id, &stop_envelope).unwrap();

        let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir)
            .with_bot_name("merge-train");
        let mut worker = RepoWorker::new(config).unwrap();

        // Add a train that we can cancel
        let train = crate::types::TrainRecord::new(PrNumber(42));
        worker.state.active_trains.insert(PrNumber(42), train);

        // Create a cancellation token for the train
        let token = worker.get_or_create_stack_token(PrNumber(42));
        assert!(
            !token.is_cancelled(),
            "Token should not be cancelled initially"
        );

        // Process the stop command delivery
        let delivery = SpooledDelivery::new(&spool_dir, delivery_id);
        worker
            .handle_immediate_cancellation_if_stop(&delivery)
            .unwrap();

        // Token should now be cancelled
        assert!(
            token.is_cancelled(),
            "Token should be cancelled after stop command"
        );
    }

    #[test]
    fn train_started_creates_cancellation_token() {
        // End-to-end test: TrainStarted event creates a cancellation token
        // that can be cancelled by a subsequent TrainStopped event.
        //
        // This tests the full flow without manually creating tokens.
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

    #[test]
    fn existing_trains_get_tokens_at_startup() {
        // Test that trains loaded from snapshot get cancellation tokens at startup
        use crate::types::TrainRecord;

        let dir = tempdir().unwrap();
        let spool_dir = dir.path().join("spool");
        let state_dir = dir.path().join("state");

        // Create a snapshot with an active train
        std::fs::create_dir_all(&state_dir).unwrap();
        let mut snapshot = PersistedRepoSnapshot::new("main");
        snapshot
            .active_trains
            .insert(PrNumber(42), TrainRecord::new(PrNumber(42)));
        snapshot
            .active_trains
            .insert(PrNumber(99), TrainRecord::new(PrNumber(99)));
        save_snapshot_atomic(&state_dir.join("snapshot.0.json"), &snapshot).unwrap();

        // Create worker - it should load the snapshot and create tokens
        let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
        let worker = RepoWorker::new(config).unwrap();

        // Verify tokens were created for existing trains
        assert!(
            worker.stack_tokens.contains_key(&PrNumber(42)),
            "Token should exist for train 42 loaded from snapshot"
        );
        assert!(
            worker.stack_tokens.contains_key(&PrNumber(99)),
            "Token should exist for train 99 loaded from snapshot"
        );

        // Tokens should not be cancelled initially
        assert!(
            !worker
                .stack_tokens
                .get(&PrNumber(42))
                .unwrap()
                .is_cancelled(),
            "Token for train 42 should not be cancelled initially"
        );
        assert!(
            !worker
                .stack_tokens
                .get(&PrNumber(99))
                .unwrap()
                .is_cancelled(),
            "Token for train 99 should not be cancelled initially"
        );
    }

    #[test]
    fn critical_event_syncs_log_before_done_marker_even_when_no_pending_batch() {
        // Test oracle: Critical events must sync the log before creating the done marker,
        // even when pending_batch is empty. This ensures durability - if we crash after
        // the done marker is created but before fsync, we might lose the critical event.
        //
        // This test verifies the fix for the bug where flush_pending_batch() early-returned
        // when pending_batch was empty, bypassing the event_log.sync() call.
        //
        // The fix separates the concerns:
        // 1. For critical events: sync log unconditionally, then mark done
        // 2. For non-critical: batch, and flush batch syncs + marks done

        let dir = tempdir().unwrap();
        let spool_dir = dir.path().join("spool");
        let state_dir = dir.path().join("state");

        // Create a start command envelope that will trigger TrainStarted (critical)
        let envelope = make_comment_envelope(42, "@merge-train start");
        let delivery_id = DeliveryId::new("critical-test");
        spool_webhook(&spool_dir, &delivery_id, &envelope).unwrap();

        // We also need a PR in state for the start command to work
        let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir)
            .with_bot_name("merge-train");
        let mut worker = RepoWorker::new(config).unwrap();

        // Add a PR to state so the start command can work
        let cached_pr = crate::types::CachedPr::new(
            PrNumber(42),
            crate::types::Sha::parse("a".repeat(40)).unwrap(),
            "feature".to_string(),
            "main".to_string(), // targets main, so it's a root
            None,               // no predecessor
            crate::types::PrState::Open,
            crate::types::MergeStateStatus::Clean,
            false,
        );
        worker.state.prs.insert(PrNumber(42), cached_pr);

        // Ensure pending_batch is empty before we start
        assert!(
            worker.pending_batch.is_empty(),
            "pending_batch should be empty initially"
        );

        // Record the initial log position
        let initial_position = worker.event_log.position().unwrap();

        // Process the event
        let result = worker.process_next().unwrap();
        assert!(result.processed, "Event should be processed");

        // After processing: log should have advanced (events written)
        let post_process_position = worker.event_log.position().unwrap();
        assert!(
            post_process_position > initial_position,
            "Event log should have events written after processing"
        );

        // For critical events, the done marker should be created immediately
        // (no batching), and the log should have been synced before that.
        let delivery = SpooledDelivery::new(&spool_dir, delivery_id.clone());
        assert!(
            delivery.is_done(),
            "Done marker should exist immediately after critical event is processed"
        );

        // Verify the train was actually started (the critical event was applied)
        assert!(
            worker.state.active_trains.contains_key(&PrNumber(42)),
            "Train should be active after start command (TrainStarted event applied)"
        );

        // Verify the event log contains the TrainStarted event
        let log_path = state_dir.join(format!("events.{}.log", worker.state.log_generation));
        assert!(log_path.exists(), "Event log file should exist");
        let log_content = std::fs::read_to_string(&log_path).unwrap();
        assert!(
            log_content.contains("train_started"),
            "Event log should contain the TrainStarted event: {}",
            log_content
        );
    }

    #[test]
    fn poll_response_updates_cached_pr_state() {
        // Test oracle: When process_github_response receives a PrRefetched response,
        // the cached PR should be updated with the new data from GitHub.
        //
        // This is essential for poll-based recovery: when webhooks are missed,
        // polling refetches PR data and the response updates local state.
        use crate::effects::PrData;

        let dir = tempdir().unwrap();
        let spool_dir = dir.path().join("spool");
        let state_dir = dir.path().join("state");

        let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
        let mut worker = RepoWorker::new(config).unwrap();

        // Add a PR to state with initial values
        let initial_sha = crate::types::Sha::parse("a".repeat(40)).unwrap();
        let cached_pr = crate::types::CachedPr::new(
            PrNumber(42),
            initial_sha.clone(),
            "feature".to_string(),
            "main".to_string(),
            None,
            crate::types::PrState::Open,
            crate::types::MergeStateStatus::Unknown,
            false,
        );
        worker.state.prs.insert(PrNumber(42), cached_pr);

        // Simulate a PrRefetched response with updated data
        let new_sha = crate::types::Sha::parse("b".repeat(40)).unwrap();
        let response = crate::effects::GitHubResponse::PrRefetched {
            pr: PrData {
                number: PrNumber(42),
                head_sha: new_sha.clone(),
                head_ref: "feature-updated".to_string(),
                base_ref: "develop".to_string(),
                state: crate::types::PrState::Open,
                is_draft: true,
            },
            merge_state: crate::types::MergeStateStatus::Clean,
        };

        // Process the response
        worker.process_github_response(response);

        // Verify the cached PR was updated
        let updated_pr = worker.state.prs.get(&PrNumber(42)).unwrap();
        assert_eq!(updated_pr.head_sha, new_sha, "head_sha should be updated");
        assert_eq!(
            updated_pr.head_ref, "feature-updated",
            "head_ref should be updated"
        );
        assert_eq!(updated_pr.base_ref, "develop", "base_ref should be updated");
        assert!(updated_pr.is_draft, "is_draft should be updated");
        assert_eq!(
            updated_pr.merge_state_status,
            crate::types::MergeStateStatus::Clean,
            "merge_state_status should be updated"
        );
    }

    #[test]
    fn poll_response_creates_new_pr_if_not_cached() {
        // Test oracle: When process_github_response receives a PrRefetched response
        // for a PR that isn't in the cache, it should create a new entry.
        //
        // This can happen during bootstrap or if a PR was created while
        // the bot was offline.
        use crate::effects::PrData;

        let dir = tempdir().unwrap();
        let spool_dir = dir.path().join("spool");
        let state_dir = dir.path().join("state");

        let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
        let mut worker = RepoWorker::new(config).unwrap();

        // Verify PR 42 doesn't exist initially
        assert!(!worker.state.prs.contains_key(&PrNumber(42)));

        // Simulate a PrRefetched response for a new PR
        let sha = crate::types::Sha::parse("c".repeat(40)).unwrap();
        let response = crate::effects::GitHubResponse::PrRefetched {
            pr: PrData {
                number: PrNumber(42),
                head_sha: sha.clone(),
                head_ref: "new-feature".to_string(),
                base_ref: "main".to_string(),
                state: crate::types::PrState::Open,
                is_draft: false,
            },
            merge_state: crate::types::MergeStateStatus::Clean,
        };

        // Process the response
        worker.process_github_response(response);

        // Verify the PR was created
        assert!(
            worker.state.prs.contains_key(&PrNumber(42)),
            "PR should be created"
        );
        let created_pr = worker.state.prs.get(&PrNumber(42)).unwrap();
        assert_eq!(created_pr.head_sha, sha);
        assert_eq!(created_pr.head_ref, "new-feature");
        assert_eq!(created_pr.base_ref, "main");
        assert!(!created_pr.is_draft);
        assert_eq!(
            created_pr.merge_state_status,
            crate::types::MergeStateStatus::Clean
        );
        // Predecessor is not discovered via API, so it should be None
        assert!(created_pr.predecessor.is_none());
    }
}
