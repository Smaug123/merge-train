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

    /// IO error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Result type for worker operations.
pub type Result<T> = std::result::Result<T, WorkerError>;

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

        Ok(RepoWorker {
            config,
            queue,
            state,
            event_log,
            pending_batch: Vec::new(),
            poll_config: PollConfig::from_env(),
            stack_tokens: HashMap::new(),
            pending_timers: Vec::new(),
            last_poll: None,
        })
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
    /// Returns `Ok(true)` if an event was processed, `Ok(false)` if the queue
    /// was empty.
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
    #[instrument(skip(self), fields(repo = %self.config.repo))]
    pub fn process_next(&mut self) -> Result<bool> {
        // Pop next event
        let queued = match self.queue.pop() {
            Some(q) => q,
            None => return Ok(false),
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
            // Critical event: fsync immediately and create done marker
            self.flush_pending_batch()?;
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

        // TODO: Execute effects (GitHub API calls, git operations)
        // This will be implemented when we integrate with the effect interpreter

        Ok(true)
    }

    /// Processes all events currently in the queue.
    ///
    /// Returns the number of events processed.
    pub fn process_all(&mut self) -> Result<usize> {
        let mut count = 0;
        while self.process_next()? {
            count += 1;
        }

        // Flush any remaining batched events
        if !self.pending_batch.is_empty() {
            self.flush_pending_batch()?;
        }

        Ok(count)
    }

    /// Flushes the pending batch: fsync log and create done markers.
    ///
    /// This should be called:
    /// - Before any irreversible operation (e.g., GitHub API call)
    /// - Periodically to limit data loss window
    /// - On shutdown
    pub fn flush_pending_batch(&mut self) -> Result<()> {
        if self.pending_batch.is_empty() {
            return Ok(());
        }

        // fsync the event log
        self.event_log.sync()?;

        // Create done markers for all pending deliveries
        for delivery_id in &self.pending_batch {
            let delivery = SpooledDelivery::new(&self.config.spool_dir, delivery_id.clone());
            mark_done(&delivery)?;
        }

        debug!(count = self.pending_batch.len(), "Flushed pending batch");

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
            while self.process_next()? {}

            // Flush pending batch after processing startup backlog
            if !self.pending_batch.is_empty() {
                self.flush_pending_batch()?;
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
                            if let Err(e) = self.handle_message(msg).await {
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
    async fn handle_message(&mut self, msg: WorkerMessage) -> Result<()> {
        match msg {
            WorkerMessage::Delivery(delivery) => {
                self.handle_delivery(delivery)?;
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
    fn handle_delivery(&mut self, delivery: SpooledDelivery) -> Result<()> {
        // Enqueue the delivery
        self.enqueue(delivery)?;

        // Process all pending events (synchronously for now)
        // In the future, this could be broken into smaller chunks
        while self.process_next()? {}

        // Flush pending batch
        if !self.pending_batch.is_empty() {
            self.flush_pending_batch()?;
        }

        Ok(())
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
    async fn handle_poll_active_trains(&mut self) -> Result<()> {
        self.last_poll = Some(Instant::now());

        let active_trains: Vec<_> = self.state.active_trains.keys().copied().collect();

        if active_trains.is_empty() {
            trace!("No active trains to poll");
            return Ok(());
        }

        debug!(
            count = active_trains.len(),
            "Polling active trains for missed webhooks"
        );

        // For each active train, we would:
        // 1. Check mergeStateStatus via GitHub API (RefetchPr effect)
        // 2. Compare with cached state
        // 3. Trigger cascade evaluation if changed
        //
        // This requires effect execution which is not fully implemented.
        // For now, we just log that polling occurred.

        for train_root in active_trains {
            trace!(
                train_root = %train_root,
                trigger = "poll",
                "Would poll train status"
            );
        }

        Ok(())
    }

    /// Handles a timer firing for non-blocking wait re-evaluation.
    async fn handle_timer_fired(
        &mut self,
        train_root: PrNumber,
        condition: WaitCondition,
    ) -> Result<()> {
        trace!(
            train_root = %train_root,
            condition = ?condition,
            "Timer fired for wait condition"
        );

        // Check if the train is still active
        if !self.state.active_trains.contains_key(&train_root) {
            debug!(
                train_root = %train_root,
                "Train no longer active, ignoring timer"
            );
            return Ok(());
        }

        // In a full implementation, we would:
        // 1. Check the wait condition via GitHub API
        // 2. If satisfied, proceed with cascade
        // 3. If not satisfied, reschedule timer (with timeout check)
        //
        // For now, just log that the timer fired.

        Ok(())
    }

    // ─── Timer Management ─────────────────────────────────────────────────────────

    /// Schedules a timer for non-blocking wait re-evaluation.
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
    #[allow(dead_code)]
    pub fn get_or_create_stack_token(&mut self, train_root: PrNumber) -> CancellationToken {
        self.stack_tokens.entry(train_root).or_default().clone()
    }

    /// Finds the train root for a PR.
    fn find_train_root(&self, pr: PrNumber) -> Option<PrNumber> {
        // Check if PR is itself a train root
        if self.state.active_trains.contains_key(&pr) {
            return Some(pr);
        }

        // Search active trains to find one containing this PR
        for (root, train) in &self.state.active_trains {
            if train.current_pr == pr {
                return Some(*root);
            }
            // Could also check frozen_descendants if available
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
        let processed = worker.process_next().unwrap();
        assert!(processed);

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
        // Test oracle: .done marker is only created AFTER fsync
        let dir = tempdir().unwrap();
        let spool_dir = dir.path().join("spool");
        let state_dir = dir.path().join("state");

        // Spool an event
        let envelope = make_pr_opened_envelope(42);
        let delivery_id = DeliveryId::new("test-delivery");
        spool_webhook(&spool_dir, &delivery_id, &envelope).unwrap();

        let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
        let mut worker = RepoWorker::new(config).unwrap();

        // Process the event
        worker.process_next().unwrap();

        // Before flush: .done marker should NOT exist
        let delivery = SpooledDelivery::new(&spool_dir, delivery_id.clone());
        assert!(
            !delivery.is_done(),
            "Done marker should not exist before flush"
        );

        // After flush: .done marker should exist
        worker.flush_pending_batch().unwrap();
        assert!(delivery.is_done(), "Done marker should exist after flush");
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
        worker.process_all().unwrap();
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
            let processed = worker.process_next().unwrap();
            assert!(processed);
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

        // process_all should return 0 without error
        let count = worker.process_all().unwrap();
        assert_eq!(count, 0);
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
}
