//! Event dispatch layer for routing webhooks to per-repo workers.
//!
//! The dispatcher routes webhook events to the appropriate per-repo worker,
//! creating workers on demand when the first event for a repository arrives.
//! Different repositories are processed concurrently, but events within a
//! single repository are strictly serialized.
//!
//! # Architecture
//!
//! From DESIGN.md:
//! ```text
//!                                          ┌─────────────────────────────────┐
//!                                          │        repo A queue             │
//!                                     ┌──► │  (priority: stop > others)      │ ──► worker A
//!                                     │    └─────────────────────────────────┘
//! ┌─────────────┐     ┌──────────┐    │
//! │   axum      │ ──► │ dispatch │ ───┤    ┌─────────────────────────────────┐
//! │  (accepts)  │     │ by repo  │    │    │        repo B queue             │
//! └─────────────┘     └──────────┘    └──► │  (priority: stop > others)      │ ──► worker B
//!       │                                  └─────────────────────────────────┘
//!       │ returns 202 Accepted
//!       ▼
//! ```
//!
//! # Worker Lifecycle
//!
//! Workers are created lazily when the first event arrives for a repository.
//! Each worker runs as an async tokio task with its own event loop.
//! Workers process messages sent via channels and respond to shutdown signals.

use std::collections::HashMap;
use std::path::PathBuf;

use thiserror::Error;
use tokio::sync::{RwLock, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, trace};

use crate::spool::delivery::SpooledDelivery;
use crate::types::{DeliveryId, PrNumber, RepoId};

use super::message::WorkerMessage;
use super::worker::{RepoWorker, WorkerConfig, WorkerError};

/// Errors that can occur during dispatch operations.
#[derive(Debug, Error)]
pub enum DispatchError {
    /// Worker error during event processing.
    #[error("worker error: {0}")]
    Worker(#[from] WorkerError),

    /// Failed to send message to worker.
    #[error("failed to send message to worker: channel closed")]
    ChannelClosed,

    /// Worker not found for repository.
    #[error("no worker found for repository: {0}")]
    WorkerNotFound(RepoId),
}

/// Result type for dispatch operations.
pub type Result<T> = std::result::Result<T, DispatchError>;

/// Configuration for the dispatcher.
#[derive(Debug, Clone)]
pub struct DispatcherConfig {
    /// Base directory for webhook spool files.
    /// Per-repo directories are created under `<spool_base>/<owner>/<repo>/`.
    pub spool_base: PathBuf,

    /// Base directory for state persistence.
    /// Per-repo directories are created under `<state_base>/<owner>/<repo>/`.
    pub state_base: PathBuf,

    /// Bot name for command parsing (e.g., "merge-train").
    pub bot_name: String,
}

impl DispatcherConfig {
    /// Creates a new dispatcher configuration.
    pub fn new(spool_base: impl Into<PathBuf>, state_base: impl Into<PathBuf>) -> Self {
        DispatcherConfig {
            spool_base: spool_base.into(),
            state_base: state_base.into(),
            bot_name: "merge-train".to_string(),
        }
    }

    /// Sets a custom bot name for command parsing.
    pub fn with_bot_name(mut self, bot_name: impl Into<String>) -> Self {
        self.bot_name = bot_name.into();
        self
    }

    /// Returns the spool directory for a specific repository.
    pub fn spool_dir(&self, repo: &RepoId) -> PathBuf {
        self.spool_base.join(&repo.owner).join(&repo.repo)
    }

    /// Returns the state directory for a specific repository.
    pub fn state_dir(&self, repo: &RepoId) -> PathBuf {
        self.state_base.join(&repo.owner).join(&repo.repo)
    }
}

/// Channel buffer size for worker messages.
const WORKER_CHANNEL_BUFFER: usize = 100;

/// Per-repo worker handle.
///
/// Contains the message channel and task handle for communicating with
/// a worker running as an async task.
struct WorkerHandle {
    /// Channel for sending messages to the worker.
    tx: mpsc::Sender<WorkerMessage>,

    /// Handle to the worker's async task.
    #[allow(dead_code)]
    task: JoinHandle<()>,

    /// Cancellation token for this worker.
    cancel: CancellationToken,
}

/// Event dispatcher that routes webhooks to per-repo workers.
///
/// The dispatcher is thread-safe and can be shared across multiple HTTP handler
/// tasks. It creates workers on demand as async tasks and routes events via
/// message channels.
pub struct Dispatcher {
    /// Dispatcher configuration.
    config: DispatcherConfig,

    /// Active workers, keyed by repository ID.
    /// Protected by RwLock for async-safe access.
    workers: RwLock<HashMap<RepoId, WorkerHandle>>,

    /// Global shutdown token.
    shutdown: CancellationToken,
}

impl Dispatcher {
    /// Creates a new dispatcher with the given configuration.
    pub fn new(config: DispatcherConfig) -> Self {
        info!(
            spool_base = %config.spool_base.display(),
            state_base = %config.state_base.display(),
            "Creating dispatcher"
        );

        Dispatcher {
            config,
            workers: RwLock::new(HashMap::new()),
            shutdown: CancellationToken::new(),
        }
    }

    /// Creates a new dispatcher with a custom shutdown token.
    pub fn new_with_shutdown(config: DispatcherConfig, shutdown: CancellationToken) -> Self {
        info!(
            spool_base = %config.spool_base.display(),
            state_base = %config.state_base.display(),
            "Creating dispatcher with custom shutdown"
        );

        Dispatcher {
            config,
            workers: RwLock::new(HashMap::new()),
            shutdown,
        }
    }

    /// Returns the dispatcher configuration.
    pub fn config(&self) -> &DispatcherConfig {
        &self.config
    }

    /// Returns the shutdown token.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Dispatches a spooled delivery to the appropriate worker.
    ///
    /// This creates a new worker for the repository if one doesn't exist,
    /// then sends the delivery to the worker via message channel.
    ///
    /// # Async Safety
    ///
    /// This method is safe to call concurrently from multiple tasks.
    /// Workers for different repositories process events concurrently,
    /// while events for the same repository are serialized by the worker's
    /// internal event loop.
    #[instrument(skip(self, delivery), fields(repo = %repo, delivery_id = %delivery.delivery_id))]
    pub async fn dispatch(&self, repo: &RepoId, delivery: SpooledDelivery) -> Result<()> {
        // Get or create worker for this repo
        let tx = self.get_or_spawn_worker(repo).await?;

        // Send the delivery to the worker
        tx.send(WorkerMessage::Delivery(delivery))
            .await
            .map_err(|_| DispatchError::ChannelClosed)?;

        Ok(())
    }

    /// Dispatches a delivery by ID, looking up the spooled file.
    ///
    /// This is a convenience method for dispatching when you only have the
    /// delivery ID and repository.
    #[instrument(skip(self), fields(repo = %repo, delivery_id = %delivery_id))]
    pub async fn dispatch_by_id(&self, repo: &RepoId, delivery_id: DeliveryId) -> Result<()> {
        let spool_dir = self.config.spool_dir(repo);
        let delivery = SpooledDelivery::new(&spool_dir, delivery_id);
        self.dispatch(repo, delivery).await
    }

    /// Sends a cancel request for a stack.
    ///
    /// This is used when a stop command is received to interrupt in-flight
    /// operations for the specified PR's stack.
    #[instrument(skip(self), fields(repo = %repo, pr = %pr))]
    pub async fn cancel_stack(&self, repo: &RepoId, pr: PrNumber) -> Result<()> {
        let workers = self.workers.read().await;

        if let Some(handle) = workers.get(repo) {
            handle
                .tx
                .send(WorkerMessage::CancelStack(pr))
                .await
                .map_err(|_| DispatchError::ChannelClosed)?;
        }

        Ok(())
    }

    /// Gets an existing worker's sender or spawns a new worker task.
    async fn get_or_spawn_worker(&self, repo: &RepoId) -> Result<mpsc::Sender<WorkerMessage>> {
        // First, try to get existing worker (read lock)
        {
            let workers = self.workers.read().await;
            if let Some(handle) = workers.get(repo) {
                return Ok(handle.tx.clone());
            }
        }

        // Worker doesn't exist, need to create one (write lock)
        let mut workers = self.workers.write().await;

        // Double-check after acquiring write lock
        if let Some(handle) = workers.get(repo) {
            return Ok(handle.tx.clone());
        }

        // Create new worker
        debug!(repo = %repo, "Spawning new worker task");
        let worker_config = WorkerConfig::new(
            repo.clone(),
            self.config.spool_dir(repo),
            self.config.state_dir(repo),
        )
        .with_bot_name(&self.config.bot_name);

        let worker = RepoWorker::new(worker_config)?;

        // Create channel and cancellation token.
        // IMPORTANT: Use the same token for both the worker task and the handle
        // so that remove_worker() cancels the correct token.
        let (tx, rx) = mpsc::channel(WORKER_CHANNEL_BUFFER);
        let cancel = self.shutdown.child_token();
        let cancel_for_handle = cancel.clone();

        // Spawn worker task
        let repo_for_task = repo.clone();
        let task = tokio::spawn(async move {
            if let Err(e) = worker.run(rx, cancel).await {
                error!(repo = %repo_for_task, error = %e, "Worker task failed");
            }
        });

        let handle = WorkerHandle {
            tx: tx.clone(),
            task,
            cancel: cancel_for_handle,
        };
        workers.insert(repo.clone(), handle);

        Ok(tx)
    }

    /// Returns the number of active workers.
    pub async fn worker_count(&self) -> usize {
        self.workers.read().await.len()
    }

    /// Checks if a worker exists for the given repository.
    pub async fn has_worker(&self, repo: &RepoId) -> bool {
        self.workers.read().await.contains_key(repo)
    }

    /// Removes a worker for the given repository.
    ///
    /// This sends a shutdown message and removes the worker handle.
    pub async fn remove_worker(&self, repo: &RepoId) -> bool {
        let mut workers = self.workers.write().await;

        if let Some(handle) = workers.remove(repo) {
            // Signal shutdown
            handle.cancel.cancel();
            // Try to send shutdown message
            let _ = handle.tx.send(WorkerMessage::Shutdown).await;
            true
        } else {
            false
        }
    }

    /// Sends a shutdown signal to all workers.
    ///
    /// This triggers graceful shutdown of all worker tasks.
    pub async fn shutdown_all(&self) {
        info!("Shutting down all workers");
        self.shutdown.cancel();

        // Send shutdown messages to all workers
        let workers = self.workers.read().await;
        for (repo, handle) in workers.iter() {
            trace!(repo = %repo, "Sending shutdown to worker");
            let _ = handle.tx.send(WorkerMessage::Shutdown).await;
        }
    }

    /// Broadcasts a poll message to all active workers.
    ///
    /// This can be used to trigger an immediate poll of all workers,
    /// for example when recovering from a network outage.
    ///
    /// Note: Workers handle their own jittered polling for normal operation.
    /// This method is for exceptional cases only.
    pub async fn broadcast_poll(&self) {
        let workers = self.workers.read().await;

        for (repo, handle) in workers.iter() {
            trace!(repo = %repo, "Broadcasting poll to worker");
            let _ = handle.tx.send(WorkerMessage::PollActiveTrains).await;
        }
    }

    /// Runs the dispatcher's background loop.
    ///
    /// This waits for the shutdown signal. Workers handle their own
    /// polling with per-repo jitter to avoid thundering herd problems.
    pub async fn run(&self) {
        info!("Dispatcher background loop started");

        // Just wait for shutdown - workers handle their own jittered polling
        self.shutdown.cancelled().await;

        info!("Dispatcher shutdown signal received, stopping");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spool::delivery::{WebhookEnvelope, spool_webhook};
    use std::collections::HashMap;
    use std::sync::Arc;
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

    // ─── Configuration tests ───

    #[test]
    fn config_creates_correct_paths() {
        let config = DispatcherConfig::new("/spool", "/state");
        let repo = RepoId::new("owner", "repo");

        assert_eq!(config.spool_dir(&repo), PathBuf::from("/spool/owner/repo"));
        assert_eq!(config.state_dir(&repo), PathBuf::from("/state/owner/repo"));
    }

    #[test]
    fn config_with_bot_name() {
        let config = DispatcherConfig::new("/spool", "/state").with_bot_name("custom-bot");

        assert_eq!(config.bot_name, "custom-bot");
    }

    // ─── Async Dispatcher tests ───

    #[tokio::test]
    async fn dispatcher_creates_worker_on_first_event() {
        let dir = tempdir().unwrap();
        let config = DispatcherConfig::new(dir.path().join("spool"), dir.path().join("state"));

        let dispatcher = Dispatcher::new(config.clone());
        let repo = RepoId::new("owner", "repo");

        // No workers initially
        assert_eq!(dispatcher.worker_count().await, 0);
        assert!(!dispatcher.has_worker(&repo).await);

        // Spool a delivery
        let envelope = make_pr_opened_envelope(42);
        let delivery_id = DeliveryId::new("test-delivery-1");
        spool_webhook(&config.spool_dir(&repo), &delivery_id, &envelope).unwrap();

        // Dispatch creates worker
        let delivery = SpooledDelivery::new(&config.spool_dir(&repo), delivery_id);
        dispatcher.dispatch(&repo, delivery).await.unwrap();

        // Give worker task time to start
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        assert_eq!(dispatcher.worker_count().await, 1);
        assert!(dispatcher.has_worker(&repo).await);
    }

    #[tokio::test]
    async fn dispatcher_reuses_existing_worker() {
        let dir = tempdir().unwrap();
        let config = DispatcherConfig::new(dir.path().join("spool"), dir.path().join("state"));

        let dispatcher = Dispatcher::new(config.clone());
        let repo = RepoId::new("owner", "repo");

        // Dispatch first event
        let envelope1 = make_pr_opened_envelope(42);
        let delivery_id1 = DeliveryId::new("test-delivery-1");
        spool_webhook(&config.spool_dir(&repo), &delivery_id1, &envelope1).unwrap();
        let delivery1 = SpooledDelivery::new(&config.spool_dir(&repo), delivery_id1);
        dispatcher.dispatch(&repo, delivery1).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert_eq!(dispatcher.worker_count().await, 1);

        // Dispatch second event - should reuse same worker
        let envelope2 = make_pr_opened_envelope(43);
        let delivery_id2 = DeliveryId::new("test-delivery-2");
        spool_webhook(&config.spool_dir(&repo), &delivery_id2, &envelope2).unwrap();
        let delivery2 = SpooledDelivery::new(&config.spool_dir(&repo), delivery_id2);
        dispatcher.dispatch(&repo, delivery2).await.unwrap();

        // Still only one worker
        assert_eq!(dispatcher.worker_count().await, 1);
    }

    #[tokio::test]
    async fn dispatcher_creates_separate_workers_per_repo() {
        let dir = tempdir().unwrap();
        let config = DispatcherConfig::new(dir.path().join("spool"), dir.path().join("state"));

        let dispatcher = Dispatcher::new(config.clone());
        let repo_a = RepoId::new("owner", "repo-a");
        let repo_b = RepoId::new("owner", "repo-b");

        // Dispatch to repo A
        let envelope_a = make_pr_opened_envelope(42);
        let delivery_id_a = DeliveryId::new("delivery-a");
        spool_webhook(&config.spool_dir(&repo_a), &delivery_id_a, &envelope_a).unwrap();
        let delivery_a = SpooledDelivery::new(&config.spool_dir(&repo_a), delivery_id_a);
        dispatcher.dispatch(&repo_a, delivery_a).await.unwrap();

        // Dispatch to repo B
        let envelope_b = make_pr_opened_envelope(43);
        let delivery_id_b = DeliveryId::new("delivery-b");
        spool_webhook(&config.spool_dir(&repo_b), &delivery_id_b, &envelope_b).unwrap();
        let delivery_b = SpooledDelivery::new(&config.spool_dir(&repo_b), delivery_id_b);
        dispatcher.dispatch(&repo_b, delivery_b).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Two separate workers
        assert_eq!(dispatcher.worker_count().await, 2);
        assert!(dispatcher.has_worker(&repo_a).await);
        assert!(dispatcher.has_worker(&repo_b).await);
    }

    #[tokio::test]
    async fn dispatcher_remove_worker() {
        let dir = tempdir().unwrap();
        let config = DispatcherConfig::new(dir.path().join("spool"), dir.path().join("state"));

        let dispatcher = Dispatcher::new(config.clone());
        let repo = RepoId::new("owner", "repo");

        // Create worker
        let envelope = make_pr_opened_envelope(42);
        let delivery_id = DeliveryId::new("test-delivery");
        spool_webhook(&config.spool_dir(&repo), &delivery_id, &envelope).unwrap();
        let delivery = SpooledDelivery::new(&config.spool_dir(&repo), delivery_id);
        dispatcher.dispatch(&repo, delivery).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(dispatcher.has_worker(&repo).await);

        // Remove worker
        assert!(dispatcher.remove_worker(&repo).await);
        assert!(!dispatcher.has_worker(&repo).await);

        // Removing again returns false
        assert!(!dispatcher.remove_worker(&repo).await);
    }

    #[tokio::test]
    async fn dispatcher_concurrent_dispatch() {
        let dir = tempdir().unwrap();
        let config = DispatcherConfig::new(dir.path().join("spool"), dir.path().join("state"));

        let dispatcher = Arc::new(Dispatcher::new(config.clone()));
        let repo = RepoId::new("owner", "repo");

        // Spool multiple deliveries
        for i in 0..5 {
            let envelope = make_pr_opened_envelope(i);
            let delivery_id = DeliveryId::new(format!("delivery-{}", i));
            spool_webhook(&config.spool_dir(&repo), &delivery_id, &envelope).unwrap();
        }

        // Dispatch from multiple concurrent tasks
        let handles: Vec<tokio::task::JoinHandle<Result<()>>> = (0..5)
            .map(|i| {
                let dispatcher = Arc::clone(&dispatcher);
                let repo = repo.clone();
                let config = config.clone();
                tokio::spawn(async move {
                    let delivery_id = DeliveryId::new(format!("delivery-{}", i));
                    let delivery = SpooledDelivery::new(&config.spool_dir(&repo), delivery_id);
                    dispatcher.dispatch(&repo, delivery).await
                })
            })
            .collect();

        // All should succeed
        for handle in handles {
            handle.await.unwrap().unwrap();
        }

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Only one worker created (all for same repo)
        assert_eq!(dispatcher.worker_count().await, 1);
    }

    #[tokio::test]
    async fn dispatcher_shutdown() {
        let dir = tempdir().unwrap();
        let config = DispatcherConfig::new(dir.path().join("spool"), dir.path().join("state"));

        let shutdown = CancellationToken::new();
        let dispatcher = Dispatcher::new_with_shutdown(config.clone(), shutdown.clone());
        let repo = RepoId::new("owner", "repo");

        // Create worker
        let envelope = make_pr_opened_envelope(42);
        let delivery_id = DeliveryId::new("test-delivery");
        spool_webhook(&config.spool_dir(&repo), &delivery_id, &envelope).unwrap();
        let delivery = SpooledDelivery::new(&config.spool_dir(&repo), delivery_id);
        dispatcher.dispatch(&repo, delivery).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert_eq!(dispatcher.worker_count().await, 1);

        // Verify shutdown token is not cancelled yet
        assert!(!shutdown.is_cancelled());

        // Shutdown all workers
        dispatcher.shutdown_all().await;

        // Verify shutdown token IS cancelled after shutdown
        assert!(
            shutdown.is_cancelled(),
            "Shutdown token should be cancelled after shutdown_all"
        );

        // Workers should stop (give them time)
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Attempt to dispatch to a NEW repo after shutdown - worker will be created
        // but should immediately see the cancelled shutdown token
        let repo2 = RepoId::new("owner", "repo2");
        let envelope2 = make_pr_opened_envelope(99);
        let delivery_id2 = DeliveryId::new("post-shutdown-delivery");
        spool_webhook(&config.spool_dir(&repo2), &delivery_id2, &envelope2).unwrap();
        let delivery2 = SpooledDelivery::new(&config.spool_dir(&repo2), delivery_id2);

        // This may succeed in creating a worker, but the worker will receive
        // a cancelled token and should exit promptly
        let _result = dispatcher.dispatch(&repo2, delivery2).await;

        // Give any new worker time to notice shutdown
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // ─── Stage 17 Integration Tests ───

    #[tokio::test]
    async fn dispatcher_run_exits_on_shutdown() {
        // Test oracle: dispatcher.run() exits when shutdown signal is received
        let dir = tempdir().unwrap();
        let config = DispatcherConfig::new(dir.path().join("spool"), dir.path().join("state"));

        let shutdown = CancellationToken::new();
        let dispatcher = Arc::new(Dispatcher::new_with_shutdown(config, shutdown.clone()));

        // Spawn the dispatcher run loop
        let dispatcher_handle = {
            let dispatcher = Arc::clone(&dispatcher);
            tokio::spawn(async move {
                dispatcher.run().await;
            })
        };

        // Give run() time to start
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Verify it's still running
        assert!(!dispatcher_handle.is_finished());

        // Signal shutdown
        shutdown.cancel();

        // Wait for run() to exit (with timeout)
        let result =
            tokio::time::timeout(std::time::Duration::from_secs(1), dispatcher_handle).await;

        assert!(
            result.is_ok(),
            "dispatcher.run() should exit after shutdown signal"
        );
    }

    #[tokio::test]
    async fn remove_worker_cancels_worker() {
        // Test oracle: remove_worker cancels the correct worker token
        let dir = tempdir().unwrap();
        let config = DispatcherConfig::new(dir.path().join("spool"), dir.path().join("state"));

        let dispatcher = Arc::new(Dispatcher::new(config.clone()));
        let repo = RepoId::new("owner", "repo");

        // Create worker
        let envelope = make_pr_opened_envelope(42);
        let delivery_id = DeliveryId::new("test-delivery");
        spool_webhook(&config.spool_dir(&repo), &delivery_id, &envelope).unwrap();
        let delivery = SpooledDelivery::new(&config.spool_dir(&repo), delivery_id);
        dispatcher.dispatch(&repo, delivery).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(dispatcher.has_worker(&repo).await);

        // Remove the worker
        let removed = dispatcher.remove_worker(&repo).await;
        assert!(removed);

        // Worker should no longer exist
        assert!(!dispatcher.has_worker(&repo).await);

        // Give worker task time to notice cancellation and exit
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Dispatching a new event should create a fresh worker
        let envelope2 = make_pr_opened_envelope(43);
        let delivery_id2 = DeliveryId::new("test-delivery-2");
        spool_webhook(&config.spool_dir(&repo), &delivery_id2, &envelope2).unwrap();
        let delivery2 = SpooledDelivery::new(&config.spool_dir(&repo), delivery_id2);
        dispatcher.dispatch(&repo, delivery2).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(dispatcher.has_worker(&repo).await);
    }

    #[tokio::test]
    async fn cancel_stack_message_is_delivered() {
        // Test that cancel_stack sends a message to the worker
        let dir = tempdir().unwrap();
        let config = DispatcherConfig::new(dir.path().join("spool"), dir.path().join("state"));

        let dispatcher = Dispatcher::new(config.clone());
        let repo = RepoId::new("owner", "repo");

        // Create worker
        let envelope = make_pr_opened_envelope(42);
        let delivery_id = DeliveryId::new("test-delivery");
        spool_webhook(&config.spool_dir(&repo), &delivery_id, &envelope).unwrap();
        let delivery = SpooledDelivery::new(&config.spool_dir(&repo), delivery_id);
        dispatcher.dispatch(&repo, delivery).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(dispatcher.has_worker(&repo).await);

        // Send cancel_stack - should not error
        let result = dispatcher.cancel_stack(&repo, PrNumber(42)).await;
        assert!(result.is_ok());

        // Cancelling for non-existent repo should not error (just no-op)
        let other_repo = RepoId::new("owner", "other");
        let result2 = dispatcher.cancel_stack(&other_repo, PrNumber(1)).await;
        assert!(result2.is_ok());
    }
}
