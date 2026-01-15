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
//! They continue processing until idle for a configurable timeout, then shut down.
//! The next event for that repository will create a new worker.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use thiserror::Error;
use tracing::{debug, error, info, instrument, warn};

use crate::spool::delivery::SpooledDelivery;
use crate::types::{DeliveryId, RepoId};

use super::worker::{RepoWorker, WorkerConfig, WorkerError};

/// Errors that can occur during dispatch operations.
#[derive(Debug, Error)]
pub enum DispatchError {
    /// Worker error during event processing.
    #[error("worker error: {0}")]
    Worker(#[from] WorkerError),

    /// Lock poisoned (worker panicked).
    #[error("worker lock poisoned")]
    LockPoisoned,
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

/// Per-repo worker handle.
///
/// Contains the worker and synchronization primitives for thread-safe access.
struct WorkerHandle {
    /// The worker instance, protected by a mutex for thread-safe access.
    worker: Mutex<RepoWorker>,
}

impl WorkerHandle {
    /// Creates a new worker handle.
    fn new(worker: RepoWorker) -> Self {
        WorkerHandle {
            worker: Mutex::new(worker),
        }
    }
}

/// Event dispatcher that routes webhooks to per-repo workers.
///
/// The dispatcher is thread-safe and can be shared across multiple HTTP handler
/// threads. It creates workers on demand and routes events to the appropriate
/// worker based on repository ID.
pub struct Dispatcher {
    /// Dispatcher configuration.
    config: DispatcherConfig,

    /// Active workers, keyed by repository ID.
    /// Protected by a mutex for thread-safe worker creation.
    workers: Mutex<HashMap<RepoId, Arc<WorkerHandle>>>,
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
            workers: Mutex::new(HashMap::new()),
        }
    }

    /// Returns the dispatcher configuration.
    pub fn config(&self) -> &DispatcherConfig {
        &self.config
    }

    /// Dispatches a spooled delivery to the appropriate worker.
    ///
    /// This creates a new worker for the repository if one doesn't exist,
    /// enqueues the delivery, and processes it.
    ///
    /// # Thread Safety
    ///
    /// This method is thread-safe and can be called concurrently from multiple
    /// threads. Workers for different repositories process events concurrently,
    /// while events for the same repository are serialized by the worker's mutex.
    #[instrument(skip(self, delivery), fields(repo = %repo, delivery_id = %delivery.delivery_id))]
    pub fn dispatch(&self, repo: &RepoId, delivery: SpooledDelivery) -> Result<()> {
        // Get or create worker for this repo
        let handle = self.get_or_create_worker(repo)?;

        // Lock the worker and process the event
        let mut worker = handle
            .worker
            .lock()
            .map_err(|_| DispatchError::LockPoisoned)?;

        // Enqueue and process
        worker.enqueue(delivery)?;
        worker.process_all()?;

        Ok(())
    }

    /// Dispatches a delivery by ID, looking up the spooled file.
    ///
    /// This is a convenience method for dispatching when you only have the
    /// delivery ID and repository.
    #[instrument(skip(self), fields(repo = %repo, delivery_id = %delivery_id))]
    pub fn dispatch_by_id(&self, repo: &RepoId, delivery_id: DeliveryId) -> Result<()> {
        let spool_dir = self.config.spool_dir(repo);
        let delivery = SpooledDelivery::new(&spool_dir, delivery_id);
        self.dispatch(repo, delivery)
    }

    /// Gets an existing worker or creates a new one.
    fn get_or_create_worker(&self, repo: &RepoId) -> Result<Arc<WorkerHandle>> {
        // First, try to get existing worker (read lock)
        {
            let workers = self
                .workers
                .lock()
                .map_err(|_| DispatchError::LockPoisoned)?;

            if let Some(handle) = workers.get(repo) {
                return Ok(Arc::clone(handle));
            }
        }

        // Worker doesn't exist, need to create one (write lock)
        let mut workers = self
            .workers
            .lock()
            .map_err(|_| DispatchError::LockPoisoned)?;

        // Double-check after acquiring write lock
        if let Some(handle) = workers.get(repo) {
            return Ok(Arc::clone(handle));
        }

        // Create new worker
        debug!(repo = %repo, "Creating new worker");
        let worker_config = WorkerConfig::new(
            repo.clone(),
            self.config.spool_dir(repo),
            self.config.state_dir(repo),
        )
        .with_bot_name(&self.config.bot_name);

        let worker = RepoWorker::new(worker_config)?;
        let handle = Arc::new(WorkerHandle::new(worker));
        workers.insert(repo.clone(), Arc::clone(&handle));

        Ok(handle)
    }

    /// Returns the number of active workers.
    pub fn worker_count(&self) -> usize {
        self.workers.lock().map(|w| w.len()).unwrap_or(0)
    }

    /// Checks if a worker exists for the given repository.
    pub fn has_worker(&self, repo: &RepoId) -> bool {
        self.workers
            .lock()
            .map(|w| w.contains_key(repo))
            .unwrap_or(false)
    }

    /// Removes a worker for the given repository.
    ///
    /// This is useful for testing or when a repository is no longer active.
    /// In normal operation, workers are kept alive until the process shuts down.
    pub fn remove_worker(&self, repo: &RepoId) -> bool {
        self.workers
            .lock()
            .map(|mut w| w.remove(repo).is_some())
            .unwrap_or(false)
    }

    /// Flushes all workers' pending batches.
    ///
    /// This should be called before shutdown to ensure all state is persisted.
    pub fn flush_all(&self) -> Result<()> {
        let workers = self
            .workers
            .lock()
            .map_err(|_| DispatchError::LockPoisoned)?;

        for (repo, handle) in workers.iter() {
            match handle.worker.lock() {
                Ok(mut worker) => {
                    if let Err(e) = worker.flush_pending_batch() {
                        error!(repo = %repo, error = %e, "Failed to flush worker");
                    }
                }
                Err(_) => {
                    warn!(repo = %repo, "Worker lock poisoned during flush");
                }
            }
        }

        Ok(())
    }

    /// Saves snapshots for all workers.
    ///
    /// This should be called periodically to enable faster recovery.
    pub fn save_all_snapshots(&self) -> Result<()> {
        let workers = self
            .workers
            .lock()
            .map_err(|_| DispatchError::LockPoisoned)?;

        for (repo, handle) in workers.iter() {
            match handle.worker.lock() {
                Ok(mut worker) => {
                    if let Err(e) = worker.save_snapshot() {
                        error!(repo = %repo, error = %e, "Failed to save snapshot");
                    }
                }
                Err(_) => {
                    warn!(repo = %repo, "Worker lock poisoned during snapshot save");
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spool::delivery::{WebhookEnvelope, spool_webhook};
    use std::collections::HashMap;
    use std::thread;
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

    // ─── Dispatcher tests ───

    #[test]
    fn dispatcher_creates_worker_on_first_event() {
        let dir = tempdir().unwrap();
        let config = DispatcherConfig::new(dir.path().join("spool"), dir.path().join("state"));

        let dispatcher = Dispatcher::new(config.clone());
        let repo = RepoId::new("owner", "repo");

        // No workers initially
        assert_eq!(dispatcher.worker_count(), 0);
        assert!(!dispatcher.has_worker(&repo));

        // Spool a delivery
        let envelope = make_pr_opened_envelope(42);
        let delivery_id = DeliveryId::new("test-delivery-1");
        spool_webhook(&config.spool_dir(&repo), &delivery_id, &envelope).unwrap();

        // Dispatch creates worker
        let delivery = SpooledDelivery::new(&config.spool_dir(&repo), delivery_id);
        dispatcher.dispatch(&repo, delivery).unwrap();

        assert_eq!(dispatcher.worker_count(), 1);
        assert!(dispatcher.has_worker(&repo));
    }

    #[test]
    fn dispatcher_reuses_existing_worker() {
        let dir = tempdir().unwrap();
        let config = DispatcherConfig::new(dir.path().join("spool"), dir.path().join("state"));

        let dispatcher = Dispatcher::new(config.clone());
        let repo = RepoId::new("owner", "repo");

        // Dispatch first event
        let envelope1 = make_pr_opened_envelope(42);
        let delivery_id1 = DeliveryId::new("test-delivery-1");
        spool_webhook(&config.spool_dir(&repo), &delivery_id1, &envelope1).unwrap();
        let delivery1 = SpooledDelivery::new(&config.spool_dir(&repo), delivery_id1);
        dispatcher.dispatch(&repo, delivery1).unwrap();

        assert_eq!(dispatcher.worker_count(), 1);

        // Dispatch second event - should reuse same worker
        let envelope2 = make_pr_opened_envelope(43);
        let delivery_id2 = DeliveryId::new("test-delivery-2");
        spool_webhook(&config.spool_dir(&repo), &delivery_id2, &envelope2).unwrap();
        let delivery2 = SpooledDelivery::new(&config.spool_dir(&repo), delivery_id2);
        dispatcher.dispatch(&repo, delivery2).unwrap();

        // Still only one worker
        assert_eq!(dispatcher.worker_count(), 1);
    }

    #[test]
    fn dispatcher_creates_separate_workers_per_repo() {
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
        dispatcher.dispatch(&repo_a, delivery_a).unwrap();

        // Dispatch to repo B
        let envelope_b = make_pr_opened_envelope(43);
        let delivery_id_b = DeliveryId::new("delivery-b");
        spool_webhook(&config.spool_dir(&repo_b), &delivery_id_b, &envelope_b).unwrap();
        let delivery_b = SpooledDelivery::new(&config.spool_dir(&repo_b), delivery_id_b);
        dispatcher.dispatch(&repo_b, delivery_b).unwrap();

        // Two separate workers
        assert_eq!(dispatcher.worker_count(), 2);
        assert!(dispatcher.has_worker(&repo_a));
        assert!(dispatcher.has_worker(&repo_b));
    }

    #[test]
    fn dispatcher_remove_worker() {
        let dir = tempdir().unwrap();
        let config = DispatcherConfig::new(dir.path().join("spool"), dir.path().join("state"));

        let dispatcher = Dispatcher::new(config.clone());
        let repo = RepoId::new("owner", "repo");

        // Create worker
        let envelope = make_pr_opened_envelope(42);
        let delivery_id = DeliveryId::new("test-delivery");
        spool_webhook(&config.spool_dir(&repo), &delivery_id, &envelope).unwrap();
        let delivery = SpooledDelivery::new(&config.spool_dir(&repo), delivery_id);
        dispatcher.dispatch(&repo, delivery).unwrap();

        assert!(dispatcher.has_worker(&repo));

        // Remove worker
        assert!(dispatcher.remove_worker(&repo));
        assert!(!dispatcher.has_worker(&repo));

        // Removing again returns false
        assert!(!dispatcher.remove_worker(&repo));
    }

    #[test]
    fn dispatcher_concurrent_access() {
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

        // Dispatch from multiple threads
        let handles: Vec<_> = (0..5)
            .map(|i| {
                let dispatcher = Arc::clone(&dispatcher);
                let repo = repo.clone();
                let config = config.clone();
                thread::spawn(move || {
                    let delivery_id = DeliveryId::new(format!("delivery-{}", i));
                    let delivery = SpooledDelivery::new(&config.spool_dir(&repo), delivery_id);
                    dispatcher.dispatch(&repo, delivery)
                })
            })
            .collect();

        // All should succeed
        for handle in handles {
            handle.join().unwrap().unwrap();
        }

        // Only one worker created (all for same repo)
        assert_eq!(dispatcher.worker_count(), 1);
    }
}
