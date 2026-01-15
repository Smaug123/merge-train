//! Polling configuration for active train recovery.
//!
//! Webhooks are the primary trigger for cascade evaluation, but they're not
//! sufficient alone. Network issues, server downtime, or GitHub outages can
//! cause missed webhooks. This module provides polling as a fallback.
//!
//! # Polling Strategy
//!
//! - **Poll interval**: 10 minutes by default (configurable via `MERGE_TRAIN_POLL_INTERVAL_MINS`)
//! - **Jitter**: 0-20% added to prevent thundering herd on restart
//! - **Initial stagger**: Based on repo ID hash to distribute load
//!
//! # Non-blocking Waits
//!
//! When waiting for GitHub state to propagate (e.g., headRefOid to match after push),
//! the bot uses timer-based re-evaluation instead of blocking:
//!
//! - **Recheck interval**: 5 seconds between condition checks
//! - **Timeout**: 5 minutes before treating as transient failure

use std::hash::{Hash, Hasher};
use std::time::Duration;

use crate::types::RepoId;

/// Default poll interval for active trains (10 minutes).
const DEFAULT_POLL_INTERVAL_SECS: u64 = 600;

/// Default timeout for waiting on GitHub state propagation (5 minutes).
const DEFAULT_WAIT_TIMEOUT_SECS: u64 = 300;

/// Default interval for rechecking wait conditions (5 seconds).
const DEFAULT_RECHECK_INTERVAL_SECS: u64 = 5;

/// Default jitter percentage (0-100).
const DEFAULT_JITTER_PERCENT: u8 = 20;

/// Configuration for polling and non-blocking waits.
#[derive(Debug, Clone)]
pub struct PollConfig {
    /// Interval between polls for active trains.
    ///
    /// Default: 10 minutes. Configure via `MERGE_TRAIN_POLL_INTERVAL_MINS`.
    pub poll_interval: Duration,

    /// Timeout for waiting on GitHub state propagation.
    ///
    /// After this duration, the wait is treated as a transient failure.
    /// Default: 5 minutes.
    pub wait_timeout: Duration,

    /// Interval between rechecks when waiting for a condition.
    ///
    /// Default: 5 seconds.
    pub recheck_interval: Duration,

    /// Jitter percentage to add to poll interval (0-100).
    ///
    /// Used to prevent thundering herd when multiple bot instances restart.
    /// Default: 20 (meaning 0-20% jitter).
    pub jitter_percent: u8,
}

impl Default for PollConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl PollConfig {
    /// Creates a new `PollConfig` with default values.
    pub fn new() -> Self {
        PollConfig {
            poll_interval: Duration::from_secs(DEFAULT_POLL_INTERVAL_SECS),
            wait_timeout: Duration::from_secs(DEFAULT_WAIT_TIMEOUT_SECS),
            recheck_interval: Duration::from_secs(DEFAULT_RECHECK_INTERVAL_SECS),
            jitter_percent: DEFAULT_JITTER_PERCENT,
        }
    }

    /// Creates a `PollConfig` from environment variables.
    ///
    /// Reads `MERGE_TRAIN_POLL_INTERVAL_MINS` for the poll interval.
    /// Other values use defaults.
    pub fn from_env() -> Self {
        let poll_mins = std::env::var("MERGE_TRAIN_POLL_INTERVAL_MINS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(DEFAULT_POLL_INTERVAL_SECS / 60);

        PollConfig {
            poll_interval: Duration::from_secs(poll_mins * 60),
            ..Self::new()
        }
    }

    /// Returns the poll interval with jitter added for a specific repository.
    ///
    /// The jitter is deterministic based on the repo ID hash, ensuring the same
    /// repo always gets the same jitter. This prevents thundering herd while
    /// maintaining predictability.
    ///
    /// # Formula
    ///
    /// `interval * (1 + (hash(repo) % jitter_percent) / 100)`
    pub fn poll_interval_with_jitter(&self, repo: &RepoId) -> Duration {
        let jitter_factor = self.jitter_factor(repo);
        Duration::from_secs_f64(self.poll_interval.as_secs_f64() * jitter_factor)
    }

    /// Returns the initial delay before first poll for a specific repository.
    ///
    /// This staggers initial polls based on repo ID hash to distribute load
    /// when multiple bot instances restart.
    ///
    /// # Formula
    ///
    /// `hash(repo) % (poll_interval / 2)`
    pub fn initial_poll_delay(&self, repo: &RepoId) -> Duration {
        let hash = self.repo_hash(repo);
        let max_delay = self.poll_interval.as_secs() / 2;
        let delay_secs = hash % max_delay.max(1);
        Duration::from_secs(delay_secs)
    }

    /// Computes the jitter factor for a repository.
    ///
    /// Returns a value between 1.0 and 1.0 + (jitter_percent / 100).
    fn jitter_factor(&self, repo: &RepoId) -> f64 {
        if self.jitter_percent == 0 {
            return 1.0;
        }
        let hash = self.repo_hash(repo);
        let jitter = (hash % self.jitter_percent as u64) as f64 / 100.0;
        1.0 + jitter
    }

    /// Computes a hash of the repository ID.
    fn repo_hash(&self, repo: &RepoId) -> u64 {
        let mut hasher = std::hash::DefaultHasher::new();
        repo.hash(&mut hasher);
        hasher.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_has_expected_values() {
        let config = PollConfig::new();

        assert_eq!(config.poll_interval, Duration::from_secs(600));
        assert_eq!(config.wait_timeout, Duration::from_secs(300));
        assert_eq!(config.recheck_interval, Duration::from_secs(5));
        assert_eq!(config.jitter_percent, 20);
    }

    #[test]
    fn jitter_is_deterministic() {
        let config = PollConfig::new();
        let repo = RepoId::new("owner", "repo");

        let jitter1 = config.poll_interval_with_jitter(&repo);
        let jitter2 = config.poll_interval_with_jitter(&repo);

        assert_eq!(jitter1, jitter2);
    }

    #[test]
    fn different_repos_get_different_jitter() {
        let config = PollConfig::new();
        let repo_a = RepoId::new("owner", "repo-a");
        let repo_b = RepoId::new("owner", "repo-b");

        let jitter_a = config.poll_interval_with_jitter(&repo_a);
        let jitter_b = config.poll_interval_with_jitter(&repo_b);

        // Different repos should likely get different jitter
        // (not guaranteed, but highly probable with good hash)
        // At minimum, both should be within expected range
        assert!(jitter_a >= config.poll_interval);
        assert!(jitter_a <= config.poll_interval.mul_f64(1.2));
        assert!(jitter_b >= config.poll_interval);
        assert!(jitter_b <= config.poll_interval.mul_f64(1.2));
    }

    #[test]
    fn zero_jitter_returns_exact_interval() {
        let config = PollConfig {
            jitter_percent: 0,
            ..PollConfig::new()
        };
        let repo = RepoId::new("owner", "repo");

        let interval = config.poll_interval_with_jitter(&repo);

        assert_eq!(interval, config.poll_interval);
    }

    #[test]
    fn initial_delay_is_deterministic() {
        let config = PollConfig::new();
        let repo = RepoId::new("owner", "repo");

        let delay1 = config.initial_poll_delay(&repo);
        let delay2 = config.initial_poll_delay(&repo);

        assert_eq!(delay1, delay2);
    }

    #[test]
    fn initial_delay_is_within_bounds() {
        let config = PollConfig::new();
        let repo = RepoId::new("owner", "repo");

        let delay = config.initial_poll_delay(&repo);
        let max_delay = config.poll_interval / 2;

        assert!(delay < max_delay);
    }
}
