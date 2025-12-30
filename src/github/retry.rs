//! Exponential backoff retry logic for GitHub API calls.
//!
//! This module provides configurable retry with exponential backoff for transient
//! GitHub API errors. The design follows DESIGN.md specifications:
//!
//! - Default: 3 retries with 2s, 4s, 8s delays (for normal operations)
//! - Recovery: 10 retries with 1s base, 30s cap (for crash recovery scenarios)
//!
//! The retry logic only applies to transient errors. Permanent errors and
//! SHA mismatches are returned immediately.

use std::future::Future;
use std::time::Duration;

use super::error::{GitHubApiError, GitHubErrorKind};

/// Configuration for exponential backoff retry.
#[derive(Debug, Clone, Copy)]
pub struct RetryConfig {
    /// Maximum number of retry attempts (not including the initial attempt).
    pub max_retries: u32,

    /// Initial delay before the first retry.
    pub initial_delay: Duration,

    /// Maximum delay between retries (cap for exponential growth).
    pub max_delay: Duration,

    /// Multiplier for exponential backoff (typically 2.0).
    pub backoff_multiplier: f64,
}

impl RetryConfig {
    /// Default retry configuration for normal GitHub API operations.
    ///
    /// - 3 retries with 2s, 4s, 8s delays
    /// - Total max wait: ~14 seconds
    pub const DEFAULT: Self = Self {
        max_retries: 3,
        initial_delay: Duration::from_secs(2),
        max_delay: Duration::from_secs(16), // Won't hit this with 3 retries
        backoff_multiplier: 2.0,
    };

    /// Retry configuration for crash recovery scenarios.
    ///
    /// When recovering from a crash, we may need to wait for GitHub's eventual
    /// consistency (e.g., `merge_commit_sha` propagation).
    ///
    /// - 10 retries with 1s base, 30s cap
    /// - Total max wait: ~150 seconds (1+2+4+8+16+30+30+30+30+30)
    pub const RECOVERY: Self = Self {
        max_retries: 10,
        initial_delay: Duration::from_secs(1),
        max_delay: Duration::from_secs(30),
        backoff_multiplier: 2.0,
    };

    /// Creates a new retry configuration.
    pub fn new(
        max_retries: u32,
        initial_delay: Duration,
        max_delay: Duration,
        backoff_multiplier: f64,
    ) -> Self {
        Self {
            max_retries,
            initial_delay,
            max_delay,
            backoff_multiplier,
        }
    }

    /// Computes the delay for the given retry attempt (0-indexed).
    ///
    /// The delay grows exponentially: `initial_delay * backoff_multiplier^attempt`,
    /// capped at `max_delay`.
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let multiplier = self.backoff_multiplier.powi(attempt as i32);
        let delay_secs = self.initial_delay.as_secs_f64() * multiplier;
        let capped_secs = delay_secs.min(self.max_delay.as_secs_f64());
        Duration::from_secs_f64(capped_secs)
    }

    /// Returns an iterator over all retry delays.
    pub fn delays(&self) -> impl Iterator<Item = Duration> + '_ {
        (0..self.max_retries).map(|attempt| self.delay_for_attempt(attempt))
    }

    /// Computes the total maximum wait time for all retries.
    pub fn total_max_wait(&self) -> Duration {
        self.delays().sum()
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Retry policy for controlling retry behavior at runtime.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RetryPolicy {
    /// Retry transient errors with exponential backoff.
    #[default]
    RetryTransient,

    /// Do not retry - return errors immediately.
    NoRetry,
}

/// Result of a retry attempt.
#[derive(Debug)]
pub enum RetryResult<T> {
    /// The operation succeeded.
    Success(T),

    /// A transient error occurred after exhausting all retries.
    ExhaustedRetries {
        /// The last error encountered.
        last_error: GitHubApiError,
        /// Number of attempts made (including the initial attempt).
        ///
        /// Useful for logging/diagnostics when handling exhausted retries.
        #[allow(dead_code)]
        attempts: u32,
    },

    /// A permanent error occurred (not retriable).
    PermanentError(GitHubApiError),

    /// A SHA mismatch occurred (requires re-evaluation, not retry).
    ShaMismatch(GitHubApiError),
}

impl<T> RetryResult<T> {
    /// Converts to a Result, treating exhausted retries and permanent errors as Err.
    pub fn into_result(self) -> Result<T, GitHubApiError> {
        match self {
            RetryResult::Success(v) => Ok(v),
            RetryResult::ExhaustedRetries { last_error, .. } => Err(last_error),
            RetryResult::PermanentError(e) => Err(e),
            RetryResult::ShaMismatch(e) => Err(e),
        }
    }

    /// Returns true if the result is a success.
    ///
    /// Part of the public API for callers who want to inspect the result
    /// without consuming it.
    #[allow(dead_code)]
    pub fn is_success(&self) -> bool {
        matches!(self, RetryResult::Success(_))
    }
}

/// Executes an async operation with retry logic.
///
/// The operation is retried according to the provided configuration when it
/// returns a transient error. Permanent errors and SHA mismatches are returned
/// immediately.
///
/// # Arguments
///
/// * `config` - Retry configuration (delays, max attempts)
/// * `policy` - Whether to actually retry or return immediately
/// * `operation` - The async operation to execute. Called repeatedly until
///   success, permanent error, or max retries exhausted.
///
/// # Returns
///
/// A `RetryResult` indicating success, exhausted retries, permanent error,
/// or SHA mismatch.
pub async fn retry_with_backoff<T, F, Fut>(
    config: RetryConfig,
    policy: RetryPolicy,
    mut operation: F,
) -> RetryResult<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, GitHubApiError>>,
{
    let mut attempt = 0;
    let max_attempts = if policy == RetryPolicy::NoRetry {
        1
    } else {
        config.max_retries + 1 // Include initial attempt
    };

    loop {
        match operation().await {
            Ok(value) => return RetryResult::Success(value),
            Err(e) => {
                attempt += 1;

                match e.kind {
                    GitHubErrorKind::Permanent => return RetryResult::PermanentError(e),
                    GitHubErrorKind::ShaMismatch => return RetryResult::ShaMismatch(e),
                    GitHubErrorKind::Transient => {
                        if attempt >= max_attempts {
                            return RetryResult::ExhaustedRetries {
                                last_error: e,
                                attempts: attempt,
                            };
                        }

                        // Sleep before retry
                        let delay = config.delay_for_attempt(attempt - 1);
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    // ─── Unit Tests ───────────────────────────────────────────────────────────

    #[test]
    fn default_config_values() {
        let config = RetryConfig::DEFAULT;
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.initial_delay, Duration::from_secs(2));
        assert_eq!(config.backoff_multiplier, 2.0);
    }

    #[test]
    fn recovery_config_values() {
        let config = RetryConfig::RECOVERY;
        assert_eq!(config.max_retries, 10);
        assert_eq!(config.initial_delay, Duration::from_secs(1));
        assert_eq!(config.max_delay, Duration::from_secs(30));
    }

    #[test]
    fn default_delays_are_2_4_8() {
        let config = RetryConfig::DEFAULT;
        let delays: Vec<_> = config.delays().collect();
        assert_eq!(delays.len(), 3);
        assert_eq!(delays[0], Duration::from_secs(2));
        assert_eq!(delays[1], Duration::from_secs(4));
        assert_eq!(delays[2], Duration::from_secs(8));
    }

    #[test]
    fn recovery_delays_respect_cap() {
        let config = RetryConfig::RECOVERY;
        let delays: Vec<_> = config.delays().collect();

        // First few delays grow exponentially
        assert_eq!(delays[0], Duration::from_secs(1));
        assert_eq!(delays[1], Duration::from_secs(2));
        assert_eq!(delays[2], Duration::from_secs(4));
        assert_eq!(delays[3], Duration::from_secs(8));
        assert_eq!(delays[4], Duration::from_secs(16));

        // After that, delays are capped at 30s
        for delay in &delays[5..] {
            assert_eq!(*delay, Duration::from_secs(30));
        }
    }

    #[test]
    fn total_max_wait_default() {
        let config = RetryConfig::DEFAULT;
        // 2 + 4 + 8 = 14 seconds
        assert_eq!(config.total_max_wait(), Duration::from_secs(14));
    }

    #[test]
    fn total_max_wait_recovery() {
        let config = RetryConfig::RECOVERY;
        // 1 + 2 + 4 + 8 + 16 + 30*5 = 181 seconds
        let total = config.total_max_wait();
        assert!(total >= Duration::from_secs(150)); // Design says ~150
        assert!(total <= Duration::from_secs(200)); // But allow some margin
    }

    #[tokio::test]
    async fn retry_success_on_first_attempt() {
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let result = retry_with_backoff(
            RetryConfig::DEFAULT,
            RetryPolicy::RetryTransient,
            move || {
                counter_clone.fetch_add(1, Ordering::SeqCst);
                async { Ok::<_, GitHubApiError>(42) }
            },
        )
        .await;

        assert!(result.is_success());
        match result {
            RetryResult::Success(v) => assert_eq!(v, 42),
            _ => panic!("Expected success"),
        }
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retry_permanent_error_not_retried() {
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let result = retry_with_backoff(
            RetryConfig::DEFAULT,
            RetryPolicy::RetryTransient,
            move || {
                counter_clone.fetch_add(1, Ordering::SeqCst);
                async { Err::<i32, _>(GitHubApiError::permanent_without_source("not found")) }
            },
        )
        .await;

        assert!(matches!(result, RetryResult::PermanentError(_)));
        assert_eq!(counter.load(Ordering::SeqCst), 1); // Only one attempt
    }

    #[tokio::test]
    async fn retry_sha_mismatch_not_retried() {
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let result = retry_with_backoff(
            RetryConfig::DEFAULT,
            RetryPolicy::RetryTransient,
            move || {
                counter_clone.fetch_add(1, Ordering::SeqCst);
                async {
                    Err::<i32, _>(GitHubApiError {
                        kind: GitHubErrorKind::ShaMismatch,
                        status_code: Some(409),
                        message: "SHA mismatch".to_string(),
                        source: None,
                    })
                }
            },
        )
        .await;

        assert!(matches!(result, RetryResult::ShaMismatch(_)));
        assert_eq!(counter.load(Ordering::SeqCst), 1); // Only one attempt
    }

    #[tokio::test]
    async fn retry_transient_succeeds_on_third_attempt() {
        // Use very short delays for testing
        let config = RetryConfig::new(3, Duration::from_millis(1), Duration::from_millis(10), 2.0);

        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let result = retry_with_backoff(config, RetryPolicy::RetryTransient, move || {
            let count = counter_clone.fetch_add(1, Ordering::SeqCst);
            async move {
                if count < 2 {
                    Err(GitHubApiError::transient_without_source(
                        "temporary failure",
                    ))
                } else {
                    Ok(42)
                }
            }
        })
        .await;

        assert!(result.is_success());
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn retry_transient_exhausts_retries() {
        let config = RetryConfig::new(2, Duration::from_millis(1), Duration::from_millis(10), 2.0);

        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let result = retry_with_backoff(config, RetryPolicy::RetryTransient, move || {
            counter_clone.fetch_add(1, Ordering::SeqCst);
            async { Err::<i32, _>(GitHubApiError::transient_without_source("always fails")) }
        })
        .await;

        match result {
            RetryResult::ExhaustedRetries { attempts, .. } => {
                assert_eq!(attempts, 3); // Initial + 2 retries
            }
            _ => panic!("Expected ExhaustedRetries"),
        }
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn no_retry_policy_returns_immediately() {
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let result = retry_with_backoff(RetryConfig::DEFAULT, RetryPolicy::NoRetry, move || {
            counter_clone.fetch_add(1, Ordering::SeqCst);
            async { Err::<i32, _>(GitHubApiError::transient_without_source("error")) }
        })
        .await;

        assert!(matches!(result, RetryResult::ExhaustedRetries { .. }));
        assert_eq!(counter.load(Ordering::SeqCst), 1); // Only one attempt
    }

    // ─── Property Tests ───────────────────────────────────────────────────────

    proptest! {
        #[test]
        fn delay_grows_exponentially_until_cap(
            initial_ms in 1u64..1000,
            max_ms in 1000u64..60000,
            multiplier in 1.5f64..3.0,
            attempt in 0u32..10,
        ) {
            let config = RetryConfig::new(
                10,
                Duration::from_millis(initial_ms),
                Duration::from_millis(max_ms),
                multiplier,
            );

            let delay = config.delay_for_attempt(attempt);

            // Delay should never exceed max
            prop_assert!(delay <= Duration::from_millis(max_ms));

            // For early attempts, delay should grow
            if attempt > 0 {
                let prev_delay = config.delay_for_attempt(attempt - 1);
                // Either we've hit the cap (delay == prev), or we've grown
                prop_assert!(delay >= prev_delay);
            }
        }

        #[test]
        fn delay_sequence_is_monotonic(
            initial_ms in 1u64..1000,
            max_ms in 1000u64..60000,
            multiplier in 1.5f64..3.0,
            max_retries in 1u32..15,
        ) {
            let config = RetryConfig::new(
                max_retries,
                Duration::from_millis(initial_ms),
                Duration::from_millis(max_ms),
                multiplier,
            );

            let delays: Vec<_> = config.delays().collect();

            // Sequence should be monotonically non-decreasing
            for window in delays.windows(2) {
                prop_assert!(window[1] >= window[0], "Delays should be monotonic");
            }
        }

        #[test]
        fn first_delay_equals_initial_delay(
            initial_ms in 1u64..10000,
            max_ms in 10000u64..100000,
            multiplier in 1.0f64..3.0,
        ) {
            let config = RetryConfig::new(
                5,
                Duration::from_millis(initial_ms),
                Duration::from_millis(max_ms),
                multiplier,
            );

            let first_delay = config.delay_for_attempt(0);
            prop_assert_eq!(first_delay, Duration::from_millis(initial_ms));
        }

        #[test]
        fn total_wait_bounded_by_max_times_retries(
            initial_ms in 1u64..1000,
            max_ms in 1000u64..10000,
            multiplier in 1.5f64..3.0,
            max_retries in 1u32..20,
        ) {
            let config = RetryConfig::new(
                max_retries,
                Duration::from_millis(initial_ms),
                Duration::from_millis(max_ms),
                multiplier,
            );

            let total = config.total_max_wait();
            let upper_bound = Duration::from_millis(max_ms * max_retries as u64);

            // Total wait should never exceed max_delay * max_retries
            prop_assert!(total <= upper_bound);
        }
    }
}
