//! GitHub API error types.
//!
//! This module defines error types that distinguish between transient and permanent
//! GitHub API failures. The distinction is critical for retry logic:
//!
//! - **Transient** errors are retriable (5xx, rate limits, certain 4xx with specific messages)
//! - **Permanent** errors require human intervention (most 4xx, merge conflicts, etc.)
//!
//! Special case:
//! - **SHA mismatch** (HTTP 409) on squash-merge indicates a race condition where
//!   someone pushed to the PR after we evaluated readiness. This is not retriable
//!   in the normal sense - the caller must re-evaluate PR state.

use std::fmt;
use thiserror::Error;

use crate::types::{PrNumber, Sha};

/// The kind of GitHub API error, categorized for retry decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GitHubErrorKind {
    /// Transient error - safe to retry with backoff.
    ///
    /// Examples:
    /// - HTTP 5xx (server errors)
    /// - HTTP 429 (rate limited)
    /// - HTTP 403 with rate limit headers
    /// - "Required status check is expected" (propagation delay)
    /// - "Base branch was modified" (concurrent push to main)
    /// - Network timeouts
    Transient,

    /// Permanent error - requires human intervention.
    ///
    /// Examples:
    /// - HTTP 4xx (except rate limits and specific transient messages)
    /// - "Pull request is not mergeable" (merge conflicts)
    /// - "Approving review required" (missing approval)
    /// - "Changes must be signed" (commit signing required)
    /// - PR not found (404)
    /// - Authentication failures (401, 403 non-rate-limit)
    Permanent,

    /// SHA mismatch on squash-merge (HTTP 409).
    ///
    /// This is a special error indicating that the PR's head SHA changed between
    /// when we evaluated readiness and when we attempted to merge. The caller
    /// must re-fetch the PR state and re-evaluate readiness.
    ///
    /// This is NOT a normal transient error - blindly retrying with the same
    /// SHA would just fail again. The caller must get the new SHA first.
    ShaMismatch,
}

impl GitHubErrorKind {
    /// Returns true if this error is retriable.
    ///
    /// Note: `ShaMismatch` returns false because it requires re-evaluation,
    /// not a simple retry.
    pub fn is_retriable(&self) -> bool {
        matches!(self, GitHubErrorKind::Transient)
    }
}

/// A GitHub API error with categorization for retry decisions.
#[derive(Debug, Error)]
pub struct GitHubApiError {
    /// The kind of error (transient, permanent, or SHA mismatch).
    pub kind: GitHubErrorKind,

    /// The HTTP status code, if available.
    pub status_code: Option<u16>,

    /// A human-readable description of the error.
    pub message: String,

    /// The underlying octocrab error, if available.
    #[source]
    pub source: Option<octocrab::Error>,
}

impl fmt::Display for GitHubApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.status_code {
            Some(code) => write!(f, "GitHub API error (HTTP {}): {}", code, self.message),
            None => write!(f, "GitHub API error: {}", self.message),
        }
    }
}

impl GitHubApiError {
    /// Creates a transient error from an octocrab error.
    pub fn transient(message: impl Into<String>, source: octocrab::Error) -> Self {
        let status_code = Self::extract_status_code(&source);
        Self {
            kind: GitHubErrorKind::Transient,
            status_code,
            message: message.into(),
            source: Some(source),
        }
    }

    /// Creates a permanent error from an octocrab error.
    pub fn permanent(message: impl Into<String>, source: octocrab::Error) -> Self {
        let status_code = Self::extract_status_code(&source);
        Self {
            kind: GitHubErrorKind::Permanent,
            status_code,
            message: message.into(),
            source: Some(source),
        }
    }

    /// Creates a SHA mismatch error.
    pub fn sha_mismatch(pr: PrNumber, expected: &Sha, source: octocrab::Error) -> Self {
        Self {
            kind: GitHubErrorKind::ShaMismatch,
            status_code: Some(409),
            message: format!(
                "SHA mismatch on PR {}: expected {}, but PR head has changed",
                pr, expected
            ),
            source: Some(source),
        }
    }

    /// Creates a permanent error without an octocrab source.
    pub fn permanent_without_source(message: impl Into<String>) -> Self {
        Self {
            kind: GitHubErrorKind::Permanent,
            status_code: None,
            message: message.into(),
            source: None,
        }
    }

    /// Creates a transient error without an octocrab source.
    pub fn transient_without_source(message: impl Into<String>) -> Self {
        Self {
            kind: GitHubErrorKind::Transient,
            status_code: None,
            message: message.into(),
            source: None,
        }
    }

    /// Categorizes an octocrab error.
    ///
    /// This function examines the error to determine if it's transient (retriable)
    /// or permanent. The categorization is based on:
    /// - HTTP status codes
    /// - Error message patterns for known GitHub API responses
    pub fn from_octocrab(err: octocrab::Error) -> Self {
        let status_code = Self::extract_status_code(&err);
        let message = Self::extract_message(&err);

        // Check for specific transient messages first
        if is_transient_message(&message) {
            return Self {
                kind: GitHubErrorKind::Transient,
                status_code,
                message,
                source: Some(err),
            };
        }

        // Categorize by status code
        //
        // NOTE: HTTP 409 is NOT automatically treated as ShaMismatch here.
        // A 409 can indicate:
        // - SHA mismatch on merge (head branch was modified)
        // - Merge conflicts (PR is not mergeable)
        // - Other conflicts
        //
        // The distinction is made at the call site (e.g., squash_merge) where we can
        // inspect the specific error message to determine if it's a retriable SHA
        // mismatch or a permanent merge conflict.
        let kind = match status_code {
            Some(429) => GitHubErrorKind::Transient, // Rate limited
            Some(403) if is_rate_limit_error(&message) => GitHubErrorKind::Transient,
            Some(code) if (500..600).contains(&code) => GitHubErrorKind::Transient,
            Some(_) => GitHubErrorKind::Permanent, // 4xx including 409
            None => {
                // No status code - check if it's a network error
                if is_network_error(&message) {
                    GitHubErrorKind::Transient
                } else {
                    GitHubErrorKind::Permanent
                }
            }
        };

        Self {
            kind,
            status_code,
            message,
            source: Some(err),
        }
    }

    /// Extracts the HTTP status code from an octocrab error, if present.
    ///
    /// Only `Error::GitHub` carries a status code; transport-level variants
    /// (Hyper, Service, ...) have none.
    fn extract_status_code(err: &octocrab::Error) -> Option<u16> {
        match err {
            octocrab::Error::GitHub { source, .. } => Some(source.status_code.as_u16()),
            _ => None,
        }
    }

    /// Extracts the error message from an octocrab error.
    ///
    /// For API errors this is the `message` field of GitHub's error body.
    /// For other variants we join the source chain, skipping the top-level
    /// snafu `Display` (it embeds a captured backtrace whose frame text must
    /// not leak into message-pattern checks).
    pub fn extract_message(err: &octocrab::Error) -> String {
        use std::error::Error;

        if let octocrab::Error::GitHub { source, .. } = err {
            return source.message.clone();
        }

        let mut messages = Vec::new();
        let mut current: Option<&(dyn Error + 'static)> = err.source();

        while let Some(e) = current {
            let msg = e.to_string();
            if !msg.is_empty() {
                messages.push(msg);
            }
            current = e.source();
        }

        if messages.is_empty() {
            err.to_string()
        } else {
            messages.join(": ")
        }
    }
}

/// Checks if an error message indicates a transient condition.
///
/// These messages indicate GitHub API quirks that resolve with retries:
/// - Status check propagation delays after a push
/// - Concurrent modifications to the base branch
pub fn is_transient_message(message: &str) -> bool {
    let message_lower = message.to_lowercase();

    // Status check hasn't propagated yet
    if message_lower.contains("required status check") && message_lower.contains("expected") {
        return true;
    }

    // Base branch was modified concurrently
    if message_lower.contains("base branch was modified") {
        return true;
    }

    // Generic "try again" suggestions from GitHub
    if message_lower.contains("try again") {
        return true;
    }

    false
}

/// Checks if an error message indicates a rate limit.
pub fn is_rate_limit_error(message: &str) -> bool {
    let message_lower = message.to_lowercase();
    message_lower.contains("rate limit")
        || message_lower.contains("api rate")
        || message_lower.contains("secondary rate")
        || message_lower.contains("abuse detection")
}

/// Checks if an error message indicates a network-level error.
pub fn is_network_error(message: &str) -> bool {
    let message_lower = message.to_lowercase();
    message_lower.contains("timeout")
        || message_lower.contains("connection")
        || message_lower.contains("network")
        || message_lower.contains("dns")
        || message_lower.contains("timed out")
}

#[cfg(test)]
pub(crate) mod test_support {
    use axum::body::Bytes;
    use http_body_util::BodyExt;

    /// Builds a real `octocrab::Error::GitHub` by passing a synthetic HTTP
    /// response through octocrab's own error-mapping path
    /// (`octocrab::map_github_error`). `GitHubError` is `#[non_exhaustive]`,
    /// so this is the only way to construct one outside octocrab.
    pub(crate) async fn github_error(status: u16, message: &str) -> octocrab::Error {
        let body = serde_json::json!({ "message": message }).to_string();
        let response = axum::http::Response::builder()
            .status(status)
            .body(
                http_body_util::Full::new(Bytes::from(body))
                    .map_err(|never: std::convert::Infallible| match never {})
                    .boxed(),
            )
            .expect("synthetic response must be valid");
        octocrab::map_github_error(response)
            .await
            .expect_err("non-success status must map to an error")
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::github_error;
    use super::*;

    #[tokio::test]
    async fn github_5xx_is_transient() {
        for status in 500..=599u16 {
            let err = github_error(status, "Server Error").await;
            let categorized = GitHubApiError::from_octocrab(err);
            assert_eq!(
                categorized.kind,
                GitHubErrorKind::Transient,
                "HTTP {} must be transient",
                status
            );
            assert_eq!(categorized.status_code, Some(status));
        }
    }

    #[tokio::test]
    async fn github_429_is_transient() {
        let err = github_error(429, "You have exceeded a secondary rate limit").await;
        let categorized = GitHubApiError::from_octocrab(err);
        assert_eq!(categorized.kind, GitHubErrorKind::Transient);
        assert_eq!(categorized.status_code, Some(429));
    }

    #[tokio::test]
    async fn github_403_rate_limit_is_transient() {
        let err = github_error(403, "API rate limit exceeded for installation ID 123456.").await;
        let categorized = GitHubApiError::from_octocrab(err);
        assert_eq!(categorized.kind, GitHubErrorKind::Transient);
        assert_eq!(categorized.status_code, Some(403));
    }

    #[tokio::test]
    async fn github_4xx_is_permanent() {
        // Real GitHub messages deliberately free of status-code digits, so a
        // string-sniffing implementation cannot pass by accident.
        let cases: &[(u16, &str)] = &[
            (400, "Problems parsing JSON"),
            (401, "Bad credentials"),
            (403, "Resource not accessible by integration"),
            (404, "Not Found"),
            (405, "Pull Request is not mergeable"),
            (409, "Merge conflict"),
            (410, "This repository is empty."),
            (422, "Validation Failed"),
        ];
        for &(status, message) in cases {
            let err = github_error(status, message).await;
            let categorized = GitHubApiError::from_octocrab(err);
            assert_eq!(
                categorized.kind,
                GitHubErrorKind::Permanent,
                "HTTP {} ({}) must be permanent",
                status,
                message
            );
            assert_eq!(categorized.status_code, Some(status));
        }
    }

    #[tokio::test]
    async fn github_error_message_is_extracted() {
        let err = github_error(404, "Branch not protected").await;
        let categorized = GitHubApiError::from_octocrab(err);
        assert!(
            categorized.message.contains("Branch not protected"),
            "message was: {}",
            categorized.message
        );
    }

    #[test]
    fn transient_message_detection() {
        assert!(is_transient_message(
            "Required status check 'ci/test' is expected"
        ));
        assert!(is_transient_message("Base branch was modified"));
        assert!(is_transient_message("Please try again later"));
        assert!(!is_transient_message("Pull request is not mergeable"));
        assert!(!is_transient_message("Approving review required"));
    }

    #[test]
    fn rate_limit_detection() {
        assert!(is_rate_limit_error("API rate limit exceeded"));
        assert!(is_rate_limit_error("secondary rate limit"));
        assert!(is_rate_limit_error("abuse detection mechanism"));
        assert!(!is_rate_limit_error("Permission denied"));
    }

    #[test]
    fn network_error_detection() {
        assert!(is_network_error("connection timeout"));
        assert!(is_network_error("DNS resolution failed"));
        assert!(is_network_error("request timed out"));
        assert!(!is_network_error("Not found"));
    }

    #[test]
    fn error_kind_retriable() {
        assert!(GitHubErrorKind::Transient.is_retriable());
        assert!(!GitHubErrorKind::Permanent.is_retriable());
        assert!(!GitHubErrorKind::ShaMismatch.is_retriable());
    }
}
