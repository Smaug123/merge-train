//! GitHub API client and effect interpreter.
//!
//! This module provides the implementation for executing GitHub effects via the octocrab
//! library. It implements the `GitHubInterpreter` trait defined in the effects module.
//!
//! Key features:
//! - Exponential backoff retry for transient failures
//! - Distinguishes transient vs permanent errors
//! - SHA guard on squash-merge to prevent racing pushes
//! - GraphQL for mergeStateStatus queries

mod client;
mod error;
mod interpreter;
mod retry;

pub use client::OctocrabClient;
pub use error::{GitHubApiError, GitHubErrorKind};
pub use interpreter::{
    interpret_github_effect, is_sha_mismatch_error, resolve_merge_state, should_fallback_to_unknown,
};
pub use retry::{RetryConfig, RetryPolicy};
