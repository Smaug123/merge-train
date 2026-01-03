//! Effects-as-data for GitHub and Git operations.
//!
//! This module defines effect types that describe operations without executing them.
//! This enables:
//! - Pure core logic that returns effects as data
//! - Testability via mock interpreters
//! - Logging/tracing of intended operations
//! - Recovery by knowing what was attempted
//!
//! Interpreters that execute these effects are defined in later stages.

use serde::{Deserialize, Serialize};

pub mod git;
pub mod github;
pub mod interpreter;

pub use git::{GitEffect, GitResponse, MergeStrategy};
pub use github::{
    BranchProtectionData, CommentData, GitHubEffect, GitHubResponse, PrData, Reaction,
    RepoSettingsData, RulesetData,
};
pub use interpreter::{GitHubInterpreter, GitInterpreter};

/// A unified effect type encompassing both Git and GitHub operations.
///
/// This is the primary effect type returned by cascade operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "effect_type", rename_all = "snake_case")]
pub enum Effect {
    /// A Git operation.
    Git(GitEffect),
    /// A GitHub API operation.
    GitHub(GitHubEffect),
}
