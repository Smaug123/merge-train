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

pub mod git;
pub mod github;
pub mod interpreter;

pub use git::{GitEffect, GitResponse, MergeStrategy};
pub use github::{
    BranchProtectionData, CommentData, GitHubEffect, GitHubResponse, PrData, Reaction,
    RepoSettingsData, RulesetData,
};
pub use interpreter::{GitHubInterpreter, GitInterpreter};
