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

use crate::types::{PrNumber, Sha};

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
    /// Record that a PR has been reconciled with its predecessor's squash commit.
    ///
    /// This updates `predecessor_squash_reconciled` on the PR, which is CRITICAL
    /// for `is_root()` to recognize retargeted descendants as valid new roots.
    /// Without this, fan-out descendants cannot start new trains after cascade.
    ///
    /// DESIGN.md: "Record that reconciliation completed — CRITICAL for is_root() to return true"
    RecordReconciliation {
        /// The PR that was reconciled.
        pr: PrNumber,
        /// The squash commit SHA that was reconciled into this PR's branch.
        squash_sha: Sha,
    },
}
