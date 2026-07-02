//! Effects-as-data for GitHub and Git operations.
//!
//! This module defines effect types that describe operations without executing them.
//! This enables:
//! - Pure core logic that returns effects as data
//! - Testability without I/O
//! - Logging/tracing of intended operations
//! - Recovery by knowing what was attempted
//!
//! GitHub effects are executed by `crate::github::interpret_github_effect`.

use serde::{Deserialize, Serialize};

pub mod git;
pub mod github;

pub use git::{GitEffect, GitResponse, MergeOutcome, MergeStrategy, PushOutcome, PushPoint};
pub use github::{
    BranchProtectionData, CommentData, GitHubEffect, GitHubResponse, PrData, Reaction,
    RepoSettingsData, RulesetData,
};

/// A unified effect type encompassing both Git and GitHub operations.
///
/// This is the primary effect type returned by cascade operations.
///
/// Note: there is deliberately no variant for "append a state event" (the old
/// `RecordReconciliation`): the cascade engine emits state events directly in
/// `StepPlan.events`, where their ordering relative to the other events in the
/// batch is atomic under `Store::append` rather than depending on executor
/// discipline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "effect_type", rename_all = "snake_case")]
pub enum Effect {
    /// A Git operation.
    Git(GitEffect),
    /// A GitHub API operation.
    #[serde(rename = "github")]
    GitHub(GitHubEffect),
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// Test that Effect serializes with expected tag values.
    /// This locks the wire format to prevent accidental breaking changes.
    #[test]
    fn effect_tag_values() {
        // Git variant
        let git_effect = Effect::Git(GitEffect::CleanupWorktree);
        let json = serde_json::to_string(&git_effect).unwrap();
        assert!(
            json.contains(r#""effect_type":"git""#),
            "Git variant should serialize with tag 'git', got: {}",
            json
        );

        // GitHub variant - explicitly renamed to avoid "git_hub"
        let github_effect = Effect::GitHub(GitHubEffect::GetRepoSettings);
        let json = serde_json::to_string(&github_effect).unwrap();
        assert!(
            json.contains(r#""effect_type":"github""#),
            "GitHub variant should serialize with tag 'github', got: {}",
            json
        );
    }

    proptest! {
        #[test]
        fn effect_serde_roundtrip_git(refspecs in prop::collection::vec("[a-z]{1,10}", 1..3)) {
            let effect = Effect::Git(GitEffect::Fetch { refspecs });
            let json = serde_json::to_string(&effect).unwrap();
            let parsed: Effect = serde_json::from_str(&json).unwrap();
            prop_assert_eq!(effect, parsed);
        }
    }

    #[test]
    fn effect_serde_roundtrip_github() {
        let effect = Effect::GitHub(GitHubEffect::GetRepoSettings);
        let json = serde_json::to_string(&effect).unwrap();
        let parsed: Effect = serde_json::from_str(&json).unwrap();
        assert_eq!(effect, parsed);
    }
}
