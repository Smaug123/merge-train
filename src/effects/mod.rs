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
    #[serde(rename = "github")]
    GitHub(GitHubEffect),
    /// Record that a PR has been reconciled with its predecessor's squash commit.
    ///
    /// This updates `predecessor_squash_reconciled` on the PR, which is required
    /// for `is_root()` to recognize retargeted descendants as valid new roots.
    /// Without this, fan-out descendants cannot start new trains after cascade.
    RecordReconciliation {
        /// The PR that was reconciled.
        pr: PrNumber,
        /// The squash commit SHA that was reconciled into this PR's branch.
        squash_sha: Sha,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn arb_sha() -> impl Strategy<Value = Sha> {
        "[0-9a-f]{40}".prop_map(|s| Sha::parse(&s).unwrap())
    }

    fn arb_pr_number() -> impl Strategy<Value = PrNumber> {
        any::<u64>().prop_map(PrNumber)
    }

    /// Test that Effect serializes with expected tag values.
    /// This locks the wire format to prevent accidental breaking changes.
    #[test]
    fn effect_tag_values() {
        // Git variant
        let git_effect = Effect::Git(GitEffect::MergeAbort);
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

        // RecordReconciliation variant
        let reconcile_effect = Effect::RecordReconciliation {
            pr: PrNumber(123),
            squash_sha: Sha::parse("a".repeat(40)).unwrap(),
        };
        let json = serde_json::to_string(&reconcile_effect).unwrap();
        assert!(
            json.contains(r#""effect_type":"record_reconciliation""#),
            "RecordReconciliation variant should serialize with tag 'record_reconciliation', got: {}",
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

        #[test]
        fn effect_serde_roundtrip_github(_unused in 0u8..1) {
            let effect = Effect::GitHub(GitHubEffect::GetRepoSettings);
            let json = serde_json::to_string(&effect).unwrap();
            let parsed: Effect = serde_json::from_str(&json).unwrap();
            prop_assert_eq!(effect, parsed);
        }

        #[test]
        fn effect_serde_roundtrip_record_reconciliation(pr in arb_pr_number(), squash_sha in arb_sha()) {
            let effect = Effect::RecordReconciliation { pr, squash_sha };
            let json = serde_json::to_string(&effect).unwrap();
            let parsed: Effect = serde_json::from_str(&json).unwrap();
            prop_assert_eq!(effect, parsed);
        }
    }
}
