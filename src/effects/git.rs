//! Git operation effect types.
//!
//! These types describe git operations as data, without executing them.
//! The interpreter (implemented in a later stage) executes these effects
//! against a local git repository.

use serde::{Deserialize, Serialize};

use crate::types::Sha;

/// Git merge strategy.
///
/// Note: These are merge *strategies* (`-s <strategy>`), not strategy *options* (`-X <option>`).
/// Git's `-s ours` strategy is fundamentally different from `-X ours` option:
/// - `-s ours`: Completely ignores the other branch's tree; result is always our tree
/// - `-X ours`: Uses recursive/ort strategy but prefers our changes on conflict
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MergeStrategy {
    /// Default merge strategy (recursive/ort).
    Default,
    /// Ours strategy (`git merge -s ours`): keep our tree entirely, ignore theirs.
    ///
    /// This creates a merge commit with the other branch as a parent, but the
    /// resulting tree is identical to our HEAD. Used during reconciliation to
    /// mark the squash commit as an ancestor without changing the working tree
    /// (which already has the correct content from merging `$SQUASH_SHA^`).
    Ours,
}

impl MergeStrategy {
    /// Returns the git command-line argument for this strategy, if any.
    ///
    /// Returns `None` for `Default` (no `-s` flag needed).
    /// Returns `Some("ours")` for `Ours` (use with `-s ours`).
    pub fn as_git_arg(&self) -> Option<&'static str> {
        match self {
            MergeStrategy::Default => None,
            MergeStrategy::Ours => Some("ours"),
        }
    }
}

/// A git operation effect.
///
/// Each variant describes a git command. Effects are worktree-scoped:
/// the interpreter is constructed with a worktree path.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GitEffect {
    /// Fetch refs from a remote.
    Fetch {
        /// Refspecs to fetch (e.g., ["origin/main", "refs/pull/123/head:refs/remotes/origin/pr/123"]).
        refspecs: Vec<String>,
    },

    /// Checkout a ref or commit.
    Checkout {
        /// The target to checkout (branch name, tag, or SHA).
        target: String,
        /// If true, checkout in detached HEAD mode.
        detach: bool,
    },

    /// Merge a branch or commit into HEAD.
    Merge {
        /// The target to merge (branch name or SHA).
        target: String,
        /// The merge strategy to use.
        strategy: MergeStrategy,
        /// The commit message for the merge.
        message: String,
    },

    /// Push refs to a remote.
    Push {
        /// The refspec to push (e.g., "HEAD:refs/heads/feature").
        refspec: String,
        /// If true, force push.
        force: bool,
    },

    /// Check if one commit is an ancestor of another.
    IsAncestor {
        /// The potential ancestor commit.
        potential_ancestor: Sha,
        /// The potential descendant commit.
        descendant: Sha,
    },

    /// Parse a revision to a SHA.
    RevParse {
        /// The revision to parse (e.g., "HEAD", "main", "abc123^").
        rev: String,
    },

    /// Create a new worktree.
    CreateWorktree {
        /// The name for the worktree (used as directory name).
        name: String,
    },

    /// Remove a worktree.
    RemoveWorktree {
        /// The name of the worktree to remove.
        name: String,
    },

    /// Abort an in-progress merge.
    MergeAbort,

    /// Hard reset to a target.
    ResetHard {
        /// The target to reset to (branch, SHA, etc.).
        target: String,
    },

    /// Clean untracked files.
    Clean {
        /// If true, also remove untracked directories.
        directories: bool,
        /// If true, force clean (required for actual removal).
        force: bool,
    },
}

/// Response from a git effect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum GitResponse {
    /// Operation completed successfully with no specific return value.
    Ok,
    /// Operation returned a SHA (e.g., from RevParse).
    Sha(Sha),
    /// Operation returned a boolean (e.g., from IsAncestor).
    Bool(bool),
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    fn hash<T: Hash>(t: &T) -> u64 {
        let mut s = DefaultHasher::new();
        t.hash(&mut s);
        s.finish()
    }

    // ─── Arbitrary Generators ─────────────────────────────────────────────────

    fn arb_sha() -> impl Strategy<Value = Sha> {
        "[0-9a-f]{40}".prop_map(|s| Sha::parse(s).unwrap())
    }

    fn arb_refspec() -> impl Strategy<Value = String> {
        prop_oneof![
            // Simple branch name
            "[a-zA-Z][a-zA-Z0-9_/-]{0,30}".prop_map(|s| s.to_string()),
            // Full refspec with colon
            (
                "[a-zA-Z][a-zA-Z0-9_/-]{0,20}",
                "[a-zA-Z][a-zA-Z0-9_/-]{0,20}"
            )
                .prop_map(|(src, dst)| format!("{}:{}", src, dst)),
            // PR ref pattern
            (1u32..10000).prop_map(|n| format!("refs/pull/{}/head", n)),
        ]
    }

    fn arb_target() -> impl Strategy<Value = String> {
        prop_oneof![
            // Branch name
            "[a-zA-Z][a-zA-Z0-9_/-]{0,30}".prop_map(|s| s.to_string()),
            // SHA
            arb_sha().prop_map(|sha| sha.as_str().to_string()),
            // Remote branch
            "[a-zA-Z][a-zA-Z0-9_/-]{0,15}".prop_map(|s| format!("origin/{}", s)),
        ]
    }

    fn arb_merge_strategy() -> impl Strategy<Value = MergeStrategy> {
        prop_oneof![Just(MergeStrategy::Default), Just(MergeStrategy::Ours),]
    }

    fn arb_message() -> impl Strategy<Value = String> {
        ".{1,100}".prop_map(|s| s.to_string())
    }

    fn arb_worktree_name() -> impl Strategy<Value = String> {
        "stack-[0-9]{1,10}".prop_map(|s| s.to_string())
    }

    fn arb_git_effect() -> impl Strategy<Value = GitEffect> {
        prop_oneof![
            prop::collection::vec(arb_refspec(), 1..5)
                .prop_map(|refspecs| GitEffect::Fetch { refspecs }),
            (arb_target(), any::<bool>())
                .prop_map(|(target, detach)| GitEffect::Checkout { target, detach }),
            (arb_target(), arb_merge_strategy(), arb_message()).prop_map(
                |(target, strategy, message)| GitEffect::Merge {
                    target,
                    strategy,
                    message
                }
            ),
            (arb_refspec(), any::<bool>())
                .prop_map(|(refspec, force)| GitEffect::Push { refspec, force }),
            (arb_sha(), arb_sha()).prop_map(|(potential_ancestor, descendant)| {
                GitEffect::IsAncestor {
                    potential_ancestor,
                    descendant,
                }
            }),
            arb_target().prop_map(|rev| GitEffect::RevParse { rev }),
            arb_worktree_name().prop_map(|name| GitEffect::CreateWorktree { name }),
            arb_worktree_name().prop_map(|name| GitEffect::RemoveWorktree { name }),
            Just(GitEffect::MergeAbort),
            arb_target().prop_map(|target| GitEffect::ResetHard { target }),
            (any::<bool>(), any::<bool>())
                .prop_map(|(directories, force)| GitEffect::Clean { directories, force }),
        ]
    }

    fn arb_git_response() -> impl Strategy<Value = GitResponse> {
        prop_oneof![
            Just(GitResponse::Ok),
            arb_sha().prop_map(GitResponse::Sha),
            any::<bool>().prop_map(GitResponse::Bool),
        ]
    }

    // ─── MergeStrategy Tests ──────────────────────────────────────────────────

    mod merge_strategy {
        use super::*;

        proptest! {
            #[test]
            fn serde_roundtrip(strategy in arb_merge_strategy()) {
                let json = serde_json::to_string(&strategy).unwrap();
                let parsed: MergeStrategy = serde_json::from_str(&json).unwrap();
                prop_assert_eq!(strategy, parsed);
            }

            #[test]
            fn hash_consistent(strategy in arb_merge_strategy()) {
                let h1 = hash(&strategy);
                let h2 = hash(&strategy);
                prop_assert_eq!(h1, h2);
            }
        }

        #[test]
        fn git_arg_values() {
            assert_eq!(MergeStrategy::Default.as_git_arg(), None);
            assert_eq!(MergeStrategy::Ours.as_git_arg(), Some("ours"));
        }
    }

    // ─── GitEffect Tests ──────────────────────────────────────────────────────

    mod git_effect {
        use super::*;

        proptest! {
            #[test]
            fn serde_roundtrip(effect in arb_git_effect()) {
                let json = serde_json::to_string(&effect).unwrap();
                let parsed: GitEffect = serde_json::from_str(&json).unwrap();
                prop_assert_eq!(effect, parsed);
            }

            #[test]
            fn eq_reflexive(effect in arb_git_effect()) {
                prop_assert_eq!(&effect, &effect);
            }

            #[test]
            fn hash_consistent(effect in arb_git_effect()) {
                let h1 = hash(&effect);
                let h2 = hash(&effect);
                prop_assert_eq!(h1, h2);
            }

            #[test]
            fn eq_implies_same_hash(e1 in arb_git_effect(), e2 in arb_git_effect()) {
                if e1 == e2 {
                    prop_assert_eq!(hash(&e1), hash(&e2));
                }
            }
        }
    }

    // ─── GitResponse Tests ────────────────────────────────────────────────────

    mod git_response {
        use super::*;

        proptest! {
            #[test]
            fn serde_roundtrip(response in arb_git_response()) {
                let json = serde_json::to_string(&response).unwrap();
                let parsed: GitResponse = serde_json::from_str(&json).unwrap();
                prop_assert_eq!(response, parsed);
            }
        }
    }
}
