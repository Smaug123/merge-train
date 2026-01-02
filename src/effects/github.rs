//! GitHub API effect types.
//!
//! These types describe GitHub API operations as data, without executing them.
//! The interpreter (implemented in a later stage) executes these effects against
//! the actual GitHub API.

use serde::{Deserialize, Serialize};

use crate::types::{CommentId, MergeStateStatus, PrNumber, PrState, Sha};

/// GitHub reaction types.
///
/// These correspond to the reactions available on GitHub comments and PRs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Reaction {
    /// +1 / thumbs up
    ThumbsUp,
    /// -1 / thumbs down
    ThumbsDown,
    /// Laugh
    Laugh,
    /// Hooray / tada
    Hooray,
    /// Confused
    Confused,
    /// Heart
    Heart,
    /// Rocket
    Rocket,
    /// Eyes
    Eyes,
}

impl Reaction {
    /// Returns the GitHub API content string for this reaction.
    pub fn as_api_str(&self) -> &'static str {
        match self {
            Reaction::ThumbsUp => "+1",
            Reaction::ThumbsDown => "-1",
            Reaction::Laugh => "laugh",
            Reaction::Hooray => "hooray",
            Reaction::Confused => "confused",
            Reaction::Heart => "heart",
            Reaction::Rocket => "rocket",
            Reaction::Eyes => "eyes",
        }
    }
}

/// A GitHub API effect.
///
/// Each variant describes a GitHub API operation. Effects are repo-scoped:
/// the interpreter is constructed with a `RepoId`, so effects don't include it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GitHubEffect {
    // ─── PR Queries ───────────────────────────────────────────────────────────
    /// Fetch a single PR by number.
    GetPr { pr: PrNumber },

    /// List all open PRs in the repository.
    ListOpenPrs,

    /// List recently merged PRs (for predecessor lookups and recovery).
    ListRecentlyMergedPrs {
        /// How many days back to look for merged PRs.
        since_days: u32,
    },

    /// Get the merge state status for a PR (via GraphQL).
    GetMergeState { pr: PrNumber },

    // ─── PR Mutations ─────────────────────────────────────────────────────────
    /// Squash-merge a PR into its base branch.
    ///
    /// The `expected_sha` is passed to GitHub's merge endpoint to prevent
    /// racing pushes: if the PR's head SHA doesn't match, the merge fails
    /// with HTTP 409 instead of merging unreviewed code.
    SquashMerge { pr: PrNumber, expected_sha: Sha },

    /// Retarget a PR to a new base branch.
    RetargetPr { pr: PrNumber, new_base: String },

    // ─── Comments ─────────────────────────────────────────────────────────────
    /// Post a new comment on a PR.
    PostComment { pr: PrNumber, body: String },

    /// Update an existing comment.
    UpdateComment { comment_id: CommentId, body: String },

    /// Add a reaction to a comment.
    AddReaction {
        comment_id: CommentId,
        reaction: Reaction,
    },

    /// List all comments on a PR.
    ListComments { pr: PrNumber },

    // ─── Repository Settings ──────────────────────────────────────────────────
    /// Get branch protection settings for a branch.
    GetBranchProtection { branch: String },

    /// Get repository rulesets (for dismiss stale reviews check).
    GetRulesets,

    /// Get repository settings (merge method configuration).
    GetRepoSettings,
}

// ─── Response Types ───────────────────────────────────────────────────────────

/// PR data returned from the GitHub API.
///
/// This is a subset of the information in `CachedPr`, containing only what
/// the API returns directly.
///
/// Note: The merge commit SHA is stored in `PrState::Merged { merge_commit_sha }`
/// rather than as a separate field, ensuring consistency (a merged PR always
/// has a SHA, and a non-merged PR never does).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrData {
    /// The PR number.
    pub number: PrNumber,
    /// The current head SHA.
    pub head_sha: Sha,
    /// The head branch name.
    pub head_ref: String,
    /// The base branch name.
    pub base_ref: String,
    /// The PR state (open, merged, closed). If merged, contains the merge commit SHA.
    pub state: PrState,
    /// Whether the PR is a draft.
    pub is_draft: bool,
}

/// Comment data returned from the GitHub API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommentData {
    /// The comment ID.
    pub id: CommentId,
    /// The author's GitHub user ID.
    pub author_id: u64,
    /// The comment body.
    pub body: String,
}

/// Branch protection settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchProtectionData {
    /// Whether "dismiss stale pull request approvals" is enabled.
    pub dismiss_stale_reviews: bool,
    /// Required status checks (context names).
    pub required_status_checks: Vec<String>,
}

/// Repository ruleset data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RulesetData {
    /// The ruleset name.
    pub name: String,
    /// Whether this ruleset dismisses stale reviews on push.
    pub dismiss_stale_reviews_on_push: bool,
    /// Branch patterns this ruleset targets (from `conditions.ref_name.include`).
    ///
    /// These are ref patterns like `"refs/heads/main"` or `"refs/heads/*"`.
    /// Callers should check if the default branch matches any of these patterns
    /// to determine if the ruleset applies.
    ///
    /// Empty if the ruleset has no ref_name conditions (applies to all branches)
    /// or if the conditions couldn't be parsed.
    pub target_branches: Vec<String>,
}

/// Repository settings related to merge methods.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoSettingsData {
    /// The repository's default branch.
    pub default_branch: String,
    /// Whether squash merging is allowed.
    pub allow_squash_merge: bool,
    /// Whether merge commits are allowed.
    pub allow_merge_commit: bool,
    /// Whether rebase merging is allowed.
    pub allow_rebase_merge: bool,
}

/// Response from a GitHub effect.
///
/// Each variant corresponds to the response from a particular effect type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum GitHubResponse {
    /// Response to `GetPr`.
    Pr(PrData),

    /// Response to `ListOpenPrs`.
    PrList(Vec<PrData>),

    /// Response to `ListRecentlyMergedPrs`.
    ///
    /// Includes a flag indicating whether results may be incomplete due to
    /// pagination limits. GitHub's PR list API doesn't have a "merged since"
    /// filter, so we list closed PRs sorted by `updated` and filter client-side.
    /// In repos with many closed PRs, we may hit the pagination limit before
    /// exhausting all recent merges.
    RecentlyMergedPrList {
        prs: Vec<PrData>,
        /// If true, results may be incomplete because the pagination limit was reached
        /// before all pages were exhausted. Callers should handle this gracefully.
        may_be_incomplete: bool,
    },

    /// Response to `GetMergeState`.
    MergeState(MergeStateStatus),

    /// Response to `SquashMerge`.
    Merged {
        /// The SHA of the squash commit on the base branch.
        sha: Sha,
    },

    /// Response to `RetargetPr`.
    Retargeted,

    /// Response to `PostComment`.
    CommentPosted {
        /// The ID of the newly created comment.
        id: CommentId,
    },

    /// Response to `UpdateComment`.
    CommentUpdated,

    /// Response to `AddReaction`.
    ReactionAdded,

    /// Response to `ListComments`.
    Comments(Vec<CommentData>),

    /// Response to `GetBranchProtection` when protection rules were successfully fetched.
    BranchProtection(BranchProtectionData),

    /// Response to `GetBranchProtection` when the result is unknown.
    ///
    /// This occurs when the API returns 404, which could mean:
    /// 1. The branch has no protection rules
    /// 2. The caller lacks permission to view protection rules
    /// 3. The branch doesn't exist
    ///
    /// Per DESIGN.md, callers should "warn and proceed" when they receive this,
    /// since we cannot distinguish missing permissions from no protection.
    BranchProtectionUnknown,

    /// Response to `GetRulesets` when rulesets were successfully fetched.
    Rulesets(Vec<RulesetData>),

    /// Response to `GetRulesets` when the result is unknown.
    ///
    /// This occurs when the API returns 404 or 403, which could mean:
    /// 1. The repository has no rulesets
    /// 2. The caller lacks permission to view rulesets
    /// 3. Rulesets aren't available (older API or not enabled)
    ///
    /// Per DESIGN.md, callers should "warn and proceed" when they receive this,
    /// since we cannot distinguish missing permissions from no rulesets.
    RulesetsUnknown,

    /// Response to `GetRepoSettings`.
    RepoSettings(RepoSettingsData),
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

    fn arb_pr_number() -> impl Strategy<Value = PrNumber> {
        (1..=u32::MAX as u64).prop_map(PrNumber)
    }

    fn arb_comment_id() -> impl Strategy<Value = CommentId> {
        any::<u64>().prop_map(CommentId)
    }

    fn arb_branch_name() -> impl Strategy<Value = String> {
        "[a-zA-Z][a-zA-Z0-9_/-]{0,49}".prop_map(|s| s.to_string())
    }

    fn arb_comment_body() -> impl Strategy<Value = String> {
        ".{0,200}".prop_map(|s| s.to_string())
    }

    fn arb_reaction() -> impl Strategy<Value = Reaction> {
        prop_oneof![
            Just(Reaction::ThumbsUp),
            Just(Reaction::ThumbsDown),
            Just(Reaction::Laugh),
            Just(Reaction::Hooray),
            Just(Reaction::Confused),
            Just(Reaction::Heart),
            Just(Reaction::Rocket),
            Just(Reaction::Eyes),
        ]
    }

    fn arb_pr_state() -> impl Strategy<Value = PrState> {
        prop_oneof![
            Just(PrState::Open),
            Just(PrState::Closed),
            arb_sha().prop_map(|sha| PrState::Merged {
                merge_commit_sha: sha
            }),
        ]
    }

    fn arb_merge_state_status() -> impl Strategy<Value = MergeStateStatus> {
        prop_oneof![
            Just(MergeStateStatus::Clean),
            Just(MergeStateStatus::Unstable),
            Just(MergeStateStatus::Blocked),
            Just(MergeStateStatus::Behind),
            Just(MergeStateStatus::Dirty),
            Just(MergeStateStatus::Unknown),
            Just(MergeStateStatus::Draft),
            Just(MergeStateStatus::HasHooks),
        ]
    }

    fn arb_pr_data() -> impl Strategy<Value = PrData> {
        (
            arb_pr_number(),
            arb_sha(),
            arb_branch_name(),
            arb_branch_name(),
            arb_pr_state(),
            any::<bool>(),
        )
            .prop_map(
                |(number, head_sha, head_ref, base_ref, state, is_draft)| PrData {
                    number,
                    head_sha,
                    head_ref,
                    base_ref,
                    state,
                    is_draft,
                },
            )
    }

    fn arb_comment_data() -> impl Strategy<Value = CommentData> {
        (arb_comment_id(), any::<u64>(), arb_comment_body()).prop_map(|(id, author_id, body)| {
            CommentData {
                id,
                author_id,
                body,
            }
        })
    }

    fn arb_branch_protection_data() -> impl Strategy<Value = BranchProtectionData> {
        (
            any::<bool>(),
            prop::collection::vec(arb_branch_name(), 0..5),
        )
            .prop_map(|(dismiss_stale_reviews, required_status_checks)| {
                BranchProtectionData {
                    dismiss_stale_reviews,
                    required_status_checks,
                }
            })
    }

    fn arb_ruleset_data() -> impl Strategy<Value = RulesetData> {
        (
            arb_branch_name(),
            any::<bool>(),
            prop::collection::vec(arb_branch_name(), 0..3),
        )
            .prop_map(|(name, dismiss_stale_reviews_on_push, target_branches)| {
                RulesetData {
                    name,
                    dismiss_stale_reviews_on_push,
                    target_branches,
                }
            })
    }

    fn arb_repo_settings_data() -> impl Strategy<Value = RepoSettingsData> {
        (
            arb_branch_name(),
            any::<bool>(),
            any::<bool>(),
            any::<bool>(),
        )
            .prop_map(
                |(default_branch, allow_squash_merge, allow_merge_commit, allow_rebase_merge)| {
                    RepoSettingsData {
                        default_branch,
                        allow_squash_merge,
                        allow_merge_commit,
                        allow_rebase_merge,
                    }
                },
            )
    }

    fn arb_github_effect() -> impl Strategy<Value = GitHubEffect> {
        prop_oneof![
            arb_pr_number().prop_map(|pr| GitHubEffect::GetPr { pr }),
            Just(GitHubEffect::ListOpenPrs),
            (1u32..365).prop_map(|since_days| GitHubEffect::ListRecentlyMergedPrs { since_days }),
            arb_pr_number().prop_map(|pr| GitHubEffect::GetMergeState { pr }),
            (arb_pr_number(), arb_sha())
                .prop_map(|(pr, expected_sha)| GitHubEffect::SquashMerge { pr, expected_sha }),
            (arb_pr_number(), arb_branch_name())
                .prop_map(|(pr, new_base)| GitHubEffect::RetargetPr { pr, new_base }),
            (arb_pr_number(), arb_comment_body())
                .prop_map(|(pr, body)| GitHubEffect::PostComment { pr, body }),
            (arb_comment_id(), arb_comment_body())
                .prop_map(|(comment_id, body)| GitHubEffect::UpdateComment { comment_id, body }),
            (arb_comment_id(), arb_reaction()).prop_map(|(comment_id, reaction)| {
                GitHubEffect::AddReaction {
                    comment_id,
                    reaction,
                }
            }),
            arb_pr_number().prop_map(|pr| GitHubEffect::ListComments { pr }),
            arb_branch_name().prop_map(|branch| GitHubEffect::GetBranchProtection { branch }),
            Just(GitHubEffect::GetRulesets),
            Just(GitHubEffect::GetRepoSettings),
        ]
    }

    fn arb_github_response() -> impl Strategy<Value = GitHubResponse> {
        prop_oneof![
            arb_pr_data().prop_map(GitHubResponse::Pr),
            prop::collection::vec(arb_pr_data(), 0..10).prop_map(GitHubResponse::PrList),
            (prop::collection::vec(arb_pr_data(), 0..10), any::<bool>()).prop_map(
                |(prs, may_be_incomplete)| GitHubResponse::RecentlyMergedPrList {
                    prs,
                    may_be_incomplete,
                }
            ),
            arb_merge_state_status().prop_map(GitHubResponse::MergeState),
            arb_sha().prop_map(|sha| GitHubResponse::Merged { sha }),
            Just(GitHubResponse::Retargeted),
            arb_comment_id().prop_map(|id| GitHubResponse::CommentPosted { id }),
            Just(GitHubResponse::CommentUpdated),
            Just(GitHubResponse::ReactionAdded),
            prop::collection::vec(arb_comment_data(), 0..10).prop_map(GitHubResponse::Comments),
            arb_branch_protection_data().prop_map(GitHubResponse::BranchProtection),
            Just(GitHubResponse::BranchProtectionUnknown),
            prop::collection::vec(arb_ruleset_data(), 0..5).prop_map(GitHubResponse::Rulesets),
            Just(GitHubResponse::RulesetsUnknown),
            arb_repo_settings_data().prop_map(GitHubResponse::RepoSettings),
        ]
    }

    // ─── Reaction Tests ───────────────────────────────────────────────────────

    mod reaction {
        use super::*;

        proptest! {
            #[test]
            fn serde_roundtrip(reaction in arb_reaction()) {
                let json = serde_json::to_string(&reaction).unwrap();
                let parsed: Reaction = serde_json::from_str(&json).unwrap();
                prop_assert_eq!(reaction, parsed);
            }

            #[test]
            fn hash_consistent(reaction in arb_reaction()) {
                let h1 = hash(&reaction);
                let h2 = hash(&reaction);
                prop_assert_eq!(h1, h2);
            }
        }

        #[test]
        fn api_str_values() {
            assert_eq!(Reaction::ThumbsUp.as_api_str(), "+1");
            assert_eq!(Reaction::ThumbsDown.as_api_str(), "-1");
            assert_eq!(Reaction::Laugh.as_api_str(), "laugh");
            assert_eq!(Reaction::Hooray.as_api_str(), "hooray");
            assert_eq!(Reaction::Confused.as_api_str(), "confused");
            assert_eq!(Reaction::Heart.as_api_str(), "heart");
            assert_eq!(Reaction::Rocket.as_api_str(), "rocket");
            assert_eq!(Reaction::Eyes.as_api_str(), "eyes");
        }
    }

    // ─── GitHubEffect Tests ───────────────────────────────────────────────────

    mod github_effect {
        use super::*;

        proptest! {
            #[test]
            fn serde_roundtrip(effect in arb_github_effect()) {
                let json = serde_json::to_string(&effect).unwrap();
                let parsed: GitHubEffect = serde_json::from_str(&json).unwrap();
                prop_assert_eq!(effect, parsed);
            }

            #[test]
            fn eq_reflexive(effect in arb_github_effect()) {
                prop_assert_eq!(&effect, &effect);
            }

            #[test]
            fn hash_consistent(effect in arb_github_effect()) {
                let h1 = hash(&effect);
                let h2 = hash(&effect);
                prop_assert_eq!(h1, h2);
            }

            #[test]
            fn eq_implies_same_hash(e1 in arb_github_effect(), e2 in arb_github_effect()) {
                if e1 == e2 {
                    prop_assert_eq!(hash(&e1), hash(&e2));
                }
            }
        }
    }

    // ─── GitHubResponse Tests ─────────────────────────────────────────────────

    mod github_response {
        use super::*;

        proptest! {
            #[test]
            fn serde_roundtrip(response in arb_github_response()) {
                let json = serde_json::to_string(&response).unwrap();
                let parsed: GitHubResponse = serde_json::from_str(&json).unwrap();
                prop_assert_eq!(response, parsed);
            }
        }
    }

    // ─── Data Type Tests ──────────────────────────────────────────────────────

    mod data_types {
        use super::*;

        proptest! {
            #[test]
            fn pr_data_serde_roundtrip(data in arb_pr_data()) {
                let json = serde_json::to_string(&data).unwrap();
                let parsed: PrData = serde_json::from_str(&json).unwrap();
                prop_assert_eq!(data, parsed);
            }

            #[test]
            fn comment_data_serde_roundtrip(data in arb_comment_data()) {
                let json = serde_json::to_string(&data).unwrap();
                let parsed: CommentData = serde_json::from_str(&json).unwrap();
                prop_assert_eq!(data, parsed);
            }

            #[test]
            fn branch_protection_data_serde_roundtrip(data in arb_branch_protection_data()) {
                let json = serde_json::to_string(&data).unwrap();
                let parsed: BranchProtectionData = serde_json::from_str(&json).unwrap();
                prop_assert_eq!(data, parsed);
            }

            #[test]
            fn ruleset_data_serde_roundtrip(data in arb_ruleset_data()) {
                let json = serde_json::to_string(&data).unwrap();
                let parsed: RulesetData = serde_json::from_str(&json).unwrap();
                prop_assert_eq!(data, parsed);
            }

            #[test]
            fn repo_settings_data_serde_roundtrip(data in arb_repo_settings_data()) {
                let json = serde_json::to_string(&data).unwrap();
                let parsed: RepoSettingsData = serde_json::from_str(&json).unwrap();
                prop_assert_eq!(data, parsed);
            }
        }
    }
}
