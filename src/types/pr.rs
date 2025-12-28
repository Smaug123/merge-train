//! Pull request types and state representations.
//!
//! These types represent the state of pull requests as tracked by the merge train bot.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::ids::{PrNumber, Sha};

/// The state of a pull request.
///
/// Note: `Merged` requires a `merge_commit_sha` - this is not optional because
/// a merged PR always has a merge commit SHA on GitHub.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum PrState {
    /// The PR is open and can be merged.
    Open,

    /// The PR was merged. The SHA of the merge commit is required.
    Merged {
        /// The SHA of the commit created by the merge (squash commit on main).
        merge_commit_sha: Sha,
    },

    /// The PR was closed without merging.
    Closed,
}

impl PrState {
    /// Returns true if the PR is open.
    pub fn is_open(&self) -> bool {
        matches!(self, PrState::Open)
    }

    /// Returns true if the PR was merged.
    pub fn is_merged(&self) -> bool {
        matches!(self, PrState::Merged { .. })
    }

    /// Returns the merge commit SHA if the PR was merged.
    pub fn merge_commit_sha(&self) -> Option<&Sha> {
        match self {
            PrState::Merged { merge_commit_sha } => Some(merge_commit_sha),
            _ => None,
        }
    }
}

/// GitHub's merge state status, representing whether a PR can be merged.
///
/// This mirrors GitHub's GraphQL `mergeStateStatus` field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MergeStateStatus {
    /// All requirements satisfied - proceed with merge.
    Clean,

    /// Non-required checks failing - can still proceed.
    Unstable,

    /// Required checks not passing or missing approvals - wait.
    Blocked,

    /// Head branch behind base (strict mode) - needs update.
    Behind,

    /// Merge conflicts exist - abort cascade.
    ///
    /// This is a permanent failure that maps to `AbortReason::MergeConflict`,
    /// not a `BlockReason`. The cascade cannot proceed until the user resolves
    /// conflicts locally and re-issues `@merge-train start`.
    Dirty,

    /// State not yet computed by GitHub - wait and re-check.
    Unknown,

    /// PR is a draft - wait for it to be marked ready for review.
    Draft,

    /// Repository has merge hooks or merge queue enabled - abort cascade.
    ///
    /// This status appears on GitHub Enterprise with pre-receive hooks, or when
    /// GitHub's merge queue is enabled. Both are incompatible with merge-train
    /// (see DESIGN.md non-goals). Maps to `AbortReason::MergeHooksEnabled`.
    HasHooks,
}

impl MergeStateStatus {
    /// Returns true if the PR is ready to merge (Clean or Unstable).
    pub fn is_mergeable(&self) -> bool {
        matches!(self, MergeStateStatus::Clean | MergeStateStatus::Unstable)
    }

    /// Returns true if we should wait for the state to change.
    pub fn should_wait(&self) -> bool {
        matches!(
            self,
            MergeStateStatus::Blocked
                | MergeStateStatus::Behind
                | MergeStateStatus::Unknown
                | MergeStateStatus::Draft
        )
    }

    /// Returns true if the PR has a permanent issue requiring human intervention.
    ///
    /// These conditions cannot auto-resolve and cause an immediate abort:
    /// - `Dirty`: Merge conflicts exist
    /// - `HasHooks`: Repository has merge hooks or merge queue (incompatible config)
    pub fn is_permanent_failure(&self) -> bool {
        matches!(self, MergeStateStatus::Dirty | MergeStateStatus::HasHooks)
    }
}

/// Cached information about a pull request.
///
/// This represents the bot's local cache of PR state, updated incrementally
/// from webhooks and periodic syncs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CachedPr {
    /// The PR number.
    pub number: PrNumber,

    /// The current head SHA of the PR branch.
    pub head_sha: Sha,

    /// The name of the PR's head branch (e.g., "feature-branch").
    pub head_ref: String,

    /// The base branch the PR targets (e.g., "main" or another PR's branch).
    pub base_ref: String,

    /// The predecessor PR number, if declared via `@merge-train predecessor #N`.
    pub predecessor: Option<PrNumber>,

    /// The current state of the PR.
    pub state: PrState,

    /// GitHub's computed merge state status.
    pub merge_state_status: MergeStateStatus,

    /// Whether the PR is a draft.
    pub is_draft: bool,

    /// When the PR was closed (if applicable). Used for retention-based pruning.
    pub closed_at: Option<DateTime<Utc>>,

    /// SHA of the predecessor's squash commit that this PR was reconciled against.
    /// Set after normal cascade or late-addition reconciliation completes.
    /// When set, indicates this PR is ready to become a new train root.
    pub predecessor_squash_reconciled: Option<Sha>,
}

impl CachedPr {
    /// Creates a new CachedPr with the given parameters.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        number: PrNumber,
        head_sha: Sha,
        head_ref: String,
        base_ref: String,
        predecessor: Option<PrNumber>,
        state: PrState,
        merge_state_status: MergeStateStatus,
        is_draft: bool,
    ) -> Self {
        // Catch obviously inconsistent states. These mirror GitHub's API fields
        // which may have precedence rules we don't fully know, but some combinations
        // are definitely wrong.
        debug_assert!(
            !(is_draft && merge_state_status == MergeStateStatus::Clean),
            "Draft PR cannot have Clean merge state"
        );
        debug_assert!(
            merge_state_status != MergeStateStatus::Draft || is_draft,
            "MergeStateStatus::Draft requires is_draft = true"
        );

        CachedPr {
            number,
            head_sha,
            head_ref,
            base_ref,
            predecessor,
            state,
            merge_state_status,
            is_draft,
            closed_at: None,
            predecessor_squash_reconciled: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn arb_sha() -> impl Strategy<Value = Sha> {
        "[0-9a-f]{40}".prop_map(|s| Sha::parse(s).unwrap())
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

    mod pr_state {
        use super::*;

        proptest! {
            #[test]
            fn serde_roundtrip(state in arb_pr_state()) {
                let json = serde_json::to_string(&state).unwrap();
                let parsed: PrState = serde_json::from_str(&json).unwrap();
                prop_assert_eq!(state, parsed);
            }
        }

        #[test]
        fn merged_requires_sha() {
            let merged = PrState::Merged {
                merge_commit_sha: Sha::parse("abc123def456789012345678901234567890abcd").unwrap(),
            };
            assert!(merged.is_merged());
            assert!(merged.merge_commit_sha().is_some());
        }

        #[test]
        fn is_open_works() {
            assert!(PrState::Open.is_open());
            assert!(!PrState::Closed.is_open());
            assert!(
                !PrState::Merged {
                    merge_commit_sha: Sha::parse("abc123def456789012345678901234567890abcd")
                        .unwrap()
                }
                .is_open()
            );
        }
    }

    mod merge_state_status {
        use super::*;

        proptest! {
            #[test]
            fn serde_roundtrip(status in arb_merge_state_status()) {
                let json = serde_json::to_string(&status).unwrap();
                let parsed: MergeStateStatus = serde_json::from_str(&json).unwrap();
                prop_assert_eq!(status, parsed);
            }
        }

        #[test]
        fn is_mergeable_correct() {
            assert!(MergeStateStatus::Clean.is_mergeable());
            assert!(MergeStateStatus::Unstable.is_mergeable());
            assert!(!MergeStateStatus::Blocked.is_mergeable());
            assert!(!MergeStateStatus::Behind.is_mergeable());
            assert!(!MergeStateStatus::Dirty.is_mergeable());
            assert!(!MergeStateStatus::Unknown.is_mergeable());
            assert!(!MergeStateStatus::Draft.is_mergeable());
            assert!(!MergeStateStatus::HasHooks.is_mergeable());
        }

        #[test]
        fn should_wait_correct() {
            assert!(!MergeStateStatus::Clean.should_wait());
            assert!(!MergeStateStatus::Unstable.should_wait());
            assert!(MergeStateStatus::Blocked.should_wait());
            assert!(MergeStateStatus::Behind.should_wait());
            assert!(!MergeStateStatus::Dirty.should_wait());
            assert!(MergeStateStatus::Unknown.should_wait());
            assert!(MergeStateStatus::Draft.should_wait());
            assert!(!MergeStateStatus::HasHooks.should_wait());
        }

        #[test]
        fn is_permanent_failure_correct() {
            assert!(!MergeStateStatus::Clean.is_permanent_failure());
            assert!(!MergeStateStatus::Unstable.is_permanent_failure());
            assert!(!MergeStateStatus::Blocked.is_permanent_failure());
            assert!(!MergeStateStatus::Behind.is_permanent_failure());
            assert!(MergeStateStatus::Dirty.is_permanent_failure());
            assert!(!MergeStateStatus::Unknown.is_permanent_failure());
            assert!(!MergeStateStatus::Draft.is_permanent_failure());
            assert!(MergeStateStatus::HasHooks.is_permanent_failure());
        }
    }

    mod cached_pr {
        use super::*;

        fn arb_branch_name() -> impl Strategy<Value = String> {
            "[a-zA-Z][a-zA-Z0-9_-]{0,49}".prop_map(|s| s.to_string())
        }

        /// Generate valid (merge_state_status, is_draft) pairs.
        ///
        /// Constraints:
        /// - If merge_state_status = Draft, then is_draft must be true
        /// - If merge_state_status = Clean, then is_draft must be false
        /// - Otherwise, is_draft can be either (we don't know GitHub's precedence)
        fn arb_merge_state_and_draft() -> impl Strategy<Value = (MergeStateStatus, bool)> {
            arb_merge_state_status().prop_flat_map(|status| {
                let is_draft_strategy: proptest::strategy::BoxedStrategy<bool> = match status {
                    MergeStateStatus::Draft => Just(true).boxed(),
                    MergeStateStatus::Clean => Just(false).boxed(),
                    _ => any::<bool>().boxed(),
                };
                is_draft_strategy.prop_map(move |is_draft| (status, is_draft))
            })
        }

        fn arb_cached_pr() -> impl Strategy<Value = CachedPr> {
            (
                any::<u64>(),
                arb_sha(),
                arb_branch_name(),
                arb_branch_name(),
                prop::option::of(any::<u64>().prop_map(PrNumber)),
                arb_pr_state(),
                arb_merge_state_and_draft(),
            )
                .prop_map(
                    |(
                        number,
                        head_sha,
                        head_ref,
                        base_ref,
                        predecessor,
                        state,
                        (merge_state_status, is_draft),
                    )| {
                        CachedPr::new(
                            PrNumber(number),
                            head_sha,
                            head_ref,
                            base_ref,
                            predecessor,
                            state,
                            merge_state_status,
                            is_draft,
                        )
                    },
                )
        }

        proptest! {
            #[test]
            fn serde_roundtrip(pr in arb_cached_pr()) {
                let json = serde_json::to_string(&pr).unwrap();
                let parsed: CachedPr = serde_json::from_str(&json).unwrap();
                prop_assert_eq!(pr, parsed);
            }
        }
    }
}
