//! Git operation effect types.
//!
//! These types describe git operations as data, without executing them.
//! The functions in `crate::git` execute them against a local git repository.

use serde::{Deserialize, Serialize};

use crate::types::{PrNumber, Sha};

/// The domain outcome of a merge, mirroring [`crate::git::MergeResult`] in
/// serde-able form (seam a).
///
/// Conflicts are *outcomes the engine branches on* (abort vs skip per
/// DESIGN.md §Pause Conditions), so the interpreter returns them as
/// `Ok(GitResponse::Merge(MergeOutcome::Conflict { .. }))`, never as `Err`.
/// Only uncharacterizable git failures are interpreter errors.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum MergeOutcome {
    /// Merge completed successfully, producing a new commit.
    Success {
        /// The SHA of the merge commit.
        commit_sha: Sha,
    },
    /// Merge resulted in a conflict.
    Conflict {
        /// Files with conflicts.
        conflicting_files: Vec<String>,
    },
    /// Merge was a no-op (already up-to-date).
    AlreadyUpToDate,
}

impl From<crate::git::MergeResult> for MergeOutcome {
    fn from(result: crate::git::MergeResult) -> Self {
        match result {
            crate::git::MergeResult::Success { commit_sha } => MergeOutcome::Success { commit_sha },
            crate::git::MergeResult::Conflict { conflicting_files } => {
                MergeOutcome::Conflict { conflicting_files }
            }
            crate::git::MergeResult::AlreadyUpToDate => MergeOutcome::AlreadyUpToDate,
        }
    }
}

/// The domain outcome of a push, mirroring [`crate::git::push::PushResult`]
/// (seam a). A non-fast-forward rejection is a domain outcome (someone else
/// pushed; DESIGN.md §Concurrent push handling), NOT an interpreter error.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum PushOutcome {
    /// Push succeeded.
    Pushed {
        /// The SHA that was pushed.
        pushed_sha: Sha,
    },
    /// The remote already had this commit (idempotent re-push).
    AlreadyUpToDate,
    /// Push was rejected (non-fast-forward): the remote advanced under us.
    Rejected {
        /// git's explanation of the rejection.
        details: String,
    },
}

impl From<crate::git::push::PushResult> for PushOutcome {
    fn from(result: crate::git::push::PushResult) -> Self {
        match result {
            crate::git::push::PushResult::Success { pushed_sha } => {
                PushOutcome::Pushed { pushed_sha }
            }
            crate::git::push::PushResult::AlreadyUpToDate => PushOutcome::AlreadyUpToDate,
            crate::git::push::PushResult::Rejected { details } => PushOutcome::Rejected { details },
        }
    }
}

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
        /// Refspecs to fetch (e.g., `["main"]` or `["refs/pull/123/head:refs/remotes/origin/pr/123"]`).
        ///
        /// These are passed to `git fetch origin <refspec>...`. Use branch names like `"main"`,
        /// or full refspecs like `"refs/pull/N/head:refs/remotes/origin/pr/N"` for PR refs.
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

    /// Prepare a descendant: merge the predecessor's pinned head into it
    /// (BEFORE the predecessor is squash-merged). Interpreted by
    /// `git::merge::prepare_descendant`; responds with
    /// [`GitResponse::Prepared`], whose `predecessor_head` is the exact commit
    /// the PR ref named at fetch time (the pin the whole force-push guard
    /// hangs on).
    ///
    /// Do NOT merge main here — see DESIGN.md "Why merging $SQUASH_SHA^ is
    /// essential".
    PrepareDescendant {
        /// The descendant's branch name.
        descendant_branch: String,
        /// The predecessor's PR number (fetched via `refs/pull/<n>/head`).
        predecessor_pr: PrNumber,
    },

    /// Reconciliation merge: two-step merge for squash commit integration.
    ///
    /// This performs the reconciliation protocol from DESIGN.md — **merges
    /// only, no push** (the push is a separate intent-bracketed [`GitEffect::Push`]):
    /// 1. `git merge $SQUASH_SHA^` - Merge the parent of the squash commit
    ///    (the default branch HEAD at the time of the squash)
    /// 2. `git merge -s ours $SQUASH_SHA` - Create a merge commit marking the
    ///    squash as an ancestor without changing the tree
    ///
    /// Step 1 brings in any commits that landed on the default branch between
    /// preparation and squash. Step 2 marks the squash commit as an ancestor
    /// without changing the tree (since the descendant was already prepared
    /// with the predecessor's content before the squash).
    ///
    /// # Interpreter Validation Requirements
    ///
    /// The interpreter (`git::merge::reconcile_descendant`) refuses the
    /// irreversible step-2 merge unless ALL of these hold:
    ///
    /// 1. **Single parent**: `squash_sha` has exactly one parent — multiple
    ///    parents means a regular merge commit, not a squash.
    /// 2. **Default branch ancestry**: `$SQUASH_SHA^` is an ancestor of
    ///    `origin/{default_branch}` HEAD.
    /// 3. **Expected parent match**: the computed `$SQUASH_SHA^` equals
    ///    `expected_squash_parent`.
    /// 4. **Squashed-head match**: the head named by the predecessor's PR ref
    ///    (frozen at merge — the head GitHub actually squashed) equals
    ///    `predecessor_pre_squash_head`.
    /// 5. **Preparation ancestry**: `predecessor_pre_squash_head` is an
    ///    ancestor of the descendant's head. Without 4+5, a force-push to
    ///    the predecessor between preparation and squash would let the
    ///    ours-merge record content as merged that was never merged —
    ///    silent, unrecoverable content loss.
    MergeReconcile {
        /// The descendant's branch name.
        descendant_branch: String,
        /// The predecessor's PR number; its PR ref names the head that was
        /// actually squash-merged (validation requirement 4).
        predecessor_pr: PrNumber,
        /// The squash commit SHA to reconcile against.
        squash_sha: Sha,
        /// The expected parent of the squash commit (`$SQUASH_SHA^`),
        /// captured when the squash was observed.
        expected_squash_parent: Sha,
        /// The predecessor head SHA that preparation actually merged into the
        /// descendant (returned by the prepare step). The interpreter refuses
        /// reconciliation unless this is an ancestor of the descendant head.
        predecessor_pre_squash_head: Sha,
        /// The default branch name (e.g., "main").
        ///
        /// Used to verify the squash parent is on the default branch history,
        /// ensuring we're reconciling against a valid squash merge.
        default_branch: String,
    },

    /// Catch up a descendant with commits that landed on the default branch
    /// AFTER the squash (a regular merge of `origin/<default_branch>`).
    /// Interpreted by `git::merge::catch_up_descendant`. A conflict here is a
    /// genuine conflict with post-squash main content (abort per DESIGN.md).
    CatchUpDescendant {
        /// The descendant's branch name.
        descendant_branch: String,
        /// The default branch name (e.g., "main").
        default_branch: String,
    },

    /// Merge the default branch into the ROOT PR's branch to satisfy
    /// "require branches up to date" protection (`mergeStateStatus: BEHIND`).
    /// Interpreted by `git::merge::update_root_for_behind`. Only sound for the
    /// root: it has no predecessor whose squash ordering could be violated.
    UpdateRootForBehind {
        /// The root PR's branch name.
        branch: String,
        /// The default branch name (e.g., "main").
        default_branch: String,
    },

    /// Recovery check: did an intended push actually land? Interpreted by
    /// `git::push::is_push_completed` (tree SHA + parent-chain verification —
    /// commit SHAs are not reproducible across retries). Responds with
    /// [`GitResponse::PushCheck`], carrying the observed remote head.
    CheckPushCompleted {
        /// The branch that was being pushed to.
        branch: String,
        /// The tree SHA the push intent expected.
        expected_tree: Sha,
        /// The remote ref before the intended push.
        pre_push_sha: Sha,
        /// For ours-merge pushes (reconciliation), the expected second parent
        /// (the squash SHA): an ours-merge doesn't change the tree, so tree
        /// comparison alone cannot identify our commit.
        second_parent: Option<Sha>,
    },

    /// Return the worktree to a clean state after an abort (aborts any
    /// in-progress merge, resets, cleans). Interpreted by
    /// `git::recovery::cleanup_worktree_on_abort`. Idempotent; also issued
    /// defensively at cascade-step entry.
    CleanupWorktree,

    /// Validate a squash commit without performing reconciliation.
    ///
    /// Use this when you need to verify a squash commit is valid but have no
    /// descendants to reconcile (e.g., external merge of a PR with no descendants).
    ///
    /// The interpreter MUST validate:
    /// 1. **Single parent check**: Verify `squash_sha` has exactly one parent.
    ///    If it has multiple parents, it's a regular merge commit, not a squash.
    /// 2. **Default branch ancestry**: Verify the computed `$SQUASH_SHA^` is on
    ///    the `origin/{default_branch}` history.
    ///
    /// If validation fails, the interpreter should return an error that causes
    /// the train to abort.
    ValidateSquashCommit {
        /// The squash commit SHA to validate.
        squash_sha: Sha,
        /// The default branch name (e.g., "main").
        default_branch: String,
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

/// The push point captured immediately after a cascade merge, while the
/// worktree HEAD still IS the merge result: HEAD's tree SHA and the remote
/// ref about to be pushed over. These land in the push intent event so
/// recovery can decide whether the push happened (DESIGN.md §Git push
/// idempotency). Captured *inside* the merge effects (via
/// `git::push::capture_pre_push_state`) rather than by a separate effect:
/// the capture is only meaningful in the same worktree-HEAD context as the
/// merge, and the pure engine has no scratch state to thread it across an
/// extra observation round-trip.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PushPoint {
    /// The tree SHA of the worktree's HEAD (deterministic across retries,
    /// unlike merge-commit SHAs).
    pub expected_tree: Sha,
    /// The remote ref before our push; `None` if the branch is gone
    /// (deleted mid-cascade — the descendant gets skipped).
    pub pre_push_sha: Option<Sha>,
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
    /// Response to `CheckPushCompleted`.
    PushCheck {
        /// Whether the intended push provably landed (tree + parent-chain
        /// verification per DESIGN.md §Git push idempotency).
        completed: bool,
        /// The remote branch head the check observed (`None` if the branch is
        /// gone). When `completed` is true this head is *proven ours*, so
        /// recovery records it as the descendant's current head.
        remote_head: Option<Sha>,
    },
    /// Response to the plain `Merge` effect.
    Merge(MergeOutcome),
    /// Response to `Push`: the domain outcome of the push.
    Push(PushOutcome),
    /// Response to `PrepareDescendant`.
    Prepared {
        /// The outcome of merging the predecessor into the descendant.
        merge: MergeOutcome,
        /// The exact predecessor commit that was merged (pinned from the PR
        /// ref at fetch time). See `PrepareResult::predecessor_head`.
        predecessor_head: Sha,
        /// The push point, captured iff `merge` is `Success` (an
        /// `AlreadyUpToDate` merge needs no push; a `Conflict` allows none).
        push_point: Option<PushPoint>,
    },
    /// Response to `MergeReconcile`/`CatchUpDescendant`/`UpdateRootForBehind`:
    /// a cascade merge whose result is destined for an intent-bracketed push.
    MergedForPush {
        /// The outcome of the merge.
        merge: MergeOutcome,
        /// The push point, captured iff `merge` is `Success`.
        push_point: Option<PushPoint>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

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

    fn arb_pr_number() -> impl Strategy<Value = PrNumber> {
        (1u64..100_000).prop_map(PrNumber)
    }

    fn arb_merge_outcome() -> impl Strategy<Value = MergeOutcome> {
        prop_oneof![
            arb_sha().prop_map(|commit_sha| MergeOutcome::Success { commit_sha }),
            prop::collection::vec("[a-z/.]{1,20}", 0..4)
                .prop_map(|conflicting_files| MergeOutcome::Conflict { conflicting_files }),
            Just(MergeOutcome::AlreadyUpToDate),
        ]
    }

    fn arb_push_outcome() -> impl Strategy<Value = PushOutcome> {
        prop_oneof![
            arb_sha().prop_map(|pushed_sha| PushOutcome::Pushed { pushed_sha }),
            Just(PushOutcome::AlreadyUpToDate),
            ".{0,40}".prop_map(|details| PushOutcome::Rejected { details }),
        ]
    }

    fn arb_git_effect() -> impl Strategy<Value = GitEffect> {
        // COMPILE-TIME EXHAUSTIVENESS CHECK
        // When you add a new GitEffect variant, this match will fail to compile,
        // reminding you to add a corresponding generator to prop_oneof! below.
        // Keep the match arms in the same order as the prop_oneof! branches.
        #[allow(dead_code, unreachable_code)]
        fn _assert_all_variants_covered(e: GitEffect) {
            match e {
                GitEffect::Fetch { .. } => {}
                GitEffect::Checkout { .. } => {}
                GitEffect::Merge { .. } => {}
                GitEffect::PrepareDescendant { .. } => {}
                GitEffect::MergeReconcile { .. } => {}
                GitEffect::CatchUpDescendant { .. } => {}
                GitEffect::UpdateRootForBehind { .. } => {}
                GitEffect::CheckPushCompleted { .. } => {}
                GitEffect::CleanupWorktree => {}
                GitEffect::ValidateSquashCommit { .. } => {}
                GitEffect::Push { .. } => {}
                GitEffect::IsAncestor { .. } => {}
                GitEffect::RevParse { .. } => {}
                GitEffect::CreateWorktree { .. } => {}
                GitEffect::RemoveWorktree { .. } => {}
                GitEffect::MergeAbort => {}
                GitEffect::ResetHard { .. } => {}
                GitEffect::Clean { .. } => {}
            }
        }

        prop_oneof![
            // Fetch
            prop::collection::vec(arb_refspec(), 1..5)
                .prop_map(|refspecs| GitEffect::Fetch { refspecs }),
            // Checkout
            (arb_target(), any::<bool>())
                .prop_map(|(target, detach)| GitEffect::Checkout { target, detach }),
            // Merge
            (arb_target(), arb_merge_strategy(), arb_message()).prop_map(
                |(target, strategy, message)| GitEffect::Merge {
                    target,
                    strategy,
                    message
                }
            ),
            // PrepareDescendant
            (arb_target(), arb_pr_number()).prop_map(|(descendant_branch, predecessor_pr)| {
                GitEffect::PrepareDescendant {
                    descendant_branch,
                    predecessor_pr,
                }
            }),
            // MergeReconcile
            (
                arb_target(),
                arb_pr_number(),
                arb_sha(),
                arb_sha(),
                arb_sha(),
                arb_target(),
            )
                .prop_map(
                    |(
                        descendant_branch,
                        predecessor_pr,
                        squash_sha,
                        expected_squash_parent,
                        predecessor_pre_squash_head,
                        default_branch,
                    )| {
                        GitEffect::MergeReconcile {
                            descendant_branch,
                            predecessor_pr,
                            squash_sha,
                            expected_squash_parent,
                            predecessor_pre_squash_head,
                            default_branch,
                        }
                    },
                ),
            // CatchUpDescendant
            (arb_target(), arb_target()).prop_map(|(descendant_branch, default_branch)| {
                GitEffect::CatchUpDescendant {
                    descendant_branch,
                    default_branch,
                }
            }),
            // UpdateRootForBehind
            (arb_target(), arb_target()).prop_map(|(branch, default_branch)| {
                GitEffect::UpdateRootForBehind {
                    branch,
                    default_branch,
                }
            }),
            // CheckPushCompleted
            (
                arb_target(),
                arb_sha(),
                arb_sha(),
                prop::option::of(arb_sha()),
            )
                .prop_map(|(branch, expected_tree, pre_push_sha, second_parent)| {
                    GitEffect::CheckPushCompleted {
                        branch,
                        expected_tree,
                        pre_push_sha,
                        second_parent,
                    }
                }),
            // CleanupWorktree
            Just(GitEffect::CleanupWorktree),
            // ValidateSquashCommit
            (arb_sha(), arb_target()).prop_map(|(squash_sha, default_branch)| {
                GitEffect::ValidateSquashCommit {
                    squash_sha,
                    default_branch,
                }
            }),
            // Push
            (arb_refspec(), any::<bool>())
                .prop_map(|(refspec, force)| GitEffect::Push { refspec, force }),
            // IsAncestor
            (arb_sha(), arb_sha()).prop_map(|(potential_ancestor, descendant)| {
                GitEffect::IsAncestor {
                    potential_ancestor,
                    descendant,
                }
            }),
            // RevParse
            arb_target().prop_map(|rev| GitEffect::RevParse { rev }),
            // CreateWorktree
            arb_worktree_name().prop_map(|name| GitEffect::CreateWorktree { name }),
            // RemoveWorktree
            arb_worktree_name().prop_map(|name| GitEffect::RemoveWorktree { name }),
            // MergeAbort
            Just(GitEffect::MergeAbort),
            // ResetHard
            arb_target().prop_map(|target| GitEffect::ResetHard { target }),
            // Clean
            (any::<bool>(), any::<bool>())
                .prop_map(|(directories, force)| GitEffect::Clean { directories, force }),
        ]
    }

    fn arb_push_point() -> impl Strategy<Value = PushPoint> {
        (arb_sha(), prop::option::of(arb_sha())).prop_map(|(expected_tree, pre_push_sha)| {
            PushPoint {
                expected_tree,
                pre_push_sha,
            }
        })
    }

    fn arb_git_response() -> impl Strategy<Value = GitResponse> {
        prop_oneof![
            Just(GitResponse::Ok),
            arb_sha().prop_map(GitResponse::Sha),
            any::<bool>().prop_map(GitResponse::Bool),
            (any::<bool>(), prop::option::of(arb_sha())).prop_map(|(completed, remote_head)| {
                GitResponse::PushCheck {
                    completed,
                    remote_head,
                }
            }),
            arb_merge_outcome().prop_map(GitResponse::Merge),
            arb_push_outcome().prop_map(GitResponse::Push),
            (
                arb_merge_outcome(),
                arb_sha(),
                prop::option::of(arb_push_point())
            )
                .prop_map(|(merge, predecessor_head, push_point)| {
                    GitResponse::Prepared {
                        merge,
                        predecessor_head,
                        push_point,
                    }
                }),
            (arb_merge_outcome(), prop::option::of(arb_push_point()))
                .prop_map(|(merge, push_point)| GitResponse::MergedForPush { merge, push_point }),
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

    // ─── Outcome mirror tests ─────────────────────────────────────────────────

    mod outcome_mirrors {
        use super::*;

        proptest! {
            #[test]
            fn merge_outcome_serde_roundtrip(outcome in arb_merge_outcome()) {
                let json = serde_json::to_string(&outcome).unwrap();
                let parsed: MergeOutcome = serde_json::from_str(&json).unwrap();
                prop_assert_eq!(outcome, parsed);
            }

            #[test]
            fn push_outcome_serde_roundtrip(outcome in arb_push_outcome()) {
                let json = serde_json::to_string(&outcome).unwrap();
                let parsed: PushOutcome = serde_json::from_str(&json).unwrap();
                prop_assert_eq!(outcome, parsed);
            }

            /// The mirror conversion is variant- and field-faithful: no git
            /// domain outcome is collapsed or dropped on its way into the
            /// effect vocabulary.
            #[test]
            fn merge_result_conversion_is_faithful(outcome in arb_merge_outcome()) {
                let result = match outcome.clone() {
                    MergeOutcome::Success { commit_sha } =>
                        crate::git::MergeResult::Success { commit_sha },
                    MergeOutcome::Conflict { conflicting_files } =>
                        crate::git::MergeResult::Conflict { conflicting_files },
                    MergeOutcome::AlreadyUpToDate => crate::git::MergeResult::AlreadyUpToDate,
                };
                prop_assert_eq!(MergeOutcome::from(result), outcome);
            }

            #[test]
            fn push_result_conversion_is_faithful(outcome in arb_push_outcome()) {
                let result = match outcome.clone() {
                    PushOutcome::Pushed { pushed_sha } =>
                        crate::git::push::PushResult::Success { pushed_sha },
                    PushOutcome::AlreadyUpToDate =>
                        crate::git::push::PushResult::AlreadyUpToDate,
                    PushOutcome::Rejected { details } =>
                        crate::git::push::PushResult::Rejected { details },
                };
                prop_assert_eq!(PushOutcome::from(result), outcome);
            }
        }
    }
}
