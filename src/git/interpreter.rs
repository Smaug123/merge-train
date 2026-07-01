//! The git effect interpreter (cascade engine stage M4): the shell bridge
//! from [`GitEffect`] data to the proven functions in this module.
//!
//! [`WorktreeGitInterpreter::interpret`] is **synchronous**: the per-repo
//! worker is a dedicated OS thread (not a tokio task — the plan's original
//! §(f) concurrency model is superseded by the S4 worker spine), so the
//! bridging decision of *where* blocking git work runs belongs to M5's
//! executor, not here. The interpreter holds no mutable state; every arm maps
//! one effect to one already-hardened function.
//!
//! Domain outcomes (conflicts, push rejections) come back inside
//! `Ok(GitResponse::..)` — the engine branches on them. `Err(GitError)` is
//! reserved for conditions the merge functions refuse on (the force-push
//! guards) and uncharacterizable failures; [`classify_git_error`] maps those
//! onto the engine's [`EffectError`] vocabulary (skip / park / abort).

use std::path::PathBuf;

use crate::cascade::EffectError;
use crate::effects::{GitEffect, GitResponse, PushPoint};
use crate::types::{PrNumber, TrainErrorKind};

use super::merge::{
    ReconcileRequest, catch_up_descendant, is_valid_squash_merge, prepare_descendant,
    reconcile_descendant, update_root_for_behind,
};
use super::push::{capture_pre_push_state, is_push_completed, push_head_to_branch};
use super::recovery::cleanup_worktree_on_abort;
use super::worktree::{remove_worktree, worktree_for_stack};
use super::{
    CommitIdentity, GitConfig, GitError, GitResult, MergeResult, fetch, parse_stack_dir_name,
    rev_parse,
};

/// Interprets [`GitEffect`]s for one train against its `stack-<root>`
/// worktree.
///
/// Worktree-scoped effects run in `config.worktree_path(root)`; the worktree
/// lifecycle effects (`CreateWorktree`/`RemoveWorktree`) are repo-scoped and
/// resolve their target from the effect's own `stack-<n>` name.
///
/// Precondition: the repository clone (`config.clone_dir()`) exists —
/// clone-on-first-use is the worker/bootstrap's job (M5/M6), and
/// `CreateWorktree` must have run before any worktree-scoped effect.
#[derive(Debug, Clone)]
pub struct WorktreeGitInterpreter {
    config: GitConfig,
    worktree: PathBuf,
    identity: CommitIdentity,
}

impl WorktreeGitInterpreter {
    /// An interpreter for the train rooted at `root`. The commit identity
    /// comes from the config (`GitConfig.commit_identity`).
    pub fn new(config: GitConfig, root: PrNumber) -> Self {
        let worktree = config.worktree_path(root);
        let identity = config.commit_identity.clone();
        WorktreeGitInterpreter {
            config,
            worktree,
            identity,
        }
    }

    /// Executes one effect. The match is exhaustive over the vocabulary: every
    /// variant the engine can emit has an interpretation.
    pub fn interpret(&self, effect: &GitEffect) -> GitResult<GitResponse> {
        match effect {
            GitEffect::Fetch { refspecs } => {
                let refs: Vec<&str> = refspecs.iter().map(String::as_str).collect();
                fetch(&self.worktree, &refs)?;
                Ok(GitResponse::Ok)
            }

            GitEffect::RevParse { rev } => Ok(GitResponse::Sha(rev_parse(&self.worktree, rev)?)),

            GitEffect::PrepareDescendant {
                descendant_branch,
                predecessor_pr,
            } => {
                let result = prepare_descendant(
                    &self.worktree,
                    descendant_branch,
                    predecessor_pr.0,
                    &self.identity,
                )?;
                let push_point = self.push_point_after(&result.merge, descendant_branch)?;
                Ok(GitResponse::Prepared {
                    merge: result.merge.into(),
                    predecessor_head: result.predecessor_head,
                    push_point,
                })
            }

            GitEffect::MergeReconcile {
                descendant_branch,
                predecessor_pr,
                squash_sha,
                expected_squash_parent,
                predecessor_pre_squash_head,
                default_branch,
            } => {
                let request = ReconcileRequest {
                    descendant_branch,
                    predecessor_pr: predecessor_pr.0,
                    squash_sha,
                    expected_squash_parent,
                    predecessor_pre_squash_head,
                    default_branch,
                };
                let merge = reconcile_descendant(&self.worktree, &request, &self.identity)?;
                let push_point = self.push_point_after(&merge, descendant_branch)?;
                Ok(GitResponse::MergedForPush {
                    merge: merge.into(),
                    push_point,
                })
            }

            GitEffect::CatchUpDescendant {
                descendant_branch,
                default_branch,
            } => {
                let merge = catch_up_descendant(
                    &self.worktree,
                    descendant_branch,
                    default_branch,
                    &self.identity,
                )?;
                let push_point = self.push_point_after(&merge, descendant_branch)?;
                Ok(GitResponse::MergedForPush {
                    merge: merge.into(),
                    push_point,
                })
            }

            GitEffect::UpdateRootForBehind {
                branch,
                default_branch,
            } => {
                let merge =
                    update_root_for_behind(&self.worktree, branch, default_branch, &self.identity)?;
                let push_point = self.push_point_after(&merge, branch)?;
                Ok(GitResponse::MergedForPush {
                    merge: merge.into(),
                    push_point,
                })
            }

            GitEffect::Push { branch } => {
                let outcome = push_head_to_branch(&self.worktree, branch)?;
                Ok(GitResponse::Push(outcome.into()))
            }

            GitEffect::CheckPushCompleted {
                branch,
                expected_tree,
                pre_push_sha,
                second_parent,
            } => {
                let completion = is_push_completed(
                    &self.worktree,
                    branch,
                    expected_tree,
                    pre_push_sha,
                    second_parent.as_ref(),
                )?;
                Ok(GitResponse::PushCheck {
                    completed: completion.completed,
                    remote_head: completion.remote_head,
                })
            }

            GitEffect::ValidateSquashCommit {
                squash_sha,
                default_branch,
            } => Ok(GitResponse::Bool(is_valid_squash_merge(
                &self.worktree,
                squash_sha,
                default_branch,
            )?)),

            GitEffect::CreateWorktree { name } => {
                let root = stack_root_of(name)?;
                worktree_for_stack(&self.config, root)?;
                Ok(GitResponse::Ok)
            }

            GitEffect::RemoveWorktree { name } => {
                let root = stack_root_of(name)?;
                remove_worktree(&self.config, root)?;
                Ok(GitResponse::Ok)
            }

            GitEffect::CleanupWorktree => {
                cleanup_worktree_on_abort(&self.worktree)?;
                Ok(GitResponse::Ok)
            }
        }
    }

    /// The push point for a just-completed merge: captured only after a
    /// `Success` (an `AlreadyUpToDate` merge needs no push; a `Conflict`
    /// allows none), while the worktree HEAD still IS the merge result.
    fn push_point_after(&self, merge: &MergeResult, branch: &str) -> GitResult<Option<PushPoint>> {
        match merge {
            MergeResult::Success { .. } => {
                let (expected_tree, pre_push_sha) = capture_pre_push_state(&self.worktree, branch)?;
                Ok(Some(PushPoint {
                    expected_tree,
                    pre_push_sha,
                }))
            }
            MergeResult::Conflict { .. } | MergeResult::AlreadyUpToDate => Ok(None),
        }
    }
}

/// The stack root named by a `stack-<n>` worktree effect.
fn stack_root_of(name: &str) -> GitResult<PrNumber> {
    parse_stack_dir_name(std::path::Path::new(name)).ok_or_else(|| GitError::WorktreeError {
        details: format!("worktree effect names must be stack-<pr>, got {name:?}"),
    })
}

/// Maps a [`GitError`] onto the engine's failure vocabulary. This table is
/// the fixed point the engine's skip/park/abort branching rests on:
///
/// - A missing remote ref is [`EffectError::BranchGone`] — during descendant
///   processing the engine skips the descendant.
/// - The merge functions' guard refusals are permanent aborts with their
///   exact kinds (the force-push race, missing preparation, non-squash
///   shapes).
/// - Everything uncharacterizable is [`EffectError::Transient`]: the train
///   parks and re-derives from the durable ledger on the next observation.
///   A *deterministic* failure therefore re-parks per event — bounded, loud
///   in logs, and strictly safer than aborting trains on network blips.
/// - Malformed inputs/outputs (`InvalidSha`, `InvalidCheckoutTarget`) mean
///   the engine or interpreter constructed something impossible: abort as an
///   internal invariant violation rather than retrying a bug.
pub fn classify_git_error(error: &GitError) -> EffectError {
    match error {
        GitError::FetchFailed { refspec, .. } => EffectError::BranchGone {
            branch: refspec.clone(),
        },
        GitError::PredecessorHeadChanged { .. } => EffectError::Permanent {
            kind: TrainErrorKind::HeadShaChanged,
            detail: error.to_string(),
        },
        GitError::PreparationNotCompleted { .. } => EffectError::Permanent {
            kind: TrainErrorKind::PreparationMissing,
            detail: error.to_string(),
        },
        GitError::InvalidSquashCommit { .. } => EffectError::Permanent {
            kind: TrainErrorKind::NonSquashMerge,
            detail: error.to_string(),
        },
        GitError::InvalidSha(_) | GitError::InvalidCheckoutTarget { .. } => {
            EffectError::Permanent {
                kind: TrainErrorKind::InternalInvariantViolation,
                detail: error.to_string(),
            }
        }
        GitError::CommandFailed { .. } | GitError::WorktreeError { .. } | GitError::Io(_) => {
            EffectError::Transient {
                detail: error.to_string(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::effects::{MergeOutcome, PushOutcome};
    use crate::git::test_support::{
        create_branch_with_file, create_pr_ref, create_test_repo_with_origin, squash_merge_to_main,
    };
    use crate::git::run_git_sync;
    use crate::types::Sha;

    /// A root PR (#1, branch `pr-1`) with one stacked descendant (#2,
    /// `pr-2`), where the root advanced after the descendant forked — so
    /// preparation is a real merge, not already-up-to-date. Returns the
    /// interpreter (train rooted at #1) and the root's current pin.
    fn stacked_fixture() -> (tempfile::TempDir, GitConfig, WorktreeGitInterpreter, Sha) {
        let (temp, config, _initial) = create_test_repo_with_origin();
        let pred_v1 = create_branch_with_file(&config, "pr-1", "pred.txt", "v1", "main");
        create_pr_ref(&config, 1, &pred_v1);
        let _desc = create_branch_with_file(&config, "pr-2", "desc.txt", "desc", "pr-1");
        create_pr_ref(&config, 2, &_desc);
        // The root advances after the descendant forked.
        let pred_v2 = create_branch_with_file(&config, "pr-1", "pred2.txt", "v2", "pr-1");
        create_pr_ref(&config, 1, &pred_v2);

        let interpreter = WorktreeGitInterpreter::new(config.clone(), PrNumber(1));
        interpreter
            .interpret(&GitEffect::CreateWorktree {
                name: "stack-1".to_string(),
            })
            .unwrap();
        (temp, config, interpreter, pred_v2)
    }

    #[test]
    fn prepare_success_pins_head_and_captures_push_point() {
        let (_temp, config, interpreter, pin) = stacked_fixture();
        let desc_head_before =
            crate::git::push::get_remote_ref(&config.worktree_path(PrNumber(1)), "pr-2")
                .unwrap()
                .unwrap();

        let response = interpreter
            .interpret(&GitEffect::PrepareDescendant {
                descendant_branch: "pr-2".to_string(),
                predecessor_pr: PrNumber(1),
            })
            .unwrap();
        let GitResponse::Prepared {
            merge,
            predecessor_head,
            push_point,
        } = response
        else {
            panic!("expected Prepared, got {response:?}");
        };
        assert!(matches!(merge, MergeOutcome::Success { .. }));
        assert_eq!(
            predecessor_head, pin,
            "the pin is the PR ref's current head"
        );
        let push_point = push_point.expect("a successful merge captures a push point");
        assert_eq!(
            push_point.pre_push_sha,
            Some(desc_head_before),
            "pre-push sha is the remote head before our push"
        );

        // The push lands and the completion check proves it, returning the
        // (proven ours) remote head.
        let pushed = interpreter
            .interpret(&GitEffect::Push {
                branch: "pr-2".to_string(),
            })
            .unwrap();
        let GitResponse::Push(PushOutcome::Pushed { pushed_sha }) = pushed else {
            panic!("expected Pushed, got {pushed:?}");
        };
        let check = interpreter
            .interpret(&GitEffect::CheckPushCompleted {
                branch: "pr-2".to_string(),
                expected_tree: push_point.expected_tree.clone(),
                pre_push_sha: push_point.pre_push_sha.clone().unwrap(),
                second_parent: None,
            })
            .unwrap();
        assert_eq!(
            check,
            GitResponse::PushCheck {
                completed: true,
                remote_head: Some(pushed_sha),
            }
        );
    }

    #[test]
    fn prepare_already_up_to_date_has_no_push_point() {
        // The descendant forked from the root's current head: nothing to merge.
        let (_temp, config, _initial) = create_test_repo_with_origin();
        let pred = create_branch_with_file(&config, "pr-1", "pred.txt", "v1", "main");
        create_pr_ref(&config, 1, &pred);
        let desc = create_branch_with_file(&config, "pr-2", "desc.txt", "desc", "pr-1");
        create_pr_ref(&config, 2, &desc);

        let interpreter = WorktreeGitInterpreter::new(config, PrNumber(1));
        interpreter
            .interpret(&GitEffect::CreateWorktree {
                name: "stack-1".to_string(),
            })
            .unwrap();
        let response = interpreter
            .interpret(&GitEffect::PrepareDescendant {
                descendant_branch: "pr-2".to_string(),
                predecessor_pr: PrNumber(1),
            })
            .unwrap();
        assert!(
            matches!(
                response,
                GitResponse::Prepared {
                    merge: MergeOutcome::AlreadyUpToDate,
                    push_point: None,
                    ..
                }
            ),
            "descendant already contains the pin: no push point, got {response:?}"
        );
    }

    #[test]
    fn prepare_conflict_is_a_domain_outcome() {
        let (_temp, config, _initial) = create_test_repo_with_origin();
        // Root and descendant both write shared.txt with different content,
        // and the descendant does NOT contain the root's version.
        let desc = create_branch_with_file(&config, "pr-2", "shared.txt", "theirs", "main");
        create_pr_ref(&config, 2, &desc);
        let pred = create_branch_with_file(&config, "pr-1", "shared.txt", "ours", "main");
        create_pr_ref(&config, 1, &pred);

        let interpreter = WorktreeGitInterpreter::new(config, PrNumber(1));
        interpreter
            .interpret(&GitEffect::CreateWorktree {
                name: "stack-1".to_string(),
            })
            .unwrap();
        let response = interpreter
            .interpret(&GitEffect::PrepareDescendant {
                descendant_branch: "pr-2".to_string(),
                predecessor_pr: PrNumber(1),
            })
            .unwrap();
        let GitResponse::Prepared {
            merge: MergeOutcome::Conflict { conflicting_files },
            push_point: None,
            ..
        } = response
        else {
            panic!("expected a conflict outcome, got {response:?}");
        };
        assert!(conflicting_files.contains(&"shared.txt".to_string()));

        // The abort-path cleanup returns the worktree to a usable state.
        interpreter.interpret(&GitEffect::CleanupWorktree).unwrap();
    }

    #[test]
    fn foreign_advance_rejects_the_push() {
        let (_temp, config, interpreter, _pin) = stacked_fixture();
        let response = interpreter
            .interpret(&GitEffect::PrepareDescendant {
                descendant_branch: "pr-2".to_string(),
                predecessor_pr: PrNumber(1),
            })
            .unwrap();
        assert!(matches!(
            response,
            GitResponse::Prepared {
                merge: MergeOutcome::Success { .. },
                ..
            }
        ));
        // A human lands a commit on pr-2 between our merge and our push.
        create_branch_with_file(&config, "pr-2", "human.txt", "fix", "pr-2");

        let pushed = interpreter
            .interpret(&GitEffect::Push {
                branch: "pr-2".to_string(),
            })
            .unwrap();
        assert!(
            matches!(pushed, GitResponse::Push(PushOutcome::Rejected { .. })),
            "a foreign advance must reject, got {pushed:?}"
        );
    }

    #[test]
    fn deleted_branch_classifies_as_branch_gone() {
        let (_temp, config, interpreter, _pin) = stacked_fixture();
        // The descendant's branch disappears.
        run_git_sync(
            &config.clone_dir(),
            &["update-ref", "-d", "refs/heads/pr-2"],
        )
        .unwrap();

        let error = interpreter
            .interpret(&GitEffect::PrepareDescendant {
                descendant_branch: "pr-2".to_string(),
                predecessor_pr: PrNumber(1),
            })
            .unwrap_err();
        assert!(
            matches!(classify_git_error(&error), EffectError::BranchGone { .. }),
            "missing branch must classify BranchGone, got {error:?}"
        );

        // The same holds for the plain-fetch paths (catch-up).
        let error = interpreter
            .interpret(&GitEffect::CatchUpDescendant {
                descendant_branch: "pr-2".to_string(),
                default_branch: "main".to_string(),
            })
            .unwrap_err();
        assert!(
            matches!(classify_git_error(&error), EffectError::BranchGone { .. }),
            "missing branch in catch-up must classify BranchGone, got {error:?}"
        );
    }

    #[test]
    fn reconcile_after_real_squash_and_guard_refusal() {
        let (_temp, config, interpreter, pin) = stacked_fixture();
        // Prepare + push, as the engine would.
        interpreter
            .interpret(&GitEffect::PrepareDescendant {
                descendant_branch: "pr-2".to_string(),
                predecessor_pr: PrNumber(1),
            })
            .unwrap();
        interpreter
            .interpret(&GitEffect::Push {
                branch: "pr-2".to_string(),
            })
            .unwrap();

        // GitHub squashes the root.
        let squash = squash_merge_to_main(&config, &pin);

        // Parent capture, as the engine plans it.
        interpreter
            .interpret(&GitEffect::Fetch {
                refspecs: vec!["main".to_string()],
            })
            .unwrap();
        let parent = interpreter
            .interpret(&GitEffect::RevParse {
                rev: format!("{}^", squash.squash_sha),
            })
            .unwrap();
        assert_eq!(parent, GitResponse::Sha(squash.prior_main_head.clone()));

        // Reconcile: merges only, push point captured, second-parent check
        // proves the pushed ours-merge.
        let response = interpreter
            .interpret(&GitEffect::MergeReconcile {
                descendant_branch: "pr-2".to_string(),
                predecessor_pr: PrNumber(1),
                squash_sha: squash.squash_sha.clone(),
                expected_squash_parent: squash.prior_main_head.clone(),
                predecessor_pre_squash_head: pin.clone(),
                default_branch: "main".to_string(),
            })
            .unwrap();
        let GitResponse::MergedForPush {
            merge: MergeOutcome::Success { .. },
            push_point: Some(push_point),
        } = response
        else {
            panic!("expected a successful reconcile, got {response:?}");
        };
        interpreter
            .interpret(&GitEffect::Push {
                branch: "pr-2".to_string(),
            })
            .unwrap();
        let check = interpreter
            .interpret(&GitEffect::CheckPushCompleted {
                branch: "pr-2".to_string(),
                expected_tree: push_point.expected_tree.clone(),
                pre_push_sha: push_point.pre_push_sha.clone().unwrap(),
                second_parent: Some(squash.squash_sha.clone()),
            })
            .unwrap();
        assert!(
            matches!(
                check,
                GitResponse::PushCheck {
                    completed: true,
                    ..
                }
            ),
            "reconcile push must verify with the squash as second parent"
        );

        // The squash is a valid squash commit.
        assert_eq!(
            interpreter
                .interpret(&GitEffect::ValidateSquashCommit {
                    squash_sha: squash.squash_sha.clone(),
                    default_branch: "main".to_string(),
                })
                .unwrap(),
            GitResponse::Bool(true)
        );

        // Guard refusal: a wrong pin (force-push race shape) refuses with the
        // exact permanent kind.
        let bogus_pin = Sha::parse("1".repeat(40)).unwrap();
        let error = interpreter
            .interpret(&GitEffect::MergeReconcile {
                descendant_branch: "pr-2".to_string(),
                predecessor_pr: PrNumber(1),
                squash_sha: squash.squash_sha.clone(),
                expected_squash_parent: squash.prior_main_head.clone(),
                predecessor_pre_squash_head: bogus_pin,
                default_branch: "main".to_string(),
            })
            .unwrap_err();
        assert!(
            matches!(
                classify_git_error(&error),
                EffectError::Permanent {
                    kind: TrainErrorKind::HeadShaChanged,
                    ..
                }
            ),
            "a pin mismatch must classify Permanent(HeadShaChanged), got {error:?}"
        );
    }

    #[test]
    fn worktree_lifecycle_effects() {
        let (_temp, config, _initial) = create_test_repo_with_origin();
        let interpreter = WorktreeGitInterpreter::new(config.clone(), PrNumber(7));

        interpreter
            .interpret(&GitEffect::CreateWorktree {
                name: "stack-7".to_string(),
            })
            .unwrap();
        assert!(config.worktree_path(PrNumber(7)).exists());

        // Cleanup on a fresh worktree is a harmless no-op.
        interpreter.interpret(&GitEffect::CleanupWorktree).unwrap();

        interpreter
            .interpret(&GitEffect::RemoveWorktree {
                name: "stack-7".to_string(),
            })
            .unwrap();
        assert!(!config.worktree_path(PrNumber(7)).exists());

        // A malformed worktree name is refused, not guessed at.
        let error = interpreter
            .interpret(&GitEffect::CreateWorktree {
                name: "bogus".to_string(),
            })
            .unwrap_err();
        assert!(matches!(error, GitError::WorktreeError { .. }));
    }

    #[test]
    fn classification_table_is_fixed() {
        let sha = || Sha::parse("a".repeat(40)).unwrap();
        let cases: Vec<(GitError, EffectError)> = vec![
            (
                GitError::FetchFailed {
                    refspec: "pr-2".to_string(),
                    details: "gone".to_string(),
                },
                EffectError::BranchGone {
                    branch: "pr-2".to_string(),
                },
            ),
            (
                GitError::PredecessorHeadChanged {
                    predecessor_pr: 1,
                    prepared: sha(),
                    squashed: sha(),
                },
                EffectError::Permanent {
                    kind: TrainErrorKind::HeadShaChanged,
                    detail: String::new(),
                },
            ),
            (
                GitError::PreparationNotCompleted {
                    descendant_branch: "pr-2".to_string(),
                    predecessor_head: sha(),
                    descendant_head: sha(),
                },
                EffectError::Permanent {
                    kind: TrainErrorKind::PreparationMissing,
                    detail: String::new(),
                },
            ),
            (
                GitError::InvalidSquashCommit {
                    squash_sha: sha(),
                    details: "two parents".to_string(),
                },
                EffectError::Permanent {
                    kind: TrainErrorKind::NonSquashMerge,
                    detail: String::new(),
                },
            ),
            (
                GitError::InvalidSha("junk".to_string()),
                EffectError::Permanent {
                    kind: TrainErrorKind::InternalInvariantViolation,
                    detail: String::new(),
                },
            ),
            (
                GitError::CommandFailed {
                    command: "git fetch".to_string(),
                    stderr: "network".to_string(),
                },
                EffectError::Transient {
                    detail: String::new(),
                },
            ),
        ];
        for (error, expected) in cases {
            let got = classify_git_error(&error);
            let matches = match (&got, &expected) {
                (EffectError::BranchGone { branch: a }, EffectError::BranchGone { branch: b }) => {
                    a == b
                }
                (
                    EffectError::Permanent { kind: a, .. },
                    EffectError::Permanent { kind: b, .. },
                ) => a == b,
                (EffectError::Transient { .. }, EffectError::Transient { .. }) => true,
                _ => false,
            };
            assert!(
                matches,
                "{error:?} classified as {got:?}, expected {expected:?}"
            );
        }
    }
}
