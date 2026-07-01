//! Maps a plan's executed effect outcomes to the next [`Observation`].
//!
//! This keeps the executor (M5) free of domain knowledge: it runs effects,
//! collects [`EffectOutcome`]s, and hands them back. Which outcome carries
//! the observation — and how multiple responses combine (the preflight trio)
//! — is decided here, purely, where the model tests can pin it.

use crate::effects::github::GitHubEffect;
use crate::effects::{Effect, GitEffect, GitHubResponse, GitResponse};

use super::plan::{EffectOutcome, EffectResponse, Observation};

/// Why outcomes could not be mapped to an observation. Always an executor or
/// engine bug (a response variant that cannot answer the effect that was
/// issued), never a domain condition.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ObserveError {
    /// No outcomes were supplied.
    #[error("no effect outcomes to observe")]
    Empty,
    /// The final effect/response pair has no observation mapping.
    #[error("cannot observe response to effect: {detail}")]
    Unmappable {
        /// Debug rendering of the unmappable pair.
        detail: String,
    },
}

/// Derives the observation from a plan's effect outcomes.
///
/// Rules, in order:
/// 1. Any failed outcome → [`Observation::EffectFailed`] for the *last*
///    outcome (the executor stops at the first failure, so the failure is
///    always last).
/// 2. The preflight trio (`GetRepoSettings`, `GetBranchProtection`,
///    `GetRulesets` as the final three outcomes) → `PreflightFetched`.
/// 3. Otherwise the final outcome maps per the table below; earlier outcomes
///    are preludes (worktree setup, fetches) whose successes carry no
///    information.
pub fn observe(outcomes: &[EffectOutcome]) -> Result<Observation, ObserveError> {
    let last = outcomes.last().ok_or(ObserveError::Empty)?;

    if let Err(error) = &last.result {
        return Ok(Observation::EffectFailed {
            effect: last.effect.clone(),
            error: error.clone(),
        });
    }

    if let Some(preflight) = observe_preflight_trio(outcomes) {
        return Ok(preflight);
    }

    let response = last.result.as_ref().expect("checked above");
    observe_single(&last.effect, response).ok_or_else(|| ObserveError::Unmappable {
        detail: format!("{:?} -> {:?}", last.effect, response),
    })
}

/// The preflight trio, if the final three outcomes are exactly
/// settings/protection/rulesets.
fn observe_preflight_trio(outcomes: &[EffectOutcome]) -> Option<Observation> {
    let [a, b, c] = outcomes.last_chunk::<3>()?;
    match (&a.effect, &b.effect, &c.effect) {
        (
            Effect::GitHub(GitHubEffect::GetRepoSettings),
            Effect::GitHub(GitHubEffect::GetBranchProtection { .. }),
            Effect::GitHub(GitHubEffect::GetRulesets),
        ) => {}
        _ => return None,
    }
    let settings = match a.result.as_ref().ok()? {
        EffectResponse::GitHub(GitHubResponse::RepoSettings(s)) => s.clone(),
        _ => return None,
    };
    let branch_protection = match b.result.as_ref().ok()? {
        EffectResponse::GitHub(r) => r.clone(),
        EffectResponse::Git(_) => return None,
    };
    let rulesets = match c.result.as_ref().ok()? {
        EffectResponse::GitHub(r) => r.clone(),
        EffectResponse::Git(_) => return None,
    };
    Some(Observation::PreflightFetched {
        settings,
        branch_protection,
        rulesets,
    })
}

/// The single-outcome mapping table.
fn observe_single(effect: &Effect, response: &EffectResponse) -> Option<Observation> {
    match (effect, response) {
        (
            Effect::Git(GitEffect::PrepareDescendant {
                descendant_branch, ..
            }),
            EffectResponse::Git(GitResponse::Prepared {
                merge,
                predecessor_head,
                push_point,
            }),
        ) => Some(Observation::Prepared {
            branch: descendant_branch.clone(),
            merge: merge.clone(),
            predecessor_head: predecessor_head.clone(),
            push_point: push_point.clone(),
        }),

        (
            Effect::Git(GitEffect::MergeReconcile {
                descendant_branch, ..
            }),
            EffectResponse::Git(GitResponse::MergedForPush { merge, push_point }),
        ) => Some(Observation::Reconciled {
            branch: descendant_branch.clone(),
            merge: merge.clone(),
            push_point: push_point.clone(),
        }),

        (
            Effect::Git(GitEffect::CatchUpDescendant {
                descendant_branch, ..
            }),
            EffectResponse::Git(GitResponse::MergedForPush { merge, push_point }),
        ) => Some(Observation::CaughtUp {
            branch: descendant_branch.clone(),
            merge: merge.clone(),
            push_point: push_point.clone(),
        }),

        (
            Effect::Git(GitEffect::UpdateRootForBehind { branch, .. }),
            EffectResponse::Git(GitResponse::MergedForPush { merge, push_point }),
        ) => Some(Observation::RootCaughtUp {
            branch: branch.clone(),
            merge: merge.clone(),
            push_point: push_point.clone(),
        }),

        (
            Effect::Git(GitEffect::Push { refspec, .. }),
            EffectResponse::Git(GitResponse::Push(outcome)),
        ) => Some(Observation::Pushed {
            branch: branch_of_refspec(refspec),
            outcome: outcome.clone(),
        }),

        (
            Effect::GitHub(GitHubEffect::SquashMerge { pr, .. }),
            EffectResponse::GitHub(GitHubResponse::Merged { sha }),
        ) => Some(Observation::Squashed {
            pr: *pr,
            sha: sha.clone(),
        }),

        (Effect::Git(GitEffect::RevParse { rev }), EffectResponse::Git(GitResponse::Sha(sha))) => {
            Some(Observation::Resolved {
                rev: rev.clone(),
                sha: sha.clone(),
            })
        }

        (
            Effect::Git(GitEffect::ValidateSquashCommit { squash_sha, .. }),
            EffectResponse::Git(GitResponse::Bool(valid)),
        ) => Some(Observation::SquashValidated {
            sha: squash_sha.clone(),
            valid: *valid,
        }),

        (
            Effect::Git(GitEffect::CheckPushCompleted { branch, .. }),
            EffectResponse::Git(GitResponse::Bool(completed)),
        ) => Some(Observation::PushChecked {
            branch: branch.clone(),
            completed: *completed,
        }),

        (
            Effect::GitHub(GitHubEffect::RetargetPr { pr, .. }),
            EffectResponse::GitHub(GitHubResponse::Retargeted),
        ) => Some(Observation::Retargeted { pr: *pr }),

        (
            Effect::GitHub(GitHubEffect::RefetchPr { pr }),
            EffectResponse::GitHub(GitHubResponse::PrRefetched {
                pr: data,
                merge_state,
            }),
        ) => Some(Observation::PrRefreshed {
            pr: *pr,
            data: data.clone(),
            merge_state: *merge_state,
        }),

        (
            Effect::GitHub(GitHubEffect::PostComment { .. }),
            EffectResponse::GitHub(GitHubResponse::CommentPosted { id }),
        ) => Some(Observation::StatusCommentCreated { id: *id }),

        _ => None,
    }
}

/// The branch name of a `HEAD:refs/heads/<branch>` push refspec (the only
/// shape the engine emits). Falls back to the raw refspec so an unexpected
/// shape surfaces loudly downstream instead of silently matching nothing.
fn branch_of_refspec(refspec: &str) -> String {
    refspec
        .strip_prefix("HEAD:refs/heads/")
        .unwrap_or(refspec)
        .to_string()
}
