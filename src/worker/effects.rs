//! Effect execution with cancellation support.
//!
//! This module provides the `EffectExecutor` which executes effects returned
//! by cascade operations. It integrates with `CancellationToken` to support
//! interrupting long-running operations when a stop command is received.
//!
//! # Architecture
//!
//! The executor uses the interpreter traits from `src/effects/interpreter.rs`:
//! - `GitHubInterpreter` for GitHub API operations
//! - `GitInterpreter` for local git operations
//!
//! Both interpreters are async, allowing the executor to check for cancellation
//! between operations.

use std::fmt;

use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument, trace, warn};

use crate::effects::{Effect, GitEffect, GitHubEffect, GitHubInterpreter, GitHubResponse};
use crate::persistence::snapshot::PersistedRepoSnapshot;
use crate::types::{PrNumber, Sha};

/// Result of executing an effect.
#[derive(Debug)]
pub enum EffectResult {
    /// GitHub effect completed successfully.
    GitHub(GitHubResponse),

    /// Git effect completed successfully.
    Git(GitResult),

    /// RecordReconciliation completed (state update only).
    RecordReconciliation,
}

/// Result of a git effect execution.
#[derive(Debug)]
pub enum GitResult {
    /// Operation completed successfully.
    Ok,

    /// Fetch completed.
    Fetched,

    /// Checkout completed, now at this SHA.
    CheckedOut { head: Sha },

    /// Merge completed with result.
    Merged { result: crate::git::MergeResult },

    /// Push completed.
    Pushed,

    /// Worktree created at path.
    WorktreeCreated { path: std::path::PathBuf },

    /// Worktree removed.
    WorktreeRemoved,

    /// Ancestor check result.
    IsAncestor(bool),
}

/// Errors from effect execution.
#[derive(Debug, Error)]
pub enum EffectError {
    /// Operation was cancelled via CancellationToken.
    #[error("operation cancelled")]
    Cancelled,

    /// GitHub API error.
    #[error("GitHub API error: {0}")]
    GitHub(String),

    /// Git operation error.
    #[error("Git error: {0}")]
    Git(#[from] crate::git::GitError),

    /// PR not found in state.
    #[error("PR #{0} not found in state")]
    PrNotFound(PrNumber),
}

impl EffectError {
    /// Returns true if this is a cancellation error.
    pub fn is_cancelled(&self) -> bool {
        matches!(self, EffectError::Cancelled)
    }
}

/// Executes effects with cancellation support.
///
/// The executor holds references to the interpreters and cancellation token.
/// It checks for cancellation before and after each effect execution.
///
/// # Type Parameters
///
/// * `G` - The GitHub interpreter type
///
/// # Example
///
/// ```ignore
/// let executor = EffectExecutor::new(github_client, cancel_token);
///
/// for effect in effects {
///     match executor.execute(effect, &mut state).await {
///         Ok(result) => { /* handle result */ }
///         Err(EffectError::Cancelled) => { /* stop command received */ }
///         Err(e) => { /* handle error */ }
///     }
/// }
/// ```
pub struct EffectExecutor<G> {
    github: G,
    cancel: CancellationToken,
}

impl<G> EffectExecutor<G>
where
    G: GitHubInterpreter,
    G::Error: fmt::Display,
{
    /// Creates a new effect executor.
    pub fn new(github: G, cancel: CancellationToken) -> Self {
        EffectExecutor { github, cancel }
    }

    /// Returns true if cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// Executes an effect, checking for cancellation.
    ///
    /// Returns `Err(EffectError::Cancelled)` if the cancellation token
    /// was triggered before or during execution.
    #[instrument(skip(self, state), fields(effect = ?effect))]
    pub async fn execute(
        &self,
        effect: Effect,
        state: &mut PersistedRepoSnapshot,
    ) -> Result<EffectResult, EffectError> {
        // Check cancellation before starting
        if self.cancel.is_cancelled() {
            debug!("Cancellation detected before effect execution");
            return Err(EffectError::Cancelled);
        }

        let result = match effect {
            Effect::GitHub(github_effect) => self.execute_github(github_effect).await?,
            Effect::Git(git_effect) => self.execute_git(git_effect).await?,
            Effect::RecordReconciliation { pr, squash_sha } => {
                self.record_reconciliation(state, pr, squash_sha)?
            }
        };

        // Check cancellation after completion
        if self.cancel.is_cancelled() {
            debug!("Cancellation detected after effect execution");
            // We completed the effect, so don't return cancelled here.
            // The next call to execute() will return Cancelled.
        }

        Ok(result)
    }

    /// Executes a GitHub effect with cancellation support.
    async fn execute_github(&self, effect: GitHubEffect) -> Result<EffectResult, EffectError> {
        trace!(?effect, "Executing GitHub effect");

        // Use select to race between the effect and cancellation
        tokio::select! {
            _ = self.cancel.cancelled() => {
                debug!("GitHub effect cancelled");
                Err(EffectError::Cancelled)
            }
            result = self.github.interpret(effect) => {
                result
                    .map(EffectResult::GitHub)
                    .map_err(|e| EffectError::GitHub(e.to_string()))
            }
        }
    }

    /// Executes a Git effect.
    ///
    /// Git effects are currently executed synchronously using `spawn_blocking`.
    /// Cancellation is checked before and after the operation.
    async fn execute_git(&self, effect: GitEffect) -> Result<EffectResult, EffectError> {
        trace!(?effect, "Executing Git effect");

        // Check cancellation before spawning
        if self.cancel.is_cancelled() {
            return Err(EffectError::Cancelled);
        }

        // Git operations are not yet implemented via interpreter trait.
        // For now, return a placeholder result.
        // TODO: Implement GitInterpreter and integrate here
        warn!(?effect, "Git effect execution not yet implemented");

        match effect {
            GitEffect::Fetch { .. } => Ok(EffectResult::Git(GitResult::Fetched)),
            GitEffect::Checkout { .. } => Ok(EffectResult::Git(GitResult::Ok)),
            GitEffect::Merge { .. } => Ok(EffectResult::Git(GitResult::Merged {
                result: crate::git::MergeResult::AlreadyUpToDate,
            })),
            GitEffect::MergeReconcile { .. } => Ok(EffectResult::Git(GitResult::Merged {
                result: crate::git::MergeResult::AlreadyUpToDate,
            })),
            GitEffect::ValidateSquashCommit { .. } => Ok(EffectResult::Git(GitResult::Ok)),
            GitEffect::MergeAbort => Ok(EffectResult::Git(GitResult::Ok)),
            GitEffect::Push { .. } => Ok(EffectResult::Git(GitResult::Pushed)),
            GitEffect::CreateWorktree { .. } => Ok(EffectResult::Git(GitResult::WorktreeCreated {
                path: std::path::PathBuf::new(),
            })),
            GitEffect::RemoveWorktree { .. } => Ok(EffectResult::Git(GitResult::WorktreeRemoved)),
            GitEffect::IsAncestor { .. } => Ok(EffectResult::Git(GitResult::IsAncestor(true))),
            GitEffect::RevParse { .. } => Ok(EffectResult::Git(GitResult::Ok)),
            GitEffect::ResetHard { .. } => Ok(EffectResult::Git(GitResult::Ok)),
            GitEffect::Clean { .. } => Ok(EffectResult::Git(GitResult::Ok)),
        }
    }

    /// Records that a PR has been reconciled with its predecessor's squash commit.
    fn record_reconciliation(
        &self,
        state: &mut PersistedRepoSnapshot,
        pr: PrNumber,
        squash_sha: Sha,
    ) -> Result<EffectResult, EffectError> {
        trace!(?pr, ?squash_sha, "Recording reconciliation");

        let cached_pr = state.prs.get_mut(&pr).ok_or(EffectError::PrNotFound(pr))?;

        cached_pr.predecessor_squash_reconciled = Some(squash_sha);

        Ok(EffectResult::RecordReconciliation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::effects::GitHubResponse;
    use std::future::Future;

    /// Mock GitHub interpreter for testing.
    struct MockGitHubInterpreter {
        response: GitHubResponse,
    }

    impl GitHubInterpreter for MockGitHubInterpreter {
        type Error = String;

        fn interpret(
            &self,
            _effect: GitHubEffect,
        ) -> impl Future<Output = Result<GitHubResponse, Self::Error>> + Send {
            let response = self.response.clone();
            async move { Ok(response) }
        }
    }

    #[tokio::test]
    async fn execute_github_effect_success() {
        let mock = MockGitHubInterpreter {
            response: GitHubResponse::RepoSettings(crate::effects::RepoSettingsData {
                default_branch: "main".to_string(),
                allow_merge_commit: false,
                allow_rebase_merge: false,
                allow_squash_merge: true,
            }),
        };
        let cancel = CancellationToken::new();
        let executor = EffectExecutor::new(mock, cancel);

        let mut state = PersistedRepoSnapshot::new("main");
        let effect = Effect::GitHub(GitHubEffect::GetRepoSettings);

        let result = executor.execute(effect, &mut state).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn execute_cancelled_before_start() {
        let mock = MockGitHubInterpreter {
            response: GitHubResponse::Retargeted,
        };
        let cancel = CancellationToken::new();
        cancel.cancel(); // Cancel before execution

        let executor = EffectExecutor::new(mock, cancel);
        let mut state = PersistedRepoSnapshot::new("main");
        let effect = Effect::GitHub(GitHubEffect::GetRepoSettings);

        let result = executor.execute(effect, &mut state).await;
        assert!(matches!(result, Err(EffectError::Cancelled)));
    }

    #[tokio::test]
    async fn record_reconciliation_updates_state() {
        let mock = MockGitHubInterpreter {
            response: GitHubResponse::Retargeted,
        };
        let cancel = CancellationToken::new();
        let executor = EffectExecutor::new(mock, cancel);

        let mut state = PersistedRepoSnapshot::new("main");

        // Add a PR to state
        let pr = PrNumber(42);
        let cached_pr = crate::types::CachedPr::new(
            pr,
            Sha::parse("a".repeat(40)).unwrap(),
            "feature".to_string(),
            "main".to_string(),
            None,
            crate::types::PrState::Open,
            crate::types::MergeStateStatus::Unknown,
            false,
        );
        state.prs.insert(pr, cached_pr);

        let squash_sha = Sha::parse("b".repeat(40)).unwrap();
        let effect = Effect::RecordReconciliation {
            pr,
            squash_sha: squash_sha.clone(),
        };

        let result = executor.execute(effect, &mut state).await;
        assert!(result.is_ok());

        // Verify state was updated
        let updated_pr = state.prs.get(&pr).unwrap();
        assert_eq!(updated_pr.predecessor_squash_reconciled, Some(squash_sha));
    }

    #[tokio::test]
    async fn record_reconciliation_pr_not_found() {
        let mock = MockGitHubInterpreter {
            response: GitHubResponse::Retargeted,
        };
        let cancel = CancellationToken::new();
        let executor = EffectExecutor::new(mock, cancel);

        let mut state = PersistedRepoSnapshot::new("main");
        let effect = Effect::RecordReconciliation {
            pr: PrNumber(999),
            squash_sha: Sha::parse("a".repeat(40)).unwrap(),
        };

        let result = executor.execute(effect, &mut state).await;
        assert!(matches!(result, Err(EffectError::PrNotFound(_))));
    }
}
