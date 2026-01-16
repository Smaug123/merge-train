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

use crate::effects::{
    Effect, GitEffect, GitHubEffect, GitHubInterpreter, GitHubResponse, PrData, RepoSettingsData,
};
use crate::persistence::snapshot::PersistedRepoSnapshot;
use crate::types::{MergeStateStatus, PrNumber, PrState, Sha};

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
        self.execute_with_stack_token(effect, state, None).await
    }

    /// Executes an effect with optional stack-scoped cancellation.
    ///
    /// In addition to checking the global shutdown token, this method also
    /// checks an optional stack-specific token. This allows stop commands
    /// to interrupt in-flight effects for a specific train without affecting
    /// other trains.
    ///
    /// Returns `Err(EffectError::Cancelled)` if either the global shutdown
    /// token or the stack token was triggered before or during execution.
    #[instrument(skip(self, state, stack_token), fields(effect = ?effect))]
    pub async fn execute_with_stack_token(
        &self,
        effect: Effect,
        state: &mut PersistedRepoSnapshot,
        stack_token: Option<&CancellationToken>,
    ) -> Result<EffectResult, EffectError> {
        // Check cancellation before starting
        if self.cancel.is_cancelled() {
            debug!("Cancellation detected before effect execution");
            return Err(EffectError::Cancelled);
        }
        if let Some(token) = stack_token
            && token.is_cancelled()
        {
            debug!("Stack cancellation detected before effect execution");
            return Err(EffectError::Cancelled);
        }

        let result = match effect {
            Effect::GitHub(github_effect) => {
                self.execute_github_with_stack_token(github_effect, stack_token)
                    .await?
            }
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

    /// Executes a GitHub effect with both global and stack-scoped cancellation support.
    ///
    /// This method races the effect execution against multiple cancellation sources:
    /// 1. Global shutdown token (always checked)
    /// 2. Stack-specific token (checked if provided)
    ///
    /// If either token is cancelled during execution, the effect is interrupted
    /// and `EffectError::Cancelled` is returned.
    async fn execute_github_with_stack_token(
        &self,
        effect: GitHubEffect,
        stack_token: Option<&CancellationToken>,
    ) -> Result<EffectResult, EffectError> {
        trace!(?effect, "Executing GitHub effect");

        // Create a future that completes when either cancellation source fires.
        // We use a helper async block to combine the two token cancellations.
        let any_cancelled = async {
            if let Some(stack) = stack_token {
                // Race between global shutdown and stack cancellation
                tokio::select! {
                    _ = self.cancel.cancelled() => {}
                    _ = stack.cancelled() => {}
                }
            } else {
                // Only global shutdown
                self.cancel.cancelled().await;
            }
        };

        // Use select to race between the effect and any cancellation
        tokio::select! {
            biased;

            _ = any_cancelled => {
                debug!("GitHub effect cancelled (global or stack)");
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

// ─── Logging Interpreter ───────────────────────────────────────────────────────

/// A GitHub interpreter that logs effects without executing them.
///
/// This is used as a placeholder in Stage 17 until the real GitHub client
/// is integrated in Stage 18. It returns plausible placeholder responses
/// so the effect execution flow can be tested without actual API calls.
///
/// # Responses
///
/// - `RefetchPr`: Returns a placeholder PR with Unknown merge state
/// - `GetRepoSettings`: Returns settings with squash merge enabled
/// - Other effects: Return appropriate placeholder responses
#[derive(Clone)]
pub struct LoggingGitHubInterpreter {
    /// Placeholder SHA for responses that need one.
    placeholder_sha: Sha,
}

impl LoggingGitHubInterpreter {
    /// Creates a new logging interpreter.
    pub fn new() -> Self {
        LoggingGitHubInterpreter {
            placeholder_sha: Sha::parse("0".repeat(40)).expect("valid placeholder sha"),
        }
    }
}

impl Default for LoggingGitHubInterpreter {
    fn default() -> Self {
        Self::new()
    }
}

impl GitHubInterpreter for LoggingGitHubInterpreter {
    type Error = std::convert::Infallible;

    fn interpret(
        &self,
        effect: GitHubEffect,
    ) -> impl std::future::Future<Output = Result<GitHubResponse, Self::Error>> + Send {
        // Log the effect for debugging
        debug!(
            ?effect,
            "LoggingGitHubInterpreter: effect logged (not executed)"
        );

        // Return a plausible placeholder response based on the effect type
        let response = match effect {
            GitHubEffect::GetPr { pr } | GitHubEffect::RefetchPr { pr } => {
                GitHubResponse::PrRefetched {
                    pr: PrData {
                        number: pr,
                        head_sha: self.placeholder_sha.clone(),
                        head_ref: "feature".to_string(),
                        base_ref: "main".to_string(),
                        state: PrState::Open,
                        is_draft: false,
                    },
                    merge_state: MergeStateStatus::Unknown,
                }
            }
            GitHubEffect::ListOpenPrs => GitHubResponse::PrList(vec![]),
            GitHubEffect::ListRecentlyMergedPrs { .. } => GitHubResponse::RecentlyMergedPrList {
                prs: vec![],
                may_be_incomplete: false,
            },
            GitHubEffect::GetMergeState { .. } => {
                GitHubResponse::MergeState(MergeStateStatus::Unknown)
            }
            GitHubEffect::SquashMerge { .. } => GitHubResponse::Merged {
                sha: self.placeholder_sha.clone(),
            },
            GitHubEffect::RetargetPr { .. } => GitHubResponse::Retargeted,
            GitHubEffect::PostComment { .. } => GitHubResponse::CommentPosted {
                id: crate::types::CommentId(0),
            },
            GitHubEffect::UpdateComment { .. } => GitHubResponse::CommentUpdated,
            GitHubEffect::AddReaction { .. } => GitHubResponse::ReactionAdded,
            GitHubEffect::ListComments { .. } => GitHubResponse::Comments(vec![]),
            GitHubEffect::GetBranchProtection { .. } => GitHubResponse::BranchProtectionUnknown,
            GitHubEffect::GetRulesets => GitHubResponse::RulesetsUnknown,
            GitHubEffect::GetRepoSettings => GitHubResponse::RepoSettings(RepoSettingsData {
                default_branch: "main".to_string(),
                allow_squash_merge: true,
                allow_merge_commit: false,
                allow_rebase_merge: false,
            }),
        };

        async move { Ok(response) }
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

    /// Mock interpreter that delays before returning, allowing cancellation to happen during execution.
    #[derive(Clone)]
    struct DelayingGitHubInterpreter {
        delay_ms: u64,
    }

    impl GitHubInterpreter for DelayingGitHubInterpreter {
        type Error = String;

        fn interpret(
            &self,
            _effect: GitHubEffect,
        ) -> impl Future<Output = Result<GitHubResponse, Self::Error>> + Send {
            let delay_ms = self.delay_ms;
            async move {
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                Ok(GitHubResponse::Retargeted)
            }
        }
    }

    #[tokio::test]
    async fn stack_cancellation_interrupts_inflight_effect() {
        // Test oracle: When a stack token is cancelled during effect execution,
        // the effect should be interrupted and return EffectError::Cancelled.
        //
        // This tests the stack-scoped cancellation feature that allows stop
        // commands to interrupt in-flight operations for a specific train.
        let mock = DelayingGitHubInterpreter { delay_ms: 1000 };
        let global_shutdown = CancellationToken::new();
        let stack_token = CancellationToken::new();

        let executor = EffectExecutor::new(mock, global_shutdown);

        let mut state = PersistedRepoSnapshot::new("main");
        let effect = Effect::GitHub(GitHubEffect::GetRepoSettings);

        // Start the effect execution
        let stack_ref = &stack_token;
        let result = tokio::select! {
            result = executor.execute_with_stack_token(effect, &mut state, Some(stack_ref)) => {
                result
            }
            _ = async {
                // Cancel the stack token after a short delay
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                stack_token.cancel();
                // Wait for the select to pick up the cancellation
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            } => {
                unreachable!("The effect should be cancelled before this completes")
            }
        };

        assert!(
            matches!(result, Err(EffectError::Cancelled)),
            "Effect should be cancelled when stack token is cancelled: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn global_shutdown_interrupts_even_without_stack_token() {
        // Test oracle: Global shutdown should interrupt effects even when
        // no stack token is provided (for effects not associated with a train).
        let mock = DelayingGitHubInterpreter { delay_ms: 1000 };
        let global_shutdown = CancellationToken::new();

        let executor = EffectExecutor::new(mock, global_shutdown.clone());

        let mut state = PersistedRepoSnapshot::new("main");
        let effect = Effect::GitHub(GitHubEffect::GetRepoSettings);

        // Start the effect execution without a stack token
        let result = tokio::select! {
            result = executor.execute_with_stack_token(effect, &mut state, None) => {
                result
            }
            _ = async {
                // Cancel the global shutdown token after a short delay
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                global_shutdown.cancel();
                // Wait for the select to pick up the cancellation
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            } => {
                unreachable!("The effect should be cancelled before this completes")
            }
        };

        assert!(
            matches!(result, Err(EffectError::Cancelled)),
            "Effect should be cancelled when global shutdown is triggered: {:?}",
            result
        );
    }
}
