//! Effect interpreter traits.
//!
//! These traits define how effects are executed. Implementations are provided
//! in later stages:
//! - Stage 8: Git interpreter (local git operations)
//! - Stage 9: GitHub interpreter (octocrab-based API calls)
//!
//! The trait-based design enables:
//! - Mock interpreters for testing
//! - Logging/tracing interpreters
//! - Batching interpreters (future optimization)

use std::future::Future;

use super::git::{GitEffect, GitResponse};
use super::github::{GitHubEffect, GitHubResponse};

/// Interprets GitHub effects against the GitHub API.
///
/// Implementations are constructed with a `RepoId`, so all effects executed
/// through a single interpreter instance are scoped to that repository.
///
/// # Example (mock for testing)
///
/// ```ignore
/// struct MockGitHubInterpreter {
///     responses: HashMap<GitHubEffect, GitHubResponse>,
/// }
///
/// impl GitHubInterpreter for MockGitHubInterpreter {
///     type Error = anyhow::Error;
///
///     async fn interpret(&self, effect: GitHubEffect) -> Result<GitHubResponse, Self::Error> {
///         self.responses.get(&effect)
///             .cloned()
///             .ok_or_else(|| anyhow!("unexpected effect: {:?}", effect))
///     }
/// }
/// ```
pub trait GitHubInterpreter {
    /// The error type returned by this interpreter.
    type Error;

    /// Execute a GitHub effect and return its response.
    fn interpret(
        &self,
        effect: GitHubEffect,
    ) -> impl Future<Output = Result<GitHubResponse, Self::Error>> + Send;
}

/// Interprets Git effects against a local repository.
///
/// Implementations are constructed with a worktree path, so all effects
/// executed through a single interpreter instance operate on that worktree.
///
/// # Example (mock for testing)
///
/// ```ignore
/// struct MockGitInterpreter {
///     ancestor_checks: HashMap<(Sha, Sha), bool>,
/// }
///
/// impl GitInterpreter for MockGitInterpreter {
///     type Error = anyhow::Error;
///
///     async fn interpret(&self, effect: GitEffect) -> Result<GitResponse, Self::Error> {
///         match effect {
///             GitEffect::IsAncestor { potential_ancestor, descendant } => {
///                 let result = self.ancestor_checks
///                     .get(&(potential_ancestor, descendant))
///                     .copied()
///                     .unwrap_or(false);
///                 Ok(GitResponse::Bool(result))
///             }
///             _ => Ok(GitResponse::Ok),
///         }
///     }
/// }
/// ```
pub trait GitInterpreter {
    /// The error type returned by this interpreter.
    type Error;

    /// Execute a Git effect and return its response.
    fn interpret(
        &self,
        effect: GitEffect,
    ) -> impl Future<Output = Result<GitResponse, Self::Error>> + Send;
}
