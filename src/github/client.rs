//! Octocrab client wrapper scoped to a specific repository.
//!
//! This module provides `OctocrabClient`, which wraps an `Octocrab` instance
//! and scopes all operations to a specific repository. This matches the design
//! where effects are repo-scoped (the `GitHubEffect` enum doesn't include repo info).

use octocrab::Octocrab;

use crate::types::RepoId;

/// A GitHub API client scoped to a specific repository.
///
/// All operations performed through this client target the same repository,
/// matching the design where `GitHubEffect` variants don't include repo info.
#[derive(Clone)]
pub struct OctocrabClient {
    /// The underlying octocrab client.
    client: Octocrab,

    /// The repository this client is scoped to.
    repo: RepoId,
}

impl OctocrabClient {
    /// Creates a new client scoped to the given repository.
    ///
    /// The `client` must have octocrab's HTTP-layer retry disabled
    /// (`add_retry_config(RetryConfig::None)`): it re-sends requests on 5xx
    /// and transport errors, which duplicates non-idempotent operations and
    /// bypasses the per-effect retry policy in the interpreter.
    pub fn new(client: Octocrab, repo: RepoId) -> Self {
        Self { client, repo }
    }

    /// Creates a client from a GitHub token.
    ///
    /// This is a convenience method for creating a client with token authentication.
    pub fn from_token(token: impl Into<String>, repo: RepoId) -> Result<Self, octocrab::Error> {
        let client = Octocrab::builder()
            .personal_token(token.into())
            .add_retry_config(octocrab::service::middleware::retry::RetryConfig::None)
            .build()?;
        Ok(Self::new(client, repo))
    }

    /// Returns a reference to the underlying octocrab client.
    pub fn inner(&self) -> &Octocrab {
        &self.client
    }

    /// Returns the repository this client is scoped to.
    pub fn repo(&self) -> &RepoId {
        &self.repo
    }

    /// Returns the repository owner.
    pub fn owner(&self) -> &str {
        &self.repo.owner
    }

    /// Returns the repository name.
    pub fn repo_name(&self) -> &str {
        &self.repo.repo
    }
}

impl std::fmt::Debug for OctocrabClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OctocrabClient")
            .field("repo", &self.repo)
            .finish_non_exhaustive()
    }
}
