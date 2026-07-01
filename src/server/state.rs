//! State inspection endpoint for observability.
//!
//! Returns a repository's materialized state as JSON, read from its SQLite
//! `Store` via a `query_only` connection ([`Store::read_snapshot`]). WAL lets
//! this read run concurrently with the owning worker's writer without blocking
//! it; the server never opens the worker's read-write `Store` (plan P1-H).

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use thiserror::Error;

use super::{AppState, InvalidPathComponent, validate_path_component};
use crate::persistence::snapshot::PersistedRepoSnapshot;
use crate::store::{Store, StoreError};

/// Errors that can occur when fetching state.
#[derive(Debug, Error)]
pub enum StateError {
    /// No state DB exists for the repository.
    #[error("repository state not found: {owner}/{repo}")]
    NotFound { owner: String, repo: String },

    /// Reading the repo's `Store` failed.
    #[error("store read error: {0}")]
    Store(#[from] StoreError),

    /// Invalid path component (e.g., path traversal attempt).
    #[error("{0}")]
    InvalidPath(#[from] InvalidPathComponent),
}

impl IntoResponse for StateError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            StateError::NotFound { .. } => (StatusCode::NOT_FOUND, self.to_string()),
            StateError::Store(_) => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
            StateError::InvalidPath(_) => (StatusCode::BAD_REQUEST, self.to_string()),
        };

        (status, message).into_response()
    }
}

/// State inspection handler.
///
/// Returns the current materialized state of a repository as JSON.
///
/// # Path Parameters
///
/// - `owner` - The repository owner
/// - `repo` - The repository name
///
/// # Response
///
/// - 200 OK with the repository's [`PersistedRepoSnapshot`] as JSON
/// - 404 Not Found if no state DB exists for the repository
/// - 400 Bad Request for an invalid owner/repo path component
/// - 500 Internal Server Error for a store read failure
pub async fn state_handler(
    State(app_state): State<AppState>,
    Path((owner, repo)): Path<(String, String)>,
) -> Result<Json<PersistedRepoSnapshot>, StateError> {
    // Validate path components to prevent path traversal attacks.
    validate_path_component(&owner)?;
    validate_path_component(&repo)?;

    let db_path = app_state
        .state_dir()
        .join(&owner)
        .join(&repo)
        .join("state.db");

    // A single indexed-row read over a `query_only` connection (fast enough to
    // run inline; the heavy single-writer work stays on the worker thread).
    match Store::read_snapshot(&db_path)? {
        Some(snapshot) => Ok(Json(snapshot)),
        None => Err(StateError::NotFound { owner, repo }),
    }
}
