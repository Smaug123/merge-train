//! State inspection endpoint for observability.
//!
//! Provides a read-only view of repository state for debugging and monitoring.

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use thiserror::Error;

use super::{AppState, InvalidPathComponent, validate_path_component};
use crate::persistence::generation::find_current_snapshot;
use crate::persistence::snapshot::{PersistedRepoSnapshot, SnapshotError, try_load_snapshot};

/// Errors that can occur when fetching state.
#[derive(Debug, Error)]
pub enum StateError {
    /// Repository state not found (no snapshot exists).
    #[error("repository state not found: {owner}/{repo}")]
    NotFound { owner: String, repo: String },

    /// IO error reading state.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Snapshot loading error.
    #[error("snapshot error: {0}")]
    Snapshot(#[from] SnapshotError),

    /// Invalid path component (e.g., path traversal attempt).
    #[error("{0}")]
    InvalidPath(#[from] InvalidPathComponent),
}

impl IntoResponse for StateError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            StateError::NotFound { .. } => (StatusCode::NOT_FOUND, self.to_string()),
            StateError::Io(_) => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
            StateError::Snapshot(_) => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
            StateError::InvalidPath(_) => (StatusCode::BAD_REQUEST, self.to_string()),
        };

        (status, message).into_response()
    }
}

/// State inspection handler.
///
/// Returns the current state of a repository as JSON. This is useful for
/// debugging and monitoring the merge train bot.
///
/// # Path Parameters
///
/// - `owner` - The repository owner
/// - `repo` - The repository name
///
/// # Response
///
/// - 200 OK with JSON body containing `PersistedRepoSnapshot`
/// - 404 Not Found if no state exists for the repository
/// - 500 Internal Server Error for IO or deserialization errors
///
/// # Example
///
/// ```ignore
/// GET /api/v1/repos/octocat/hello-world/state HTTP/1.1
///
/// HTTP/1.1 200 OK
/// Content-Type: application/json
///
/// {
///   "schema_version": 1,
///   "snapshot_at": "2024-01-15T12:00:00Z",
///   ...
/// }
/// ```
pub async fn state_handler(
    State(app_state): State<AppState>,
    Path((owner, repo)): Path<(String, String)>,
) -> Result<Json<PersistedRepoSnapshot>, StateError> {
    // Validate path components to prevent path traversal attacks.
    validate_path_component(&owner)?;
    validate_path_component(&repo)?;

    // Construct the path to the repository's state directory.
    // Structure: <state_dir>/<owner>/<repo>/
    let repo_state_dir = app_state.state_dir().join(&owner).join(&repo);

    // Find the current snapshot file using the same resolution rule as
    // recovery: the snapshot with the highest generation number.
    match find_current_snapshot(&repo_state_dir)? {
        Some((_generation, path)) => {
            let snapshot = try_load_snapshot(&path)?.ok_or(StateError::NotFound { owner, repo })?;
            Ok(Json(snapshot))
        }
        None => Err(StateError::NotFound { owner, repo }),
    }
}

// Resolution of the "current" snapshot is tested where it lives:
// `crate::persistence::generation::find_current_snapshot`.
