//! State inspection endpoint for observability.
//!
//! Provides a read-only view of repository state for debugging and monitoring.

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use std::fs;
use thiserror::Error;

use super::{AppState, InvalidPathComponent, validate_path_component};
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

    // Find the current snapshot file.
    // Snapshots are named snapshot.<gen>.json where <gen> is the generation number.
    // We need to find the highest generation number.
    let snapshot_path = find_current_snapshot(&repo_state_dir)?;

    match snapshot_path {
        Some(path) => {
            let snapshot = try_load_snapshot(&path)?.ok_or(StateError::NotFound { owner, repo })?;
            Ok(Json(snapshot))
        }
        None => Err(StateError::NotFound { owner, repo }),
    }
}

/// Finds the current snapshot file in a repository's state directory.
///
/// Snapshots are named `snapshot.<gen>.json` where `<gen>` is the generation number.
/// Returns the path to the snapshot with the highest generation number.
fn find_current_snapshot(
    repo_state_dir: &std::path::Path,
) -> Result<Option<std::path::PathBuf>, StateError> {
    let read_dir = match fs::read_dir(repo_state_dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };

    let mut highest_gen: Option<(u64, std::path::PathBuf)> = None;

    for entry in read_dir {
        let entry = entry?;
        let path = entry.path();

        if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
            // Parse filenames like "snapshot.0.json", "snapshot.1.json", etc.
            if let Some(generation) = parse_snapshot_filename(filename) {
                match &highest_gen {
                    Some((current_gen, _)) if generation > *current_gen => {
                        highest_gen = Some((generation, path));
                    }
                    None => {
                        highest_gen = Some((generation, path));
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(highest_gen.map(|(_, path)| path))
}

/// Parses a snapshot filename and extracts the generation number.
///
/// Returns `Some(gen)` for filenames like "snapshot.0.json", "snapshot.42.json".
/// Returns `None` for non-matching filenames.
fn parse_snapshot_filename(filename: &str) -> Option<u64> {
    let stripped = filename.strip_prefix("snapshot.")?;
    let stripped = stripped.strip_suffix(".json")?;
    stripped.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::snapshot::{PersistedRepoSnapshot, save_snapshot_atomic};
    use tempfile::tempdir;

    #[test]
    fn parse_snapshot_filename_valid() {
        assert_eq!(parse_snapshot_filename("snapshot.0.json"), Some(0));
        assert_eq!(parse_snapshot_filename("snapshot.1.json"), Some(1));
        assert_eq!(parse_snapshot_filename("snapshot.42.json"), Some(42));
        assert_eq!(parse_snapshot_filename("snapshot.999.json"), Some(999));
    }

    #[test]
    fn parse_snapshot_filename_invalid() {
        assert_eq!(parse_snapshot_filename("snapshot.json"), None);
        assert_eq!(parse_snapshot_filename("snapshot.abc.json"), None);
        assert_eq!(parse_snapshot_filename("other.0.json"), None);
        assert_eq!(parse_snapshot_filename("snapshot.0.txt"), None);
        assert_eq!(parse_snapshot_filename("snapshot.0.json.tmp"), None);
    }

    #[test]
    fn find_current_snapshot_empty_dir() {
        let dir = tempdir().unwrap();
        let result = find_current_snapshot(dir.path()).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn find_current_snapshot_nonexistent_dir() {
        let dir = tempdir().unwrap();
        let nonexistent = dir.path().join("does-not-exist");
        let result = find_current_snapshot(&nonexistent).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn find_current_snapshot_single_file() {
        let dir = tempdir().unwrap();
        let snapshot = PersistedRepoSnapshot::new("main");
        let path = dir.path().join("snapshot.0.json");
        save_snapshot_atomic(&path, &snapshot).unwrap();

        let result = find_current_snapshot(dir.path()).unwrap();
        assert_eq!(result, Some(path));
    }

    #[test]
    fn find_current_snapshot_multiple_generations() {
        let dir = tempdir().unwrap();
        let snapshot = PersistedRepoSnapshot::new("main");

        // Create snapshots for generations 0, 1, 2
        for generation in 0..=2 {
            let path = dir.path().join(format!("snapshot.{}.json", generation));
            save_snapshot_atomic(&path, &snapshot).unwrap();
        }

        let result = find_current_snapshot(dir.path()).unwrap();
        assert_eq!(result, Some(dir.path().join("snapshot.2.json")));
    }

    #[test]
    fn find_current_snapshot_ignores_temp_files() {
        let dir = tempdir().unwrap();
        let snapshot = PersistedRepoSnapshot::new("main");

        // Create a valid snapshot
        let path = dir.path().join("snapshot.0.json");
        save_snapshot_atomic(&path, &snapshot).unwrap();

        // Create a temp file that should be ignored
        fs::write(dir.path().join("snapshot.1.json.tmp"), "{}").unwrap();

        let result = find_current_snapshot(dir.path()).unwrap();
        assert_eq!(result, Some(path));
    }
}
