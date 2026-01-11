//! HTTP server for the merge train bot.
//!
//! This module implements the HTTP server that:
//! - Accepts webhooks from GitHub, validates signatures, and spools them durably
//! - Provides state inspection endpoints for observability
//! - Provides health checks for liveness probes
//!
//! # Endpoints
//!
//! - `POST /webhook` - Accepts GitHub webhook deliveries (returns 202 Accepted)
//! - `GET /api/v1/repos/{owner}/{repo}/state` - Returns repository state as JSON
//! - `GET /health` - Returns 200 if server is running

use std::path::PathBuf;
use std::sync::Arc;

pub mod health;
pub mod state;
pub mod webhook;

pub use health::health_handler;
pub use state::state_handler;
pub use webhook::webhook_handler;

/// Shared application state.
///
/// This is passed to all handlers via Axum's `State` extractor.
/// It contains the configuration needed for webhook processing
/// and state inspection.
#[derive(Clone)]
pub struct AppState {
    inner: Arc<AppStateInner>,
}

struct AppStateInner {
    /// Directory where webhook deliveries are spooled.
    spool_dir: PathBuf,

    /// Directory where repository state is persisted.
    /// Structure: `<state_dir>/<owner>/<repo>/snapshot.<gen>.json`
    state_dir: PathBuf,

    /// Webhook secret for HMAC-SHA256 signature verification.
    webhook_secret: Vec<u8>,
}

impl AppState {
    /// Creates a new `AppState` with the given configuration.
    ///
    /// # Arguments
    ///
    /// * `spool_dir` - Directory for spooling webhook deliveries
    /// * `state_dir` - Directory for persisted repository state
    /// * `webhook_secret` - Secret for verifying webhook signatures
    pub fn new(
        spool_dir: impl Into<PathBuf>,
        state_dir: impl Into<PathBuf>,
        webhook_secret: impl Into<Vec<u8>>,
    ) -> Self {
        AppState {
            inner: Arc::new(AppStateInner {
                spool_dir: spool_dir.into(),
                state_dir: state_dir.into(),
                webhook_secret: webhook_secret.into(),
            }),
        }
    }

    /// Returns the spool directory path.
    pub fn spool_dir(&self) -> &PathBuf {
        &self.inner.spool_dir
    }

    /// Returns the state directory path.
    pub fn state_dir(&self) -> &PathBuf {
        &self.inner.state_dir
    }

    /// Returns the webhook secret.
    pub fn webhook_secret(&self) -> &[u8] {
        &self.inner.webhook_secret
    }
}

/// Builds the axum Router with all endpoints.
pub fn build_router(app_state: AppState) -> axum::Router {
    use axum::routing::{get, post};

    axum::Router::new()
        .route("/webhook", post(webhook_handler))
        .route("/api/v1/repos/{owner}/{repo}/state", get(state_handler))
        .route("/health", get(health_handler))
        .with_state(app_state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn app_state_accessors_work() {
        let spool_dir = tempdir().unwrap();
        let state_dir = tempdir().unwrap();
        let secret = b"test-secret";

        let state = AppState::new(spool_dir.path(), state_dir.path(), secret.to_vec());

        assert_eq!(state.spool_dir(), spool_dir.path());
        assert_eq!(state.state_dir(), state_dir.path());
        assert_eq!(state.webhook_secret(), secret);
    }

    #[test]
    fn app_state_is_clone() {
        let spool_dir = tempdir().unwrap();
        let state_dir = tempdir().unwrap();

        let state = AppState::new(spool_dir.path(), state_dir.path(), b"secret".to_vec());
        let cloned = state.clone();

        assert_eq!(state.spool_dir(), cloned.spool_dir());
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tempfile::tempdir;
    use tower::ServiceExt;

    use crate::persistence::snapshot::{PersistedRepoSnapshot, save_snapshot_atomic};
    use crate::webhooks::{compute_signature, format_signature_header};

    /// Creates a test app state with temporary directories.
    fn test_app_state(secret: &[u8]) -> (AppState, tempfile::TempDir, tempfile::TempDir) {
        let spool_dir = tempdir().unwrap();
        let state_dir = tempdir().unwrap();
        let state = AppState::new(spool_dir.path(), state_dir.path(), secret.to_vec());
        (state, spool_dir, state_dir)
    }

    /// Creates a valid webhook request with proper signature.
    fn create_webhook_request(
        secret: &[u8],
        event_type: &str,
        delivery_id: &str,
        body: &serde_json::Value,
    ) -> Request<Body> {
        let body_bytes = serde_json::to_vec(body).unwrap();
        let signature = compute_signature(&body_bytes, secret);
        let signature_header = format_signature_header(&signature);

        Request::builder()
            .method("POST")
            .uri("/webhook")
            .header("content-type", "application/json")
            .header("x-github-event", event_type)
            .header("x-github-delivery", delivery_id)
            .header("x-hub-signature-256", signature_header)
            .body(Body::from(body_bytes))
            .unwrap()
    }

    // ─── Health endpoint tests ───

    #[tokio::test]
    async fn health_returns_200() {
        let (state, _spool, _state_dir) = test_app_state(b"secret");
        let app = build_router(state);

        let request = Request::builder()
            .uri("/health")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"OK");
    }

    // ─── Webhook endpoint tests ───

    #[tokio::test]
    async fn webhook_valid_returns_202() {
        let secret = b"test-secret";
        let (state, spool_dir, _state_dir) = test_app_state(secret);
        let app = build_router(state);

        let body = serde_json::json!({
            "action": "opened",
            "repository": {
                "name": "hello-world",
                "owner": {
                    "login": "octocat"
                }
            }
        });

        let request = create_webhook_request(
            secret,
            "pull_request",
            "550e8400-e29b-41d4-a716-446655440000",
            &body,
        );

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);

        // Verify file appears in spool directory
        let repo_spool = spool_dir.path().join("octocat").join("hello-world");
        let payload_file = repo_spool.join("550e8400-e29b-41d4-a716-446655440000.json");
        assert!(payload_file.exists(), "Webhook should be spooled to disk");
    }

    #[tokio::test]
    async fn webhook_invalid_signature_returns_401() {
        let (state, spool_dir, _state_dir) = test_app_state(b"correct-secret");
        let app = build_router(state);

        // Sign with wrong secret
        let body = serde_json::json!({
            "action": "opened",
            "repository": {
                "name": "hello-world",
                "owner": {
                    "login": "octocat"
                }
            }
        });

        let request = create_webhook_request(
            b"wrong-secret",
            "pull_request",
            "550e8400-e29b-41d4-a716-446655440001",
            &body,
        );

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        // Verify no file in spool directory
        let repo_spool = spool_dir.path().join("octocat").join("hello-world");
        assert!(
            !repo_spool.exists(),
            "No spool directory should be created for invalid signature"
        );
    }

    #[tokio::test]
    async fn webhook_missing_event_header_returns_400() {
        let secret = b"test-secret";
        let (state, _spool, _state_dir) = test_app_state(secret);
        let app = build_router(state);

        let body = serde_json::json!({
            "repository": {
                "name": "hello-world",
                "owner": { "login": "octocat" }
            }
        });
        let body_bytes = serde_json::to_vec(&body).unwrap();
        let signature = compute_signature(&body_bytes, secret);
        let signature_header = format_signature_header(&signature);

        // Missing x-github-event header
        let request = Request::builder()
            .method("POST")
            .uri("/webhook")
            .header("content-type", "application/json")
            .header("x-github-delivery", "550e8400-e29b-41d4-a716-446655440002")
            .header("x-hub-signature-256", signature_header)
            .body(Body::from(body_bytes))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn webhook_missing_repository_returns_400() {
        let secret = b"test-secret";
        let (state, _spool, _state_dir) = test_app_state(secret);
        let app = build_router(state);

        // Body without repository field
        let body = serde_json::json!({
            "action": "opened"
        });

        let request = create_webhook_request(
            secret,
            "pull_request",
            "550e8400-e29b-41d4-a716-446655440003",
            &body,
        );

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn webhook_duplicate_delivery_returns_202() {
        let secret = b"test-secret";
        let (state, _spool, _state_dir) = test_app_state(secret);
        let app = build_router(state.clone());

        let body = serde_json::json!({
            "action": "opened",
            "repository": {
                "name": "hello-world",
                "owner": { "login": "octocat" }
            }
        });

        let delivery_id = "550e8400-e29b-41d4-a716-446655440004";

        // First request
        let request = create_webhook_request(secret, "pull_request", delivery_id, &body);
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        // Second request with same delivery ID (duplicate)
        let app2 = build_router(state);
        let request2 = create_webhook_request(secret, "pull_request", delivery_id, &body);
        let response2 = app2.oneshot(request2).await.unwrap();

        // Should still return 202 (idempotent)
        assert_eq!(response2.status(), StatusCode::ACCEPTED);
    }

    // ─── State endpoint tests ───

    #[tokio::test]
    async fn state_returns_json_for_existing_repo() {
        let (state, _spool, state_dir) = test_app_state(b"secret");
        let app = build_router(state);

        // Create a snapshot for the test repository
        let repo_state_dir = state_dir.path().join("octocat").join("hello-world");
        std::fs::create_dir_all(&repo_state_dir).unwrap();

        let snapshot = PersistedRepoSnapshot::new("main");
        let snapshot_path = repo_state_dir.join("snapshot.0.json");
        save_snapshot_atomic(&snapshot_path, &snapshot).unwrap();

        let request = Request::builder()
            .uri("/api/v1/repos/octocat/hello-world/state")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let parsed: PersistedRepoSnapshot = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed.default_branch, "main");
    }

    #[tokio::test]
    async fn state_returns_404_for_nonexistent_repo() {
        let (state, _spool, _state_dir) = test_app_state(b"secret");
        let app = build_router(state);

        let request = Request::builder()
            .uri("/api/v1/repos/nonexistent/repo/state")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn state_returns_latest_generation() {
        let (state, _spool, state_dir) = test_app_state(b"secret");
        let app = build_router(state);

        // Create snapshots for multiple generations
        let repo_state_dir = state_dir.path().join("octocat").join("hello-world");
        std::fs::create_dir_all(&repo_state_dir).unwrap();

        // Create generation 0 with "develop" branch
        let mut snapshot0 = PersistedRepoSnapshot::new("develop");
        snapshot0.log_generation = 0;
        save_snapshot_atomic(&repo_state_dir.join("snapshot.0.json"), &snapshot0).unwrap();

        // Create generation 1 with "main" branch (this should be returned)
        let mut snapshot1 = PersistedRepoSnapshot::new("main");
        snapshot1.log_generation = 1;
        save_snapshot_atomic(&repo_state_dir.join("snapshot.1.json"), &snapshot1).unwrap();

        let request = Request::builder()
            .uri("/api/v1/repos/octocat/hello-world/state")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let parsed: PersistedRepoSnapshot = serde_json::from_slice(&body).unwrap();
        // Should return the latest generation (1) with "main" branch
        assert_eq!(parsed.default_branch, "main");
        assert_eq!(parsed.log_generation, 1);
    }
}
