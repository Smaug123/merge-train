//! HTTP server for the merge train bot.
//!
//! This module implements the HTTP server that:
//! - Accepts webhooks from GitHub, validates signatures, and hands each to the
//!   repo's worker, which durably enqueues it before the handler replies
//! - Provides state inspection endpoints for observability
//! - Provides health checks for liveness probes
//!
//! # Endpoints
//!
//! - `POST /webhook` - Accepts GitHub webhook deliveries (returns 200 once the
//!   delivery is durably enqueued on the repo's worker)
//! - `GET /api/v1/repos/{owner}/{repo}/state` - Returns repository state as JSON
//! - `GET /health` - Returns 200 if server is running
//!
//! # Platform Support
//!
//! This module is Unix-only. The path traversal protection in [`validate_path_component`]
//! does not handle Windows-specific path semantics (e.g., drive letters like `C:`).

#[cfg(windows)]
compile_error!("This crate is Unix-only. Windows is not supported.");

use std::path::PathBuf;
use std::sync::Arc;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use thiserror::Error;

use crate::worker::WorkerRegistry;

pub mod health;
pub mod state;
pub mod webhook;

pub use health::health_handler;
pub use state::state_handler;
pub use webhook::webhook_handler;

/// Error returned when a path component is invalid (e.g., contains path traversal sequences).
#[derive(Debug, Error, PartialEq, Eq)]
#[error("invalid path component: {reason}")]
pub struct InvalidPathComponent {
    reason: &'static str,
}

impl IntoResponse for InvalidPathComponent {
    fn into_response(self) -> Response {
        (StatusCode::BAD_REQUEST, self.to_string()).into_response()
    }
}

/// Validates that a string is safe to use as a path component.
///
/// Rejects:
/// - Empty strings
/// - `.` and `..` (directory traversal)
/// - Strings containing `/`, `\`, or null bytes
///
/// This prevents path traversal attacks when building filesystem paths from
/// untrusted input (e.g., URL segments or webhook payload data).
pub fn validate_path_component(s: &str) -> Result<(), InvalidPathComponent> {
    if s.is_empty() {
        return Err(InvalidPathComponent {
            reason: "empty string",
        });
    }

    if s == "." || s == ".." {
        return Err(InvalidPathComponent {
            reason: "directory traversal",
        });
    }

    if s.contains('/') || s.contains('\\') || s.contains('\0') {
        return Err(InvalidPathComponent {
            reason: "contains path separator or null byte",
        });
    }

    Ok(())
}

/// Shared application state.
///
/// Passed to every handler via Axum's `State` extractor. It owns the
/// [`WorkerRegistry`] (the per-repo workers the webhook handler routes
/// deliveries to) and the webhook signing secret. The async server never
/// touches a `Store` directly — only the workers do (plan P1-H).
#[derive(Clone)]
pub struct AppState {
    inner: Arc<AppStateInner>,
}

struct AppStateInner {
    /// The per-repo worker registry; owns the repos' DBs (under `state_dir`).
    workers: WorkerRegistry,

    /// Webhook secret for HMAC-SHA256 signature verification.
    webhook_secret: Vec<u8>,
}

impl AppState {
    /// Creates a new `AppState`.
    ///
    /// # Arguments
    ///
    /// * `state_dir` - Root for per-repo state DBs (`<state_dir>/<owner>/<repo>/state.db`)
    /// * `webhook_secret` - Secret for verifying webhook signatures
    pub fn new(state_dir: impl Into<PathBuf>, webhook_secret: impl Into<Vec<u8>>) -> Self {
        AppState {
            inner: Arc::new(AppStateInner {
                workers: WorkerRegistry::new(state_dir),
                webhook_secret: webhook_secret.into(),
            }),
        }
    }

    /// Returns the per-repo worker registry.
    pub fn workers(&self) -> &WorkerRegistry {
        &self.inner.workers
    }

    /// Returns the state directory path.
    pub fn state_dir(&self) -> &PathBuf {
        self.inner.workers.state_dir()
    }

    /// Returns the webhook secret.
    pub fn webhook_secret(&self) -> &[u8] {
        &self.inner.webhook_secret
    }
}

/// Builds the axum Router with all endpoints.
///
/// The webhook route carries an explicit body limit of
/// [`webhook::WEBHOOK_BODY_LIMIT_BYTES`] (GitHub's 25MB payload cap): axum's
/// 2MB default would silently 413 legitimate large deliveries.
pub fn build_router(app_state: AppState) -> axum::Router {
    use axum::extract::DefaultBodyLimit;
    use axum::routing::{get, post};

    axum::Router::new()
        .route(
            "/webhook",
            post(webhook_handler).layer(DefaultBodyLimit::max(webhook::WEBHOOK_BODY_LIMIT_BYTES)),
        )
        .route("/api/v1/repos/{owner}/{repo}/state", get(state_handler))
        .route("/health", get(health_handler))
        .with_state(app_state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_path_component_accepts_valid() {
        assert!(validate_path_component("octocat").is_ok());
        assert!(validate_path_component("hello-world").is_ok());
        assert!(validate_path_component("my_repo").is_ok());
        assert!(validate_path_component("repo.name").is_ok());
        assert!(validate_path_component("123").is_ok());
    }

    #[test]
    fn validate_path_component_rejects_empty() {
        assert_eq!(
            validate_path_component(""),
            Err(InvalidPathComponent {
                reason: "empty string"
            })
        );
    }

    #[test]
    fn validate_path_component_rejects_dot() {
        assert_eq!(
            validate_path_component("."),
            Err(InvalidPathComponent {
                reason: "directory traversal"
            })
        );
    }

    #[test]
    fn validate_path_component_rejects_dotdot() {
        assert_eq!(
            validate_path_component(".."),
            Err(InvalidPathComponent {
                reason: "directory traversal"
            })
        );
    }

    #[test]
    fn validate_path_component_rejects_forward_slash() {
        assert_eq!(
            validate_path_component("foo/bar"),
            Err(InvalidPathComponent {
                reason: "contains path separator or null byte"
            })
        );
        assert_eq!(
            validate_path_component("../etc/passwd"),
            Err(InvalidPathComponent {
                reason: "contains path separator or null byte"
            })
        );
    }

    #[test]
    fn validate_path_component_rejects_backslash() {
        assert_eq!(
            validate_path_component("foo\\bar"),
            Err(InvalidPathComponent {
                reason: "contains path separator or null byte"
            })
        );
    }

    #[test]
    fn validate_path_component_rejects_null_byte() {
        assert_eq!(
            validate_path_component("foo\0bar"),
            Err(InvalidPathComponent {
                reason: "contains path separator or null byte"
            })
        );
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

    use crate::persistence::snapshot::PersistedRepoSnapshot;
    use crate::webhooks::{compute_signature, format_signature_header};

    /// Creates a test app state rooted at a temporary state directory.
    fn test_app_state(secret: &[u8]) -> (AppState, tempfile::TempDir) {
        let state_dir = tempdir().unwrap();
        let state = AppState::new(state_dir.path(), secret.to_vec());
        (state, state_dir)
    }

    /// Creates a valid webhook request with proper signature from raw body bytes.
    fn create_webhook_request_raw(
        secret: &[u8],
        event_type: &str,
        delivery_id: &str,
        body_bytes: Vec<u8>,
    ) -> Request<Body> {
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

    /// Creates a valid webhook request with proper signature.
    fn create_webhook_request(
        secret: &[u8],
        event_type: &str,
        delivery_id: &str,
        body: &serde_json::Value,
    ) -> Request<Body> {
        let body_bytes = serde_json::to_vec(body).unwrap();
        create_webhook_request_raw(secret, event_type, delivery_id, body_bytes)
    }

    // ─── Health endpoint tests ───

    #[tokio::test]
    async fn health_returns_200() {
        let (state, _state_dir) = test_app_state(b"secret");
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
    async fn webhook_valid_returns_200() {
        let secret = b"test-secret";
        let (state, _state_dir) = test_app_state(secret);
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

        // 200 means the worker durably enqueued the delivery (it acked before
        // the handler replied). The durable-INSERT itself is covered by the
        // worker/Store tests; here we can't open the Store (the worker holds
        // its lock), so we assert the contract at the HTTP boundary.
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn webhook_invalid_signature_returns_401() {
        let (state, state_dir) = test_app_state(b"correct-secret");
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

        // Rejection happens before routing, so no worker is spawned and no DB
        // is created for the repo.
        let repo_db = state_dir.path().join("octocat").join("hello-world");
        assert!(
            !repo_db.exists(),
            "no per-repo state should be created for an unauthenticated request"
        );
    }

    #[tokio::test]
    async fn webhook_missing_event_header_returns_400() {
        let secret = b"test-secret";
        let (state, _state_dir) = test_app_state(secret);
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
        let (state, _state_dir) = test_app_state(secret);
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
    async fn webhook_duplicate_delivery_returns_200() {
        let secret = b"test-secret";
        let (state, _state_dir) = test_app_state(secret);
        // Both requests share one AppState (hence one worker), so the second
        // hits the same Store and is recognised as a redelivery.
        let app = build_router(state.clone());

        let body = serde_json::json!({
            "action": "opened",
            "repository": {
                "name": "hello-world",
                "owner": { "login": "octocat" }
            }
        });

        let delivery_id = "550e8400-e29b-41d4-a716-446655440004";

        let request = create_webhook_request(secret, "pull_request", delivery_id, &body);
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Same delivery id again: idempotent, still 200.
        let app2 = build_router(state);
        let request2 = create_webhook_request(secret, "pull_request", delivery_id, &body);
        let response2 = app2.oneshot(request2).await.unwrap();
        assert_eq!(response2.status(), StatusCode::OK);
    }

    /// GitHub sends webhook payloads up to 25MB. The default axum body limit
    /// (2MB) would reject these with 413, and GitHub does not retry 413s
    /// indefinitely - the delivery would be lost forever.
    #[tokio::test]
    async fn webhook_large_body_within_25mb_accepted() {
        let secret = b"test-secret";
        let (state, _state_dir) = test_app_state(secret);
        let app = build_router(state);

        // 3MB payload: above the 2MB axum default, below the 25MB GitHub max.
        let padding = "x".repeat(3 * 1024 * 1024);
        let body = serde_json::json!({
            "action": "opened",
            "padding": padding,
            "repository": {
                "name": "hello-world",
                "owner": { "login": "octocat" }
            }
        });

        let request = create_webhook_request(
            secret,
            "pull_request",
            "550e8400-e29b-41d4-a716-446655440020",
            &body,
        );

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    /// Bodies over the documented 25MB GitHub maximum are rejected.
    #[tokio::test]
    async fn webhook_body_over_25mb_rejected() {
        let secret = b"test-secret";
        let (state, _state_dir) = test_app_state(secret);
        let app = build_router(state);

        let padding = "x".repeat(26 * 1024 * 1024);
        let body = serde_json::json!({
            "padding": padding,
            "repository": {
                "name": "hello-world",
                "owner": { "login": "octocat" }
            }
        });

        let request = create_webhook_request(
            secret,
            "pull_request",
            "550e8400-e29b-41d4-a716-446655440021",
            &body,
        );

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    /// An empty X-GitHub-Event header is malformed client input: 400, not 500.
    /// (500 invites pointless retries of a permanently-bad request.)
    #[tokio::test]
    async fn webhook_empty_event_header_returns_400() {
        let secret = b"test-secret";
        let (state, _state_dir) = test_app_state(secret);
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

        let request = Request::builder()
            .method("POST")
            .uri("/webhook")
            .header("content-type", "application/json")
            .header("x-github-event", "")
            .header("x-github-delivery", "550e8400-e29b-41d4-a716-446655440022")
            .header("x-hub-signature-256", signature_header)
            .body(Body::from(body_bytes))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    /// An invalid delivery id (e.g. leading dot) is malformed client input:
    /// 400, not 500.
    #[tokio::test]
    async fn webhook_invalid_delivery_id_returns_400() {
        let secret = b"test-secret";
        let (state, _state_dir) = test_app_state(secret);
        let app = build_router(state);

        let body = serde_json::json!({
            "action": "opened",
            "repository": {
                "name": "hello-world",
                "owner": { "login": "octocat" }
            }
        });

        // Leading dot is rejected by `DeliveryId::parse`.
        let request = create_webhook_request(secret, "pull_request", ".hidden-delivery", &body);

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    /// A missing X-Hub-Signature-256 header is an authentication failure (401),
    /// just like an invalid signature - not a 400.
    #[tokio::test]
    async fn webhook_missing_signature_returns_401() {
        let secret = b"test-secret";
        let (state, _state_dir) = test_app_state(secret);
        let app = build_router(state);

        let body = serde_json::json!({
            "repository": {
                "name": "hello-world",
                "owner": { "login": "octocat" }
            }
        });
        let body_bytes = serde_json::to_vec(&body).unwrap();

        let request = Request::builder()
            .method("POST")
            .uri("/webhook")
            .header("content-type", "application/json")
            .header("x-github-event", "pull_request")
            .header("x-github-delivery", "550e8400-e29b-41d4-a716-446655440023")
            .body(Body::from(body_bytes))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    // (Raw-body byte fidelity — that the exact signed bytes survive for the
    // worker's `parse_webhook` — is now a Store/worker concern, covered by
    // `worker::tests::enqueued_body_is_stored_verbatim`. The signature is
    // verified once at intake and never re-verified at drain.)

    // ─── State endpoint tests ───

    #[tokio::test]
    async fn state_returns_json_for_existing_repo() {
        use crate::persistence::event::StateEventPayload;
        use crate::store::Store;
        use crate::types::PrNumber;

        let (state, state_dir) = test_app_state(b"secret");
        let app = build_router(state);

        // Materialize state by appending an event to the repo's Store (this also
        // creates `<state_dir>/octocat/hello-world/state.db`), then release it.
        let db_path = state_dir
            .path()
            .join("octocat")
            .join("hello-world")
            .join("state.db");
        {
            let mut store = Store::open(&db_path).unwrap();
            store
                .append(
                    StateEventPayload::TrainStarted {
                        root_pr: PrNumber(42),
                        current_pr: PrNumber(42),
                    },
                    chrono::Utc::now(),
                )
                .unwrap();
        }

        let request = Request::builder()
            .uri("/api/v1/repos/octocat/hello-world/state")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let parsed: PersistedRepoSnapshot = serde_json::from_slice(&body).unwrap();
        assert!(parsed.active_trains.contains_key(&PrNumber(42)));
    }

    #[tokio::test]
    async fn state_returns_404_for_nonexistent_repo() {
        let (state, _state_dir) = test_app_state(b"secret");
        let app = build_router(state);

        let request = Request::builder()
            .uri("/api/v1/repos/nonexistent/repo/state")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // ─── Path traversal protection tests ───

    #[tokio::test]
    async fn state_rejects_path_traversal_in_owner() {
        let (state, _state_dir) = test_app_state(b"secret");
        let app = build_router(state);

        // Attempt path traversal in owner segment
        let request = Request::builder()
            .uri("/api/v1/repos/../hello-world/state")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn state_rejects_path_traversal_in_repo() {
        let (state, _state_dir) = test_app_state(b"secret");
        let app = build_router(state);

        // Attempt path traversal in repo segment
        let request = Request::builder()
            .uri("/api/v1/repos/octocat/../state")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn webhook_rejects_path_traversal_in_owner() {
        let secret = b"test-secret";
        let (state, _state_dir) = test_app_state(secret);
        let app = build_router(state);

        // Payload with path traversal in owner
        let body = serde_json::json!({
            "action": "opened",
            "repository": {
                "name": "hello-world",
                "owner": {
                    "login": "../etc"
                }
            }
        });

        let request = create_webhook_request(
            secret,
            "pull_request",
            "550e8400-e29b-41d4-a716-446655440010",
            &body,
        );

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn webhook_rejects_path_traversal_in_repo() {
        let secret = b"test-secret";
        let (state, _state_dir) = test_app_state(secret);
        let app = build_router(state);

        // Payload with path traversal in repo name
        let body = serde_json::json!({
            "action": "opened",
            "repository": {
                "name": "../../secrets",
                "owner": {
                    "login": "octocat"
                }
            }
        });

        let request = create_webhook_request(
            secret,
            "pull_request",
            "550e8400-e29b-41d4-a716-446655440011",
            &body,
        );

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn webhook_rejects_path_traversal_in_delivery_id() {
        let secret = b"test-secret";
        let (state, _state_dir) = test_app_state(secret);
        let app = build_router(state);

        // Valid owner/repo but path traversal in delivery ID header
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
            "../../../etc/passwd", // Path traversal in delivery ID
            &body,
        );

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
