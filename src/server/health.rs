//! Health check endpoint for liveness probes.
//!
//! Returns 200 OK if the server is running. This is intended for use
//! with load balancers and orchestration systems (e.g., Kubernetes liveness probes).

use axum::http::StatusCode;

/// Health check handler.
///
/// Returns 200 OK with the text "OK". This simple endpoint is used
/// to verify that the server is running and accepting connections.
///
/// # Example
///
/// ```ignore
/// GET /health HTTP/1.1
///
/// HTTP/1.1 200 OK
/// Content-Type: text/plain
///
/// OK
/// ```
pub async fn health_handler() -> (StatusCode, &'static str) {
    (StatusCode::OK, "OK")
}
