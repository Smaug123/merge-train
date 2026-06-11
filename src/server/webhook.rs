//! Webhook endpoint handler.
//!
//! Accepts GitHub webhook deliveries, verifies signatures, and spools them
//! durably before returning 202 Accepted. The envelope stores the raw signed
//! body; intake parses only the routing fields (repository owner/name) needed
//! to pick the per-repo spool directory. Full payload parsing happens at
//! drain time (`crate::webhooks::parse_webhook`).

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use thiserror::Error;
use tracing::{info, warn};

use super::{AppState, InvalidPathComponent, validate_path_component};
use crate::spool::delivery::{
    ArrivalMarker, EmptyEventType, SpoolError, WebhookEnvelope, spool_webhook,
};
use crate::types::DeliveryId;
use crate::types::ids::InvalidDeliveryId;
use crate::webhooks::verify_signature;

/// Header name for GitHub event type.
const HEADER_EVENT: &str = "x-github-event";
/// Header name for GitHub delivery ID.
const HEADER_DELIVERY: &str = "x-github-delivery";
/// Header name for GitHub signature.
const HEADER_SIGNATURE: &str = "x-hub-signature-256";

/// Maximum accepted webhook body size: GitHub caps payloads at 25MB.
///
/// Axum's 2MB default would 413 legitimate large payloads, and GitHub does
/// not retry 413s, so those deliveries would be lost.
pub const WEBHOOK_BODY_LIMIT_BYTES: usize = 25 * 1024 * 1024;

/// Errors that can occur when processing a webhook.
#[derive(Debug, Error)]
pub enum WebhookError {
    /// Missing required header.
    #[error("missing required header: {0}")]
    MissingHeader(&'static str),

    /// The signature header is absent: an authentication failure, the same
    /// class as [`WebhookError::InvalidSignature`], not a malformed request.
    #[error("missing {HEADER_SIGNATURE} header")]
    MissingSignature,

    /// Invalid signature.
    #[error("invalid signature")]
    InvalidSignature,

    /// The delivery ID is not usable as a spool filename.
    #[error(transparent)]
    InvalidDeliveryId(#[from] InvalidDeliveryId),

    /// The body is not valid UTF-8 (the envelope stores it as a string, and
    /// GitHub webhook bodies are JSON and therefore UTF-8).
    #[error("body is not valid UTF-8")]
    BodyNotUtf8,

    /// The body is not JSON carrying the repository routing fields.
    #[error("unparseable webhook payload: {0}")]
    InvalidPayload(#[from] serde_json::Error),

    /// Repository owner/name is unusable as a filesystem path component.
    #[error("{0}")]
    InvalidPath(#[from] InvalidPathComponent),

    /// Empty `X-GitHub-Event` header.
    #[error(transparent)]
    EmptyEventType(#[from] EmptyEventType),

    /// Spool error.
    #[error("spool error: {0}")]
    Spool(#[from] SpoolError),
}

impl IntoResponse for WebhookError {
    fn into_response(self) -> Response {
        let status = match &self {
            WebhookError::MissingHeader(_)
            | WebhookError::InvalidDeliveryId(_)
            | WebhookError::BodyNotUtf8
            | WebhookError::InvalidPayload(_)
            | WebhookError::InvalidPath(_)
            | WebhookError::EmptyEventType(_) => StatusCode::BAD_REQUEST,
            WebhookError::MissingSignature | WebhookError::InvalidSignature => {
                StatusCode::UNAUTHORIZED
            }
            WebhookError::Spool(e) => {
                debug_assert!(
                    !matches!(e, SpoolError::DuplicateDelivery(_)),
                    "the handler acknowledges duplicates with 202 before constructing an error"
                );
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };

        (status, self.to_string()).into_response()
    }
}

/// The routing fields intake needs from the payload. Everything else is
/// parsed at drain time from the raw stored body.
#[derive(Debug, Deserialize)]
struct RoutingPayload {
    repository: RoutingRepository,
}

#[derive(Debug, Deserialize)]
struct RoutingRepository {
    name: String,
    owner: RoutingOwner,
}

#[derive(Debug, Deserialize)]
struct RoutingOwner {
    login: String,
}

/// Webhook handler.
///
/// # Request
///
/// - Method: POST
/// - Required headers:
///   - `X-GitHub-Event`: event type (non-empty)
///   - `X-GitHub-Delivery`: delivery ID (must be a valid [`DeliveryId`])
///   - `X-Hub-Signature-256`: HMAC-SHA256 signature of the raw body
/// - Body: JSON webhook payload, at most [`WEBHOOK_BODY_LIMIT_BYTES`]
///
/// # Response
///
/// - 202 Accepted: spooled durably (including duplicate redeliveries, whose
///   surviving copy is fsynced before the 202)
/// - 400 Bad Request: missing/empty event header, invalid delivery ID, or
///   unusable payload
/// - 401 Unauthorized: missing or invalid signature
/// - 413 Payload Too Large: body over [`WEBHOOK_BODY_LIMIT_BYTES`]
/// - 500 Internal Server Error: spool failure (the delivery is NOT durable;
///   GitHub will retry)
pub async fn webhook_handler(
    State(app_state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, &'static str), WebhookError> {
    let event_type = get_header(&headers, HEADER_EVENT)?;
    let delivery_id = DeliveryId::parse(get_header(&headers, HEADER_DELIVERY)?)?;
    let signature_header = headers
        .get(HEADER_SIGNATURE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
        .ok_or(WebhookError::MissingSignature)?;

    // Verify against the raw bytes before any parsing or I/O.
    if !verify_signature(&body, &signature_header, app_state.webhook_secret()) {
        warn!(delivery_id = %delivery_id, "Invalid webhook signature");
        return Err(WebhookError::InvalidSignature);
    }

    let body = String::from_utf8(body.to_vec()).map_err(|_| WebhookError::BodyNotUtf8)?;

    let routing: RoutingPayload = serde_json::from_str(&body)?;
    let owner = routing.repository.owner.login;
    let repo = routing.repository.name;
    validate_path_component(&owner)?;
    validate_path_component(&repo)?;

    let envelope = WebhookEnvelope::new(event_type, signature_header, body, ArrivalMarker::now())?;

    let repo_spool_dir = app_state.spool_dir().join(&owner).join(&repo);

    match spool_webhook(&repo_spool_dir, &delivery_id, &envelope) {
        Ok(_) => {
            info!(
                delivery_id = %delivery_id,
                owner = %owner,
                repo = %repo,
                event_type = %envelope.event_type(),
                "Webhook spooled"
            );
            Ok((StatusCode::ACCEPTED, "Accepted"))
        }
        // The surviving copy is durable before spool_webhook reports a
        // duplicate, so acknowledging is safe.
        Err(SpoolError::DuplicateDelivery(_)) => Ok((StatusCode::ACCEPTED, "Accepted (duplicate)")),
        Err(e) => {
            warn!(delivery_id = %delivery_id, error = %e, "Failed to spool webhook");
            Err(WebhookError::Spool(e))
        }
    }
}

/// Extracts a required header value as a string.
fn get_header(headers: &HeaderMap, name: &'static str) -> Result<String, WebhookError> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
        .ok_or(WebhookError::MissingHeader(name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routing_payload_parses_owner_and_repo() {
        let body = r#"{
            "action": "opened",
            "repository": {
                "name": "hello-world",
                "owner": { "login": "octocat" }
            }
        }"#;

        let routing: RoutingPayload = serde_json::from_str(body).unwrap();
        assert_eq!(routing.repository.owner.login, "octocat");
        assert_eq!(routing.repository.name, "hello-world");
    }

    #[test]
    fn routing_payload_rejects_missing_repository() {
        let result: Result<RoutingPayload, _> = serde_json::from_str(r#"{"action": "opened"}"#);
        assert!(result.is_err());
    }

    #[test]
    fn routing_payload_rejects_missing_owner() {
        let result: Result<RoutingPayload, _> =
            serde_json::from_str(r#"{"repository": {"name": "hello-world"}}"#);
        assert!(result.is_err());
    }

    #[test]
    fn routing_payload_rejects_missing_name() {
        let result: Result<RoutingPayload, _> =
            serde_json::from_str(r#"{"repository": {"owner": {"login": "octocat"}}}"#);
        assert!(result.is_err());
    }

    #[test]
    fn get_header_present() {
        let mut headers = HeaderMap::new();
        headers.insert("x-github-event", "pull_request".parse().unwrap());

        let result = get_header(&headers, "x-github-event").unwrap();
        assert_eq!(result, "pull_request");
    }

    #[test]
    fn get_header_missing() {
        let headers = HeaderMap::new();

        let result = get_header(&headers, "x-github-event");
        assert!(matches!(result, Err(WebhookError::MissingHeader(_))));
    }
}
