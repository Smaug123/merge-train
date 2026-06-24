//! Webhook endpoint handler.
//!
//! Accepts GitHub webhook deliveries, verifies signatures, and hands them to
//! the repo's worker, which durably enqueues them on its `Store` and acks
//! before the handler replies 200. Intake parses only the routing fields
//! (repository owner/name) needed to pick the worker; full payload parsing
//! happens on the worker at drain time (`crate::webhooks::parse_webhook`).

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::oneshot;
use tracing::{info, warn};

use super::{AppState, InvalidPathComponent, validate_path_component};
use crate::store::StoreError;
use crate::types::DeliveryId;
use crate::types::ids::InvalidDeliveryId;
use crate::webhooks::verify_signature;
use crate::worker::{EnqueueOutcome, IntakeDelivery, WorkerError, WorkerMsg};

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

    /// The `X-GitHub-Event` header is present but empty: malformed client input.
    #[error("empty {HEADER_EVENT} header")]
    EmptyEvent,

    /// The signature header is absent: an authentication failure, the same
    /// class as [`WebhookError::InvalidSignature`], not a malformed request.
    #[error("missing {HEADER_SIGNATURE} header")]
    MissingSignature,

    /// Invalid signature.
    #[error("invalid signature")]
    InvalidSignature,

    /// The delivery ID is not a usable [`DeliveryId`].
    #[error(transparent)]
    InvalidDeliveryId(#[from] InvalidDeliveryId),

    /// The body is not JSON carrying the repository routing fields.
    #[error("unparseable webhook payload: {0}")]
    InvalidPayload(#[from] serde_json::Error),

    /// Repository owner/name is unusable as a filesystem path component.
    #[error("{0}")]
    InvalidPath(#[from] InvalidPathComponent),

    /// Routing the delivery to its worker failed (could not open the repo's
    /// `Store`, or the worker is gone).
    #[error("worker error: {0}")]
    Worker(#[from] WorkerError),

    /// The worker received the delivery but the durable enqueue failed.
    #[error("enqueue failed: {0}")]
    Enqueue(StoreError),
}

impl IntoResponse for WebhookError {
    fn into_response(self) -> Response {
        let status = match &self {
            WebhookError::MissingHeader(_)
            | WebhookError::EmptyEvent
            | WebhookError::InvalidDeliveryId(_)
            | WebhookError::InvalidPayload(_)
            | WebhookError::InvalidPath(_) => StatusCode::BAD_REQUEST,
            WebhookError::MissingSignature | WebhookError::InvalidSignature => {
                StatusCode::UNAUTHORIZED
            }
            // The repo is owned by another process, or its worker is
            // temporarily gone: retryable, so 503 (GitHub redelivers).
            WebhookError::Worker(WorkerError::Open(StoreError::Locked(_)))
            | WebhookError::Worker(WorkerError::Unavailable) => StatusCode::SERVICE_UNAVAILABLE,
            // A genuine store/open failure: not the delivery's fault.
            WebhookError::Worker(WorkerError::Open(_)) | WebhookError::Enqueue(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };

        (status, self.to_string()).into_response()
    }
}

/// The routing fields intake needs from the payload. Everything else is
/// parsed by the worker at drain time from the raw stored body.
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
///   - `X-GitHub-Event`: event type
///   - `X-GitHub-Delivery`: delivery ID (must be a valid [`DeliveryId`])
///   - `X-Hub-Signature-256`: HMAC-SHA256 signature of the raw body
/// - Body: JSON webhook payload, at most [`WEBHOOK_BODY_LIMIT_BYTES`]
///
/// # Response
///
/// - 200 OK: the delivery is durably enqueued on the repo's worker (including
///   duplicate redeliveries, which are idempotent no-ops)
/// - 400 Bad Request: missing event/delivery header, invalid delivery ID, or
///   unusable payload
/// - 401 Unauthorized: missing or invalid signature
/// - 413 Payload Too Large: body over [`WEBHOOK_BODY_LIMIT_BYTES`]
/// - 503 Service Unavailable: the repo is locked by another process or its
///   worker is unavailable (the delivery is NOT durable; GitHub will retry)
/// - 500 Internal Server Error: a store/enqueue failure (not durable; retried)
pub async fn webhook_handler(
    State(app_state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, &'static str), WebhookError> {
    let event_type = get_header(&headers, HEADER_EVENT)?;
    if event_type.is_empty() {
        return Err(WebhookError::EmptyEvent);
    }
    let delivery_id = DeliveryId::parse(get_header(&headers, HEADER_DELIVERY)?)?;
    let signature_header = headers
        .get(HEADER_SIGNATURE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
        .ok_or(WebhookError::MissingSignature)?;

    // Verify against the raw bytes before any parsing or routing.
    if !verify_signature(&body, &signature_header, app_state.webhook_secret()) {
        warn!(delivery_id = %delivery_id, "Invalid webhook signature");
        return Err(WebhookError::InvalidSignature);
    }

    let routing: RoutingPayload = serde_json::from_slice(&body)?;
    let owner = routing.repository.owner.login;
    let repo = routing.repository.name;
    validate_path_component(&owner)?;
    validate_path_component(&repo)?;

    let delivery = IntakeDelivery {
        delivery_id: delivery_id.to_string(),
        event_type: event_type.clone(),
        headers: captured_headers(&delivery_id, &event_type, &signature_header),
        body: body.to_vec(),
    };

    // Route to the repo's worker and await its durable-enqueue ack before
    // replying 200 (at-least-once intake; the server never opens the Store).
    let sender = app_state.workers().sender_for(&owner, &repo).await?;
    let (ack_tx, ack_rx) = oneshot::channel();
    sender
        .send(WorkerMsg::Enqueue {
            delivery,
            ack: ack_tx,
        })
        .await
        .map_err(|_| WebhookError::Worker(WorkerError::Unavailable))?;

    match ack_rx.await {
        Ok(Ok(outcome)) => {
            let note = match outcome {
                EnqueueOutcome::Enqueued => "enqueued",
                EnqueueOutcome::Duplicate => "duplicate",
            };
            info!(delivery_id = %delivery_id, owner, repo, event_type, note, "Webhook accepted");
            Ok((StatusCode::OK, "OK"))
        }
        Ok(Err(e)) => {
            warn!(delivery_id = %delivery_id, error = %e, "Durable enqueue failed");
            Err(WebhookError::Enqueue(e))
        }
        // The worker dropped the ack without answering: it is gone.
        Err(_) => Err(WebhookError::Worker(WorkerError::Unavailable)),
    }
}

/// The GitHub headers we persist alongside a delivery, as a JSON object (for
/// audit/replay; the signature is already verified at intake).
fn captured_headers(delivery_id: &DeliveryId, event_type: &str, signature: &str) -> String {
    let mut map = serde_json::Map::new();
    map.insert(HEADER_EVENT.to_owned(), event_type.into());
    map.insert(HEADER_DELIVERY.to_owned(), delivery_id.to_string().into());
    map.insert(HEADER_SIGNATURE.to_owned(), signature.into());
    serde_json::Value::Object(map).to_string()
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
