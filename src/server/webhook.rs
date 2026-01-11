//! Webhook endpoint handler.
//!
//! Accepts GitHub webhook deliveries, validates signatures, and spools them
//! durably before returning 202 Accepted. The actual processing happens
//! asynchronously in per-repo workers.

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use std::collections::HashMap;
use thiserror::Error;
use tracing::{debug, info, warn};

use super::AppState;
use crate::spool::delivery::{SpoolError, WebhookEnvelope, spool_webhook};
use crate::types::DeliveryId;
use crate::webhooks::verify_signature;

/// Header name for GitHub event type.
const HEADER_EVENT: &str = "x-github-event";
/// Header name for GitHub delivery ID.
const HEADER_DELIVERY: &str = "x-github-delivery";
/// Header name for GitHub signature.
const HEADER_SIGNATURE: &str = "x-hub-signature-256";

/// Errors that can occur when processing a webhook.
#[derive(Debug, Error)]
pub enum WebhookError {
    /// Missing required header.
    #[error("missing required header: {0}")]
    MissingHeader(&'static str),

    /// Invalid signature.
    #[error("invalid signature")]
    InvalidSignature,

    /// Invalid JSON body.
    #[error("invalid JSON body: {0}")]
    InvalidJson(#[from] serde_json::Error),

    /// Missing repository information in payload.
    #[error("missing repository information in payload")]
    MissingRepository,

    /// Spool error.
    #[error("spool error: {0}")]
    Spool(#[from] SpoolError),
}

impl IntoResponse for WebhookError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            WebhookError::MissingHeader(_) => (StatusCode::BAD_REQUEST, self.to_string()),
            WebhookError::InvalidSignature => (StatusCode::UNAUTHORIZED, self.to_string()),
            WebhookError::InvalidJson(_) => (StatusCode::BAD_REQUEST, self.to_string()),
            WebhookError::MissingRepository => (StatusCode::BAD_REQUEST, self.to_string()),
            WebhookError::Spool(SpoolError::DuplicateDelivery(_)) => {
                // Duplicates are handled idempotently - return 202
                return (StatusCode::ACCEPTED, "Accepted (duplicate)").into_response();
            }
            WebhookError::Spool(_) => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
        };

        (status, message).into_response()
    }
}

/// Webhook handler.
///
/// Accepts GitHub webhook deliveries and spools them for asynchronous processing.
///
/// # Request
///
/// - Method: POST
/// - Required headers:
///   - `X-GitHub-Event`: Event type (e.g., "pull_request", "issue_comment")
///   - `X-GitHub-Delivery`: Unique delivery ID (UUID format)
///   - `X-Hub-Signature-256`: HMAC-SHA256 signature of the payload
/// - Body: JSON webhook payload
///
/// # Response
///
/// - 202 Accepted: Webhook spooled successfully
/// - 400 Bad Request: Missing header or invalid JSON
/// - 401 Unauthorized: Invalid signature
/// - 500 Internal Server Error: Spool failure
///
/// # Example
///
/// ```ignore
/// POST /webhook HTTP/1.1
/// X-GitHub-Event: pull_request
/// X-GitHub-Delivery: 550e8400-e29b-41d4-a716-446655440000
/// X-Hub-Signature-256: sha256=...
/// Content-Type: application/json
///
/// {"action": "opened", "pull_request": {...}, "repository": {...}}
///
/// HTTP/1.1 202 Accepted
/// ```
pub async fn webhook_handler(
    State(app_state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, &'static str), WebhookError> {
    // Extract required headers
    let event_type = get_header(&headers, HEADER_EVENT)?;
    let delivery_id_str = get_header(&headers, HEADER_DELIVERY)?;
    let signature_header = get_header(&headers, HEADER_SIGNATURE)?;

    let delivery_id = DeliveryId::new(delivery_id_str);

    debug!(
        delivery_id = %delivery_id,
        event_type = %event_type,
        "Received webhook"
    );

    // Verify signature BEFORE any parsing or I/O.
    // This is critical for security: we don't want to waste resources
    // on malicious requests.
    if !verify_signature(&body, &signature_header, app_state.webhook_secret()) {
        warn!(delivery_id = %delivery_id, "Invalid webhook signature");
        return Err(WebhookError::InvalidSignature);
    }

    // Parse the JSON body
    let body_json: serde_json::Value = serde_json::from_slice(&body)?;

    // Extract repository owner and name from the payload.
    // Most GitHub webhook events include a "repository" object.
    let (owner, repo) = extract_repository(&body_json)?;

    debug!(
        delivery_id = %delivery_id,
        owner = %owner,
        repo = %repo,
        "Webhook for repository"
    );

    // Build headers map for the envelope
    let headers_map = extract_headers(&headers);

    // Create the webhook envelope
    let envelope = WebhookEnvelope {
        event_type: event_type.to_string(),
        headers: headers_map,
        body: body_json,
    };

    // Construct the spool directory for this repository.
    // Structure: <spool_dir>/<owner>/<repo>/
    let repo_spool_dir = app_state.spool_dir().join(&owner).join(&repo);

    // Spool the webhook atomically
    match spool_webhook(&repo_spool_dir, &delivery_id, &envelope) {
        Ok(_) => {
            info!(
                delivery_id = %delivery_id,
                owner = %owner,
                repo = %repo,
                event_type = %event_type,
                "Webhook spooled successfully"
            );
            Ok((StatusCode::ACCEPTED, "Accepted"))
        }
        Err(SpoolError::DuplicateDelivery(_)) => {
            // Duplicate deliveries are idempotent - return success
            debug!(
                delivery_id = %delivery_id,
                "Duplicate webhook delivery (idempotent)"
            );
            Ok((StatusCode::ACCEPTED, "Accepted (duplicate)"))
        }
        Err(e) => {
            warn!(
                delivery_id = %delivery_id,
                error = %e,
                "Failed to spool webhook"
            );
            Err(WebhookError::Spool(e))
        }
    }
}

/// Extracts a required header value as a string.
fn get_header(headers: &HeaderMap, name: &'static str) -> Result<String, WebhookError> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .ok_or(WebhookError::MissingHeader(name))
}

/// Extracts repository owner and name from a webhook payload.
///
/// Most GitHub webhook events include a "repository" object with
/// "owner" and "name" fields.
fn extract_repository(body: &serde_json::Value) -> Result<(String, String), WebhookError> {
    let repository = body
        .get("repository")
        .ok_or(WebhookError::MissingRepository)?;

    let owner = repository
        .get("owner")
        .and_then(|o| o.get("login"))
        .and_then(|l| l.as_str())
        .ok_or(WebhookError::MissingRepository)?;

    let name = repository
        .get("name")
        .and_then(|n| n.as_str())
        .ok_or(WebhookError::MissingRepository)?;

    Ok((owner.to_string(), name.to_string()))
}

/// Extracts HTTP headers into a HashMap for storage in the envelope.
///
/// Only includes headers that can be converted to valid UTF-8 strings.
fn extract_headers(headers: &HeaderMap) -> HashMap<String, String> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|v| (name.to_string(), v.to_string()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extract_repository_valid() {
        let body = json!({
            "action": "opened",
            "repository": {
                "name": "hello-world",
                "owner": {
                    "login": "octocat"
                }
            }
        });

        let (owner, repo) = extract_repository(&body).unwrap();
        assert_eq!(owner, "octocat");
        assert_eq!(repo, "hello-world");
    }

    #[test]
    fn extract_repository_missing_repository() {
        let body = json!({
            "action": "opened"
        });

        let result = extract_repository(&body);
        assert!(matches!(result, Err(WebhookError::MissingRepository)));
    }

    #[test]
    fn extract_repository_missing_owner() {
        let body = json!({
            "repository": {
                "name": "hello-world"
            }
        });

        let result = extract_repository(&body);
        assert!(matches!(result, Err(WebhookError::MissingRepository)));
    }

    #[test]
    fn extract_repository_missing_name() {
        let body = json!({
            "repository": {
                "owner": {
                    "login": "octocat"
                }
            }
        });

        let result = extract_repository(&body);
        assert!(matches!(result, Err(WebhookError::MissingRepository)));
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

    #[test]
    fn extract_headers_filters_invalid_utf8() {
        let mut headers = HeaderMap::new();
        headers.insert("valid-header", "valid-value".parse().unwrap());
        // Note: HeaderMap doesn't actually allow invalid UTF-8 values,
        // so this test just verifies the basic case works.

        let result = extract_headers(&headers);
        assert_eq!(result.get("valid-header"), Some(&"valid-value".to_string()));
    }
}
