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

use super::{AppState, InvalidPathComponent, validate_path_component};
use crate::commands::{Command, parse_command};
use crate::spool::delivery::{SpoolError, SpooledDelivery, WebhookEnvelope, spool_webhook};
use crate::types::{DeliveryId, PrNumber, RepoId};
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

    /// Invalid path component (e.g., path traversal attempt).
    #[error("{0}")]
    InvalidPath(#[from] InvalidPathComponent),

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
            WebhookError::InvalidPath(_) => (StatusCode::BAD_REQUEST, self.to_string()),
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
///   - `X-GitHub-Delivery`: Unique delivery ID (validated for path safety, not UUID format)
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

    // Validate path components to prevent path traversal attacks.
    // All three values (owner, repo, delivery_id) are used in filesystem paths.
    validate_path_component(&owner)?;
    validate_path_component(&repo)?;
    validate_path_component(delivery_id.as_str())?;

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

            // Dispatch to worker if dispatcher is configured.
            // Fire-and-forget: don't block the 202 response.
            if let Some(dispatcher) = app_state.dispatcher() {
                let dispatcher = std::sync::Arc::clone(dispatcher);
                let repo_id = RepoId::new(&owner, &repo);
                let delivery = SpooledDelivery::new(&repo_spool_dir, delivery_id.clone());

                // Check for immediate cancellation: if this is a stop command,
                // trigger stack cancellation before dispatching. This ensures
                // in-flight operations are interrupted promptly, even if the
                // worker's queue has other events ahead of this one.
                if let Some(pr) =
                    detect_stop_command(&event_type, &envelope.body, app_state.bot_name())
                {
                    info!(
                        delivery_id = %delivery_id,
                        pr = %pr,
                        "Stop command detected, triggering immediate stack cancellation"
                    );
                    let dispatcher_cancel = std::sync::Arc::clone(&dispatcher);
                    let repo_id_cancel = repo_id.clone();
                    tokio::spawn(async move {
                        if let Err(e) = dispatcher_cancel.cancel_stack(&repo_id_cancel, pr).await {
                            warn!(
                                repo = %repo_id_cancel,
                                pr = %pr,
                                error = %e,
                                "Failed to cancel stack"
                            );
                        }
                    });
                }

                tokio::spawn(async move {
                    if let Err(e) = dispatcher.dispatch(&repo_id, delivery).await {
                        warn!(
                            repo = %repo_id,
                            error = %e,
                            "Failed to dispatch webhook to worker"
                        );
                    }
                });
            }

            Ok((StatusCode::ACCEPTED, "Accepted"))
        }
        Err(SpoolError::DuplicateDelivery(_)) => {
            // Duplicate deliveries are idempotent - return success without dispatching.
            // The delivery was already spooled and will be (or has been) processed.
            // Dispatching again would risk double-processing.
            debug!(
                delivery_id = %delivery_id,
                "Duplicate webhook delivery (idempotent, not dispatched)"
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

/// Detects if a webhook is a stop command and extracts the PR number.
///
/// This enables immediate stack cancellation from the webhook handler,
/// before the event is processed by the worker. Stop commands get
/// highest priority, and cancelling immediately ensures in-flight
/// operations are interrupted as soon as possible.
///
/// Returns `Some(pr_number)` if this is a stop command for a PR,
/// or `None` if it's not a stop command.
fn detect_stop_command(
    event_type: &str,
    body: &serde_json::Value,
    bot_name: &str,
) -> Option<PrNumber> {
    // Only issue_comment events can contain stop commands
    if event_type != "issue_comment" {
        return None;
    }

    // Only "created" actions are relevant (not "edited" or "deleted")
    let action = body.get("action")?.as_str()?;
    if action != "created" {
        return None;
    }

    // Must be a comment on a PR, not a regular issue
    // GitHub includes "pull_request" field in issue for PR comments
    body.get("issue")?.get("pull_request")?;

    // Extract the PR number
    let pr_number = body.get("issue")?.get("number")?.as_u64()?;

    // Extract the comment body
    let comment_body = body.get("comment")?.get("body")?.as_str()?;

    // Check if it's a stop command
    if let Some(cmd) = parse_command(comment_body, bot_name)
        && matches!(cmd, Command::Stop | Command::StopForce)
    {
        return Some(PrNumber(pr_number));
    }

    None
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
        use axum::http::HeaderValue;

        let mut headers = HeaderMap::new();
        headers.insert("valid-header", "valid-value".parse().unwrap());

        // Create a header value with invalid UTF-8 using from_bytes.
        // Bytes 0x80-0xFF are invalid as standalone UTF-8 bytes.
        let invalid_utf8_value = HeaderValue::from_bytes(&[0x80, 0x81, 0x82]).unwrap();
        headers.insert("invalid-header", invalid_utf8_value);

        let result = extract_headers(&headers);

        // Valid header should be present
        assert_eq!(result.get("valid-header"), Some(&"valid-value".to_string()));

        // Invalid UTF-8 header should be filtered out
        assert!(!result.contains_key("invalid-header"));
    }

    // ─── detect_stop_command tests ───

    #[test]
    fn detect_stop_command_with_stop() {
        let body = json!({
            "action": "created",
            "issue": {
                "number": 42,
                "pull_request": {}
            },
            "comment": {
                "body": "@merge-train stop"
            }
        });

        let result = detect_stop_command("issue_comment", &body, "merge-train");
        assert_eq!(result, Some(PrNumber(42)));
    }

    #[test]
    fn detect_stop_command_with_stop_force() {
        let body = json!({
            "action": "created",
            "issue": {
                "number": 99,
                "pull_request": {}
            },
            "comment": {
                "body": "@merge-train stop --force"
            }
        });

        let result = detect_stop_command("issue_comment", &body, "merge-train");
        assert_eq!(result, Some(PrNumber(99)));
    }

    #[test]
    fn detect_stop_command_wrong_event_type() {
        let body = json!({
            "action": "created",
            "issue": {
                "number": 42,
                "pull_request": {}
            },
            "comment": {
                "body": "@merge-train stop"
            }
        });

        // Wrong event type
        let result = detect_stop_command("pull_request", &body, "merge-train");
        assert_eq!(result, None);
    }

    #[test]
    fn detect_stop_command_not_a_stop() {
        let body = json!({
            "action": "created",
            "issue": {
                "number": 42,
                "pull_request": {}
            },
            "comment": {
                "body": "@merge-train start"
            }
        });

        // Start command, not stop
        let result = detect_stop_command("issue_comment", &body, "merge-train");
        assert_eq!(result, None);
    }

    #[test]
    fn detect_stop_command_regular_issue_not_pr() {
        let body = json!({
            "action": "created",
            "issue": {
                "number": 42
                // No pull_request field - this is a regular issue
            },
            "comment": {
                "body": "@merge-train stop"
            }
        });

        let result = detect_stop_command("issue_comment", &body, "merge-train");
        assert_eq!(result, None);
    }

    #[test]
    fn detect_stop_command_edited_action() {
        let body = json!({
            "action": "edited",  // Not "created"
            "issue": {
                "number": 42,
                "pull_request": {}
            },
            "comment": {
                "body": "@merge-train stop"
            }
        });

        let result = detect_stop_command("issue_comment", &body, "merge-train");
        assert_eq!(result, None);
    }

    #[test]
    fn detect_stop_command_custom_bot_name() {
        let body = json!({
            "action": "created",
            "issue": {
                "number": 42,
                "pull_request": {}
            },
            "comment": {
                "body": "@custom-bot stop"
            }
        });

        // With default bot name
        let result = detect_stop_command("issue_comment", &body, "merge-train");
        assert_eq!(result, None);

        // With matching bot name
        let result = detect_stop_command("issue_comment", &body, "custom-bot");
        assert_eq!(result, Some(PrNumber(42)));
    }
}
