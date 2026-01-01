//! Spooled delivery management with atomic file operations.
//!
//! Provides crash-safe spooling of webhook deliveries and marker file management.

use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::persistence::fsync::{fsync_dir, fsync_file};
use crate::types::DeliveryId;

/// Counter for generating unique temp file names within a process.
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generates a unique temp file path for atomic writes.
///
/// Uses PID + monotonic counter to ensure uniqueness across concurrent operations.
/// This prevents the race condition where concurrent spool_delivery calls for the
/// same delivery ID could clobber each other's temp files.
///
/// For a base path like `<id>.json`, produces `<id>.json.tmp.<pid>.<counter>`.
/// This preserves the original extension so cleanup can find orphaned temp files
/// by pattern matching on `<id>.json.tmp.*`.
fn unique_temp_path(base_path: &Path) -> PathBuf {
    let pid = std::process::id();
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    // Append .tmp.<pid>.<counter> to existing extension to produce <id>.json.tmp.<pid>.<counter>
    let new_extension = match base_path.extension().and_then(|e| e.to_str()) {
        Some(ext) => format!("{}.tmp.{}.{}", ext, pid, counter),
        None => format!("tmp.{}.{}", pid, counter),
    };
    base_path.with_extension(new_extension)
}

/// A structured webhook envelope for crash-safe spooling.
///
/// This stores the complete webhook delivery: headers + body, as specified
/// in DESIGN.md. The headers are required for:
/// 1. **Replay**: The `X-GitHub-Event` header determines how to parse the body
/// 2. **Debugging**: Headers like `X-Hub-Signature-256` aid in troubleshooting
/// 3. **Audit**: The full request context is preserved for analysis
///
/// # Why Not Just Store Raw Bytes?
///
/// Storing raw bytes is dangerous because:
/// 1. On replay, we don't know what type to deserialize the body as
/// 2. The event type is in the HTTP headers, which would be lost
/// 3. We can't validate that the payload is correctly structured
///
/// By using a structured envelope, we "parse, don't validate" at the
/// boundary: the API enforces that event_type is always present.
///
/// # Example
///
/// ```ignore
/// use std::collections::HashMap;
///
/// let mut headers = HashMap::new();
/// headers.insert("X-GitHub-Event".to_string(), "pull_request".to_string());
/// headers.insert("X-Hub-Signature-256".to_string(), "sha256=...".to_string());
///
/// let envelope = WebhookEnvelope {
///     event_type: "pull_request".to_string(),
///     headers,
///     body: serde_json::from_slice(&request_body)?,
/// };
/// spool_webhook(&spool_dir, &delivery_id, &envelope)?;
///
/// // On replay:
/// let envelope = delivery.read_webhook()?;
/// match envelope.event_type.as_str() {
///     "pull_request" => {
///         let event: PullRequestEvent = serde_json::from_value(envelope.body)?;
///     }
///     "check_suite" => { /* ... */ }
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WebhookEnvelope {
    /// The GitHub event type from the `X-GitHub-Event` header.
    /// This is required for determining how to parse the body on replay.
    /// Stored separately for efficient access without parsing all headers.
    pub event_type: String,

    /// All HTTP headers from the webhook request.
    /// Stored for replay/debug purposes. Keys are header names (case-preserved),
    /// values are the header values as strings.
    #[serde(default)]
    pub headers: std::collections::HashMap<String, String>,

    /// The webhook body as a JSON value.
    /// This is the parsed JSON payload from GitHub. Using `serde_json::Value`
    /// validates the body is valid JSON while preserving the exact structure.
    pub body: serde_json::Value,
}

/// Errors that can occur during spool operations.
#[derive(Debug, Error)]
pub enum SpoolError {
    /// IO error during file operations.
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    /// JSON serialization/deserialization error.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Duplicate delivery ID (already exists in spool).
    #[error("duplicate delivery ID: {0}")]
    DuplicateDelivery(DeliveryId),

    /// Invalid delivery ID (contains path separators or other unsafe characters).
    #[error("invalid delivery ID: contains unsafe characters: {0}")]
    InvalidDeliveryId(DeliveryId),

    /// Invalid webhook envelope (empty event_type).
    #[error("invalid webhook envelope: event_type is empty")]
    EmptyEventType,
}

/// Result type for spool operations.
pub type Result<T> = std::result::Result<T, SpoolError>;

/// Validates that a delivery ID is safe to use in filenames.
///
/// A delivery ID is unsafe if it:
/// - Contains path separators (`/` or `\`)
/// - Contains null bytes
/// - Is empty
/// - Starts with a dot (hidden file, could conflict with markers)
/// - Is `.` or `..` (directory traversal)
fn validate_delivery_id(delivery_id: &DeliveryId) -> Result<()> {
    let id = delivery_id.as_str();

    if id.is_empty() {
        return Err(SpoolError::InvalidDeliveryId(delivery_id.clone()));
    }

    // Check for path separators, null bytes, and other unsafe characters
    if id.contains('/') || id.contains('\\') || id.contains('\0') {
        return Err(SpoolError::InvalidDeliveryId(delivery_id.clone()));
    }

    // Reject hidden files and directory traversal
    if id.starts_with('.') {
        return Err(SpoolError::InvalidDeliveryId(delivery_id.clone()));
    }

    Ok(())
}

/// A webhook delivery in the spool.
///
/// Each delivery is identified by GitHub's `X-GitHub-Delivery` header value.
/// The delivery progresses through states tracked by marker files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpooledDelivery {
    /// The delivery ID from GitHub's X-GitHub-Delivery header.
    pub delivery_id: DeliveryId,

    /// Path to the payload file (<delivery-id>.json).
    pub payload_path: PathBuf,

    /// Path to the spool directory.
    pub spool_dir: PathBuf,
}

impl SpooledDelivery {
    /// Creates a new SpooledDelivery for a given delivery ID.
    pub fn new(spool_dir: &Path, delivery_id: DeliveryId) -> Self {
        let payload_path = spool_dir.join(format!("{}.json", delivery_id.as_str()));
        SpooledDelivery {
            delivery_id,
            payload_path,
            spool_dir: spool_dir.to_path_buf(),
        }
    }

    /// Returns the path to the processing marker file.
    pub fn proc_marker_path(&self) -> PathBuf {
        self.payload_path.with_extension("json.proc")
    }

    /// Returns the path to the done marker file.
    pub fn done_marker_path(&self) -> PathBuf {
        self.payload_path.with_extension("json.done")
    }

    /// Returns the path to the temp file used during atomic writes.
    pub fn temp_path(&self) -> PathBuf {
        self.payload_path.with_extension("json.tmp")
    }

    /// Checks if the delivery is pending (payload exists, not processing, not done).
    ///
    /// A delivery is pending if:
    /// - The payload file exists
    /// - No `.proc` marker exists (not currently being processed)
    /// - No `.done` marker exists (not already completed)
    ///
    /// This ensures `drain_pending` won't return items that are actively being
    /// processed by a worker. At startup, `cleanup_interrupted_processing()` removes
    /// stale `.proc` markers from crashed runs, allowing those deliveries to become
    /// pending again.
    pub fn is_pending(&self) -> bool {
        self.payload_path.exists()
            && !self.proc_marker_path().exists()
            && !self.done_marker_path().exists()
    }

    /// Checks if the delivery is being processed (proc marker exists, no done marker).
    pub fn is_processing(&self) -> bool {
        self.proc_marker_path().exists() && !self.done_marker_path().exists()
    }

    /// Checks if the delivery is done (done marker exists).
    pub fn is_done(&self) -> bool {
        self.done_marker_path().exists()
    }

    /// Reads and deserializes the payload.
    pub fn read_payload<T: for<'de> Deserialize<'de>>(&self) -> Result<T> {
        let bytes = std::fs::read(&self.payload_path)?;
        let payload = serde_json::from_slice(&bytes)?;
        Ok(payload)
    }

    /// Reads the raw payload bytes.
    pub fn read_payload_bytes(&self) -> Result<Vec<u8>> {
        Ok(std::fs::read(&self.payload_path)?)
    }

    /// Reads and deserializes the webhook envelope.
    ///
    /// This is the preferred method for reading spooled webhooks, as it
    /// returns a structured envelope with the event type needed for parsing.
    ///
    /// # Errors
    ///
    /// Returns `SpoolError::Io` if the file cannot be read.
    /// Returns `SpoolError::Json` if the file is not a valid `WebhookEnvelope`.
    pub fn read_webhook(&self) -> Result<WebhookEnvelope> {
        let bytes = std::fs::read(&self.payload_path)?;
        let envelope = serde_json::from_slice(&bytes)?;
        Ok(envelope)
    }
}

/// Spools a webhook delivery to disk atomically.
///
/// The delivery is written using the write-to-temp-then-link pattern:
/// 1. Write to `<delivery-id>.json.tmp`
/// 2. fsync the temp file
/// 3. hard_link to `<delivery-id>.json` (fails atomically if exists)
/// 4. Remove the temp file
/// 5. fsync the directory
///
/// Using hard_link instead of rename provides atomic duplicate detection:
/// on Unix, link() fails with EEXIST if the target already exists, preventing
/// the race condition where two concurrent spool_delivery calls could both
/// pass exists() checks and the later rename would overwrite the first.
///
/// # Errors
///
/// Returns `SpoolError::DuplicateDelivery` if a delivery with the same ID already exists.
/// Returns `SpoolError::Io` for filesystem errors.
///
/// # Note
///
/// This is an internal function. Use [`spool_webhook`] for the public API,
/// which enforces the envelope structure required for crash-replay.
pub(crate) fn spool_delivery(
    spool_dir: &Path,
    delivery_id: &DeliveryId,
    payload: &[u8],
) -> Result<SpooledDelivery> {
    // Validate delivery ID to prevent path traversal attacks
    validate_delivery_id(delivery_id)?;

    // Ensure spool directory exists, with durable creation.
    //
    // When create_dir_all creates intermediate directories, each parent must be
    // fsynced to make its child's directory entry durable. For example, if
    // creating /a/b/c/d where only /a exists, we must fsync /a (for b), /a/b
    // (for c), and /a/b/c (for d). Without this, a crash could drop any of the
    // intermediate directories even after create_dir_all returns successfully.
    //
    // Algorithm:
    // 1. Find the first existing ancestor before create_dir_all
    // 2. After create_dir_all, fsync all directories from that ancestor down
    //    to (but not including) spool_dir
    let first_existing_ancestor = {
        let mut path = spool_dir.to_path_buf();
        while !path.exists() {
            if let Some(parent) = path.parent() {
                path = parent.to_path_buf();
            } else {
                break;
            }
        }
        if path.exists() { Some(path) } else { None }
    };

    std::fs::create_dir_all(spool_dir)?;

    // Fsync all directories from first_existing_ancestor to spool_dir's parent
    if let Some(ancestor) = first_existing_ancestor {
        // Collect directories that need fsyncing: from ancestor to spool_dir.parent()
        // We need to fsync each directory to make its child's entry durable.
        let mut dirs_to_fsync = Vec::new();
        let mut current = spool_dir.to_path_buf();
        while current != ancestor {
            if let Some(parent) = current.parent() {
                dirs_to_fsync.push(parent.to_path_buf());
                current = parent.to_path_buf();
            } else {
                break;
            }
        }
        // Fsync from ancestor down to immediate parent of spool_dir
        // (the list is in reverse order: [parent, grandparent, ...])
        for dir in dirs_to_fsync.into_iter().rev() {
            fsync_dir(&dir)?;
        }
    }

    let delivery = SpooledDelivery::new(spool_dir, delivery_id.clone());

    // Early check for done marker (processed but not yet cleaned up)
    // This is an optimization to fail fast; the atomic check below is authoritative.
    if delivery.done_marker_path().exists() {
        return Err(SpoolError::DuplicateDelivery(delivery_id.clone()));
    }

    // Write to temp file with unique name to prevent concurrent clobbering.
    // Without unique names, concurrent spool_delivery calls for the same ID
    // could overwrite each other's temp files before hard_link, leading to
    // nondeterministic payload contents.
    let temp_path = unique_temp_path(&delivery.payload_path);
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_path)?;
        file.write_all(payload)?;
        fsync_file(&file)?;
    }

    // Atomic link - fails with EEXIST if target already exists
    // This is the authoritative duplicate check that prevents race conditions.
    match std::fs::hard_link(&temp_path, &delivery.payload_path) {
        Ok(()) => {
            // Link succeeded, remove the temp file
            let _ = std::fs::remove_file(&temp_path);
        }
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
            // Target already exists - this is a duplicate
            let _ = std::fs::remove_file(&temp_path);
            return Err(SpoolError::DuplicateDelivery(delivery_id.clone()));
        }
        Err(e) => {
            // Some other error - clean up temp file and propagate
            let _ = std::fs::remove_file(&temp_path);
            return Err(e.into());
        }
    }

    // fsync directory to ensure the link is durable
    fsync_dir(spool_dir)?;

    Ok(delivery)
}

/// Spools a webhook with structured envelope to disk atomically.
///
/// This is the preferred API for spooling webhooks, as it ensures the event
/// type is always stored alongside the body. This is required for crash
/// recovery: on replay, we need to know the event type to correctly parse
/// the body.
///
/// The envelope is serialized to JSON before being written to disk.
///
/// # Example
///
/// ```ignore
/// use std::collections::HashMap;
///
/// let envelope = WebhookEnvelope {
///     event_type: "pull_request".to_string(),
///     headers: HashMap::new(), // or populate with actual headers
///     body: request_body,
/// };
/// let delivery = spool_webhook(&spool_dir, &delivery_id, &envelope)?;
/// ```
///
/// # Errors
///
/// Returns `SpoolError::DuplicateDelivery` if a delivery with the same ID already exists.
/// Returns `SpoolError::Io` for filesystem errors.
/// Returns `SpoolError::Json` if serialization fails.
pub fn spool_webhook(
    spool_dir: &Path,
    delivery_id: &DeliveryId,
    envelope: &WebhookEnvelope,
) -> Result<SpooledDelivery> {
    // Validate event_type is non-empty (required for crash-replay).
    // This is enforced at the boundary to ensure the invariant holds.
    if envelope.event_type.is_empty() {
        return Err(SpoolError::EmptyEventType);
    }

    let payload = serde_json::to_vec(envelope)?;
    spool_delivery(spool_dir, delivery_id, &payload)
}

/// Marks a delivery as being processed by creating the `.proc` marker.
///
/// This is idempotent - calling it multiple times has no additional effect.
///
/// # Errors
///
/// Returns `SpoolError::Io` for filesystem errors.
pub fn mark_processing(delivery: &SpooledDelivery) -> Result<()> {
    create_marker_file(&delivery.proc_marker_path(), &delivery.spool_dir)
}

/// Marks a delivery as done by creating the `.done` marker.
///
/// This should only be called after all state effects from the delivery
/// are durably persisted (fsynced to event log).
///
/// This is idempotent - calling it multiple times has no additional effect.
///
/// # Errors
///
/// Returns `SpoolError::Io` for filesystem errors.
pub fn mark_done(delivery: &SpooledDelivery) -> Result<()> {
    create_marker_file(&delivery.done_marker_path(), &delivery.spool_dir)
}

/// Creates an empty marker file atomically using temp-file + fsync + rename.
///
/// The marker is created with the following crash-safe sequence:
/// 1. Create a temp file with `.tmp` suffix
/// 2. fsync the temp file (even though empty, this ensures metadata is durable)
/// 3. Atomic rename to the final path
/// 4. fsync the directory to ensure the rename is durable
///
/// This ensures that if we crash at any point:
/// - Before rename: no marker exists (temp file is ignored on recovery)
/// - After rename but before dir fsync: marker may or may not exist (but if it
///   exists, it's complete)
fn create_marker_file(path: &Path, spool_dir: &Path) -> Result<()> {
    // If marker already exists, this is a no-op (idempotent)
    if path.exists() {
        return Ok(());
    }

    // Create temp file path by appending .tmp to the marker path
    // e.g., "delivery.json.done" -> "delivery.json.done.tmp"
    let temp_path = path.with_extension(
        path.extension()
            .map(|e| format!("{}.tmp", e.to_string_lossy()))
            .unwrap_or_else(|| "tmp".to_string()),
    );

    // Create and fsync the temp file
    {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_path)?;
        // fsync the file itself to ensure it's durable before rename
        fsync_file(&file)?;
    }

    // Atomic rename - if this succeeds, the marker is created
    // If we crash before this, the temp file is ignored on recovery
    std::fs::rename(&temp_path, path)?;

    // fsync directory to ensure the rename is durable
    fsync_dir(spool_dir)?;

    Ok(())
}

/// Removes a spooled delivery and all its marker files.
///
/// This should be called after the grace period has passed for done deliveries.
///
/// # Deletion Order (Crash Safety)
///
/// Files are deleted in this order to ensure crash safety:
/// 1. **Payload first**: The payload is deleted before any markers. This ensures
///    that if we crash mid-cleanup, the delivery cannot become "pending" again.
///    (`is_pending()` requires `payload_path.exists()`, so deleting the payload
///    first prevents reprocessing.)
/// 2. **Markers second**: After the payload is gone, marker deletion order doesn't
///    matter. Orphaned markers are harmless (they just consume disk space).
/// 3. **Directory fsync last**: Ensures all deletions are durable. Without this,
///    a power loss could "resurrect" deleted files.
///
/// The WRONG order (markers before payload) would be dangerous: a crash after
/// deleting `.done` but before deleting the payload would leave a payload without
/// a done marker, which `drain_pending()` would treat as pending → reprocessing!
///
/// # Error Handling
///
/// If payload deletion fails (for any reason other than the file not existing),
/// this function returns an error WITHOUT deleting any markers. This prevents
/// a dangerous state where the payload exists but markers are deleted, which
/// would cause `is_pending()` to return true for an already-processed delivery.
pub fn remove_delivery(delivery: &SpooledDelivery) -> Result<()> {
    // CRITICAL: Delete payload FIRST to prevent reprocessing on crash.
    // If we crash after this, the delivery cannot become pending again because
    // is_pending() requires the payload file to exist.
    //
    // CRITICAL: We must NOT ignore errors here! If payload deletion fails but
    // we proceed to delete markers, we leave the system in a dangerous state
    // where is_pending() returns true for an already-processed delivery.
    match std::fs::remove_file(&delivery.payload_path) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            // Payload already deleted (idempotent) - safe to continue
        }
        Err(e) => {
            // Payload deletion failed - DO NOT delete markers!
            // Returning here preserves the invariant: if payload exists,
            // markers also exist (so is_pending() remains false).
            return Err(e.into());
        }
    }

    // Now safe to delete markers (orphaned markers are harmless).
    // We can ignore errors here because:
    // 1. Payload is confirmed deleted, so is_pending() will return false
    // 2. Orphaned markers just consume disk space, they don't affect correctness
    let _ = std::fs::remove_file(delivery.done_marker_path());
    let _ = std::fs::remove_file(delivery.proc_marker_path());

    // Clean up any orphaned temp files from interrupted spool operations.
    // These have the pattern <id>.json.tmp.<pid>.<counter> or the legacy <id>.json.tmp
    let _ = std::fs::remove_file(delivery.temp_path());
    // Also try to clean any unique temp files by pattern matching
    if let Some(parent) = delivery.payload_path.parent()
        && let Some(stem) = delivery.payload_path.file_name().and_then(|n| n.to_str())
        && let Ok(entries) = std::fs::read_dir(parent)
    {
        let pattern = format!("{}.tmp.", stem);
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str()
                && name.starts_with(&pattern)
            {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }

    // Also clean up any orphaned temp marker files from interrupted marker creation
    let done_temp = delivery.done_marker_path().with_extension("done.tmp");
    let proc_temp = delivery.proc_marker_path().with_extension("proc.tmp");
    let _ = std::fs::remove_file(done_temp);
    let _ = std::fs::remove_file(proc_temp);

    // fsync the directory to ensure all deletions are durable.
    // Without this, a power loss could "resurrect" deleted files.
    // See DESIGN.md: "All directory fsyncs are mandatory, not best-effort."
    fsync_dir(&delivery.spool_dir)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use tempfile::tempdir;

    /// Generate valid delivery IDs (UUID-like format).
    fn arb_delivery_id() -> impl Strategy<Value = DeliveryId> {
        "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}".prop_map(DeliveryId::new)
    }

    /// Generate arbitrary payload bytes.
    fn arb_payload() -> impl Strategy<Value = Vec<u8>> {
        prop::collection::vec(any::<u8>(), 0..1000)
    }

    /// Generate arbitrary GitHub event types.
    fn arb_event_type() -> impl Strategy<Value = String> {
        prop::sample::select(vec![
            "pull_request",
            "push",
            "check_suite",
            "check_run",
            "status",
            "issue_comment",
            "pull_request_review",
            "pull_request_review_comment",
        ])
        .prop_map(|s| s.to_string())
    }

    /// Generate arbitrary JSON values for webhook bodies.
    fn arb_json_value() -> impl Strategy<Value = serde_json::Value> {
        // Generate simple JSON objects that look like webhook payloads
        (
            "[a-z_]{3,10}",        // action
            0u64..100000,          // id
            "[a-zA-Z0-9_-]{3,20}", // sender login
        )
            .prop_map(|(action, id, sender)| {
                serde_json::json!({
                    "action": action,
                    "sender": {
                        "id": id,
                        "login": sender
                    }
                })
            })
    }

    /// Generate arbitrary HTTP headers for webhook envelopes.
    fn arb_headers() -> impl Strategy<Value = std::collections::HashMap<String, String>> {
        prop::collection::hash_map(
            "[A-Za-z][A-Za-z0-9-]{2,20}", // Header names like X-GitHub-Event
            "[a-zA-Z0-9/_=.-]{1,50}",     // Header values
            0..5,
        )
    }

    /// Generate arbitrary webhook envelopes.
    fn arb_webhook_envelope() -> impl Strategy<Value = WebhookEnvelope> {
        (arb_event_type(), arb_headers(), arb_json_value()).prop_map(
            |(event_type, headers, body)| WebhookEnvelope {
                event_type,
                headers,
                body,
            },
        )
    }

    proptest! {
        /// Webhook envelope roundtrip: spool_webhook + read_webhook preserves data.
        ///
        /// This test verifies the structured envelope API correctly preserves
        /// event type, headers, and body through serialization/deserialization.
        #[test]
        fn webhook_envelope_roundtrip(
            delivery_id in arb_delivery_id(),
            envelope in arb_webhook_envelope(),
        ) {
            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            // Spool using structured envelope API
            let delivery = spool_webhook(spool_dir, &delivery_id, &envelope).unwrap();

            // Read back using structured envelope API
            let read_envelope = delivery.read_webhook().unwrap();

            // Event type, headers, and body must be preserved exactly
            prop_assert_eq!(envelope.event_type, read_envelope.event_type,
                "Event type must survive roundtrip");
            prop_assert_eq!(envelope.headers, read_envelope.headers,
                "Headers must survive roundtrip");
            prop_assert_eq!(envelope.body, read_envelope.body,
                "Body must survive roundtrip");
        }

        /// Event type is required for crash-replay: spool_webhook rejects empty event types.
        ///
        /// This test verifies that spool_webhook enforces the invariant at the boundary:
        /// empty event_type is rejected with EmptyEventType error. This ensures that
        /// any successfully spooled webhook can be correctly replayed on recovery.
        #[test]
        fn webhook_envelope_rejects_empty_event_type(
            delivery_id in arb_delivery_id(),
            body in arb_json_value(),
        ) {
            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            // Empty event_type must be rejected
            let envelope = WebhookEnvelope {
                event_type: String::new(),
                headers: std::collections::HashMap::new(),
                body: body.clone(),
            };
            let result = spool_webhook(spool_dir, &delivery_id, &envelope);
            prop_assert!(matches!(result, Err(SpoolError::EmptyEventType)),
                "Empty event_type must be rejected at boundary");

            // Whitespace-only would NOT be caught by is_empty() - document this.
            // If stricter validation is needed, use .trim().is_empty() instead.
        }

        /// Non-empty event types are preserved through roundtrip.
        ///
        /// This complements webhook_envelope_rejects_empty_event_type by verifying
        /// that valid event types survive the roundtrip.
        #[test]
        fn webhook_envelope_preserves_event_type(
            delivery_id in arb_delivery_id(),
            envelope in arb_webhook_envelope(),
        ) {
            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            // The generator produces non-empty event types, so this should succeed
            let delivery = spool_webhook(spool_dir, &delivery_id, &envelope).unwrap();
            let read_envelope = delivery.read_webhook().unwrap();

            // Event type must be preserved exactly
            prop_assert_eq!(envelope.event_type, read_envelope.event_type,
                "Event type must be preserved through roundtrip");
        }
    }

    proptest! {
        /// Spooled delivery survives normal write sequence.
        #[test]
        fn spool_delivery_roundtrip(
            delivery_id in arb_delivery_id(),
            payload in arb_payload(),
        ) {
            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            let delivery = spool_delivery(spool_dir, &delivery_id, &payload).unwrap();

            // Payload should be readable
            let read_payload = delivery.read_payload_bytes().unwrap();
            prop_assert_eq!(payload, read_payload);

            // Delivery should be pending
            prop_assert!(delivery.is_pending());
            prop_assert!(!delivery.is_processing());
            prop_assert!(!delivery.is_done());
        }

        /// Duplicate delivery IDs are rejected atomically.
        ///
        /// This test verifies that:
        /// 1. The first spool succeeds
        /// 2. The second spool with the same ID fails with DuplicateDelivery
        /// 3. The first payload is NOT overwritten by the second attempt
        ///
        /// The atomic rejection uses hard_link() which fails with EEXIST if the
        /// target already exists, preventing race conditions in concurrent scenarios.
        #[test]
        fn duplicate_delivery_rejected(
            delivery_id in arb_delivery_id(),
            payload1 in arb_payload(),
            payload2 in arb_payload(),
        ) {
            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            // First spool succeeds
            let delivery = spool_delivery(spool_dir, &delivery_id, &payload1).unwrap();

            // Second spool with same ID fails
            let result = spool_delivery(spool_dir, &delivery_id, &payload2);
            prop_assert!(matches!(result, Err(SpoolError::DuplicateDelivery(_))));

            // CRITICAL: First payload is preserved, not overwritten
            let read_payload = delivery.read_payload_bytes().unwrap();
            prop_assert_eq!(payload1, read_payload, "First payload must be preserved");

            // Temp files from second attempt should be cleaned up
            // (we use unique temp paths like <id>.json.tmp.<pid>.<counter>)
            let temp_files: Vec<_> = std::fs::read_dir(spool_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.file_name()
                        .to_str()
                        .is_some_and(|n| n.contains(".tmp"))
                })
                .collect();
            prop_assert!(temp_files.is_empty(), "temp files should be cleaned up");
        }

        /// Processing marker is idempotent.
        #[test]
        fn mark_processing_idempotent(
            delivery_id in arb_delivery_id(),
            payload in arb_payload(),
        ) {
            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            let delivery = spool_delivery(spool_dir, &delivery_id, &payload).unwrap();

            // Mark processing multiple times
            mark_processing(&delivery).unwrap();
            prop_assert!(delivery.is_processing());

            mark_processing(&delivery).unwrap();
            prop_assert!(delivery.is_processing());

            // Should still be processable, not done
            prop_assert!(!delivery.is_done());
        }

        /// Done marker is idempotent.
        #[test]
        fn mark_done_idempotent(
            delivery_id in arb_delivery_id(),
            payload in arb_payload(),
        ) {
            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            let delivery = spool_delivery(spool_dir, &delivery_id, &payload).unwrap();

            // Mark done multiple times
            mark_done(&delivery).unwrap();
            prop_assert!(delivery.is_done());

            mark_done(&delivery).unwrap();
            prop_assert!(delivery.is_done());
        }

        /// State transitions work correctly.
        #[test]
        fn delivery_state_transitions(
            delivery_id in arb_delivery_id(),
            payload in arb_payload(),
        ) {
            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            let delivery = spool_delivery(spool_dir, &delivery_id, &payload).unwrap();

            // Initial state: pending
            prop_assert!(delivery.is_pending());
            prop_assert!(!delivery.is_processing());
            prop_assert!(!delivery.is_done());

            // Mark processing
            mark_processing(&delivery).unwrap();
            prop_assert!(!delivery.is_pending()); // NOT pending while processing (prevents double-processing)
            prop_assert!(delivery.is_processing());
            prop_assert!(!delivery.is_done());

            // Mark done
            mark_done(&delivery).unwrap();
            prop_assert!(!delivery.is_pending()); // No longer pending
            prop_assert!(!delivery.is_processing()); // No longer processing (done takes precedence)
            prop_assert!(delivery.is_done());
        }

        /// Remove delivery cleans up all files.
        #[test]
        fn remove_delivery_cleanup(
            delivery_id in arb_delivery_id(),
            payload in arb_payload(),
        ) {
            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            let delivery = spool_delivery(spool_dir, &delivery_id, &payload).unwrap();
            mark_processing(&delivery).unwrap();
            mark_done(&delivery).unwrap();

            // All files should exist
            prop_assert!(delivery.payload_path.exists());
            prop_assert!(delivery.proc_marker_path().exists());
            prop_assert!(delivery.done_marker_path().exists());

            // Remove
            remove_delivery(&delivery).unwrap();

            // All files should be gone
            prop_assert!(!delivery.payload_path.exists());
            prop_assert!(!delivery.proc_marker_path().exists());
            prop_assert!(!delivery.done_marker_path().exists());
        }

        // ─── Crash recovery property tests ───
        //
        // These tests verify the system recovers correctly from crashes at
        // any point in the write sequence.

        /// Crash during spool: only temp file exists.
        ///
        /// If a crash occurs after writing the temp file but before the atomic
        /// hard_link, only the temp file exists (pattern: <id>.json.tmp.<pid>.<counter>).
        /// On recovery:
        /// - The orphaned temp file is NOT picked up as a pending delivery
        /// - The delivery can be re-spooled (no duplicate error)
        #[test]
        fn crash_during_spool_temp_only(
            delivery_id in arb_delivery_id(),
            payload in arb_payload(),
        ) {
            let dir = tempdir().unwrap();
            let spool_dir = dir.path();
            std::fs::create_dir_all(spool_dir).unwrap();

            // Simulate crash: temp file exists but not payload.
            // Use unique_temp_path to create the actual temp file pattern that
            // spool_delivery would create during a real crash.
            let delivery = SpooledDelivery::new(spool_dir, delivery_id.clone());
            let orphaned_temp = unique_temp_path(&delivery.payload_path);
            std::fs::write(&orphaned_temp, &payload).unwrap();

            // Verify: temp file exists, payload doesn't
            prop_assert!(orphaned_temp.exists());
            prop_assert!(!delivery.payload_path.exists());

            // Recovery: delivery should NOT be pending (temp files are ignored)
            prop_assert!(!delivery.is_pending());

            // Re-spooling should succeed (no duplicate)
            let result = spool_delivery(spool_dir, &delivery_id, &payload);
            prop_assert!(result.is_ok());
        }

        /// Crash after spool complete: only payload file exists.
        ///
        /// If the spool completed successfully, only the .json file exists.
        /// On recovery, the delivery should be pending and processable.
        #[test]
        fn crash_after_spool_payload_only(
            delivery_id in arb_delivery_id(),
            payload in arb_payload(),
        ) {
            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            // Simulate: spool completed, then crash before any processing
            let delivery = spool_delivery(spool_dir, &delivery_id, &payload).unwrap();

            // Verify state
            prop_assert!(delivery.payload_path.exists());
            prop_assert!(!delivery.proc_marker_path().exists());
            prop_assert!(!delivery.done_marker_path().exists());

            // Recovery: should be pending
            prop_assert!(delivery.is_pending());
            prop_assert!(!delivery.is_processing());
            prop_assert!(!delivery.is_done());

            // Payload should be readable
            let read_payload = delivery.read_payload_bytes().unwrap();
            prop_assert_eq!(payload, read_payload);
        }

        /// Crash during processing: payload + proc marker exist.
        ///
        /// If a crash occurs during processing (after .proc marker created but
        /// before .done marker), the delivery needs to be reprocessed.
        #[test]
        fn crash_during_processing(
            delivery_id in arb_delivery_id(),
            payload in arb_payload(),
        ) {
            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            // Simulate: processing started, then crash
            let delivery = spool_delivery(spool_dir, &delivery_id, &payload).unwrap();
            mark_processing(&delivery).unwrap();

            // Verify state
            prop_assert!(delivery.payload_path.exists());
            prop_assert!(delivery.proc_marker_path().exists());
            prop_assert!(!delivery.done_marker_path().exists());

            // Recovery state (before cleanup_interrupted_processing):
            // - is_pending is FALSE (has proc marker, prevents double-processing)
            // - is_processing is TRUE (has proc, no done)
            // - is_done is FALSE
            //
            // The delivery remains in this state until cleanup_interrupted_processing()
            // is called at startup, which removes the proc marker and returns the
            // delivery to pending state. That behavior is tested in drain.rs tests.
            prop_assert!(!delivery.is_pending());
            prop_assert!(delivery.is_processing());
            prop_assert!(!delivery.is_done());
        }

        /// Crash after processing complete: all markers exist.
        ///
        /// If the processing completed (done marker created), the delivery
        /// should NOT be reprocessed on recovery.
        #[test]
        fn crash_after_processing_complete(
            delivery_id in arb_delivery_id(),
            payload in arb_payload(),
        ) {
            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            // Simulate: processing completed, then crash
            let delivery = spool_delivery(spool_dir, &delivery_id, &payload).unwrap();
            mark_processing(&delivery).unwrap();
            mark_done(&delivery).unwrap();

            // Verify state
            prop_assert!(delivery.payload_path.exists());
            prop_assert!(delivery.proc_marker_path().exists());
            prop_assert!(delivery.done_marker_path().exists());

            // Recovery: should be done, not pending
            prop_assert!(!delivery.is_pending());
            prop_assert!(!delivery.is_processing());
            prop_assert!(delivery.is_done());
        }

        /// Crash during marker creation: temp marker file exists.
        ///
        /// If a crash occurs during marker creation (after writing temp file but
        /// before the atomic rename), only the .done.tmp or .proc.tmp file exists.
        /// On recovery, the temp marker file is NOT treated as a valid marker.
        #[test]
        fn crash_during_marker_creation_temp_only(
            delivery_id in arb_delivery_id(),
            payload in arb_payload(),
        ) {
            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            // Spool a delivery
            let delivery = spool_delivery(spool_dir, &delivery_id, &payload).unwrap();

            // Simulate crash during mark_done: temp marker exists but not final marker
            let done_temp = delivery.done_marker_path().with_extension("done.tmp");
            std::fs::write(&done_temp, b"").unwrap();

            // The temp file should exist
            prop_assert!(done_temp.exists());
            // But the final marker should NOT exist
            prop_assert!(!delivery.done_marker_path().exists());

            // Delivery should still be pending (no done marker)
            prop_assert!(delivery.is_pending());
            prop_assert!(!delivery.is_done());

            // Now complete the marker creation normally
            mark_done(&delivery).unwrap();

            // Now done marker should exist
            prop_assert!(delivery.done_marker_path().exists());
            prop_assert!(delivery.is_done());
            prop_assert!(!delivery.is_pending());
        }

        /// Partial state: proc marker exists without payload.
        ///
        /// This is an edge case that shouldn't happen in practice (proc marker
        /// is only created after payload exists), but if files are manually
        /// deleted or corrupted, the system should handle it gracefully.
        ///
        /// The key property is that the delivery is NOT pending, so it won't
        /// be picked up by drain_pending and cause errors.
        #[test]
        fn orphaned_proc_marker_no_payload(
            delivery_id in arb_delivery_id(),
        ) {
            let dir = tempdir().unwrap();
            let spool_dir = dir.path();
            std::fs::create_dir_all(spool_dir).unwrap();

            // Simulate: somehow only proc marker exists (manual deletion of payload)
            let delivery = SpooledDelivery::new(spool_dir, delivery_id);
            std::fs::write(delivery.proc_marker_path(), b"").unwrap();

            // Key property: NOT pending (no payload), so won't be queued for processing
            prop_assert!(!delivery.is_pending());

            // Note: is_processing() returns TRUE here (proc exists, no done).
            // This is technically "correct" per its definition, but doesn't matter
            // because is_pending() is false and drain_pending() won't return it.
            prop_assert!(delivery.is_processing());
            prop_assert!(!delivery.is_done());
        }

        /// remove_delivery cleans up orphaned temp files from unique_temp_path.
        ///
        /// Property: If a crash occurred after unique_temp_path created a temp file
        /// but before hard_link completed, that orphaned temp file should be cleaned
        /// up when remove_delivery is called for that delivery ID.
        #[test]
        fn remove_delivery_cleans_orphaned_unique_temp_files(
            delivery_id in arb_delivery_id(),
            payload in arb_payload(),
        ) {
            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            // Successfully spool a delivery first
            let delivery = spool_delivery(spool_dir, &delivery_id, &payload).unwrap();

            // Simulate an orphaned temp file from a concurrent/crashed spool attempt.
            // This mimics what unique_temp_path actually produces.
            let orphaned_temp = unique_temp_path(&delivery.payload_path);
            std::fs::write(&orphaned_temp, b"orphaned temp data").unwrap();
            prop_assert!(orphaned_temp.exists(), "orphaned temp file should exist before cleanup");

            // remove_delivery should clean up the orphaned temp file
            remove_delivery(&delivery).unwrap();

            // The orphaned temp file should be gone
            prop_assert!(!orphaned_temp.exists(),
                "orphaned temp file at {:?} should be cleaned up by remove_delivery",
                orphaned_temp);
        }

        /// Partial state: done marker exists without payload.
        ///
        /// If the payload was deleted but done marker remains, the delivery
        /// should still be considered done (not pending).
        #[test]
        fn orphaned_done_marker_no_payload(
            delivery_id in arb_delivery_id(),
        ) {
            let dir = tempdir().unwrap();
            let spool_dir = dir.path();
            std::fs::create_dir_all(spool_dir).unwrap();

            // Simulate: somehow only done marker exists
            let delivery = SpooledDelivery::new(spool_dir, delivery_id.clone());
            std::fs::write(delivery.done_marker_path(), b"").unwrap();

            // Should be done (done marker exists)
            prop_assert!(delivery.is_done());
            // Should not be pending (done takes precedence, even without payload)
            prop_assert!(!delivery.is_pending());

            // Re-spooling should fail (done marker blocks it)
            let result = spool_delivery(spool_dir, &delivery_id, b"new payload");
            prop_assert!(matches!(result, Err(SpoolError::DuplicateDelivery(_))));
        }

        // ─── Crash-during-cleanup property tests ───
        //
        // These tests verify that a crash during remove_delivery does not
        // cause already-processed deliveries to be reprocessed on recovery.
        //
        // The FIX (payload deleted before markers) ensures that any crash during
        // remove_delivery leaves the system in a safe state:
        // - Crash before payload deletion: .done marker still exists → is_done() = true
        // - Crash after payload deletion: no payload → is_pending() = false
        //   (is_pending requires payload_path.exists())

        /// Crash during remove_delivery BEFORE payload deletion: delivery stays done.
        ///
        /// If remove_delivery crashes before deleting the payload, all files
        /// including the .done marker still exist → delivery is still done.
        #[test]
        fn crash_before_payload_deletion_stays_done(
            delivery_id in arb_delivery_id(),
            payload in arb_payload(),
        ) {
            use crate::spool::drain::cleanup_interrupted_processing;

            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            // Set up a completed delivery
            let delivery = spool_delivery(spool_dir, &delivery_id, &payload).unwrap();
            mark_processing(&delivery).unwrap();
            mark_done(&delivery).unwrap();

            // Simulate crash BEFORE remove_delivery does anything
            // All files still exist: payload, .proc, .done

            // Run recovery
            cleanup_interrupted_processing(spool_dir).unwrap();

            // Delivery is still done, not pending
            prop_assert!(delivery.is_done(), "Delivery should still be done");
            prop_assert!(!delivery.is_pending(), "Delivery should not be pending");
        }

        /// Crash during remove_delivery AFTER payload deletion: delivery not pending.
        ///
        /// The FIX ensures payload is deleted FIRST. If we crash after payload
        /// deletion but before marker deletion, the delivery cannot be pending
        /// because is_pending() requires payload_path.exists() to be true.
        #[test]
        fn crash_after_payload_deletion_not_pending(
            delivery_id in arb_delivery_id(),
            payload in arb_payload(),
        ) {
            use crate::spool::drain::cleanup_interrupted_processing;

            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            // Set up a completed delivery
            let delivery = spool_delivery(spool_dir, &delivery_id, &payload).unwrap();
            mark_processing(&delivery).unwrap();
            mark_done(&delivery).unwrap();

            // Simulate crash AFTER payload deletion but BEFORE marker deletion.
            // This is the state after step 1 of the FIXED remove_delivery:
            // - Payload: DELETED
            // - .done marker: still exists
            // - .proc marker: still exists
            std::fs::remove_file(&delivery.payload_path).ok();

            // Run recovery
            cleanup_interrupted_processing(spool_dir).unwrap();

            // CRITICAL: Not pending because payload doesn't exist
            prop_assert!(
                !delivery.is_pending(),
                "Delivery must not be pending after payload deletion. \
                 payload exists: {}, proc exists: {}, done exists: {}",
                delivery.payload_path.exists(),
                delivery.proc_marker_path().exists(),
                delivery.done_marker_path().exists()
            );

            // The .done marker still exists, so is_done() is true
            prop_assert!(delivery.is_done(), ".done marker should still exist");
        }

        /// Crash after payload and .done marker deletion: still not pending.
        ///
        /// Tests the state after both payload and .done are deleted but .proc remains.
        #[test]
        fn crash_after_payload_and_done_deletion_not_pending(
            delivery_id in arb_delivery_id(),
            payload in arb_payload(),
        ) {
            use crate::spool::drain::cleanup_interrupted_processing;

            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            // Set up a completed delivery
            let delivery = spool_delivery(spool_dir, &delivery_id, &payload).unwrap();
            mark_processing(&delivery).unwrap();
            mark_done(&delivery).unwrap();

            // Simulate crash after payload and .done deletion:
            // - Payload: DELETED
            // - .done marker: DELETED
            // - .proc marker: still exists
            std::fs::remove_file(&delivery.payload_path).ok();
            std::fs::remove_file(delivery.done_marker_path()).ok();

            // Run recovery
            cleanup_interrupted_processing(spool_dir).unwrap();

            // CRITICAL: Not pending because payload doesn't exist
            prop_assert!(
                !delivery.is_pending(),
                "Delivery must not be pending - no payload. \
                 payload exists: {}, proc exists: {}, done exists: {}",
                delivery.payload_path.exists(),
                delivery.proc_marker_path().exists(),
                delivery.done_marker_path().exists()
            );
        }

        /// CRITICAL: If payload deletion FAILS (not crashes), markers must NOT be deleted.
        ///
        /// Bug discovered by review: The code uses `let _ = remove_file(payload)` which
        /// ignores errors. If payload deletion fails (e.g., permissions, locked file),
        /// the code proceeds to delete markers anyway. This leaves:
        /// - Payload: EXISTS (deletion failed)
        /// - .done marker: DELETED
        /// - .proc marker: DELETED
        ///
        /// Result: is_pending() returns true → already-processed delivery gets reprocessed!
        ///
        /// Property: If payload deletion fails, remove_delivery must either:
        /// 1. Return an error and NOT delete any markers, OR
        /// 2. Not call remove_delivery at all (fail earlier)
        ///
        /// The safe invariant: markers are only deleted AFTER payload is confirmed deleted.
        #[test]
        #[cfg(unix)]
        fn remove_delivery_preserves_markers_if_payload_deletion_fails(
            delivery_id in arb_delivery_id(),
            payload in arb_payload(),
        ) {
            use std::os::unix::fs::PermissionsExt;

            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            // Set up a completed delivery
            let delivery = spool_delivery(spool_dir, &delivery_id, &payload).unwrap();
            mark_processing(&delivery).unwrap();
            mark_done(&delivery).unwrap();

            // Verify all files exist before the test
            prop_assert!(delivery.payload_path.exists(), "payload should exist");
            prop_assert!(delivery.done_marker_path().exists(), ".done should exist");
            prop_assert!(delivery.proc_marker_path().exists(), ".proc should exist");

            // Make directory read-only to prevent file deletion
            let original_perms = std::fs::metadata(spool_dir).unwrap().permissions();
            let mut readonly_perms = original_perms.clone();
            readonly_perms.set_mode(0o555); // r-xr-xr-x (no write)
            std::fs::set_permissions(spool_dir, readonly_perms).unwrap();

            // Attempt remove_delivery - payload deletion will fail due to permissions
            let result = remove_delivery(&delivery);

            // Restore permissions before assertions (so tempdir cleanup works)
            std::fs::set_permissions(spool_dir, original_perms).unwrap();

            // CRITICAL INVARIANT: If payload still exists, markers must also exist!
            // If payload deletion failed, we must NOT have deleted the markers.
            if delivery.payload_path.exists() {
                prop_assert!(
                    delivery.done_marker_path().exists(),
                    "BUG: Payload deletion failed but .done marker was deleted! \
                     This would cause is_pending() to return true for an already-processed \
                     delivery, leading to reprocessing. \
                     result={:?}",
                    result
                );
                prop_assert!(
                    delivery.proc_marker_path().exists(),
                    "BUG: Payload deletion failed but .proc marker was deleted! \
                     result={:?}",
                    result
                );
                // remove_delivery should have returned an error
                prop_assert!(
                    result.is_err(),
                    "remove_delivery should return error when payload deletion fails"
                );
            }
            // If payload was somehow deleted (shouldn't happen with readonly dir),
            // then marker deletion is fine.
        }

        /// Regression test: OLD buggy deletion order would cause reprocessing.
        ///
        /// This test documents the bug that existed before the fix: if markers
        /// were deleted before the payload, a crash could leave the system in
        /// a state where is_pending() returns true for an already-processed delivery.
        ///
        /// This scenario (markers gone, payload exists) should be impossible with
        /// the FIXED code, but could occur with:
        /// 1. Legacy data from before the fix
        /// 2. External tampering (manual deletion of markers)
        ///
        /// We document this as a known limitation - the system cannot distinguish
        /// between a legitimately pending delivery and one whose markers were
        /// externally deleted.
        #[test]
        fn legacy_buggy_state_documented(
            delivery_id in arb_delivery_id(),
            payload in arb_payload(),
        ) {
            use crate::spool::drain::cleanup_interrupted_processing;

            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            // Set up a completed delivery
            let delivery = spool_delivery(spool_dir, &delivery_id, &payload).unwrap();
            mark_processing(&delivery).unwrap();
            mark_done(&delivery).unwrap();

            // Simulate the LEGACY BUGGY state: markers deleted, payload remains.
            // This can ONLY happen with:
            // 1. Old code that deleted markers before payload
            // 2. External tampering
            std::fs::remove_file(delivery.done_marker_path()).ok();
            std::fs::remove_file(delivery.proc_marker_path()).ok();
            // payload still exists - this is the dangerous state

            // Run recovery
            cleanup_interrupted_processing(spool_dir).unwrap();

            // KNOWN LIMITATION: In this corrupted state, is_pending() returns true.
            // The system has no way to know this delivery was previously processed.
            // This test documents the limitation rather than asserting it's fixed.
            prop_assert!(
                delivery.is_pending(),
                "Expected: legacy corrupted state appears pending (known limitation). \
                 The FIX prevents this state from occurring with new code."
            );

            // The FIX prevents this state from occurring during normal operation.
            // For legacy data migration, operators should either:
            // 1. Re-run with old state cleared
            // 2. Accept potential reprocessing of in-flight deliveries during upgrade
        }
    }

    // ─── Unit tests ───

    #[test]
    fn spool_creates_directory_if_needed() {
        let dir = tempdir().unwrap();
        let spool_dir = dir.path().join("nested").join("spool");

        let delivery_id = DeliveryId::new("test-delivery-1");
        let payload = b"test payload";

        let delivery = spool_delivery(&spool_dir, &delivery_id, payload).unwrap();
        assert!(delivery.payload_path.exists());
    }

    #[test]
    fn spool_rejects_delivery_with_existing_done_marker() {
        let dir = tempdir().unwrap();
        let spool_dir = dir.path();

        let delivery_id = DeliveryId::new("test-delivery-2");
        let payload = b"test payload";

        // Spool and mark done
        let delivery = spool_delivery(spool_dir, &delivery_id, payload).unwrap();
        mark_done(&delivery).unwrap();

        // Remove the payload but keep the done marker
        std::fs::remove_file(&delivery.payload_path).unwrap();

        // Re-spooling should fail (done marker still exists)
        let result = spool_delivery(spool_dir, &delivery_id, payload);
        assert!(matches!(result, Err(SpoolError::DuplicateDelivery(_))));
    }

    #[test]
    fn temp_file_cleaned_up_on_success() {
        let dir = tempdir().unwrap();
        let spool_dir = dir.path();

        let delivery_id = DeliveryId::new("test-delivery-3");
        let payload = b"test payload";

        let delivery = spool_delivery(spool_dir, &delivery_id, payload).unwrap();

        // No temp files should exist after successful spool
        // (we use unique temp paths like <id>.json.tmp.<pid>.<counter>)
        let temp_files: Vec<_> = std::fs::read_dir(spool_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_str().is_some_and(|n| n.contains(".tmp")))
            .collect();
        assert!(temp_files.is_empty(), "temp files should be cleaned up");
        // But payload should exist
        assert!(delivery.payload_path.exists());
    }

    // ─── Path traversal prevention tests ───

    #[test]
    fn rejects_delivery_id_with_forward_slash() {
        let dir = tempdir().unwrap();
        let spool_dir = dir.path();

        let delivery_id = DeliveryId::new("../../../etc/passwd");
        let result = spool_delivery(spool_dir, &delivery_id, b"payload");
        assert!(matches!(result, Err(SpoolError::InvalidDeliveryId(_))));
    }

    #[test]
    fn rejects_delivery_id_with_backslash() {
        let dir = tempdir().unwrap();
        let spool_dir = dir.path();

        let delivery_id = DeliveryId::new("..\\..\\..\\windows\\system32");
        let result = spool_delivery(spool_dir, &delivery_id, b"payload");
        assert!(matches!(result, Err(SpoolError::InvalidDeliveryId(_))));
    }

    #[test]
    fn rejects_delivery_id_with_null_byte() {
        let dir = tempdir().unwrap();
        let spool_dir = dir.path();

        let delivery_id = DeliveryId::new("delivery\0id");
        let result = spool_delivery(spool_dir, &delivery_id, b"payload");
        assert!(matches!(result, Err(SpoolError::InvalidDeliveryId(_))));
    }

    #[test]
    fn rejects_empty_delivery_id() {
        let dir = tempdir().unwrap();
        let spool_dir = dir.path();

        let delivery_id = DeliveryId::new("");
        let result = spool_delivery(spool_dir, &delivery_id, b"payload");
        assert!(matches!(result, Err(SpoolError::InvalidDeliveryId(_))));
    }

    #[test]
    fn rejects_delivery_id_starting_with_dot() {
        let dir = tempdir().unwrap();
        let spool_dir = dir.path();

        // Hidden files could conflict with marker files or be invisible
        let delivery_id = DeliveryId::new(".hidden-delivery");
        let result = spool_delivery(spool_dir, &delivery_id, b"payload");
        assert!(matches!(result, Err(SpoolError::InvalidDeliveryId(_))));

        // Directory traversal attempts
        let delivery_id = DeliveryId::new(".");
        let result = spool_delivery(spool_dir, &delivery_id, b"payload");
        assert!(matches!(result, Err(SpoolError::InvalidDeliveryId(_))));

        let delivery_id = DeliveryId::new("..");
        let result = spool_delivery(spool_dir, &delivery_id, b"payload");
        assert!(matches!(result, Err(SpoolError::InvalidDeliveryId(_))));
    }

    #[test]
    fn rejects_absolute_path_delivery_id() {
        let dir = tempdir().unwrap();
        let spool_dir = dir.path();

        let delivery_id = DeliveryId::new("/etc/passwd");
        let result = spool_delivery(spool_dir, &delivery_id, b"payload");
        assert!(matches!(result, Err(SpoolError::InvalidDeliveryId(_))));
    }

    #[test]
    fn accepts_valid_uuid_delivery_id() {
        let dir = tempdir().unwrap();
        let spool_dir = dir.path();

        // GitHub delivery IDs are UUIDs
        let delivery_id = DeliveryId::new("550e8400-e29b-41d4-a716-446655440000");
        let result = spool_delivery(spool_dir, &delivery_id, b"payload");
        assert!(result.is_ok());
    }

    // ─── Path traversal property tests ───

    proptest! {
        /// Any delivery ID containing path separators is rejected.
        #[test]
        fn rejects_any_id_with_path_separators(
            prefix in "[a-zA-Z0-9-]{0,10}",
            suffix in "[a-zA-Z0-9-]{0,10}",
            separator in prop::sample::select(vec!['/', '\\']),
        ) {
            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            let malicious_id = format!("{}{}{}", prefix, separator, suffix);
            let delivery_id = DeliveryId::new(&malicious_id);
            let result = spool_delivery(spool_dir, &delivery_id, b"payload");
            prop_assert!(matches!(result, Err(SpoolError::InvalidDeliveryId(_))));
        }

        /// Any delivery ID starting with a dot is rejected.
        #[test]
        fn rejects_any_id_starting_with_dot(
            suffix in "[a-zA-Z0-9-]{0,20}",
        ) {
            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            let malicious_id = format!(".{}", suffix);
            let delivery_id = DeliveryId::new(&malicious_id);
            let result = spool_delivery(spool_dir, &delivery_id, b"payload");
            prop_assert!(matches!(result, Err(SpoolError::InvalidDeliveryId(_))));
        }

        /// Valid UUID-format delivery IDs are always accepted.
        #[test]
        fn accepts_all_valid_uuids(
            delivery_id in arb_delivery_id(),
            payload in arb_payload(),
        ) {
            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            let result = spool_delivery(spool_dir, &delivery_id, &payload);
            // Should succeed (not an InvalidDeliveryId error)
            prop_assert!(!matches!(result, Err(SpoolError::InvalidDeliveryId(_))));
        }

        /// Resulting file is always within spool_dir (path canonicalization check).
        #[test]
        fn payload_path_stays_within_spool_dir(
            delivery_id in arb_delivery_id(),
            payload in arb_payload(),
        ) {
            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            let delivery = spool_delivery(spool_dir, &delivery_id, &payload).unwrap();

            // The payload path must be a child of spool_dir
            prop_assert!(delivery.payload_path.starts_with(spool_dir));

            // And must not contain any .. components after canonicalization
            let canonical = delivery.payload_path.canonicalize().unwrap();
            let spool_canonical = spool_dir.canonicalize().unwrap();
            prop_assert!(canonical.starts_with(&spool_canonical));
        }
    }
}
