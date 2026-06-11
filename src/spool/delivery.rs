//! Spooled delivery management with atomic file operations.
//!
//! Provides crash-safe spooling of webhook deliveries and marker file management.

use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::persistence::fsync::{fsync_dir, fsync_file};
use crate::types::DeliveryId;

/// Counter for generating unique temp file names within a process.
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Counter for ordering arrivals within a process (ties in wall-clock time).
static ARRIVAL_COUNTER: AtomicU64 = AtomicU64::new(0);

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

/// A monotonic arrival marker recorded when a webhook is accepted at intake.
///
/// Drains replay deliveries in `(arrival, delivery_id)` order, so this marker
/// determines replay order. GitHub delivery IDs are UUIDs and carry no ordering
/// information; without an arrival marker, replay order would be effectively
/// random.
///
/// The marker is a wall-clock timestamp (milliseconds since the Unix epoch)
/// plus a process-local atomic counter that breaks ties within a process.
/// Within a single process the ordering is strictly monotonic; across process
/// restarts it is as accurate as the system clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ArrivalMarker {
    /// Milliseconds since the Unix epoch at arrival time.
    unix_ms: u64,
    /// Process-local sequence number breaking ties within a process.
    seq: u64,
}

impl ArrivalMarker {
    /// Captures the current arrival marker.
    pub fn now() -> Self {
        let unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
            .unwrap_or(0);
        ArrivalMarker {
            unix_ms,
            seq: ARRIVAL_COUNTER.fetch_add(1, Ordering::Relaxed),
        }
    }

    /// Constructs a marker from raw parts. Test-only: production code must
    /// capture markers with [`ArrivalMarker::now`].
    #[cfg(test)]
    pub(crate) fn from_parts(unix_ms: u64, seq: u64) -> Self {
        ArrivalMarker { unix_ms, seq }
    }
}

/// Error returned when constructing an envelope with an empty event type.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid webhook envelope: event_type is empty")]
pub struct EmptyEventType;

/// A webhook envelope for crash-safe spooling.
///
/// Stores the *raw signed body bytes* plus the minimal request metadata needed
/// to replay the delivery:
///
/// - `event_type`: the `X-GitHub-Event` header, needed to parse the body at
///   drain time (via `crate::webhooks::parse_webhook`)
/// - `signature`: the `X-Hub-Signature-256` header, so the stored body can be
///   re-verified against the webhook secret at any time
/// - `body`: the exact bytes GitHub signed
/// - `arrival`: a monotonic arrival marker; drains replay in arrival order
///
/// (The delivery ID is the spool filename, not an envelope field. The `http`
/// crate lowercases header names, so the original header casing is not
/// available to store.)
///
/// # Body Representation Constraint
///
/// The body is stored as a JSON string, which requires it to be valid UTF-8.
/// GitHub webhook bodies are JSON and therefore UTF-8; intake rejects
/// non-UTF-8 bodies before spooling. For valid UTF-8, the
/// `String -> JSON string -> String` roundtrip through serde_json is
/// byte-exact, so `verify_signature(envelope.body().as_bytes(), ...)` against
/// the stored signature always reproduces the original verification.
///
/// # Invariants
///
/// `event_type` is non-empty. This is enforced by construction: the only ways
/// to obtain a `WebhookEnvelope` are [`WebhookEnvelope::new`] and
/// deserialization, both of which reject empty event types.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WebhookEnvelope {
    event_type: String,
    signature: String,
    body: String,
    arrival: ArrivalMarker,
}

impl WebhookEnvelope {
    /// Creates an envelope, rejecting empty event types.
    ///
    /// An empty event type would make the spooled delivery unreplayable: the
    /// event type selects the parser at drain time.
    pub fn new(
        event_type: impl Into<String>,
        signature: impl Into<String>,
        body: impl Into<String>,
        arrival: ArrivalMarker,
    ) -> std::result::Result<Self, EmptyEventType> {
        let event_type = event_type.into();
        if event_type.is_empty() {
            return Err(EmptyEventType);
        }
        Ok(WebhookEnvelope {
            event_type,
            signature: signature.into(),
            body: body.into(),
            arrival,
        })
    }

    /// The GitHub event type from the `X-GitHub-Event` header. Non-empty.
    pub fn event_type(&self) -> &str {
        &self.event_type
    }

    /// The `X-Hub-Signature-256` header value, re-verifiable against [`Self::body`].
    pub fn signature(&self) -> &str {
        &self.signature
    }

    /// The raw signed body bytes (valid UTF-8; see the type-level docs).
    pub fn body(&self) -> &str {
        &self.body
    }

    /// The arrival marker recorded at intake.
    pub fn arrival(&self) -> ArrivalMarker {
        self.arrival
    }
}

/// Wire format for [`WebhookEnvelope`]; deserialization funnels through
/// [`WebhookEnvelope::new`] so the non-empty `event_type` invariant also holds
/// for envelopes read back from disk.
#[derive(Deserialize)]
struct WebhookEnvelopeWire {
    event_type: String,
    signature: String,
    body: String,
    arrival: ArrivalMarker,
}

impl<'de> Deserialize<'de> for WebhookEnvelope {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = WebhookEnvelopeWire::deserialize(deserializer)?;
        WebhookEnvelope::new(wire.event_type, wire.signature, wire.body, wire.arrival)
            .map_err(serde::de::Error::custom)
    }
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

    /// A spooled payload exists but is not a valid [`WebhookEnvelope`].
    ///
    /// This is an uncharacterizable state (the spool only ever writes
    /// envelopes), so it is surfaced loudly rather than skipped.
    #[error("corrupt spool envelope for delivery {delivery_id}: {source}")]
    CorruptEnvelope {
        delivery_id: DeliveryId,
        #[source]
        source: serde_json::Error,
    },
}

/// Result type for spool operations.
pub type Result<T> = std::result::Result<T, SpoolError>;

/// Outcome of attempting to claim a delivery for processing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimOutcome {
    /// This caller created the `.proc` marker and owns the delivery.
    Claimed,
    /// Another worker already owns the delivery.
    AlreadyClaimed,
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

    /// Reads the raw payload bytes.
    pub fn read_payload_bytes(&self) -> Result<Vec<u8>> {
        Ok(std::fs::read(&self.payload_path)?)
    }

    /// Reads and deserializes the webhook envelope.
    ///
    /// # Errors
    ///
    /// Returns `SpoolError::Io` if the file cannot be read.
    /// Returns `SpoolError::CorruptEnvelope` if the payload is not a valid
    /// `WebhookEnvelope`.
    pub fn read_webhook(&self) -> Result<WebhookEnvelope> {
        let bytes = std::fs::read(&self.payload_path)?;
        serde_json::from_slice(&bytes).map_err(|source| SpoolError::CorruptEnvelope {
            delivery_id: self.delivery_id.clone(),
            source,
        })
    }
}

/// Spools a webhook delivery to disk atomically.
///
/// The delivery is written using the write-to-temp-then-link pattern:
/// 1. Write to `<delivery-id>.json.tmp.<pid>.<counter>`
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
/// Returns `SpoolError::DuplicateDelivery` if a delivery with the same ID already
/// exists. The spool directory is fsynced before this error is returned: callers
/// acknowledge duplicates (HTTP 202), so the surviving copy must be durable
/// before the acknowledgement. (The original spool attempt may have crashed
/// after its hard_link but before its directory fsync.)
///
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
            let _ = std::fs::remove_file(&temp_path);
            // The existing payload entry may not yet be durable: the original
            // spool attempt could have crashed between its hard_link and its
            // directory fsync. The caller will acknowledge this duplicate
            // (HTTP 202), so make the surviving entry durable first -
            // otherwise a power loss could erase a delivery GitHub believes
            // we accepted.
            fsync_dir(spool_dir)?;
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

/// Spools a webhook envelope to disk atomically.
///
/// This is the public API for spooling webhooks. The [`WebhookEnvelope`] is
/// correct by construction (non-empty event type, raw signed body), so any
/// successfully spooled delivery can be replayed on recovery.
///
/// # Errors
///
/// Returns `SpoolError::DuplicateDelivery` if a delivery with the same ID already
/// exists (durably, see [`spool_delivery`]).
/// Returns `SpoolError::Io` for filesystem errors.
/// Returns `SpoolError::Json` if serialization fails.
pub fn spool_webhook(
    spool_dir: &Path,
    delivery_id: &DeliveryId,
    envelope: &WebhookEnvelope,
) -> Result<SpooledDelivery> {
    let payload = serde_json::to_vec(envelope)?;
    spool_delivery(spool_dir, delivery_id, &payload)
}

/// Attempts to claim a delivery for processing by creating the `.proc` marker.
///
/// The claim is atomic: the marker is created with `O_CREAT | O_EXCL`
/// (`create_new`), so exactly one of any number of concurrent callers receives
/// [`ClaimOutcome::Claimed`]; the rest receive [`ClaimOutcome::AlreadyClaimed`]
/// and must not process the delivery.
///
/// # Durability
///
/// The marker and directory are fsynced after a successful claim. If power is
/// lost before the fsync completes, the marker may vanish - which is safe:
/// a payload without `.proc`/`.done` markers is simply pending again, and
/// `.proc` markers without `.done` are cleared at startup anyway (interrupted
/// processing is reprocessed).
///
/// # Errors
///
/// Returns `SpoolError::Io` for filesystem errors other than the marker
/// already existing.
pub fn mark_processing(delivery: &SpooledDelivery) -> Result<ClaimOutcome> {
    match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(delivery.proc_marker_path())
    {
        Ok(file) => {
            fsync_file(&file)?;
            fsync_dir(&delivery.spool_dir)?;
            Ok(ClaimOutcome::Claimed)
        }
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(ClaimOutcome::AlreadyClaimed),
        Err(e) => Err(e.into()),
    }
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
/// The dangerous state is *payload present with markers gone*: `is_pending()`
/// would return true and an already-processed delivery would be reprocessed.
/// POSIX gives no ordering guarantees for un-fsynced unlinks across power
/// loss, so simply unlinking payload-then-markers with one fsync at the end is
/// not enough: the marker unlinks could become durable while the payload
/// unlink does not.
///
/// Therefore the order is:
/// 1. **Unlink the payload.** Errors (other than NotFound) abort immediately,
///    leaving all markers in place.
/// 2. **fsync the directory.** The payload's disappearance is now durable; from
///    this point no power loss can resurrect it.
/// 3. **Unlink the markers** (and any orphaned temp files). Orphaned markers
///    are harmless if we crash here: a marker without a payload is never
///    pending.
/// 4. **fsync the directory** so the marker deletions are durable.
///
/// # Error Handling
///
/// If payload deletion fails (for any reason other than the file not existing),
/// this function returns an error WITHOUT deleting any markers. This prevents
/// a dangerous state where the payload exists but markers are deleted, which
/// would cause `is_pending()` to return true for an already-processed delivery.
pub fn remove_delivery(delivery: &SpooledDelivery) -> Result<()> {
    // Step 1: delete payload FIRST to prevent reprocessing on crash.
    //
    // We must NOT ignore errors here. If payload deletion fails but we proceed to
    // delete markers, is_pending() returns true for an already-processed delivery.
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

    // Step 2: make the payload's disappearance durable BEFORE touching markers.
    fsync_dir(&delivery.spool_dir)?;

    // Step 3: now safe to delete markers (orphaned markers are harmless).
    // We can ignore errors here because:
    // 1. Payload is durably deleted, so is_pending() will return false
    // 2. Orphaned markers just consume disk space, they don't affect correctness
    let _ = std::fs::remove_file(delivery.done_marker_path());
    let _ = std::fs::remove_file(delivery.proc_marker_path());

    // Clean up any orphaned temp files from interrupted spool operations
    // (pattern: <id>.json.tmp.<pid>.<counter>).
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

    // Step 4: fsync the directory so the marker deletions are durable.
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
        "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
            .prop_map(|s| DeliveryId::parse(s).unwrap())
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

    /// Generate arbitrary signature header values.
    fn arb_signature() -> impl Strategy<Value = String> {
        "[0-9a-f]{64}".prop_map(|hex| format!("sha256={}", hex))
    }

    /// Generate arbitrary raw JSON body text, including formatting that a
    /// re-serialization would not preserve.
    fn arb_raw_body() -> impl Strategy<Value = String> {
        (
            "[a-z_]{3,10}",        // action
            0u64..100000,          // id
            "[a-zA-Z0-9_-]{3,20}", // sender login
        )
            .prop_map(|(action, id, sender)| {
                // Deliberately odd whitespace and a duplicate key: the raw
                // bytes must survive spooling exactly.
                format!(
                    "{{ \"action\": \"{action}\",  \"action\": \"{action}\",\n  \"sender\": {{ \"id\": {id}, \"login\": \"{sender}\" }} }}"
                )
            })
    }

    /// Generate arbitrary webhook envelopes.
    fn arb_webhook_envelope() -> impl Strategy<Value = WebhookEnvelope> {
        (arb_event_type(), arb_signature(), arb_raw_body()).prop_map(
            |(event_type, signature, body)| {
                WebhookEnvelope::new(event_type, signature, body, ArrivalMarker::now()).unwrap()
            },
        )
    }

    proptest! {
        /// Webhook envelope roundtrip: spool_webhook + read_webhook preserves
        /// the event type, signature, raw body bytes, and arrival marker.
        #[test]
        fn webhook_envelope_roundtrip(
            delivery_id in arb_delivery_id(),
            envelope in arb_webhook_envelope(),
        ) {
            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            let delivery = spool_webhook(spool_dir, &delivery_id, &envelope).unwrap();
            let read_envelope = delivery.read_webhook().unwrap();

            prop_assert_eq!(envelope.event_type(), read_envelope.event_type(),
                "Event type must survive roundtrip");
            prop_assert_eq!(envelope.signature(), read_envelope.signature(),
                "Signature must survive roundtrip");
            prop_assert_eq!(envelope.body().as_bytes(), read_envelope.body().as_bytes(),
                "Body must survive roundtrip byte-exactly");
            prop_assert_eq!(envelope.arrival(), read_envelope.arrival(),
                "Arrival marker must survive roundtrip");
        }

        /// The stored signature can be re-verified against the stored body.
        ///
        /// This is the property that motivates storing raw bytes: signing the
        /// body, spooling it, reading it back, and re-verifying must succeed.
        #[test]
        fn stored_signature_reverifies_against_stored_body(
            delivery_id in arb_delivery_id(),
            event_type in arb_event_type(),
            body in arb_raw_body(),
            secret in prop::collection::vec(any::<u8>(), 1..64),
        ) {
            use crate::webhooks::{compute_signature, format_signature_header, verify_signature};

            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            let signature = format_signature_header(&compute_signature(body.as_bytes(), &secret));
            let envelope =
                WebhookEnvelope::new(event_type, signature, body, ArrivalMarker::now()).unwrap();

            let delivery = spool_webhook(spool_dir, &delivery_id, &envelope).unwrap();
            let read_envelope = delivery.read_webhook().unwrap();

            prop_assert!(
                verify_signature(read_envelope.body().as_bytes(), read_envelope.signature(), &secret),
                "stored signature must verify against stored body"
            );
        }

        /// Event type is required for crash-replay: the envelope constructor
        /// rejects empty event types, so unreplayable envelopes are
        /// unrepresentable.
        #[test]
        fn envelope_rejects_empty_event_type(
            signature in arb_signature(),
            body in arb_raw_body(),
        ) {
            let result = WebhookEnvelope::new("", signature, body, ArrivalMarker::now());
            prop_assert_eq!(result, Err(EmptyEventType));
        }

        /// Deserialization enforces the same invariant as the constructor:
        /// an on-disk envelope with an empty event type is rejected.
        #[test]
        fn envelope_deserialize_rejects_empty_event_type(
            signature in arb_signature(),
        ) {
            let json = serde_json::json!({
                "event_type": "",
                "signature": signature,
                "body": "{}",
                "arrival": { "unix_ms": 0, "seq": 0 },
            });
            let result: std::result::Result<WebhookEnvelope, _> =
                serde_json::from_value(json);
            prop_assert!(result.is_err());
        }

        /// Arrival markers captured later compare strictly greater within a process.
        #[test]
        fn arrival_markers_monotonic_within_process(_n in 0u8..10) {
            let a = ArrivalMarker::now();
            let b = ArrivalMarker::now();
            prop_assert!(a < b, "arrival markers must be strictly monotonic in-process");
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

            // First payload is preserved, not overwritten
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

        /// The `.proc` claim is exclusive: of two claim attempts, exactly the
        /// first wins; the second observes AlreadyClaimed.
        #[test]
        fn mark_processing_claim_is_exclusive(
            delivery_id in arb_delivery_id(),
            payload in arb_payload(),
        ) {
            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            let delivery = spool_delivery(spool_dir, &delivery_id, &payload).unwrap();

            prop_assert_eq!(mark_processing(&delivery).unwrap(), ClaimOutcome::Claimed);
            prop_assert!(delivery.is_processing());

            // A second claim (e.g. a racing drainer) must NOT succeed.
            prop_assert_eq!(mark_processing(&delivery).unwrap(), ClaimOutcome::AlreadyClaimed);
            prop_assert!(delivery.is_processing());
            prop_assert!(!delivery.is_done());
        }

        /// Concurrent claims from multiple threads: exactly one wins.
        #[test]
        fn mark_processing_concurrent_single_winner(
            delivery_id in arb_delivery_id(),
            payload in arb_payload(),
        ) {
            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            let delivery = spool_delivery(spool_dir, &delivery_id, &payload).unwrap();

            let claims: Vec<ClaimOutcome> = std::thread::scope(|scope| {
                let handles: Vec<_> = (0..4)
                    .map(|_| {
                        let delivery = &delivery;
                        scope.spawn(move || mark_processing(delivery).unwrap())
                    })
                    .collect();
                handles.into_iter().map(|h| h.join().unwrap()).collect()
            });

            let winners = claims
                .iter()
                .filter(|c| **c == ClaimOutcome::Claimed)
                .count();
            prop_assert_eq!(winners, 1, "exactly one concurrent claim must win: {:?}", claims);
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

            // Claim for processing
            prop_assert_eq!(mark_processing(&delivery).unwrap(), ClaimOutcome::Claimed);
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

        /// Crash during done-marker creation: temp marker file exists.
        ///
        /// If a crash occurs during mark_done (after writing temp file but
        /// before the atomic rename), only the .done.tmp file exists.
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
        // remove_delivery deletes the payload first and makes that deletion
        // durable (fsync) before touching markers, so any crash leaves a safe
        // state:
        // - Crash before payload deletion: .done marker still exists → is_done()
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

        /// The corrected removal ordering never yields a pending state.
        ///
        /// remove_delivery performs, in order: unlink payload, fsync dir,
        /// unlink done marker, unlink proc marker, fsync dir. We cannot
        /// simulate power loss in a unit test, but we CAN verify that every
        /// crash point in that unlink sequence leaves a state in which the
        /// delivery is not pending - i.e. the dangerous state (payload
        /// present, markers gone) never occurs at any step. The interleaved
        /// fsyncs only narrow the set of post-power-loss states towards these
        /// crash-point states; they cannot introduce new ones, because the
        /// payload unlink is made durable before any marker unlink begins.
        #[test]
        fn corrected_removal_ordering_never_yields_pending(
            delivery_id in arb_delivery_id(),
            payload in arb_payload(),
        ) {
            use crate::spool::drain::cleanup_interrupted_processing;

            // Replay the exact unlink sequence remove_delivery performs,
            // checking the invariant after each step.
            let unlink_sequence: [fn(&SpooledDelivery) -> PathBuf; 3] = [
                |d| d.payload_path.clone(),
                |d| d.done_marker_path(),
                |d| d.proc_marker_path(),
            ];

            for crash_after in 0..=unlink_sequence.len() {
                let dir = tempdir().unwrap();
                let spool_dir = dir.path();

                let delivery = spool_delivery(spool_dir, &delivery_id, &payload).unwrap();
                mark_processing(&delivery).unwrap();
                mark_done(&delivery).unwrap();

                // Perform the first `crash_after` unlinks, then "crash".
                for step in &unlink_sequence[..crash_after] {
                    std::fs::remove_file(step(&delivery)).unwrap();
                }

                // Run startup recovery as the restarted process would.
                cleanup_interrupted_processing(spool_dir).unwrap();

                prop_assert!(
                    !delivery.is_pending(),
                    "crash after {} unlinks must not yield a pending delivery \
                     (payload exists: {}, proc exists: {}, done exists: {})",
                    crash_after,
                    delivery.payload_path.exists(),
                    delivery.proc_marker_path().exists(),
                    delivery.done_marker_path().exists(),
                );
            }
        }

        /// If payload deletion fails (not crashes), markers must NOT be deleted.
        ///
        /// The code must not use `let _ = remove_file(payload)` which ignores errors.
        /// If payload deletion fails (e.g., permissions, locked file),
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

            // If payload still exists, markers must also exist.
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
    }

    // ─── Unit tests ───

    #[test]
    fn spool_creates_directory_if_needed() {
        let dir = tempdir().unwrap();
        let spool_dir = dir.path().join("nested").join("spool");

        let delivery_id = DeliveryId::parse("test-delivery-1").unwrap();
        let payload = b"test payload";

        let delivery = spool_delivery(&spool_dir, &delivery_id, payload).unwrap();
        assert!(delivery.payload_path.exists());
    }

    #[test]
    fn spool_rejects_delivery_with_existing_done_marker() {
        let dir = tempdir().unwrap();
        let spool_dir = dir.path();

        let delivery_id = DeliveryId::parse("test-delivery-2").unwrap();
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

        let delivery_id = DeliveryId::parse("test-delivery-3").unwrap();
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

    /// read_webhook on a payload that is not a valid envelope fails loudly
    /// with the delivery ID in the error.
    #[test]
    fn read_webhook_reports_corrupt_envelope() {
        let dir = tempdir().unwrap();
        let spool_dir = dir.path();

        let delivery_id = DeliveryId::parse("corrupt-delivery").unwrap();
        let delivery = spool_delivery(spool_dir, &delivery_id, b"not an envelope").unwrap();

        let result = delivery.read_webhook();
        match result {
            Err(SpoolError::CorruptEnvelope {
                delivery_id: id, ..
            }) => assert_eq!(id.as_str(), "corrupt-delivery"),
            other => panic!("expected CorruptEnvelope, got {:?}", other.map(|_| ())),
        }
    }
}
