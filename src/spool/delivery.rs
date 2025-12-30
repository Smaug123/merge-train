//! Spooled delivery management with atomic file operations.
//!
//! Provides crash-safe spooling of webhook deliveries and marker file management.

use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use serde::Deserialize;
use thiserror::Error;

use crate::persistence::fsync::{fsync_dir, fsync_file};
use crate::types::DeliveryId;

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
}

/// Result type for spool operations.
pub type Result<T> = std::result::Result<T, SpoolError>;

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

    /// Checks if the delivery is pending (payload exists, no done marker).
    pub fn is_pending(&self) -> bool {
        self.payload_path.exists() && !self.done_marker_path().exists()
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
}

/// Spools a webhook delivery to disk atomically.
///
/// The delivery is written using the write-to-temp-then-rename pattern:
/// 1. Write to `<delivery-id>.json.tmp`
/// 2. fsync the temp file
/// 3. Rename to `<delivery-id>.json`
/// 4. fsync the directory
///
/// # Errors
///
/// Returns `SpoolError::DuplicateDelivery` if a delivery with the same ID already exists.
/// Returns `SpoolError::Io` for filesystem errors.
pub fn spool_delivery(
    spool_dir: &Path,
    delivery_id: &DeliveryId,
    payload: &[u8],
) -> Result<SpooledDelivery> {
    // Ensure spool directory exists
    std::fs::create_dir_all(spool_dir)?;

    let delivery = SpooledDelivery::new(spool_dir, delivery_id.clone());

    // Check for duplicate - reject if payload file already exists
    if delivery.payload_path.exists() {
        return Err(SpoolError::DuplicateDelivery(delivery_id.clone()));
    }

    // Also check for done marker (processed but not yet cleaned up)
    if delivery.done_marker_path().exists() {
        return Err(SpoolError::DuplicateDelivery(delivery_id.clone()));
    }

    // Write to temp file
    let temp_path = delivery.temp_path();
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_path)?;
        file.write_all(payload)?;
        fsync_file(&file)?;
    }

    // Atomic rename
    std::fs::rename(&temp_path, &delivery.payload_path)?;

    // fsync directory to ensure the rename is durable
    fsync_dir(spool_dir)?;

    Ok(delivery)
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

/// Creates an empty marker file atomically.
///
/// The marker is created by opening with O_CREAT | O_EXCL semantics where possible,
/// then fsyncing the directory to ensure durability.
fn create_marker_file(path: &Path, spool_dir: &Path) -> Result<()> {
    // If marker already exists, this is a no-op (idempotent)
    if path.exists() {
        return Ok(());
    }

    // Create empty marker file
    // Note: This isn't strictly atomic in the "exclusive create" sense,
    // but the marker being empty means any partial state is equivalent to complete.
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    // Empty file, no content to write
    drop(file);

    // fsync directory to ensure the marker is durable
    fsync_dir(spool_dir)?;

    Ok(())
}

/// Removes a spooled delivery and all its marker files.
///
/// This should be called after the grace period has passed for done deliveries.
pub fn remove_delivery(delivery: &SpooledDelivery) -> Result<()> {
    // Remove in reverse order: done marker, proc marker, payload
    // Ignore "not found" errors since partial cleanup is fine
    let _ = std::fs::remove_file(delivery.done_marker_path());
    let _ = std::fs::remove_file(delivery.proc_marker_path());
    let _ = std::fs::remove_file(&delivery.payload_path);
    let _ = std::fs::remove_file(delivery.temp_path());

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

        /// Duplicate delivery IDs are rejected.
        #[test]
        fn duplicate_delivery_rejected(
            delivery_id in arb_delivery_id(),
            payload1 in arb_payload(),
            payload2 in arb_payload(),
        ) {
            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            // First spool succeeds
            let _delivery = spool_delivery(spool_dir, &delivery_id, &payload1).unwrap();

            // Second spool with same ID fails
            let result = spool_delivery(spool_dir, &delivery_id, &payload2);
            prop_assert!(matches!(result, Err(SpoolError::DuplicateDelivery(_))));
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
            prop_assert!(delivery.is_pending()); // Still pending (not done)
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

        // Temp file should not exist after successful spool
        assert!(!delivery.temp_path().exists());
        // But payload should exist
        assert!(delivery.payload_path.exists());
    }
}
