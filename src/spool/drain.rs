//! Draining pending deliveries from the spool.
//!
//! On startup or during normal processing, the spool is drained to find
//! deliveries that need processing.

use std::path::Path;

use crate::types::DeliveryId;

use super::delivery::{Result, SpooledDelivery};

/// Drains the spool directory, returning all pending deliveries.
///
/// Returns all pending deliveries (`.json` files without `.done` markers)
/// in deterministic order (sorted by delivery ID) for reproducible behavior.
///
/// **Important**: This function does NOT clean up interrupted processing markers.
/// Call [`cleanup_interrupted_processing`] once at startup before starting any
/// workers, then use this function for subsequent drains during normal operation.
/// Calling cleanup while workers are active would delete their in-progress markers,
/// causing double-processing.
///
/// # Errors
///
/// Returns an error if the spool directory cannot be read.
pub fn drain_pending(spool_dir: &Path) -> Result<Vec<SpooledDelivery>> {
    // If spool directory doesn't exist, return empty (no pending deliveries)
    if !spool_dir.exists() {
        return Ok(Vec::new());
    }

    // Find all pending deliveries
    let mut pending = Vec::new();

    for entry in std::fs::read_dir(spool_dir)? {
        let entry = entry?;
        let path = entry.path();

        // Only look at .json files (payload files)
        if path.extension().is_some_and(|e| e == "json") {
            // Extract delivery ID from filename
            if let Some(delivery_id) = extract_delivery_id(&path) {
                let delivery = SpooledDelivery::new(spool_dir, delivery_id);

                // Include if pending (has payload, no done marker)
                if delivery.is_pending() {
                    pending.push(delivery);
                }
            }
        }
    }

    // Sort by delivery ID for deterministic order
    pending.sort_by(|a, b| a.delivery_id.as_str().cmp(b.delivery_id.as_str()));

    Ok(pending)
}

/// Cleans up interrupted processing by removing orphaned `.proc` markers.
///
/// If a crash occurred during processing, we'll have a `.proc` marker
/// but no `.done` marker. The delivery needs to be reprocessed, so we
/// remove the `.proc` marker to put it back in the pending state.
///
/// **Critical**: This function MUST only be called at startup, before any
/// workers begin processing deliveries. Calling it while workers are active
/// would delete their in-progress markers, causing the same delivery to be
/// picked up and processed again (double-processing).
///
/// # Safety Guarantee
///
/// If this function is called exactly once at startup before workers start:
/// - Any `.proc` without `.done` is from a previous crashed run
/// - Removing these `.proc` markers allows the deliveries to be reprocessed
/// - No race condition is possible because no workers are running yet
///
/// # Durability
///
/// After removing all orphaned `.proc` markers, this function fsyncs the
/// directory to ensure the deletions are durable. Without this, a power
/// loss could "resurrect" the deleted markers (see DESIGN.md: "All directory
/// fsyncs are mandatory, not best-effort").
pub fn cleanup_interrupted_processing(spool_dir: &Path) -> Result<()> {
    use crate::persistence::fsync::fsync_dir;

    // If spool directory doesn't exist, nothing to clean up
    if !spool_dir.exists() {
        return Ok(());
    }

    let mut removed_any = false;

    for entry in std::fs::read_dir(spool_dir)? {
        let entry = entry?;
        let path = entry.path();

        // Look for .proc files
        if path.extension().is_some_and(|e| e == "proc") {
            // Check if there's a corresponding .done marker
            let done_path = path.with_extension("done");
            if !done_path.exists() {
                // No .done marker means processing was interrupted
                // Remove the .proc marker so it can be reprocessed
                if std::fs::remove_file(&path).is_ok() {
                    removed_any = true;
                }
            }
        }
    }

    // fsync the directory to ensure all deletions are durable.
    // Only do this if we actually removed something to avoid unnecessary IO.
    if removed_any {
        fsync_dir(spool_dir)?;
    }

    Ok(())
}

/// Extracts the delivery ID from a payload file path.
///
/// The path should be `<spool_dir>/<delivery-id>.json`.
fn extract_delivery_id(path: &Path) -> Option<DeliveryId> {
    let file_name = path.file_stem()?.to_str()?;
    Some(DeliveryId::new(file_name))
}

/// Returns the number of pending deliveries in the spool.
///
/// This is a lightweight check that doesn't load payload data.
pub fn count_pending(spool_dir: &Path) -> Result<usize> {
    if !spool_dir.exists() {
        return Ok(0);
    }

    let mut count = 0;

    for entry in std::fs::read_dir(spool_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.extension().is_some_and(|e| e == "json")
            && let Some(delivery_id) = extract_delivery_id(&path)
        {
            let delivery = SpooledDelivery::new(spool_dir, delivery_id);
            if delivery.is_pending() {
                count += 1;
            }
        }
    }

    Ok(count)
}

/// Cleans up done deliveries that are older than the grace period.
///
/// A delivery is eligible for cleanup when:
/// 1. Its `.done` marker exists
/// 2. The `.done` marker is older than the grace period
///
/// This should be called periodically to prevent unbounded spool growth.
pub fn cleanup_done_deliveries(
    spool_dir: &Path,
    grace_period: std::time::Duration,
) -> Result<usize> {
    if !spool_dir.exists() {
        return Ok(0);
    }

    // Use checked_sub to handle potential underflow with very large grace periods
    // or extreme clock skew. If underflow would occur, use UNIX_EPOCH as cutoff
    // (effectively: nothing is old enough to clean up).
    let cutoff = std::time::SystemTime::now()
        .checked_sub(grace_period)
        .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
    let mut removed = 0;

    for entry in std::fs::read_dir(spool_dir)? {
        let entry = entry?;
        let path = entry.path();

        // Look for .done files
        if path.extension().is_some_and(|e| e == "done")
            && let Ok(metadata) = path.metadata()
            && let Ok(modified) = metadata.modified()
            && modified < cutoff
            && let Some(json_path) = path.file_stem()
            && let Some(delivery_id) = Path::new(json_path).file_stem()
            && let Some(id_str) = delivery_id.to_str()
        {
            // The path is <id>.json.done, we want <id>
            let delivery = SpooledDelivery::new(spool_dir, DeliveryId::new(id_str));
            super::delivery::remove_delivery(&delivery)?;
            removed += 1;
        }
    }

    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spool::delivery::{mark_done, mark_processing, spool_delivery};
    use proptest::prelude::*;
    use std::time::Duration;
    use tempfile::tempdir;

    fn arb_delivery_id() -> impl Strategy<Value = DeliveryId> {
        "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}".prop_map(DeliveryId::new)
    }

    fn arb_payload() -> impl Strategy<Value = Vec<u8>> {
        prop::collection::vec(any::<u8>(), 0..100)
    }

    proptest! {
        /// Drain returns all pending deliveries.
        #[test]
        fn drain_returns_pending(
            ids in prop::collection::vec(arb_delivery_id(), 1..10),
            payloads in prop::collection::vec(arb_payload(), 1..10),
        ) {
            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            // Ensure we have the same number of IDs and payloads
            let count = ids.len().min(payloads.len());
            let ids = &ids[..count];
            let payloads = &payloads[..count];

            // Dedupe IDs (in case proptest generates duplicates)
            let mut unique_ids: Vec<_> = ids.to_vec();
            unique_ids.sort_by(|a, b| a.as_str().cmp(b.as_str()));
            unique_ids.dedup_by(|a, b| a.as_str() == b.as_str());

            // Spool deliveries
            for (id, payload) in unique_ids.iter().zip(payloads.iter()) {
                let _ = spool_delivery(spool_dir, id, payload);
            }

            // Drain pending
            let pending = drain_pending(spool_dir).unwrap();

            // All spooled deliveries should be pending
            prop_assert_eq!(pending.len(), unique_ids.len());
        }

        /// Drain returns deliveries in deterministic order.
        #[test]
        fn drain_returns_deterministic_order(
            ids in prop::collection::vec(arb_delivery_id(), 2..5),
            payloads in prop::collection::vec(arb_payload(), 2..5),
        ) {
            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            let count = ids.len().min(payloads.len());
            let ids = &ids[..count];
            let payloads = &payloads[..count];

            // Dedupe
            let mut unique_ids: Vec<_> = ids.to_vec();
            unique_ids.sort_by(|a, b| a.as_str().cmp(b.as_str()));
            unique_ids.dedup_by(|a, b| a.as_str() == b.as_str());

            for (id, payload) in unique_ids.iter().zip(payloads.iter()) {
                let _ = spool_delivery(spool_dir, id, payload);
            }

            // Drain twice
            let pending1 = drain_pending(spool_dir).unwrap();
            let pending2 = drain_pending(spool_dir).unwrap();

            // Order should be identical
            prop_assert_eq!(pending1.len(), pending2.len());
            for (p1, p2) in pending1.iter().zip(pending2.iter()) {
                prop_assert_eq!(p1.delivery_id.as_str(), p2.delivery_id.as_str());
            }

            // Order should be sorted by delivery ID
            for window in pending1.windows(2) {
                prop_assert!(window[0].delivery_id.as_str() <= window[1].delivery_id.as_str());
            }
        }

        /// In-progress deliveries (with .proc marker) are never returned by drain.
        ///
        /// This is a critical safety property: without this, workers would double-process
        /// deliveries when drain_pending is called during normal operation.
        #[test]
        fn drain_never_returns_in_progress_deliveries(
            pending_ids in prop::collection::vec(arb_delivery_id(), 1..5),
            processing_ids in prop::collection::vec(arb_delivery_id(), 1..5),
            payload in arb_payload(),
        ) {
            use std::collections::HashSet;

            // Ensure no overlap between pending and processing IDs
            let pending_set: HashSet<_> = pending_ids.iter().map(|id| id.as_str()).collect();
            let processing_set: HashSet<_> = processing_ids.iter().map(|id| id.as_str()).collect();
            let overlap = pending_set.intersection(&processing_set).count();
            prop_assume!(overlap == 0);

            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            // Create pending deliveries (no .proc marker)
            for id in &pending_ids {
                let _ = spool_delivery(spool_dir, id, &payload);
            }

            // Create in-progress deliveries (with .proc marker)
            for id in &processing_ids {
                let delivery = spool_delivery(spool_dir, id, &payload).unwrap();
                mark_processing(&delivery).unwrap();
            }

            // Drain pending - should ONLY return pending, never in-progress
            let pending = drain_pending(spool_dir).unwrap();
            let returned_ids: HashSet<_> = pending.iter().map(|d| d.delivery_id.as_str()).collect();

            // All pending IDs should be returned
            for id in &pending_ids {
                prop_assert!(returned_ids.contains(id.as_str()),
                    "Pending delivery {} was not returned", id.as_str());
            }

            // NO in-progress IDs should be returned
            for id in &processing_ids {
                prop_assert!(!returned_ids.contains(id.as_str()),
                    "In-progress delivery {} was incorrectly returned", id.as_str());
            }
        }

        /// Done deliveries are not returned by drain.
        #[test]
        fn drain_excludes_done(
            id1 in arb_delivery_id(),
            id2 in arb_delivery_id(),
            payload in arb_payload(),
        ) {
            prop_assume!(id1.as_str() != id2.as_str());

            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            // Spool two deliveries
            let delivery1 = spool_delivery(spool_dir, &id1, &payload).unwrap();
            let _ = spool_delivery(spool_dir, &id2, &payload).unwrap();

            // Mark first as done
            mark_done(&delivery1).unwrap();

            // Drain should only return the second
            let pending = drain_pending(spool_dir).unwrap();
            prop_assert_eq!(pending.len(), 1);
            prop_assert_eq!(pending[0].delivery_id.as_str(), id2.as_str());
        }

        /// Interrupted processing is cleaned up by cleanup_interrupted_processing.
        #[test]
        fn cleanup_removes_interrupted_proc_markers(
            id in arb_delivery_id(),
            payload in arb_payload(),
        ) {
            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            // Spool and mark processing
            let delivery = spool_delivery(spool_dir, &id, &payload).unwrap();
            mark_processing(&delivery).unwrap();

            // Simulate crash: proc marker exists but not done
            prop_assert!(delivery.proc_marker_path().exists());
            prop_assert!(!delivery.done_marker_path().exists());

            // Call cleanup (simulating startup recovery)
            cleanup_interrupted_processing(spool_dir).unwrap();

            // Proc marker should be removed
            prop_assert!(!delivery.proc_marker_path().exists());

            // Now drain should return the delivery as pending
            let pending = drain_pending(spool_dir).unwrap();
            prop_assert_eq!(pending.len(), 1);
            prop_assert_eq!(pending[0].delivery_id.as_str(), id.as_str());
        }

        // ─── Integrated crash recovery tests ───
        //
        // These tests verify the full startup recovery sequence:
        // cleanup_interrupted_processing() followed by drain_pending()

        /// Startup recovery with mixed states.
        ///
        /// Simulates a crash leaving the spool in a mixed state with:
        /// - Orphaned temp files (crash during spool)
        /// - Pending deliveries (crash after spool)
        /// - Interrupted processing (crash during processing)
        /// - Completed deliveries (crash after done marker)
        #[test]
        fn startup_recovery_mixed_states(
            pending_ids in prop::collection::vec(arb_delivery_id(), 1..3),
            interrupted_ids in prop::collection::vec(arb_delivery_id(), 1..3),
            done_ids in prop::collection::vec(arb_delivery_id(), 1..3),
            temp_ids in prop::collection::vec(arb_delivery_id(), 1..3),
            payload in arb_payload(),
        ) {
            let dir = tempdir().unwrap();
            let spool_dir = dir.path();
            std::fs::create_dir_all(spool_dir).unwrap();

            // Dedupe all IDs
            use std::collections::HashSet;
            let all_ids: HashSet<_> = pending_ids.iter()
                .chain(interrupted_ids.iter())
                .chain(done_ids.iter())
                .chain(temp_ids.iter())
                .map(|id| id.as_str())
                .collect();

            // Skip if any duplicates (simplifies test logic)
            let total = pending_ids.len() + interrupted_ids.len() + done_ids.len() + temp_ids.len();
            prop_assume!(all_ids.len() == total);

            // Set up each state
            for id in &pending_ids {
                spool_delivery(spool_dir, id, &payload).unwrap();
            }

            for id in &interrupted_ids {
                let d = spool_delivery(spool_dir, id, &payload).unwrap();
                mark_processing(&d).unwrap();
            }

            for id in &done_ids {
                let d = spool_delivery(spool_dir, id, &payload).unwrap();
                mark_processing(&d).unwrap();
                mark_done(&d).unwrap();
            }

            for id in &temp_ids {
                use crate::spool::delivery::SpooledDelivery;
                let d = SpooledDelivery::new(spool_dir, id.clone());
                std::fs::write(d.temp_path(), &payload).unwrap();
            }

            // Run startup recovery
            cleanup_interrupted_processing(spool_dir).unwrap();
            let pending = drain_pending(spool_dir).unwrap();

            // Should return pending + interrupted (now cleared) deliveries
            let expected_count = pending_ids.len() + interrupted_ids.len();
            prop_assert_eq!(pending.len(), expected_count);

            // Verify correct IDs are pending
            let pending_set: HashSet<_> = pending.iter().map(|d| d.delivery_id.as_str()).collect();
            for id in &pending_ids {
                prop_assert!(pending_set.contains(id.as_str()));
            }
            for id in &interrupted_ids {
                prop_assert!(pending_set.contains(id.as_str()));
            }
            for id in &done_ids {
                prop_assert!(!pending_set.contains(id.as_str()));
            }
            for id in &temp_ids {
                prop_assert!(!pending_set.contains(id.as_str()));
            }
        }

        /// Repeated drain calls return consistent results.
        ///
        /// After startup recovery, subsequent drain calls should return the same
        /// pending deliveries (idempotent) until they are processed.
        #[test]
        fn drain_is_idempotent(
            ids in prop::collection::vec(arb_delivery_id(), 2..5),
            payloads in prop::collection::vec(arb_payload(), 2..5),
        ) {
            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            let count = ids.len().min(payloads.len());
            let ids = &ids[..count];
            let payloads = &payloads[..count];

            // Dedupe IDs
            let mut unique_ids: Vec<_> = ids.to_vec();
            unique_ids.sort_by(|a, b| a.as_str().cmp(b.as_str()));
            unique_ids.dedup_by(|a, b| a.as_str() == b.as_str());

            for (id, payload) in unique_ids.iter().zip(payloads.iter()) {
                let _ = spool_delivery(spool_dir, id, payload);
            }

            // Drain multiple times
            let drain1 = drain_pending(spool_dir).unwrap();
            let drain2 = drain_pending(spool_dir).unwrap();
            let drain3 = drain_pending(spool_dir).unwrap();

            // All should return the same IDs
            prop_assert_eq!(drain1.len(), drain2.len());
            prop_assert_eq!(drain2.len(), drain3.len());

            for ((d1, d2), d3) in drain1.iter().zip(drain2.iter()).zip(drain3.iter()) {
                prop_assert_eq!(d1.delivery_id.as_str(), d2.delivery_id.as_str());
                prop_assert_eq!(d2.delivery_id.as_str(), d3.delivery_id.as_str());
            }
        }
    }

    // ─── Unit tests ───

    #[test]
    fn drain_empty_spool() {
        let dir = tempdir().unwrap();
        let pending = drain_pending(dir.path()).unwrap();
        assert!(pending.is_empty());
    }

    #[test]
    fn drain_nonexistent_spool() {
        let dir = tempdir().unwrap();
        let spool_dir = dir.path().join("nonexistent");
        let pending = drain_pending(&spool_dir).unwrap();
        assert!(pending.is_empty());
    }

    #[test]
    fn count_pending_matches_drain() {
        let dir = tempdir().unwrap();
        let spool_dir = dir.path();

        // Spool some deliveries
        let id1 = DeliveryId::new("delivery-1");
        let id2 = DeliveryId::new("delivery-2");
        let id3 = DeliveryId::new("delivery-3");

        spool_delivery(spool_dir, &id1, b"p1").unwrap();
        spool_delivery(spool_dir, &id2, b"p2").unwrap();
        let delivery3 = spool_delivery(spool_dir, &id3, b"p3").unwrap();

        // Mark one as done
        mark_done(&delivery3).unwrap();

        // Count should match drain
        let count = count_pending(spool_dir).unwrap();
        let pending = drain_pending(spool_dir).unwrap();
        assert_eq!(count, pending.len());
        assert_eq!(count, 2);
    }

    #[test]
    fn cleanup_done_respects_grace_period() {
        let dir = tempdir().unwrap();
        let spool_dir = dir.path();

        let id = DeliveryId::new("delivery-to-clean");
        let delivery = spool_delivery(spool_dir, &id, b"payload").unwrap();
        mark_done(&delivery).unwrap();

        // With a very long grace period, nothing should be cleaned
        let removed = cleanup_done_deliveries(spool_dir, Duration::from_secs(3600)).unwrap();
        assert_eq!(removed, 0);
        assert!(delivery.payload_path.exists());

        // With a zero grace period, it should be cleaned
        let removed = cleanup_done_deliveries(spool_dir, Duration::ZERO).unwrap();
        assert_eq!(removed, 1);
        assert!(!delivery.payload_path.exists());
        assert!(!delivery.done_marker_path().exists());
    }

    #[test]
    fn extract_delivery_id_works() {
        let path = Path::new("/spool/abc-123.json");
        let id = extract_delivery_id(path).unwrap();
        assert_eq!(id.as_str(), "abc-123");
    }

    /// Temp files from interrupted spooling are ignored by drain.
    ///
    /// If a crash occurs during spool_delivery (after writing the temp file
    /// but before the atomic rename), the orphaned .json.tmp file should not
    /// be picked up as a pending delivery.
    #[test]
    fn drain_ignores_temp_files() {
        let dir = tempdir().unwrap();
        let spool_dir = dir.path();

        // Simulate an orphaned temp file from an interrupted spool operation
        std::fs::create_dir_all(spool_dir).unwrap();
        std::fs::write(
            spool_dir.join("orphan-delivery.json.tmp"),
            b"partial payload",
        )
        .unwrap();

        // Also add a real delivery to ensure drain still works
        let id = DeliveryId::new("real-delivery");
        spool_delivery(spool_dir, &id, b"real payload").unwrap();

        let pending = drain_pending(spool_dir).unwrap();

        // Only the real delivery should be returned, not the orphaned temp file
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].delivery_id.as_str(), "real-delivery");
    }

    /// Drain ignores non-delivery files in the spool directory.
    ///
    /// Random files that happen to be in the spool directory (e.g., from
    /// manual debugging or other tools) should not cause drain to fail.
    #[test]
    fn drain_ignores_unrelated_files() {
        let dir = tempdir().unwrap();
        let spool_dir = dir.path();
        std::fs::create_dir_all(spool_dir).unwrap();

        // Create various unrelated files
        std::fs::write(spool_dir.join("README.txt"), b"notes").unwrap();
        std::fs::write(spool_dir.join(".DS_Store"), b"macos junk").unwrap();
        std::fs::write(spool_dir.join("debug.log"), b"log data").unwrap();

        // Add a real delivery
        let id = DeliveryId::new("actual-delivery");
        spool_delivery(spool_dir, &id, b"payload").unwrap();

        let pending = drain_pending(spool_dir).unwrap();

        // Only the actual delivery should be returned
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].delivery_id.as_str(), "actual-delivery");
    }

    /// Drain handles marker files without corresponding payload.
    ///
    /// If for some reason marker files exist without a payload file
    /// (e.g., manual deletion of payload during debugging), drain should
    /// handle this gracefully.
    #[test]
    fn drain_handles_orphaned_markers() {
        let dir = tempdir().unwrap();
        let spool_dir = dir.path();
        std::fs::create_dir_all(spool_dir).unwrap();

        // Create orphaned marker files (no corresponding .json payload)
        std::fs::write(spool_dir.join("orphan.json.proc"), b"").unwrap();
        std::fs::write(spool_dir.join("another.json.done"), b"").unwrap();

        // Add a real delivery
        let id = DeliveryId::new("valid-delivery");
        spool_delivery(spool_dir, &id, b"payload").unwrap();

        // Call cleanup first (simulating startup)
        cleanup_interrupted_processing(spool_dir).unwrap();

        let pending = drain_pending(spool_dir).unwrap();

        // Only the valid delivery should be returned
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].delivery_id.as_str(), "valid-delivery");

        // The orphaned .proc marker should be cleaned up (no .done, so it's "interrupted")
        assert!(!spool_dir.join("orphan.json.proc").exists());
    }

    /// Drain does NOT return deliveries with proc markers (they're in-progress).
    ///
    /// This is important because drain may be called while workers are processing.
    /// If drain returned items with proc markers, it would cause double-processing.
    /// The proc markers are cleaned up by `cleanup_interrupted_processing()` at
    /// startup only, which makes those deliveries pending again.
    #[test]
    fn drain_excludes_in_progress_deliveries() {
        let dir = tempdir().unwrap();
        let spool_dir = dir.path();

        // Spool and mark processing (simulating an active worker)
        let id = DeliveryId::new("in-progress-delivery");
        let delivery = spool_delivery(spool_dir, &id, b"payload").unwrap();
        mark_processing(&delivery).unwrap();

        // Drain WITHOUT calling cleanup_interrupted_processing
        let pending = drain_pending(spool_dir).unwrap();

        // Delivery should NOT be returned (it has .proc marker = in-progress)
        assert_eq!(pending.len(), 0);

        // The proc marker should still exist (drain doesn't clean it up)
        assert!(delivery.proc_marker_path().exists());

        // After cleanup_interrupted_processing, it becomes pending again
        cleanup_interrupted_processing(spool_dir).unwrap();
        let pending = drain_pending(spool_dir).unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].delivery_id.as_str(), "in-progress-delivery");
    }
}
