//! Draining pending deliveries from the spool.
//!
//! On startup or during normal processing, the spool is drained to find
//! deliveries that need processing.

use std::path::Path;

use crate::types::DeliveryId;

use super::delivery::{Result, SpooledDelivery};

/// Drains the spool directory, returning all pending deliveries.
///
/// This function:
/// 1. Cleans up interrupted processing (`.proc` markers without `.done`)
/// 2. Returns all pending deliveries (`.json` files without `.done` markers)
///
/// Deliveries are returned in deterministic order (sorted by delivery ID)
/// for reproducible behavior.
///
/// # Errors
///
/// Returns an error if the spool directory cannot be read.
pub fn drain_pending(spool_dir: &Path) -> Result<Vec<SpooledDelivery>> {
    // If spool directory doesn't exist, return empty (no pending deliveries)
    if !spool_dir.exists() {
        return Ok(Vec::new());
    }

    // Step 1: Clean up interrupted processing
    // Delete .proc markers that don't have corresponding .done markers
    cleanup_interrupted_processing(spool_dir)?;

    // Step 2: Find all pending deliveries
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
fn cleanup_interrupted_processing(spool_dir: &Path) -> Result<()> {
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
                let _ = std::fs::remove_file(&path);
            }
        }
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

    let cutoff = std::time::SystemTime::now() - grace_period;
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
            let mut unique_ids: Vec<_> = ids.iter().cloned().collect();
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
            let mut unique_ids: Vec<_> = ids.iter().cloned().collect();
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

        /// Interrupted processing is cleaned up.
        #[test]
        fn drain_cleans_up_interrupted(
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

            // Drain should clean up the proc marker
            let pending = drain_pending(spool_dir).unwrap();

            // Delivery should be in pending list (ready for reprocessing)
            prop_assert_eq!(pending.len(), 1);
            prop_assert_eq!(pending[0].delivery_id.as_str(), id.as_str());

            // Proc marker should be removed
            prop_assert!(!delivery.proc_marker_path().exists());
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
}
