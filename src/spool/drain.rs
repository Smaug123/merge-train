//! Draining pending deliveries from the spool.
//!
//! On startup or during normal processing, the spool is drained to find
//! deliveries that need processing.
//!
//! Deliveries are returned in `(arrival, delivery_id)` order: the arrival
//! marker recorded at intake, with the delivery ID breaking ties (which can
//! occur across process restarts). Event-priority ordering
//! (`crate::webhooks::priority`) is the engine's concern, applied after
//! parsing; the spool replays in arrival order only.

use std::path::Path;

use crate::types::DeliveryId;

use super::delivery::{Result, SpooledDelivery};

/// Drains the spool directory, returning all pending deliveries in
/// `(arrival, delivery_id)` order.
///
/// Reads each pending payload's envelope to obtain its arrival marker:
/// GitHub delivery IDs are UUIDs, so sorting by ID alone would replay in
/// effectively random order.
///
/// **Important**: This function does NOT clean up interrupted processing markers.
/// Call [`cleanup_interrupted_processing`] once at startup before starting any
/// workers, then use this function for subsequent drains during normal operation.
/// Calling cleanup while workers are active would delete their in-progress markers,
/// causing double-processing.
///
/// # Errors
///
/// Returns an error if the spool directory cannot be read, or
/// [`super::delivery::SpoolError::CorruptEnvelope`] if a pending payload is
/// not a valid envelope: the spool only ever writes envelopes, so a corrupt
/// payload is an uncharacterizable state and is surfaced rather than skipped.
pub fn drain_pending(spool_dir: &Path) -> Result<Vec<SpooledDelivery>> {
    // If spool directory doesn't exist, return empty (no pending deliveries)
    if !spool_dir.exists() {
        return Ok(Vec::new());
    }

    let mut pending = Vec::new();

    for entry in std::fs::read_dir(spool_dir)? {
        let entry = entry?;
        let path = entry.path();

        // Only look at .json files (payload files)
        if path.extension().is_some_and(|e| e == "json")
            && let Some(delivery_id) = extract_delivery_id(&path)
        {
            let delivery = SpooledDelivery::new(spool_dir, delivery_id);

            // Include if pending (has payload, no proc/done marker)
            if delivery.is_pending() {
                let arrival = delivery.read_webhook()?.arrival();
                pending.push((arrival, delivery));
            }
        }
    }

    pending.sort_by(|(arrival_a, a), (arrival_b, b)| {
        arrival_a
            .cmp(arrival_b)
            .then_with(|| a.delivery_id.as_str().cmp(b.delivery_id.as_str()))
    });

    Ok(pending.into_iter().map(|(_, delivery)| delivery).collect())
}

/// Cleans up interrupted work left by a crashed run: orphaned `.proc`
/// markers and orphaned spool temp files.
///
/// If a crash occurred during processing, we'll have a `.proc` marker
/// but no `.done` marker. The delivery needs to be reprocessed, so we
/// remove the `.proc` marker to put it back in the pending state.
///
/// If a crash occurred during spooling (between the temp-file write and the
/// hard_link), an orphaned `<id>.json.tmp.<pid>.<counter>` file remains; it
/// is removed here. This is only safe at startup: during normal operation a
/// temp file may belong to an intake write that is about to hard_link it.
///
/// **Critical**: This function MUST only be called at startup, before any
/// workers begin processing deliveries and before the server accepts
/// webhooks. Calling it while workers are active would delete their
/// in-progress markers, causing the same delivery to be picked up and
/// processed again (double-processing).
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
/// After removing files, this function fsyncs the directory to ensure the
/// deletions are durable. Without this, a power loss could "resurrect" the
/// deleted markers (see DESIGN.md: "All directory fsyncs are mandatory, not
/// best-effort").
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

        // An interrupted .proc marker (no corresponding .done) must be
        // removed so the delivery becomes pending again.
        let interrupted_proc =
            path.extension().is_some_and(|e| e == "proc") && !path.with_extension("done").exists();

        let orphaned_temp = path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(is_orphaned_spool_temp);

        if interrupted_proc || orphaned_temp {
            // We propagate errors here because a failed removal leaves the
            // .proc marker stuck, preventing the delivery from ever becoming
            // pending again. This is a critical failure that should not be
            // silently ignored.
            match std::fs::remove_file(&path) {
                Ok(()) => {
                    removed_any = true;
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // File was already removed (race with another cleanup),
                    // this is fine - the goal is achieved.
                }
                Err(e) => {
                    return Err(e.into());
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

/// True for spool temp files (`<stem>.json.tmp.<pid>.<counter>`).
///
/// Payload (`.json`) and marker (`.proc`/`.done`) files never match: the temp
/// suffix ends in numeric segments, and a delivery ID that itself contains
/// `.json.tmp.` still produces filenames ending in one of the excluded
/// suffixes.
fn is_orphaned_spool_temp(name: &str) -> bool {
    name.contains(".json.tmp.")
        && !name.ends_with(".json")
        && !name.ends_with(".proc")
        && !name.ends_with(".done")
}

/// Extracts the delivery ID from a payload file path (`<id>.json`).
///
/// Returns `None` for stems that are not valid delivery IDs: the spool only
/// creates payloads for validated IDs, so such files are foreign and are
/// ignored like any other unrelated file in the directory.
fn extract_delivery_id(path: &Path) -> Option<DeliveryId> {
    let file_name = path.file_stem()?.to_str()?;
    DeliveryId::parse(file_name).ok()
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
            && let Ok(delivery_id) = DeliveryId::parse(id_str)
        {
            // The path is <id>.json.done, we want <id>
            let delivery = SpooledDelivery::new(spool_dir, delivery_id);
            super::delivery::remove_delivery(&delivery)?;
            removed += 1;
        }
    }

    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spool::delivery::{
        ArrivalMarker, SpoolError, WebhookEnvelope, mark_done, mark_processing, spool_delivery,
        spool_webhook,
    };
    use proptest::prelude::*;
    use std::time::Duration;
    use tempfile::tempdir;

    fn arb_delivery_id() -> impl Strategy<Value = DeliveryId> {
        "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
            .prop_map(|s| DeliveryId::parse(s).unwrap())
    }

    /// Spools a minimal valid envelope with an explicit arrival marker.
    fn spool_envelope_with_arrival(
        spool_dir: &Path,
        id: &DeliveryId,
        arrival: ArrivalMarker,
    ) -> SpooledDelivery {
        let envelope = WebhookEnvelope::new(
            "pull_request",
            "sha256=0000",
            r#"{"action":"opened"}"#,
            arrival,
        )
        .unwrap();
        spool_webhook(spool_dir, id, &envelope).unwrap()
    }

    /// Spools a minimal valid envelope; arrival order follows call order.
    fn spool_test_envelope(spool_dir: &Path, id: &DeliveryId) -> SpooledDelivery {
        spool_envelope_with_arrival(spool_dir, id, ArrivalMarker::now())
    }

    /// Dedupes delivery IDs, preserving first-seen order.
    fn dedupe_preserving_order(ids: Vec<DeliveryId>) -> Vec<DeliveryId> {
        let mut seen = std::collections::HashSet::new();
        ids.into_iter()
            .filter(|id| seen.insert(id.as_str().to_string()))
            .collect()
    }

    proptest! {
        /// Drain returns all pending deliveries.
        #[test]
        fn drain_returns_pending(
            ids in prop::collection::vec(arb_delivery_id(), 1..10),
        ) {
            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            let unique_ids = dedupe_preserving_order(ids);
            for id in &unique_ids {
                spool_test_envelope(spool_dir, id);
            }

            let pending = drain_pending(spool_dir).unwrap();
            prop_assert_eq!(pending.len(), unique_ids.len());
        }

        /// Drain replays in arrival order, not delivery-ID order.
        ///
        /// GitHub delivery IDs are UUIDs: sorting by ID would replay in
        /// effectively random order. The arrival marker recorded at intake
        /// determines replay order.
        #[test]
        fn drain_orders_by_arrival_not_id(
            ids in prop::collection::vec(arb_delivery_id(), 2..6),
        ) {
            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            // Spool in generated order; ArrivalMarker::now() is monotonic
            // in-process, so arrival order == spool order.
            let unique_ids = dedupe_preserving_order(ids);
            for id in &unique_ids {
                spool_test_envelope(spool_dir, id);
            }

            let drained = drain_pending(spool_dir).unwrap();
            let drained_ids: Vec<&str> =
                drained.iter().map(|d| d.delivery_id.as_str()).collect();
            let spooled_ids: Vec<&str> = unique_ids.iter().map(|id| id.as_str()).collect();
            prop_assert_eq!(drained_ids, spooled_ids,
                "drain must replay in arrival order");
        }

        /// Repeated drains return the same order (deterministic).
        #[test]
        fn drain_returns_deterministic_order(
            ids in prop::collection::vec(arb_delivery_id(), 2..5),
        ) {
            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            let unique_ids = dedupe_preserving_order(ids);
            for id in &unique_ids {
                spool_test_envelope(spool_dir, id);
            }

            let pending1 = drain_pending(spool_dir).unwrap();
            let pending2 = drain_pending(spool_dir).unwrap();

            prop_assert_eq!(pending1.len(), pending2.len());
            for (p1, p2) in pending1.iter().zip(pending2.iter()) {
                prop_assert_eq!(p1.delivery_id.as_str(), p2.delivery_id.as_str());
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
        ) {
            use std::collections::HashSet;

            // Ensure no overlap between pending and processing IDs
            let pending_set: HashSet<_> = pending_ids.iter().map(|id| id.as_str()).collect();
            let processing_set: HashSet<_> = processing_ids.iter().map(|id| id.as_str()).collect();
            let overlap = pending_set.intersection(&processing_set).count();
            prop_assume!(overlap == 0);
            prop_assume!(pending_set.len() == pending_ids.len());
            prop_assume!(processing_set.len() == processing_ids.len());

            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            // Create pending deliveries (no .proc marker)
            for id in &pending_ids {
                spool_test_envelope(spool_dir, id);
            }

            // Create in-progress deliveries (with .proc marker)
            for id in &processing_ids {
                let delivery = spool_test_envelope(spool_dir, id);
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
        ) {
            prop_assume!(id1.as_str() != id2.as_str());

            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            // Spool two deliveries
            let delivery1 = spool_test_envelope(spool_dir, &id1);
            let _ = spool_test_envelope(spool_dir, &id2);

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
        ) {
            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            // Spool and mark processing
            let delivery = spool_test_envelope(spool_dir, &id);
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
        /// - Orphaned temp files (crash during spool) - removed by cleanup
        /// - Pending deliveries (crash after spool) - drained
        /// - Interrupted processing (crash during processing) - drained
        /// - Completed deliveries (crash after done marker) - not drained
        #[test]
        fn startup_recovery_mixed_states(
            pending_ids in prop::collection::vec(arb_delivery_id(), 1..3),
            interrupted_ids in prop::collection::vec(arb_delivery_id(), 1..3),
            done_ids in prop::collection::vec(arb_delivery_id(), 1..3),
            temp_ids in prop::collection::vec(arb_delivery_id(), 1..3),
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
                spool_test_envelope(spool_dir, id);
            }

            for id in &interrupted_ids {
                let d = spool_test_envelope(spool_dir, id);
                mark_processing(&d).unwrap();
            }

            for id in &done_ids {
                let d = spool_test_envelope(spool_dir, id);
                mark_processing(&d).unwrap();
                mark_done(&d).unwrap();
            }

            // Orphaned temp files use the real spool pattern:
            // <id>.json.tmp.<pid>.<counter>
            let mut temp_paths = Vec::new();
            for (i, id) in temp_ids.iter().enumerate() {
                let temp = spool_dir.join(format!("{}.json.tmp.4242.{}", id.as_str(), i));
                std::fs::write(&temp, b"partial payload").unwrap();
                temp_paths.push(temp);
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

            // Orphaned temp files are removed at startup
            for temp in &temp_paths {
                prop_assert!(!temp.exists(),
                    "orphaned temp file {:?} should be removed by startup cleanup", temp);
            }
        }

        /// Repeated drain calls return consistent results.
        ///
        /// After startup recovery, subsequent drain calls should return the same
        /// pending deliveries (idempotent) until they are processed.
        #[test]
        fn drain_is_idempotent(
            ids in prop::collection::vec(arb_delivery_id(), 2..5),
        ) {
            let dir = tempdir().unwrap();
            let spool_dir = dir.path();

            let unique_ids = dedupe_preserving_order(ids);
            for id in &unique_ids {
                spool_test_envelope(spool_dir, id);
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

    /// Equal arrival markers (possible across process restarts, where only
    /// the wall clock orders arrivals) break ties by delivery ID.
    #[test]
    fn drain_breaks_arrival_ties_by_delivery_id() {
        let dir = tempdir().unwrap();
        let spool_dir = dir.path();

        let tied = ArrivalMarker::from_parts(1_000, 0);
        let id_b = DeliveryId::parse("bbbbbbbb-0000-0000-0000-000000000000").unwrap();
        let id_a = DeliveryId::parse("aaaaaaaa-0000-0000-0000-000000000000").unwrap();

        // Spool b first: with tied arrivals, ID order must win regardless.
        spool_envelope_with_arrival(spool_dir, &id_b, tied);
        spool_envelope_with_arrival(spool_dir, &id_a, tied);

        let drained = drain_pending(spool_dir).unwrap();
        let drained_ids: Vec<&str> = drained.iter().map(|d| d.delivery_id.as_str()).collect();
        assert_eq!(drained_ids, vec![id_a.as_str(), id_b.as_str()]);
    }

    /// A pending payload that is not a valid envelope fails the drain loudly.
    ///
    /// The spool only ever writes envelopes, so a corrupt payload is an
    /// uncharacterizable state: surfacing it beats silently skipping (or
    /// reordering) a delivery GitHub believes we accepted.
    #[test]
    fn drain_fails_loudly_on_corrupt_envelope() {
        let dir = tempdir().unwrap();
        let spool_dir = dir.path();

        let id = DeliveryId::parse("corrupt-delivery").unwrap();
        spool_delivery(spool_dir, &id, b"not an envelope").unwrap();

        let result = drain_pending(spool_dir);
        assert!(
            matches!(result, Err(SpoolError::CorruptEnvelope { .. })),
            "expected CorruptEnvelope, got {:?}",
            result.map(|v| v.len())
        );
    }

    #[test]
    fn cleanup_done_respects_grace_period() {
        let dir = tempdir().unwrap();
        let spool_dir = dir.path();

        let id = DeliveryId::parse("delivery-to-clean").unwrap();
        let delivery = spool_test_envelope(spool_dir, &id);
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

    /// Files whose stem is not a valid delivery ID are foreign and ignored.
    #[test]
    fn extract_delivery_id_rejects_invalid_stem() {
        assert!(extract_delivery_id(Path::new("/spool/.hidden.json")).is_none());
    }

    /// Startup cleanup removes orphaned spool temp files.
    ///
    /// A crash between the temp-file write and the hard_link leaves an
    /// orphaned `<id>.json.tmp.<pid>.<counter>` file that nothing else
    /// reclaims (remove_delivery only sees temps for IDs it is removing).
    #[test]
    fn cleanup_removes_orphaned_spool_temp_files() {
        let dir = tempdir().unwrap();
        let spool_dir = dir.path();
        std::fs::create_dir_all(spool_dir).unwrap();

        let orphan = spool_dir.join("dead-beef.json.tmp.12345.0");
        std::fs::write(&orphan, b"partial payload").unwrap();

        let id = DeliveryId::parse("real-delivery").unwrap();
        let delivery = spool_test_envelope(spool_dir, &id);

        cleanup_interrupted_processing(spool_dir).unwrap();

        assert!(
            !orphan.exists(),
            "orphaned temp file should be removed at startup"
        );
        assert!(delivery.payload_path.exists(), "payload must survive");
    }

    /// A delivery ID may legally contain dots, so a payload filename can
    /// contain the temp pattern; cleanup must only remove true temp files.
    #[test]
    fn cleanup_spares_payload_whose_name_contains_temp_pattern() {
        let dir = tempdir().unwrap();
        let spool_dir = dir.path();

        // Payload file is "a.json.tmp.5.json": contains ".json.tmp." but is
        // a durably spooled delivery, not a temp file.
        let id = DeliveryId::parse("a.json.tmp.5").unwrap();
        let delivery = spool_test_envelope(spool_dir, &id);

        cleanup_interrupted_processing(spool_dir).unwrap();

        assert!(
            delivery.payload_path.exists(),
            "spooled payload must survive startup cleanup"
        );
        let pending = drain_pending(spool_dir).unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].delivery_id.as_str(), id.as_str());
    }

    /// Temp files from interrupted spooling are ignored by drain.
    ///
    /// If a crash occurs during spool_delivery (after writing the temp file
    /// but before the atomic hard_link), the orphaned temp file should not
    /// be picked up as a pending delivery.
    #[test]
    fn drain_ignores_temp_files() {
        let dir = tempdir().unwrap();
        let spool_dir = dir.path();

        // Simulate an orphaned temp file from an interrupted spool operation
        // (real pattern: <id>.json.tmp.<pid>.<counter>)
        std::fs::create_dir_all(spool_dir).unwrap();
        std::fs::write(
            spool_dir.join("orphan-delivery.json.tmp.999.7"),
            b"partial payload",
        )
        .unwrap();

        // Also add a real delivery to ensure drain still works
        let id = DeliveryId::parse("real-delivery").unwrap();
        spool_test_envelope(spool_dir, &id);

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
        let id = DeliveryId::parse("actual-delivery").unwrap();
        spool_test_envelope(spool_dir, &id);

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
        let id = DeliveryId::parse("valid-delivery").unwrap();
        spool_test_envelope(spool_dir, &id);

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
        let id = DeliveryId::parse("in-progress-delivery").unwrap();
        let delivery = spool_test_envelope(spool_dir, &id);
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
