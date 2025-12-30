//! Append-only event log with crash-safe replay.
//!
//! The event log uses JSON Lines format: one JSON object per line.
//! This format is crash-safe because:
//! - Complete lines are always valid JSON
//! - Partial lines (from crash mid-write) are detected and truncated on replay
//!
//! # Recovery
//!
//! On startup:
//! 1. Load snapshot to get `log_position`
//! 2. Call `replay_from(path, log_position)` to replay events
//! 3. If final line is incomplete, it's truncated automatically
//!
//! # fsync Strategy
//!
//! - Critical events: `sync_all()` immediately after write
//! - Non-critical events: No fsync (caller batches)

use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use chrono::Utc;
use thiserror::Error;

use super::event::{StateEvent, StateEventPayload};
use super::fsync::{fsync_dir, fsync_file};

/// Errors that can occur during event log operations.
#[derive(Debug, Error)]
pub enum EventLogError {
    /// IO error during file operations.
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    /// JSON serialization/deserialization error.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Sequence number mismatch during replay.
    #[error("sequence mismatch: expected {expected}, got {got}")]
    SequenceMismatch { expected: u64, got: u64 },
}

/// Result type for event log operations.
pub type Result<T> = std::result::Result<T, EventLogError>;

/// An append-only event log.
///
/// Events are written in JSON Lines format (one JSON object per line).
/// The log tracks the next sequence number to assign.
pub struct EventLog {
    /// The underlying file handle, opened for append.
    file: File,
    /// Path to the log file.
    path: PathBuf,
    /// Next sequence number to assign.
    next_seq: u64,
}

impl EventLog {
    /// Opens an existing log file or creates a new one.
    ///
    /// If the file exists, it's opened for append. The `next_seq` is set to 0;
    /// call `replay_from` to determine the actual next sequence number.
    ///
    /// If the file doesn't exist, it's created and the parent directory is
    /// fsynced to ensure the new file survives a crash.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let is_new = !path.exists();

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;

        // Seek to EOF so that position() returns the correct offset.
        // Files opened with append(true) may have cursor at 0 until first write.
        file.seek(SeekFrom::End(0))?;

        // fsync the parent directory when creating a new file to ensure the
        // directory entry is durable. Without this, a crash could lose the file
        // even if its contents were fsynced.
        if is_new && let Some(parent) = path.parent() {
            fsync_dir(parent)?;
        }

        Ok(EventLog {
            file,
            path,
            next_seq: 0,
        })
    }

    /// Opens an existing log file or creates a new one, with a known next sequence number.
    ///
    /// Use this after calling `replay_from` to create a log ready for appending.
    ///
    /// If the file doesn't exist, it's created and the parent directory is
    /// fsynced to ensure the new file survives a crash.
    pub fn open_with_seq(path: impl AsRef<Path>, next_seq: u64) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let is_new = !path.exists();

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;

        // Seek to EOF so that position() returns the correct offset.
        // Files opened with append(true) may have cursor at 0 until first write.
        file.seek(SeekFrom::End(0))?;

        // fsync the parent directory when creating a new file to ensure the
        // directory entry is durable. Without this, a crash could lose the file
        // even if its contents were fsynced.
        if is_new && let Some(parent) = path.parent() {
            fsync_dir(parent)?;
        }

        Ok(EventLog {
            file,
            path,
            next_seq,
        })
    }

    /// Appends an event to the log.
    ///
    /// The event is assigned the next sequence number and the current timestamp.
    /// If the payload is critical, fsync is called immediately after writing.
    ///
    /// Returns the complete event that was written.
    pub fn append(&mut self, payload: StateEventPayload) -> Result<StateEvent> {
        let event = StateEvent {
            seq: self.next_seq,
            ts: Utc::now(),
            payload,
        };

        // Serialize to JSON and write with newline
        let json = serde_json::to_string(&event)?;
        writeln!(self.file, "{}", json)?;

        // fsync if critical
        if event.is_critical() {
            fsync_file(&self.file)?;
        }

        self.next_seq += 1;
        Ok(event)
    }

    /// Appends an event with explicit fsync control.
    ///
    /// Use this when you want to override the default fsync behavior,
    /// such as when batching non-critical events before a manual sync.
    pub fn append_with_sync(
        &mut self,
        payload: StateEventPayload,
        sync: bool,
    ) -> Result<StateEvent> {
        let event = StateEvent {
            seq: self.next_seq,
            ts: Utc::now(),
            payload,
        };

        // Serialize to JSON and write with newline
        let json = serde_json::to_string(&event)?;
        writeln!(self.file, "{}", json)?;

        if sync {
            fsync_file(&self.file)?;
        }

        self.next_seq += 1;
        Ok(event)
    }

    /// Forces fsync of the log file.
    ///
    /// Call this after batching multiple non-critical events.
    pub fn sync(&self) -> io::Result<()> {
        fsync_file(&self.file)
    }

    /// Returns the current byte position in the log file.
    ///
    /// This is used to record `log_position` in snapshots.
    pub fn position(&mut self) -> io::Result<u64> {
        self.file.stream_position()
    }

    /// Returns the next sequence number that will be assigned.
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// Returns the path to the log file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Replays events from a byte offset, truncating any partial line at EOF.
    ///
    /// This is a static method because it may need to truncate the file,
    /// which requires opening it in write mode.
    ///
    /// # Arguments
    ///
    /// * `offset` - Byte offset to start collecting events from. Must be line-aligned
    ///   (i.e., point to the start of a line, not the middle of a JSON record).
    ///   In practice, this should always come from [`EventLog::position`] which
    ///   returns the offset immediately after the last written newline.
    ///
    /// # Returns
    ///
    /// Returns `(events, next_seq)` where:
    /// - `events` is the list of valid events replayed from the offset
    /// - `next_seq` is the next sequence number to use (max seq in file + 1)
    ///
    /// If the file doesn't exist or is empty, returns `(vec![], 0)`.
    ///
    /// **Note**: `next_seq` is always based on the maximum sequence number in the
    /// entire file, not just the events replayed from the offset. This ensures
    /// correct recovery when `offset` is at or past EOF (e.g., when replaying
    /// from a snapshot's `log_position`).
    ///
    /// # Truncation
    ///
    /// If the final line doesn't parse as valid JSON (crash mid-write),
    /// the file is truncated at the start of that line. This ensures
    /// the log always contains a valid prefix of events.
    ///
    /// # Crash recovery
    ///
    /// This function handles several crash scenarios:
    /// - Partial JSON (crash mid-write): truncated
    /// - Valid JSON without trailing newline (crash between JSON and newline): newline appended
    /// - Invalid UTF-8 (crash mid-multibyte character): treated as corruption, truncated
    pub fn replay_from(path: impl AsRef<Path>, offset: u64) -> Result<(Vec<StateEvent>, u64)> {
        let path = path.as_ref();

        // Open for reading, treating NotFound as empty log but propagating other errors
        let file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return Ok((vec![], 0));
            }
            Err(e) => return Err(e.into()),
        };
        let file_len = file.metadata()?.len();

        // If file is empty, nothing to replay
        if file_len == 0 {
            return Ok((vec![], 0));
        }

        // Always scan from the beginning to find the true max_seq,
        // but only collect events from the requested offset.
        let mut reader = BufReader::new(file);

        let mut events = Vec::new();
        let mut last_valid_pos = 0u64;
        let mut current_pos = 0u64;
        let mut max_seq: Option<u64> = None;
        let mut last_line_had_newline = true;

        loop {
            // Read bytes until newline to handle invalid UTF-8 gracefully
            let mut line_bytes = Vec::new();
            let bytes_read = reader.read_until(b'\n', &mut line_bytes)?;

            if bytes_read == 0 {
                // EOF reached
                break;
            }

            let line_start = current_pos;
            current_pos += bytes_read as u64;

            // Check if line ends with newline
            let has_newline = line_bytes.last() == Some(&b'\n');

            // Try to convert to UTF-8
            let line = match std::str::from_utf8(&line_bytes) {
                Ok(s) => s,
                Err(_) => {
                    // Invalid UTF-8 - treat as corruption, truncate at line start
                    last_valid_pos = line_start;
                    break;
                }
            };

            // Skip empty lines
            let trimmed = line.trim();
            if trimmed.is_empty() {
                last_valid_pos = current_pos;
                last_line_had_newline = has_newline;
                continue;
            }

            // Try to parse as JSON
            match serde_json::from_str::<StateEvent>(trimmed) {
                Ok(event) => {
                    // Validate sequence number is monotonically increasing
                    if max_seq.is_some_and(|prev_max| event.seq <= prev_max) {
                        // Non-monotonic sequence - this is corruption
                        // Truncate at this line
                        break;
                    }
                    // Warn on gaps - doesn't require truncation (events are still valid),
                    // but indicates a logic bug in the writer
                    if let Some(prev) = max_seq
                        && event.seq != prev + 1
                    {
                        tracing::warn!(
                            prev,
                            current = event.seq,
                            path = %path.display(),
                            "sequence gap detected in event log - possible logic bug"
                        );
                    }
                    max_seq = Some(event.seq);
                    // Only collect events from the requested offset onwards
                    if line_start >= offset {
                        events.push(event);
                    }
                    last_valid_pos = current_pos;
                    last_line_had_newline = has_newline;
                }
                Err(_) => {
                    // Invalid JSON - this is the partial line from a crash
                    // Don't update last_valid_pos, and stop reading
                    // We'll truncate at line_start
                    last_valid_pos = line_start;
                    break;
                }
            }
        }

        // Determine if we need to modify the file
        let needs_truncation = last_valid_pos < file_len;
        let needs_newline = !needs_truncation && !last_line_had_newline && file_len > 0;

        if needs_truncation {
            let file = OpenOptions::new().write(true).open(path)?;
            file.set_len(last_valid_pos)?;
            fsync_file(&file)?;
        } else if needs_newline {
            // File ends with valid JSON but no newline - append newline to prevent
            // corruption when the next event is appended
            let mut file = OpenOptions::new().append(true).open(path)?;
            writeln!(file)?;
            fsync_file(&file)?;
        }

        let next_seq = max_seq.map(|s| s + 1).unwrap_or(0);
        Ok((events, next_seq))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::arb_state_event_payload;
    use crate::types::{CascadePhase, DescendantProgress, PrNumber, Sha};
    use proptest::prelude::*;
    use std::io::Write;
    use tempfile::tempdir;

    // ─── Basic functionality tests ───

    #[test]
    fn open_creates_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("events.log");

        assert!(!path.exists());
        let _log = EventLog::open(&path).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn append_writes_json_line() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("events.log");

        let mut log = EventLog::open(&path).unwrap();
        log.append(StateEventPayload::TrainStarted {
            root_pr: PrNumber(123),
            current_pr: PrNumber(123),
        })
        .unwrap();

        // Read the file and verify JSON
        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 1);

        let event: StateEvent = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(event.seq, 0);
        assert!(matches!(
            event.payload,
            StateEventPayload::TrainStarted { root_pr, .. } if root_pr == PrNumber(123)
        ));
    }

    #[test]
    fn sequence_numbers_increment() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("events.log");

        let mut log = EventLog::open(&path).unwrap();

        for i in 0..5 {
            let event = log
                .append(StateEventPayload::TrainStarted {
                    root_pr: PrNumber(i),
                    current_pr: PrNumber(i),
                })
                .unwrap();
            assert_eq!(event.seq, i);
        }

        assert_eq!(log.next_seq(), 5);
    }

    // ─── Property tests ───

    proptest! {
        /// Write N events, replay yields exactly N events.
        #[test]
        fn roundtrip_n_events(payloads in prop::collection::vec(arb_state_event_payload(), 1..20)) {
            let dir = tempdir().unwrap();
            let path = dir.path().join("events.log");

            // Write events
            let mut log = EventLog::open(&path).unwrap();
            let mut written_events = Vec::new();
            for payload in &payloads {
                let event = log.append(payload.clone()).unwrap();
                written_events.push(event);
            }
            drop(log); // Close file

            // Replay and verify
            let (replayed, next_seq) = EventLog::replay_from(&path, 0).unwrap();

            prop_assert_eq!(replayed.len(), payloads.len());
            prop_assert_eq!(next_seq, payloads.len() as u64);

            for (written, replayed) in written_events.iter().zip(replayed.iter()) {
                prop_assert_eq!(written.seq, replayed.seq);
                prop_assert_eq!(&written.payload, &replayed.payload);
            }
        }

        /// Offset-based replay returns only events after offset.
        #[test]
        fn offset_replay(payloads in prop::collection::vec(arb_state_event_payload(), 3..10)) {
            let dir = tempdir().unwrap();
            let path = dir.path().join("events.log");

            // Write events, recording position after each
            let mut log = EventLog::open(&path).unwrap();
            let mut positions = vec![0u64]; // Position before first event

            for payload in &payloads {
                log.append(payload.clone()).unwrap();
                positions.push(log.position().unwrap());
            }
            drop(log);

            // Pick a random split point
            let split = payloads.len() / 2;
            let offset = positions[split];

            // Replay from offset
            let (replayed, next_seq) = EventLog::replay_from(&path, offset).unwrap();

            prop_assert_eq!(replayed.len(), payloads.len() - split);
            // next_seq should be max_seq + 1, which equals total event count
            prop_assert_eq!(next_seq, payloads.len() as u64);
            for (i, event) in replayed.iter().enumerate() {
                prop_assert_eq!(event.seq, (split + i) as u64);
            }
        }

        /// next_seq is invariant with respect to offset (always equals max_seq_in_file + 1).
        ///
        /// This property catches bugs where replay_from returns wrong next_seq
        /// when called with offset at/past EOF, which would break recovery.
        #[test]
        fn next_seq_invariant_of_offset(
            payloads in prop::collection::vec(arb_state_event_payload(), 1..10),
            // offset_multiplier: 0.0 = start, 1.0 = EOF, >1.0 = past EOF
            offset_multiplier in 0.0f64..2.0
        ) {
            let dir = tempdir().unwrap();
            let path = dir.path().join("events.log");

            // Write events
            let mut log = EventLog::open(&path).unwrap();
            for payload in &payloads {
                log.append(payload.clone()).unwrap();
            }
            let file_len = log.position().unwrap();
            drop(log);

            // Calculate offset (can be past EOF when multiplier > 1.0)
            let offset = (file_len as f64 * offset_multiplier) as u64;

            // Replay from arbitrary offset
            let (_, next_seq) = EventLog::replay_from(&path, offset).unwrap();

            // next_seq must always be correct regardless of offset
            prop_assert_eq!(
                next_seq,
                payloads.len() as u64,
                "next_seq should be {} for offset {} (file_len={})",
                payloads.len(),
                offset,
                file_len
            );
        }

        /// Partial line at EOF is truncated.
        #[test]
        fn partial_line_recovery(payloads in prop::collection::vec(arb_state_event_payload(), 1..10)) {
            let dir = tempdir().unwrap();
            let path = dir.path().join("events.log");

            // Write complete events
            let mut log = EventLog::open(&path).unwrap();
            for payload in &payloads {
                log.append(payload.clone()).unwrap();
            }
            let valid_len = log.position().unwrap();
            drop(log);

            // Append partial JSON (simulating crash mid-write)
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            write!(file, r#"{{"seq":999,"ts":"2024-01-01T00:00:00Z","ty"#).unwrap();
            drop(file);

            // Verify file is longer now
            let file_len = std::fs::metadata(&path).unwrap().len();
            prop_assert!(file_len > valid_len);

            // Replay should recover only complete events
            let (replayed, next_seq) = EventLog::replay_from(&path, 0).unwrap();
            prop_assert_eq!(replayed.len(), payloads.len());
            prop_assert_eq!(next_seq, payloads.len() as u64);

            // File should be truncated
            let new_len = std::fs::metadata(&path).unwrap().len();
            prop_assert_eq!(new_len, valid_len);
        }

        /// Crash at random byte position recovers valid prefix.
        #[test]
        fn crash_simulation(
            payloads in prop::collection::vec(arb_state_event_payload(), 2..10),
            truncate_ratio in 0.1f64..0.99
        ) {
            let dir = tempdir().unwrap();
            let path = dir.path().join("events.log");

            // Write events
            let mut log = EventLog::open(&path).unwrap();
            for payload in &payloads {
                log.append(payload.clone()).unwrap();
            }
            log.sync().unwrap();
            drop(log);

            // Get file size and truncate at random position
            let file_len = std::fs::metadata(&path).unwrap().len();
            let truncate_pos = (file_len as f64 * truncate_ratio) as u64;

            {
                let file = OpenOptions::new().write(true).open(&path).unwrap();
                file.set_len(truncate_pos).unwrap();
            }

            // Replay should not panic and recover valid prefix
            let result = EventLog::replay_from(&path, 0);
            prop_assert!(result.is_ok(), "replay_from should not panic");

            let (replayed, next_seq) = result.unwrap();

            // Should have recovered some events (maybe 0 if truncated very early)
            prop_assert!(replayed.len() <= payloads.len());

            // Sequence numbers should be valid and payloads should match originals
            // (i.e., recovered events are a true prefix of what was written)
            if !replayed.is_empty() {
                for (i, event) in replayed.iter().enumerate() {
                    prop_assert_eq!(event.seq, i as u64);
                    prop_assert_eq!(&event.payload, &payloads[i],
                        "Recovered event {} has wrong payload", i);
                }
                prop_assert_eq!(next_seq, replayed.len() as u64);
            } else {
                prop_assert_eq!(next_seq, 0);
            }
        }
    }

    // ─── Critical event tests ───
    //
    // Note: These tests verify that critical events are written and can be read back.
    // They do NOT validate that fsync actually persists to disk (which would require
    // simulating power loss). The fsync calls provide durability guarantees from the
    // OS, but we can only test the write-read path here.

    #[test]
    fn critical_events_written_to_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("events.log");

        let mut log = EventLog::open(&path).unwrap();

        // Write a critical event
        log.append(StateEventPayload::TrainStarted {
            root_pr: PrNumber(1),
            current_pr: PrNumber(1),
        })
        .unwrap();

        // The file should be readable (verifies write succeeded, not durability)
        let (events, _) = EventLog::replay_from(&path, 0).unwrap();
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn non_critical_events_with_manual_sync() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("events.log");

        let mut log = EventLog::open(&path).unwrap();

        // Write non-critical events
        for i in 0..10 {
            log.append(StateEventPayload::PredecessorDeclared {
                pr: PrNumber(i),
                predecessor: PrNumber(i + 1),
            })
            .unwrap();
        }

        // Manual sync
        log.sync().unwrap();

        // Verify all events are present
        let (events, _) = EventLog::replay_from(&path, 0).unwrap();
        assert_eq!(events.len(), 10);
    }

    // ─── Edge cases ───

    #[test]
    fn replay_empty_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("events.log");

        // Create empty file
        File::create(&path).unwrap();

        let (events, next_seq) = EventLog::replay_from(&path, 0).unwrap();
        assert!(events.is_empty());
        assert_eq!(next_seq, 0);
    }

    #[test]
    fn replay_nonexistent_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nonexistent.log");

        let (events, next_seq) = EventLog::replay_from(&path, 0).unwrap();
        assert!(events.is_empty());
        assert_eq!(next_seq, 0);
    }

    #[test]
    fn replay_with_offset_past_eof() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("events.log");

        let mut log = EventLog::open(&path).unwrap();
        log.append(StateEventPayload::TrainStarted {
            root_pr: PrNumber(1),
            current_pr: PrNumber(1),
        })
        .unwrap();
        drop(log);

        // Replay from past EOF - should still return correct next_seq
        // based on existing events in the file (seq 0 exists, so next_seq = 1)
        let (events, next_seq) = EventLog::replay_from(&path, 10000).unwrap();
        assert!(events.is_empty());
        assert_eq!(
            next_seq, 1,
            "next_seq should be 1 (one event with seq=0 exists)"
        );
    }

    #[test]
    fn replay_from_eof_returns_correct_next_seq() {
        // This test specifically covers the recovery scenario where:
        // 1. Snapshot was taken at log_position (which is EOF)
        // 2. replay_from is called with that position
        // 3. Even though no events are returned, next_seq must be correct
        let dir = tempdir().unwrap();
        let path = dir.path().join("events.log");

        let mut log = EventLog::open(&path).unwrap();
        for i in 0..5 {
            log.append(StateEventPayload::TrainStarted {
                root_pr: PrNumber(i),
                current_pr: PrNumber(i),
            })
            .unwrap();
        }
        let eof_position = log.position().unwrap();
        drop(log);

        // Replay from exactly EOF (simulating recovery with snapshot at end of log)
        let (events, next_seq) = EventLog::replay_from(&path, eof_position).unwrap();
        assert!(events.is_empty(), "no new events after EOF");
        assert_eq!(next_seq, 5, "next_seq should be 5 (events 0-4 exist)");
    }

    #[test]
    fn handles_empty_lines() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("events.log");

        // Write event, then empty line, then another event
        let mut log = EventLog::open(&path).unwrap();
        log.append(StateEventPayload::TrainStarted {
            root_pr: PrNumber(1),
            current_pr: PrNumber(1),
        })
        .unwrap();
        drop(log);

        // Manually add empty line
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        writeln!(file).unwrap();
        drop(file);

        // Write another event
        let mut log = EventLog::open_with_seq(&path, 1).unwrap();
        log.append(StateEventPayload::TrainStopped {
            root_pr: PrNumber(1),
        })
        .unwrap();
        drop(log);

        // Replay should handle empty lines
        let (events, next_seq) = EventLog::replay_from(&path, 0).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(next_seq, 2);
    }

    #[test]
    fn non_monotonic_sequence_treated_as_corruption() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("events.log");

        // Write two valid events with seq 0 and 1
        let mut log = EventLog::open(&path).unwrap();
        log.append(StateEventPayload::TrainStarted {
            root_pr: PrNumber(1),
            current_pr: PrNumber(1),
        })
        .unwrap();
        log.append(StateEventPayload::TrainStopped {
            root_pr: PrNumber(1),
        })
        .unwrap();
        drop(log);

        // Manually append an event with a duplicate sequence number (seq=1 again)
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        writeln!(
            file,
            r#"{{"seq":1,"ts":"2024-01-01T00:00:00Z","type":"train_completed","root_pr":1}}"#
        )
        .unwrap();
        // Also add a valid event after to show we stop at corruption
        writeln!(
            file,
            r#"{{"seq":2,"ts":"2024-01-01T00:00:01Z","type":"train_completed","root_pr":2}}"#
        )
        .unwrap();
        drop(file);

        // Replay should stop at the non-monotonic event and truncate
        let (events, next_seq) = EventLog::replay_from(&path, 0).unwrap();
        assert_eq!(events.len(), 2, "Should recover only the valid prefix");
        assert_eq!(next_seq, 2);
        assert!(matches!(
            events[0].payload,
            StateEventPayload::TrainStarted { .. }
        ));
        assert!(matches!(
            events[1].payload,
            StateEventPayload::TrainStopped { .. }
        ));

        // File should be truncated to remove corruption
        let (events_after, _) = EventLog::replay_from(&path, 0).unwrap();
        assert_eq!(events_after.len(), 2, "File should be truncated");
    }

    #[test]
    fn complex_payload_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("events.log");

        let mut log = EventLog::open(&path).unwrap();

        // Write a complex PhaseTransition event
        let sha = Sha::parse("a".repeat(40)).unwrap();
        let payload = StateEventPayload::PhaseTransition {
            train_root: PrNumber(100),
            current_pr: PrNumber(200),
            predecessor_pr: Some(PrNumber(99)),
            last_squash_sha: Some(sha.clone()),
            phase: CascadePhase::Reconciling {
                progress: DescendantProgress::new(vec![PrNumber(201), PrNumber(202)]),
                squash_sha: sha,
            },
        };

        log.append(payload.clone()).unwrap();
        drop(log);

        let (events, _) = EventLog::replay_from(&path, 0).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].payload, payload);
    }

    // ─── Crash recovery property tests ───

    proptest! {
        /// After crash recovery, subsequent appends produce a valid log.
        ///
        /// This property tests the full crash-recovery-continue cycle:
        /// 1. Write N events
        /// 2. Optionally strip trailing newline (simulating crash between JSON and newline)
        /// 3. Call replay_from to recover
        /// 4. Write M more events
        /// 5. Final replay should recover exactly N + M events
        #[test]
        fn recovery_then_append_produces_valid_log(
            initial_payloads in prop::collection::vec(arb_state_event_payload(), 1..5),
            additional_payloads in prop::collection::vec(arb_state_event_payload(), 1..5),
            strip_final_newline in proptest::bool::ANY,
        ) {
            let dir = tempdir().unwrap();
            let path = dir.path().join("events.log");

            // Write initial events
            let mut log = EventLog::open(&path).unwrap();
            for payload in &initial_payloads {
                log.append(payload.clone()).unwrap();
            }
            drop(log);

            // Optionally strip trailing newline to simulate crash
            if strip_final_newline {
                let content = std::fs::read(&path).unwrap();
                if content.ends_with(b"\n") {
                    std::fs::write(&path, &content[..content.len() - 1]).unwrap();
                }
            }

            // Recover
            let (events, next_seq) = EventLog::replay_from(&path, 0).unwrap();
            prop_assert_eq!(events.len(), initial_payloads.len());
            prop_assert_eq!(next_seq, initial_payloads.len() as u64);

            // Write additional events
            let mut log = EventLog::open_with_seq(&path, next_seq).unwrap();
            for payload in &additional_payloads {
                log.append(payload.clone()).unwrap();
            }
            drop(log);

            // Final replay should have all events
            let (final_events, final_next_seq) = EventLog::replay_from(&path, 0).unwrap();
            let expected_total = initial_payloads.len() + additional_payloads.len();
            prop_assert_eq!(
                final_events.len(),
                expected_total,
                "Expected {} events ({}+{}), got {}. strip_newline={}",
                expected_total,
                initial_payloads.len(),
                additional_payloads.len(),
                final_events.len(),
                strip_final_newline
            );
            prop_assert_eq!(final_next_seq, expected_total as u64);

            // Verify sequence numbers are correct
            for (i, event) in final_events.iter().enumerate() {
                prop_assert_eq!(event.seq, i as u64);
            }
        }

        /// position() always equals file length, regardless of how the log was opened.
        ///
        /// This property verifies the invariant: position() == file metadata length
        /// after any sequence of open/append operations.
        #[test]
        fn position_equals_file_length(
            payloads in prop::collection::vec(arb_state_event_payload(), 0..10),
            reopen_after in prop::collection::vec(0usize..10, 0..3),
        ) {
            let dir = tempdir().unwrap();
            let path = dir.path().join("events.log");

            // Determine reopen points (indices after which we reopen)
            let reopen_set: std::collections::HashSet<_> = reopen_after.into_iter().collect();

            let mut log = EventLog::open(&path).unwrap();
            let mut next_seq = 0u64;

            for (i, payload) in payloads.iter().enumerate() {
                log.append(payload.clone()).unwrap();
                next_seq += 1;

                // Check invariant after each append
                let pos = log.position().unwrap();
                let file_len = std::fs::metadata(&path).unwrap().len();
                prop_assert_eq!(
                    pos, file_len,
                    "After append {}: position()={} but file_len={}",
                    i, pos, file_len
                );

                // Optionally reopen the log
                if reopen_set.contains(&i) {
                    drop(log);
                    log = EventLog::open_with_seq(&path, next_seq).unwrap();

                    // Check invariant after reopen (before any writes)
                    let pos = log.position().unwrap();
                    let file_len = std::fs::metadata(&path).unwrap().len();
                    prop_assert_eq!(
                        pos, file_len,
                        "After reopen at {}: position()={} but file_len={}",
                        i, pos, file_len
                    );
                }
            }

            // Final check on empty log case
            if payloads.is_empty() {
                let pos = log.position().unwrap();
                prop_assert_eq!(pos, 0, "Empty log should have position 0");
            }
        }

        /// Arbitrary trailing bytes (including invalid UTF-8) do not prevent recovery.
        ///
        /// This property verifies that:
        /// 1. Recovery succeeds regardless of what garbage bytes are at EOF
        /// 2. All complete, valid events before the garbage are recovered
        /// 3. The file is truncated to remove the garbage
        #[test]
        fn arbitrary_trailing_bytes_do_not_prevent_recovery(
            payloads in prop::collection::vec(arb_state_event_payload(), 1..5),
            // Generate arbitrary bytes including invalid UTF-8 sequences
            garbage_bytes in prop::collection::vec(prop::num::u8::ANY, 1..100),
        ) {
            use std::sync::atomic::{AtomicU32, Ordering};
            static INVALID_UTF8_COUNT: AtomicU32 = AtomicU32::new(0);
            static TOTAL_COUNT: AtomicU32 = AtomicU32::new(0);

            let dir = tempdir().unwrap();
            let path = dir.path().join("events.log");

            // Write valid events
            let mut log = EventLog::open(&path).unwrap();
            for payload in &payloads {
                log.append(payload.clone()).unwrap();
            }
            let valid_len = log.position().unwrap();
            drop(log);

            // Append arbitrary garbage bytes (simulating crash mid-write)
            {
                let mut file = OpenOptions::new().append(true).open(&path).unwrap();
                file.write_all(&garbage_bytes).unwrap();
            }

            // Track whether we're testing invalid UTF-8
            let is_invalid_utf8 = std::str::from_utf8(&garbage_bytes).is_err();
            TOTAL_COUNT.fetch_add(1, Ordering::Relaxed);
            if is_invalid_utf8 {
                INVALID_UTF8_COUNT.fetch_add(1, Ordering::Relaxed);
            }

            // Recovery must succeed
            let result = EventLog::replay_from(&path, 0);
            prop_assert!(
                result.is_ok(),
                "replay_from should succeed with garbage bytes {:?}, got error: {:?}",
                &garbage_bytes[..garbage_bytes.len().min(20)],
                result.err()
            );

            let (events, next_seq) = result.unwrap();

            // All valid events should be recovered (at minimum; garbage could
            // theoretically deserialize into a valid event, though astronomically unlikely)
            prop_assert!(
                events.len() >= payloads.len(),
                "Should recover at least {} valid events, got {}",
                payloads.len(),
                events.len()
            );

            // Verify the original events are present and in order
            for (i, (event, expected_payload)) in events.iter().zip(payloads.iter()).enumerate() {
                prop_assert_eq!(
                    event.seq, i as u64,
                    "Event {} has wrong seq", i
                );
                prop_assert_eq!(
                    &event.payload, expected_payload,
                    "Event {} has wrong payload", i
                );
            }

            // next_seq should be at least payloads.len() (could be higher if garbage was valid)
            prop_assert!(
                next_seq >= payloads.len() as u64,
                "next_seq should be at least {}, got {}",
                payloads.len(),
                next_seq
            );

            // File should be truncated (or have newline appended if garbage was valid JSON)
            let new_len = std::fs::metadata(&path).unwrap().len();
            // If garbage parsed as valid JSON without newline, file may be valid_len + 1 (newline added)
            // Otherwise it should be truncated to valid_len
            prop_assert!(
                new_len <= valid_len + 1,
                "File should be truncated or have at most one newline added. \
                 Expected <= {}, got {}",
                valid_len + 1,
                new_len
            );

            // After the test suite, verify distribution (on drop would be better but this is simpler)
            // We check periodically rather than at the end
            let total = TOTAL_COUNT.load(Ordering::Relaxed);
            if total > 0 && total % 100 == 0 {
                let invalid_count = INVALID_UTF8_COUNT.load(Ordering::Relaxed);
                let invalid_ratio = invalid_count as f64 / total as f64;
                // Random bytes should produce invalid UTF-8 most of the time
                // (valid UTF-8 is a small subset of all byte sequences)
                prop_assert!(
                    invalid_ratio > 0.5,
                    "Expected >50% invalid UTF-8 cases, got {:.1}% ({}/{})",
                    invalid_ratio * 100.0,
                    invalid_count,
                    total
                );
            }
        }
    }

    // ─── Bug reproduction tests ───

    /// Bug: If a crash occurs after writing JSON but before the newline is written,
    /// the log file contains valid JSON without a trailing newline. replay_from
    /// accepts it (read_line returns EOF without newline), but the next append()
    /// will concatenate two JSON objects on one line (`}{`), breaking future replays.
    #[test]
    fn crash_between_json_and_newline_does_not_corrupt_next_write() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("events.log");

        // Write a valid event
        let mut log = EventLog::open(&path).unwrap();
        log.append(StateEventPayload::TrainStarted {
            root_pr: PrNumber(1),
            current_pr: PrNumber(1),
        })
        .unwrap();
        drop(log);

        // Simulate crash after JSON but before newline:
        // Truncate off the trailing newline
        {
            let content = std::fs::read(&path).unwrap();
            assert!(content.ends_with(b"\n"), "Event should end with newline");
            let without_newline = &content[..content.len() - 1];
            std::fs::write(&path, without_newline).unwrap();
        }

        // Verify file doesn't end with newline
        let content = std::fs::read(&path).unwrap();
        assert!(!content.ends_with(b"\n"), "Should have removed newline");

        // Replay to recover (this should detect and fix the missing newline)
        let (events, next_seq) = EventLog::replay_from(&path, 0).unwrap();
        assert_eq!(events.len(), 1, "First event should be recovered");
        assert_eq!(next_seq, 1);

        // Reopen and write another event
        let mut log = EventLog::open_with_seq(&path, next_seq).unwrap();
        log.append(StateEventPayload::TrainStopped {
            root_pr: PrNumber(1),
        })
        .unwrap();
        drop(log);

        // Both events should be recoverable
        let (events, next_seq) = EventLog::replay_from(&path, 0).unwrap();
        assert_eq!(
            events.len(),
            2,
            "Both events should be recoverable (got {} events). \
             File contents: {:?}",
            events.len(),
            String::from_utf8_lossy(&std::fs::read(&path).unwrap())
        );
        assert_eq!(next_seq, 2);
    }

    /// Bug: position() uses stream_position() which may return 0 on a file opened
    /// with append(true) if no write/seek has occurred yet. This causes snapshot
    /// log_position to be incorrect.
    #[test]
    fn position_returns_correct_offset_on_existing_log() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("events.log");

        // Create a log with some events
        let mut log = EventLog::open(&path).unwrap();
        log.append(StateEventPayload::TrainStarted {
            root_pr: PrNumber(1),
            current_pr: PrNumber(1),
        })
        .unwrap();
        log.append(StateEventPayload::TrainStopped {
            root_pr: PrNumber(1),
        })
        .unwrap();
        let expected_len = log.position().unwrap();
        drop(log);

        // Verify file has content
        let file_len = std::fs::metadata(&path).unwrap().len();
        assert_eq!(expected_len, file_len);
        assert!(file_len > 0);

        // Reopen the log (simulating process restart)
        let mut log = EventLog::open_with_seq(&path, 2).unwrap();

        // Position should be at EOF, not 0
        let pos = log.position().unwrap();
        assert_eq!(
            pos, file_len,
            "position() should return file length ({}) on reopened log, got {}",
            file_len, pos
        );
    }

    /// Bug: replay_from uses read_line(&mut String) which returns InvalidData on
    /// invalid UTF-8. A crash that truncates in the middle of a multi-byte UTF-8
    /// character will cause recovery to fail entirely instead of truncating the
    /// partial record.
    #[test]
    fn crash_mid_utf8_character_recovers() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("events.log");

        // Write an event with multi-byte UTF-8 characters
        // Note: We write directly to file to include UTF-8 in the JSON
        // The emoji "🚀" is 4 bytes: F0 9F 9A 80
        {
            let mut file = File::create(&path).unwrap();
            // Write a complete valid event first
            writeln!(
                file,
                r#"{{"seq":0,"ts":"2024-01-01T00:00:00Z","type":"train_started","root_pr":1,"current_pr":1}}"#
            )
            .unwrap();
            // Write partial second event with multi-byte char truncated mid-character
            // This simulates a crash that cut off in the middle of writing a UTF-8 sequence
            let partial =
                r#"{"seq":1,"ts":"2024-01-01T00:00:01Z","type":"train_stopped","root_pr":1"#;
            file.write_all(partial.as_bytes()).unwrap();
            // Add invalid UTF-8: first two bytes of a 4-byte sequence (e.g., emoji)
            file.write_all(&[0xF0, 0x9F]).unwrap(); // Incomplete UTF-8 sequence
        }

        // Replay should recover the valid first event, not fail with InvalidData
        let result = EventLog::replay_from(&path, 0);
        assert!(
            result.is_ok(),
            "replay_from should succeed with invalid UTF-8, got: {:?}",
            result.err()
        );

        let (events, next_seq) = result.unwrap();
        assert_eq!(
            events.len(),
            1,
            "Should recover the one valid event before the corrupted line"
        );
        assert_eq!(next_seq, 1);
    }
}
