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
use super::fsync::fsync_file;

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
    /// If the file doesn't exist, it's created.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;

        Ok(EventLog {
            file,
            path,
            next_seq: 0,
        })
    }

    /// Opens an existing log file or creates a new one, with a known next sequence number.
    ///
    /// Use this after calling `replay_from` to create a log ready for appending.
    pub fn open_with_seq(path: impl AsRef<Path>, next_seq: u64) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;

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
    /// # Returns
    ///
    /// Returns `(events, next_seq)` where:
    /// - `events` is the list of valid events replayed
    /// - `next_seq` is the next sequence number to use (max seq + 1)
    ///
    /// If the file doesn't exist or is empty, returns `(vec![], 0)`.
    ///
    /// # Truncation
    ///
    /// If the final line doesn't parse as valid JSON (crash mid-write),
    /// the file is truncated at the start of that line. This ensures
    /// the log always contains a valid prefix of events.
    pub fn replay_from(path: impl AsRef<Path>, offset: u64) -> Result<(Vec<StateEvent>, u64)> {
        let path = path.as_ref();

        // Check if file exists
        if !path.exists() {
            return Ok((vec![], 0));
        }

        // Open for reading
        let file = File::open(path)?;
        let file_len = file.metadata()?.len();

        // If seeking past end or file is empty, nothing to replay
        if offset >= file_len {
            return Ok((vec![], 0));
        }

        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(offset))?;

        let mut events = Vec::new();
        let mut last_valid_pos = offset;
        let mut current_pos = offset;
        let mut max_seq: Option<u64> = None;

        loop {
            let mut line = String::new();
            let bytes_read = reader.read_line(&mut line)?;

            if bytes_read == 0 {
                // EOF reached
                break;
            }

            let line_start = current_pos;
            current_pos += bytes_read as u64;

            // Skip empty lines
            let trimmed = line.trim();
            if trimmed.is_empty() {
                last_valid_pos = current_pos;
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
                    max_seq = Some(event.seq);
                    events.push(event);
                    last_valid_pos = current_pos;
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

        // If we need to truncate, do so
        if last_valid_pos < file_len {
            let file = OpenOptions::new().write(true).open(path)?;
            file.set_len(last_valid_pos)?;
            fsync_file(&file)?;
        }

        let next_seq = max_seq.map(|s| s + 1).unwrap_or(0);
        Ok((events, next_seq))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{CascadePhase, DescendantProgress, PrNumber, Sha};
    use proptest::prelude::*;
    use std::io::Write;
    use tempfile::tempdir;

    // ─── Arbitrary implementations ───

    fn arb_pr_number() -> impl Strategy<Value = PrNumber> {
        (1u64..10000).prop_map(PrNumber)
    }

    fn arb_simple_payload() -> impl Strategy<Value = StateEventPayload> {
        // Use simpler payloads for log tests to focus on log behavior
        prop_oneof![
            (arb_pr_number(), arb_pr_number()).prop_map(|(r, c)| StateEventPayload::TrainStarted {
                root_pr: r,
                current_pr: c
            }),
            arb_pr_number().prop_map(|r| StateEventPayload::TrainStopped { root_pr: r }),
            arb_pr_number().prop_map(|r| StateEventPayload::TrainCompleted { root_pr: r }),
            (arb_pr_number(), arb_pr_number()).prop_map(|(pr, pred)| {
                StateEventPayload::PredecessorDeclared {
                    pr,
                    predecessor: pred,
                }
            }),
        ]
    }

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
        fn roundtrip_n_events(payloads in prop::collection::vec(arb_simple_payload(), 1..20)) {
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
        fn offset_replay(payloads in prop::collection::vec(arb_simple_payload(), 3..10)) {
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

        /// Partial line at EOF is truncated.
        #[test]
        fn partial_line_recovery(payloads in prop::collection::vec(arb_simple_payload(), 1..10)) {
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
            payloads in prop::collection::vec(arb_simple_payload(), 2..10),
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

    // ─── Critical event fsync tests ───

    #[test]
    fn critical_events_are_synced() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("events.log");

        let mut log = EventLog::open(&path).unwrap();

        // Write a critical event
        log.append(StateEventPayload::TrainStarted {
            root_pr: PrNumber(1),
            current_pr: PrNumber(1),
        })
        .unwrap();

        // The file should be readable immediately (fsync was called)
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

        // Replay from past EOF
        let (events, next_seq) = EventLog::replay_from(&path, 10000).unwrap();
        assert!(events.is_empty());
        assert_eq!(next_seq, 0);
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
}
