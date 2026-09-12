//! Status comment formatting and parsing for recovery.
//!
//! Status comments are the bot's backup recovery mechanism when local state is lost.
//! They embed machine-readable JSON in an HTML comment, alongside human-readable status.
//!
//! # Format
//!
//! ```text
//! <!-- merge-train-state
//! {"version": 1, "recovery_seq": 42, ...}
//! -->
//! **Merge Train Status**
//!
//! Human-readable status message.
//! ```
//!
//! The JSON contains the `TrainRecord` for disaster recovery (excluding `status_comment_id`,
//! with error text possibly truncated for size).

pub mod format;
pub mod ledger;
pub mod parse;
pub mod receipt;

pub use format::{GITHUB_COMMENT_SIZE_LIMIT, format_status_comment, truncate_for_size_limit};
pub use ledger::{Declaration, StackLedger, format_stack_ledger, parse_stack_ledger};
pub use parse::{ParseError, parse_status_comment};
pub use receipt::{Receipt, format_rejection_receipt, format_retraction_receipt, parse_receipt};
