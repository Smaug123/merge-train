//! Command parsing for `@merge-train` bot commands.
//!
//! This module provides types and parsing for commands that users issue via
//! GitHub PR/issue comments to interact with the merge train bot.
//!
//! # Supported Commands
//!
//! - `@merge-train predecessor #N` - Declares that the current PR is stacked on PR #N
//! - `@merge-train start` - Starts the merge train cascade for this stack
//! - `@merge-train stop` - Stops the active cascade
//! - `@merge-train stop --force` - Force stops with additional admin actions
//!
//! # Example
//!
//! ```
//! use merge_train::commands::{parse_command, Command};
//! use merge_train::types::PrNumber;
//!
//! let comment = "Please start the merge train.\n\n@merge-train start";
//! assert_eq!(parse_command(comment), Some(Command::Start));
//!
//! let comment = "@merge-train predecessor #42";
//! assert_eq!(parse_command(comment), Some(Command::Predecessor(PrNumber(42))));
//! ```

mod parser;
mod types;

pub use parser::parse_command;
pub use types::Command;
