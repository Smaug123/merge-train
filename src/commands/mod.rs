//! Command parsing for bot commands.
//!
//! This module provides types and parsing for commands that users issue via
//! GitHub PR/issue comments to interact with the merge train bot.
//!
//! # Supported Commands
//!
//! - `@{bot_name} predecessor #N` - Declares that the current PR is stacked on PR #N
//! - `@{bot_name} start` - Starts the merge train cascade for this stack
//! - `@{bot_name} stop` - Stops the active cascade
//! - `@{bot_name} stop --force` - Force stops with additional admin actions
//!
//! # Example
//!
//! ```
//! use merge_train::commands::{parse_command, Command};
//! use merge_train::types::PrNumber;
//!
//! let comment = "Please start the merge train.\n\n@merge-train start";
//! assert_eq!(parse_command(comment, "merge-train"), Some(Command::Start));
//!
//! let comment = "@merge-train predecessor #42";
//! assert_eq!(parse_command(comment, "merge-train"), Some(Command::Predecessor(PrNumber(42))));
//!
//! // Works with different bot names
//! let comment = "@my-bot start";
//! assert_eq!(parse_command(comment, "my-bot"), Some(Command::Start));
//! ```

mod parser;
mod types;

pub use parser::parse_command;
pub use types::Command;
