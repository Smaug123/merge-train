//! Command types for `@merge-train` bot commands.
//!
//! These commands are parsed from GitHub issue/PR comments.

use serde::{Deserialize, Serialize};

use crate::types::PrNumber;

/// A parsed `@merge-train` command from a comment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    /// Declares predecessor relationship: `@merge-train predecessor #N`
    ///
    /// This declares that the current PR is stacked on top of PR #N.
    Predecessor(PrNumber),

    /// Starts cascade: `@merge-train start`
    ///
    /// Initiates the merge train for the stack rooted at this PR.
    Start,

    /// Stops cascade: `@merge-train stop`
    ///
    /// Halts the active cascade for the stack containing this PR.
    Stop,

    /// Force stops with admin actions: `@merge-train stop --force`
    ///
    /// Like `Stop`, but performs additional admin actions (closes status comment,
    /// optionally closes affected PRs).
    StopForce,
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn arb_command() -> impl Strategy<Value = Command> {
        prop_oneof![
            (1u64..=u64::MAX).prop_map(|n| Command::Predecessor(PrNumber(n))),
            Just(Command::Start),
            Just(Command::Stop),
            Just(Command::StopForce),
        ]
    }

    proptest! {
        #[test]
        fn command_serde_roundtrip(cmd in arb_command()) {
            let json = serde_json::to_string(&cmd).unwrap();
            let parsed: Command = serde_json::from_str(&json).unwrap();
            prop_assert_eq!(cmd, parsed);
        }
    }

    #[test]
    fn command_debug_format() {
        assert!(format!("{:?}", Command::Start).contains("Start"));
        assert!(format!("{:?}", Command::Stop).contains("Stop"));
        assert!(format!("{:?}", Command::StopForce).contains("StopForce"));
        assert!(format!("{:?}", Command::Predecessor(PrNumber(42))).contains("42"));
    }
}
