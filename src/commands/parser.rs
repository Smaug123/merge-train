//! Parser for `@merge-train` commands in comment text.
//!
//! This module provides a pure parser that extracts structured commands from
//! unstructured GitHub comment text.

use crate::types::PrNumber;

use super::types::Command;

/// The trigger string that starts a command.
/// Case-sensitive because GitHub mentions are case-sensitive.
const TRIGGER: &str = "@merge-train";

/// Parses the first `@merge-train` command found in comment text.
///
/// # Parsing Rules
///
/// - The trigger `@merge-train` is case-sensitive
/// - Command names (`start`, `stop`, `predecessor`) are case-insensitive
/// - Whitespace between tokens is flexible (spaces, tabs)
/// - If multiple commands are present, the first valid one wins
/// - Returns `None` if no valid command is found
///
/// # Examples
///
/// ```
/// use merge_train::commands::parse_command;
/// use merge_train::commands::Command;
/// use merge_train::types::PrNumber;
///
/// assert_eq!(parse_command("@merge-train start"), Some(Command::Start));
/// assert_eq!(parse_command("@merge-train stop"), Some(Command::Stop));
/// assert_eq!(
///     parse_command("@merge-train predecessor #42"),
///     Some(Command::Predecessor(PrNumber(42)))
/// );
/// assert_eq!(parse_command("no command here"), None);
/// ```
pub fn parse_command(text: &str) -> Option<Command> {
    // Find all occurrences of @merge-train and try to parse each
    let mut search_start = 0;
    while let Some(pos) = text[search_start..].find(TRIGGER) {
        let abs_pos = search_start + pos;
        let after_trigger = &text[abs_pos + TRIGGER.len()..];

        if let Some(cmd) = try_parse_after_trigger(after_trigger) {
            return Some(cmd);
        }

        // Move past this trigger and continue searching
        search_start = abs_pos + TRIGGER.len();
    }
    None
}

/// Attempts to parse a command from text immediately following the trigger.
fn try_parse_after_trigger(text: &str) -> Option<Command> {
    // Must have at least one whitespace character after trigger
    let text = text.strip_prefix(|c: char| c.is_ascii_whitespace())?;
    // Skip any additional whitespace
    let text = text.trim_start();

    // Extract the command word (everything up to the next whitespace)
    let (cmd_word, rest) = split_first_word(text);

    match cmd_word.to_ascii_lowercase().as_str() {
        "predecessor" => parse_predecessor(rest),
        "start" => Some(Command::Start),
        "stop" => parse_stop(rest),
        _ => None,
    }
}

/// Parses the predecessor command arguments: `#N`
fn parse_predecessor(text: &str) -> Option<Command> {
    let text = text.trim_start();
    // Must have a # prefix
    let text = text.strip_prefix('#')?;
    // Extract the number (up to whitespace or end)
    let (num_str, _) = split_first_word(text);

    // Parse as u64
    let n: u64 = num_str.parse().ok()?;

    // PR numbers start at 1; 0 is invalid
    if n == 0 {
        return None;
    }

    Some(Command::Predecessor(PrNumber(n)))
}

/// Parses optional flags after `stop` command.
fn parse_stop(text: &str) -> Option<Command> {
    let text = text.trim_start();

    // If nothing follows, or next char isn't a dash, it's a plain stop
    if text.is_empty() || !text.starts_with('-') {
        return Some(Command::Stop);
    }

    // Check for --force flag
    let (flag, _) = split_first_word(text);
    if flag.eq_ignore_ascii_case("--force") {
        Some(Command::StopForce)
    } else {
        // Unknown flag, treat as plain stop
        Some(Command::Stop)
    }
}

/// Splits text at the first whitespace, returning (word, rest).
/// If no whitespace, returns (text, "").
fn split_first_word(text: &str) -> (&str, &str) {
    match text.find(|c: char| c.is_ascii_whitespace()) {
        Some(pos) => (&text[..pos], &text[pos..]),
        None => (text, ""),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // ==================== Valid command parsing ====================

    proptest! {
        /// Any valid PR number (1 to u64::MAX) should parse correctly.
        #[test]
        fn predecessor_parses_any_valid_number(n in 1u64..=u64::MAX) {
            let text = format!("@merge-train predecessor #{}", n);
            prop_assert_eq!(parse_command(&text), Some(Command::Predecessor(PrNumber(n))));
        }
    }

    #[test]
    fn start_parses() {
        assert_eq!(parse_command("@merge-train start"), Some(Command::Start));
    }

    #[test]
    fn stop_parses() {
        assert_eq!(parse_command("@merge-train stop"), Some(Command::Stop));
    }

    #[test]
    fn stop_force_parses() {
        assert_eq!(
            parse_command("@merge-train stop --force"),
            Some(Command::StopForce)
        );
    }

    // ==================== Robustness: never panic ====================

    proptest! {
        /// Arbitrary text should never cause a panic.
        #[test]
        fn arbitrary_text_never_panics(text: String) {
            let _ = parse_command(&text);
        }

        /// Arbitrary bytes after the trigger should never cause a panic.
        #[test]
        fn arbitrary_suffix_after_trigger_never_panics(suffix: String) {
            let text = format!("@merge-train{}", suffix);
            let _ = parse_command(&text);
        }
    }

    // ==================== Whitespace handling ====================

    proptest! {
        /// Various amounts of whitespace between tokens should work.
        #[test]
        fn whitespace_between_tokens(
            ws1 in "[ \t]{1,5}",
            ws2 in "[ \t]{1,5}",
            n in 1u64..1_000_000u64
        ) {
            let text = format!("@merge-train{}predecessor{}#{}", ws1, ws2, n);
            prop_assert_eq!(parse_command(&text), Some(Command::Predecessor(PrNumber(n))));
        }

        /// Commands embedded in longer text should still parse.
        #[test]
        fn command_embedded_in_text(
            prefix in "[a-zA-Z ]{0,20}",
            n in 1u64..1_000_000u64
        ) {
            let text = format!("{} @merge-train predecessor #{}", prefix, n);
            prop_assert_eq!(parse_command(&text), Some(Command::Predecessor(PrNumber(n))));
        }
    }

    #[test]
    fn whitespace_variations() {
        // Tabs
        assert_eq!(parse_command("@merge-train\tstart"), Some(Command::Start));
        // Multiple spaces
        assert_eq!(parse_command("@merge-train    start"), Some(Command::Start));
        // Mixed
        assert_eq!(
            parse_command("@merge-train \t \t start"),
            Some(Command::Start)
        );
    }

    // ==================== Case insensitivity for command names ====================

    proptest! {
        #[test]
        fn case_variations_start(cmd in prop_oneof![
            Just("start"),
            Just("START"),
            Just("Start"),
            Just("sTaRt"),
            Just("StArT")
        ]) {
            let text = format!("@merge-train {}", cmd);
            prop_assert_eq!(parse_command(&text), Some(Command::Start));
        }

        #[test]
        fn case_variations_stop(cmd in prop_oneof![
            Just("stop"),
            Just("STOP"),
            Just("Stop"),
            Just("sToP")
        ]) {
            let text = format!("@merge-train {}", cmd);
            prop_assert_eq!(parse_command(&text), Some(Command::Stop));
        }

        #[test]
        fn case_variations_predecessor(
            cmd in prop_oneof![
                Just("predecessor"),
                Just("PREDECESSOR"),
                Just("Predecessor"),
                Just("PrEdEcEsSoR")
            ],
            n in 1u64..1000u64
        ) {
            let text = format!("@merge-train {} #{}", cmd, n);
            prop_assert_eq!(parse_command(&text), Some(Command::Predecessor(PrNumber(n))));
        }

        #[test]
        fn case_variations_force_flag(flag in prop_oneof![
            Just("--force"),
            Just("--FORCE"),
            Just("--Force"),
            Just("--fOrCe")
        ]) {
            let text = format!("@merge-train stop {}", flag);
            prop_assert_eq!(parse_command(&text), Some(Command::StopForce));
        }
    }

    // ==================== Multiple commands: first wins ====================

    #[test]
    fn first_command_wins() {
        assert_eq!(
            parse_command("@merge-train start\n@merge-train stop"),
            Some(Command::Start)
        );
        assert_eq!(
            parse_command("Please @merge-train predecessor #1 and also @merge-train start"),
            Some(Command::Predecessor(PrNumber(1)))
        );
        assert_eq!(
            parse_command("@merge-train stop\nLater: @merge-train stop --force"),
            Some(Command::Stop)
        );
    }

    #[test]
    fn skips_invalid_and_finds_valid() {
        // First trigger has no valid command, second does
        assert_eq!(
            parse_command("@merge-train unknown @merge-train start"),
            Some(Command::Start)
        );
        // First has malformed predecessor, second is valid
        assert_eq!(
            parse_command("@merge-train predecessor #0 @merge-train predecessor #1"),
            Some(Command::Predecessor(PrNumber(1)))
        );
    }

    // ==================== Edge cases ====================

    #[test]
    fn edge_cases() {
        // # followed by non-number
        assert_eq!(parse_command("@merge-train predecessor #abc"), None);

        // u64::MAX should work
        let max = format!("@merge-train predecessor #{}", u64::MAX);
        assert_eq!(
            parse_command(&max),
            Some(Command::Predecessor(PrNumber(u64::MAX)))
        );

        // Overflow should fail
        let overflow = format!("@merge-train predecessor #{}0", u64::MAX);
        assert_eq!(parse_command(&overflow), None);

        // Zero PR number is invalid
        assert_eq!(parse_command("@merge-train predecessor #0"), None);

        // Missing # prefix
        assert_eq!(parse_command("@merge-train predecessor 123"), None);

        // Empty after @merge-train
        assert_eq!(parse_command("@merge-train"), None);

        // Just whitespace after trigger
        assert_eq!(parse_command("@merge-train   "), None);

        // Unknown command
        assert_eq!(parse_command("@merge-train unknown"), None);

        // Trigger without whitespace before command
        assert_eq!(parse_command("@merge-trainstart"), None);

        // Similar but not exact trigger
        assert_eq!(parse_command("@merge-trains start"), None);

        // Case matters for the trigger (GitHub mentions are case-sensitive)
        assert_eq!(parse_command("@Merge-Train start"), None);
        assert_eq!(parse_command("@MERGE-TRAIN start"), None);
        assert_eq!(parse_command("@Merge-train start"), None);

        // Negative number (the - is not part of number parsing)
        assert_eq!(parse_command("@merge-train predecessor #-1"), None);

        // Floating point
        assert_eq!(parse_command("@merge-train predecessor #1.5"), None);

        // Number with leading zeros works
        assert_eq!(
            parse_command("@merge-train predecessor #007"),
            Some(Command::Predecessor(PrNumber(7)))
        );
    }

    #[test]
    fn stop_with_unknown_flags() {
        // Unknown flags are ignored, treated as plain stop
        assert_eq!(
            parse_command("@merge-train stop --unknown"),
            Some(Command::Stop)
        );
        assert_eq!(parse_command("@merge-train stop -f"), Some(Command::Stop));
        // Only --force is recognized
        assert_eq!(
            parse_command("@merge-train stop -force"),
            Some(Command::Stop)
        );
    }

    // ==================== Real-world comment examples ====================

    #[test]
    fn real_world_comments() {
        // Command at start of comment
        assert_eq!(
            parse_command("@merge-train predecessor #42\n\nThis PR depends on #42"),
            Some(Command::Predecessor(PrNumber(42)))
        );

        // Command after explanation
        assert_eq!(
            parse_command("I'm ready to merge this stack.\n\n@merge-train start"),
            Some(Command::Start)
        );

        // Command in middle of text
        assert_eq!(
            parse_command("Please @merge-train stop this cascade, there's an issue"),
            Some(Command::Stop)
        );

        // Multiple paragraphs
        assert_eq!(
            parse_command("First paragraph.\n\nSecond paragraph.\n\n@merge-train start\n\nThird."),
            Some(Command::Start)
        );

        // With markdown formatting
        assert_eq!(
            parse_command("**Important**: @merge-train start immediately"),
            Some(Command::Start)
        );

        // In a code fence (still matches - caller should handle context)
        assert_eq!(
            parse_command("```\n@merge-train start\n```"),
            Some(Command::Start)
        );

        // With emoji
        assert_eq!(
            parse_command("Let's go! @merge-train start"),
            Some(Command::Start)
        );

        // After mentioning other users
        assert_eq!(
            parse_command("cc @reviewer @merge-train start"),
            Some(Command::Start)
        );

        // Complex real-world scenario
        assert_eq!(
            parse_command(
                r#"Thanks for the review!

I've addressed all the feedback. The PR is ready.

@merge-train predecessor #123

This builds on top of the auth refactor in #123."#
            ),
            Some(Command::Predecessor(PrNumber(123)))
        );
    }

    // ==================== Helper function tests ====================

    #[test]
    fn split_first_word_works() {
        assert_eq!(split_first_word("hello world"), ("hello", " world"));
        assert_eq!(split_first_word("hello"), ("hello", ""));
        assert_eq!(split_first_word(""), ("", ""));
        assert_eq!(split_first_word("a b c"), ("a", " b c"));
        assert_eq!(split_first_word("hello\tworld"), ("hello", "\tworld"));
    }
}
