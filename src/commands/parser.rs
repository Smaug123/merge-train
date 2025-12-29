//! Parser for bot commands in comment text.
//!
//! This module provides a pure parser that extracts structured commands from
//! unstructured GitHub comment text.

use crate::types::PrNumber;

use super::types::Command;

/// Parses the first bot command found in comment text.
///
/// # Arguments
///
/// * `text` - The comment text to parse
/// * `bot_name` - The bot name without the `@` prefix (e.g., `"merge-train"`)
///
/// # Parsing Rules
///
/// - The trigger `@{bot_name}` is case-insensitive (like GitHub mentions)
/// - The trigger must be at a word boundary (not preceded by alphanumeric chars)
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
/// assert_eq!(parse_command("@merge-train start", "merge-train"), Some(Command::Start));
/// assert_eq!(parse_command("@Merge-Train stop", "merge-train"), Some(Command::Stop));
/// assert_eq!(
///     parse_command("@MERGE-TRAIN predecessor #42", "merge-train"),
///     Some(Command::Predecessor(PrNumber(42)))
/// );
/// assert_eq!(parse_command("no command here", "merge-train"), None);
/// // Not a valid mention (preceded by alphanumeric):
/// assert_eq!(parse_command("foo@merge-train start", "merge-train"), None);
/// ```
pub fn parse_command(text: &str, bot_name: &str) -> Option<Command> {
    let trigger = format!("@{}", bot_name);
    // Find all occurrences of @bot_name (case-insensitive, at word boundary)
    let mut search_start = 0;
    while let Some(abs_pos) = find_trigger(text, search_start, &trigger) {
        let after_trigger = &text[abs_pos + trigger.len()..];

        if let Some(cmd) = try_parse_after_trigger(after_trigger) {
            return Some(cmd);
        }

        // Move past this trigger and continue searching
        search_start = abs_pos + trigger.len();
    }
    None
}

/// Finds the next occurrence of the trigger (case-insensitive) at a valid word boundary.
/// Returns the byte position of the `@` character if found.
fn find_trigger(text: &str, start: usize, trigger: &str) -> Option<usize> {
    let mut search_pos = start;

    while search_pos < text.len() {
        // Find the next '@' character
        let at_pos = text[search_pos..].find('@')?;
        let abs_pos = search_pos + at_pos;

        // Try to get the candidate slice. This may return None if the end position
        // lands in the middle of a multi-byte UTF-8 character.
        if let Some(candidate) = text.get(abs_pos..abs_pos + trigger.len()) {
            // Check if it matches the trigger case-insensitively
            if candidate.eq_ignore_ascii_case(trigger) {
                // Check left boundary: must be start of string or preceded by non-alphanumeric
                let valid_boundary = abs_pos == 0 || {
                    // Safe: abs_pos > 0, so there's at least one char before
                    let prev_char = text[..abs_pos].chars().next_back().unwrap();
                    !prev_char.is_alphanumeric()
                };
                if valid_boundary {
                    return Some(abs_pos);
                }
            }
        }

        // Move past this '@' and continue searching
        search_pos = abs_pos + 1;
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

    /// Default bot name used in tests.
    const BOT: &str = "merge-train";

    /// The full trigger including @ prefix.
    const TRIGGER: &str = "@merge-train";

    // ==================== Valid command parsing ====================

    proptest! {
        /// Any valid PR number (1 to u64::MAX) should parse correctly.
        #[test]
        fn predecessor_parses_any_valid_number(n in 1u64..=u64::MAX) {
            let text = format!("@merge-train predecessor #{}", n);
            prop_assert_eq!(parse_command(&text, BOT), Some(Command::Predecessor(PrNumber(n))));
        }
    }

    #[test]
    fn start_parses() {
        assert_eq!(
            parse_command("@merge-train start", BOT),
            Some(Command::Start)
        );
    }

    #[test]
    fn stop_parses() {
        assert_eq!(parse_command("@merge-train stop", BOT), Some(Command::Stop));
    }

    #[test]
    fn stop_force_parses() {
        assert_eq!(
            parse_command("@merge-train stop --force", BOT),
            Some(Command::StopForce)
        );
    }

    // ==================== Robustness: never panic ====================

    proptest! {
        /// Arbitrary text should never cause a panic.
        #[test]
        fn arbitrary_text_never_panics(text: String) {
            let _ = parse_command(&text, BOT);
        }

        /// Arbitrary bytes after the trigger should never cause a panic.
        #[test]
        fn arbitrary_suffix_after_trigger_never_panics(suffix: String) {
            let text = format!("@merge-train{}", suffix);
            let _ = parse_command(&text, BOT);
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
            prop_assert_eq!(parse_command(&text, BOT), Some(Command::Predecessor(PrNumber(n))));
        }

        /// Commands embedded in longer text should still parse.
        /// The prefix must end with a non-alphanumeric char for the @ to be at a word boundary.
        #[test]
        fn command_embedded_in_text(
            prefix in "[a-zA-Z]{0,10}[ !:.,;?]{1,2}",
            n in 1u64..1_000_000u64
        ) {
            let text = format!("{}@merge-train predecessor #{}", prefix, n);
            prop_assert_eq!(parse_command(&text, BOT), Some(Command::Predecessor(PrNumber(n))));
        }

        /// Trigger preceded by alphanumeric should not match.
        #[test]
        fn trigger_after_alphanumeric_is_ignored(
            prefix in "[a-zA-Z0-9]{1,10}",
            n in 1u64..1_000_000u64
        ) {
            let text = format!("{}@merge-train predecessor #{}", prefix, n);
            prop_assert_eq!(parse_command(&text, BOT), None);
        }
    }

    #[test]
    fn whitespace_variations() {
        // Tabs
        assert_eq!(
            parse_command("@merge-train\tstart", BOT),
            Some(Command::Start)
        );
        // Multiple spaces
        assert_eq!(
            parse_command("@merge-train    start", BOT),
            Some(Command::Start)
        );
        // Mixed
        assert_eq!(
            parse_command("@merge-train \t \t start", BOT),
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
            prop_assert_eq!(parse_command(&text, BOT), Some(Command::Start));
        }

        #[test]
        fn case_variations_stop(cmd in prop_oneof![
            Just("stop"),
            Just("STOP"),
            Just("Stop"),
            Just("sToP")
        ]) {
            let text = format!("@merge-train {}", cmd);
            prop_assert_eq!(parse_command(&text, BOT), Some(Command::Stop));
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
            prop_assert_eq!(parse_command(&text, BOT), Some(Command::Predecessor(PrNumber(n))));
        }

        #[test]
        fn case_variations_force_flag(flag in prop_oneof![
            Just("--force"),
            Just("--FORCE"),
            Just("--Force"),
            Just("--fOrCe")
        ]) {
            let text = format!("@merge-train stop {}", flag);
            prop_assert_eq!(parse_command(&text, BOT), Some(Command::StopForce));
        }
    }

    // ==================== Multiple commands: first wins ====================

    #[test]
    fn first_command_wins() {
        assert_eq!(
            parse_command("@merge-train start\n@merge-train stop", BOT),
            Some(Command::Start)
        );
        assert_eq!(
            parse_command(
                "Please @merge-train predecessor #1 and also @merge-train start",
                BOT
            ),
            Some(Command::Predecessor(PrNumber(1)))
        );
        assert_eq!(
            parse_command("@merge-train stop\nLater: @merge-train stop --force", BOT),
            Some(Command::Stop)
        );
    }

    #[test]
    fn skips_invalid_and_finds_valid() {
        // First trigger has no valid command, second does
        assert_eq!(
            parse_command("@merge-train unknown @merge-train start", BOT),
            Some(Command::Start)
        );
        // First has malformed predecessor, second is valid
        assert_eq!(
            parse_command(
                "@merge-train predecessor #0 @merge-train predecessor #1",
                BOT
            ),
            Some(Command::Predecessor(PrNumber(1)))
        );
    }

    // ==================== Edge cases ====================

    #[test]
    fn edge_cases() {
        // # followed by non-number
        assert_eq!(parse_command("@merge-train predecessor #abc", BOT), None);

        // u64::MAX should work
        let max = format!("@merge-train predecessor #{}", u64::MAX);
        assert_eq!(
            parse_command(&max, BOT),
            Some(Command::Predecessor(PrNumber(u64::MAX)))
        );

        // Overflow should fail
        let overflow = format!("@merge-train predecessor #{}0", u64::MAX);
        assert_eq!(parse_command(&overflow, BOT), None);

        // Zero PR number is invalid
        assert_eq!(parse_command("@merge-train predecessor #0", BOT), None);

        // Missing # prefix
        assert_eq!(parse_command("@merge-train predecessor 123", BOT), None);

        // Empty after @merge-train
        assert_eq!(parse_command("@merge-train", BOT), None);

        // Just whitespace after trigger
        assert_eq!(parse_command("@merge-train   ", BOT), None);

        // Unknown command
        assert_eq!(parse_command("@merge-train unknown", BOT), None);

        // Trigger without whitespace before command
        assert_eq!(parse_command("@merge-trainstart", BOT), None);

        // Similar but not exact trigger
        assert_eq!(parse_command("@merge-trains start", BOT), None);

        // Negative number (the - is not part of number parsing)
        assert_eq!(parse_command("@merge-train predecessor #-1", BOT), None);

        // Floating point
        assert_eq!(parse_command("@merge-train predecessor #1.5", BOT), None);

        // Number with leading zeros works
        assert_eq!(
            parse_command("@merge-train predecessor #007", BOT),
            Some(Command::Predecessor(PrNumber(7)))
        );
    }

    // ==================== Trigger case insensitivity ====================

    #[test]
    fn trigger_case_insensitive() {
        // GitHub mentions are case-insensitive
        assert_eq!(
            parse_command("@Merge-Train start", BOT),
            Some(Command::Start)
        );
        assert_eq!(
            parse_command("@MERGE-TRAIN start", BOT),
            Some(Command::Start)
        );
        assert_eq!(
            parse_command("@Merge-train start", BOT),
            Some(Command::Start)
        );
        assert_eq!(
            parse_command("@mErGe-TrAiN start", BOT),
            Some(Command::Start)
        );

        // Also works with other commands
        assert_eq!(parse_command("@MERGE-TRAIN stop", BOT), Some(Command::Stop));
        assert_eq!(
            parse_command("@Merge-Train predecessor #42", BOT),
            Some(Command::Predecessor(PrNumber(42)))
        );
        assert_eq!(
            parse_command("@MERGE-TRAIN stop --force", BOT),
            Some(Command::StopForce)
        );
    }

    // ==================== Left boundary check ====================

    #[test]
    fn trigger_requires_word_boundary() {
        // Alphanumeric before @ is NOT a valid boundary (looks like email)
        assert_eq!(parse_command("foo@merge-train start", BOT), None);
        assert_eq!(parse_command("user123@merge-train start", BOT), None);
        assert_eq!(parse_command("A@merge-train start", BOT), None);
        assert_eq!(parse_command("9@merge-train start", BOT), None);

        // Non-alphanumeric before @ IS a valid boundary
        assert_eq!(
            parse_command("(@merge-train start", BOT),
            Some(Command::Start)
        );
        assert_eq!(
            parse_command("[@merge-train start", BOT),
            Some(Command::Start)
        );
        assert_eq!(
            parse_command(":@merge-train start", BOT),
            Some(Command::Start)
        );
        assert_eq!(
            parse_command(".@merge-train start", BOT),
            Some(Command::Start)
        );
        assert_eq!(
            parse_command("!@merge-train start", BOT),
            Some(Command::Start)
        );
        assert_eq!(
            parse_command("-@merge-train start", BOT),
            Some(Command::Start)
        );

        // Whitespace before @ is valid
        assert_eq!(
            parse_command(" @merge-train start", BOT),
            Some(Command::Start)
        );
        assert_eq!(
            parse_command("\t@merge-train start", BOT),
            Some(Command::Start)
        );
        assert_eq!(
            parse_command("\n@merge-train start", BOT),
            Some(Command::Start)
        );

        // Start of string is valid
        assert_eq!(
            parse_command("@merge-train start", BOT),
            Some(Command::Start)
        );

        // Invalid trigger followed by valid one should find the valid one
        assert_eq!(
            parse_command("foo@merge-train bar @merge-train start", BOT),
            Some(Command::Start)
        );
    }

    #[test]
    fn stop_with_unknown_flags() {
        // Unknown flags are ignored, treated as plain stop
        assert_eq!(
            parse_command("@merge-train stop --unknown", BOT),
            Some(Command::Stop)
        );
        assert_eq!(
            parse_command("@merge-train stop -f", BOT),
            Some(Command::Stop)
        );
        // Only --force is recognized
        assert_eq!(
            parse_command("@merge-train stop -force", BOT),
            Some(Command::Stop)
        );
    }

    // ==================== Real-world comment examples ====================

    #[test]
    fn real_world_comments() {
        // Command at start of comment
        assert_eq!(
            parse_command(
                "@merge-train predecessor #42\n\nThis PR depends on #42",
                BOT
            ),
            Some(Command::Predecessor(PrNumber(42)))
        );

        // Command after explanation
        assert_eq!(
            parse_command("I'm ready to merge this stack.\n\n@merge-train start", BOT),
            Some(Command::Start)
        );

        // Command in middle of text
        assert_eq!(
            parse_command(
                "Please @merge-train stop this cascade, there's an issue",
                BOT
            ),
            Some(Command::Stop)
        );

        // Multiple paragraphs
        assert_eq!(
            parse_command(
                "First paragraph.\n\nSecond paragraph.\n\n@merge-train start\n\nThird.",
                BOT
            ),
            Some(Command::Start)
        );

        // With markdown formatting
        assert_eq!(
            parse_command("**Important**: @merge-train start immediately", BOT),
            Some(Command::Start)
        );

        // In a code fence (still matches - caller should handle context)
        assert_eq!(
            parse_command("```\n@merge-train start\n```", BOT),
            Some(Command::Start)
        );

        // With emoji
        assert_eq!(
            parse_command("Let's go! @merge-train start", BOT),
            Some(Command::Start)
        );

        // After mentioning other users
        assert_eq!(
            parse_command("cc @reviewer @merge-train start", BOT),
            Some(Command::Start)
        );

        // Complex real-world scenario
        assert_eq!(
            parse_command(
                r#"Thanks for the review!

I've addressed all the feedback. The PR is ready.

@merge-train predecessor #123

This builds on top of the auth refactor in #123."#,
                BOT
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

    #[test]
    fn find_trigger_works() {
        // Basic cases
        assert_eq!(find_trigger("@merge-train start", 0, TRIGGER), Some(0));
        assert_eq!(
            find_trigger("hello @merge-train start", 0, TRIGGER),
            Some(6)
        );
        assert_eq!(find_trigger("no trigger here", 0, TRIGGER), None);

        // Case insensitive
        assert_eq!(find_trigger("@MERGE-TRAIN start", 0, TRIGGER), Some(0));
        assert_eq!(find_trigger("@Merge-Train start", 0, TRIGGER), Some(0));

        // Boundary checks
        assert_eq!(find_trigger("foo@merge-train start", 0, TRIGGER), None);
        assert_eq!(find_trigger("(@merge-train start", 0, TRIGGER), Some(1));
        assert_eq!(find_trigger(" @merge-train start", 0, TRIGGER), Some(1));

        // Start offset
        assert_eq!(
            find_trigger("@merge-train @merge-train", TRIGGER.len(), TRIGGER),
            Some(13)
        );

        // Invalid followed by valid
        assert_eq!(
            find_trigger("foo@merge-train @merge-train", 0, TRIGGER),
            Some(16)
        );
    }

    proptest! {
        /// Trigger case variations should all be found.
        #[test]
        fn trigger_case_variations_found(
            case_pattern in proptest::collection::vec(proptest::bool::ANY, TRIGGER.len())
        ) {
            // Build a case-varied version of the trigger
            let varied: String = TRIGGER
                .chars()
                .zip(case_pattern.iter())
                .map(|(c, &upper)| if upper { c.to_ascii_uppercase() } else { c })
                .collect();
            let text = format!("{} start", varied);
            prop_assert_eq!(parse_command(&text, BOT), Some(Command::Start));
        }
    }

    // ==================== Different bot names ====================

    #[test]
    fn different_bot_names() {
        // Works with different bot names
        assert_eq!(
            parse_command("@my-bot start", "my-bot"),
            Some(Command::Start)
        );
        assert_eq!(
            parse_command("@stack-bot predecessor #42", "stack-bot"),
            Some(Command::Predecessor(PrNumber(42)))
        );
        assert_eq!(
            parse_command("@ci-helper stop --force", "ci-helper"),
            Some(Command::StopForce)
        );

        // Case insensitive for different bot names
        assert_eq!(
            parse_command("@MY-BOT start", "my-bot"),
            Some(Command::Start)
        );
        assert_eq!(parse_command("@My-Bot stop", "my-bot"), Some(Command::Stop));

        // Wrong bot name doesn't match
        assert_eq!(parse_command("@merge-train start", "other-bot"), None);
        assert_eq!(parse_command("@my-bot start", "merge-train"), None);
    }
}
