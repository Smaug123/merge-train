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
/// - Commands inside fenced code blocks (``` or ~~~), inline code spans
///   (backticks), or blockquoted lines (`>`) are ignored: quoting or
///   documenting a command must not issue it
/// - Trailing punctuation after a predecessor number is ignored
///   (`#42.` parses as 42), but the number itself must be all digits
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
/// // Quoted commands are not issued:
/// assert_eq!(parse_command("> @merge-train start", "merge-train"), None);
/// assert_eq!(parse_command("`@merge-train start`", "merge-train"), None);
/// ```
pub fn parse_command(text: &str, bot_name: &str) -> Option<Command> {
    let text = mask_non_command_regions(text);
    let trigger = format!("@{}", bot_name);
    // Find all occurrences of @bot_name (case-insensitive, at word boundary)
    let mut search_start = 0;
    while let Some(abs_pos) = find_trigger(&text, search_start, &trigger) {
        let after_trigger = &text[abs_pos + trigger.len()..];

        if let Some(cmd) = try_parse_after_trigger(after_trigger) {
            return Some(cmd);
        }

        // Move past this trigger and continue searching
        search_start = abs_pos + trigger.len();
    }
    None
}

/// Replaces markdown regions in which commands must not be recognised with
/// spaces: fenced code blocks (``` or ~~~), blockquoted lines (`>`), and
/// inline code spans (backticks). Spaces preserve word boundaries for the
/// surrounding text.
fn mask_non_command_regions(text: &str) -> String {
    let mut masked = vec![false; text.len()];
    mask_block_regions(text, &mut masked);
    mask_inline_code_spans(text, &mut masked);

    let mut out = String::with_capacity(text.len());
    for (idx, c) in text.char_indices() {
        if masked[idx] {
            out.push(' ');
        } else {
            out.push(c);
        }
    }
    out
}

fn mask_range(masked: &mut [bool], start: usize, end: usize) {
    for flag in &mut masked[start..end] {
        *flag = true;
    }
}

/// Length of the leading run of '`' or '~' characters (0 if neither).
fn leading_fence_run(s: &str) -> usize {
    match s.chars().next() {
        Some(c @ ('`' | '~')) => s.chars().take_while(|&x| x == c).count(),
        _ => 0,
    }
}

/// Masks fenced code blocks and blockquoted lines.
fn mask_block_regions(text: &str, masked: &mut [bool]) {
    // (fence char, opening run length) while inside a fenced code block
    let mut fence: Option<(char, usize)> = None;
    let mut pos = 0;
    while pos < text.len() {
        let line_end = text[pos..].find('\n').map_or(text.len(), |i| pos + i + 1);
        let line = text[pos..line_end].trim_end_matches(['\n', '\r']);
        let trimmed = line.trim_start();
        let fence_run = leading_fence_run(trimmed);

        match fence {
            Some((ch, open_len)) => {
                // The whole line is code, including the closing fence itself
                mask_range(masked, pos, line_end);
                let closes = trimmed.starts_with(ch)
                    && fence_run >= open_len
                    && trimmed.trim_end().chars().all(|c| c == ch);
                if closes {
                    fence = None;
                }
            }
            None => {
                if trimmed.starts_with('>') {
                    mask_range(masked, pos, line_end);
                } else if fence_run >= 3 {
                    // A backtick fence's info string may not contain backticks
                    // (such a line is inline code, handled in the inline pass)
                    let is_fence = trimmed.starts_with('~') || !trimmed[fence_run..].contains('`');
                    if is_fence {
                        mask_range(masked, pos, line_end);
                        // An unclosed fence swallows the rest of the comment
                        fence = Some((trimmed.chars().next().unwrap(), fence_run));
                    }
                }
            }
        }
        pos = line_end;
    }
}

/// Masks inline code spans: a run of N backticks closed by the next run of
/// exactly N backticks; unmatched runs are literal. Spans are sought only
/// within contiguous unmasked regions, so backticks inside fenced code or
/// blockquotes neither open nor close spans.
fn mask_inline_code_spans(text: &str, masked: &mut [bool]) {
    let mut region_start = 0;
    while region_start < text.len() {
        if masked[region_start] {
            region_start += 1;
            continue;
        }
        let mut region_end = region_start;
        while region_end < text.len() && !masked[region_end] {
            region_end += 1;
        }
        mask_spans_in_segment(text, region_start, region_end, masked);
        region_start = region_end;
    }
}

/// Masks inline code spans within `text[start..end]`.
fn mask_spans_in_segment(text: &str, start: usize, end: usize, masked: &mut [bool]) {
    // Backtick runs as (byte offset, length)
    let bytes = text.as_bytes();
    let mut runs: Vec<(usize, usize)> = Vec::new();
    let mut i = start;
    while i < end {
        if bytes[i] == b'`' {
            let run_start = i;
            while i < end && bytes[i] == b'`' {
                i += 1;
            }
            runs.push((run_start, i - run_start));
        } else {
            i += 1;
        }
    }

    let mut idx = 0;
    while idx < runs.len() {
        let (open_pos, open_len) = runs[idx];
        // The closer must have exactly the opener's length; shorter or longer
        // runs in between are span content
        match (idx + 1..runs.len()).find(|&j| runs[j].1 == open_len) {
            Some(j) => {
                let (close_pos, close_len) = runs[j];
                mask_range(masked, open_pos, close_pos + close_len);
                idx = j + 1;
            }
            None => {
                // An unmatched backtick run is literal text
                idx += 1;
            }
        }
    }
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
    // Extract the number token (up to whitespace or end)
    let (token, _) = split_first_word(text);

    // Trailing punctuation is conversational, not part of the number
    // ("predecessor #42." / "#42,"). Anything else mixed into the token is
    // ambiguous and rejected by the digits check below.
    let num_str = token.trim_end_matches(|c: char| c.is_ascii_punctuation());

    // Digits only: u64::from_str would also accept a leading '+'
    if num_str.is_empty() || !num_str.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }

    // Fails on overflow
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

    // ==================== Markdown context: code and quotes ====================

    #[test]
    fn command_in_code_fence_is_ignored() {
        assert_eq!(parse_command("```\n@merge-train start\n```", BOT), None);
        // With an info string on the opening fence
        assert_eq!(parse_command("```text\n@merge-train stop\n```", BOT), None);
        // Tilde fences
        assert_eq!(parse_command("~~~\n@merge-train start\n~~~", BOT), None);
        // Unclosed fence swallows the rest of the comment
        assert_eq!(parse_command("```\n@merge-train start --force", BOT), None);
    }

    #[test]
    fn command_in_inline_code_is_ignored() {
        assert_eq!(
            parse_command("use `@merge-train start` to begin", BOT),
            None
        );
        // Multi-backtick spans
        assert_eq!(
            parse_command("use ``@merge-train start`` to begin", BOT),
            None
        );
        // An unmatched backtick is literal, not a span opener
        assert_eq!(
            parse_command("a stray ` then @merge-train start", BOT),
            Some(Command::Start)
        );
    }

    #[test]
    fn command_in_blockquote_is_ignored() {
        assert_eq!(parse_command("> @merge-train start", BOT), None);
        assert_eq!(
            parse_command("quoting:\n> @merge-train stop --force\nend", BOT),
            None
        );
        // Indented blockquote marker
        assert_eq!(parse_command("   > @merge-train start", BOT), None);
    }

    #[test]
    fn command_after_fence_closes_is_parsed() {
        assert_eq!(
            parse_command("```\n@merge-train stop\n```\n@merge-train start", BOT),
            Some(Command::Start)
        );
        assert_eq!(
            parse_command("~~~\nexample\n~~~\n\n@merge-train predecessor #7", BOT),
            Some(Command::Predecessor(PrNumber(7)))
        );
    }

    #[test]
    fn command_outside_quote_and_code_is_parsed() {
        assert_eq!(
            parse_command(
                "> someone said @merge-train stop\n\n@merge-train start",
                BOT
            ),
            Some(Command::Start)
        );
        assert_eq!(
            parse_command("see `the docs` then @merge-train start", BOT),
            Some(Command::Start)
        );
    }

    proptest! {
        /// Any backtick/tilde-free text wrapped in a code fence never parses
        /// as a command (the fence cannot be closed early).
        #[test]
        fn prop_fenced_text_never_parses(text in "[^`~]{0,200}") {
            let fenced = format!("```\n{}\n```", text);
            prop_assert_eq!(parse_command(&fenced, BOT), None);
        }

        /// Any single-line text behind a blockquote marker never parses.
        #[test]
        fn prop_blockquoted_line_never_parses(line in "[^\n\r]{0,200}") {
            let quoted = format!("> {}", line);
            prop_assert_eq!(parse_command(&quoted, BOT), None);
        }

        /// Any backtick-free single-line text inside an inline code span never
        /// parses.
        #[test]
        fn prop_inline_code_never_parses(text in "[^`\n\r]{0,200}") {
            let body = format!("see `{}` for details", text);
            prop_assert_eq!(parse_command(&body, BOT), None);
        }
    }

    // ==================== Trailing punctuation after PR number ====================

    #[test]
    fn predecessor_accepts_trailing_punctuation() {
        assert_eq!(
            parse_command("@merge-train predecessor #42.", BOT),
            Some(Command::Predecessor(PrNumber(42)))
        );
        assert_eq!(
            parse_command("@merge-train predecessor #42,", BOT),
            Some(Command::Predecessor(PrNumber(42)))
        );
        assert_eq!(
            parse_command("@merge-train predecessor #42!", BOT),
            Some(Command::Predecessor(PrNumber(42)))
        );
        assert_eq!(
            parse_command("Stacked on #41: @merge-train predecessor #41).", BOT),
            Some(Command::Predecessor(PrNumber(41)))
        );
    }

    #[test]
    fn predecessor_requires_digits_only() {
        // u64::from_str accepts a leading '+'; the parser must not
        assert_eq!(parse_command("@merge-train predecessor #+42", BOT), None);
        // Digits after non-digit junk are ambiguous: reject
        assert_eq!(parse_command("@merge-train predecessor #1.5", BOT), None);
        assert_eq!(parse_command("@merge-train predecessor #-1", BOT), None);
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
