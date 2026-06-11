//! Dismiss stale approvals preflight check.
//!
//! Checks whether "dismiss stale pull request approvals when new commits are pushed"
//! is enabled in branch protection or rulesets. This setting is **incompatible** with
//! the merge train bot because:
//!
//! 1. The bot pushes merge commits to descendant PR branches during cascade
//! 2. Each push invalidates existing approvals
//! 3. The cascade would stall waiting for re-approval
//!
//! Unlike the merge method check, this is a **soft warning** - the bot proceeds but
//! warns the user. If a review dismissal occurs during cascade, the bot aborts
//! immediately (rather than waiting for re-approval that never comes).
//!
//! Per DESIGN.md, the bot cannot distinguish between:
//! - Branch protection not configured
//! - Insufficient permissions to view protection
//! - Branch doesn't exist
//!
//! When the API returns 404/403, we "warn and proceed" since we can't know for sure.

use std::fmt;

use thiserror::Error;

use crate::effects::{BranchProtectionData, GitHubResponse, RulesetData};

/// Warning returned when dismiss stale approvals is detected or unknown.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreflightWarning {
    /// "Dismiss stale approvals" is explicitly enabled in branch protection.
    DismissStaleApprovalsEnabled {
        /// Where the setting was found.
        source: DismissStaleSource,
    },
    /// Could not determine if dismiss stale approvals is enabled.
    ///
    /// This happens when the API returns 404/403, which could mean no protection,
    /// insufficient permissions, or missing branch. We warn and proceed.
    DismissStaleApprovalsUnknown {
        /// Explanation of why we couldn't determine the setting.
        reason: String,
    },
}

/// Where the dismiss stale approvals setting was found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DismissStaleSource {
    /// Found in branch protection rules.
    BranchProtection,
    /// Found in a repository ruleset.
    Ruleset {
        /// Name of the ruleset with the setting enabled.
        ruleset_name: String,
    },
}

impl PreflightWarning {
    /// Formats the warning as a user-facing message suitable for posting as a comment.
    pub fn format_comment(&self) -> String {
        match self {
            PreflightWarning::DismissStaleApprovalsEnabled { source } => {
                let source_desc = match source {
                    DismissStaleSource::BranchProtection => {
                        "in branch protection settings".to_string()
                    }
                    DismissStaleSource::Ruleset { ruleset_name } => {
                        format!("in ruleset \"{}\"", ruleset_name)
                    }
                };

                format!(
                    r#"**Warning**: This repository has "Dismiss stale pull request approvals when new commits are pushed" enabled {}.

The merge train bot pushes merge commits to PR branches during cascade operations, which will invalidate approvals after each step. If a review is dismissed during cascade, the train will **abort immediately** rather than wait for re-approval.

To avoid mid-cascade aborts, consider either:

1. Disabling "Dismiss stale pull request approvals" in branch protection settings, OR
2. Using a different workflow for stacked PRs in this repository

See: https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches/about-protected-branches#require-pull-request-reviews-before-merging

Proceeding with train start..."#,
                    source_desc
                )
            }
            PreflightWarning::DismissStaleApprovalsUnknown { reason } => {
                format!(
                    r#"**Warning**: Could not determine if "dismiss stale approvals" is enabled.

{}

If this setting is enabled and a review is dismissed during cascade, the train will abort immediately.

Proceeding with train start..."#,
                    reason
                )
            }
        }
    }
}

impl fmt::Display for PreflightWarning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PreflightWarning::DismissStaleApprovalsEnabled { source } => {
                let source_desc = match source {
                    DismissStaleSource::BranchProtection => "branch protection".to_string(),
                    DismissStaleSource::Ruleset { ruleset_name } => {
                        format!("ruleset \"{}\"", ruleset_name)
                    }
                };
                write!(f, "Dismiss stale approvals enabled in {}", source_desc)
            }
            PreflightWarning::DismissStaleApprovalsUnknown { reason } => {
                write!(
                    f,
                    "Could not determine dismiss stale approvals status: {}",
                    reason
                )
            }
        }
    }
}

/// Branch protection state, as needed by the dismiss stale approvals check.
///
/// A typed projection of the relevant [`GitHubResponse`] variants: the pure
/// check function receives exactly the states it can handle, rather than the
/// whole response union.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BranchProtectionInput {
    /// Branch protection settings were fetched successfully.
    Known(BranchProtectionData),
    /// Branch protection could not be queried (404/403): may be no protection,
    /// insufficient permissions, or a missing branch.
    Unknown,
}

/// Ruleset state, as needed by the dismiss stale approvals check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RulesetsInput {
    /// Rulesets were fetched successfully.
    Known(Vec<RulesetData>),
    /// Rulesets could not be queried (404/403): may be no rulesets,
    /// insufficient permissions, or an unsupported API.
    Unknown,
}

/// Error converting [`GitHubResponse`] values into a
/// [`DismissStaleApprovalsCheckInput`].
///
/// A mismatched variant means the interpreter answered a different request
/// than the one issued - a bug, not a user-facing condition.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DismissStaleInputError {
    /// The response supplied for branch protection was not a
    /// branch-protection variant.
    #[error("expected a branch protection response, got {response_type}")]
    UnexpectedBranchProtectionResponse {
        /// Name of the variant actually received.
        response_type: &'static str,
    },
    /// The response supplied for rulesets was not a rulesets variant.
    #[error("expected a rulesets response, got {response_type}")]
    UnexpectedRulesetsResponse {
        /// Name of the variant actually received.
        response_type: &'static str,
    },
}

/// Stable variant name of a [`GitHubResponse`], for error reporting.
fn response_variant_name(response: &GitHubResponse) -> &'static str {
    match response {
        GitHubResponse::Pr(_) => "Pr",
        GitHubResponse::PrList(_) => "PrList",
        GitHubResponse::RecentlyMergedPrList { .. } => "RecentlyMergedPrList",
        GitHubResponse::MergeState(_) => "MergeState",
        GitHubResponse::Merged { .. } => "Merged",
        GitHubResponse::Retargeted => "Retargeted",
        GitHubResponse::CommentPosted { .. } => "CommentPosted",
        GitHubResponse::CommentUpdated => "CommentUpdated",
        GitHubResponse::ReactionAdded => "ReactionAdded",
        GitHubResponse::Comments(_) => "Comments",
        GitHubResponse::BranchProtection(_) => "BranchProtection",
        GitHubResponse::BranchProtectionUnknown => "BranchProtectionUnknown",
        GitHubResponse::Rulesets(_) => "Rulesets",
        GitHubResponse::RulesetsUnknown => "RulesetsUnknown",
        GitHubResponse::RepoSettings(_) => "RepoSettings",
    }
}

/// Input for the dismiss stale approvals check.
///
/// This bundles together all the data needed to perform the check, making
/// the pure check function easy to test. Build it from raw GitHub responses
/// with [`DismissStaleApprovalsCheckInput::from_responses`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DismissStaleApprovalsCheckInput {
    /// The repository's default branch name (e.g., "main").
    pub default_branch: String,
    /// Branch protection state (known data or unknown).
    pub branch_protection: BranchProtectionInput,
    /// Rulesets state (known data or unknown).
    pub rulesets: RulesetsInput,
}

impl DismissStaleApprovalsCheckInput {
    /// Builds the check input from raw GitHub responses.
    ///
    /// This is the single conversion point from the [`GitHubResponse`] union
    /// to the typed input. Mismatched variants are surfaced as a structured
    /// error instead of leaking into user-facing warning text.
    pub fn from_responses(
        default_branch: String,
        branch_protection: GitHubResponse,
        rulesets: GitHubResponse,
    ) -> Result<Self, DismissStaleInputError> {
        let branch_protection = match branch_protection {
            GitHubResponse::BranchProtection(data) => BranchProtectionInput::Known(data),
            GitHubResponse::BranchProtectionUnknown => BranchProtectionInput::Unknown,
            other => {
                return Err(DismissStaleInputError::UnexpectedBranchProtectionResponse {
                    response_type: response_variant_name(&other),
                });
            }
        };
        let rulesets = match rulesets {
            GitHubResponse::Rulesets(data) => RulesetsInput::Known(data),
            GitHubResponse::RulesetsUnknown => RulesetsInput::Unknown,
            other => {
                return Err(DismissStaleInputError::UnexpectedRulesetsResponse {
                    response_type: response_variant_name(&other),
                });
            }
        };
        Ok(DismissStaleApprovalsCheckInput {
            default_branch,
            branch_protection,
            rulesets,
        })
    }
}

/// Result of checking dismiss stale approvals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DismissStaleApprovalsCheck {
    /// No dismiss stale approvals setting detected.
    NotEnabled,
    /// Warning: setting is enabled or unknown.
    Warning(PreflightWarning),
}

impl DismissStaleApprovalsCheck {
    /// Returns true if a warning should be shown to the user.
    pub fn has_warning(&self) -> bool {
        matches!(self, DismissStaleApprovalsCheck::Warning(_))
    }

    /// Returns the warning if present.
    pub fn warning(&self) -> Option<&PreflightWarning> {
        match self {
            DismissStaleApprovalsCheck::NotEnabled => None,
            DismissStaleApprovalsCheck::Warning(w) => Some(w),
        }
    }
}

/// Checks if dismiss stale approvals is enabled.
///
/// This is a pure function that analyzes the branch protection and ruleset
/// data to determine if the setting is enabled.
///
/// # Arguments
///
/// * `input` - The check input containing default branch and API responses
///
/// # Returns
///
/// * `DismissStaleApprovalsCheck::NotEnabled` if the setting is not detected
/// * `DismissStaleApprovalsCheck::Warning` if enabled or unknown
pub fn check_dismiss_stale_approvals(
    input: &DismissStaleApprovalsCheckInput,
) -> DismissStaleApprovalsCheck {
    let mut unknown_reasons = Vec::new();

    // Check branch protection
    match &input.branch_protection {
        BranchProtectionInput::Known(data) => {
            if data.dismiss_stale_reviews {
                return DismissStaleApprovalsCheck::Warning(
                    PreflightWarning::DismissStaleApprovalsEnabled {
                        source: DismissStaleSource::BranchProtection,
                    },
                );
            }
        }
        BranchProtectionInput::Unknown => {
            unknown_reasons.push(
                "Branch protection settings could not be queried (404/403) - \
                 may be no protection, insufficient permissions, or missing branch"
                    .to_string(),
            );
        }
    }

    // Check rulesets
    match &input.rulesets {
        RulesetsInput::Known(rulesets) => {
            if let Some(warning) = check_rulesets_for_dismiss_stale(rulesets, &input.default_branch)
            {
                return DismissStaleApprovalsCheck::Warning(warning);
            }
        }
        RulesetsInput::Unknown => {
            unknown_reasons.push(
                "Repository rulesets could not be queried (404/403) - \
                 may be no rulesets, insufficient permissions, or unsupported API"
                    .to_string(),
            );
        }
    }

    // If we had any unknowns, warn the user
    if !unknown_reasons.is_empty() {
        return DismissStaleApprovalsCheck::Warning(
            PreflightWarning::DismissStaleApprovalsUnknown {
                reason: unknown_reasons.join("\n\n"),
            },
        );
    }

    // Both checks passed and were queryable
    DismissStaleApprovalsCheck::NotEnabled
}

/// Checks rulesets for dismiss stale approvals on push.
///
/// Returns `Some(warning)` if any ruleset has the setting enabled and targets
/// the default branch.
fn check_rulesets_for_dismiss_stale(
    rulesets: &[RulesetData],
    default_branch: &str,
) -> Option<PreflightWarning> {
    for ruleset in rulesets {
        if !ruleset.dismiss_stale_reviews_on_push {
            continue;
        }

        // Check if this ruleset targets the default branch
        if ruleset_targets_branch(ruleset, default_branch) {
            return Some(PreflightWarning::DismissStaleApprovalsEnabled {
                source: DismissStaleSource::Ruleset {
                    ruleset_name: ruleset.name.clone(),
                },
            });
        }
    }

    None
}

/// Checks if a ruleset targets a specific branch.
///
/// A branch is targeted if:
/// 1. It matches any pattern in `target_branches` (or `target_branches` is empty)
/// 2. AND it doesn't match any pattern in `exclude_patterns`
///
/// Patterns use GitHub's ref pattern format (see [`pattern_matches`]):
/// - `refs/heads/main` - exact match
/// - `refs/heads/**` - matches all branches
/// - `~ALL` / `~DEFAULT_BRANCH` - special tokens
fn ruleset_targets_branch(ruleset: &RulesetData, branch: &str) -> bool {
    let branch_ref = format!("refs/heads/{}", branch);

    // Check exclusions first - if excluded, this ruleset doesn't target the branch
    for pattern in &ruleset.exclude_patterns {
        if pattern_matches(&branch_ref, pattern, branch) {
            return false;
        }
    }

    // If no target patterns, the ruleset applies to all branches
    if ruleset.target_branches.is_empty() {
        return true;
    }

    // Check if any target pattern matches
    for pattern in &ruleset.target_branches {
        if pattern_matches(&branch_ref, pattern, branch) {
            return true;
        }
    }

    false
}

/// Checks if a branch ref matches a GitHub ruleset ref pattern.
///
/// GitHub ruleset conditions use fnmatch-style patterns plus special tokens:
/// - `~ALL` matches every branch
/// - `~DEFAULT_BRANCH` matches the repository's default branch
/// - `*` matches any characters within one path segment (never `/`)
/// - `**` matches any characters across segments
///   (`refs/heads/**` is GitHub's canonical "all branches" include)
/// - A pattern without a `refs/` prefix is matched as `refs/heads/{pattern}`
fn pattern_matches(branch_ref: &str, pattern: &str, default_branch: &str) -> bool {
    if pattern == "~ALL" {
        return true;
    }
    if pattern == "~DEFAULT_BRANCH" {
        return branch_ref == format!("refs/heads/{}", default_branch);
    }
    // Git ref names cannot contain '~', so any other '~' token matches nothing
    if pattern.starts_with('~') {
        return false;
    }

    if pattern.starts_with("refs/") {
        glob_match(pattern, branch_ref)
    } else {
        glob_match(&format!("refs/heads/{}", pattern), branch_ref)
    }
}

/// A single element of a glob pattern.
enum GlobToken {
    /// A literal byte.
    Literal(u8),
    /// `*`: any run of non-`/` bytes (possibly empty).
    Star,
    /// `**`: any run of bytes (possibly empty), crossing `/`.
    DoubleStar,
}

/// Splits a pattern into glob tokens. Runs of two or more `*` collapse to
/// `**`.
fn glob_tokens(pattern: &str) -> Vec<GlobToken> {
    let bytes = pattern.as_bytes();
    let mut tokens = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'*' {
            let start = i;
            while i < bytes.len() && bytes[i] == b'*' {
                i += 1;
            }
            tokens.push(if i - start >= 2 {
                GlobToken::DoubleStar
            } else {
                GlobToken::Star
            });
        } else {
            tokens.push(GlobToken::Literal(bytes[i]));
            i += 1;
        }
    }
    tokens
}

/// Hand-rolled fnmatch-style glob matcher: `*` does not cross `/`, `**` does.
/// No escapes or character classes - GitHub ruleset patterns have none.
fn glob_match(pattern: &str, text: &str) -> bool {
    let text = text.as_bytes();
    // dp[j]: the tokens consumed so far can match text[..j]
    let mut dp = vec![false; text.len() + 1];
    dp[0] = true;
    for token in glob_tokens(pattern) {
        let mut next = vec![false; text.len() + 1];
        match token {
            GlobToken::Literal(c) => {
                for j in 1..=text.len() {
                    next[j] = dp[j - 1] && text[j - 1] == c;
                }
            }
            GlobToken::Star => {
                next[0] = dp[0];
                for j in 1..=text.len() {
                    next[j] = dp[j] || (next[j - 1] && text[j - 1] != b'/');
                }
            }
            GlobToken::DoubleStar => {
                next[0] = dp[0];
                for j in 1..=text.len() {
                    next[j] = dp[j] || next[j - 1];
                }
            }
        }
        dp = next;
    }
    dp[text.len()]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::effects::BranchProtectionData;
    use proptest::prelude::*;

    // ─── Test Helpers ─────────────────────────────────────────────────────────

    fn make_branch_protection(dismiss_stale_reviews: bool) -> BranchProtectionInput {
        BranchProtectionInput::Known(BranchProtectionData {
            dismiss_stale_reviews,
            required_status_checks: vec![],
        })
    }

    fn make_ruleset(
        name: &str,
        dismiss_stale: bool,
        target_branches: Vec<&str>,
        exclude_patterns: Vec<&str>,
    ) -> RulesetData {
        RulesetData {
            name: name.to_string(),
            dismiss_stale_reviews_on_push: dismiss_stale,
            target_branches: target_branches.into_iter().map(String::from).collect(),
            exclude_patterns: exclude_patterns.into_iter().map(String::from).collect(),
        }
    }

    fn make_input(
        default_branch: &str,
        protection: BranchProtectionInput,
        rulesets: RulesetsInput,
    ) -> DismissStaleApprovalsCheckInput {
        DismissStaleApprovalsCheckInput {
            default_branch: default_branch.to_string(),
            branch_protection: protection,
            rulesets,
        }
    }

    // ─── Branch Protection Tests ──────────────────────────────────────────────

    #[test]
    fn branch_protection_dismiss_stale_enabled() {
        let input = make_input(
            "main",
            make_branch_protection(true),
            RulesetsInput::Known(vec![]),
        );
        let result = check_dismiss_stale_approvals(&input);
        assert!(matches!(
            result,
            DismissStaleApprovalsCheck::Warning(PreflightWarning::DismissStaleApprovalsEnabled {
                source: DismissStaleSource::BranchProtection
            })
        ));
    }

    #[test]
    fn branch_protection_dismiss_stale_disabled() {
        let input = make_input(
            "main",
            make_branch_protection(false),
            RulesetsInput::Known(vec![]),
        );
        let result = check_dismiss_stale_approvals(&input);
        assert_eq!(result, DismissStaleApprovalsCheck::NotEnabled);
    }

    #[test]
    fn branch_protection_unknown_warns() {
        let input = make_input(
            "main",
            BranchProtectionInput::Unknown,
            RulesetsInput::Known(vec![]),
        );
        let result = check_dismiss_stale_approvals(&input);
        assert!(matches!(
            result,
            DismissStaleApprovalsCheck::Warning(
                PreflightWarning::DismissStaleApprovalsUnknown { .. }
            )
        ));
    }

    // ─── Ruleset Tests ────────────────────────────────────────────────────────

    #[test]
    fn ruleset_dismiss_stale_enabled_targets_default() {
        let ruleset = make_ruleset("protect-main", true, vec!["refs/heads/main"], vec![]);
        let input = make_input(
            "main",
            make_branch_protection(false),
            RulesetsInput::Known(vec![ruleset]),
        );
        let result = check_dismiss_stale_approvals(&input);
        assert!(matches!(
            result,
            DismissStaleApprovalsCheck::Warning(PreflightWarning::DismissStaleApprovalsEnabled {
                source: DismissStaleSource::Ruleset { ruleset_name }
            }) if ruleset_name == "protect-main"
        ));
    }

    #[test]
    fn ruleset_dismiss_stale_enabled_wildcard_targets_all() {
        let ruleset = make_ruleset("all-branches", true, vec!["refs/heads/*"], vec![]);
        let input = make_input(
            "main",
            make_branch_protection(false),
            RulesetsInput::Known(vec![ruleset]),
        );
        let result = check_dismiss_stale_approvals(&input);
        assert!(result.has_warning());
    }

    #[test]
    fn ruleset_dismiss_stale_enabled_double_star_targets_all() {
        // `refs/heads/**` is GitHub's canonical "all branches" include
        for branch in ["main", "feature/foo", "a/b/c"] {
            let ruleset = make_ruleset("all-branches", true, vec!["refs/heads/**"], vec![]);
            let input = make_input(
                branch,
                make_branch_protection(false),
                RulesetsInput::Known(vec![ruleset]),
            );
            let result = check_dismiss_stale_approvals(&input);
            assert!(
                result.has_warning(),
                "refs/heads/** must target branch {:?}",
                branch
            );
        }
    }

    #[test]
    fn ruleset_dismiss_stale_enabled_tilde_all_targets_all() {
        let ruleset = make_ruleset("everything", true, vec!["~ALL"], vec![]);
        let input = make_input(
            "main",
            make_branch_protection(false),
            RulesetsInput::Known(vec![ruleset]),
        );
        let result = check_dismiss_stale_approvals(&input);
        assert!(result.has_warning(), "~ALL must target every branch");
    }

    #[test]
    fn ruleset_dismiss_stale_enabled_midstring_wildcard() {
        let ruleset = make_ruleset("releases", true, vec!["refs/heads/release*"], vec![]);
        let input = make_input(
            "release1",
            make_branch_protection(false),
            RulesetsInput::Known(vec![ruleset.clone()]),
        );
        let result = check_dismiss_stale_approvals(&input);
        assert!(
            result.has_warning(),
            "refs/heads/release* must target release1"
        );

        // Negative: the same ruleset does not target main
        let input = make_input(
            "main",
            make_branch_protection(false),
            RulesetsInput::Known(vec![ruleset]),
        );
        let result = check_dismiss_stale_approvals(&input);
        assert_eq!(
            result,
            DismissStaleApprovalsCheck::NotEnabled,
            "refs/heads/release* must not target main"
        );
    }

    #[test]
    fn ruleset_dismiss_stale_enabled_empty_targets_all() {
        // Empty target_branches means all branches
        let ruleset = make_ruleset("all-branches", true, vec![], vec![]);
        let input = make_input(
            "main",
            make_branch_protection(false),
            RulesetsInput::Known(vec![ruleset]),
        );
        let result = check_dismiss_stale_approvals(&input);
        assert!(result.has_warning());
    }

    #[test]
    fn ruleset_dismiss_stale_enabled_default_branch_marker() {
        let ruleset = make_ruleset("default-only", true, vec!["~DEFAULT_BRANCH"], vec![]);
        let input = make_input(
            "main",
            make_branch_protection(false),
            RulesetsInput::Known(vec![ruleset]),
        );
        let result = check_dismiss_stale_approvals(&input);
        assert!(result.has_warning());
    }

    #[test]
    fn ruleset_dismiss_stale_enabled_but_excluded() {
        // Ruleset targets all branches but excludes main
        let ruleset = make_ruleset(
            "non-main",
            true,
            vec!["refs/heads/*"],
            vec!["refs/heads/main"],
        );
        let input = make_input(
            "main",
            make_branch_protection(false),
            RulesetsInput::Known(vec![ruleset]),
        );
        let result = check_dismiss_stale_approvals(&input);
        assert_eq!(result, DismissStaleApprovalsCheck::NotEnabled);
    }

    #[test]
    fn ruleset_dismiss_stale_enabled_different_branch() {
        // Ruleset only targets 'develop', not 'main'
        let ruleset = make_ruleset("protect-develop", true, vec!["refs/heads/develop"], vec![]);
        let input = make_input(
            "main",
            make_branch_protection(false),
            RulesetsInput::Known(vec![ruleset]),
        );
        let result = check_dismiss_stale_approvals(&input);
        assert_eq!(result, DismissStaleApprovalsCheck::NotEnabled);
    }

    #[test]
    fn ruleset_dismiss_stale_disabled() {
        let ruleset = make_ruleset(
            "protect-main",
            false, // dismiss_stale_reviews_on_push = false
            vec!["refs/heads/main"],
            vec![],
        );
        let input = make_input(
            "main",
            make_branch_protection(false),
            RulesetsInput::Known(vec![ruleset]),
        );
        let result = check_dismiss_stale_approvals(&input);
        assert_eq!(result, DismissStaleApprovalsCheck::NotEnabled);
    }

    #[test]
    fn rulesets_unknown_warns() {
        let input = make_input(
            "main",
            make_branch_protection(false),
            RulesetsInput::Unknown,
        );
        let result = check_dismiss_stale_approvals(&input);
        assert!(matches!(
            result,
            DismissStaleApprovalsCheck::Warning(
                PreflightWarning::DismissStaleApprovalsUnknown { .. }
            )
        ));
    }

    #[test]
    fn both_unknown_warns_with_both_reasons() {
        let input = make_input(
            "main",
            BranchProtectionInput::Unknown,
            RulesetsInput::Unknown,
        );
        let result = check_dismiss_stale_approvals(&input);
        if let DismissStaleApprovalsCheck::Warning(
            PreflightWarning::DismissStaleApprovalsUnknown { reason },
        ) = result
        {
            assert!(
                reason.contains("Branch protection") && reason.contains("rulesets"),
                "Should mention both unknowns: {}",
                reason
            );
        } else {
            panic!("Expected unknown warning");
        }
    }

    // ─── Input Conversion Tests ───────────────────────────────────────────────

    #[test]
    fn from_responses_accepts_known_data() {
        let input = DismissStaleApprovalsCheckInput::from_responses(
            "main".to_string(),
            GitHubResponse::BranchProtection(BranchProtectionData {
                dismiss_stale_reviews: true,
                required_status_checks: vec![],
            }),
            GitHubResponse::Rulesets(vec![]),
        )
        .expect("matching variants must convert");
        assert_eq!(input.default_branch, "main");
        assert!(matches!(
            input.branch_protection,
            BranchProtectionInput::Known(ref data) if data.dismiss_stale_reviews
        ));
        assert_eq!(input.rulesets, RulesetsInput::Known(vec![]));
    }

    #[test]
    fn from_responses_accepts_unknown_responses() {
        let input = DismissStaleApprovalsCheckInput::from_responses(
            "main".to_string(),
            GitHubResponse::BranchProtectionUnknown,
            GitHubResponse::RulesetsUnknown,
        )
        .expect("unknown variants must convert");
        assert_eq!(input.branch_protection, BranchProtectionInput::Unknown);
        assert_eq!(input.rulesets, RulesetsInput::Unknown);
    }

    #[test]
    fn from_responses_rejects_mismatched_branch_protection() {
        let result = DismissStaleApprovalsCheckInput::from_responses(
            "main".to_string(),
            // A rulesets response where branch protection was expected
            GitHubResponse::Rulesets(vec![]),
            GitHubResponse::RulesetsUnknown,
        );
        assert_eq!(
            result,
            Err(DismissStaleInputError::UnexpectedBranchProtectionResponse {
                response_type: "Rulesets"
            })
        );
    }

    #[test]
    fn from_responses_rejects_mismatched_rulesets() {
        let result = DismissStaleApprovalsCheckInput::from_responses(
            "main".to_string(),
            GitHubResponse::BranchProtectionUnknown,
            GitHubResponse::Retargeted,
        );
        assert_eq!(
            result,
            Err(DismissStaleInputError::UnexpectedRulesetsResponse {
                response_type: "Retargeted"
            })
        );
    }

    #[test]
    fn input_error_display_names_the_variant() {
        let err = DismissStaleInputError::UnexpectedRulesetsResponse {
            response_type: "Retargeted",
        };
        assert_eq!(
            err.to_string(),
            "expected a rulesets response, got Retargeted"
        );
    }

    // ─── Pattern Matching Tests ───────────────────────────────────────────────

    #[test]
    fn pattern_matches_exact_ref() {
        assert!(pattern_matches(
            "refs/heads/main",
            "refs/heads/main",
            "main"
        ));
        assert!(!pattern_matches(
            "refs/heads/main",
            "refs/heads/develop",
            "main"
        ));
    }

    #[test]
    fn pattern_matches_wildcard() {
        assert!(pattern_matches("refs/heads/main", "refs/heads/*", "main"));
        // fnmatch semantics: a single `*` does not cross `/`, so
        // `refs/heads/*` matches only top-level branches
        assert!(!pattern_matches(
            "refs/heads/feature/foo",
            "refs/heads/*",
            "feature/foo"
        ));
        // `**` is the all-branches form
        assert!(pattern_matches(
            "refs/heads/feature/foo",
            "refs/heads/**",
            "feature/foo"
        ));
    }

    #[test]
    fn pattern_matches_double_star() {
        for branch in ["main", "feature/foo", "a/b/c"] {
            assert!(
                pattern_matches(&format!("refs/heads/{}", branch), "refs/heads/**", branch),
                "refs/heads/** must match {:?}",
                branch
            );
        }
        // `**` mid-pattern crosses segments
        assert!(pattern_matches(
            "refs/heads/release/a/b/final",
            "refs/heads/release/**/final",
            "release/a/b/final"
        ));
    }

    #[test]
    fn pattern_matches_tilde_all() {
        for branch in ["main", "feature/foo"] {
            assert!(
                pattern_matches(&format!("refs/heads/{}", branch), "~ALL", branch),
                "~ALL must match {:?}",
                branch
            );
        }
    }

    #[test]
    fn pattern_matches_unknown_tilde_token_matches_nothing() {
        assert!(!pattern_matches(
            "refs/heads/main",
            "~SOMETHING_ELSE",
            "main"
        ));
    }

    #[test]
    fn pattern_matches_midstring_wildcard() {
        assert!(pattern_matches(
            "refs/heads/release1",
            "refs/heads/release*",
            "release1"
        ));
        assert!(pattern_matches(
            "refs/heads/release-2.0",
            "refs/heads/release*",
            "release-2.0"
        ));
        assert!(!pattern_matches(
            "refs/heads/main",
            "refs/heads/release*",
            "main"
        ));
        // The single `*` still does not cross a segment boundary
        assert!(!pattern_matches(
            "refs/heads/release/v1",
            "refs/heads/release*",
            "release/v1"
        ));
        // Wildcard in the middle of a segment
        assert!(pattern_matches(
            "refs/heads/v1-release-x",
            "refs/heads/v*-release-*",
            "v1-release-x"
        ));
    }

    #[test]
    fn pattern_matches_prefix_wildcard() {
        assert!(pattern_matches(
            "refs/heads/feature/foo",
            "refs/heads/feature/*",
            "feature/foo"
        ));
        assert!(!pattern_matches(
            "refs/heads/main",
            "refs/heads/feature/*",
            "main"
        ));
    }

    #[test]
    fn pattern_matches_prefix_wildcard_respects_slash_boundary() {
        // Regression test: `release/*` should NOT match `release-1`
        // because the `/` boundary must be respected
        assert!(!pattern_matches(
            "refs/heads/release-1",
            "refs/heads/release/*",
            "release-1"
        ));
        // But it SHOULD match `release/v1`
        assert!(pattern_matches(
            "refs/heads/release/v1",
            "refs/heads/release/*",
            "release/v1"
        ));
    }

    #[test]
    fn pattern_matches_bare_branch_name() {
        // Pattern without refs/heads/ prefix
        assert!(pattern_matches("refs/heads/main", "main", "main"));
        assert!(!pattern_matches("refs/heads/main", "develop", "main"));
    }

    #[test]
    fn pattern_matches_default_branch_marker() {
        assert!(pattern_matches(
            "refs/heads/main",
            "~DEFAULT_BRANCH",
            "main"
        ));
        assert!(pattern_matches(
            "refs/heads/develop",
            "~DEFAULT_BRANCH",
            "develop"
        ));
        // The marker matches only the default branch, not any branch
        assert!(!pattern_matches(
            "refs/heads/feature",
            "~DEFAULT_BRANCH",
            "main"
        ));
    }

    // ─── Warning Message Tests ────────────────────────────────────────────────

    #[test]
    fn warning_message_branch_protection_mentions_settings() {
        let warning = PreflightWarning::DismissStaleApprovalsEnabled {
            source: DismissStaleSource::BranchProtection,
        };
        let msg = warning.format_comment();
        assert!(
            msg.contains("branch protection"),
            "Should mention branch protection: {}",
            msg
        );
        assert!(
            msg.contains("docs.github.com"),
            "Should include documentation link: {}",
            msg
        );
    }

    #[test]
    fn warning_message_ruleset_mentions_name() {
        let warning = PreflightWarning::DismissStaleApprovalsEnabled {
            source: DismissStaleSource::Ruleset {
                ruleset_name: "my-ruleset".to_string(),
            },
        };
        let msg = warning.format_comment();
        assert!(
            msg.contains("my-ruleset"),
            "Should mention ruleset name: {}",
            msg
        );
    }

    #[test]
    fn warning_message_unknown_mentions_proceeding() {
        let warning = PreflightWarning::DismissStaleApprovalsUnknown {
            reason: "Test reason".to_string(),
        };
        let msg = warning.format_comment();
        assert!(
            msg.contains("Proceeding"),
            "Should mention proceeding: {}",
            msg
        );
    }

    // ─── Property Tests ───────────────────────────────────────────────────────

    proptest! {
        /// Property: Branch protection dismiss_stale_reviews=true always triggers warning
        #[test]
        fn prop_dismiss_stale_enabled_always_warns(
            branch in "[a-zA-Z][a-zA-Z0-9_-]{0,20}",
        ) {
            let input = make_input(
                &branch,
                make_branch_protection(true),
                RulesetsInput::Known(vec![]),
            );
            let result = check_dismiss_stale_approvals(&input);
            prop_assert!(
                result.has_warning(),
                "dismiss_stale_reviews=true should always trigger warning"
            );
        }

        /// Property: Branch protection dismiss_stale_reviews=false with no rulesets is NotEnabled
        #[test]
        fn prop_dismiss_stale_disabled_no_rulesets_is_ok(
            branch in "[a-zA-Z][a-zA-Z0-9_-]{0,20}",
        ) {
            let input = make_input(
                &branch,
                make_branch_protection(false),
                RulesetsInput::Known(vec![]),
            );
            let result = check_dismiss_stale_approvals(&input);
            prop_assert_eq!(
                result,
                DismissStaleApprovalsCheck::NotEnabled,
                "No dismiss_stale and no rulesets should be NotEnabled"
            );
        }

        /// Property: BranchProtectionUnknown always triggers warning
        #[test]
        fn prop_branch_protection_unknown_always_warns(
            branch in "[a-zA-Z][a-zA-Z0-9_-]{0,20}",
        ) {
            let input = make_input(
                &branch,
                BranchProtectionInput::Unknown,
                RulesetsInput::Known(vec![]),
            );
            let result = check_dismiss_stale_approvals(&input);
            prop_assert!(
                result.has_warning(),
                "BranchProtectionUnknown should always trigger warning"
            );
        }

        /// Property: RulesetsUnknown always triggers warning (when protection is ok)
        #[test]
        fn prop_rulesets_unknown_always_warns(
            branch in "[a-zA-Z][a-zA-Z0-9_-]{0,20}",
        ) {
            let input = make_input(
                &branch,
                make_branch_protection(false),
                RulesetsInput::Unknown,
            );
            let result = check_dismiss_stale_approvals(&input);
            prop_assert!(
                result.has_warning(),
                "RulesetsUnknown should always trigger warning"
            );
        }

        /// Property: Warning messages always contain actionable information
        #[test]
        fn prop_warning_message_contains_guidance(
            branch in "[a-zA-Z][a-zA-Z0-9_-]{0,20}",
            ruleset_name in "[a-zA-Z][a-zA-Z0-9_-]{0,20}",
        ) {
            // Test branch protection warning
            let bp_warning = PreflightWarning::DismissStaleApprovalsEnabled {
                source: DismissStaleSource::BranchProtection,
            };
            let bp_msg = bp_warning.format_comment();
            prop_assert!(
                bp_msg.contains("branch protection") || bp_msg.contains("settings"),
                "Branch protection warning should mention settings"
            );

            // Test ruleset warning
            let rs_warning = PreflightWarning::DismissStaleApprovalsEnabled {
                source: DismissStaleSource::Ruleset {
                    ruleset_name: ruleset_name.clone(),
                },
            };
            let rs_msg = rs_warning.format_comment();
            prop_assert!(
                rs_msg.contains(&ruleset_name),
                "Ruleset warning should mention ruleset name"
            );

            // Test unknown warning
            let unknown_warning = PreflightWarning::DismissStaleApprovalsUnknown {
                reason: format!("Test reason for {}", branch),
            };
            let unknown_msg = unknown_warning.format_comment();
            prop_assert!(
                unknown_msg.contains("Proceeding"),
                "Unknown warning should mention proceeding"
            );
        }

        /// Property: a glob pattern with no wildcards matches exactly itself
        #[test]
        fn prop_literal_glob_is_exact_match(
            a in "[a-z/]{1,20}",
            b in "[a-z/]{1,20}",
        ) {
            prop_assert_eq!(glob_match(&a, &b), a == b);
        }

        /// Property: `refs/heads/**` matches every branch, slashes or not
        #[test]
        fn prop_double_star_matches_any_branch(
            branch in "[a-z][a-z0-9/_-]{0,30}",
        ) {
            let branch_ref = format!("refs/heads/{}", branch);
            let matches = pattern_matches(&branch_ref, "refs/heads/**", &branch);
            prop_assert!(matches, "refs/heads/** must match {}", branch_ref);
        }

        /// Property: a single `*` never matches across a `/` boundary
        #[test]
        fn prop_single_star_never_crosses_segments(
            branch in "[a-z][a-z0-9_-]{0,10}/[a-z][a-z0-9_-]{0,10}",
        ) {
            let branch_ref = format!("refs/heads/{}", branch);
            let matches = pattern_matches(&branch_ref, "refs/heads/*", &branch);
            prop_assert!(!matches, "refs/heads/* must not match {}", branch_ref);
        }

        /// Property: `~ALL` matches every branch
        #[test]
        fn prop_tilde_all_matches_everything(
            branch in "[a-z][a-z0-9/_-]{0,30}",
        ) {
            let branch_ref = format!("refs/heads/{}", branch);
            let matches = pattern_matches(&branch_ref, "~ALL", &branch);
            prop_assert!(matches, "~ALL must match {}", branch_ref);
        }

        /// Property: Ruleset excluded from default branch doesn't trigger warning
        #[test]
        fn prop_excluded_ruleset_no_warning(
            branch in "[a-zA-Z][a-zA-Z0-9_-]{0,20}",
        ) {
            let ruleset = RulesetData {
                name: "excluded".to_string(),
                dismiss_stale_reviews_on_push: true,
                target_branches: vec!["refs/heads/*".to_string()],
                exclude_patterns: vec![format!("refs/heads/{}", branch)],
            };
            let input = make_input(
                &branch,
                make_branch_protection(false),
                RulesetsInput::Known(vec![ruleset]),
            );
            let result = check_dismiss_stale_approvals(&input);
            prop_assert_eq!(
                result,
                DismissStaleApprovalsCheck::NotEnabled,
                "Excluded branch should not trigger warning"
            );
        }
    }
}
