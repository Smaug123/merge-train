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

use crate::effects::{GitHubResponse, RulesetData};

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

/// Input for the dismiss stale approvals check.
///
/// This bundles together all the data needed to perform the check, making
/// the pure check function easy to test.
#[derive(Debug, Clone)]
pub struct DismissStaleApprovalsCheckInput {
    /// The repository's default branch name (e.g., "main").
    pub default_branch: String,
    /// Branch protection response (could be data or Unknown).
    pub branch_protection: GitHubResponse,
    /// Rulesets response (could be data or Unknown).
    pub rulesets: GitHubResponse,
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
        GitHubResponse::BranchProtection(data) => {
            if data.dismiss_stale_reviews {
                return DismissStaleApprovalsCheck::Warning(
                    PreflightWarning::DismissStaleApprovalsEnabled {
                        source: DismissStaleSource::BranchProtection,
                    },
                );
            }
        }
        GitHubResponse::BranchProtectionUnknown => {
            unknown_reasons.push(
                "Branch protection settings could not be queried (404/403) - \
                 may be no protection, insufficient permissions, or missing branch"
                    .to_string(),
            );
        }
        other => {
            // Unexpected response type - treat as unknown
            unknown_reasons.push(format!(
                "Unexpected branch protection response type: {:?}",
                std::mem::discriminant(other)
            ));
        }
    }

    // Check rulesets
    match &input.rulesets {
        GitHubResponse::Rulesets(rulesets) => {
            if let Some(warning) = check_rulesets_for_dismiss_stale(rulesets, &input.default_branch)
            {
                return DismissStaleApprovalsCheck::Warning(warning);
            }
        }
        GitHubResponse::RulesetsUnknown => {
            unknown_reasons.push(
                "Repository rulesets could not be queried (404/403) - \
                 may be no rulesets, insufficient permissions, or unsupported API"
                    .to_string(),
            );
        }
        other => {
            // Unexpected response type - treat as unknown
            unknown_reasons.push(format!(
                "Unexpected rulesets response type: {:?}",
                std::mem::discriminant(other)
            ));
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
/// Patterns use GitHub's ref pattern format:
/// - `refs/heads/main` - exact match
/// - `refs/heads/*` - matches all branches
/// - `~DEFAULT_BRANCH` - special pattern for default branch
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

/// Checks if a branch ref matches a GitHub ref pattern.
///
/// Supports:
/// - Exact match: `refs/heads/main`
/// - Wildcard: `refs/heads/*` (matches any branch)
/// - Wildcard prefix: `refs/heads/feature/*` (matches feature branches)
/// - Default branch marker: `~DEFAULT_BRANCH`
/// - Branch name without prefix: `main` (matches as `refs/heads/main`)
fn pattern_matches(branch_ref: &str, pattern: &str, branch_name: &str) -> bool {
    // Special case: ~DEFAULT_BRANCH matches the default branch
    if pattern == "~DEFAULT_BRANCH" {
        return true; // We're always checking the default branch
    }

    // Normalize pattern to refs/heads/ form if it's just a branch name
    let normalized_pattern = if !pattern.starts_with("refs/") && !pattern.starts_with('~') {
        format!("refs/heads/{}", pattern)
    } else {
        pattern.to_string()
    };

    // Exact match
    if normalized_pattern == branch_ref {
        return true;
    }

    // Wildcard match: pattern ends with /*
    if let Some(prefix) = normalized_pattern.strip_suffix("/*")
        && branch_ref.starts_with(prefix)
        && branch_ref.len() > prefix.len()
    {
        return true;
    }

    // Full wildcard: refs/heads/*
    if normalized_pattern == "refs/heads/*" {
        return true;
    }

    // Also try matching against just the branch name (some patterns may be plain names)
    if pattern == branch_name {
        return true;
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::effects::BranchProtectionData;
    use proptest::prelude::*;

    // ─── Test Helpers ─────────────────────────────────────────────────────────

    fn make_branch_protection(dismiss_stale_reviews: bool) -> GitHubResponse {
        GitHubResponse::BranchProtection(BranchProtectionData {
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
        protection: GitHubResponse,
        rulesets: GitHubResponse,
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
            GitHubResponse::Rulesets(vec![]),
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
            GitHubResponse::Rulesets(vec![]),
        );
        let result = check_dismiss_stale_approvals(&input);
        assert_eq!(result, DismissStaleApprovalsCheck::NotEnabled);
    }

    #[test]
    fn branch_protection_unknown_warns() {
        let input = make_input(
            "main",
            GitHubResponse::BranchProtectionUnknown,
            GitHubResponse::Rulesets(vec![]),
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
            GitHubResponse::Rulesets(vec![ruleset]),
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
            GitHubResponse::Rulesets(vec![ruleset]),
        );
        let result = check_dismiss_stale_approvals(&input);
        assert!(result.has_warning());
    }

    #[test]
    fn ruleset_dismiss_stale_enabled_empty_targets_all() {
        // Empty target_branches means all branches
        let ruleset = make_ruleset("all-branches", true, vec![], vec![]);
        let input = make_input(
            "main",
            make_branch_protection(false),
            GitHubResponse::Rulesets(vec![ruleset]),
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
            GitHubResponse::Rulesets(vec![ruleset]),
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
            GitHubResponse::Rulesets(vec![ruleset]),
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
            GitHubResponse::Rulesets(vec![ruleset]),
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
            GitHubResponse::Rulesets(vec![ruleset]),
        );
        let result = check_dismiss_stale_approvals(&input);
        assert_eq!(result, DismissStaleApprovalsCheck::NotEnabled);
    }

    #[test]
    fn rulesets_unknown_warns() {
        let input = make_input(
            "main",
            make_branch_protection(false),
            GitHubResponse::RulesetsUnknown,
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
            GitHubResponse::BranchProtectionUnknown,
            GitHubResponse::RulesetsUnknown,
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
        assert!(pattern_matches(
            "refs/heads/feature/foo",
            "refs/heads/*",
            "feature/foo"
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
                GitHubResponse::Rulesets(vec![]),
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
                GitHubResponse::Rulesets(vec![]),
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
                GitHubResponse::BranchProtectionUnknown,
                GitHubResponse::Rulesets(vec![]),
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
                GitHubResponse::RulesetsUnknown,
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
                GitHubResponse::Rulesets(vec![ruleset]),
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
