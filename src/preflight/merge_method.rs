//! Merge method preflight check.
//!
//! Verifies that the repository is configured for squash-only merges. The bot
//! requires this because:
//!
//! 1. Squash merges create a single commit on the base branch, which is required
//!    for the reconciliation logic to work correctly.
//! 2. Rebase merges would create multiple commits that break parent detection.
//! 3. Merge commits create two-parent commits that complicate the cascade.
//!
//! Per DESIGN.md, this is a "hard requirement" - the bot refuses to start if
//! any non-squash merge method is allowed.

use std::fmt;

use crate::effects::RepoSettingsData;

/// Error returned when the merge method preflight check fails.
///
/// This error includes the current settings and actionable guidance for the user.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreflightError {
    /// Whether squash merging is allowed.
    pub allow_squash_merge: bool,
    /// Whether merge commits are allowed.
    pub allow_merge_commit: bool,
    /// Whether rebase merging is allowed.
    pub allow_rebase_merge: bool,
}

impl PreflightError {
    /// Formats the error as a user-facing message suitable for posting as a comment.
    pub fn format_comment(&self) -> String {
        let squash_status = if self.allow_squash_merge {
            "✓"
        } else {
            "✗"
        };
        let merge_status = if self.allow_merge_commit {
            "✗ (should be disabled)"
        } else {
            "✓"
        };
        let rebase_status = if self.allow_rebase_merge {
            "✗ (should be disabled)"
        } else {
            "✓"
        };

        let mut problems = Vec::new();
        if !self.allow_squash_merge {
            problems.push("squash merge is disabled");
        }
        if self.allow_merge_commit {
            problems.push("merge commits are enabled");
        }
        if self.allow_rebase_merge {
            problems.push("rebase merging is enabled");
        }

        format!(
            r#"Cannot start merge train: this repository must be configured for squash-only merges.

The merge train bot requires:
  {} "Allow squash merging" enabled
  {} "Allow merge commits" disabled
  {} "Allow rebase merging" disabled

Current settings:
  allow_squash_merge: {}
  allow_merge_commit: {}
  allow_rebase_merge: {}

**Issue**: {}

Please update your repository settings at: Settings → General → Pull Requests"#,
            squash_status,
            merge_status,
            rebase_status,
            self.allow_squash_merge,
            self.allow_merge_commit,
            self.allow_rebase_merge,
            problems.join(", ")
        )
    }
}

impl fmt::Display for PreflightError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut issues = Vec::new();
        if !self.allow_squash_merge {
            issues.push("squash merge disabled");
        }
        if self.allow_merge_commit {
            issues.push("allow_merge_commit: true");
        }
        if self.allow_rebase_merge {
            issues.push("allow_rebase_merge: true");
        }
        write!(
            f,
            "Repository not configured for squash-only merges: {}",
            issues.join(", ")
        )
    }
}

impl std::error::Error for PreflightError {}

/// Checks that the repository is configured for squash-only merges.
///
/// Returns `Ok(())` if the configuration is valid (squash-only), or
/// `Err(PreflightError)` if any non-squash merge method is allowed.
///
/// # Requirements (per DESIGN.md)
///
/// - `allow_squash_merge: true`
/// - `allow_merge_commit: false`
/// - `allow_rebase_merge: false`
///
/// # Example
///
/// ```
/// use merge_train::effects::RepoSettingsData;
/// use merge_train::preflight::check_merge_method_preflight;
///
/// // Valid: squash-only
/// let settings = RepoSettingsData {
///     default_branch: "main".to_string(),
///     allow_squash_merge: true,
///     allow_merge_commit: false,
///     allow_rebase_merge: false,
/// };
/// assert!(check_merge_method_preflight(&settings).is_ok());
///
/// // Invalid: merge commits allowed
/// let settings = RepoSettingsData {
///     default_branch: "main".to_string(),
///     allow_squash_merge: true,
///     allow_merge_commit: true,
///     allow_rebase_merge: false,
/// };
/// assert!(check_merge_method_preflight(&settings).is_err());
/// ```
pub fn check_merge_method_preflight(settings: &RepoSettingsData) -> Result<(), PreflightError> {
    let is_squash_only =
        settings.allow_squash_merge && !settings.allow_merge_commit && !settings.allow_rebase_merge;

    if is_squash_only {
        Ok(())
    } else {
        Err(PreflightError {
            allow_squash_merge: settings.allow_squash_merge,
            allow_merge_commit: settings.allow_merge_commit,
            allow_rebase_merge: settings.allow_rebase_merge,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn make_settings(
        allow_squash: bool,
        allow_merge: bool,
        allow_rebase: bool,
    ) -> RepoSettingsData {
        RepoSettingsData {
            default_branch: "main".to_string(),
            allow_squash_merge: allow_squash,
            allow_merge_commit: allow_merge,
            allow_rebase_merge: allow_rebase,
        }
    }

    // ─── Unit Tests ───────────────────────────────────────────────────────────

    #[test]
    fn squash_only_succeeds() {
        let settings = make_settings(true, false, false);
        assert!(check_merge_method_preflight(&settings).is_ok());
    }

    #[test]
    fn merge_commit_allowed_fails() {
        let settings = make_settings(true, true, false);
        let err = check_merge_method_preflight(&settings).unwrap_err();
        assert!(err.allow_merge_commit);
        assert!(!err.allow_rebase_merge);
        assert!(err.allow_squash_merge);
    }

    #[test]
    fn rebase_merge_allowed_fails() {
        let settings = make_settings(true, false, true);
        let err = check_merge_method_preflight(&settings).unwrap_err();
        assert!(!err.allow_merge_commit);
        assert!(err.allow_rebase_merge);
        assert!(err.allow_squash_merge);
    }

    #[test]
    fn squash_disabled_fails() {
        let settings = make_settings(false, false, false);
        let err = check_merge_method_preflight(&settings).unwrap_err();
        assert!(!err.allow_squash_merge);
    }

    #[test]
    fn all_methods_allowed_fails() {
        let settings = make_settings(true, true, true);
        let err = check_merge_method_preflight(&settings).unwrap_err();
        assert!(err.allow_merge_commit);
        assert!(err.allow_rebase_merge);
    }

    // ─── Error Message Tests ──────────────────────────────────────────────────

    #[test]
    fn error_message_mentions_squash_disabled() {
        let err = PreflightError {
            allow_squash_merge: false,
            allow_merge_commit: false,
            allow_rebase_merge: false,
        };
        let msg = err.to_string();
        assert!(
            msg.contains("squash merge disabled"),
            "Error should mention squash disabled: {}",
            msg
        );
    }

    #[test]
    fn error_message_mentions_merge_commit() {
        let err = PreflightError {
            allow_squash_merge: true,
            allow_merge_commit: true,
            allow_rebase_merge: false,
        };
        let msg = err.to_string();
        assert!(
            msg.contains("allow_merge_commit"),
            "Error should mention allow_merge_commit: {}",
            msg
        );
    }

    #[test]
    fn error_message_mentions_rebase() {
        let err = PreflightError {
            allow_squash_merge: true,
            allow_merge_commit: false,
            allow_rebase_merge: true,
        };
        let msg = err.to_string();
        assert!(
            msg.contains("allow_rebase_merge"),
            "Error should mention allow_rebase_merge: {}",
            msg
        );
    }

    #[test]
    fn format_comment_includes_settings_path() {
        let err = PreflightError {
            allow_squash_merge: true,
            allow_merge_commit: true,
            allow_rebase_merge: false,
        };
        let comment = err.format_comment();
        assert!(
            comment.contains("Settings"),
            "Comment should mention settings path: {}",
            comment
        );
        assert!(
            comment.contains("Pull Requests"),
            "Comment should mention Pull Requests settings: {}",
            comment
        );
    }

    #[test]
    fn format_comment_shows_current_values() {
        let err = PreflightError {
            allow_squash_merge: true,
            allow_merge_commit: true,
            allow_rebase_merge: false,
        };
        let comment = err.format_comment();
        assert!(
            comment.contains("allow_squash_merge: true"),
            "Comment should show current settings"
        );
        assert!(
            comment.contains("allow_merge_commit: true"),
            "Comment should show current settings"
        );
        assert!(
            comment.contains("allow_rebase_merge: false"),
            "Comment should show current settings"
        );
    }

    // ─── Property Tests ───────────────────────────────────────────────────────

    proptest! {
        /// Property 7 from DESIGN.md: Preflight check rejects non-squash-only repositories
        #[test]
        fn preflight_rejects_non_squash_only(
            allow_squash: bool,
            allow_merge: bool,
            allow_rebase: bool,
        ) {
            // The bot should only accept repos where squash is the ONLY allowed method
            let should_accept = allow_squash && !allow_merge && !allow_rebase;

            let repo_settings = make_settings(allow_squash, allow_merge, allow_rebase);
            let result = check_merge_method_preflight(&repo_settings);

            if should_accept {
                prop_assert!(result.is_ok(), "Should accept squash-only config");
            } else {
                prop_assert!(result.is_err(), "Should reject non-squash-only config");

                // Verify error message mentions the specific issue
                let err = result.unwrap_err();
                let err_str = err.to_string();
                if !allow_squash {
                    prop_assert!(
                        err_str.contains("squash merge disabled"),
                        "Error should mention squash disabled: {}",
                        err_str
                    );
                } else if allow_merge {
                    prop_assert!(
                        err_str.contains("merge_commit") || err_str.contains("allow_merge_commit"),
                        "Error should mention merge commit: {}",
                        err_str
                    );
                } else if allow_rebase {
                    prop_assert!(
                        err_str.contains("rebase") || err_str.contains("allow_rebase_merge"),
                        "Error should mention rebase: {}",
                        err_str
                    );
                }
            }
        }

        /// Property: Error messages always contain actionable guidance
        #[test]
        fn error_message_contains_guidance(
            allow_squash: bool,
            allow_merge: bool,
            allow_rebase: bool,
        ) {
            // Only test error cases
            let should_accept = allow_squash && !allow_merge && !allow_rebase;
            if should_accept {
                return Ok(());
            }

            let err = PreflightError {
                allow_squash_merge: allow_squash,
                allow_merge_commit: allow_merge,
                allow_rebase_merge: allow_rebase,
            };

            let comment = err.format_comment();

            // Must mention where to find settings
            prop_assert!(
                comment.contains("Settings"),
                "Comment should mention settings: {}",
                comment
            );

            // Must show current values
            prop_assert!(
                comment.contains(&format!("allow_squash_merge: {}", allow_squash)),
                "Comment should show allow_squash_merge value"
            );
            prop_assert!(
                comment.contains(&format!("allow_merge_commit: {}", allow_merge)),
                "Comment should show allow_merge_commit value"
            );
            prop_assert!(
                comment.contains(&format!("allow_rebase_merge: {}", allow_rebase)),
                "Comment should show allow_rebase_merge value"
            );
        }

        /// Property: PreflightError fields match input settings
        #[test]
        fn error_preserves_settings(
            allow_squash: bool,
            allow_merge: bool,
            allow_rebase: bool,
        ) {
            let settings = make_settings(allow_squash, allow_merge, allow_rebase);

            // Only test error cases
            if let Err(err) = check_merge_method_preflight(&settings) {
                prop_assert_eq!(err.allow_squash_merge, allow_squash);
                prop_assert_eq!(err.allow_merge_commit, allow_merge);
                prop_assert_eq!(err.allow_rebase_merge, allow_rebase);
            }
        }
    }
}
