//! Preflight checks for merge train operations.
//!
//! Before starting a train, the bot performs preflight checks to verify repository
//! configuration is compatible. These checks prevent starting a cascade that would
//! inevitably fail due to incompatible settings.
//!
//! Two checks are performed:
//!
//! 1. **Merge method check** (hard requirement): The repository must be configured
//!    for squash-only merges. Other merge methods would break the cascade logic.
//!
//! 2. **Dismiss stale approvals check** (soft warning): If "dismiss stale approvals"
//!    is enabled in branch protection or rulesets, the bot warns but allows proceeding.
//!    However, the cascade will abort on first review dismissal.

pub mod branch_protection;
pub mod merge_method;

pub use branch_protection::{
    DismissStaleApprovalsCheck, DismissStaleApprovalsCheckInput, PreflightWarning,
};
pub use merge_method::{PreflightError, check_merge_method_preflight};
