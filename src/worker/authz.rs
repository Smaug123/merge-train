//! Pure command-authorization decisions (DESIGN §Command authorization).
//!
//! The pure M3 handlers assume commands are already authorized; the worker
//! enforces authorization *before* routing a command into `handle_event`.
//! The decision splits into two pure phases because the maintainer check
//! requires an API lookup (an effect):
//!
//! 1. [`authorize_by_author`] decides from identities alone — the webhook
//!    payload carries both the commenter's and the PR author's user id. It
//!    answers `Allowed`, `Denied`, or `NeedsRole` (the one case where the
//!    collaborator role matters).
//! 2. On `NeedsRole` the worker runs `GetCollaboratorPermission` and feeds
//!    the role to [`authorize_by_role`], which cannot answer `NeedsRole` —
//!    the types make "asked GitHub, still undecided" unrepresentable.
//!
//! | Command       | Who can issue                        |
//! |---------------|--------------------------------------|
//! | `predecessor` | PR author only                       |
//! | `start`       | PR author only                       |
//! | `stop`        | PR author OR repo admin/maintainer   |
//! | `stop --force`| Repository admin only                |

use crate::commands::Command;
use crate::effects::github::CollaboratorRole;

/// Phase-1 decision, from identities alone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthorDecision {
    /// The commenter may run the command.
    Allowed,
    /// The commenter may not run the command; `reason` is the rejection
    /// comment to post.
    Denied {
        /// Human-readable rejection, posted as a PR comment.
        reason: String,
    },
    /// Undecidable from identities: the command admits admin/maintainer
    /// override, so the worker must fetch the commenter's role and call
    /// [`authorize_by_role`].
    NeedsRole,
}

/// Phase-2 decision, from the fetched collaborator role.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoleDecision {
    /// The commenter may run the command.
    Allowed,
    /// The commenter may not run the command; `reason` is the rejection
    /// comment to post.
    Denied {
        /// Human-readable rejection, posted as a PR comment.
        reason: String,
    },
}

/// Decides a command's authorization from the commenter's and PR author's
/// identities (both present in every `issue_comment` payload).
pub fn authorize_by_author(
    command: &Command,
    commenter_id: u64,
    pr_author_id: u64,
) -> AuthorDecision {
    let is_author = commenter_id == pr_author_id;
    match command {
        // Author-only commands: no role overrides these — an admin who wants
        // to drive someone else's stack must be its author or ask them.
        Command::Predecessor(_) | Command::Start => {
            if is_author {
                AuthorDecision::Allowed
            } else {
                AuthorDecision::Denied {
                    reason: format!("Only the PR author can issue `{}`.", command_name(command)),
                }
            }
        }
        // Stop: the author may always halt their own train; anyone else needs
        // admin/maintain (so a stuck train is stoppable when the author is
        // unavailable).
        Command::Stop => {
            if is_author {
                AuthorDecision::Allowed
            } else {
                AuthorDecision::NeedsRole
            }
        }
        // Force-stop is admin-only regardless of authorship: it performs
        // admin actions, so even the author's own train requires the role.
        Command::StopForce => AuthorDecision::NeedsRole,
    }
}

/// Decides a role-gated command from the commenter's fetched repository role.
///
/// Only meaningful after [`authorize_by_author`] answered `NeedsRole`; the
/// author-only commands never reach here (they were already decided).
pub fn authorize_by_role(command: &Command, role: &CollaboratorRole) -> RoleDecision {
    let allowed = match command {
        // Already decided by phase 1; a role never grants author-only rights.
        Command::Predecessor(_) | Command::Start => false,
        Command::Stop => matches!(role, CollaboratorRole::Admin | CollaboratorRole::Maintain),
        Command::StopForce => matches!(role, CollaboratorRole::Admin),
    };
    if allowed {
        RoleDecision::Allowed
    } else {
        RoleDecision::Denied {
            reason: format!(
                "Only {} can issue `{}`.",
                match command {
                    Command::StopForce => "a repository admin",
                    _ => "the PR author or a repository admin/maintainer",
                },
                command_name(command)
            ),
        }
    }
}

/// Decides a predecessor *retraction* — editing the declaring comment so it
/// no longer declares, or deleting it outright. A retraction changes the
/// stack topology exactly like a declaration, so it is author-only too, and
/// no role overrides it (Codex M5 round 3, P1: without this gate, anyone
/// with comment edit/delete rights could reshape the stack and abort an
/// active train).
pub fn authorize_retraction(sender_id: u64, pr_author_id: u64) -> AuthorDecision {
    if sender_id == pr_author_id {
        AuthorDecision::Allowed
    } else {
        AuthorDecision::Denied {
            reason: "Only the PR author can retract a predecessor declaration; \
                     the declaration stands."
                .to_owned(),
        }
    }
}

/// The command's user-facing spelling, for rejection comments.
fn command_name(command: &Command) -> &'static str {
    match command {
        Command::Predecessor(_) => "predecessor",
        Command::Start => "start",
        Command::Stop => "stop",
        Command::StopForce => "stop --force",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PrNumber;
    use proptest::prelude::*;

    const AUTHOR: u64 = 10;
    const STRANGER: u64 = 20;

    fn all_commands() -> Vec<Command> {
        vec![
            Command::Predecessor(PrNumber(5)),
            Command::Start,
            Command::Stop,
            Command::StopForce,
        ]
    }

    fn arb_command() -> impl Strategy<Value = Command> {
        prop_oneof![
            (1u64..1000).prop_map(|n| Command::Predecessor(PrNumber(n))),
            Just(Command::Start),
            Just(Command::Stop),
            Just(Command::StopForce),
        ]
    }

    fn arb_role() -> impl Strategy<Value = CollaboratorRole> {
        prop_oneof![
            Just(CollaboratorRole::Admin),
            Just(CollaboratorRole::Maintain),
            Just(CollaboratorRole::Write),
            Just(CollaboratorRole::Triage),
            Just(CollaboratorRole::Read),
            Just(CollaboratorRole::None),
            "[a-z]{1,12}".prop_map(CollaboratorRole::Other),
        ]
    }

    // ─── The DESIGN table, row by row ───

    #[test]
    fn author_may_declare_predecessor_and_start() {
        for cmd in [Command::Predecessor(PrNumber(5)), Command::Start] {
            assert_eq!(
                authorize_by_author(&cmd, AUTHOR, AUTHOR),
                AuthorDecision::Allowed
            );
        }
    }

    #[test]
    fn non_author_predecessor_and_start_denied_without_role_lookup() {
        for cmd in [Command::Predecessor(PrNumber(5)), Command::Start] {
            assert!(matches!(
                authorize_by_author(&cmd, STRANGER, AUTHOR),
                AuthorDecision::Denied { .. }
            ));
        }
    }

    #[test]
    fn author_may_stop_without_role_lookup() {
        assert_eq!(
            authorize_by_author(&Command::Stop, AUTHOR, AUTHOR),
            AuthorDecision::Allowed
        );
    }

    #[test]
    fn non_author_stop_needs_role() {
        assert_eq!(
            authorize_by_author(&Command::Stop, STRANGER, AUTHOR),
            AuthorDecision::NeedsRole
        );
    }

    #[test]
    fn stop_force_needs_role_even_for_the_author() {
        assert_eq!(
            authorize_by_author(&Command::StopForce, AUTHOR, AUTHOR),
            AuthorDecision::NeedsRole
        );
    }

    #[test]
    fn admin_and_maintain_may_stop() {
        for role in [CollaboratorRole::Admin, CollaboratorRole::Maintain] {
            assert_eq!(
                authorize_by_role(&Command::Stop, &role),
                RoleDecision::Allowed
            );
        }
    }

    #[test]
    fn only_admin_may_stop_force() {
        assert_eq!(
            authorize_by_role(&Command::StopForce, &CollaboratorRole::Admin),
            RoleDecision::Allowed
        );
        assert!(matches!(
            authorize_by_role(&Command::StopForce, &CollaboratorRole::Maintain),
            RoleDecision::Denied { .. }
        ));
    }

    proptest! {
        /// Author-only commands never defer to a role lookup: phase 1 fully
        /// decides them, and phase 2 (were it ever consulted) denies.
        #[test]
        fn author_only_commands_never_need_role(
            commenter in 1u64..100, author in 1u64..100, role in arb_role(),
        ) {
            for cmd in [Command::Predecessor(PrNumber(5)), Command::Start] {
                prop_assert_ne!(
                    authorize_by_author(&cmd, commenter, author),
                    AuthorDecision::NeedsRole
                );
                let role_denies = matches!(
                    authorize_by_role(&cmd, &role),
                    RoleDecision::Denied { .. }
                );
                prop_assert!(role_denies);
            }
        }

        /// No role below maintain — and no custom role, whatever its name —
        /// ever authorizes anything.
        #[test]
        fn weak_and_custom_roles_authorize_nothing(cmd in arb_command(), role in arb_role()) {
            prop_assume!(!matches!(
                role,
                CollaboratorRole::Admin | CollaboratorRole::Maintain
            ));
            let denied = matches!(
                authorize_by_role(&cmd, &role),
                RoleDecision::Denied { .. }
            );
            prop_assert!(denied);
        }

        /// The identity comparison is exact: phase 1 allows a non-stop-force
        /// command iff commenter == author (or defers to the role).
        #[test]
        fn phase1_allows_only_the_author(commenter in 1u64..100, author in 1u64..100) {
            for cmd in all_commands() {
                let decision = authorize_by_author(&cmd, commenter, author);
                if decision == AuthorDecision::Allowed {
                    prop_assert_eq!(commenter, author);
                    prop_assert_ne!(cmd, Command::StopForce);
                }
            }
        }
    }
}
