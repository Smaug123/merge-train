//! Phase transitions for the cascade state machine.
//!
//! Pure functions for computing the next phase based on the current phase
//! and the outcome of operations.

use crate::types::{CascadePhase, DescendantProgress, PrNumber, Sha};

/// The outcome of completing a phase operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhaseOutcome {
    /// All descendants processed successfully in the current phase.
    AllComplete,

    /// A descendant was skipped (PR closed, branch deleted, etc.).
    DescendantSkipped { pr: PrNumber },

    /// A descendant was completed successfully.
    DescendantCompleted { pr: PrNumber },

    /// The squash-merge was performed.
    SquashComplete { squash_sha: Sha },
}

/// Error returned when a phase transition is invalid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransitionError {
    /// The transition is not allowed from the current phase.
    InvalidTransition { from: &'static str, outcome: String },

    /// Missing required data for the transition.
    MissingData { field: &'static str },

    /// Frozen descendants cannot be modified after entering Preparing.
    FrozenDescendantsModified,
}

impl std::fmt::Display for TransitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransitionError::InvalidTransition { from, outcome } => {
                write!(
                    f,
                    "Invalid transition from {} with outcome {}",
                    from, outcome
                )
            }
            TransitionError::MissingData { field } => {
                write!(f, "Missing required field: {}", field)
            }
            TransitionError::FrozenDescendantsModified => {
                write!(
                    f,
                    "Frozen descendants cannot be modified after entering Preparing phase"
                )
            }
        }
    }
}

impl std::error::Error for TransitionError {}

/// Computes the next phase based on the current phase and outcome.
///
/// This is the core state machine transition logic. It ensures:
/// - Phases proceed in order: Idle -> Preparing -> SquashPending -> Reconciling -> CatchingUp -> Retargeting -> Idle
/// - frozen_descendants is preserved through all phases after Preparing
/// - skipped set is preserved and only grows across phases
/// - completed set is reset at phase boundaries (tracks per-phase progress)
///
/// Returns the new phase, or an error if the transition is invalid.
pub fn next_phase(
    current: &CascadePhase,
    outcome: PhaseOutcome,
) -> Result<CascadePhase, TransitionError> {
    match (current, outcome) {
        // === Idle transitions ===

        // Starting a cascade with descendants to prepare
        (CascadePhase::Idle, PhaseOutcome::AllComplete) => {
            // This represents "start with empty descendants" - go directly to SquashPending
            Ok(CascadePhase::SquashPending {
                progress: DescendantProgress::new(vec![]),
            })
        }

        // === Preparing transitions ===

        // A descendant completed preparation
        (CascadePhase::Preparing { progress }, PhaseOutcome::DescendantCompleted { pr }) => {
            let mut new_progress = progress.clone();
            new_progress.mark_completed(pr);

            if new_progress.is_complete() {
                Ok(CascadePhase::SquashPending {
                    progress: new_progress,
                })
            } else {
                Ok(CascadePhase::Preparing {
                    progress: new_progress,
                })
            }
        }

        // A descendant was skipped during preparation
        (CascadePhase::Preparing { progress }, PhaseOutcome::DescendantSkipped { pr }) => {
            let mut new_progress = progress.clone();
            new_progress.mark_skipped(pr);

            if new_progress.is_complete() {
                Ok(CascadePhase::SquashPending {
                    progress: new_progress,
                })
            } else {
                Ok(CascadePhase::Preparing {
                    progress: new_progress,
                })
            }
        }

        // All descendants prepared (explicit signal, alternative to checking is_complete)
        (CascadePhase::Preparing { progress }, PhaseOutcome::AllComplete) => {
            Ok(CascadePhase::SquashPending {
                progress: progress.clone(),
            })
        }

        // === SquashPending transitions ===

        // Squash completed, move to Reconciling
        (CascadePhase::SquashPending { progress }, PhaseOutcome::SquashComplete { squash_sha }) => {
            // Reset completed for reconciliation phase while preserving frozen and skipped
            let mut new_progress = DescendantProgress::new(progress.frozen_descendants.clone());
            new_progress.skipped = progress.skipped.clone();

            if new_progress.is_complete() {
                // No descendants to reconcile, go to CatchingUp
                Ok(CascadePhase::CatchingUp {
                    progress: new_progress,
                    squash_sha,
                })
            } else {
                Ok(CascadePhase::Reconciling {
                    progress: new_progress,
                    squash_sha,
                })
            }
        }

        // NOTE: SquashPending does NOT accept AllComplete. The only valid exit
        // is via SquashComplete, which provides the squash_sha. Even with empty
        // frozen_descendants, the squash must actually happen before transitioning.
        // The SquashComplete arm handles empty descendants by going to CatchingUp
        // (which will immediately complete and proceed to Retargeting -> Idle).

        // === Reconciling transitions ===

        // A descendant completed reconciliation
        (
            CascadePhase::Reconciling {
                progress,
                squash_sha,
            },
            PhaseOutcome::DescendantCompleted { pr },
        ) => {
            let mut new_progress = progress.clone();
            new_progress.mark_completed(pr);

            if new_progress.is_complete() {
                // Reset completed for catch-up phase
                let mut catchup_progress =
                    DescendantProgress::new(progress.frozen_descendants.clone());
                catchup_progress.skipped = new_progress.skipped.clone();

                Ok(CascadePhase::CatchingUp {
                    progress: catchup_progress,
                    squash_sha: squash_sha.clone(),
                })
            } else {
                Ok(CascadePhase::Reconciling {
                    progress: new_progress,
                    squash_sha: squash_sha.clone(),
                })
            }
        }

        // A descendant was skipped during reconciliation
        (
            CascadePhase::Reconciling {
                progress,
                squash_sha,
            },
            PhaseOutcome::DescendantSkipped { pr },
        ) => {
            let mut new_progress = progress.clone();
            new_progress.mark_skipped(pr);

            if new_progress.is_complete() {
                let mut catchup_progress =
                    DescendantProgress::new(progress.frozen_descendants.clone());
                catchup_progress.skipped = new_progress.skipped.clone();

                Ok(CascadePhase::CatchingUp {
                    progress: catchup_progress,
                    squash_sha: squash_sha.clone(),
                })
            } else {
                Ok(CascadePhase::Reconciling {
                    progress: new_progress,
                    squash_sha: squash_sha.clone(),
                })
            }
        }

        // All descendants reconciled
        (
            CascadePhase::Reconciling {
                progress,
                squash_sha,
            },
            PhaseOutcome::AllComplete,
        ) => {
            let mut catchup_progress = DescendantProgress::new(progress.frozen_descendants.clone());
            catchup_progress.skipped = progress.skipped.clone();

            Ok(CascadePhase::CatchingUp {
                progress: catchup_progress,
                squash_sha: squash_sha.clone(),
            })
        }

        // === CatchingUp transitions ===

        // A descendant completed catch-up
        (
            CascadePhase::CatchingUp {
                progress,
                squash_sha,
            },
            PhaseOutcome::DescendantCompleted { pr },
        ) => {
            let mut new_progress = progress.clone();
            new_progress.mark_completed(pr);

            if new_progress.is_complete() {
                let mut retarget_progress =
                    DescendantProgress::new(progress.frozen_descendants.clone());
                retarget_progress.skipped = new_progress.skipped.clone();

                Ok(CascadePhase::Retargeting {
                    progress: retarget_progress,
                    squash_sha: squash_sha.clone(),
                })
            } else {
                Ok(CascadePhase::CatchingUp {
                    progress: new_progress,
                    squash_sha: squash_sha.clone(),
                })
            }
        }

        // A descendant was skipped during catch-up
        (
            CascadePhase::CatchingUp {
                progress,
                squash_sha,
            },
            PhaseOutcome::DescendantSkipped { pr },
        ) => {
            let mut new_progress = progress.clone();
            new_progress.mark_skipped(pr);

            if new_progress.is_complete() {
                let mut retarget_progress =
                    DescendantProgress::new(progress.frozen_descendants.clone());
                retarget_progress.skipped = new_progress.skipped.clone();

                Ok(CascadePhase::Retargeting {
                    progress: retarget_progress,
                    squash_sha: squash_sha.clone(),
                })
            } else {
                Ok(CascadePhase::CatchingUp {
                    progress: new_progress,
                    squash_sha: squash_sha.clone(),
                })
            }
        }

        // All descendants caught up
        (
            CascadePhase::CatchingUp {
                progress,
                squash_sha,
            },
            PhaseOutcome::AllComplete,
        ) => {
            let mut retarget_progress =
                DescendantProgress::new(progress.frozen_descendants.clone());
            retarget_progress.skipped = progress.skipped.clone();

            Ok(CascadePhase::Retargeting {
                progress: retarget_progress,
                squash_sha: squash_sha.clone(),
            })
        }

        // === Retargeting transitions ===

        // A descendant completed retargeting
        (
            CascadePhase::Retargeting {
                progress,
                squash_sha,
            },
            PhaseOutcome::DescendantCompleted { pr },
        ) => {
            let mut new_progress = progress.clone();
            new_progress.mark_completed(pr);

            if new_progress.is_complete() {
                Ok(CascadePhase::Idle)
            } else {
                Ok(CascadePhase::Retargeting {
                    progress: new_progress,
                    squash_sha: squash_sha.clone(),
                })
            }
        }

        // A descendant was skipped during retargeting
        (
            CascadePhase::Retargeting {
                progress,
                squash_sha,
            },
            PhaseOutcome::DescendantSkipped { pr },
        ) => {
            let mut new_progress = progress.clone();
            new_progress.mark_skipped(pr);

            if new_progress.is_complete() {
                Ok(CascadePhase::Idle)
            } else {
                Ok(CascadePhase::Retargeting {
                    progress: new_progress,
                    squash_sha: squash_sha.clone(),
                })
            }
        }

        // All descendants retargeted
        (CascadePhase::Retargeting { .. }, PhaseOutcome::AllComplete) => Ok(CascadePhase::Idle),

        // === Invalid transitions ===
        (phase, outcome) => Err(TransitionError::InvalidTransition {
            from: phase.name(),
            outcome: format!("{:?}", outcome),
        }),
    }
}

/// Creates a new Preparing phase with the given descendants.
///
/// This is the entry point for starting a cascade. The frozen_descendants
/// list is captured here and carried through all subsequent phases.
pub fn start_preparing(descendants: Vec<PrNumber>) -> CascadePhase {
    if descendants.is_empty() {
        CascadePhase::SquashPending {
            progress: DescendantProgress::new(vec![]),
        }
    } else {
        CascadePhase::Preparing {
            progress: DescendantProgress::new(descendants),
        }
    }
}

/// Verifies that a phase transition maintains invariants.
///
/// Checks:
/// - frozen_descendants is unchanged from the previous phase
///
/// Note: completed is reset at phase boundaries (intentionally), so growth
/// is not checked. Skipped set preservation is handled by next_phase logic.
pub fn verify_transition_invariants(
    from: &CascadePhase,
    to: &CascadePhase,
) -> Result<(), TransitionError> {
    // Get progress from both phases (if they have progress)
    let from_progress = from.progress();
    let to_progress = to.progress();

    match (from_progress, to_progress) {
        (Some(from_p), Some(to_p)) => {
            // Frozen descendants must be identical
            if from_p.frozen_descendants != to_p.frozen_descendants {
                return Err(TransitionError::FrozenDescendantsModified);
            }

            // Note: We don't check that completed/skipped only grow here because
            // between phases (e.g., Preparing -> SquashPending -> Reconciling),
            // the completed set is reset. The growth invariant only applies
            // within a single phase.
        }
        (None, Some(to_p)) => {
            // Transitioning from Idle to a phase with progress is valid
            // (this is start_preparing)
            let _ = to_p;
        }
        (Some(_from_p), None) => {
            // Transitioning from a phase with progress to Idle is valid
            // (this is completing a cascade step)
        }
        (None, None) => {
            // Both Idle, nothing to check
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn make_sha(s: &str) -> Sha {
        // Pad to exactly 40 hex characters
        let padded = format!("{:0<40}", s);
        Sha::parse(padded).unwrap()
    }

    fn make_progress(frozen: &[u64]) -> DescendantProgress {
        DescendantProgress::new(frozen.iter().map(|&n| PrNumber(n)).collect())
    }

    mod next_phase_tests {
        use super::*;

        #[test]
        fn idle_to_squash_pending_with_empty_descendants() {
            let result = next_phase(&CascadePhase::Idle, PhaseOutcome::AllComplete);
            assert!(result.is_ok());

            let phase = result.unwrap();
            assert!(matches!(phase, CascadePhase::SquashPending { .. }));
        }

        #[test]
        fn preparing_to_squash_pending_when_all_complete() {
            let progress = make_progress(&[1, 2, 3]);
            let mut complete_progress = progress.clone();
            complete_progress.mark_completed(PrNumber(1));
            complete_progress.mark_completed(PrNumber(2));
            complete_progress.mark_completed(PrNumber(3));

            let phase = CascadePhase::Preparing {
                progress: complete_progress,
            };
            let result = next_phase(&phase, PhaseOutcome::AllComplete);

            assert!(result.is_ok());
            assert!(matches!(
                result.unwrap(),
                CascadePhase::SquashPending { .. }
            ));
        }

        #[test]
        fn preparing_stays_preparing_when_not_all_complete() {
            let progress = make_progress(&[1, 2, 3]);
            let phase = CascadePhase::Preparing { progress };

            let result = next_phase(
                &phase,
                PhaseOutcome::DescendantCompleted { pr: PrNumber(1) },
            );

            assert!(result.is_ok());
            let new_phase = result.unwrap();
            assert!(matches!(new_phase, CascadePhase::Preparing { .. }));

            if let CascadePhase::Preparing { progress: p } = new_phase {
                assert!(p.completed.contains(&PrNumber(1)));
                assert!(!p.is_complete());
            }
        }

        #[test]
        fn squash_pending_to_reconciling_on_squash_complete() {
            let progress = make_progress(&[1, 2]);
            let phase = CascadePhase::SquashPending { progress };
            let sha = make_sha("abc123");

            let result = next_phase(
                &phase,
                PhaseOutcome::SquashComplete {
                    squash_sha: sha.clone(),
                },
            );

            assert!(result.is_ok());
            let new_phase = result.unwrap();
            assert!(matches!(new_phase, CascadePhase::Reconciling { .. }));

            if let CascadePhase::Reconciling {
                squash_sha,
                progress,
            } = new_phase
            {
                assert_eq!(squash_sha, sha);
                assert_eq!(progress.frozen_descendants.len(), 2);
            }
        }

        #[test]
        fn reconciling_to_catching_up_when_all_complete() {
            let mut progress = make_progress(&[1]);
            progress.mark_completed(PrNumber(1));
            let sha = make_sha("abc123");

            let phase = CascadePhase::Reconciling {
                progress,
                squash_sha: sha.clone(),
            };

            let result = next_phase(&phase, PhaseOutcome::AllComplete);

            assert!(result.is_ok());
            assert!(matches!(result.unwrap(), CascadePhase::CatchingUp { .. }));
        }

        #[test]
        fn catching_up_to_retargeting_when_all_complete() {
            let mut progress = make_progress(&[1]);
            progress.mark_completed(PrNumber(1));
            let sha = make_sha("abc123");

            let phase = CascadePhase::CatchingUp {
                progress,
                squash_sha: sha.clone(),
            };

            let result = next_phase(&phase, PhaseOutcome::AllComplete);

            assert!(result.is_ok());
            assert!(matches!(result.unwrap(), CascadePhase::Retargeting { .. }));
        }

        #[test]
        fn retargeting_to_idle_when_all_complete() {
            let mut progress = make_progress(&[1]);
            progress.mark_completed(PrNumber(1));
            let sha = make_sha("abc123");

            let phase = CascadePhase::Retargeting {
                progress,
                squash_sha: sha.clone(),
            };

            let result = next_phase(&phase, PhaseOutcome::AllComplete);

            assert!(result.is_ok());
            assert!(matches!(result.unwrap(), CascadePhase::Idle));
        }

        #[test]
        fn invalid_transition_from_idle_with_descendant_completed() {
            let result = next_phase(
                &CascadePhase::Idle,
                PhaseOutcome::DescendantCompleted { pr: PrNumber(1) },
            );

            assert!(result.is_err());
        }

        #[test]
        fn squash_pending_requires_squash_complete_not_all_complete() {
            // SquashPending must exit via SquashComplete (with squash_sha),
            // not AllComplete. Even with empty descendants, the squash must happen.
            let progress = make_progress(&[]);
            let phase = CascadePhase::SquashPending { progress };

            let result = next_phase(&phase, PhaseOutcome::AllComplete);

            assert!(
                result.is_err(),
                "SquashPending + AllComplete should be invalid"
            );
        }

        #[test]
        fn skipped_descendants_are_preserved_across_phases() {
            let mut progress = make_progress(&[1, 2]);
            progress.mark_skipped(PrNumber(1));
            progress.mark_completed(PrNumber(2));

            let phase = CascadePhase::Preparing { progress };
            let result = next_phase(&phase, PhaseOutcome::AllComplete);

            assert!(result.is_ok());
            if let CascadePhase::SquashPending { progress: p } = result.unwrap() {
                assert!(p.skipped.contains(&PrNumber(1)));
            }
        }
    }

    mod start_preparing_tests {
        use super::*;

        #[test]
        fn with_descendants_returns_preparing() {
            let phase = start_preparing(vec![PrNumber(1), PrNumber(2)]);

            assert!(matches!(phase, CascadePhase::Preparing { .. }));
            if let CascadePhase::Preparing { progress: p } = phase {
                assert_eq!(p.frozen_descendants.len(), 2);
            }
        }

        #[test]
        fn with_empty_descendants_returns_squash_pending() {
            let phase = start_preparing(vec![]);

            assert!(matches!(phase, CascadePhase::SquashPending { .. }));
        }
    }

    mod verify_transition_invariants_tests {
        use super::*;

        #[test]
        fn same_frozen_descendants_is_valid() {
            let progress = make_progress(&[1, 2, 3]);
            let from = CascadePhase::Preparing {
                progress: progress.clone(),
            };
            let to = CascadePhase::SquashPending { progress };

            assert!(verify_transition_invariants(&from, &to).is_ok());
        }

        #[test]
        fn different_frozen_descendants_is_invalid() {
            let from = CascadePhase::Preparing {
                progress: make_progress(&[1, 2, 3]),
            };
            let to = CascadePhase::SquashPending {
                progress: make_progress(&[1, 2, 4]), // Different!
            };

            let result = verify_transition_invariants(&from, &to);
            assert!(matches!(
                result,
                Err(TransitionError::FrozenDescendantsModified)
            ));
        }

        #[test]
        fn idle_to_preparing_is_valid() {
            let from = CascadePhase::Idle;
            let to = CascadePhase::Preparing {
                progress: make_progress(&[1, 2]),
            };

            assert!(verify_transition_invariants(&from, &to).is_ok());
        }

        #[test]
        fn retargeting_to_idle_is_valid() {
            let progress = make_progress(&[1]);
            let from = CascadePhase::Retargeting {
                progress,
                squash_sha: make_sha("abc123"),
            };
            let to = CascadePhase::Idle;

            assert!(verify_transition_invariants(&from, &to).is_ok());
        }
    }

    mod property_tests {
        use super::*;

        fn arb_pr_number() -> impl Strategy<Value = PrNumber> {
            (1u64..100).prop_map(PrNumber)
        }

        fn arb_sha() -> impl Strategy<Value = Sha> {
            "[0-9a-f]{40}".prop_map(|s| Sha::parse(s).unwrap())
        }

        fn arb_descendants() -> impl Strategy<Value = Vec<PrNumber>> {
            prop::collection::vec(arb_pr_number(), 0..10)
        }

        proptest! {
            /// Transitions preserve frozen_descendants through the COMPLETE cascade:
            /// Preparing → SquashPending → Reconciling → CatchingUp → Retargeting → Idle
            #[test]
            fn frozen_descendants_preserved_through_full_cascade(
                descendants in prop::collection::vec(arb_pr_number(), 1..10),
                sha in arb_sha()
            ) {
                // start_preparing with non-empty descendants will create Preparing phase
                let initial = start_preparing(descendants.clone());
                let frozen = match &initial {
                    CascadePhase::Preparing { progress: p } => p.frozen_descendants.clone(),
                    CascadePhase::SquashPending { progress } => progress.frozen_descendants.clone(),
                    _ => Vec::new(),
                };

                // === Phase 1: Preparing ===
                let mut phase = initial;
                for &pr in &descendants {
                    if let CascadePhase::Preparing { .. } = &phase {
                        phase = next_phase(&phase, PhaseOutcome::DescendantCompleted { pr })
                            .expect("Preparing transition should succeed");
                    }
                }
                prop_assert!(
                    matches!(phase, CascadePhase::SquashPending { .. }),
                    "After completing all Preparing, should be in SquashPending"
                );

                // Verify frozen_descendants preserved after Preparing
                if let Some(progress) = phase.progress() {
                    prop_assert_eq!(&progress.frozen_descendants, &frozen, "frozen_descendants changed after Preparing");
                }

                // === Phase 2: SquashPending → Reconciling ===
                phase = next_phase(&phase, PhaseOutcome::SquashComplete { squash_sha: sha.clone() })
                    .expect("SquashPending transition should succeed");
                prop_assert!(
                    matches!(phase, CascadePhase::Reconciling { .. }),
                    "After SquashComplete, should be in Reconciling"
                );

                // Verify frozen_descendants preserved after SquashPending
                if let Some(progress) = phase.progress() {
                    prop_assert_eq!(&progress.frozen_descendants, &frozen, "frozen_descendants changed after SquashPending");
                }

                // === Phase 3: Reconciling → CatchingUp ===
                for &pr in &descendants {
                    if let CascadePhase::Reconciling { .. } = &phase {
                        phase = next_phase(&phase, PhaseOutcome::DescendantCompleted { pr })
                            .expect("Reconciling transition should succeed");
                    }
                }
                prop_assert!(
                    matches!(phase, CascadePhase::CatchingUp { .. }),
                    "After completing all Reconciling, should be in CatchingUp"
                );

                // Verify frozen_descendants preserved after Reconciling
                if let Some(progress) = phase.progress() {
                    prop_assert_eq!(&progress.frozen_descendants, &frozen, "frozen_descendants changed after Reconciling");
                }

                // === Phase 4: CatchingUp → Retargeting ===
                for &pr in &descendants {
                    if let CascadePhase::CatchingUp { .. } = &phase {
                        phase = next_phase(&phase, PhaseOutcome::DescendantCompleted { pr })
                            .expect("CatchingUp transition should succeed");
                    }
                }
                prop_assert!(
                    matches!(phase, CascadePhase::Retargeting { .. }),
                    "After completing all CatchingUp, should be in Retargeting"
                );

                // Verify frozen_descendants preserved after CatchingUp
                if let Some(progress) = phase.progress() {
                    prop_assert_eq!(&progress.frozen_descendants, &frozen, "frozen_descendants changed after CatchingUp");
                }

                // === Phase 5: Retargeting → Idle ===
                for &pr in &descendants {
                    if let CascadePhase::Retargeting { .. } = &phase {
                        phase = next_phase(&phase, PhaseOutcome::DescendantCompleted { pr })
                            .expect("Retargeting transition should succeed");
                    }
                }
                prop_assert!(
                    matches!(phase, CascadePhase::Idle),
                    "After completing all Retargeting, should be in Idle"
                );
            }

            /// Skipped descendants are preserved through all phase transitions.
            #[test]
            fn skipped_preserved_through_transitions(
                descendants in prop::collection::vec(arb_pr_number(), 2..10),
                skip_idx in 0usize..2,
                sha in arb_sha()
            ) {
                let to_skip = descendants[skip_idx % descendants.len()];
                let initial = start_preparing(descendants.clone());

                let mut phase = initial;

                // Skip one, complete the rest
                for (i, &pr) in descendants.iter().enumerate() {
                    if let CascadePhase::Preparing { .. } = &phase {
                        if i == skip_idx % descendants.len() {
                            phase = next_phase(&phase, PhaseOutcome::DescendantSkipped { pr })
                                .expect("transition should succeed");
                        } else {
                            phase = next_phase(&phase, PhaseOutcome::DescendantCompleted { pr })
                                .expect("transition should succeed");
                        }
                    }
                }

                // Move to Reconciling
                if let CascadePhase::SquashPending { .. } = &phase {
                    phase = next_phase(&phase, PhaseOutcome::SquashComplete { squash_sha: sha })
                        .expect("transition should succeed");
                }

                // Check skipped is preserved
                if let Some(progress) = phase.progress() {
                    prop_assert!(progress.skipped.contains(&to_skip));
                }
            }

            /// Phase names never panic.
            #[test]
            fn phase_name_never_panics(
                descendants in arb_descendants(),
                sha in arb_sha()
            ) {
                let phases = [
                    CascadePhase::Idle,
                    start_preparing(descendants.clone()),
                    CascadePhase::SquashPending { progress: DescendantProgress::new(descendants.clone()) },
                    CascadePhase::Reconciling {
                        progress: DescendantProgress::new(descendants.clone()),
                        squash_sha: sha.clone(),
                    },
                    CascadePhase::CatchingUp {
                        progress: DescendantProgress::new(descendants.clone()),
                        squash_sha: sha.clone(),
                    },
                    CascadePhase::Retargeting {
                        progress: DescendantProgress::new(descendants),
                        squash_sha: sha,
                    },
                ];

                for phase in &phases {
                    let _ = phase.name();
                }
            }
        }
    }
}
