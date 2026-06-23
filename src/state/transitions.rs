//! Phase transitions for the cascade state machine.
//!
//! Pure functions for computing the next phase based on the current phase
//! and the outcome of operations.

use crate::types::{CascadePhase, DescendantProgress, PhaseKind, PrNumber, ProgressError, Sha};

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

    /// AllComplete was signaled but progress.is_complete() is false.
    /// This indicates a bug in the caller - remaining descendants were not processed.
    IncompleteProgress {
        phase: &'static str,
        remaining_count: usize,
    },

    /// A descendant was marked in a way the progress invariants forbid
    /// (not in the frozen set, or completing a skipped PR / skipping a
    /// completed one). Indicates a bug in the caller.
    InvalidDescendant {
        phase: &'static str,
        error: ProgressError,
    },
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
            TransitionError::IncompleteProgress {
                phase,
                remaining_count,
            } => {
                write!(
                    f,
                    "AllComplete signaled in {} phase but {} descendants remain unprocessed",
                    phase, remaining_count
                )
            }
            TransitionError::InvalidDescendant { phase, error } => {
                write!(
                    f,
                    "invalid descendant marking in {} phase: {}",
                    phase, error
                )
            }
        }
    }
}

impl std::error::Error for TransitionError {}

/// Computes the next phase based on the current phase and outcome.
///
/// This is the core state machine transition logic. The phase order is
/// defined once, in [`PhaseKind::ORDER`]; this function only knows the
/// per-phase rules:
/// - Descendant-processing phases (Preparing, Reconciling, CatchingUp,
///   Retargeting) accept `DescendantCompleted`/`DescendantSkipped` for PRs in
///   the frozen set, and `AllComplete` as an assertion that nothing remains.
/// - `SquashPending` exits only via `SquashComplete`, which supplies the
///   squash SHA. Even with no descendants, the squash must actually happen.
/// - `Idle` accepts no outcomes; cascades start via [`start_preparing`].
/// - A phase whose work is already complete is never parked in: the cascade
///   settles forward (possibly several phases, possibly to Idle).
///
/// Invariants maintained: `frozen_descendants` is preserved verbatim through
/// every transition; `skipped` only grows; `completed` is cleared when
/// entering each post-squash processing phase (it tracks per-phase progress).
pub fn next_phase(
    current: &CascadePhase,
    outcome: PhaseOutcome,
) -> Result<CascadePhase, TransitionError> {
    match (current, outcome) {
        (CascadePhase::SquashPending { progress }, PhaseOutcome::SquashComplete { squash_sha }) => {
            enter(PhaseKind::Reconciling, progress.clone(), Some(&squash_sha))
        }

        (phase, PhaseOutcome::DescendantCompleted { pr })
            if phase.kind().processes_descendants() =>
        {
            let mut progress = expect_progress(phase).clone();
            progress
                .mark_completed(pr)
                .map_err(|error| TransitionError::InvalidDescendant {
                    phase: phase.name(),
                    error,
                })?;
            settle(phase.kind(), progress, phase.squash_sha())
        }

        (phase, PhaseOutcome::DescendantSkipped { pr }) if phase.kind().processes_descendants() => {
            let mut progress = expect_progress(phase).clone();
            progress
                .mark_skipped(pr)
                .map_err(|error| TransitionError::InvalidDescendant {
                    phase: phase.name(),
                    error,
                })?;
            settle(phase.kind(), progress, phase.squash_sha())
        }

        (phase, PhaseOutcome::AllComplete) if phase.kind().processes_descendants() => {
            let progress = expect_progress(phase);
            if !progress.is_complete() {
                return Err(TransitionError::IncompleteProgress {
                    phase: phase.name(),
                    remaining_count: progress.remaining().count(),
                });
            }
            settle(phase.kind(), progress.clone(), phase.squash_sha())
        }

        (phase, outcome) => Err(TransitionError::InvalidTransition {
            from: phase.name(),
            outcome: format!("{:?}", outcome),
        }),
    }
}

fn expect_progress(phase: &CascadePhase) -> &DescendantProgress {
    phase
        .progress()
        .expect("descendant-processing phases carry progress")
}

/// Wraps `progress` back into `kind` if work remains; otherwise advances to
/// the successor phase via [`enter`].
fn settle(
    kind: PhaseKind,
    progress: DescendantProgress,
    squash_sha: Option<&Sha>,
) -> Result<CascadePhase, TransitionError> {
    if progress.is_complete() {
        let next = kind
            .successor()
            .expect("settle is only called for active phases");
        enter(next, progress, squash_sha)
    } else {
        make_phase(kind, progress, squash_sha)
    }
}

/// Enters `kind` afresh. Post-squash processing phases start with a cleared
/// per-phase completion ledger; a phase entered with no remaining work is
/// settled straight through to its successor. `SquashPending` always parks
/// (its exit is the squash itself, not descendant bookkeeping); `Idle` is
/// terminal.
fn enter(
    kind: PhaseKind,
    progress: DescendantProgress,
    squash_sha: Option<&Sha>,
) -> Result<CascadePhase, TransitionError> {
    match kind {
        PhaseKind::Idle | PhaseKind::SquashPending => make_phase(kind, progress, squash_sha),
        PhaseKind::Preparing => settle(kind, progress, squash_sha),
        PhaseKind::Reconciling | PhaseKind::CatchingUp | PhaseKind::Retargeting => {
            settle(kind, progress.with_completed_reset(), squash_sha)
        }
    }
}

fn make_phase(
    kind: PhaseKind,
    progress: DescendantProgress,
    squash_sha: Option<&Sha>,
) -> Result<CascadePhase, TransitionError> {
    let require_sha = || {
        squash_sha.cloned().ok_or(TransitionError::MissingData {
            field: "squash_sha",
        })
    };
    match kind {
        PhaseKind::Idle => Ok(CascadePhase::Idle),
        PhaseKind::Preparing => Ok(CascadePhase::Preparing { progress }),
        PhaseKind::SquashPending => Ok(CascadePhase::SquashPending { progress }),
        PhaseKind::Reconciling => Ok(CascadePhase::Reconciling {
            progress,
            squash_sha: require_sha()?,
        }),
        PhaseKind::CatchingUp => Ok(CascadePhase::CatchingUp {
            progress,
            squash_sha: require_sha()?,
        }),
        PhaseKind::Retargeting => Ok(CascadePhase::Retargeting {
            progress,
            squash_sha: require_sha()?,
        }),
    }
}

/// Creates the starting phase for a cascade with the given descendants.
///
/// The frozen_descendants list is captured here and carried through all
/// subsequent phases. With no descendants there is no preparation work, so
/// the cascade starts at SquashPending.
pub fn start_preparing(descendants: Vec<PrNumber>) -> CascadePhase {
    enter(
        PhaseKind::Preparing,
        DescendantProgress::new(descendants),
        None,
    )
    .expect("no path from Preparing to a squash-sha-bearing phase")
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
    // Entering from Idle and returning to Idle are unconstrained; between two
    // progress-bearing phases the frozen set must be byte-identical.
    // (completed resets at phase boundaries by design, so growth is only an
    // intra-phase invariant and is not checked here.)
    if let (Some(from_p), Some(to_p)) = (from.progress(), to.progress())
        && from_p.frozen_descendants() != to_p.frozen_descendants()
    {
        return Err(TransitionError::FrozenDescendantsModified);
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
        fn preparing_to_squash_pending_when_all_complete() {
            let progress = make_progress(&[1, 2, 3]);
            let mut complete_progress = progress.clone();
            complete_progress.mark_completed(PrNumber(1)).unwrap();
            complete_progress.mark_completed(PrNumber(2)).unwrap();
            complete_progress.mark_completed(PrNumber(3)).unwrap();

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
                assert!(p.completed().contains(&PrNumber(1)));
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
                assert_eq!(progress.frozen_descendants().len(), 2);
            }
        }

        #[test]
        fn reconciling_to_catching_up_when_all_complete() {
            let mut progress = make_progress(&[1]);
            progress.mark_completed(PrNumber(1)).unwrap();
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
            progress.mark_completed(PrNumber(1)).unwrap();
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
            progress.mark_completed(PrNumber(1)).unwrap();
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
        fn idle_accepts_no_outcomes() {
            // Cascade entry goes through start_preparing, never through next_phase.
            // (Idle, AllComplete) used to be a back door meaning "start with empty
            // descendants"; that is start_preparing(vec![])'s job.
            let result = next_phase(&CascadePhase::Idle, PhaseOutcome::AllComplete);
            assert!(result.is_err());
        }

        #[test]
        fn squash_complete_with_nothing_left_advances_to_idle() {
            // All frozen descendants were skipped during Preparing: after the
            // squash there is no reconcile/catch-up/retarget work, so the train
            // settles straight back to Idle rather than parking in a
            // trivially-complete intermediate phase.
            let mut progress = make_progress(&[1]);
            progress.mark_skipped(PrNumber(1)).unwrap();
            let phase = CascadePhase::SquashPending { progress };

            let result = next_phase(
                &phase,
                PhaseOutcome::SquashComplete {
                    squash_sha: make_sha("abc123"),
                },
            );

            assert!(matches!(result, Ok(CascadePhase::Idle)));
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
            progress.mark_skipped(PrNumber(1)).unwrap();
            progress.mark_completed(PrNumber(2)).unwrap();

            let phase = CascadePhase::Preparing { progress };
            let result = next_phase(&phase, PhaseOutcome::AllComplete);

            assert!(result.is_ok());
            if let CascadePhase::SquashPending { progress: p } = result.unwrap() {
                assert!(p.skipped().contains(&PrNumber(1)));
            }
        }

        // === AllComplete guard tests ===

        #[test]
        fn preparing_all_complete_fails_when_not_complete() {
            // Progress has 3 descendants, none completed
            let progress = make_progress(&[1, 2, 3]);
            let phase = CascadePhase::Preparing { progress };

            let result = next_phase(&phase, PhaseOutcome::AllComplete);

            assert!(matches!(
                result,
                Err(TransitionError::IncompleteProgress {
                    phase: "preparing",
                    remaining_count: 3,
                })
            ));
        }

        #[test]
        fn reconciling_all_complete_fails_when_not_complete() {
            let progress = make_progress(&[1, 2]);
            let sha = make_sha("abc123");
            let phase = CascadePhase::Reconciling {
                progress,
                squash_sha: sha,
            };

            let result = next_phase(&phase, PhaseOutcome::AllComplete);

            assert!(matches!(
                result,
                Err(TransitionError::IncompleteProgress {
                    phase: "reconciling",
                    remaining_count: 2,
                })
            ));
        }

        #[test]
        fn catching_up_all_complete_fails_when_not_complete() {
            let mut progress = make_progress(&[1, 2, 3]);
            progress.mark_completed(PrNumber(1)).unwrap(); // Only 1 of 3 completed
            let sha = make_sha("abc123");
            let phase = CascadePhase::CatchingUp {
                progress,
                squash_sha: sha,
            };

            let result = next_phase(&phase, PhaseOutcome::AllComplete);

            assert!(matches!(
                result,
                Err(TransitionError::IncompleteProgress {
                    phase: "catching_up",
                    remaining_count: 2,
                })
            ));
        }

        #[test]
        fn retargeting_all_complete_fails_when_not_complete() {
            let progress = make_progress(&[1]);
            let sha = make_sha("abc123");
            let phase = CascadePhase::Retargeting {
                progress,
                squash_sha: sha,
            };

            let result = next_phase(&phase, PhaseOutcome::AllComplete);

            assert!(matches!(
                result,
                Err(TransitionError::IncompleteProgress {
                    phase: "retargeting",
                    remaining_count: 1,
                })
            ));
        }

        #[test]
        fn all_complete_with_empty_frozen_descendants_succeeds() {
            // Empty frozen_descendants means is_complete() is true
            let progress = make_progress(&[]);
            let sha = make_sha("abc123");

            // Reconciling with no descendants should succeed
            let phase = CascadePhase::Reconciling {
                progress: progress.clone(),
                squash_sha: sha.clone(),
            };
            let result = next_phase(&phase, PhaseOutcome::AllComplete);
            assert!(result.is_ok());

            // CatchingUp with no descendants should succeed
            let phase = CascadePhase::CatchingUp {
                progress: progress.clone(),
                squash_sha: sha.clone(),
            };
            let result = next_phase(&phase, PhaseOutcome::AllComplete);
            assert!(result.is_ok());

            // Retargeting with no descendants should succeed
            let phase = CascadePhase::Retargeting {
                progress,
                squash_sha: sha,
            };
            let result = next_phase(&phase, PhaseOutcome::AllComplete);
            assert!(result.is_ok());
        }
    }

    mod start_preparing_tests {
        use super::*;

        #[test]
        fn with_descendants_returns_preparing() {
            let phase = start_preparing(vec![PrNumber(1), PrNumber(2)]);

            assert!(matches!(phase, CascadePhase::Preparing { .. }));
            if let CascadePhase::Preparing { progress: p } = phase {
                assert_eq!(p.frozen_descendants().len(), 2);
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

        /// Generate a vector of unique PR numbers (no duplicates).
        /// Duplicates would cause issues because progress tracking uses HashSets,
        /// so completing PR #X once completes all instances of #X in frozen_descendants.
        fn arb_unique_descendants(min: usize, max: usize) -> impl Strategy<Value = Vec<PrNumber>> {
            prop::collection::hash_set(arb_pr_number(), min..max)
                .prop_map(|set| set.into_iter().collect())
        }

        proptest! {
            /// Transitions preserve frozen_descendants through the COMPLETE cascade:
            /// Preparing → SquashPending → Reconciling → CatchingUp → Retargeting → Idle
            #[test]
            fn frozen_descendants_preserved_through_full_cascade(
                descendants in arb_unique_descendants(1, 10),
                sha in arb_sha()
            ) {
                // start_preparing with non-empty descendants will create Preparing phase
                let initial = start_preparing(descendants.clone());
                let frozen = match &initial {
                    CascadePhase::Preparing { progress: p } => p.frozen_descendants().to_vec(),
                    CascadePhase::SquashPending { progress } => progress.frozen_descendants().to_vec(),
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
                    prop_assert_eq!(progress.frozen_descendants(), &frozen[..], "frozen_descendants changed after Preparing");
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
                    prop_assert_eq!(progress.frozen_descendants(), &frozen[..], "frozen_descendants changed after SquashPending");
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
                    prop_assert_eq!(progress.frozen_descendants(), &frozen[..], "frozen_descendants changed after Reconciling");
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
                    prop_assert_eq!(progress.frozen_descendants(), &frozen[..], "frozen_descendants changed after CatchingUp");
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
                descendants in arb_unique_descendants(2, 10),
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
                    prop_assert!(progress.skipped().contains(&to_skip));
                }
            }

            /// Driving a cascade with arbitrary complete/skip decisions:
            /// every observable transition moves forward in the phase order
            /// (and satisfies can_transition_to), the frozen set is preserved
            /// verbatim, skipped only grows, and the cascade terminates Idle.
            ///
            /// This also exercises the never-parked-complete invariant: in any
            /// non-Idle, non-SquashPending phase there is always at least one
            /// remaining descendant (the `.expect` below would panic otherwise).
            #[test]
            fn cascade_walk_is_forward_only(
                descendants in arb_unique_descendants(1, 8),
                skip_mask in any::<u8>(),
                sha in arb_sha()
            ) {
                let mut phase = start_preparing(descendants.clone());
                let mut steps = 0;
                while !matches!(phase, CascadePhase::Idle) {
                    steps += 1;
                    prop_assert!(steps < 100, "cascade did not terminate");

                    let outcome = match &phase {
                        CascadePhase::SquashPending { .. } => PhaseOutcome::SquashComplete {
                            squash_sha: sha.clone(),
                        },
                        p => {
                            let progress = p.progress().expect("active phases carry progress");
                            let pr = *progress
                                .remaining()
                                .next()
                                .expect("non-SquashPending active phases always have work");
                            if skip_mask & (1 << (pr.0 % 8)) != 0 {
                                PhaseOutcome::DescendantSkipped { pr }
                            } else {
                                PhaseOutcome::DescendantCompleted { pr }
                            }
                        }
                    };

                    let next = next_phase(&phase, outcome).expect("valid outcome");

                    if next.kind() != phase.kind() {
                        prop_assert!(
                            phase.can_transition_to(&next),
                            "{} -> {} violates the phase order",
                            phase.name(),
                            next.name()
                        );
                    }
                    if let (Some(a), Some(b)) = (phase.progress(), next.progress()) {
                        prop_assert_eq!(a.frozen_descendants(), b.frozen_descendants());
                        prop_assert!(a.skipped().is_subset(b.skipped()));
                    }
                    phase = next;
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
