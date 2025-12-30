//! Shared test utilities and arbitrary generators for property-based testing.

use crate::persistence::event::{StateEvent, StateEventPayload};
use crate::types::{CascadePhase, DescendantProgress, PrNumber, Sha, TrainError};
use proptest::prelude::*;

pub fn arb_pr_number() -> impl Strategy<Value = PrNumber> {
    any::<u64>().prop_map(PrNumber)
}

pub fn arb_sha() -> impl Strategy<Value = Sha> {
    "[0-9a-f]{40}".prop_map(|s| Sha::parse(s).unwrap())
}

pub fn arb_train_error() -> impl Strategy<Value = TrainError> {
    ("[a-z_]{1,20}", "[a-zA-Z0-9 ]{1,100}").prop_map(|(t, m)| TrainError::new(t, m))
}

pub fn arb_descendant_progress() -> impl Strategy<Value = DescendantProgress> {
    prop::collection::vec(arb_pr_number(), 0..5).prop_map(DescendantProgress::new)
}

pub fn arb_cascade_phase() -> impl Strategy<Value = CascadePhase> {
    prop_oneof![
        Just(CascadePhase::Idle),
        arb_descendant_progress().prop_map(|p| CascadePhase::Preparing { progress: p }),
        arb_descendant_progress().prop_map(|p| CascadePhase::SquashPending { progress: p }),
        (arb_descendant_progress(), arb_sha()).prop_map(|(p, s)| CascadePhase::Reconciling {
            progress: p,
            squash_sha: s
        }),
        (arb_descendant_progress(), arb_sha()).prop_map(|(p, s)| CascadePhase::CatchingUp {
            progress: p,
            squash_sha: s
        }),
        (arb_descendant_progress(), arb_sha()).prop_map(|(p, s)| CascadePhase::Retargeting {
            progress: p,
            squash_sha: s
        }),
    ]
}

pub fn arb_branch_name() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9/-]{0,50}".prop_map(String::from)
}

pub fn arb_state_event_payload() -> impl Strategy<Value = StateEventPayload> {
    prop_oneof![
        // Train lifecycle
        (arb_pr_number(), arb_pr_number()).prop_map(|(r, c)| StateEventPayload::TrainStarted {
            root_pr: r,
            current_pr: c
        }),
        arb_pr_number().prop_map(|r| StateEventPayload::TrainStopped { root_pr: r }),
        arb_pr_number().prop_map(|r| StateEventPayload::TrainCompleted { root_pr: r }),
        (arb_pr_number(), arb_train_error()).prop_map(|(r, e)| {
            StateEventPayload::TrainAborted {
                root_pr: r,
                error: e,
            }
        }),
        // Phase transitions
        (
            arb_pr_number(),
            arb_pr_number(),
            prop::option::of(arb_pr_number()),
            prop::option::of(arb_sha()),
            arb_cascade_phase()
        )
            .prop_map(|(tr, cp, pp, ls, ph)| StateEventPayload::PhaseTransition {
                train_root: tr,
                current_pr: cp,
                predecessor_pr: pp,
                last_squash_sha: ls,
                phase: ph
            }),
        (arb_pr_number(), arb_pr_number(), arb_sha()).prop_map(|(tr, pr, sh)| {
            StateEventPayload::SquashCommitted {
                train_root: tr,
                pr,
                sha: sh,
            }
        }),
        // Intent/done - prep
        (arb_pr_number(), arb_branch_name(), arb_sha(), arb_sha()).prop_map(
            |(tr, br, pre, exp)| StateEventPayload::IntentPushPrep {
                train_root: tr,
                branch: br,
                pre_push_sha: pre,
                expected_tree: exp
            }
        ),
        (arb_pr_number(), arb_branch_name()).prop_map(|(tr, br)| {
            StateEventPayload::DonePushPrep {
                train_root: tr,
                branch: br,
            }
        }),
        // Intent squash
        (arb_pr_number(), arb_pr_number())
            .prop_map(|(tr, pr)| StateEventPayload::IntentSquash { train_root: tr, pr }),
        // Intent/done - reconcile
        (arb_pr_number(), arb_branch_name(), arb_sha(), arb_sha()).prop_map(
            |(tr, br, pre, exp)| StateEventPayload::IntentPushReconcile {
                train_root: tr,
                branch: br,
                pre_push_sha: pre,
                expected_tree: exp
            }
        ),
        (arb_pr_number(), arb_branch_name()).prop_map(|(tr, br)| {
            StateEventPayload::DonePushReconcile {
                train_root: tr,
                branch: br,
            }
        }),
        // Intent/done - catchup
        (arb_pr_number(), arb_branch_name(), arb_sha(), arb_sha()).prop_map(
            |(tr, br, pre, exp)| StateEventPayload::IntentPushCatchup {
                train_root: tr,
                branch: br,
                pre_push_sha: pre,
                expected_tree: exp
            }
        ),
        (arb_pr_number(), arb_branch_name()).prop_map(|(tr, br)| {
            StateEventPayload::DonePushCatchup {
                train_root: tr,
                branch: br,
            }
        }),
        // Intent/done - retarget
        (arb_pr_number(), arb_pr_number(), arb_branch_name()).prop_map(|(tr, pr, nb)| {
            StateEventPayload::IntentRetarget {
                train_root: tr,
                pr,
                new_base: nb,
            }
        }),
        (arb_pr_number(), arb_pr_number())
            .prop_map(|(tr, pr)| StateEventPayload::DoneRetarget { train_root: tr, pr }),
        // Fan-out
        (
            arb_pr_number(),
            prop::collection::vec(arb_pr_number(), 1..5),
            arb_pr_number()
        )
            .prop_map(|(old, new, orig)| StateEventPayload::FanOutCompleted {
                old_root: old,
                new_roots: new,
                original_root_pr: orig
            }),
        // Non-critical
        (arb_pr_number(), arb_sha()).prop_map(|(pr, sha)| StateEventPayload::PrMerged { pr, sha }),
        (arb_pr_number(), "[a-z]{1,10}".prop_map(String::from))
            .prop_map(|(pr, st)| StateEventPayload::PrStateChanged { pr, state: st }),
        (arb_pr_number(), arb_pr_number()).prop_map(|(pr, pred)| {
            StateEventPayload::PredecessorDeclared {
                pr,
                predecessor: pred,
            }
        }),
    ]
}

pub fn arb_state_event() -> impl Strategy<Value = StateEvent> {
    (any::<u64>(), arb_state_event_payload())
        .prop_map(|(seq, payload)| StateEvent::new(seq, payload))
}
