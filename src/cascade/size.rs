//! How large a merge train may be.
//!
//! The limit exists because the status comment is the train's backup state
//! store: a train's whole member list lives in that comment's JSON, and
//! nothing truncates it (only the error fields are truncatable). A train
//! too large to write down would abort *mid-cascade* the first time its
//! status comment had to be posted, which is the expensive moment to find
//! out. So the size is checked once, when the train is asked to start, and
//! the cap is chosen so that every train it admits can be written down.
//!
//! The cap is an operator's to set (`MERGE_TRAIN_MAX_STACK_SIZE`), but not
//! to set freely: above
//! [`MAX_SUPPORTED_TRAIN_SIZE`](crate::status::MAX_SUPPORTED_TRAIN_SIZE) the
//! bot could admit a train it cannot record, which is the failure the cap
//! exists to prevent. A configuration that would do that is refused at
//! startup rather than honoured into an abort much later.

use thiserror::Error;

use crate::status::MAX_SUPPORTED_TRAIN_SIZE;

/// The maximum number of PRs in one train: its root plus all transitive
/// descendants.
///
/// Parsed, not validated: holding one is proof that a train of this size
/// has a status comment to live in, so the engine can check a train against
/// it without rechecking where the number came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TrainSizeCap(usize);

/// Why a configured cap was refused.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum InvalidTrainSizeCap {
    /// Zero admits no train at all, not even a lone root PR, so every
    /// `@merge-train start` would be rejected. That is a misconfiguration,
    /// not a way to switch the bot off.
    #[error("a train size cap of zero would refuse every train, including a single PR")]
    Zero,

    /// Above the ceiling the bot could start a train whose status comment
    /// cannot be written, which is an abort mid-cascade instead of a
    /// refusal at the start.
    #[error(
        "a train size cap of {requested} is above {MAX_SUPPORTED_TRAIN_SIZE}, the largest \
         train whose status comment is guaranteed to fit in a GitHub comment"
    )]
    AboveCeiling {
        /// What was asked for.
        requested: usize,
    },
}

impl TrainSizeCap {
    /// The cap in force when nothing is configured.
    ///
    /// Well below [`MAX_SUPPORTED_TRAIN_SIZE`]: a stack of fifty is already
    /// far past what a human reviews in one go, and the cascade runs CI
    /// quadratically often in the train's length, so the default is chosen
    /// for the cost of running a train rather than for the cost of writing
    /// one down.
    pub const DEFAULT: Self = TrainSizeCap(50);

    /// Parses a configured cap.
    pub fn new(cap: usize) -> Result<Self, InvalidTrainSizeCap> {
        match cap {
            0 => Err(InvalidTrainSizeCap::Zero),
            n if n > MAX_SUPPORTED_TRAIN_SIZE => {
                Err(InvalidTrainSizeCap::AboveCeiling { requested: n })
            }
            n => Ok(TrainSizeCap(n)),
        }
    }

    /// The cap as a number, for comparing a train's size against it.
    pub fn get(self) -> usize {
        self.0
    }

    /// Whether a train of `members` PRs is within the cap.
    pub fn admits(self, members: usize) -> bool {
        members <= self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn zero_is_refused() {
        assert_eq!(TrainSizeCap::new(0).unwrap_err(), InvalidTrainSizeCap::Zero);
    }

    #[test]
    fn one_is_allowed() {
        // A lone root PR is a legitimate, if pointless, train.
        assert_eq!(TrainSizeCap::new(1).unwrap().get(), 1);
    }

    #[test]
    fn the_ceiling_itself_is_allowed() {
        assert_eq!(
            TrainSizeCap::new(MAX_SUPPORTED_TRAIN_SIZE).unwrap().get(),
            MAX_SUPPORTED_TRAIN_SIZE
        );
    }

    #[test]
    fn above_the_ceiling_is_refused() {
        assert_eq!(
            TrainSizeCap::new(MAX_SUPPORTED_TRAIN_SIZE + 1).unwrap_err(),
            InvalidTrainSizeCap::AboveCeiling {
                requested: MAX_SUPPORTED_TRAIN_SIZE + 1
            }
        );
        assert!(TrainSizeCap::new(usize::MAX).is_err());
    }

    /// The default has to be a cap the parser would itself accept; a
    /// `const` constructor cannot check that, so a test does.
    #[test]
    fn the_default_is_a_cap_the_parser_accepts() {
        assert_eq!(
            TrainSizeCap::new(TrainSizeCap::DEFAULT.get()).unwrap(),
            TrainSizeCap::DEFAULT
        );
        assert_eq!(TrainSizeCap::DEFAULT.get(), 50);
    }

    proptest! {
        /// Property: a cap is accepted exactly when it is a size the status
        /// comment can carry and at least one PR.
        #[test]
        fn prop_accepts_exactly_the_usable_caps(cap: usize) {
            let usable = (1..=MAX_SUPPORTED_TRAIN_SIZE).contains(&cap);
            prop_assert_eq!(TrainSizeCap::new(cap).is_ok(), usable);
        }

        /// Property: an accepted cap reports back the number it was given —
        /// parsing never silently clamps, which would let an operator
        /// believe in a larger train than they have.
        #[test]
        fn prop_an_accepted_cap_is_the_number_asked_for(cap in 1usize..=MAX_SUPPORTED_TRAIN_SIZE) {
            prop_assert_eq!(TrainSizeCap::new(cap).unwrap().get(), cap);
        }

        /// Property: `admits` is exactly "at most the cap" — the boundary
        /// belongs to the cap, so a train of exactly `cap` PRs may run.
        #[test]
        fn prop_admits_up_to_and_including_the_cap(
            cap in 1usize..=MAX_SUPPORTED_TRAIN_SIZE,
            members in 0usize..=(MAX_SUPPORTED_TRAIN_SIZE + 16),
        ) {
            let cap = TrainSizeCap::new(cap).unwrap();
            prop_assert_eq!(cap.admits(members), members <= cap.get());
        }
    }
}
