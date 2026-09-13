//! First-contact bootstrap: the pure decision half of the GitHub crawl
//! (DESIGN §Bootstrap algorithm, Phase 2).
//!
//! A fresh store — a brand-new repo, or a repo whose state DB was LOST —
//! knows nothing: no default branch, no PR cache, no predecessor topology,
//! no trains. Webhooks only describe the future, so the first delivery
//! triggers a crawl of the present: repository settings, open PRs, recently
//! merged PRs, and every crawled PR's comments. This module turns those
//! fetched facts into state events; the pipeline does the fetching and
//! appends the result as one atomic batch.
//!
//! What the crawl rebuilds here: the default branch and the PR cache —
//! open and recently merged PRs, plus any *seed* PR the wake-up webhook
//! named that the list endpoints miss (a closed-unmerged PR, fetched
//! individually by the caller). The comments the pipeline lists are read
//! for one purpose: to judge whether the deliveries that straddle the
//! crawl are still current against the present it fetched.
//!
//! Not rebuilt here, deliberately: the predecessor topology (the bot's own
//! stack ledgers, `status::ledger`, read back rather than re-derived — the
//! next change) and the trains (the bot's status comments, the change
//! after). Until then a lost database loses every edge and every train;
//! a fresh declaration or `start` gets the live path's loud validations
//! rather than silence.

use crate::effects::PrData;
use crate::persistence::event::StateEventPayload;
use crate::types::{MergeStateStatus, PrNumber};

use super::pipeline::cache_fill_events;

/// The crawl's decision: events to append, and every PR the crawl
/// *referenced* but did not fetch. The caller fetches those, lists their
/// comments, and RE-RUNS the crawl to a fixpoint. Nothing references a PR
/// yet — the topology crawl will — so the set is always empty here, and
/// the fixpoint loop it drives is in place for it.
pub(crate) struct CrawlOutcome {
    pub events: Vec<StateEventPayload>,
    pub referenced_uncrawled: Vec<PrNumber>,
}

/// Everything `crawl_events` reads: repository facts and the fetched PRs.
pub(crate) struct CrawlInput<'a> {
    pub default_branch: &'a str,
    /// Open, recently merged, and individually fetched PRs, as one slice:
    /// every check reads `state`, so the split never matters.
    pub crawled_prs: &'a [PrData],
}

/// Turns the crawled present into the events that rebuild the store's
/// settings and PR cache.
pub(crate) fn crawl_events(input: &CrawlInput<'_>) -> CrawlOutcome {
    let CrawlInput {
        default_branch,
        crawled_prs,
    } = *input;
    let mut events = vec![StateEventPayload::DefaultBranchSet {
        branch: default_branch.to_owned(),
    }];
    // PR cache fills: `apply_event` skips events about unknown PRs, so
    // everything that follows a fill must come after it.
    for pr in crawled_prs {
        events.extend(cache_fill_events(pr.number, pr, MergeStateStatus::Unknown));
    }
    CrawlOutcome {
        events,
        referenced_uncrawled: Vec::new(),
    }
}
