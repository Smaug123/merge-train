# Staged Implementation Plan: bounded comment listings

Produced 2026-09-16, after the lost-DB crawl stack landed (#76, #78, #70,
#77). Addresses the deferred crawl-scalability P2 "bound comment
pagination" (Codex crawl review). The sibling P2 — "crawl off the intake
thread" — is NOT covered here and remains open; this plan is deliberately
first, because bounding each listing is what makes the crawl's total cost
envelope characterizable, which that architectural change will need.

## The defect

`github::interpreter::list_comments` follows GraphQL pages of 100 until
`hasNextPage` is false, accumulating every comment in memory. Nothing
bounds the page count or the accumulated body bytes, so one
pathological PR (thousands of comments, or comments near GitHub's 64 KiB
body limit) costs unbounded memory, API quota, and wall-clock time —
and, because the bootstrap crawl maps *every* listing failure to
`Bootstrap::Unavailable`, a PR whose listing can never finish inside the
rate-limit window wedges its repository's queue at the stall cadence for
ever, re-crawling and discarding progress each time (the same shape
`MAX_COMMENT_LISTINGS` already fixed for the *number* of PRs listed).

## The guarantee (binding)

After this plan lands:

- **A listing is COMPLETE or it is refused.** No caller ever sees a
  partial comment list. This extends the existing GraphQL-partial rule
  ("a response carrying ANY error is not a listing"): a listing that
  would exceed the caps is not a listing either. Absence from a listing
  is evidence (it discharges obligations, corroborates 404s,
  neutralizes forgeries), so a truncated vector must be
  unrepresentable, not merely discouraged.
- **Per-listing cost is bounded**: at most `MAX_COMMENT_PAGES` pages
  and `MAX_COMMENT_BODY_BYTES` of accumulated body text per call
  (whichever trips first). Proposed constants: 20 pages (2,000
  comments) and 16 MiB; both are far beyond any conversation the bot
  can meaningfully manage, and a PR beyond them needs an operator
  anyway.
- **A repository containing an over-cap PR still bootstraps.** The
  crawl lands with `comments_truncated` — the existing fail-closed
  machinery: no onboarding, `start` refused durably, adopted trains
  abort `Abort::Truncated` — instead of releasing the delivery for
  ever. Degraded, characterized, loud.
- **Nothing discharges on a refused listing.** Ledger and status-sync
  obligations on an over-cap PR stay owed and retry at the stall
  cadence for ever: bounded per-attempt cost, loud logs, operator
  action required. `@merge-train stop` keeps working throughout.
- **Repositories under the caps are unaffected**: bounded listing ≡
  unbounded listing (property-tested against the pre-change behaviour
  as reference).

## Resolved seam decision (binding for all stages)

Truncation is a property of the ANSWER, not a failure of the call, and
every consumer must be forced to decide what it means at its site. So it
is a response shape, not an error variant (an error would be swallowed
silently by the crawl's generic `fetch!` arm into `Unavailable` — the
exact wedge this plan removes):

```rust
pub enum CommentListing {
    /// Every comment on the PR, in id order.
    Complete(Vec<CommentData>),
    /// The listing exceeded MAX_COMMENT_PAGES or MAX_COMMENT_BODY_BYTES.
    /// Carries no comments: a partial listing is not a listing.
    Truncated,
}
// GitHubResponse::Comments(Vec<CommentData>) becomes
// GitHubResponse::Comments(CommentListing)
```

The compiler then walks the implementation to every consumer:
- the crawl's per-PR fetch (`pipeline.rs`, the `fetch!` site),
- the crawled-delivery freshness re-check,
- `recover_inherited`,
- the status-sync probe and ledger probe outcomes,
- `worker::recovery::decide_comment_recovery`'s caller,
- the fake (`github::test_support`).

---

Implement this plan with each stage on its own branch, stacked as
necessary on previous branches, so that a reviewer can review each
branch in isolation.

## Stage 1: the bounded interpreter and the fake's knob

**Dependencies**: none.

**Implements**: §The guarantee (bullets 1–2), §Resolved seam decision.

Introduce `CommentListing`, thread it through `GitHubResponse`, and
enforce both caps in `list_comments` (count pages; accumulate
`body.len()`); on overflow, abandon the loop and return `Truncated`,
discarding what was fetched. Give `FakeGitHub` an `oversized_prs:
HashSet<PrNumber>` knob whose members answer `Truncated`. Every existing
consumer maps `Truncated` to its CURRENT failure path for now (crawl:
`Bootstrap::Unavailable`; probes: their existing error arms) so this
stage changes no observable behaviour for repos under the caps — the
semantic mappings are Stages 2–3.

**Correctness oracle**:
- Mock-server tests: pages under both caps → `Complete`, byte-identical
  to today's result (existing tests updated mechanically); a
  `MAX_COMMENT_PAGES + 1`-page PR and a bodies-over-16-MiB PR each →
  `Truncated`, and the request log shows pagination stopped at the cap.
- Property (fake-level): for generated comment sets under the caps,
  bounded listing ≡ the unbounded reference.
- The full suite passes unchanged: no consumer's behaviour moved.

## Stage 2: the crawl fails closed, not unavailable

**Dependencies**: Stage 1.

**Implements**: §The guarantee (bullet 3).

In the bootstrap crawl's per-PR loop, `Truncated` no longer takes the
`fetch!` error arm: skip that PR's comments and set
`comments_truncated`, exactly as the `MAX_COMMENT_LISTINGS` cap does.
The crawl COMMITS. Everything downstream is existing machinery:
`topology_incomplete` refuses `start`, onboarding is refused, adopted
trains abort `Abort::Truncated`.

**Correctness oracle**:
- Worker test: a repo whose only oversized PR is mid-stack → bootstrap
  lands (delivery closed, not released), `topology_incomplete()` true,
  `start` answered with the operator-facing refusal.
- Worker test: an adopted active train on such a repo aborts
  `Truncated` rather than recovering.
- Worker test (regression guard): a TRANSIENT listing failure still
  releases the delivery — truncation and unavailability stay distinct.
- Mutation check: reverting the crawl arm to `Unavailable` fails the
  first test (the delivery releases for ever).

## Stage 3: live and recovery consumers refuse loudly

**Dependencies**: Stage 1 (parallel with Stage 2).

**Implements**: §The guarantee (bullet 4).

- Freshness re-check of a crawled comment delivery: `Truncated` closes
  "crawled but unverifiable" with the operator-facing answer (the
  existing permanent-failure arm's semantics, now a distinguished
  match).
- `recover_inherited`: park at the stall cadence with the loud
  operator log, exactly as a permanent listing failure parks; `stop`
  still works.
- Status-sync and ledger probes: a `Truncated` probe discharges
  nothing; the obligation stays owed and retries at the cadence.

**Correctness oracle**:
- One worker test per consumer (four), each asserting the mapped
  behaviour — refusal answered, park-and-heal, obligation kept then
  discharged on heal — so a truncated listing observably changes
  nothing an obligation depends on.
- The ∀-form of "a truncated probe discharges nothing" is carried by
  Stage 4's recovery-model extension rather than a store-level
  property: generating arbitrary obligation states in isolation would
  duplicate the store harness for little marginal power over the
  deterministic heal tests plus the model.

**Resolved in implementation**: Stage 1's conservative mappings already
gave every consumer its final BEHAVIOUR (the probes' `find_map` treats
`Truncated` as a failed listing; recovery's wrong-variant arm parks);
this stage makes each site a distinguished match with an accurate,
operator-facing message — the freshness refusal names the caps rather
than blaming the token — and pins all four behaviours with tests.

## Stage 4: the recovery model covers oversized PRs

**Dependencies**: Stages 2 and 3.

**Implements**: end-to-end confidence across the guarantee.

Extend the worker recovery model with the "oversized PR" shape (the
fake's knob), asserted from both sides of a loss rather than as naive
live/recovered agreement — the two sides are RIGHT to differ:

- LIVE, the listing gates nothing about the edge: webhooks carry the
  truth, so the edge an unlistable world builds is exactly the edge a
  listable one builds (only ledger discharge waits).
- RECOVERED, the crawl cannot read the PR, so it grants NOTHING — no
  edge, from any crash point — and the topology is incomplete durably,
  refusing starts, instead of the repository's queue pausing for ever.

**Correctness oracle**:
- The extended model passes a `PROPTEST_CASES=300` shake-out.
- The deterministic Stage 2/3 tests remain the named regression tests
  (the generator is too sparse to replace them — established lesson).
