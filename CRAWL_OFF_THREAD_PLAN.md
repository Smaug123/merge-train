# Staged Implementation Plan: the crawl off the intake thread

Produced 2026-09-17, after the bounded-listings stack landed (#79–#82,
`COMMENT_PAGINATION_PLAN.md`). Addresses the last deferred
crawl-scalability P2 from the Codex crawl review: "the crawl blocks the
worker thread, stalling intake acks against the process-wide
`MAX_INFLIGHT_INTAKE_BYTES` budget". It was sequenced second because it
needed a characterizable cost envelope for one crawl, which bounded
listings now supply:

| bound | constant | value |
|---|---|---|
| PRs whose comments one crawl lists | `MAX_COMMENT_LISTINGS` | 1000 |
| pages per listing | `MAX_COMMENT_PAGES` | 20 |
| body bytes per listing | `MAX_COMMENT_BODY_BYTES` | 16 MiB |
| referenced PRs fetched one by one | `MAX_REFERENCED_FETCHES` | 200 |
| list calls | settings, open, recently merged | 3 |

That envelope is what a crawl costs wherever it runs. This plan moves
where it runs; it does not change what it reads, how it judges, or what
it commits.

## The defect

`Processor::bootstrap_crawl` (`src/worker/pipeline.rs`) runs inside
`process_claimed`, on the per-repo worker thread, and blocks it for the
crawl's whole duration — up to the envelope above, which under rate
limiting is minutes to hours. The worker loop (`worker::run`) services
its mailbox only between pipeline steps, so while a crawl runs:

- **No delivery for that repo is acked.** Each webhook waits in the
  mailbox as a `WorkerMsg::Enqueue`; the handler awaits the ack before
  replying, so GitHub's delivery timeout passes long before the 200.
  (The delivery is still stored when the crawl finishes — nothing is
  lost — but GitHub records a failed delivery.)
- **Each waiting message holds its byte permit** against the
  process-wide `MAX_INFLIGHT_INTAKE_BYTES` budget (256 MiB). Once one
  repo's waiting bodies reach the budget, `reserve_intake` blocks
  every handler for every repo: one repository's first contact stalls
  intake for the whole process, for as long as its crawl takes. The
  mailbox count bound (`MAILBOX_CAPACITY`, 1024) blocks the handler
  the same way.

The module doc's promise — "the worker thread never blocks on effects
... so intake acks and delivery processing continue while a
multi-minute git saga runs" — holds for sagas and not for the crawl,
which is the only O(repository) GitHub work the thread does. Every
other GitHub call left on it (the crawled-delivery freshness re-check,
`precache`, the permission lookup, inherited recovery's root listing,
`adopt_replacement`'s fetch) is a single effect per delivery or per
root, bounded by `RetryConfig::DEFAULT` (three retries, 2 + 4 + 8 s).

## The guarantee (binding)

After this plan lands:

1. **The worker thread performs no O(repository) GitHub work.** The
   crawl's reads run on a spawned *crawl thread* — the same shape as a
   saga's effects on an executor thread — and its result returns
   through the worker's own mailbox.
2. **Intake acks stay prompt during a crawl.** While a repo's crawl is
   in flight its worker keeps servicing its mailbox every turn, so an
   `Enqueue` is durably stored and acked within one loop turn of
   non-crawl work, and its byte permit is released then. The
   process-wide budget therefore drains at intake speed however long
   any crawl takes.
3. **The store stays single-writer.** The crawl thread receives a
   `GitHubExec` clone and plain data. It never sees the `Store`. The
   crawl's events, the backlog mark and the trigger's close still
   commit as one transaction on the worker thread, exactly as today.
4. **One crawl per repo, and no delivery is processed while it runs.**
   `claim` is gated on the crawl slot as `pump` is gated on the saga
   slot. Deliveries received during the crawl are stored and acked,
   and judged against the crawl's present when it lands — by the
   straddling-delivery machinery that already exists
   (`mark_backlog_crawled_in` marks every pending row; the
   `crawl_landed_at` counter marks late-stored ones by receipt time).
   This plan adds no judgement rule.
5. **A lost crawl is a crash mid-crawl.** A crawl commits nothing until
   it lands, so a crawl thread that dies, or a worker that dies with a
   crawl out, leaves exactly today's mid-crawl state: the trigger row
   `processing`, requeued when the store reopens, re-crawled then. A
   crawl thread that unwinds, or cannot be spawned, is fatal to its
   worker — as a failed executor spawn is — because the slot would
   otherwise wait for ever.
6. **The cost envelope is unchanged.** Same per-crawl bounds; still at
   most one crawl per repo, and no more repos crawl concurrently than
   today (each already crawled on its own thread). The crawl thread
   holds what the worker thread held before and ships it once.

## Resolved seam decisions (binding for all stages)

1. **The seam is data, not a callback.** The fetch loop becomes a free
   function in `src/worker/bootstrap.rs`:

   ```rust
   pub(crate) struct CrawlRequest {
       seed_prs: Vec<PrNumber>,   // event.referenced_prs()
       bot_name: String,
       bot_user_id: u64,
       now: DateTime<Utc>,        // what crawl_events gets today
   }
   pub(crate) enum CrawlFetch {
       /// Any failure: the delivery is released (the stall cadence).
       Unavailable,
       Fetched(CrawlReads),
   }
   pub(crate) struct CrawlReads {
       default_branch: String,
       crawled: Vec<PrData>,
       comments: Vec<(PrNumber, Vec<CommentData>)>,
       listed: HashSet<PrNumber>,
       unfetchable: HashSet<PrNumber>,
       comments_truncated: bool,
       outcome: CrawlOutcome,      // crawl_events at the fixpoint
   }
   pub(crate) fn crawl(github: &GitHubExec, request: &CrawlRequest) -> CrawlFetch;
   ```

   `crawl_events` is already pure; the fixpoint loop that drives it
   moves with the reads. `CrawlRequest` and `CrawlFetch` are `Send`,
   asserted at compile time, because a thread carries them. Nothing in
   the request comes from the store: a first-contact store is empty,
   and the crawl reads the bot's own records, not the trigger.

2. **Judgement stays on the worker, and pure.** The trigger's freshness
   verdict — the `stale` match in today's `bootstrap_crawl` — becomes
   `judge_trigger(&CrawlReads, Option<&TriggerFreshness>, retried) ->
   TriggerVerdict { Current, Doubted, Stale }`, a function over data
   called when the fetch lands. `retried` (`doubt_has_stood`) is read
   when the crawl is REQUESTED and carried with the parked delivery, as
   today it is read immediately before the reads: judging it later
   would let a crawl's own duration promote a doubt to a belief.

3. **The `Processor` parks the delivery; the caller runs the fetch.**
   On an unbootstrapped store `process_claimed` parses the event,
   parks `(delivery, request, freshness, retried)` and returns a new
   `PipelineOutcome::Crawling`. Then:

   ```rust
   pub fn take_crawl_request(&mut self) -> Option<CrawlRequest>; // once
   pub fn crawl_in_flight(&self) -> bool;
   pub fn on_crawl_finished(&mut self, fetch: CrawlFetch)
       -> Result<PipelineOutcome, StoreError>;                    // Processed | Released
   ```

   `on_crawl_finished` is today's `match self.bootstrap_crawl(..)` with
   the fetch supplied: `Unavailable` → release; `Doubted` → doubt +
   release; `Stale` → `commit_delivery_closing_crawl` with the
   restatement transfer; `Current` → `append_batch_marking` and the
   ordinary pipeline continues in crawl context. `claim` returns `None`
   while a crawl is parked, before its startup-evaluation logic. There
   is no trait and no closure: the worker loop and the test harness are
   the only callers, and each runs `crawl` where it likes — a thread,
   or inline. This mirrors how sagas are tested (`execute_batch` runs
   inline in tests, on an executor thread in production).

4. **A mailbox variant carries the report.** `WorkerMsg::CrawlFinished
   (CrawlReport)` with `enum CrawlReport { Finished(CrawlFetch), Died
   }`. The crawl thread sends through a drop guard so that an unwinding
   thread still reports `Died`; the worker treats `Died` as fatal
   (`fatal_spawn`'s reasoning). A `Released` outcome from the finish
   sets `stalled` and calls `request_retry`, exactly as step (3) of the
   loop does for a released claim — nothing else would wake a
   traffic-less repo.

5. **The harness gets one helper.** `process(processor, delivery) ->
   PipelineOutcome` in `src/worker/tests.rs`: `process_claimed`, and on
   `Crawling` run `bootstrap::crawl(processor.github(), &request)`
   inline and `on_crawl_finished`. `drain`, `drain_with_cooldowns` and
   every direct `process_claimed` call in the tests (about 130 sites)
   go through it. The change is a mechanical rename; no assertion
   moves. Interleavings the thread makes possible are then expressible
   in the synchronous harness: enqueue between `take_crawl_request` and
   `on_crawl_finished`.

---

Implement this plan with each stage on its own branch, stacked as
necessary on previous branches, so that a reviewer can review each
branch in isolation.

## Stage 1: the crawl and the verdict become functions over data

**Dependencies**: none.

**Implements**: §Resolved seam decisions 1–2. No behaviour change.

Move the fetch loop out of `Processor::bootstrap_crawl` into
`bootstrap::crawl`, and the freshness match into `judge_trigger`.
`bootstrap_crawl` becomes three lines: build the request, call `crawl`,
call `judge_trigger`; its `Bootstrap` enum and both commit branches in
`process_claimed` are untouched. The `fetch!` macro, the caps, the seed
ordering and every log line move verbatim.

**Correctness oracle**:
- The full suite passes with no test edited (1013 at the time of
  writing). The reviewer's check is `git diff --color-moved`: the loop
  and the match bodies are moves, with `self.deps.x` → `request.x`.
- `const _: () = { fn assert_send<T: Send>() {} ... }` for
  `CrawlRequest` and `CrawlFetch`: the thread-carry is a compile-time
  fact before any thread exists.
- One unit test per `TriggerVerdict` arm on constructed `CrawlReads`
  (present-and-matching → `Current`; other body, not retried →
  `Doubted`; retried → `Stale`; absent on an unfetchable PR →
  `Current`; absent on an unlisted PR → `Stale`; disagreeing PR
  snapshot → `Doubted`/`Stale` by `retried`). These are the arms the
  worker tests already reach through GitHub; pinning them on data is
  what makes the function safe to reason about when the next stage
  moves its call site.

**Resolved in implementation**: the Codex P2 on #80 — a first-contact
COMMAND on an over-cap PR was doubted, re-crawled, then closed as stale
with no answer — turned out never to have reached main (the fix was
lost in a rebase). It is re-applied in its own PR (#84, the base of this
stack): an `unread` set of refused listings, and a three-way
`TriggerStanding { Current, Stale, Unverifiable }`. This stage folds
the doubt into the same enum (`Doubted`), so `judge_trigger` returns
one value and `Bootstrap::TriggerDoubted` is gone. `CrawlFetch` boxes
the reads (a repository's worth of listings travels by value).

## Stage 2: the `Processor` parks the delivery and resumes on the fetch

**Dependencies**: Stage 1.

**Implements**: §Resolved seam decisions 3 and 5; §The guarantee 4–5 at
the `Processor` level.

Add `PipelineOutcome::Crawling`, the parked state,
`take_crawl_request`, `crawl_in_flight`, `on_crawl_finished`, and the
`claim` gate. Add the harness `process` helper and route every test
call through it. Production still runs the crawl on the worker thread
(the loop calls `crawl` inline on `Crawling` for now), so this stage
changes no observable behaviour: it changes who holds the delivery
while the reads happen.

**Correctness oracle**:
- The suite passes through `process`, assertions untouched.
- `claim_yields_nothing_while_a_crawl_is_parked`: after `Crawling`,
  `claim` is `None` with a second delivery pending; after
  `on_crawl_finished`, it is claimed `crawled = true`. Mutation: remove
  the gate → the second delivery is claimed and triggers a second
  crawl on an unbootstrapped store.
- `a_delivery_stored_during_the_crawl_is_judged_against_its_present`:
  enqueue between `take_crawl_request` and `on_crawl_finished` (a) a
  comment on a listed PR → handled after re-check; (b) an unmerged
  `closed` for a PR the crawl fetched open → doubted, then stale. Both
  are the straddling rules; the test pins that the new storage order
  reaches them (the row is marked by `mark_backlog_crawled_in`, not by
  `crawl_landed_at`).
- `a_crawl_whose_result_is_lost_is_a_crash_mid_crawl`: park, take the
  request, drop the processor without finishing; reopen: the store is
  unbootstrapped, the trigger is requeued and the next `process` crawls
  again and lands. Mutation: commit anything before the finish → the
  reopen finds a default branch.
- **Recovery model**: generalize `recovered_after(events, crash)` to
  `recovered_straddling(events, crash, read_at)` with `crash ≤ read_at
  ≤ len`: `[0, crash)` processed live; the DB lost; `[crash, read_at)`
  enqueued before the fetch (plus the stranger's remark as the wake-up
  when that range is empty); `[read_at, len)` enqueued between the
  fetch and `on_crawl_finished`; then `drain_with_cooldowns`. Assert
  the existing live/recovered agreement with the existing permitted
  deviation. `read_at = len` is today's crash-index property, which
  this replaces. 48 cases in CI; a `PROPTEST_CASES=300` shake-out
  before review. This is the ∀-form of guarantee 4: the new window is
  not a new judgement.

**Resolved in implementation**: `process_claimed` splits into the
parking prelude and `continue_pipeline` (the note, the dedupe, the
crawled re-check, the handler); `on_crawl_finished` is the old
`Landed` arm followed by `continue_pipeline` in crawl context. The
parked state is a `ParkedCrawl { delivery, event, key, freshness,
retried, request }`, so the resumption needs nothing re-derived. The
worker loop runs `bootstrap::crawl` inline on `Crawling` for now. The
recovery model's `recovered_after(events, crash, oversized)` became
`recovered_straddling(events, crash, read_at, oversized)`; the
oversized property keeps `read_at = len`. The harness helper is
`process`; the eleven call sites whose receiver was already `&mut
Processor` pass it through.

## Stage 3: a loop-level harness (infrastructure)

**Dependencies**: none (parallel with Stages 1–2).

**Implements**: the means to observe guarantee 2, which no synchronous
test can state.

The worker loop `run` has no tests of its own; the server tests spawn
workers through the registry but only exercise intake. Add a test
module beside `run` that spawns a real worker via `WorkerRegistry::new`
+ `fake_shared_deps` + `sender_for`, sends `WorkerMsg::Enqueue` with a
permit from `reserve_intake`, and observes the fake through two new
knobs on `FakeGitHub`, both deterministic:

- `effect_log: Option<std::sync::mpsc::Sender<GitHubEffect>>` — every
  effect `execute` receives is sent before it is answered. Tests
  `recv` on the receiver (a channel wait is a synchronization
  primitive; polling the fake under a sleep is not). The timeout on
  `recv_timeout` exists to fail instead of hang, never to pass.
- `listing_gate: Option<Arc<(Mutex<bool>, Condvar)>>` — while closed,
  `ListComments` blocks. It blocks with the fake's mutex held, so a
  test may not lock the fake while the gate is closed; document that
  on the knob. Everything a gated test needs (send, await ack, read
  the log) needs no lock.

**Correctness oracle**:
- `a_first_contact_delivery_is_crawled_end_to_end`: one delivery on a
  fresh repo; the log shows `GetRepoSettings`, the listings, and the
  delivery's own handling (its ack `PostComment` for a command).
- `the_gate_holds_the_crawl_at_its_listing`: gate closed; the log
  shows the crawl reach `ListComments` and nothing after; open the
  gate; the log shows it finish. This proves the harness can observe
  the crawl's midpoint, which Stage 4's oracle stands on. At this
  stage the crawl still blocks the worker — the test says nothing
  about acks yet.

## Stage 4: the crawl thread

**Dependencies**: Stages 2 and 3.

**Implements**: §Resolved seam decisions 4; §The guarantee 1–3, 5–6.

`dispatch_crawl(processor, request, tx) -> bool` spawns
`crawl-{owner}-{repo}` with a `GitHubExec` clone, runs `crawl`, and
sends `CrawlFinished` through the drop guard. In `run`: after a
`Crawling` outcome, take the request and dispatch (spawn failure →
`fatal_spawn`); step (3) claims only while `!crawl_in_flight()`; the
idle predicate treats a crawl in flight like a saga in flight;
`handle_msg`'s `CrawlFinished` arm calls `on_crawl_finished`, and a
`Released` there stalls and requests the retry as step (3) does; `Died`
is fatal. Update the module doc (the crawl now belongs with sagas under
"Where the work happens"), and the `Store::enqueue` comment that
explains `crawl_landed_at` by "webhooks received during the crawl's
reads wait in the mailbox" — they no longer do; the counter still
covers a delivery received before the landing but stored after it
(the handler's `reserve_intake` wait and mailbox latency).

**Correctness oracle**:
- `an_intake_ack_does_not_wait_for_the_crawl` (loop-level): gate
  closed; first delivery → the log shows the crawl at its listing;
  send a second delivery and await its ack — it must arrive with the
  gate still closed; open the gate; the log shows the crawl finish and
  the second delivery re-checked as crawled and handled. Mutation: run
  the fetch inline on `Crawling` → the second ack never arrives. This
  is guarantee 2; guarantee 1 is the code, guarantee 3 is that the
  crawl thread's closure captures only the `GitHubExec` clone and the
  request — a reviewer checks the spawn, as for `dispatch`.
- `a_released_crawl_arms_the_stall_retry` (loop-level): the fake
  `unavailable`; the log shows `GetRepoSettings` fail; clear
  `unavailable`; the log shows a second `GetRepoSettings` after the
  25 ms cadence and the crawl landing. Mutation: drop the
  `request_retry` on the finish's `Released` → no second fetch.
- `a_crawl_thread_that_dies_kills_the_worker` (loop-level): a fake
  knob that panics inside `ListComments` once; the mailbox sender
  reports closed; `sender_for` respawns; with the knob cleared the log
  shows the requeued trigger crawled again. Guarantee 5's "requeued on
  reopen" is the store's existing `processing_is_requeued_on_open`.
- The process-wide budget consequence is not tested at scale: it
  follows from permits releasing on ack (already tested, Codex #53)
  plus the ack test above.
- Suite green; Stage 2's model unchanged (the thread adds no
  interleaving the synchronous window did not already express).

## Explicitly out of scope (with reasons)

- **The remaining single-effect GitHub calls on the worker thread.**
  Each is bounded by the retry policy; moving them would be a
  saga-shaped rewrite of the pipeline's control flow for a bounded
  latency, i.e. speculative.
- **The second full crawl a doubted trigger costs.** A `Doubted` fetch
  commits nothing and the retry re-reads the repository. Landing the
  crawl and doubting only the delivery would change the judgement
  rules (the crawl's events would precede the trigger's own
  suppressed-creation transfer), which this plan deliberately keeps
  fixed. A plan of its own if the cost matters.
- **A process-wide cap on concurrent crawls.** Unchanged from today:
  each repo already crawled on its own thread.
- **Cancelling an orphaned crawl thread** when its worker dies. It
  finishes its bounded reads and its report is dropped; the cost is one
  envelope's worth of quota, and a cancellation token would thread
  through every fetch for that.
