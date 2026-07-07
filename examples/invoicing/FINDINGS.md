# Findings: building invoicing on charpente

A running log of confusion, documentation gaps, and API surprises hit while
building this app strictly from README.md and docs/GUIDE.md. Candor here is
a deliverable per the brief.

## Design gaps GUIDE.md left me to resolve myself

### 1. No documented way for a transaction to read current (virtual) time

GUIDE §4 states plainly: "`f` only ever receives `&mut Tx` — no `Disk`,
`Clock`, `Entropy`, or `Net` handle reaches inside it," and later warns that
nothing stops you from calling `std::time::SystemTime::now()` inside a
transaction but that doing so reintroduces exactly the nondeterminism the
platform exists to remove ("adopt the same deny-list in your own app for the
same guarantee"). Nowhere in §4-§8 is there a *sanctioned* way for a route
handler, job handler, or cron-triggered job to learn "what time is it" —
not even indirectly (e.g. no `Tx::now()`, no clock argument threaded through
`App::schedule`'s `make: Fn() -> J`).

This is a real gap for an invoicing app: "overdue" is inherently a
time-relative concept, and the brief explicitly asks for a daily cron tick
that computes it. I could not find a documented mechanism to do this
"correctly" against wall-clock time without ambient nondeterminism.

**My resolution**: since cron is the platform's own deterministic clock —
ticks happen at real intervals in production and virtual ones in the
simulator (`charpente_sim::Sim`'s virtual clock), and its catch-up rule
(§7: "catch-up fires once, never backfills") is already exactly the
semantics the brief asks for — I made the daily cron tick itself the only
source of time in this app. A singleton `AppClock` row (`model.rs`) holds a
`day: u64` counter that only `AdvanceDay` (jobs.rs) ever increments, once
per tick. Invoices carry a `due_day: u64` in this same app-relative space
(the API caller picks it, e.g. "due in 30 days" becomes `due_day: 30`
computed client-side, or a fixed value for a net-terms policy) rather than
an absolute timestamp. This is fully transactional, fully deterministic,
and inherits cron's own no-backfill behavior for free, but it means this
app cannot express "overdue by more real-world days than ticks actually
fired" — which, per the brief, is exactly the desired behavior, not a
workaround limitation. I'd still call this a genuine documentation gap:
an app that legitimately needs wall-clock timestamps (e.g. "created_at" for
audit logs) has no documented answer at all.

### 2. No documented id-generation primitive for `#[table]` rows

Every `#[table]`-row creation example in the guide takes the id from a
`:param` in the route path (`POST /notes/:id`) — i.e. the caller picks the
id. There's no equivalent of the platform's own internal `next_job_id`
counter exposed to app code. This is fine for top-level entities that have
a natural external identity from the caller (I used this for `Customer` and
`Invoice` — routes are `POST /customers/:id`, `POST /invoices/:id`), but
breaks down for child rows created *inside* a transaction with no path
param to source an id from (a `CreateInvoiceBody`'s `line_items: Vec<...>`
has no per-item id from the caller).

I first considered bit-packing an id from `(invoice_id << 20) | index`, but
rejected it: invoice ids are caller-supplied and unbounded (any `u64`), so a
large invoice id shifted left silently wraps and can collide with another
invoice's line items. Instead I built a tiny `IdCounter` singleton-row
pattern (`model.rs::next_id`) — read-increment-write a counter row inside
the same transaction — mirroring the platform's own `next_job_id`
mechanism described in CLAUDE.md. It works, but it's exactly the kind of
thing I'd expect a "typed end-to-end" framework to ship a documented helper
for (e.g. `tx.next_id::<LineItem>()`).

### 3. Unclear whether `Id<T>` (or a `#[table]` row) implements `serde::Serialize`

Every route example in GUIDE.md returns a bare `u64` or `String` as its
`Ok(T)` — never a `#[table]` row, and never an `Id<T>` value directly. I
could not find a statement either way on whether `Id<T>: Serialize`. Rather
than gamble a compile cycle finding out, I designed dedicated `*View`
response DTOs (`CustomerView`, `InvoiceView`, `LineItemView` in routes.rs)
that convert via `.raw()` (which *is* documented, e.g. `counter.id.raw()` in
GUIDE §8) to plain `u64` fields. This is probably good API hygiene
regardless (storage rows and wire responses are different concerns), but I
would have liked the guide to say explicitly whether `Id<T>` round-trips
through serde.

### 4. `#[index]` is only demonstrated on `Id<T>` and `String` fields

I wanted to index `Invoice.state` (an enum, hand-rolled `Codec`) to make the
daily overdue scan cheaper, but GUIDE.md never shows `#[index]` on anything
but `Id<T>` (`org: Id<Org>`) or `String` (`token: String`), and never states
what bound a field needs beyond the general `Codec` requirement for row
fields. Rather than risk an unclear macro rejection, I did a full
`tx.scan::<Invoice>()` and filtered in Rust in `AdvanceDay` — correct, just
not indexed. Worth documenting the actual bound `#[index]` fields need.

## Smaller surprises

- The guide's own examples (§4, §6) hand-roll `Codec` for single-field
  payloads (`SendReceipt { invoice: u64 }`) by just calling
  `self.invoice.encode(out)` with no length prefix or tag — and later (§8)
  decode a `JobRow.payload` for a single-field job back with a bare
  `u64::decode(...)`, confirming there's no struct envelope at all: a
  struct's `Codec` is simply the concatenation of its fields' own codecs, in
  order. This isn't stated as a general rule anywhere (`#[table]`'s "fields
  encode/decode in declaration order" is documented for rows, but the
  hand-rolled `Codec` examples for jobs/events rely on the same rule
  implicitly without saying so) — I only trusted it because two independent
  examples did the identical thing.
- `App::schedule`'s interval-overflow panic fires at kernel startup, not at
  the `.schedule(...)` call (documented, but easy to miss) — irrelevant for
  a 24h daily tick, but worth double-checking if anyone changes
  `DAILY_TICK` in lib.rs to something exotic.
- Nothing in GUIDE.md says whether `Request::new(...).json(&body)` requires
  `T: Serialize` explicitly, but it's the obvious inference from context
  (guide's own `HitBody` derives both `Serialize` and `Deserialize` for
  exactly this reason: same type used to build the test request and to
  deserialize the route argument). I mirrored that — every `*Body` struct
  in routes.rs derives both.

## What worked well

- The `#[table]` / `#[route]` / `#[job]` error messages (§9's index) were
  accurate every time I hit one — I never needed to guess a fix.
- `replays_identically` and the whole-app `Kernel`/`SimSource` machinery in
  §8 worked exactly as documented on the first attempt structurally; the
  only iteration needed was around my own schema/route design, not the
  platform's sim API surface.
- The single-`charpente`-dependency shape (post macro-path-unification, per
  CLAUDE.md) meant `examples/invoicing/Cargo.toml` needed exactly one path
  dependency plus serde — no second `charpente-core` dependency needed, as
  an older note in CLAUDE.md had warned might still be required.

## Build/test iteration notes

- The `routes![...]`/`jobs![...]` macros only accept bare identifiers, not
  path-qualified ones (`routes::login` fails to parse: "no rules expected
  `::`"). Not documented either way in GUIDE §5/§6 (every example there
  already has the handler in scope via a flat `use`), but worth calling out
  since it cost one compile cycle — fixed by `use`-importing every route/job
  function/type by name into `lib.rs` before the macro call.
- Everything else — table/row/job/event/route definitions, the `Codec`
  hand-rolling, the `#[table]`/`#[route]`/`#[job]` macros themselves, the
  whole-app `Kernel`/`SimSource` sim machinery — compiled clean on the
  first or second pass, including `tests/sim.rs`. That's a real point in
  the docs' favor: `charpente build/test` errors were exactly as
  informative as GUIDE §9 promised, every time.
- The sim harness caught one genuine bug in my own test code, immediately,
  the very first time I ran `charpente test --sim` at 16 universes: my
  `receipts_exactly_once` workload picked out "the last N response statuses"
  to find the finalize-request acks, assuming all `2N+1` scripted requests
  always got a recorded response. Under a fault schedule where the kernel
  halts early (e.g. `crash_apply: 1.0` — the engine poisons and the loop
  stops), fewer responses land, so "last N" silently grabbed the wrong
  requests' statuses and produced a false "ack-durability violated"
  failure, printing an exact seed and repro line
  (`charpente test --sim --filter receipts_exactly_once --seed 0x...`) on
  the first try — exactly the workflow §8 describes. Fixed by indexing
  responses positionally (`responses.get(N + 1 + i)`, `None` counts as "not
  acked") instead of reversing. Re-running the same pinned seed after the
  fix confirmed the failure was in my invariant check, not the platform.
- Final evidence bar: `cargo test --workspace` green (28 test-result blocks,
  0 failed); `charpente test --sim --dir examples/invoicing` green at 32,
  256, 512, and 1024 fresh-seed universes (1024 in ~23s wall time over 8
  jobs), with every `require`d coverage key (`restart`, `kernel:served`,
  `reminder:sent`) hit in every batch; `cargo clippy --workspace
  --all-targets` and `cargo fmt --check` both clean.

---
Evidence (2026-07-07): `charpente test --sim --dir examples/invoicing --universes 10000 --jobs 8` — all green, master seed 0x483e230baed4922b.
