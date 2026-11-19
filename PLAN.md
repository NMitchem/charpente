# Charpente — Plan

Bias: end-to-end working system as fast as possible, then refine. Every milestone ends in
something that runs and demonstrates a guarantee no incumbent stack offers.

## M0 — The demo that sells the thesis [shipped 2026-07-07]

**Goal:** an agent builds a small SaaS on charpente in one session, and `--sim` catches an
injected durability bug that would silently eat data on a conventional stack.

Scope, in dependency order:

1. **Sim-capable IO layer.** `Clock`/`Entropy`/`Disk`/`Net` traits; production impls
   (tokio/std); simulator impl with seeded PRNG, virtual time, IO reordering, and a fault
   matrix (torn write, failed sync, crash-restart between any two operations, partition).
   *This is first because it is the one thing that cannot be retrofitted.*
2. **Log + deterministic core.** Single-threaded apply loop; WAL append/sync/replay;
   crash-recovery invariant tests running under the simulator from the first week.
3. **Typed tables on redb** behind the `Disk` boundary: `#[table]`, `Id<T>`, get/put/scan,
   one secondary-index form, MVCC snapshots for reads.
4. **Transactions.** `fn(&mut Tx) -> Result<T, E>`; enqueue + emit recorded in the same
   log record; serializable by construction.
5. **HTTP layer.** `#[route]` macro over a hyper-based server; typed params/body/response;
   sessions (minimal auth).
6. **Queue + cron.** Transactional enqueue, post-commit dispatch, idempotency keys,
   typed retry policy; cron ticks as transactions.
7. **`charpente test --sim`** CLI: seeded parallel universes, fault schedules, failure =
   seed + minimal repro command.
8. **The demo app** (invoicing or todo-SaaS) written *by an agent against the docs*, plus
   the recorded sim run that catches an injected torn-write bug. The demo doubles as the
   launch post.

**Exit criteria:** demo app passes 10k seeded sim universes with fault injection; kill -9
at any point during commit never loses an acknowledged write; the agent-authored app
required zero framework patches.

## M1 — The platform becomes real [shipped 2026-07-27; four bullets deferred, marked below]

- **Replication:** async log shipping, follower reads, manual/arbiter failover. *Shipped.*
  Backup = log segments + snapshots to object storage: **deferred to M2, not built** —
  `serve::backup` is a promotable hot standby, not a snapshot shipper.
- **Realtime:** SSE subscription surface over `#[event]` with exact resume-from-cursor.
  *Shipped.* WebSockets and TS client codegen for events and routes: **deferred to M2**
  (both were declared non-goals of the realtime spec).
- **Auth, properly:** OIDC (static-JWKS RS256), API keys, sessions. *Shipped.* Tenant
  scoping as typed key prefixes: **deferred to M2**, and shipped there 2026-09-04 (see the
  M2 bullet below; DESIGN §8 carries the mechanism).
- **Migrations:** schema versions as log records; compile-time incompatibility detection.
  *Shipped.*
- **Error/introspection polish:** `charpente describe`, structured sim-failure reports —
  the agent-interface quality bar (DESIGN §7) gets its own test suite. *Shipped.*
- **Edge harness:** Vörtex-style black-box rig for the hyper edge — stock binary,
  TCP-proxy fault injection, process kill/pause. The sim proves the kernel; this covers
  the wire (`serve`). *Shipped.*

**Exit criteria:** a follower promoted mid-write-load loses nothing and serves reads
throughout; a reconnecting realtime client provably misses no events. **Both discharged
2026-07-27**, each with an oracle at two layers (an in-sim property plus a wire oracle in
the edge harness). None of the four deferred bullets above gates either criterion — which
is why the milestone closed with them outstanding rather than silently absorbing them.
**The second layer's cadence, stated because the claim above used to imply more than was
true:** the wire oracle is 15 `#[ignore]`d tests that `cargo test --workspace` skips, so
from 2026-07-27 until 2026-09-05 it ran only when a human ran `scripts/edge-harness.sh`.
It now runs nightly and on demand (`.github/workflows/edge-harness.yml`); it is still not
a per-push gate, by choice — MANIFESTO §6's cadence for expensive fault work is per night.
That job runs **34** ignored tests, not 15: the 15 wire scenarios in `crates/edge-harness/
tests/` plus 19 real-disk/real-process unit tests under its `src/` that `--ignored` also
picks up and `cargo test --workspace` also skipped. First runner execution 2026-09-05
(run 33974865000): green, 34/34, 3m28s.

## M2 — The moat deepens [closed 2026-09-13 — six shipped, one dropped, the rest reclassified]

M2 never had an exit criterion; it was a list, and a list closes when what remains on it
has been sorted rather than when it is empty. Sorted 2026-09-13 against the thesis: the
six bullets marked *Shipped* below stand; the compile-latency bullet stands as dropped;
the five unshipped bullets (own LSM engine, blob store, WebSockets, typed telemetry,
Postgres-wire) move to "After launch" below, each with the trigger that reopens it, and
the hosted control plane leaves the roadmap altogether. The reasoning is in
docs/LEDGER.md's 2026-09-13 entry; the short form is that none of the five strengthens
the oracle, and the oracle is the product.

- **Honest benchmark harness** — the "publish honest benchmarks early" mitigation below,
  and the instrument DECISIONS §7's LSM build triggers are read against. *Shipped
  2026-08-10*; numbers of record and method in [docs/BENCH.md](docs/BENCH.md). Trigger (a)
  did not fire on that reading: the commit path measured fsync-bound at ≈1x the device
  ceiling, so redb was not the binding constraint at that workload on that hardware. **That
  reading pre-dates group commit**, which is precisely what removes the
  one-fsync-per-transaction ceiling it rests on — the core layer is unchanged, the kernel
  layer is not — so the trigger was owed a re-read. **That re-read was delivered
  2026-08-20** by the per-batch-apply sub-project below, on the same instrument and
  protocol with a fresh interleaved baseline: **trigger (a) does not fire under any
  reading** of "largest single non-idle phase" — the field reading leads by 71-73
  points, and the two readings that would previously have fired now decline. DECISIONS
  §7 remains untouched and Noah's to sign off; what changed is that the ambiguity which
  made it a judgment call is gone, so the deferral simply stands
  (docs/LEDGER.md carries both the item and the numbers).
- **Primary-side group commit** — the follow-up the harness above named and sized. *Shipped
  2026-08-15.* One commit point per kernel loop iteration rather than one per transaction:
  `begin_group`/`stage`/`commit`, a two-level overlay, one fsync for the whole batch,
  `MAX_BATCH = 64`. Ack-after-durable is unchanged — apply stays strictly after sync, and
  `Db::transact` is the group-of-one case, so the single-request path executes the
  pre-batching instruction sequence. Numbers and the conditioning that must travel with
  them are in [docs/BENCH.md](docs/BENCH.md): the win exists where the queue is deep enough
  to fill a batch and is nothing at one connection, on a device whose slow fsync flatters
  the mechanism. The bare-metal Linux run stays owed.
- **Per-batch apply (the store's commit point)** — the follow-on group commit exposed:
  the WAL sync was amortized 62x while the store still paid one write transaction *and*
  one read transaction per record. *Shipped 2026-08-20.* `Store::apply_batch` commits a
  contiguous run's row effects atomically with `applied_seq` in ONE redb write
  transaction; every record commits or none does. Kernel throughput 1.78-1.88x, Disk ops
  13.37 -> 1.23 per log record, store block 6.2x. It also **retracted a guarantee**:
  followers converge *logically*, not byte-identically — batch boundaries are a
  scheduling artifact, not part of the log (DESIGN §1, ruled 2026-08-20). Numbers and the
  provenance rules that constrain how they may be quoted are in
  [docs/BENCH.md](docs/BENCH.md).
- **Object-storage backup** (deferred from M1) — log segments and seq-cadenced snapshots
  shipped off-site, Litestream-style. *Shipped 2026-08-29.* `ObjectStore` joins
  `charpente-core::io` as the fifth top-level trait behind hard rule 1, with `FsObjectStore` in
  `prod` and a seeded `SimObjectStore`; the archive runs as a kernel arm, so it ships from
  the same log the primary commits to and holds a **bounded** retention pin that composes
  with replication's. `charpente restore` rebuilds a Db from a bucket and refuses a diverged
  timeline, an unreadable manifest and every §7 corruption case **by name** rather than
  guessing. Generations are committed by a manifest swap, and a manifest names its writer:
  an archive that finds another writer's manifest stops rather than interleaving. Bucket
  reclamation deletes a superseded generation's objects **by name, out of the manifest it
  just replaced** — never a LIST-and-sweep, which was refused because a sweep whose
  arithmetic is wrong deletes what a restore needs. The flagship oracle is a convergence
  property over 256 universes on each of four pinned bases: what the archive holds restores
  to what the Db held.
  **Two limits travel with it, both documented rather than engineered around.** Objects
  orphaned by an interrupted upload, an abandoned generation, or a drain cut short are
  unreachable by any code — an object is orphaned precisely in that no manifest names it —
  so operators MUST configure a bucket lifecycle rule; `docs/GUIDE.md` §15 states it as a
  requirement, not a footnote. And the delete-fault axis, absent when reclamation shipped,
  now exists — `FaultConfig.object_delete_error`, rolled from its own
  `keyed_stream(seed, "object_store_delete")` so that declaring it perturbed no pinned
  universe (all twelve convergence coverage counts across four bases reproduce exactly) —
  and **the convergence property now turns it on**: a delete fails in 32 of 256 universes at
  the pinned base, all 32 still restore, and `archive:delete_failed` is a `require:` key so a
  batch in which no delete ever failed is a red property. "Correct when every delete fails
  forever" keeps its two aimed hand-driven modes, which a per-op probability cannot replace,
  but it is no longer the only evidence.

- **TypeScript client codegen** (deferred from M1). *Shipped 2026-08-29.*
  `charpente_tsgen::emit_client` is a pure `Contract -> client.ts` function: branded id types, a
  discriminated `Result<T, E>` per route, and per-event SSE dispatch for a route's
  `emits(...)`. `charpente gen-client`/`check-client` are the CLI verbs; `check-client` is
  CI's drift oracle (0 identical / 1 stale / 2 build-or-usage fault / 101 a contract the
  emitter refuses by name). A type-check alone is half a gate — `tsc --noEmit` proves a
  call site type-checks, never that a runtime value matches its declared type — which is
  why `ts-fixture/`'s Node-native suite runs the generated client against real
  `serde_json` documents through a stubbed `fetch`, not `tsc` alone. No bare `u64`/`i64`
  reaches the wire: the `Wire` bound refuses one at compile time, and `WireU64`/`WireI64`
  carry a 64-bit value as a decimal string, `bigint` client-side. Transport is SSE only.
- **Tenant scoping as typed key prefixes** (deferred from M1). *Shipped 2026-09-04.*
  `#[table(scoped_by = Org)]` makes `(scope, id)` the row key and prefixes every secondary
  index, and the scope-taking primitives are private to the core — so a scoped row is
  reachable only through `tx.scope::<Org>(org, ctx)?`, which runs the app's own
  `Tenant::authorize` first. "Every query is tenant-scoped" stops being a review checklist
  item: a scoped read on a bare `Tx` is a compile error naming the table, its scope, and
  the line to write, and the traits underneath it (`NoTenant`, `UnscopedRow`) are sealed,
  so an app cannot opt back out in one orphan-legal line. The scope travels the whole way
  down — WAL effects, replication, archive, `restore`, snapshot seeding, and
  `migrate_schema`'s watermark — and a binary compiled against the unscoped shape of a
  scoped table refuses the record by name rather than misreading it. Realtime is the same mechanism rather than a second
  one: an emit inside a scope records the scope's tenant, apart from any app key, and a
  tenant feed built from the scope receives only its tenant's events. Enumerating every tenant is a
  **declared capability**, not an escape hatch — `#[route(…, cross_tenant)]` /
  `#[job(…, cross_tenant)]` mint the token `Tx::across_tenants` requires, rows come back
  paired with their scope, and `charpente describe` turns "audit the cross-tenant reads"
  into a mechanical inventory (DECISIONS §11). The flagship oracle is
  `every_scoped_read_returns_only_its_own_tenants_rows` — all four read paths, two tenants
  sharing an id space *and* an index key, 256 universes at a pinned base plus 768 over
  three fresh ones — with two naive victims that fail it by name, a tenant-feed realtime
  sibling, and a two-tenant wire oracle over real TCP in the edge harness.
  The event log's shared-raw-id limit was closed 2026-10-04 by M3's tenancy completion. A
  scoped event records its tenant in a field of its own, apart from its key;
  `Subscription::admits` delivers a row only into a feed of exactly that tenancy, and a resume
  from another tenancy's row is refused by name on the same test. A scoped event's fingerprint
  mixes its tenant type's `Tenant::NAME`, and a build-time `PAYLOAD_FINGERPRINT` check at every
  typed emit and subscribe site holds a hand-written `impl Event` to that mix. The `NAME` is
  unique across the app's tenant census, which does not see a tenant type the app reaches only
  through an event it never mounts. **One limit travels with it:** the guarantee's boundary is the
  typed surface handed to handlers and jobs: a `Db` holder reaches every tenant and can
  mint the cross-tenant capability, with the standing of whoever holds the data directory —
  an operator, not a request (DECISIONS §12, ratified 2026-09-05).
  Deliberately not built: nested scopes, cross-tenant writes in one transaction,
  platform-derived tenancy, tenant-local ids, and any forced retrofit of existing tables.
- **Compile-latency work: DROPPED 2026-09-09**, on the reading in docs/BENCH.md
  "Compile latency (2026-09-09)". The prebuilt-platform-dylib item was justified on the
  inner loop, and the inner loop is not where the cost is: its stated mechanism — "app
  iteration recompiles app code only" — is already delivered by cargo's fingerprinting,
  proven by 9 of 9 incremental observations printing exactly one `Compiling` line, every
  one of them `invoicing`. Its residual headroom is the once-per-checkout clean build,
  which is a CI-checkout cost and not this item's justification. Cranelift hot reload
  stays an open question in DESIGN §9.1, unchanged and unpromoted.

## M3 — Launch [shipped 2026-10-10; defined 2026-09-13]

**Goal:** a stranger reproduces the wager in one sitting, and the reviewer catches what it
claims to. This is M0's shape again — it ends in a recorded run, not a checklist — because
M0's exit criterion is the only one that has ever been re-run against the platform, and on
2026-09-03 it failed: a docs-only agent building a helpdesk shipped three Critical defects
(`docs/experiments/2026-09-helpdesk/README.md`). One of the three — the one-sided sim
property — now has a mechanical oracle (`charpente test --mutants`, 2026-09-07). The other
two — an unauthenticated route and an unscoped existence check — are guarantees that exist
but are opt-in, which for a framework whose thesis is *the framework is the reviewer* is the
gap that matters.

Sub-projects, in dependency order:

1. **Publish path** [shipped 2026-10-10; spec
   `docs/superpowers/specs/2026-09-13-publish-path-design.md`; `0.1.0` on crates.io].
   Workspace to `0.1.0`; the six crates on crates.io (five names reserved 2026-07-01,
   confirmed 2026-09-13; `charpente-tsgen` publishes fresh). `charpente new
   <name>` scaffolds GUIDE §1's layout — `Cargo.toml` with the `[[test]] name = "sim"`
   block, `src/main.rs`, `tests/sim.rs` — and the scaffold is compile-gated the way
   GUIDE's Rust code blocks are, so it cannot drift from the guide it claims to match.
   GUIDE §1's dependency line is the workspace's `major.minor` (`charpente = "0.1"` at
   that publish), and a test holds the guide's `Cargo.toml` block byte-identical to the
   verb's output.
2. **Default-secure surfaces** [shipped 2026-09-16; spec
   `docs/superpowers/specs/2026-09-16-default-secure-surfaces-design.md`]. A
   `#[route]` that declares neither `auth` nor an explicit `public` is a compile error
   naming both. A table that should be tenant-scoped and is not is a compile error — *how*
   "should be" is detected is that sub-project's design question, not settled here. The
   oracle is already in the tree: the helpdesk app at `8913038`, rebased, must fail to
   compile at both of its Critical sites.
3. **Third docs-only build** [shipped 2026-09-24; record
   `docs/experiments/2026-09-statuspage/`]. A new domain — not invoicing, not helpdesk —
   with the brief written first and the agent given README + GUIDE only. Exit: zero
   framework patches, zero Criticals in review that a platform oracle should have caught,
   `--mutants` clean over its sim properties. Every finding ends with a disposition: fixed
   (with the commit), a v1.x row below (with its trigger), or refused (with the reason).
   That rule replaced "findings ledgered as testimony" in 0.4.0 (spec §4 G) and holds for
   every docs-only or stranger build since; 0.4.0's testimony census applied it backward to
   this build's findings and every earlier build's, after a finding ledgered here with no
   row, the spent App, recurred in an outside agent's build on 0.3.0. The 0.1.0 publish
   waited on two platform fixes this build found,
   and both are fixed: (1) `#[event]` payload evolution is unguarded, and a delivery-time
   decode failure stops the whole node with an error naming neither the event nor a fix —
   **fixed 2026-09-25** (spec `docs/superpowers/specs/2026-09-24-payload-shapes-design.md`:
   every payload records its shape and is never decoded under another; delivery fails a
   tail, never the node; a stopped kernel ends the process); (2) a job cannot open a tenant
   scope the way GUIDE §16 documents, and `authorize` sees neither the operation nor the
   caller kind — **fixed 2026-09-30** (spec
   `docs/superpowers/specs/2026-09-27-job-tenancy-design.md`: a job declares `global` or
   `scoped_by`, and a scoped one runs in the tenant it was enqueued from, or that its
   fan-out named; `authorize` is asked about a principal only; a route's stance, `auth`,
   `public` or `public(Org)`, says who reaches a tenant). Its pre-publish follow-ups closed
   2026-10-04 (spec `docs/superpowers/specs/2026-09-30-tenancy-completion-design.md`): a tenant
   type's `NAME` is its identity, an event declares `global` or `scoped_by` and a tenant's feed
   is built from its scope, app code neither reads nor writes the platform's own tables, and the
   capability tokens cannot outlive the `Group` that minted them, nor a `Principal` its request.
4. **Claims audit** [shipped 2026-09-22; spec
   `docs/superpowers/specs/2026-09-21-claims-audit-design.md`]. The bare-metal Linux BENCH
   run owed since 2026-08-15 lands, or the README scaling paragraph is rewritten to what
   has been measured. README guarantee 1 stops naming telemetry until telemetry exists.
   F1 from the helpdesk experiment — the raw panic trace that "demonstrably destroyed a
   competent agent's work" — is verified closed against the helpdesk repro, not assumed
   closed because a hook now exists.
5. **The launch run** [shipped 2026-10-10; spec
   `docs/superpowers/specs/2026-10-06-launch-run-design.md`; record
   `docs/experiments/2026-10-launch-run/`]. DEMO.md re-executed against the published
   version by a docs-only agent starting from `cargo install charpente-cli`: reaches a red sim
   with a seed, replays it. That transcript is the launch post. The spec ran README's "Try it" rather than DEMO.md,
   with the stranger given the published README and one sentence. A rehearsal against the
   packaged crates passed first, then the real attempt against crates.io passed, both signed
   by Noah.

**Exit criteria:** (a) the launch run, recorded with its seeds; (b) the third docs-only
build passes on the bar in sub-project 3; (c) every number in README.md and MANIFESTO.md
that is a claim about charpente is in docs/BENCH.md or absent, and every other figure
cites its source or carries no digits (held by `claims_census`). All three are met: (a) by
the launch run's real attempt on 2026-10-10, (b) on 2026-09-24, and (c) by `claims_census`.

## After launch (v1.x) — five from M2, one from DECISIONS §6, one from M3's claims audit, three from M3's payload-shapes fix, one from M3's job-tenancy fix, two from M3's tenancy completion, seven from outbound effects, five from the outbound docs-only build, one from the outbound client's compile reading, three from 0.3.0's oracle-density review, three from 0.3.0's rulings, one from 0.3.0's final review, one from 0.3.0's docs-only build, three from the 0.4.0 spec, two named for 0.5.0, two from 0.5.0's reviews, ten from 0.4.0's testimony census, two from 0.4.0's docs-only build, one from the outside agents' 0.4.0 retest, two from the launch demo's rehearsal, one from 0.5.0's mutation run, each with the trigger that reopens it

**Shipped 2026-11-01: 0.4.0, the oracle as hard to get wrong as the app** (spec
[docs/superpowers/specs/2026-10-28-0.4.0-design.md](docs/superpowers/specs/2026-10-28-0.4.0-design.md)),
from three outside agents' builds on 0.3.0: no second kernel on a spent App, a check that
tested nothing fails, a library helper for the recovery code every app pastes, a shrinker
that deletes whole items, a mutation run that fits a session, typed event reads, and the
API on one compiled page. **Shipped: 0.5.0 (unreleased), what apps build because the platform does
not:** a hook committed with a job's Dead mark (the Dead row below, its trigger met by the
third agent's build, 2026-10-29), a transaction's own time (two of the three agents built
a clock by hand), and a re-issued job that keeps its effect's key. Their rows landed with
0.4.0's records (2026-11-01), beside the 0.4.0 spec's three and its testimony census's ten.

The seven rows that open the table, the five from M2, DECISIONS §6's and the Linux run, are
not debt. Each was weighed against the thesis on 2026-09-13 (the Linux run on 2026-09-21) —
does it make the reviewer catch more, or a stranger's first hour shorter — and none does.
Every row is listed with its trigger so that "is it time yet" has a mechanical answer. The
payload-shapes three were deferred on 2026-09-24 by the spec that shipped detection and
refusal (its §1 and §8): a foreign shape is already refused by name, so each waits for its
trigger. The job-tenancy one was deferred on 2026-09-27 by the
job-tenancy spec's §12. The tenancy-completion two were deferred on 2026-09-30
by that spec's §12.

Outbound effects shipped on 2026-10-14 (spec
[docs/superpowers/specs/2026-10-13-outbound-effects-design.md](docs/superpowers/specs/2026-10-13-outbound-effects-design.md),
plan [docs/superpowers/plans/2026-10-13-outbound-effects.md](docs/superpowers/plans/2026-10-13-outbound-effects.md);
DECISIONS §14). Its exit criterion 5 (spec §16), the docs-only build, was met on attempt 1,
signed by Noah 2026-10-14 (record
[docs/experiments/2026-10-outbound/](docs/experiments/2026-10-outbound/);
[docs/LEDGER.md](docs/LEDGER.md), 2026-10-14, "the outbound docs-only build"). What it
refused or left unproven became the seven outbound rows near the end of
this table: spec §15 refused the first five, plan amendment A19 pinned oracle 2's
switchover row at the write gate rather than through a two-node rig (the sixth), and the
seventh is a node-wiring gap. Each reopens on the first app that needs it. The five rows
after them came from that build's own record
([docs/experiments/2026-10-outbound/](docs/experiments/2026-10-outbound/)): three are
gaps its review found in the outbound oracles (I3, I4, I1), and two are credential facts
its testimony and review surfaced (the id, I5's revocation). The row after them is the
outbound client's compile reading, which spec §16 exit criterion 7 asked BENCH.md to carry
and the one attempt was refused (2026-10-15), so it is owed.

0.4.0's records (2026-11-01) added fifteen rows. Three sit beside the rows they belong
with: the enum `Codec` derive beside the deep fingerprint, and the two rows named for 0.5.0
beside the Dead row. The rest close the table. Two are non-goals of the 0.4.0 spec (§2):
the state-machine declaration, with the spec's trigger, and properties derived from an
app's shape, with the spec's trigger and a second channel beside it (its row says why). The
ten after them come from 0.4.0's testimony census, which gave every finding a docs-only or
stranger build had reported a disposition, backward (spec §4 G): each is a finding still
open that no row covered, with the trigger the census wrote. None of the fifteen was
weighed as the seven that open the table were. A row whose trigger the record already meets
says so in its trigger cell, with the evidence. That makes it eligible, not scheduled: a
release names it, as 0.5.0 names the Dead row. Noah decides whether a row with a met
trigger is scheduled, at the next milestone's planning. The two rows after the census's ten
came from 0.4.0's docs-only build (2026-11-01, review I4 and its fixes' report); that build is also testimony in the Dead, transaction
time and first-credential rows, and it meets the trigger of the small-API-gaps row. The
last row came from the outside agents' retest on 0.4.0 (2026-11-08, agent A;
[docs/LEDGER.md](docs/LEDGER.md), that date): the one finding the retest left open after
its three fixes, given a row by the docs-only bar's rule (sub-project 3).

| Item | Trigger |
|---|---|
| **Blob store** (content-addressed, spill-over columns, direct streaming) | First adopter needs attachments. `ObjectStore` exists behind hard rule 1 since the archive shipped; the design cost is what it was minus that trait. |
| **WebSockets** | First adopter needs bidirectional. A transport swap behind the same generated API (routes, types, `Result<T, E>`), not a second client. |
| **Typed telemetry with change-time checks** — the observability contract as a lockfile | On-thesis, and the one item here that is reviewer work; when there is a dashboard to protect. |
| **Read-only Postgres-wire endpoint** | First BI ask. Compatible with DECISIONS §4; never the primary interface. |
| **Own LSM storage engine** | DECISIONS §7's triggers, unchanged. Trigger (c) — post-launch adoption — is after M3 by definition, which is why the engine cannot gate it. |
| **Design C timeline epoch** (DECISIONS §6's sharpening step short of consensus) | The first loss-window finding that the bounded, named window does not cover. |
| **Bare-metal Linux BENCH run** (owed since 2026-08-15) | A Linux box Noah runs docs/BENCH.md's protocol on. Resizes README's scaling paragraph, which since 2026-09-21 states the laptop figure and says no bare-metal run exists. |
| **Versioned migrations for event and job payloads** (payload-shapes spec §8, approach B) | The first app that needs exact resume, or a Pending job, to survive a shape change it cannot express as a new type. |
| **A deep fingerprint** — every `Codec` type carries its own shape constant, so a new variant on a nested enum changes the fingerprint of every table and payload that holds it | The first nested-type change that misdecodes in a real app or a docs-only build. |
| **A derive for an enum's `Codec`** (0.4.0 spec §2) — today GUIDE §2 teaches a hand-written tag table, where a misnumbered tag compiles | The first misnumbered tag found in a docs-only build or a review. A derive that numbers tags by declaration order makes reordering variants a silent format change, which is the deep fingerprint's problem (the row above), so the two are weighed together. |
| **A commit-time shape lock** — a committed fingerprint file plus a check verb | The first shape change that reaches a deploy unnoticed. Apps on the generated TS client already get a CI signal for event payload changes from `check-client`'s staleness diff. |
| **`enqueue_keyed_in`** — a keyed declared fan-out (job-tenancy spec §12) | The first fan-out whose job needs an external idempotency key. `enqueue_in` and `ScopedTx::enqueue_keyed` both exist; the keyed fan-out is the one combination deferred. |
| **A cross-tenant subscription** — one feed over every tenant's events (tenancy-completion spec §12) | The first app that needs a feed across tenants. It arrives as a declared capability `describe` lists (DECISIONS §11), the shape `cross_tenant` already gives reads. |
| **A tenant's own jobs and events, read through `ScopedTx`** — for a job-status or activity view (tenancy-completion spec §12) | The first app with a job-status or activity view. Until then an app keeps its own table of what it enqueued and emitted, and the platform's tables stay the operator's. |
| **Outbound with `cross_tenant`** (outbound-effects spec §15) | The first app that needs it. |
| **HTTP/2 for outbound calls** (spec §15) | The first app that needs it. |
| **Outbound proxies** (spec §15) | The first app that needs it. |
| **Request signing** — AWS SigV4, which SES needs (spec §15) | The first app that needs it. |
| **Streaming replies** (spec §15) | The first app that needs it. |
| **A two-node switchover under outbound load** (plan amendment A19) | The first replicated app with outbound jobs. Oracle 2's switchover row is pinned at the write gate (`Db::forbid_writes`), not through a two-node rig. |
| **`OutboundOptions` on `serve_replicated`, `backup` and `archive`** — they take none, so a replicated node can reach only public-CA hosts at their real addresses | An app that calls an internal API behind a private CA and must run replicated. |
| **A per-host coverage key for an outbound timeout that fired** (outbound docs-only build, review I3) | The first app that must `require` that its own timeout path ran. The outbound coverage keys are per fault axis, so a host's timeout path can be dead in every universe with no oracle saying so. |
| **Dead reachable in the sim, and a hook or documented path for an outbound job that goes Dead** (review I4) | The first app that must act on an outbound effect it gave up on. `outbound_settles_by_horizon` rightly counts Dead as settled (spec §10), but no universe reaches Dead, and an app has no documented way to notice one. **Shipped in 0.5.0.** Met 2026-10-29 by the third outside agent's build on 0.3.0 (C, an agent runtime; 0.4.0 spec §1 item 12): a Dead job is invisible to an app, since no hook fires and an app cannot read `JobRow`, so C built a watchdog and a redrive, about a third of its jobs code, and asked for a hook when a job goes Dead. 0.4.0's docs-only build (holdqueue, 2026-11-01) is further testimony: its ready notification goes Dead after about 68 minutes of notify outage and the patron is never told, because nothing an app can see marks the job Dead (review I5). 0.5.0 ships the `on_dead` hook, required stance and all, and a sim universe reaches Dead. |
| **A transaction's own time** — a deterministic `tx.now()` (0.4.0 spec §1 item 12 and §2; agents A and C) | **Shipped in 0.5.0.** `tx.now()` and the timed enqueues carry it, and holdqueue's T4 ask (no time inside a transaction and no delayed job) is met. Two of the three outside agents on 0.3.0 (A's commitment ledger, C's agent runtime) each built a logical clock by hand, an `AppClock` row advanced by cron, because a transaction has no time, and C asked for a deterministic `tx.now()`. The M0 invoicing build did the same (`examples/invoicing/FINDINGS.md`), as did the helpdesk and statuspage builds, and 0.4.0's docs-only build (holdqueue, 2026-11-01), whose cron-advanced clock drives a 7-day hold expiry that fires late by every downtime, since a catch-up tick advances it one step (review T4, T13; GUIDE §7 now says so). It was expected to share the stored wall reading's cost (0.3.0 ruling R3, below), a new log record that changes the log format and every pinned seed's byte counts; the shipped `tx.now()` records no time in the log (a seed's replay reproduces the held reading), and only a timed enqueue's due time is a new record (`0x0e`). |
| **A re-issued job that keeps its effect's key** — a caller-chosen key on enqueue (0.4.0 spec §1 item 9; agent C) | **Shipped in 0.5.0** (`tx.reissue_at` and `tx.reissue_after`, from an `on_dead` hook). The minted outbound key belongs to one job and covers its retries and re-dispatches, so an effect enqueued twice (a redrive after Dead, a re-issued step) sends two keys, and a remote that honours keys applies it twice. C planted exactly this double fire, and the sim caught it. GUIDE §6 now says a re-issue is a new job with a new key, so its `call` must send an id of the effect itself; the platform half lands with the Dead hook, whose redrive is the re-issue that needs it. |
| **Mechanical enforcement of GUIDE §8's rule that `durable_corruption_fired` may excuse a give-up, never wrong data** (review I1) | The second app found routing a consistency or isolation oracle's failure through `durable_corruption_fired`. Today the rule is stated in prose and enforced by nothing, so only a review or a mutation proof finds one. **Trigger met** (0.4.0's testimony census, 2026-10-31): the statuspage build's review I-5 (2026-09-24) found its app's `expect` excusing a cross-org 200 under `durable_corruption_fired`, and the outbound build's triage app, found by its review's mutation run (I1, 2026-10-14), was the second. |
| **Credential row ids drawn independently of the secret** (testimony row 12) | The first app that must show a key's or session's id to someone other than its holder. Today a `Session`'s or `ApiKey`'s id is the first 64 bits of its secret's draw (GUIDE §5). |
| **Revocation that ends what a credential minted** (review I5) | The first app whose revoke must also end the sessions and keys minted by a request that presented the revoked credential. Today revoking a key ends that key alone (GUIDE §5), so a session traded for it stays live until it expires. |
| **The outbound client's clean-build compile delta** (owed since 2026-10-15; spec §16 exit 7; docs/BENCH.md, "Compile latency of the outbound client (2026-10-15) — refused, no delta published") | Either an instrument change that lands for its own stated reason, which is what lets `scripts/compile-bench.sh` start again, or a clean build reported as an adoption cost. The refused run (sha 3b9529cb, handler-body `Unstable`) stays attempt 1 and is never re-rolled; no gate moves to unblock it. |
| **Coverage-guided fault and input search** | A mutant that survives drawn inputs because the case that distinguishes it is too rare for uniform draws. The second agent's surviving mutant (2026-10-22) is caught by plain drawn inputs. |
| **An agent-integration contract (MCP tools) generated from `describe`** | The first app whose consumer is an agent calling it as tools. |
| **One outbound job over several declared hosts** | The first app where duplicated outbound jobs cause a defect, not only boilerplate. |
| **A stored wall reading** (0.3.0 ruling R3), which tightens the across-restart bound from the time since the store's latest recorded time plus `CLOCK_TOLERANCE` to the tolerance alone | An adopter whose timed obligations, or whose sessions, must not run late across a backwards clock step at restart. Today the open-time bound is recovered from times the store already holds (DESIGN §1), because a stored reading is a new log record that changes the log format and every pinned seed's byte counts. |
| **Gating the hand-written platform tables** (`charpente.jobs`, `.events` and `.schema_versions`) as a named step (0.3.0 ruling R6) | The first release that changes one of them. Today they have no data-directory detector (DESIGN §5, "Deploy"), because gating them adds an adoption record to the first job or event write of every run, which moves every recorded batch. |
| **A pair search that does not wait for a quiet round** (0.3.0 ruling R39), such as trying the pairs of adjacent positions first | A shrunk run reported from a real app, not a probe, that a pair of equal choices would have lowered further. Today the pairs of equal choices wait for a round in which everything else kept nothing (GUIDE §8 "Shrinking a failure"), so a stall only a pair can leave, among 24 other draws, ends on the budget with every draw still at the pair's value (27 choices of 5 in 500 replays), where trying the pairs in every round reached the minimum in about 430 (0.3.0's round-5 review, m-b at n=24). Trying them in every round is what left fault axes on and a fault-coupled input high, which misdirects the fix. |
| **The request arm stops on a checkpoint failure nothing else reports** (0.3.0 final review, M1), as `Db::transact` answers `Err` and the job and cron arms stop | The first node observed with WAL GC stalled by a repeating checkpoint failure. Today a request batch whose records reached the store keeps its 200s when the horizon breadcrumb's sync then fails, which poisons and latches nothing, and no one is told until the next job or cron commit stops the node, at most a day on a node that serves only requests. Stopping at once moved six of `crates/charpente/tests/realtime.rs`'s swarm properties and pinned universes (2026-10-25), so it lands with their re-measurement. |
| **A worked first-credential example, and a sanctioned way for deploy-time configuration to reach a handler** (0.3.0 docs-only build, review (e) and its deploy-configuration row; ruling R48) | A second app that needs either. The 0.3.0 docs-only build's app minted its first operator key from `PRICEWATCH_OPERATOR_SECRET`, read once at start into a set-once `static` (a `OnceLock`) that its route compares against. The route mints one key per request and its effect is its writes, which the log carries, but the `static` is state the hard-rule fence cannot see. GUIDE §5 sanctions minting a first credential in prose and works no example, and shows no way for deploy configuration to reach a route, so a set-once static is the only way today and nothing says whether it is sanctioned. **Trigger met** (0.4.0's testimony census, 2026-10-31): three builds before this one each had to invent a first credential, the helpdesk (FINDINGS), the statuspage (review I-2) and the outbound triage app (review Minor 1, testimony T5), so pricewatch was the fourth app to need it. 0.4.0's docs-only build (holdqueue, 2026-11-01) is the fifth: its `POST /operator` is trust on first use (review T2, I8). |
| **A state-machine declaration** — an app declares its states and the transitions between them as a table the framework holds it to (0.4.0 spec §2; agent A's sixth ask) | A docs-only build's mutation run misses a mutant in a state transition's match, or its review finds a Critical a declared transition table would have refused. Every app so far hand-writes one: invoicing, the helpdesk, statuspage, and A's commitment ledger with its five-state lifecycle. |
| **Properties the framework derives from an app's shape**, such as an event-proof check for every route that emits (0.4.0 spec §2; agent A's second ask, its template half) — the strongest answer to "who writes the oracle", and too large for 0.4.0 | A docs-only build's mutation run misses a mutant that removes a `tx.enqueue` or `tx.emit`, or its review or hand pass deletes one and its suite stays green. The spec's wording names only the mutation run, and that channel is narrow, not closed: cargo-mutants 27.1.0 never deletes a statement, so it removes such a call only by replacing the whole body of a function whose effect the call is, when it can build that function's return value (GUIDE §8), as for a helper that only emits and returns `Ok(())`. 0.4.0's records added the second channel so that a deletion inside a larger function, such as a handler's, fires it too; the instrument half of that gap is the statement-deletion row below. Deleting either in A's app went red (0.4.0 spec §1), so it has not fired. |
| **Mutants that delete a statement** — a write, an enqueue, an emit, a scope line or a request header, beside cargo-mutants' body and operator replacements (testimony census P1: statuspage C-2 and platform finding 10, outbound review Minor 11 and I2, the 0.3.0 docs-only build's hand deletions H1-H5). Today cargo-mutants 27.1.0 never deletes a statement (GUIDE §8): it removes one only by replacing the whole body of a function whose effect it is, so every build hand-mutates the rest of its writes, and "0 missed" says nothing about a write inside a larger function, such as a handler's | A docs-only build's review or hand pass deletes a write, enqueue, emit, scope line or header in a file `charpente test --mutants` reported clean, and the app's suite stays green. The gap is shown, though this wording has not fired: statuspage's review (C-2, 2026-09-24) deleted `create_operator`'s scope line and rewrote four write routes to `tx.open_scope::<Org>(org, None)?`, and the batch on the same master seed stayed byte-identical; none of `operators.rs`'s 22 mutants removes that line, but that build's mutation run covered four other files, so `operators.rs` was never reported clean. |
| **A clean stop for a `launch` or `serve` node, or an upgrade-barrier text a launch node can follow** (testimony census P2: statuspage platform finding 3). The barrier says to run the previous binary once to a clean shutdown, and GUIDE §12 says a clean exit always checkpoints, but a `launch` node has no clean exit ("no graceful shutdown in v0", GUIDE §5) and the platform handles no signal. The remedy that works, a traffic-free boot of the previous binary (recovery checkpoints), is written only in `db.rs` | The first version-bumped deploy that the upgrade barrier refuses. **Trigger met** (the census): statuspage hit it on its own v2 upgrade (its FINDINGS, 2026-09-21), and against hard rule 5 the error named a fix the documented production shape cannot perform. |
| **The counter pattern's two hazards, and a per-scope id primitive** (testimony census P3: invoicing FINDINGS; statuspage review I-3 and platform finding 6; outbound review Minor 2). GUIDE §2's one `#[table(global)]` counter row shares one id space across tenants, so any id a public route returns counts every tenant's writes, and a single-node corruption cut rewinds the counter and re-mints ids, so a stale client id names a different row. GUIDE §16 still says "Ids stay globally unique" | Doc half: the next commit that edits GUIDE §2 or §16 states both hazards, shows a scoped counter, and qualifies "globally unique"; two builds, statuspage and the outbound triage app, have already found the side channel. Primitive half: the first adopter whose clients hold ids across a single-node corruption cut, or that must not reveal volume through ids. |
| **Per-tenant event cursors**, so a public feed reveals no other tenant's event or commit volume (testimony census P4: statuspage platform finding 11, the platform floor under its review's I-3). Today a cursor is a global `<id>.<seq>` (GUIDE §11), and a tenant's tail skips other tenants' rows without renumbering them | The first app that must not reveal one tenant's activity volume to another through a public feed. Per-tenant numbering is a new log-record shape that moves every pinned seed (0.3.0 ruling R3's cost). |
| **`replays_identically` over the universe's own fault config, with an inner give-up reaching the batch** (testimony census P5: the helpdesk's F2, second consequence; statuspage review M-3; outbound review Minor 9). Today it builds two `Sim::new` worlds under `FaultConfig::default()`, so a replay property never replays its universe's swarm or outbound faults, and a give-up inside the workload lands on an inner world: statuspage counted verdicts in which both inner runs gave up and compared `None == None` | A determinism defect that a swarm- or outbound-faulted universe shows and a default-fault replay does not, found by a `--master-seed` double run, a review or an adopter. |
| **A password rung on the resolution ladder**, a salted KDF and a constant-time verify (testimony census P6: helpdesk FINDINGS; statuspage #10, #11 and platform finding 9; outbound testimony T4). GUIDE §5 says charpente has no password check; the stored hashes are unsalted SHA-256 of 256-bit tokens, and `charpente::auth::hash_token` is public and undocumented | The first app whose users must sign in with a password and cannot use an IdP. |
| **Permissions inside a tenant**, API-key scopes and roles (testimony census P7: helpdesk FINDINGS). GUIDE §16 calls them "their own future design", and no row covered them | The first app whose principals in one tenant need different rights. |
| **An `Idempotent` model that does not store a transient reply**, a 5xx or 429 its inner model returned (testimony census P8: outbound testimony T18, T40). Today a repeated key gets the first reply again whatever its status (GUIDE §8), so a model's 503 replays until the job is Dead, and the triage build hand-wrote its mailer to avoid that | A second app that hand-writes its own idempotent remote model because `Idempotent` replays a stored 5xx. |
| **Four small API gaps from statuspage, and `TxError: Display`** (testimony census P9; 0.4.0's docs-only build, review T5): `ApiError: Display` (#7: a job can only `{e:?}` it); `TxError<E: Display>: Display` (`TxError` derives `Debug` alone, so a `TxError<ApiError>` that formats needs both impls, and GUIDE §4 shows a hand-written `describe`); `MigrationInfo: Debug` (#37); a `Codec` for `Vec<T: Codec>` (#8: a generic impl overlaps today's `Vec<u8>` impl, since `u8: Codec`, so it replaces that impl, byte-identically if the length prefix matches); and `serde_json` reachable from `charpente` or listed in the scaffold's dev-dependencies (#18) | A second build that hits one of them. **Trigger met** (0.4.0's docs-only build, 2026-11-01): the holdqueue app needed a `TxError<ApiError>` to format, which takes `ApiError: Display` as well, so it is the second build to hit one of them, after statuspage's #7 (review T5). |
| **Two GUIDE gaps, docs only** (testimony census P10): (a) §16 never says that the table `authorize` reads is global (`authorize` takes `&Tx`), so membership sits outside the type-level guarantee and only the app's properties check it (statuspage platform finding 10); (b) §4 never said that a hand-written struct `Codec` is its fields' codecs in declaration order, though its own `impl Codec for SendReceipt` relies on it, and GUIDE stated the rule only for `#[table]` rows, in §2 (invoicing FINDINGS); **(b) closed 2026-11-01** by the commit that edited §4 for 0.4.0's docs-only build | The next commit that edits §16 closes (a). |
| **A shrinker retry that lowers a count when an item's deletion fails to keep** (0.4.0 docs-only build, review I4). Pass 1 deletes an item whole, but in a loop whose length is a drawn count the deletion leaves the count stale, and lowering the count alone drops the last item, so neither half of the joint move keeps alone and the shrink stalls: the build's shrunk `copies_are_never_set_aside_twice_or_beyond_stock` failure kept 64 draws and 7 holds where a 28-draw run with 2 holds fails the same check (review (e)). The retry: when an item's deletion fails to keep, try it again with the nearest earlier nonzero choice, the loop's count, lowered by one. GUIDE §8 now teaches a per-item "more" draw for loops whose later items matter; the scaffold and `drawn_inputs.rs` keep their count, since changing them moves recorded batches | A second build whose shrunk failure keeps items a smaller count would drop. |
| **`examples/invoicing`'s two logins mint a session for a subject the request body names** (0.4.0 docs-only build, its fixes' report, concern 5) | `src/routes.rs:57-60` and `src/bin/tenant_node.rs:143-146` are `public` routes that pass a body field to `create_session`, the takeover GUIDE §5 forbids and the API page no longer shows. The first carries a "Demo only" comment and the second carries none. invoicing is `publish = false`, so no reader of the crates sees it, and the credential census walks only what ships. Making both routes `auth` moves invoicing's recorded batches and its `describe` golden once | The next commit that changes `examples/invoicing`'s routes, or a build or review that copies either login. |
| **`charpente test --mutants` applies the relative-path dev-dependency rewrite in its own scratch copy instead of refusing** (the outside agents' 0.4.0 retest, agent A). cargo-mutants 27.1.0 rewrites relative paths only in `[dependencies]`, `[replace]` and `[patch]`, so a relative dev-, build- or target-dependency path outside the workspace root fails the copied baseline. Since 0.4.0 the verb refuses that layout before the install check, naming the manifest, the table, the dependency and the fix (an absolute path, or `[patch.crates-io]`). Agent A found that it names the fix but leaves applying it to the app | An adopter's own layout (not an eval checkout depending on a local charpente) hits the refusal, or a second build hand-edits its manifest to run the verb. |
| **A hand driver's settle check judged at the kernel's held wall** (0.5.0's reviews) | The first hand-driven app whose tail opens over a store recorded ahead of its clock. A hand driver passes the clock's wall as `at`, which leaves a window of at most 60 s in which a lost attempt-0 row passes the settle check. `Drive` judges "due" at the kernel's held wall, so only a hand driver is open. |
| **A bound on steps per virtual instant** (0.5.0's reviews) | The first app or sim run seen spinning at one instant. A zero-backoff job under an outage whose `on_dead` re-enqueues at once never lets virtual time advance. It is the class of a job that re-enqueues itself immediately, which predates 0.5.0. |
| **A `#[route]` that answers a status other than 200** (the launch demo's rehearsal, review F1). `#[route]`'s glue answers 200 on `Ok` (GUIDE §5), and `ApiError` maps to 400, 401, 404 and 500 only. The task's brief asks for a 201 on a create and a 409 on a conflicting key or transition, so the charpente arm hand-built all seven routes as `Route` structs GUIDE never documents, each parsing its own body: `describe` reports a `Null` response for every one, and the typed surface, the oracle for the HTTP layer, is gone | Fix red-first before the launch demo's public attempt (ruling R43): the judged brief asks for a 201 and a 409, so every charpente entry meets it. A declared success status and a typed conflict on `#[route]` are public API and get their own spec. |
| **A `#[route]` that reads a request header** (the launch demo's rehearsal, review F2). A `Request` carries its headers, but neither a `#[route]` handler nor a hand-built `Glue` can reach them. The brief mandates an `Idempotency-Key` header, so the charpente arm ran a second hyper server in front of the kernel (`edge.rs`, 179 lines, the fence lifted for the module) that lifts the header into the body. It sits in the binary crate, outside every sim oracle, and its `SystemTime::now()` was the second clock behind review F3 | Fix red-first before the launch demo's public attempt (ruling R43): the judged brief's key arrives as a header, so every charpente entry meets it. One shape is a declared header argument typed like a path param, listed by the `describe` contract and drawn by the sim. |
| **`charpente test --mutants` derives its `--timeout` from the `--file`'s package as well as the test set** (0.5.0's mutation run, its instrument finding; ruling R-22). cargo-mutants 27.1.0 bounds its unmutated baseline by the `--timeout` a pass gives it and runs that baseline over the mutated file's own package, while the verb derives the timeout as 5x its timed `cargo test` over the test set. On `kernel/mod.rs` under `--test-package charpente-sim` that was 69 s against a 101 s `charpente` baseline, which read Timeout. Since 0.5.0 the refusal names the timeout and the fix (a larger `--timeout`, or a test set that includes the file's package); the verb does not yet time the file's package and derive from the larger | The first cross-package run that pays a baseline timeout. The scaffold's advertised run (one package, the file's) derives no timeout, so it cannot meet it. |

**Hosted control plane — off the roadmap.** "If this becomes a company: managed replicas,
backups, failover, metrics; the binary stays fully self-hostable" is a business decision,
not a milestone, and a roadmap that carries it reads as if the platform were incomplete
without it. It is not. The moat is the platform; the business, if there is one, is
not-having-to-operate-it.

## Risks, honestly

| Risk | Read | Mitigation |
|---|---|---|
| Rust compile latency poisons the dev loop | **Measured 2026-09-09, and smaller than the bet assumed** — on one 12-core laptop, `dev` profile, against `examples/invoicing`: the inner loop an agent actually meets is 436 ms (handler-body edit) and 515 ms (schema edit); only the once-per-checkout clean build is expensive, at 5.97 s wall / 37.6 s compiler CPU | Prebuilt platform dylib **dropped 2026-09-09** — see the M2 list above; cranelift hot reload remains an open question, not a scheduled mitigation; toolchains are improving underneath us. The "measure app-only incremental times in M0" mitigation **did not happen in M0**: an instrument landed 2026-09-07 (`bench-harness` `compile`/`compile_accounting`), its first reading was refused in full, and the accepted reading came on 2026-09-09. The number, its conditioning and every refusal along the way are in docs/BENCH.md, "Compile latency (2026-09-09)" — read the three phases together, because no one of them is "the compile time". One laptop, one small app, `dev`, n=3 moves this row from "unmeasured" to "measured once", not to "closed". |
| Storage engine is the hardest 20% | Real, why it's staged | redb first; own engine only after the sim harness can prove it; the harness is the safety equipment |
| "No await in transactions" alienates users | Real for humans, smaller for agents | Docs sell the constraint as the feature it is; queue idiom must be *excellent* and heavily exampled |
| Single-writer throughput ceiling | Overrated | No network hop between logic and data; TigerBeetle/Redis-class evidence; publish honest benchmarks early |
| Greenfield-only adoption | Accepted | Agents mint greenfield backends daily; distribution rides agent preference (MANIFESTO §3), not migration |
| Convex/PocketBase move into the quadrant | Watch | Neither has determinism or Rust's compile-time oracle; DST-from-day-one is a multi-year retrofit for them — that's the moat |

## Non-goals (permanent)

No sharding. No multi-master. No SQL-first API. No service mesh. No optional-determinism
escape hatches. No feature flags on guarantees.

## Naming

Crate `charpente` — verified available on crates.io 2026-07-01 (along with the rejected
alternates; `socle`, `massif`, `menhir`, `chassis`, `armature` are taken). Reserve the
crate name early with an empty publish. Publishable at 0.1.0 since the 2026-09-13
publish-path sub-project; the real publish is Noah's, recorded in the ledger when it
happens.
