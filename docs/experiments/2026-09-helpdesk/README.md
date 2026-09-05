# The docs-only helpdesk experiment (2026-09-03 → 2026-09-05)

An agent was given `BRIEF.md`, `README.md` and `docs/GUIDE.md` — nothing else, explicitly
not `crates/`, `DESIGN.md` or the specs — and asked to build a multi-tenant helpdesk on
charpente. The point was never the app. It was to find out what the docs alone are enough
to build, against the tenant-scoping surface that had just landed. `FINDINGS.md` is the
agent's own friction ledger, written as it went.

**The code is not merged and should not be.** It lives on the `docs-only-helpdesk` branch
(`8913038`), forked from `tenant-scoping` at `6827d75`. Its five commits touch only
`examples/helpdesk/` and the workspace `members` line. Review on 2026-09-05 found three
Critical defects in the app *as an example* — an unauthenticated `POST /agents/:id` that
lets anyone join any org, an unscoped `tx.get(id)?.is_some()` existence check that works as
a cross-tenant oracle, and five sim properties that are all one-sided (deleting the entire
SLA sweep loop leaves the suite green). It also adds `examples/helpdesk` to `members`
without updating CLAUDE.md's inventory, so it reddens `doc_inventory` at its own merge
base. Shipping it as an example would teach the hand-filtering pattern GUIDE §16 exists to
replace. The branch is kept, pushed and unmerged; these two documents are the part worth
keeping.

## Read FINDINGS.md as testimony, not as findings

It is an honest record of what the build *felt* like, and that is its value. Two of its
claims about charpente are wrong, and one is right about a symptom for the wrong reason.
Checked against the code at `14f5a94`:

- **F1 — right that it is a defect, wrong about the cause.** The agent read a raw redb
  `page_manager.rs:231` panic trace and concluded redb was breaking its durability
  property, then discarded the property. In fact `Store::open` wraps the open in
  `catch_unwind` (`crates/charpente-core/src/table.rs:756-773`) and `Db::open` rebuilds the
  store from the WAL (`db.rs:2080-2122`) — the universe was green the whole time. But no
  panic hook is installed anywhere in `crates/` (grep for `set_hook`/`take_hook`: zero
  hits), so the default hook prints the trace on the way past. Under hard rule 5 that is a
  false alarm that demonstrably destroyed a competent agent's work. Still unfixed.
- **F2 — right, and root-caused here rather than in FINDINGS.** The cause is neither thing
  the document guesses. `checks::acked_implies_durable` takes a *seed* and builds its own
  `Sim::new(seed)` internally (`crates/charpente-sim/src/checks.rs:171-177`), so the
  `restart` coverage that `Sim::restart` credits (`lib.rs:667`) accrues to a sim the harness
  never reads — a `require: ["restart"]` over that checker cannot fire, ever. Second
  consequence, not previously written down anywhere: `Sim::new` is `FaultConfig::default()`,
  which zeroes `bit_flip`, `misdirected_write`, `dir_entry_loss`, `read_error`, `conn_drop`
  and `net_spike`, so any property built on that checker never sees the swarm matrix —
  contra this brief's "under the full fault matrix". **This is the experiment's real
  payoff**: a platform-level oracle defect that no amount of reading the platform's own
  tests would have surfaced, found because an outsider used the public checker library the
  way the docs describe.
- **F4 — factually wrong, struck.** It claims no GUIDE example derives `Serialize` on a
  route body. `docs/GUIDE.md:2039` is `#[derive(Wire, Serialize, Deserialize)] struct
  HitBody` and `:2122` sends it with `Request::json` — in §8, a section FINDINGS says it
  read in full.

## What the brief asked for that the app did not deliver

Worth keeping visible, because it calibrates how much weight the friction ledger carries:
`BRIEF.md` required authentication on "every ticket, comment and agent route" and the
tenancy invariant "under the fault matrix". The app shipped an unauthenticated agent route
and one-sided properties. So where FINDINGS reports that something was hard, that is real
signal; where it reports that something was *done*, check the code.
