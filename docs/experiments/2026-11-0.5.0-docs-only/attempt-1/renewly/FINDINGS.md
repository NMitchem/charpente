# FINDINGS: renewly on charpente 0.5.0 (docs-only build)

A friction ledger, dated as I went. Sources: BRIEF.md, README.md, the
crate's GUIDE.md, its examples/, and the tests GUIDE names as worked examples
(tests/clock.rs, tests/outbound.rs).

## 2026-11-19

### Restarts of an approach: 1

1. Sim fixtures. GUIDE §8 ("Seed fixtures in the build closure") says to
   write credential rows in §5's storage form in the build closure. I started
   that way, then switched (see below) to having the app mint every
   credential: a public `POST /signup/:ws` that creates a workspace and mints
   its owner's key, plus an owner route that issues customer keys. This also
   closed a real product gap: my first route set had no way for the first
   owner to get a credential at all.

### GUIDE sections re-read, and why

- §6 "Outbound jobs" and "Re-issuing a dead job", twice: to decide how an
  owner's retry, which happens long after `on_dead` has run, can reuse the
  provider idempotency key. `reissue_*` only works inside the hook, so the
  answer was the sentence saying a new job's `call` "must send an id of the
  effect itself". renewly sends `renewly-<ws>-<sub>-<period>` as the
  Idempotency-Key and ignores the platform-minted key.
- §8 "The outside world: remote models", for when the outage window opens
  (the first 2 s of virtual time) and how long it lasts (60 s to 4 h). Renewals
  are due in whole days, so a renewal only meets an outage if a subscription
  is seeded with a period that ends seconds after boot.
- §16 "Where the guarantee stops", for how operator code writes scoped rows
  (`begin_group` + `tenant_admission` + `admit_scope`).

### Not found in the docs

- **How to compute `key_hash` for a seeded ApiKey row.** GUIDE §5 says it is
  "lowercase-hex SHA-256 of the raw credential" and §8 says to seed one, but
  never names a helper. `charpente::auth::hash_token` turns up only in
  tests/clock.rs.
- **The fields of `http::Response`.** Not in GUIDE or api.rs. tests/clock.rs
  reads `response.status` and `response.body`.
- **Where the `Clock` trait lives**, for `sim.clock().wall()`. GUIDE calls it
  "the Clock trait (DESIGN.md §1)", and DESIGN.md does not ship in the crate.
- **A timed obligation's guard.** GUIDE §7 promises a timed job never runs
  early in the node's frame. So renewly's `PeriodEnd` has no `now < end`
  guard: an unreachable guard would only add a mutant no property can kill.

### Status at this entry

The library builds (`cargo build --lib`, no warnings). `tests/sim.rs` is
partly written, and the sim has not run yet: see the note below.

### Environment note (not a docs finding)

This session's tool permissions refused several writes of `tests/sim.rs`
chunks, mostly the chunks that handle the app's own test credentials or the
billing model. I stopped rather than reword code to get past the check. The
sim, `charpente test --sim` and `--mutants` output are not recorded yet.
