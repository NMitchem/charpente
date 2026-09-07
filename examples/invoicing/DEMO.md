# DEMO: the sim catches the outbox bug

`finalize_invoice` (`src/routes.rs`) does three things when an invoice goes
Draft → Final: mark the row Final, emit `InvoiceFinalized`, and enqueue
`SendReceipt`. In charpente those three effects are one atomic commit — the
platform gives you no way to split them without reaching for an escape
hatch, because routes only ever see `&mut Tx`, never the `Db` a second
transaction would need. So to show what happens if you *do* split them, we
temporarily added one: `Ctx::enqueue_in_a_second_transaction`
(`crates/charpente/src/http/app.rs`), backed by `Tx::enqueue_raw`
(`crates/charpente-core/src/db.rs`). With it, `finalize_invoice` committed
transaction A (mark Final, emit) as the request's normal ack-after-durable
commit, then a second, independent transaction B enqueued `SendReceipt`
afterward — exactly the shape of the classic dual-write bug that the
outbox pattern exists to work around on conventional stacks: write the
state change, then separately tell the queue about it.

This is the bug every queue-backed stack can carry silently. A crash
between "mark finalized" and "enqueue receipt" leaves an invoice that is
durably Final — the client got its 200, the money is owed and recorded —
and a receipt that will never send. Nothing errors. Nothing logs. There is
no dead-letter, no retry, no trace, because nothing ever recorded that the
second step was owed in the first place. On a conventional stack this
surfaces weeks later as an angry customer email, if it surfaces at all.
Charpente's one-commit-point design deletes this bug class structurally —
which is exactly why reproducing it for this demo required patching the
framework to add a way to opt out. `charpente test --sim` caught the
regression in the very first 1024-universe batch we ran against it.

## The commits

- **WRONG** — `c0dbcea` "demo: WRONG — split finalize into two
  transactions (the outbox bug, deliberately)"
- **RIGHT** — `bfecd64` "demo: RIGHT — one transaction again; the sim
  caught the split (seed 0xc9e68010250237f4)" (a `git revert` of `c0dbcea`)

## The failing seed

24 of 1024 universes failed on the first batch run against the WRONG
commit (master seed `0xb000b04a66676484`) — every failure in
`receipts_exactly_once`, none anywhere else. First failure:

```
sim failure in receipts_exactly_once: ack-durability violated: 1 finalize 200s acked but only 0 SendReceipt jobs durable
  seed:   0xc9e68010250237f4
  config: FaultConfig { write_error: 0.001, sync_error: 0.0, crash_apply: 1.0, crash_torn: 0.0, disk_latency: (0ns, 2ms), net_latency: (0ns, 0ns) }
  repro:  charpente test --sim --filter receipts_exactly_once --seed 0xc9e68010250237f4
```

One command reproduces it byte-for-byte:

```
$ charpente test --sim --dir examples/invoicing --filter receipts_exactly_once --seed 0xc9e68010250237f4 --format human

sim failure in receipts_exactly_once: ack-durability violated: 1 finalize 200s acked but only 0 SendReceipt jobs durable
  seed:   0xc9e68010250237f4
  config: FaultConfig { write_error: 0.001, sync_error: 0.0, crash_apply: 1.0, crash_torn: 0.0, disk_latency: (0ns, 2ms), net_latency: (0ns, 0ns) }
  repro:  charpente test --sim --filter receipts_exactly_once --seed 0xc9e68010250237f4
```
(exit code 1, message identical to the original batch run's failure report.)

## The green re-run

After the revert (`bfecd64`), the same 1024-universe batch against the same
app is fully green:

```
$ charpente test --sim --dir examples/invoicing --universes 1024 --jobs 8 --format human
sim: 1024 universe(s) per test from master seed 0xd9334e2eda1d4e3c
sim: 1024 universe(s) per test, all green (master seed: 0xd9334e2eda1d4e3c)
```

(Transcript preserved as it was printed. That summary line has since been
retired: a run now ends with its verdict/give-up split — `N verdict(s), M gave
up, no failures` — instead of "all green", because "green" was equally true of
a universe that gave up and one that adjudicated. This app does not yet call
`Sim::give_up`, so the give-up count it reports today is unverified rather than
measured. See docs/GUIDE.md's "Giving up" section.)
