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

(Transcript preserved as it was printed; `--universes` is now `--runs`. That
summary line has since been retired: a batch now prints a line per check with
how many runs tested it and how many were skipped, and ends `sim result: ok. N
passed; 0 failed. T of R runs tested a check.` instead of "all green", because
"green" was equally true of a universe that gave up and one that adjudicated. Since 2026-10-01
`acked_writes_survive_restart` calls `Sim::give_up` when durable corruption cut
an acknowledged write. Since 2026-10-02 every fault-fallout exit of the app's
fault-injected properties gives up too, as does a universe that ended with
nothing to check (no write acked, no finalize or receipt to count, no reminder
fired), so a verdict is a universe that checked something. A status no fault
produces, at the minted key's first read or at the revoked key's read, fails
the universe instead. Each of the four requires the coverage key its verdict
marks, so a give-up arm that swallowed every universe fails the batch rather
than passing with no verdicts. At master seed
0x53b00051, 256 universes each, the four such properties reach 125
(`acked_writes_survive_restart`), 120 (`receipts_exactly_once`), 93
(`overdue_reminders_over_months`) and 118
(`api_key_authorizes_then_revoked_is_rejected`) verdicts.
`app_replays_deterministically` compares both worlds' durable state
even after a faulted boot, so it always reaches a verdict. See docs/GUIDE.md's
"Giving up" section.)
