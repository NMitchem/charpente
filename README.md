# charpente

A Rust backend in one binary: server, database, jobs and auth. It comes with a simulator that
runs your routes, jobs and data through crashes, lost replies and outages, and every failure
replays from its seed.

You, or your coding agent, write the app as Rust. `charpente test --sim` runs it hundreds of
times under injected faults and checks the properties you wrote after every run.

https://github.com/user-attachments/assets/9dd78104-beee-4ed8-aab7-5a6c4075be9f

## Example: a double charge

[`examples/double-charge`][double-charge] is an order API with one table, one route and
one job. `POST /orders/:id` stores an order and enqueues a `Charge` job, which calls a payment
gateway, `pay.example`, after the commit. In the simulator the gateway is a model, the demo's
`tests/gateway`. This is the route:

```rust
#[route(POST "/orders/:id", public)]
pub fn place_order(tx: &mut Tx, id: Id<Order>) -> Result<Id<Order>, ApiError> {
    tx.put(&Order { id, paid: false });
    tx.enqueue(Charge { order: id });
    Ok(id)
}
```

It is missing three lines: the ones that answer an order that is already stored. Of the checks
you would run on it, only the simulator says no:

| Check | What it says |
|---|---|
| `cargo build` | It compiles. |
| `cargo clippy --all-targets -- -D warnings` | No warnings, with the app's deny-list of clocks, randomness and raw IO in force. |
| `cargo test --test once` | It passes. Each order is posted once, with no faults, and each is charged once. |
| `charpente test --sim` | `order 9 charged twice`, with the seed that replays it and the fix. |

Each row is checked by a test in
[`crates/charpente-cli/tests/demo.rs`][demo.rs]. This is the
simulator's run:

```console
$ charpente test --sim --master-seed 0x434841524745
running 1 sim check, 256 runs each, under random injected faults (master seed 0x0000434841524745)

sim failure in each_order_is_charged_once: order 9 charged twice: its POST was sent again after a fault hid its success and placed the order again; answer an order that is already stored
  seed:   0x82eac71fbdeda9de
  fired:  boot:booted-after-retry: 1, boot:retried: 2, crash:applied: 1, fault:outbound_lost_reply: 1, fault:sync_error: 3, outbound:completed: 51, restart: 1, restart:process: 2
  inputs: orders=48
  shrunk: orders 71 to 48, 6 faults off, in 13 replays
  repro:  charpente test --sim --filter each_order_is_charged_once --seed 0x82eac71fbdeda9de --inputs 00 --faults-off crash_torn,object_put_error,object_get_error,outbound_refused,disk_latency,net_latency

skipped runs hit an injected fault before reaching the check, so they prove nothing either way:
  each_order_is_charged_once — 8 runs; it failed, so only --format json lists their reasons

check each_order_is_charged_once ... FAILED  (248 tested over 49 distinct inputs: 28 failed; 8 skipped)

sim result: FAILED. 0 passed; 1 failed. Runs tested: 248 of 256.
rerun exactly this batch: charpente test --sim --master-seed 0x434841524745
```

The `repro:` line replays the batch's first failure on its own, with smaller inputs and fewer
faults, and it fails the same way:

```console
$ charpente test --sim --filter each_order_is_charged_once --seed 0x82eac71fbdeda9de --inputs 00 --faults-off crash_torn,object_put_error,object_get_error,outbound_refused,disk_latency,net_latency

sim failure in each_order_is_charged_once: order 9 charged twice: its POST was sent again after a fault hid its success and placed the order again; answer an order that is already stored
  seed:   0x82eac71fbdeda9de
  fired:  boot:booted-after-retry: 1, boot:retried: 2, crash:applied: 1, fault:outbound_lost_reply: 1, fault:sync_error: 3, outbound:completed: 51, restart: 1, restart:process: 2
  inputs: orders=48
  repro:  charpente test --sim --filter each_order_is_charged_once --seed 0x82eac71fbdeda9de --inputs 00 --faults-off crash_torn,object_put_error,object_get_error,outbound_refused,disk_latency,net_latency

check each_order_is_charged_once ... FAILED

sim result: FAILED. 0 passed; 1 failed; 0 skipped, on seed 0x82eac71fbdeda9de with the given inputs, with faults off: crash_torn, object_put_error, object_get_error, outbound_refused, disk_latency, net_latency.
```

The first `POST /orders/9` committed the order and its `Charge` job in one transaction, and then
an injected fault kept the client from hearing that it succeeded. A client that hears no success
sends again, so the POST arrived a second time. `place_order` ran again: `tx.put` stored the order
again, and `tx.enqueue` added a second `Charge`. Each job carries its own idempotency key, so the
gateway, which dedupes on that key, took two charges. A test that sends each order once never
meets this. It takes a commit, a fault that hides it from the client and a re-send, in that order:
the simulator injects the fault, and its recovery step re-sends what got no answer or a fault's
error reply, as a client would.

The bug needs both halves of `place_order` as written: `tx.put` is an upsert, and the enqueue
after it runs every time. A route that `INSERT`s on the client's id would refuse the second POST
with a unique violation, roll back its job and charge once. The same double charge happens
wherever a create upserts or mints its id on the server, but not every way of writing this route
has it.

The fix is the three missing lines, at the top of `place_order` (`tx.get` also needs `View` in
the `use charpente::{…}` line):

```rust
if tx.get(id)?.is_some() {
    return Ok(id);
}
```

With them back, the same command passes.

To run it yourself, [install the CLI](#install), then:

```sh
git clone https://github.com/NMitchem/charpente
cd charpente/examples/double-charge
charpente test --sim --master-seed 0x434841524745
```

The first run builds the demo and its dependencies before it prints anything. The demo pins
charpente 0.6.0, which printed the output above, and the master seed fixes every run in the
batch, so you get the same output. Leave out `--master-seed` and the batch draws a master seed of
its own: the order number and the seed change, and `demo.rs` checks that other master seeds fail
the same check with the same fix.

## Install

```sh
cargo install charpente-cli
```

You'll need Rust 1.99 or newer. If cargo refuses the install because charpente-cli needs a newer
rustc, run `rustup update stable` and install again.

## Use

```sh
charpente new myapp && cd myapp   # one table, route, job and event, and a sim with four checks
cargo clippy --all-targets -- -D warnings   # the deny-list: no clocks, randomness or raw IO in app code
charpente test --sim              # every check, under injected faults, each run from its own seed
charpente test --mutants --file src/lib.rs --test-package myapp   # each mutant in src/lib.rs that no check catches
```

Then delete the `tx.enqueue` line in `src/lib.rs` and run the sim again: the second check,
`acked_post_implies_the_echo_after_recovery`, fails with the inputs that break it, shrunk to a
run no shrinking step can make smaller, and a command that replays it. `test --mutants` runs
cargo-mutants (`cargo install cargo-mutants --version 27.1.0 --locked`), which replaces function
bodies and swaps operators but never deletes a statement, so a deleted line like that `tx.enqueue`
is one to try by hand. If clippy is missing, `rustup component add clippy`.
[docs/GUIDE.md][guide] is the full guide; §8 covers both test verbs.

## How it works

An app is a Rust crate that links charpente, and the two compile into one binary that serves
HTTP and keeps its data in one directory. Tables, routes, jobs and events are Rust types, so a
change to a table that breaks a route is a compile error. A route runs as one transaction: its
writes, the jobs it enqueues and the events it emits commit together, as one record in one log,
or not at all, and it can't await the network. A job that calls the outside world, like `Charge`,
makes its request after the commit, under an idempotency key charpente mints. Table state, the
job queue, realtime feeds and followers are all built from that log. The core does no IO of its
own: disk, network, clock and randomness come through traits with a production implementation
and a simulated one. The simulated ones run from one seed, so a run's crashes, torn writes, lost
replies and clock jumps happen the same way every time it replays. [DESIGN.md][design] has
the details.

## Limits

charpente is a technical preview. [CHANGELOG.md][changelog] dates each release once its crates
are on crates.io. These are the limits you'll hit first:

- It runs on one primary. Writes go through one ordered log on one machine, and followers replay
  it for reads and failover. There is no sharding, and there won't be ([DESIGN.md][design]
  §5). One machine's ceiling is its disk's durable write rate; [docs/BENCH.md][bench] has
  what has been measured, on a dev laptop only.
- Apps are written in Rust, and there is no SQL interface: queries are typed Rust calls. charpente
  can generate a TypeScript client for the front end ([docs/GUIDE.md][guide] §14).
- The simulator drives your app's kernel, not its socket. The HTTP server (`charpente::serve`,
  hyper over TCP) never runs in it. charpente's own behaviour on the wire, such as a `kill -9` or
  a dropped connection, is tested by a separate rig that runs the real binary, with no seed to
  replay ([docs/GUIDE.md][guide] §8).
- The simulator finds only what your checks check. The demo's check counts the charges per order.
- A `#[route]` answers `200` on success and can't read a request header yet, so the demo uses
  the client's order id where a real API might take an `Idempotency-Key` header. Both are rows
  in [PLAN.md][plan]'s after-launch table.
- There is no blob store and no WebSocket transport, and outbound calls have no `HTTP/2`,
  proxies, request signing or streaming replies.
- It's for new backends. Porting an existing app is not a goal ([MANIFESTO.md][manifesto] §8).

## Docs

| Doc | What it is |
|---|---|
| [MANIFESTO.md][manifesto] | Why this exists — the agent-era argument |
| [DESIGN.md][design] | Architecture: the deterministic core, write model, typed surface, simulation |
| [PLAN.md][plan] | Milestones (M0–M3), what waits until after launch, risks, non-goals |
| [docs/GUIDE.md][guide] | How to build an app — agent-first, every runnable example compile-gated |
| [docs/BENCH.md][bench] | Measured numbers, method first — dev hardware |
| [examples/invoicing][invoicing] | The M0 demo: agent-built against the docs alone; DEMO.md + FINDINGS.md |

[double-charge]: https://github.com/NMitchem/charpente/tree/main/examples/double-charge
[demo.rs]: https://github.com/NMitchem/charpente/blob/main/crates/charpente-cli/tests/demo.rs
[guide]: https://github.com/NMitchem/charpente/blob/main/docs/GUIDE.md
[design]: https://github.com/NMitchem/charpente/blob/main/DESIGN.md
[changelog]: https://github.com/NMitchem/charpente/blob/main/CHANGELOG.md
[bench]: https://github.com/NMitchem/charpente/blob/main/docs/BENCH.md
[plan]: https://github.com/NMitchem/charpente/blob/main/PLAN.md
[manifesto]: https://github.com/NMitchem/charpente/blob/main/MANIFESTO.md
[invoicing]: https://github.com/NMitchem/charpente/tree/main/examples/invoicing
