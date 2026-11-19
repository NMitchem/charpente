# renewly: product brief for a docs-only build (requirements, never implementation)

You are building a small backend on the charpente platform. Use only what charpente ships.

You have a fresh Linux machine for this, and it has no network access. Run every command there with `./box '<command>'` from this directory. The command runs in that machine's shell, in a working directory that persists between calls. The machine's cargo already points at a registry holding charpente 0.5.0 and its dependencies.

Your sources are these, and nothing else:
- this brief;
- `README.md` beside it;
- the documentation that ships inside the `charpente` crate: its `GUIDE.md`, its `examples/`, and the files under its `tests/` that GUIDE names as worked examples. This machine's cargo reads crates from a directory registry, so you will find them under `/opt/registry/charpente-0.5.0/`.

Do not read the platform's implementation. That rules out the `src/` of any charpente crate, every other file under `tests/`, and anything online. If the documentation does not tell you how to do something, record that in `FINDINGS.md` and choose the simplest thing it does support. Never patch the framework.

## Setup

Install the CLI with `cargo install charpente-cli`. Then run `charpente new renewly`, and run `git init` inside `renewly/`, because the scaffold does neither. Every commit touches `renewly/` only.

## Domain

- **Workspaces and plans.** Each workspace is a tenant. A workspace's owner creates plans for it; a plan has a name, a price in cents and a period of a whole number of days. A workspace can see only its own plans and customers.
- **Customers and subscriptions.** A workspace registers customers. Each customer has a name and an email address. A customer subscribes to a plan, and the subscription records when its current period started and when it ends.
- **Subscription states:**
  - **active**: paid for the current period;
  - **renewing**: the period has ended, and the renewal charge has been sent and not yet settled;
  - **past due**: the renewal charge was given up, as described below;
  - **cancelled**: the customer cancelled, and the subscription ends at the end of its current period with no further charge.
- **Renewal.** When a subscription's period ends, the workspace charges the customer the plan's price for the next period. The charge goes through an outside payment provider at `billing.example.net`, which deduplicates repeated requests that carry the same `Idempotency-Key` header. Once the charge settles, the subscription is active again for a new period that starts where the old one ended.
- **Cancellation.** A customer may cancel at any time before their period ends. They keep access until the end of the period, and they are never charged for the next one.
- **Giving up a renewal.** The provider can be unreachable for hours at a time, which is longer than any retry should wait. When a renewal charge is given up, the subscription becomes past due. The customer must also be told: they get a notice saying which plan's renewal failed and when. A customer can read their own notices.
- **Recovering a past-due subscription.** An owner can ask for a past-due subscription to be charged again. That retry charges the customer for the same period, at most once.
- **Reads.** A customer can read their own subscription and its state. An owner can list the workspace's subscriptions with their states and period ends.

## Behaviours

- An acknowledged subscription, cancellation or retry request is never lost, across crashes and restarts.
- A renewal is never charged before its period ends.
- A period is charged at most once, even with retries, crashes, restarts, a given-up charge and an owner's retry. Charging twice for one period is the worst bug this service can have.
- A cancelled subscription is never charged again.
- Every renewal that is given up leaves the subscription past due and gives its customer exactly one notice.
- No workspace ever sees or changes another workspace's plans, customers, subscriptions or notices.
- If a transition is not allowed by the states above, it is refused by name, and the refusal lists the transitions the subscription's current state does allow. Examples: cancelling a cancelled subscription, or retrying an active one.

## Proof (the part that matters)

Write `tests/sim.rs` using the platform's simulation harness. Its properties must prove the behaviours above under the full fault matrix, including a provider that is unreachable for longer than your retries wait. Run `charpente test --sim`. It must be green; record what it prints.

Then run `charpente test --mutants` (GUIDE §8) over the files that carry renewals and give-ups, and record its full output in `FINDINGS.md`. The target is every mutant caught or unviable. A MISSED mutant is a one-sided property, and you must fix it before you are done.

## Deliverables

- `renewly/` as its own git repository, building with `cargo test` and `cargo clippy --all-targets -- -D warnings`.
- `FINDINGS.md` in the repository: a friction ledger in your own words, dated as you go. It records:
  - how many times you restarted an approach;
  - every GUIDE section you had to re-read, and why;
  - what you could not find in the docs;
  - what the sim's output told you, and whether it was ever unclear;
  - the `--mutants` output.
- Commit as you go, in small commits with named paths.

When you're done, say what you built, what you saw, and where the documentation failed you.
