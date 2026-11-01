# holdqueue: product brief for a docs-only build (requirements, never implementation)

You are building a small backend on the charpente platform, from what charpente ships and nothing else.

You have a fresh Linux machine for this, with no network access. Run every command there with `./box '<command>'` from this directory. The command runs in that machine's shell, in a working directory that persists between calls. The machine's cargo is already pointed at a registry that holds charpente 0.4.0 and its dependencies.

Your sources are the following, and nothing else:
- this brief;
- `README.md` beside it;
- the documentation that ships inside the `charpente` crate: its `GUIDE.md`, its `examples/`, and the files under its `tests/` that GUIDE names as worked examples. This machine's cargo reads crates from a directory registry, so they are under `/opt/registry/charpente-0.4.0/`.

Do not read the platform's implementation. That means no `src/` of any charpente crate, no other file under `tests/`, and nothing online. If the documentation does not tell you how to do something, record that in `FINDINGS.md` and choose the simplest thing it does support. Never patch the framework.

## Setup

Install the CLI with `cargo install charpente-cli`. Then run `charpente new holdqueue` and `git init` inside `holdqueue/`, since the scaffold does neither. Every commit touches `holdqueue/` only.

## Domain

- An operator registers branches and titles, and records how many copies of a title each branch holds.
- A patron places a hold on a title, naming the branch where they will pick it up. A hold gives the patron id, the title id and the pickup branch.
- A hold moves through these states:
  - **requested**: placed, waiting for a copy;
  - **ready for pickup**: a copy at the pickup branch is set aside for it;
  - **picked up**: the patron collected the copy;
  - **expired**: the hold was ready for 7 days and nobody collected it;
  - **cancelled**: the patron withdrew it before picking it up.
- When a copy of a title becomes free at a branch (it is returned there, or a ready hold on it expires or is cancelled), it goes to the earliest-placed requested hold for that title at that branch, if there is one.
- When a hold becomes ready, the patron is told by an outside messaging service, at `notify.example.org`, with a message naming the title and the branch.
- A patron can read their own holds and the state of each one. An operator can list the holds waiting on a title at a branch, in the order they will be served.

## Behaviours

- An acknowledged hold is never lost, across crashes and restarts.
- Copies go to holds in the order the holds were placed. A copy never goes to a hold while an earlier-placed requested hold for the same title at the same branch is waiting.
- A copy is set aside for at most one hold at a time, and a branch never sets aside more copies of a title than it holds.
- A hold becomes ready at most once, and every hold that becomes ready gets its patron told.
- A transition the states above do not allow (picking up a hold that is not ready, cancelling one that was picked up) is refused by name, and the refusal says which transitions are allowed from the hold's state.

## Proof (the part that matters)

Write `tests/sim.rs` using the platform's simulation harness, with properties that prove the behaviours above under the full fault matrix. Run `charpente test --sim`. It must be green; record what it prints.

Then run `charpente test --mutants` (GUIDE §8) over the files that carry the queue, and record its full output in `FINDINGS.md`. The target is every mutant caught or unviable. A MISSED mutant is a one-sided property, and you must fix it before you are done.

## Deliverables

- `holdqueue/` as its own git repository, building with `cargo test` and `cargo clippy --all-targets -- -D warnings`.
- `FINDINGS.md` in the repository, a friction ledger in your own words, dated as you go. It records:
  - how many times you restarted an approach;
  - every GUIDE section you had to re-read, and why;
  - what you could not find in the docs;
  - what the sim's output told you, and whether it was ever unclear;
  - the `--mutants` output.
- Commit as you go: small commits, with named paths.

When you're done, say what you built, what you saw, and where the documentation failed you.
