# Publish path and `charpente new` — design

**Date:** 2026-09-13. **Status:** approved in chat (Noah), spec committed for the plan.
**Predecessor:** the same day's roadmap ruling (`c840b41`; docs/LEDGER.md "complete
defined"), which closed M2 and defined M3 — Launch. This is M3's first sub-project, and
the one every later one stands on: sub-project 3's third docs-only build and sub-project
5's launch run both start from `cargo install charpente-cli` and `charpente new`.

## Context

M3's goal is that a stranger reproduces the wager in one sitting. Today they cannot start:
the six crates sit at `0.0.0` behind name reservations (five reserved 2026-07-01, confirmed
by Noah 2026-09-13; `charpente-tsgen` is not reserved and is `publish = false`), GUIDE §1's
`charpente = "0.0.0"` dependency line resolves to a reservation stub, and there is no
scaffold — the shortest path to a first app is copying `crates/sim-fixture` and hand-editing
its workspace-shaped `Cargo.toml`.

Two of MANIFESTO §6's oracles do not reach a stranger's app at all. Hard rule 1's clippy
fence is a workspace file (`clippy.toml`) that in-tree examples inherit through
`[lints] workspace = true`; an app outside the workspace gets nothing, so "no ambient IO
in a handler" is a review rule for adopters and a check for us. And the DEMO.md outbox-bug
demonstration is scripted against `examples/invoicing`, so a stranger has to build invoicing
to see the sim catch anything.

One inconsistency surfaced while designing this and is fixed here rather than discovered
by the first outsider: GUIDE §1 says "one dependency is all a charpente app needs", while
§5's route bodies write `use serde::Deserialize` and `#[derive(Wire, Deserialize)]`. That
compiles in `crates/guide-examples` only because that crate depends on `serde` directly;
`charpente` re-exports serde (`pub use serde;`, `lib.rs:19`), but serde's derive emits
`::serde::` paths and needs the crate in the extern prelude. A one-dependency app cannot
write the §5 idiom.

## §1 Decisions already made (Noah, 2026-09-13)

1. **Launch path is crates.io plus a scaffold**, not clone-and-go. Agents `cargo add` things.
2. **The scaffold is the thesis demo**: table, route, job, event, `launch` main, honest sim
   properties — green under `--sim` out of the box, and the shape the outbox-bug demo mutates.
3. **The scaffold emits the fence**: `clippy.toml` plus `[lints.clippy]` deny lines.
4. **Release is a local script with a CI dry-run gate**, not a tag-triggered workflow and
   not bare `cargo publish` by hand.

## §2 Versions and metadata

- `[workspace.package] version = "0.1.0"`. Both `[workspace.dependencies]` entries and
  `charpente-cli`'s `charpente-tsgen` dependency carry `version = "0.1.0"`.
- `charpente-tsgen` drops `publish = false`. It is a normal dependency of `charpente-cli`,
  so `charpente-cli` cannot publish without it.
- `repository = "https://github.com/NMitchem/charpente"` joins `[workspace.package]`; the six
  publishable crates inherit it with `repository.workspace = true`.
- `readme = "../../README.md"` on `charpente` and `charpente-cli` — the two crates.io pages a
  stranger lands on. Cargo copies a readme from outside the package root into the `.crate`
  as `README.md`; §3's `cargo package` step is what proves that here, not this sentence.
  The other four keep their one-line `description`.
- `sim-fixture`, `ts-fixture-app`, `guide-examples`, `edge-harness`, `bench-harness` and
  `examples/invoicing` stay `publish = false`.

Publish order is a fact of the dependency graph, not a script: `charpente-core` →
`charpente-macros` → `charpente-sim` → `charpente` → `charpente-tsgen` → `charpente-cli`. The
three inter-crate dev-dependencies (`core` ← `sim`, `macros` ← `charpente`, `charpente` ←
`sim`) are path-only with no `version`, which `cargo publish` strips; no cycle reaches the
registry.

`crates/charpente/src/launch.rs:508` has `include_str!("../../../docs/GUIDE.md")` inside a
`#[test]`. `cargo package`'s verify build compiles the library, not its tests, so packaging
passes; a user running `cargo test` on the downloaded `.crate` would fail that one test.
Accepted and noted — it is the doc-truth guard doing its job in-tree.

## §3 Publish mechanics

Cargo 1.95 (the toolchain here; `cargo publish --workspace` stabilised in 1.90) publishes a
workspace in dependency order and handles the unpublished-sibling problem with a local
overlay. So:

- **`scripts/publish.sh`**: refuses a dirty tree (`git status --porcelain` non-empty) and
  prints the version it is about to publish. Default action is
  `cargo publish --workspace --dry-run` — the full verify build of every packaged crate.
  `--for-real` runs `cargo publish --workspace`. Nothing else: no tagging, no changelog.
- **CI** (`.github/workflows/ci.yml`, the `check` job): one step,
  `cargo package --workspace --no-verify`. Seconds, and it catches the realistic failures —
  a `publish = false` somewhere in the graph, a missing `description`, a path-only normal
  dependency, a version mismatch between the path and the requirement. The verify build
  stays in the local dry-run: it is a sixth rebuild of the workspace and the minutes are
  metered. **CI's gate is therefore weaker than "publishable"** — it proves the manifests,
  not the packaged sources; the script's dry-run proves both, and the plan runs it once.

## §4 The verb: `charpente new <name>`

`crates/charpente-cli/src/new.rs`, dispatched from `main.rs` like the other verbs.

Writes `<name>/` in the current directory with exactly six files:

| File | Source |
|---|---|
| `Cargo.toml` | rendered from `templates/Cargo.toml.tmpl` |
| `clippy.toml` | `templates/clippy.toml`, verbatim |
| `.gitignore` | `templates/gitignore`, verbatim (`/target`, `/data`) |
| `src/lib.rs` | `templates/lib.rs`, verbatim |
| `src/main.rs` | rendered from `templates/main.rs.tmpl` |
| `tests/sim.rs` | rendered from `templates/sim.rs.tmpl` |

Placeholders are `{{name}}` (the package name, as given) and `{{ident}}` (the crate
identifier: `-` → `_`). Rendering is `str::replace`; the unit test below asserts no `{{`
survives. `{{version}}` in `Cargo.toml.tmpl` is `CARGO_PKG_VERSION` truncated to
`major.minor` at build time (`"0.1"`), so the requirement a stranger gets is `^0.1` and the
number lives in one place, the workspace manifest.

The verb never runs `git init`, never runs cargo, never prompts.

**Refusals** (hard rule 5: name the contract, the site, the fix), all exit 2:

- No name: the usage line — `charpente new <name>` — and one sentence on what it writes.
- A name cargo would reject. The check is cargo's own rule, `[a-zA-Z][a-zA-Z0-9_-]*`, plus
  the Rust strict keywords (the ident lands in `use <ident>::…` in two files). Message
  names the rule and an example: `` `123app` is not a crate name — cargo needs
  `[a-zA-Z][a-zA-Z0-9_-]*`; write `charpente new my-app` ``.
- The target path exists (file or directory, empty or not): `` `<path>` already exists —
  `charpente new` never writes into an existing directory; pick another name or remove it ``.
- A write fails partway: report the `io::Error` and the path; the partial directory is left
  for inspection, not removed.

Exit 0 on success, printing the six paths and the two commands that come next
(`cargo clippy`, `charpente test --sim`).

## §5 The template

**`Cargo.toml.tmpl`** — this rendered for `myapp` is, byte for byte, GUIDE §1's ```toml
block (§7 gates that):

```toml
[package]
name = "{{name}}"
version = "0.1.0"
edition = "2024"

[dependencies]
charpente = "{{version}}"
serde = { version = "1", features = ["derive"] }

[dev-dependencies]
charpente-sim = "{{version}}"

[[test]]
name = "sim"
harness = false

[lints.clippy]
disallowed_methods = "deny"
disallowed_types = "deny"
```

Two dependencies, and GUIDE §1 says so — "`charpente`, and `serde` for the request and
response types you define" — replacing the one-dependency sentence. The alternative,
`#[serde(crate = "charpente::serde")]` on every wire type, is boilerplate on the surface an
agent writes most; and changing `#[derive(Wire)]` to absorb serde is a macro change, out of
scope here (§8).

**`clippy.toml`** — the std-library entries of the workspace file only: `SystemTime::now`,
`Instant::now`, `thread::sleep/spawn/scope`, the `std::fs::*` functions, `TcpStream::connect`,
`TcpListener::bind`, `UdpSocket::bind` as methods; `HashMap`, `HashSet`, `File`,
`OpenOptions`, `TcpStream`, `TcpListener`, `UdpSocket`, `SystemTime`, `thread::Builder` as
types. Reasons say "use the Clock/Disk/Net trait (DESIGN §1)" as they do in-tree. Nothing
about redb, tokio, rsa or the panic hook — those fence the platform's internals, which an
app never names. Each entry is copied, not paraphrased, so the plan can diff the two files
and show the app list is a subset.

**`lib.rs`** — one of each, and no more:

- `#[table] Note { id: Id<Note>, body: String, echoed: bool }`.
- `#[derive(Wire, Deserialize)] NewNote { body: String }` — the POST body.
- `#[event] NoteAdded { note: Id<Note> }`.
- `#[job] Echo { note: Id<Note> }` whose `run` sets `echoed = true` on the row — an
  in-row effect, so a property can pin it and `--mutants` has something to catch.
- `#[route(POST "/notes")] fn add_note(tx, body: NewNote) -> Result<Note, ApiError>`:
  `tx.put`, `tx.enqueue(Echo)`, `tx.emit(NoteAdded)`, return the row. The three effects in
  one transaction is the second guarantee in the README, in the stranger's own file.
- `pub fn build_app(...)` in the shape `charpente::launch` takes (GUIDE §13).

Route auth is whatever `#[route]` requires today; sub-project 2 (default-secure surfaces)
will change that, and the template changes with it — that is the point of having the
scaffold gated rather than hand-maintained.

**`main.rs.tmpl`** — `charpente::launch("0.0.0.0:8080", "./data", {{ident}}::build_app)`.

**`sim.rs.tmpl`** — `charpente_sim::sim_tests!` with two properties, both in the give-up
idiom `crates/sim-fixture/tests/sim.rs` uses (`is_fault_fallout` → `sim.give_up`, and the
`durable_corruption_fired` excuse for an acked row):

1. `acked_note_survives_a_restart` (`require ["restart"]`): put through `db.transact`,
   `sim.restart()`, reopen, read back.
2. `acked_post_implies_the_echo_after_recovery`: drive `POST /notes` through the app,
   restart, dispatch due jobs, read the row: `echoed` must be `true`. This is the thesis in
   one property, and it is DEMO.md's own oracle shape (`receipts_exactly_once`): acked ⇒ an
   `Echo` job row is durable; `echoed` ≡ that job is Done. It goes red when `Echo::run` is
   neutered (`charpente test --mutants`), when the `tx.enqueue` line is deleted, and — under
   the framework patch DEMO.md describes (`c0dbcea`), which is the only way to write it —
   when the enqueue is split into a second transaction. So the launch run mutates the
   scaffold, not invoicing; whether it re-applies that patch is sub-project 5's call.

Nothing one-sided ships. The `counter_bump_job_leaves_the_row_readable` demonstration stays
in `sim-fixture`, where GUIDE's mutants walkthrough points.

## §6 What is deliberately not a workspace member

The template is not rendered into an in-tree crate. A workspace member would need
`charpente.workspace = true` (a path dep) where the stranger needs `charpente = "0.1"` (a
registry dep), and `[lints] workspace = true` where the stranger needs the inline deny
lines — so the member's `Cargo.toml` could never be the file the verb writes, and a
transformation between the two is the drift this sub-project exists to prevent. The
template's Rust is compiled by the end-to-end oracle in §7 on every push instead.

## §7 Oracles

`crates/charpente-cli/tests/new.rs`, in the default suite (`cli.rs` already spawns cargo
there; nothing here is `#[ignore]`d). CLAUDE.md's `charpente-cli` test list gains `new`;
`guide-examples/tests/doc_inventory.rs` holds that.

**Unit:**

- Each refusal in §4, asserted on stderr text and exit code 2.
- Success writes exactly the six paths, and no rendered file contains `{{`.
- The rendered `Cargo.toml` for `myapp` byte-equals the first ```toml block after
  `## 1. Project setup` in `docs/GUIDE.md`, read at test time (the `describe_contract.rs`
  pattern: the doc is the spec, and the test diffs the code against it).
- The rendered `clippy.toml` is a subset of the workspace `clippy.toml`, entry by entry.

**End to end**, one test:

1. `charpente new e2e` into a temp dir.
2. Append to its `Cargo.toml`:
   `[patch.crates-io] charpente = { path = "<workspace>/crates/charpente" }` and the same for
   `charpente-sim`. This is the Cargo book's own mechanism for building against an
   unpublished version; the requirement `^0.1` is satisfied by the path crate's `0.1.0`
   whether or not the registry has it yet. The only delta from a stranger's build is the
   registry itself, which §3's dry-run covers.
3. With `CARGO_TARGET_DIR` set to the workspace's target directory (so the dependency
   graph is warm and the test costs one small crate's compile, not hyper's), run in that
   dir: `cargo fmt --check`; `cargo clippy --all-targets -- -D warnings` — which proves the
   emitted fence is live against the template's own code; and
   `charpente test --sim --universes 64 --dir <tmp>` — exit 0. Both scaffold properties
   `require ["restart"]`, and ~55% of universes give up before a restart occurs, so at 8
   universes the requirement goes unmet on ~2.5% of fresh master seeds (replay:
   `--universes 8 --master-seed 0x7ad351d01d2ca664` exits 1, `--universes 64` exits 0); 64
   puts the miss at ~1e-17.

Every `Command` here is `charpente-cli`'s to spawn; the deny-list's `Command::new` entry
names this crate as the sanctioned site.

## §8 Docs, in the same commits (hard rule 6)

- **GUIDE §1** rewritten around the verb: `cargo install charpente-cli` (the crate is
  `charpente-cli`, the binary is `charpente` — the `sqlx-cli`/`wasm-pack` convention),
  `charpente new`, the six files and what each is for, the fence and what `cargo clippy`
  refuses by name, the two-dependency count, and the `Cargo.toml` block the test pins. The
  data-directory paragraph and `ProdDisk` block stay.
- **README** gains a short "Try it" block above the doc table: install, `new`, `clippy`,
  `test --sim`. Sub-project 5 writes the launch post; this is the four lines it starts from.
- **PLAN.md M3 sub-project 1**: the reservation is confirmed (five names), `charpente-tsgen`
  publishes fresh, and `cargo install charpente` in sub-project 5's text becomes
  `cargo install charpente-cli`.
- **DESIGN §7**'s verb inventory gains `new`.
- **LEDGER** entry at close, with the serde finding and the first real dry-run's output.

## §9 Refused, so the plan does not grow them

No `--template`/`--minimal` flag. No interactive prompts. No `git init`. No `cargo build`
after scaffolding. No release workflow in CI. No `Wire`-derive change to absorb serde. No
`charpente init` for an existing directory. No crates.io token anywhere in the tree.

## §10 Exit criteria

1. `scripts/publish.sh` (dry-run) exits 0 on `main` with its full verify build, output
   recorded in the ledger; CI's `cargo package --workspace --no-verify` step is green.
2. `charpente new myapp && cd myapp && cargo clippy --all-targets -- -D warnings &&
   charpente test --sim` is green from a shell that has only the installed binary and
   crates.io — the e2e test proves it against the workspace; the ledger records one
   post-publish run against the registry.
3. The four refusals print the text in §4; `tests/new.rs` pins each.
4. GUIDE §1's `Cargo.toml` block and the verb's output are byte-identical, held by a test.
5. Under the template, `charpente test --mutants` reports `Echo::run` → `Ok(())` **caught**
   by `acked_post_implies_the_echo_after_recovery`; and deleting the `tx.enqueue` line turns
   that property red under `--sim`, with a seed. Both recorded — they are what the launch
   run will show. (The dual-write proper needs DEMO.md's framework patch; sub-project 5.)
