# Pre-publish follow-ups Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the four items the payload-shapes close-out left on "Noah's list" that the
north-star documents already decide, plus run the six guard-deletion hand mutations that
were never attempted, so that the only remaining pre-publish platform work is C-2.

**Architecture:** Four independent fixes: a sim-harness defect (N-1), a macro change that
makes the structural fingerprint independent of the compiler's token printer, a dispatch
guard that becomes an asserted invariant, and a CI line for two `#[ignore]`d process-level
guards. Then a controller-run mutation pass and one LEDGER entry.

**Tech Stack:** Rust workspace (charpente, charpente-core, charpente-macros, charpente-sim,
edge-harness), GitHub Actions.

**Spec:** No new spec — every item is already specified and ruled:
- N-1: `.superpowers/sdd/2026-09-24-payload-shapes/n1-diagnosis.md` §5, Ruling R33, and
  `docs/LEDGER.md` 2026-09-25 "payload shapes" follow-up (1).
- Fingerprint: `.superpowers/sdd/2026-09-24-payload-shapes/final-review.md` Recommendation 2;
  DESIGN.md:651-656 ("a hand-rolled, fixed 64-bit FNV-1a"); GUIDE §12 "What the fingerprint
  cannot see".
- Dispatch guard: Ruling R35; CLAUDE.md ("a third survivor is a property to write, not an
  entry to add"; "in core code, prefer asserting an invariant over handling an 'impossible'
  state"); `docs/superpowers/specs/2026-09-12-wal-one-sided-properties-design.md` §6.
- CI line: final-review.md Recommendation 3; the precedent in `.github/workflows/ci.yml`'s
  passthrough-guard block.

## Global Constraints

- Commit directly to `main`. **No `Co-Authored-By` trailers** (CLAUDE.md). `git add` named
  paths only — never `-A` or `.`; another session may share this tree.
- Before committing, run `cargo fmt --all --check` and
  `cargo clippy --workspace --all-targets -- -D warnings`, plus the named suites for your task.
- The shell is zsh. `${PIPESTATUS[0]}` is empty in zsh; use `${pipestatus[1]}` or
  `set -o pipefail`. Pipe cargo through `tee`, never `tail`. Confirm green by COUNTING
  `^test result: ok` lines, never from the absence of `FAILED`.
- Do NOT background `cargo test` (`run_in_background`, `&`, `nohup`). Run it in the
  foreground with an explicit timeout of up to 600000 ms, and partition by package or test
  file so each run fits. `cargo test --workspace` does not fit one call; the controller runs
  that gate.
- There is no `timeout`/`gtimeout` on this machine.
- `$SCRATCH` is the scratch directory the controller names in your dispatch. Write every
  log there, never into the repo.
- Every sim seed you find gets pinned, and the pin asserts the REASON, not only the outcome:
  a pinned seed names a universe, and a later re-roll can land a different give-up on it.
- Hard rule 5: tests assert error text. Hard rule 6: a public-contract change updates
  GUIDE/DESIGN in the same commit.
- Subagents must NOT delete, disable or weaken a product guard, even temporarily. Those
  mutations are Task 5's, and the controller runs them in an isolated worktree.
- Comments only for constraints the code cannot express. No task, round or finding
  references in committed code or rustdoc.

## Review Focus

1. **The give-up-doesn't-stop class beyond the two known sites.** Every workload that calls
   `absorb` or `give_up` inside an inner loop must stop that universe once `sim.gave_up()`
   is `Some`, including the replicated realtime workload (`realtime.rs` ~3935/3953).
   Task 1 Step 4 sweeps it.
2. **A type spelled with multi-character punctuation or nested groups.** `::`, `>>`, `&'a`,
   `[T; N]` and tuples must render deterministically from the tokens alone. Task 2's golden
   test covers `::`, `>>`, a tuple and an array.
3. **A handler's own IO error must still bubble as a platform fault, never trip the new
   assert.** Task 3 Step 3 names the existing test that pins this.
4. **Floors after N-1.** The scoped sibling's exact floors may drop when universes now stop
   at their first give-up. Re-measure only a floor that fails, and record the change as a
   table row (Task 1 Step 6). Never lower a floor that still passes.
5. **A CI runner whose port 8080 is taken.** The launch test already names that cause rather
   than passing or failing by coincidence. Task 4 must not weaken this.

---

### Task 1: N-1 — a realtime give-up stops its universe

**Model:** opus (judgment: floor re-measurement, a class sweep).

**Files:**
- Modify: `crates/charpente/tests/realtime.rs`: `realtime_workload`'s round loop (~2103-2150),
  `scoped_realtime_workload`'s round loop (~4916-4955), the give-up texts at ~1769, ~2265 and
  ~5043, and the scoped floors and their doc table (~5100-5200).

**Interfaces:** none consumed or produced; test-only.

- [ ] **Step 1: Reproduce at HEAD.**

```zsh
CHARPENTE_SEED=0xb247eceae0daf689 cargo test -p charpente --test realtime -- reconnecting_client_misses_nothing_under_the_fault_matrix --exact --nocapture 2>&1 | tee $SCRATCH/n1-repro.txt
```

Expected: FAILED, with `content mismatch at event id 3: expected (ping, {"n":"712"}) but received (ping, {"n":"430"})`.
If it does NOT reproduce at HEAD, later commits have renamed the universe. Re-find it by
running the flagship at the base where it first appeared, which prints the failing seed's
repro line, then use that seed everywhere below:

```zsh
CHARPENTE_BASE_SEED=0x9C4177E3 cargo test -p charpente --test realtime -- reconnecting_client_misses_nothing_under_the_fault_matrix --exact --nocapture 2>&1 | tee $SCRATCH/n1-base.txt
```

If neither reproduces, STOP and report: the fix must not land without a red first (hard rule 3).

- [ ] **Step 2: Write the pin, and watch it fail.** Add it directly after
`a_universe_that_never_booted_a_node_gives_up_rather_than_reporting_a_miss`, reconstructing
the universe exactly as that test does:

```rust
/// Seed `0xb247eceae0daf689` (base `0x9C4177E3`). A durable, acked event is
/// destroyed by `bit_flip` above the store's checkpoint horizon, recovery cuts
/// there, and the next emit re-mints its id with other content. The drain
/// probe classifies that correctly as a give-up in the first round after the
/// restart; the universe must END there. Carrying on let retention collect the
/// diverged row, after which the probe could no longer see the divergence and
/// the stale frame came back as a hard failure.
#[test]
fn a_diverged_ledger_gives_up_at_first_detection_and_stops() {
    const SEED: u64 = 0xb247_ecea_e0da_f689;
    let mut state = SEED;
    let cfg_seed = charpente_sim::rng::splitmix64(&mut state);
    let mut cfg = FaultConfig::swarm(&mut charpente_sim::rng::Rng::new(cfg_seed));
    charpente_sim::sample_extended_fields(SEED, &mut cfg);
    let mut sim = Sim::with_faults(SEED, cfg);

    let result = realtime_workload(&mut sim);
    assert!(
        result.is_ok(),
        "a universe whose probe found a diverged ledger must give up, not fail: {:?}",
        result.err()
    );
    let reason = sim.gave_up().expect("the divergence must be RECORDED as a give-up");
    assert!(
        reason.contains("ledger diverged at event id 3"),
        "and it must be THIS give-up, the divergence at id 3: {reason}"
    );
}
```

If Step 1 re-found a different seed, use that seed and its diverged id, and the base it came
from, in the literal, the doc and the assertion.

```zsh
cargo test -p charpente --test realtime -- a_diverged_ledger_gives_up_at_first_detection_and_stops --exact --nocapture 2>&1 | tee $SCRATCH/n1-pin-red.txt
```

Expected: FAILED on the first assert, with `content mismatch at event id 3` inside the `Err`.

- [ ] **Step 3: Fix both round loops.** In `realtime_workload`, directly after
`client.absorb(sim, kernel.app().db(), &new_responses, subscribe, &sinks)?;` inside
`for _round in 0..ROUNDS_PER_EPOCH`, add:

```rust
            if sim.gave_up().is_some() {
                return Ok(client.trace);
            }
```

Add the identical three lines after the matching `client.absorb(...)?;` in
`scoped_realtime_workload`'s round loop. Both functions already return `Ok(client.trace)`
from their epoch-top `gave_up` checks, so the shape is unchanged.

- [ ] **Step 4: Sweep the class.** List every `absorb(` and `give_up(` call site:
`grep -n "absorb(\|give_up(" crates/charpente/tests/realtime.rs`. For each one inside a
loop, confirm that the enclosing workload stops the universe on the next iteration, or
immediately, once `sim.gave_up()` is `Some`. That covers at least the replicated workload
around ~3935/3953 and the settle loops around ~2229/5013. Where it does not, add the same
check at the same place. Write the site list, with the verdict for each, into your report.
Then run the same grep over `crates/charpente/tests/*.rs` and `crates/charpente-sim/tests/*.rs`
for `give_up(` inside `for`/`loop`/`while` bodies, and report any site where the loop
continues after a give-up. Report those sites; do not fix them unless they are in realtime.rs.

- [ ] **Step 5: Correct the give-up justification text.** Three texts, in
`sync_and_drain_or_give_up` (~1769) and the two settle-phase `rolled_back` give-ups (~2265,
~5043), say "a crash discarded a committed-but-not-durable event". On a single node that is
false. `append → sync → apply` means nothing reaches the store unsynced, so a store rolled
back past a ledgered event means a durable-corruption fault cut it above the checkpoint
horizon. On the replicated workload it can also be a failover inside the replication loss
window. Reword each text so it names what this node's timeline lost and why the universe
cannot adjudicate it, with both causes, keeping the leading phrases the tests assert:
`ledger diverged at event id {id}` and `ledger id {id} is behind this node's timeline`.
Suggested form for ~1769:

```rust
"ledger diverged at event id {id}: recorded {recorded} when scanned, store now holds \
 {stored} — this node's timeline no longer holds the recorded event (durable corruption \
 cut it above the checkpoint horizon, or a failover lost it inside the replication loss \
 window) and a replacement re-minted this id, so nothing here adjudicates delivery ({e})"
```

Update the `diverged_ledger_entry` and `sync_and_drain_or_give_up` doc comments where they
say "committed, not durable" to the same effect. Do NOT gate the give-up on
`durable_corruption_fired`. The diagnosis proposed that as an option, but the probe is
shared with the replicated workload, where a failover can re-mint legitimately with no
corruption fault. It is out of scope here.

- [ ] **Step 6: Run the pin green, then the batches, and re-measure only failing floors.**

```zsh
cargo test -p charpente --test realtime -- a_diverged_ledger_gives_up_at_first_detection_and_stops --exact 2>&1 | tee $SCRATCH/n1-pin-green.txt
cargo test -p charpente --test realtime --no-fail-fast 2>&1 | tee $SCRATCH/n1-realtime.txt; echo "exit ${pipestatus[1]}"
```

Then run the flagship and the scoped sibling at the fresh base plus three more fresh bases
(`0x9C4177E3`, `0x51A7_0927`, `0xB0C4_0927`, `0x7E11_0927`):

```zsh
for b in 0x9C4177E3 0x51A70927 0xB0C40927 0x7E110927; do
  CHARPENTE_BASE_SEED=$b cargo test -p charpente --test realtime -- reconnecting_client_misses_nothing_under_the_fault_matrix a_reconnecting_tenant_subscriber_misses_nothing_and_leaks_nothing --exact --nocapture 2>&1 | tee $SCRATCH/n1-fresh-$b.txt; echo "base $b exit ${pipestatus[1]}"
done
```

Fresh bases skip `floor:` and `require:` checks, so they are verdict-only. If any fresh base
fails, STOP and report its repro line. Do not loosen anything.

If `a_reconnecting_tenant_subscriber_misses_nothing_and_leaks_nothing` fails a `floor:` at
its pinned base, re-measure the way the file already does: run it twice, confirm the
coverage counts are identical across both runs, and set only the failing floor constants to
the measured value. Add a `**Re-measured 2026-09-27**` paragraph and table below the existing
2026-09-07 one, in the same format, giving the reason: universes now stop at their first
give-up, and a universe that gave up adjudicated nothing, so its coverage was never evidence
for these rates. Leave passing floors alone.

- [ ] **Step 7: Gate and commit.**

```zsh
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings
git add crates/charpente/tests/realtime.rs
git commit -m "test(realtime): a give-up ends its universe in every round loop — pin 0xb247eceae0daf689, re-measure <list or 'no'> floors, N universes over 4 fresh bases"
```

Replace the placeholders with measured facts. Report: the red and green quotes, the sweep
table, every batch's verdict line and universe count, and the floor measurements.

---

### Task 2: The fingerprint is charpente's own rendering of the type tokens

**Model:** sonnet implement, opus review.

**Files:**
- Modify: `crates/charpente-macros/src/fingerprint.rs` (the whole file is 54 lines)
- Test: `crates/charpente/tests/typed_tables.rs` (new golden test after
  `bare_table_stays_version_one_and_now_fingerprinted`, ~line 691)
- Re-pin: `crates/charpente-cli/tests/describe.rs`, `examples/invoicing/tests/describe_golden.rs`,
  and `docs/GUIDE.md` §13's `describe` JSON (~4260-4400, every `"fingerprint": <n>`)
- Docs: `docs/GUIDE.md` §12 "What the fingerprint cannot see" (~3983), and DESIGN.md:651-656

**Interfaces:**
- Consumes: `charpente::core::migration::fnv1a_hash(&[u8]) -> u64` (public, core).
- Produces: `fingerprint::compute` keeps its exact signature
  `pub(crate) fn compute(field_idents: &[syn::Ident], field_types: &[Type]) -> u64`.

**Why:** `compute` hashes `quote!(#ty).to_string()`. Inside a proc macro that is the
compiler's `TokenStream` `Display`, which the Rust documentation states is subject to change
("there might be changes in the whitespace used between tokens"). Fingerprints are persisted
in `charpente.schema_versions` and beside every emit and enqueue. So a toolchain bump that
changed the printer would drift-refuse every versioned table, reshape every retained event,
and refuse every Pending job, with no code change. Before 0.1.0 the fix costs a one-time
re-pin. After 0.1.0 the fix is itself that breaking reshape.

- [ ] **Step 1: Write the failing golden test** in `typed_tables.rs`, right after
`bare_table_stays_version_one_and_now_fingerprinted`:

```rust
#[charpente::table(global)]
struct Canon {
    id: charpente::core::table::Id<Canon>,
    bytes: Vec<u8>,
    nested: Option<Vec<Vec<u8>>>,
    pair: (u32, String),
    fixed: [u8; 4],
    spelled: std::string::String,
}

/// A fingerprint is a function of a field's type tokens as written, rendered
/// by charpente itself with one space between tokens, and never of the
/// compiler's printer, whose spacing rustc does not keep stable. So the
/// expected value is recomputed here from spelled-out canonical text: a
/// toolchain that printed `Vec<u8>` where another printed `Vec < u8 >`, or
/// `std::string` where another printed `std :: string`, cannot move it.
#[test]
fn a_fingerprint_hashes_charpentes_own_token_rendering_not_the_compilers() {
    use charpente::core::table::Row;
    let canonical = "id\0charpente : : core : : table : : Id < Canon >\0\
                     bytes\0Vec < u8 >\0\
                     nested\0Option < Vec < Vec < u8 > > >\0\
                     pair\0( u32 , String )\0\
                     fixed\0[ u8 ; 4 ]\0\
                     spelled\0std : : string : : String\0";
    assert_eq!(
        Canon::FINGERPRINT,
        Some(charpente::core::migration::fnv1a_hash(canonical.as_bytes())),
        "the fingerprint must be FNV-1a over `name\\0<tokens joined by one space>\\0` per field"
    );
}
```

If `#[charpente::table(global)]` rejects a field type because it has no `Codec` (the tuple or
the array), drop that field and its line from `canonical`, and say so in the report. Keep
`bytes`, `nested` (for `>>`) and `spelled` (for `::`): together they make this test red under
any printer that glues punctuation. If no field type with a `(`…`)` or `[`…`]` group can be
used in a table, add a macros unit test for groups in Step 3 instead.

- [ ] **Step 2: Run it and record the red.**

```zsh
cargo test -p charpente --test typed_tables -- a_fingerprint_hashes_charpentes_own_token_rendering_not_the_compilers --exact 2>&1 | tee $SCRATCH/fp-red.txt
```

Expected: FAILED, with the left (compiled) and right (canonical) values differing. Quote them.

- [ ] **Step 3: Implement** by replacing `compute` and adding `render`. The rest of
`fingerprint.rs` stays as it is:

```rust
use proc_macro2::{Delimiter, TokenStream, TokenTree};
use quote::ToTokens;
use syn::Type;

// … fnv1a unchanged …

/// NUL-separated fold so ("ab","c") cannot collide with ("a","bc"). Each type
/// is rendered by `render`, not by `TokenStream`'s `Display`: that printer's
/// spacing is the compiler's and is documented as subject to change, and
/// these hashes are persisted.
pub(crate) fn compute(field_idents: &[syn::Ident], field_types: &[Type]) -> u64 {
    let mut buf = String::new();
    for (ident, ty) in field_idents.iter().zip(field_types) {
        buf.push_str(&ident.to_string());
        buf.push('\0');
        let mut words = Vec::new();
        render(ty.to_token_stream(), &mut words);
        buf.push_str(&words.join(" "));
        buf.push('\0');
    }
    fnv1a(buf.as_bytes())
}

/// Every token as its own word: an identifier, one punctuation character, a
/// literal's source text, or a group's delimiters around its contents. An
/// invisible (`Delimiter::None`) group contributes only its contents.
fn render(tokens: TokenStream, words: &mut Vec<String>) {
    for tree in tokens {
        match tree {
            TokenTree::Group(g) => {
                let (open, close) = match g.delimiter() {
                    Delimiter::Parenthesis => ("(", ")"),
                    Delimiter::Brace => ("{", "}"),
                    Delimiter::Bracket => ("[", "]"),
                    Delimiter::None => ("", ""),
                };
                if !open.is_empty() {
                    words.push(open.to_string());
                }
                render(g.stream(), words);
                if !close.is_empty() {
                    words.push(close.to_string());
                }
            }
            TokenTree::Ident(i) => words.push(i.to_string()),
            TokenTree::Punct(p) => words.push(p.as_char().to_string()),
            TokenTree::Literal(l) => words.push(l.to_string()),
        }
    }
}
```

Add one unit test to the existing `mod tests`. It runs under proc-macro2's fallback printer,
which is a different printer from the compiler's, and that is the point:

```rust
    #[test]
    fn compute_hashes_the_rendered_tokens_not_a_printer() {
        let idents: Vec<syn::Ident> = vec![parse_quote!(a), parse_quote!(b)];
        let types: Vec<Type> = vec![
            parse_quote!(std::collections::BTreeMap<String, Vec<(u8, [u16; 2])>>),
            parse_quote!(&'static str),
        ];
        let expected = "a\0std : : collections : : BTreeMap < String , Vec < ( u8 , [ u16 ; 2 ] ) > >\0\
                        b\0& ' static str\0";
        assert_eq!(compute(&idents, &types), fnv1a(expected.as_bytes()));
    }
```

- [ ] **Step 4: Run both tests green.**

```zsh
cargo test -p charpente-macros --lib 2>&1 | tee $SCRATCH/fp-unit.txt
cargo test -p charpente --test typed_tables 2>&1 | tee $SCRATCH/fp-green.txt
```

- [ ] **Step 5: Re-pin the describe goldens and GUIDE §13.** Every fingerprint changes once.
Run the golden suites, take each new value from the failure diff, and replace only the
`"fingerprint": <n>` values. `null` values stay `null`, because hand-written `Row` impls are
unaffected.

```zsh
cargo test -p charpente-cli --test describe 2>&1 | tee $SCRATCH/fp-cli-describe.txt
cargo test -p invoicing --test describe_golden 2>&1 | tee $SCRATCH/fp-inv-golden.txt
cargo test -p guide-examples --test describe_contract 2>&1 | tee $SCRATCH/fp-contract.txt
```

Loop until all three are green. `describe_contract` diffs GUIDE §13's JSON against a fresh
invoicing build, so the GUIDE values must equal invoicing's. Then run every other suite that
names fingerprints, and confirm none pins a literal the change moved:

```zsh
cargo test -p charpente-core --no-fail-fast 2>&1 | tee $SCRATCH/fp-core.txt; echo "exit ${pipestatus[1]}"
cargo test -p charpente --no-fail-fast 2>&1 | tee $SCRATCH/fp-charpente.txt; echo "exit ${pipestatus[1]}"
```

The second command can exceed 600 s. If it does, split it by `--test <file>` using the list
in CLAUDE.md, and count `test result: ok` across the parts. A pinned-seed test that goes red
here means persisted bytes moved a universe. Do NOT re-pin it: STOP and report the repro line.

- [ ] **Step 6: Docs, in the same commit (hard rule 6).** In GUIDE §12, "What the fingerprint
cannot see", after "It hashes each field's name and the *tokens* of its type, not the type's
definition.", insert:

```markdown
The tokens are hashed as written, rendered by charpente itself with one space between
tokens: `Vec<u8>` and `std::vec::Vec<u8>` are different shapes to it, so respelling a
field's type is a shape change, and a compiler upgrade never is.
```

In DESIGN.md:651-653, change "a hand-rolled, fixed 64-bit FNV-1a over its
`(field_name, type_name)` sequence" to "a hand-rolled, fixed 64-bit FNV-1a over its
`(field_name, type tokens)` sequence — the tokens rendered by the macro itself, one space
between them, never by the compiler's printer, whose spacing is not stable across
toolchains". Then grep for any other doc sentence that describes how the type is rendered,
and fix it: `grep -n "to_string\|type_name\|tokens of" DESIGN.md docs/GUIDE.md`.

- [ ] **Step 7: Gate and commit.**

```zsh
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo build -p guide-examples
git add crates/charpente-macros/src/fingerprint.rs crates/charpente/tests/typed_tables.rs crates/charpente-cli/tests/describe.rs examples/invoicing/tests/describe_golden.rs docs/GUIDE.md DESIGN.md
git commit -m "fix(macros): the structural fingerprint renders type tokens itself — never the compiler's printer, whose spacing is not stable; every fingerprint re-pinned once, before 0.1.0"
```

---

### Task 3: Dispatch asserts the job-shape invariant instead of guarding it

**Model:** sonnet implement, opus review.

**Files:**
- Modify: `crates/charpente/src/kernel/dispatch.rs:257-272`
- Modify: `crates/charpente-core/src/db.rs:215-225` (the `JobShapeRefusal` doc comment)

**Why:** `Group::stage` has exactly one `TxError::Io` exit, the job-shape refusal
(`db.rs`, the `effects.enqueues` loop). The handler's own read errors arrive as
`TxError::App(JobError::Io)` through `map_err(TxError::App)`, and the write gate surfaces as
`NotPrimary`/`Verifying`. So `if JobShapeRefusal::of(&e).is_some()` at `dispatch.rs:269` is
always true, and cargo-mutants' `→ true` mutant is equivalent and survives. CLAUDE.md
forbids a third exclusion and says to prefer asserting an invariant over handling an
impossible state. The assert keeps the guard's stated purpose, since a future second `Io`
exit stops the process loudly instead of dead-lettering as an attempt, and the survivor no
longer exists.

- [ ] **Step 1: Confirm the survivor exists at HEAD.**

```zsh
cargo mutants --list --file crates/charpente/src/kernel/dispatch.rs 2>/dev/null | grep -n "dispatch.rs:2[5-7][0-9]" | tee $SCRATCH/disp-mutants-before.txt
```

Expected: a line for `dispatch.rs:269` replacing the match guard with `true`. If
`cargo mutants` is missing, say so and skip Steps 1 and 4's listing. Do not install anything.

- [ ] **Step 2: Replace the guarded arm.** Replace the comment block and `let attempt = match
attempt { … };` at ~257-272 with:

```rust
        // `Group::stage`'s only `TxError::Io` exit is its enqueue refusal: this
        // handler's transaction enqueued another shape of a mounted job name.
        // That is the handler's own deterministic bug, like a panic — an
        // attempt, never a platform fault, since no restart could clear it.
        // Asserted, not handled (hard rule 4): a second `Io` exit in `stage`
        // must be classified here on purpose, not dead-lettered by default.
        let attempt = match attempt {
            Err(TxError::Io(e)) => {
                assert!(
                    JobShapeRefusal::of(&e).is_some(),
                    "dispatcher invariant: Group::stage returned an io error that is not its \
                     job-shape refusal — classify it in dispatch before it can reach a job: {e}"
                );
                Err(TxError::App(JobError::App(e.to_string())))
            }
            other => other,
        };
```

In `db.rs`, change `JobShapeRefusal`'s doc comment. Its sentence "so dispatch
(`kernel/dispatch.rs`) can narrow to exactly it — defensively: …" becomes a statement that
dispatch ASSERTS every `Io` error out of `Group::stage` is this refusal. Keep the rest: why it
is a distinct type, and that `Refusal::of` still finds it.

- [ ] **Step 3: Run the behaviour.** The job-shape refusal must still dead-letter as an
attempt, and a handler's genuine platform IO error must still bubble with the row left
Pending:

```zsh
cargo test -p charpente --test kernel --no-fail-fast 2>&1 | tee $SCRATCH/disp-kernel.txt; echo "exit ${pipestatus[1]}"
cargo test -p charpente --lib 2>&1 | tee $SCRATCH/disp-lib.txt
```

Both must be all-green. Name, in the report, the kernel test that pins the attempt
classification and the one that pins IO bubbling (from R34, "a job whose own transaction
fails with a genuine platform IO error must bubble"). Find them with
`grep -n "genuine\|bubble\|JobShapeRefusal" crates/charpente/tests/kernel.rs`.

- [ ] **Step 4: Confirm the mutant is gone.**

```zsh
cargo mutants --list --file crates/charpente/src/kernel/dispatch.rs 2>/dev/null | grep -n "dispatch.rs:2[5-8][0-9]" | tee $SCRATCH/disp-mutants-after.txt
```

Expected: no mutant replaces a match guard in this block. `.cargo/mutants.toml` stays
unchanged, at 2 entries.

- [ ] **Step 5: Gate and commit.**

```zsh
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings
git add crates/charpente/src/kernel/dispatch.rs crates/charpente-core/src/db.rs
git commit -m "fix(dispatch): assert the job-shape invariant instead of guarding it — the proven-equivalent survivor at dispatch.rs:269 no longer exists, no exclusion added"
```

---

### Task 4: CI runs the two process-level launch guards

**Model:** sonnet.

**Files:**
- Modify: `.github/workflows/ci.yml`, directly after the passthrough-guard `- run:` line (~90)

**Why:** The locked-stderr launch hang (a shipped-hang regression) is pinned only by two
`#[ignore]`d tests in `crates/edge-harness/tests/smoke.rs`:
`a_launch_based_binary_whose_kernel_refuses_exits_nonzero_within_a_bound` and
`a_node_whose_kernel_cannot_start_exits_nonzero_with_its_named_line`. `ci.yml` already states
the rule three times: a guard that "only a human remembering a command" enforces is one that
never fires.

- [ ] **Step 1: Measure the marginal cost.** Both tests call `build_bins()`, which runs
`cargo build --release -p invoicing -p charpente-cli --bins` and then
`--example seed_foreign_job`. CI has already built `charpente` in release for the replay
pins. Reproduce that state, then time the new line:

```zsh
cargo test --release -p charpente --test replication --no-run 2>&1 | tee $SCRATCH/ci-warm.txt
/usr/bin/time -p cargo test -p edge-harness --test smoke -- a_launch_based_binary_whose_kernel_refuses_exits_nonzero_within_a_bound a_node_whose_kernel_cannot_start_exits_nonzero_with_its_named_line --exact --ignored 2>&1 | tee $SCRATCH/ci-launch.txt
```

Expected: `running 2 tests`, then `test result: ok. 2 passed`. Record the `real` seconds, and
how many seconds of it were the release build (the first run after a clean
`cargo clean -p invoicing -p charpente-cli` shows the build cost; the second run shows the
test cost). Do NOT run `cargo clean` on the whole workspace.

- [ ] **Step 2: Add the line**, in the house format:

```yaml
      # The launch-exit guards, with --ignored — added 2026-09-27. A launch-based node
      # whose kernel stops must exit nonzero with its named line, not park behind an HTTP
      # edge that answers 500 forever; that hang shipped once. Both tests are `#[ignore]`d
      # because they spawn real processes over real TCP, so `cargo test --workspace` above
      # lists them as "ignored" and runs nothing — the same condition the three blocks above
      # describe. Named --exact so no other ignored edge test rides along. Marginal cost
      # measured locally: <N> s, most of it the release build of the invoicing and
      # charpente-cli bins (charpente itself is already warm from the release replay pins).
      - run: cargo test -p edge-harness --test smoke -- a_launch_based_binary_whose_kernel_refuses_exits_nonzero_within_a_bound a_node_whose_kernel_cannot_start_exits_nonzero_with_its_named_line --exact --ignored
```

Replace `<N>` with Step 1's measurement.

- [ ] **Step 3: Validate the YAML and commit.**

```zsh
python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci.yml')); print('yaml ok')"
git add .github/workflows/ci.yml
git commit -m "ci: run the two process-level launch-exit guards — --exact --ignored, <N> s marginal"
```

---

### Task 5: The six guard-deletion hand mutations (controller only)

**Runs in:** a detached worktree at the post-Task-4 HEAD, with its own `CARGO_TARGET_DIR`.
Never the main checkout, never a shared target dir. Needs Noah's permission: the
tool-permission classifier refused this class of edit on 2026-09-25.

| # | From | Mutation | Must go red |
|---|---|---|---|
| 1 | T4 Step 3 | `tail.rs` `deliver_unit`: delete the `row.shape != Some(entry.fingerprint)` block | the mixed-shapes resume property, as a mismatch against the expected reshaped frame |
| 2 | T4 Step 3 | `validate_subscribe`: delete the reshaped 409 block | "a cursor on foreign row … must be refused 409" |
| 3 | T6 Step 6 (1) | `seed_schema()`: add `schema.job_shapes = BTreeMap::new();` before returning | `a_promote_after_a_snapshot_reseed_still_refuses_foreign_pending_jobs` |
| 4 | T6 Step 6 (2) | delete `after_repl_step`'s `refuse_foreign_pending_jobs` line | `a_write_gate_opening_over_foreign_pending_jobs_stops_the_kernel_by_name` |
| 5 | T6 Step 6 (4) | move `do_promote`'s check to after `store_role` | `…_the_node_stays_a_follower` |
| 6 | T6 Step 6 (5) | delete the dispatch `assert_eq!(row.shape, Some(def.fingerprint), …)` | `dispatch_asserts_it_never_decodes_a_foreign_shape` |

For each mutation, apply it alone, run the named test and quote the red, restore, then
confirm `git -C <wt> diff --quiet`. If a mutation comes out GREEN, print the observable
behaviour before concluding anything: a green mutation is ambiguous between a vacuous test
and an inert mutation.

---

### Task 6: Final gate and ledger (controller)

- [ ] Workspace gate, run in the background by the controller:
  `cargo test --workspace --no-fail-fast 2>&1 | tee <scratch>/gate.txt`. Count
  `^test result: ok` lines against the suite count, which was 87 at `35a5813` plus any
  suite this plan added. Also run fmt, clippy, `cargo check -p charpente-core`, and
  `cargo publish --workspace --dry-run --no-verify`.
- [ ] Fresh seeds for the dispatch and fingerprint changes: the kernel fault-matrix property
  at two fresh bases, and `cargo run -p charpente-cli -- test --sim --dir crates/sim-fixture --universes 1000`.
- [ ] One `docs/LEDGER.md` entry covering Tasks 1-5. Check every hash with `git show <sha> --numstat`
  before writing it.
- [ ] PLAN.md M3 sub-project 3: no change. C-2 is still the open precondition.
