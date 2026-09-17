# Default-Secure Surfaces Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A `#[route]` that says neither `auth` nor `public`, and a `#[table]` that says neither `scoped_by = X` nor `global`, are compile errors naming both choices — M3 sub-project 2.

**Architecture:** Two proc-macro argument parsers (`crates/charpente-macros/src/route.rs`, `crates/charpente-macros/src/lib.rs`) each learn one new word, then refuse its absence. `public` and `global` are no-ops in expansion — they emit exactly what a bare attribute emits today — so the tree-wide sweep that adds them is syntax only, and the commit that flips the refusal on is the commit where the compiler proves the sweep found every site. Six new trybuild fixtures pin the error text byte-for-byte; two of them quote the helpdesk oracle.

**Tech Stack:** Rust proc macros (`syn`/`quote`), `trybuild` compile-fail pins, `cargo-mutants` 27.1.0 via `scripts/mutants.sh`, perl one-liners for the sweep.

**Spec:** `docs/superpowers/specs/2026-09-16-default-secure-surfaces-design.md` — read it first; every error message below is copied from it verbatim.

**Working directory for mid-flight state:** `.superpowers/sdd/2026-09-16-default-secure-surfaces/` (gitignored). Reviewers write their report to a file there before returning it — deliveries have been lost in flight before.

## Global Constraints

- **Commit on `main` directly, or on a branch `default-secure` merged back with a merge commit like `d731fde`** — CLAUDE.md permits both; the previous sub-project used the branch. **No `Co-Authored-By` trailers** (CLAUDE.md "Workflow"); this overrides any harness attribution reminder.
- **Every commit green.** The order of tasks is chosen so no commit leaves the workspace red: accept the words (Tasks 1–2), add the words everywhere (Task 3), refuse their absence (Task 4).
- **The three CI commands, counted:** `cargo fmt --all --check`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo test --workspace --no-fail-fast`. Green is proven by `grep -c "^test result: ok"` equalling `grep -c "^test result:"` on a `tee`'d log, plus zero `FAILED` — never by the absence of `FAILED` alone. The full suite can exceed the 600 s tool ceiling: run it in the background to a log file and poll the file, never `tail` the pipe (it eats the counts).
- **zsh, not bash:** `${PIPESTATUS[0]}` is empty in this shell. Test a `tee`'d command with `${pipestatus[1]}` or read the log; an unquoted `$var` does not word-split.
- **`.stderr` files are generated** with `TRYBUILD=overwrite cargo test -p charpente-macros --test trybuild`, then **reviewed line-by-line against the macro source string** before commit (the rig's own rule, `tests/trybuild.rs:6-8`). A generated pin nobody read is not a pin.
- **Error text is product surface (hard rule 5).** The messages below are the spec's; do not "improve" them — GUIDE §9 and the `.stderr` pins quote them.
- **Docs in the same commit as the API change (hard rule 6).** Task 4 carries GUIDE/DESIGN/README with the flip.
- **`docs/GUIDE.md` is compiled:** every ```rust block by `crates/guide-examples/build.rs` (` ```rust,ignore ` and ` ```text ` are not); `cargo build -p guide-examples` checks it alone. ` ```text ` blocks preceded by `<!-- pinned -->` are held to the code by `tests/doc_truth.rs` (fragment match on `.rs` files under `crates/` and `examples/`, split on lowercase-initial `<placeholder>` tokens, fragments ≥ 16 chars); `PINNED_FLOOR` in that file is a floor on the marker count.
- **Never share `CARGO_TARGET_DIR` across worktrees** (Task 6 builds in a sibling worktree with its own `target/`). **Commit before the mutation run** (Task 7).

---

### Task 1: `#[route]` accepts `public`

**Files:**
- Modify: `crates/charpente-macros/src/route.rs:1,20-34,50-65,125-141,148-160`
- Create: `crates/charpente-macros/tests/compile_fail/route_auth_and_public.rs` (+ generated `.stderr`)
- Create: `crates/charpente-macros/tests/compile_fail/route_duplicate_public.rs` (+ generated `.stderr`)
- Create: `crates/charpente-macros/tests/compile_pass/route_public_flag.rs`
- Create: `crates/charpente-macros/tests/compile_pass/route_public_cross_tenant.rs`
- Modify: `crates/charpente-macros/tests/compile_fail/route_unknown_flag.stderr` (FLAGS wording changes)
- Modify: `docs/superpowers/specs/2026-09-16-default-secure-surfaces-design.md` (two implementation details, below)

**Interfaces:**
- Produces: `RouteArgs.public: bool` (crate-private), read by Task 4's refusal and by `undeclared_capability`. No change to generated code: `auth: #auth` in the `RouteDef` glue is untouched.

- [ ] **Step 1: Write the four fixtures**

`crates/charpente-macros/tests/compile_fail/route_auth_and_public.rs`:

```rust
// A route is `auth` or `public`, never both (default-secure spec §2): the two
// flags answer one question — who reaches this handler — and a route that
// says both has not answered it. Refused at the second flag, by name.
//
// Positive controls: compile_pass/route_auth_flag.rs and
// compile_pass/route_public_flag.rs — this same route with one flag.
//
// Paths are fully qualified so the dropped expansion leaves no unused-import
// warnings in the pinned .stderr.
use charpente::route;

#[route(GET "/x", auth, public)]
fn handler(tx: &mut charpente::Tx) -> Result<(), charpente::http::ApiError> {
    let _ = tx;
    Ok(())
}

fn main() {}
```

`crates/charpente-macros/tests/compile_fail/route_duplicate_public.rs`:

```rust
// A flag written twice is refused, not silently folded — the same call
// route_duplicate_auth.rs makes for `auth`. `public` is a yes/no, so the
// second one adds nothing and is always a typo or a bad merge.
//
// Positive control: compile_pass/route_public_flag.rs — this same route with
// `public` written once.
use charpente::route;

#[route(GET "/x", public, public)]
fn handler(tx: &mut charpente::Tx) -> Result<(), charpente::http::ApiError> {
    let _ = tx;
    Ok(())
}

fn main() {}
```

`crates/charpente-macros/tests/compile_pass/route_public_flag.rs`:

```rust
// Positive control for compile_fail/route_duplicate_public.rs and
// compile_fail/route_auth_and_public.rs: `public` once is the legal spelling
// of "anonymous callers reach this handler" (default-secure spec §2), and it
// expands to exactly what a bare route expanded to before the flag existed.
use charpente::route;

#[route(GET "/x", public)]
fn handler(tx: &mut charpente::Tx) -> Result<(), charpente::http::ApiError> {
    let _ = tx;
    Ok(())
}

fn main() {}
```

`crates/charpente-macros/tests/compile_pass/route_public_cross_tenant.rs`:

```rust
// `public` composes with `cross_tenant` (default-secure spec §2; DECISIONS
// §11): an anonymous cross-tenant read is the maximal-leak shape, which is
// exactly why it must stay declarable and enumerable — `charpente describe`
// prints `auth=false cross_tenant=true` for it, and that line is the audit —
// rather than inexpressible. This is route_cross_tenant_capability.rs with
// `public` where `auth` stood.
use charpente::auth::Principal;
use charpente::http::ApiError;
use charpente::wire::WireU64;
use charpente::{CrossTenant, Id, Tenant, Tx, route, table};

#[table]
struct Org {
    id: Id<Org>,
    name: String,
}

#[table(scoped_by = Org)]
struct Invoice {
    id: Id<Invoice>,
    total: u32,
}

impl Tenant for Org {
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: Option<&Principal>, _scope: Id<Org>) -> Result<(), ApiError> {
        Ok(())
    }
}

#[route(GET "/admin/revenue", public, cross_tenant)]
fn revenue(tx: &mut Tx, cap: &CrossTenant) -> Result<WireU64, ApiError> {
    let rows: Vec<(Id<Org>, Invoice)> = tx.across_tenants::<Org>(cap).scan::<Invoice>()?;
    Ok(WireU64(rows.iter().map(|(_, i)| u64::from(i.total)).sum()))
}

fn main() {}
```

(Task 3's sweep will turn the `#[table]` on `Org` above into `#[table(global)]`; write it bare now so this file matches its sibling until then.)

- [ ] **Step 2: Run trybuild to see it fail**

Run: `cargo test -p charpente-macros --test trybuild 2>&1 | tee /tmp/t1-red.log; grep -E "route_(auth_and_public|duplicate_public|public_flag|public_cross_tenant)" /tmp/t1-red.log`

Expected: the two `compile_pass` cases FAIL with `error: a route flag is `auth`, `cross_tenant` or `emits(Ev, …)` …` (the parser does not know `public`); the two `compile_fail` cases FAIL because their stderr (that same FLAGS message) does not match a `.stderr` that does not yet exist.

- [ ] **Step 3: Teach the parser `public`**

In `crates/charpente-macros/src/route.rs`:

Line 1, the module doc grammar:
```rust
//! `#[route(METHOD "/path/:param", auth | public[, cross_tenant][, emits(Ev, …)])]`
```

`RouteArgs` (line 20): add the field after `auth`:
```rust
    auth: bool,
    /// `public` — the explicit declaration that anonymous callers reach this
    /// handler (default-secure spec §2). Expands to nothing: a `public` route
    /// is what a bare route was before absence became a refusal. Exactly one
    /// of `auth`/`public` is required (checked in `expand`, where the handler
    /// name is known for the message).
    public: bool,
```

The parse loop (lines 50-65): declare `let mut public = false;` next to `auth`, and replace the `auth` branch with:
```rust
            if flag == "auth" {
                if auth {
                    return Err(Error::new(
                        flag.span(),
                        "`auth` given twice — write it once",
                    ));
                }
                if public {
                    return Err(Error::new(flag.span(), ONE_OR_THE_OTHER));
                }
                auth = true;
            } else if flag == "public" {
                if public {
                    return Err(Error::new(
                        flag.span(),
                        "`public` given twice — write it once",
                    ));
                }
                if auth {
                    return Err(Error::new(flag.span(), ONE_OR_THE_OTHER));
                }
                public = true;
            } else if flag == "cross_tenant" {
```

The struct literal at the end of `parse` (line 125): `Ok(RouteArgs { method, path, auth, public, cross_tenant, emits })`.

`FLAGS` (line 139) and a new const beside it:
```rust
const FLAGS: &str = "a route flag is `auth`, `public`, `cross_tenant` or `emits(Ev, …)` — write \
                     `#[route(GET \"/path\", auth)]`, `#[route(GET \"/path\", public)]`, or \
                     `#[route(SSE \"/path\", emits(NoteAdded, NoteRemoved))]`";

/// A route answers "who reaches this handler" once. Shared by the two
/// branches that can see both flags so the wording cannot drift.
const ONE_OR_THE_OTHER: &str =
    "`auth` and `public` on one route — a route is one or the other; drop one";
```

`undeclared_capability` (line 151): the reconstructed attribute must carry whichever stance the route declared:
```rust
    let stance = if args.auth {
        ", auth"
    } else if args.public {
        ", public"
    } else {
        ""
    };
```
and rename the two `{auth}` interpolations in that format string to `{stance}`.

- [ ] **Step 4: Generate and review the pins**

Run: `TRYBUILD=overwrite cargo test -p charpente-macros --test trybuild 2>&1 | tee /tmp/t1-gen.log; git status --short crates/charpente-macros/tests/`

Expected new/changed files: `route_auth_and_public.stderr`, `route_duplicate_public.stderr` (new), `route_unknown_flag.stderr` (FLAGS wording). Open each and check:
- `route_auth_and_public.stderr` first line is exactly `` error: `auth` and `public` on one route — a route is one or the other; drop one `` and the caret is under `public`.
- `route_duplicate_public.stderr` first line is exactly `` error: `public` given twice — write it once `` and the caret is under the second `public`.
- `route_unknown_flag.stderr` first line is the new `FLAGS` text, nothing else in the file changed.
- No other `.stderr` changed (`git status` shows exactly those three).

- [ ] **Step 5: Run the macros package green**

Run: `cargo test -p charpente-macros --no-fail-fast 2>&1 | tee /tmp/t1.log; grep -c "^test result: ok" /tmp/t1.log; grep -c "FAILED" /tmp/t1.log`

Expected: the first count equals `grep -c "^test result:" /tmp/t1.log`; the second is `0`.

- [ ] **Step 6: Amend the spec's two implementation details**

The plan deviates from the spec on two implementation details; record them so the spec stays true. In `docs/superpowers/specs/2026-09-16-default-secure-surfaces-design.md`:

§3, replace the sentence starting "The first error is at the attribute (`Span::call_site()` when the argument list is empty; the `version` argument's span otherwise)" with:
```
The first error is at the attribute (`Span::call_site()`, which for an attribute macro is the
`#[table(…)]` invocation itself, whatever its argument list)
```

§7, replace the paragraph starting "**Order is the verification.**" with:
```
**Order is the verification, and every commit is green.** The words are accepted first
(`public`, `global` — no-ops in expansion), the sweep adds them everywhere second, and the
refusal of their absence lands third: that third commit is where the compiler proves the
sweep found every site, and no commit in between leaves the workspace red. The `charpente
new` template and GUIDE §1's block are held byte-identical by test, so both change together
or the test says so.
```

- [ ] **Step 7: fmt, clippy, commit**

Run: `cargo fmt --all --check && cargo clippy -p charpente-macros --all-targets -- -D warnings`
Expected: both silent (exit 0).

```bash
git add crates/charpente-macros/src/route.rs crates/charpente-macros/tests/compile_fail/route_auth_and_public.rs crates/charpente-macros/tests/compile_fail/route_auth_and_public.stderr crates/charpente-macros/tests/compile_fail/route_duplicate_public.rs crates/charpente-macros/tests/compile_fail/route_duplicate_public.stderr crates/charpente-macros/tests/compile_pass/route_public_flag.rs crates/charpente-macros/tests/compile_pass/route_public_cross_tenant.rs crates/charpente-macros/tests/compile_fail/route_unknown_flag.stderr docs/superpowers/specs/2026-09-16-default-secure-surfaces-design.md
git commit -m "macros(route): \`public\` is a flag — the explicit spelling of an anonymous route, one of auth/public, never both

Accepted only; absence is not yet refused (that lands after the sweep so no
commit is red). Two compile-fail pins, two compile-pass controls; FLAGS names
the new word; the undeclared-capability message reconstructs whichever stance
the route declared. Spec §3/§7 amended with the span and the green-commit order."
```

---

### Task 2: `#[table]` accepts `global`

**Files:**
- Modify: `crates/charpente-macros/src/lib.rs:20-31,88-166`
- Create: `crates/charpente-macros/tests/compile_fail/table_global_and_scoped_by.rs` (+ generated `.stderr`)
- Create: `crates/charpente-macros/tests/compile_fail/table_duplicate_global.rs` (+ generated `.stderr`)
- Create: `crates/charpente-macros/tests/compile_pass/table_global_with_version.rs`
- Modify: `crates/charpente-macros/tests/compile_fail/table_unknown_arg.stderr` (argument-list wording changes)

**Interfaces:**
- Produces: `TableArgs.global: bool` (crate-private), read by Task 4's refusal. `table::expand(input, version, scoped_by)` is unchanged — `global` changes nothing in expansion.

- [ ] **Step 1: Write the three fixtures**

`crates/charpente-macros/tests/compile_fail/table_global_and_scoped_by.rs`:

```rust
// A table is `scoped_by = X` or `global`, never both (default-secure spec
// §3): the two answer one question — do these rows belong to a tenant — and
// a table that says both has not answered it. Refused at `global`, naming
// the scope it contradicts.
//
// Positive controls: compile_pass/table_global_with_version.rs (`global`
// alone) and compile_pass/table_scoped_by_with_tenant_scope_impl.rs
// (`scoped_by` alone).
use charpente::table;

#[table]
struct Org {
    id: charpente::core::table::Id<Org>,
}

#[table(scoped_by = Org, global)]
struct W {
    id: charpente::core::table::Id<W>,
}

fn main() {}
```

`crates/charpente-macros/tests/compile_fail/table_duplicate_global.rs`:

```rust
// A key written twice is refused, not silently folded — the same call
// table_duplicate_scoped_by.rs and table_duplicate_version.rs make for their
// keys. `global` is a yes/no, so the second one is always a typo or a bad
// merge.
//
// Positive control: compile_pass/table_global_with_version.rs — `global`
// written once.
use charpente::table;

#[table(global, global)]
struct W {
    id: charpente::core::table::Id<W>,
}

fn main() {}
```

`crates/charpente-macros/tests/compile_pass/table_global_with_version.rs`:

```rust
// Positive control for compile_fail/table_duplicate_global.rs and
// compile_fail/table_global_and_scoped_by.rs, and the grammar pin: `global`
// composes with `version = N` in either order (default-secure spec §3), and
// on its own. Each expands to exactly what the bare attribute expanded to.
use charpente::table;

#[table(global)]
struct A {
    id: charpente::core::table::Id<A>,
}

#[table(version = 1, global)]
struct B {
    id: charpente::core::table::Id<B>,
}

#[table(global, version = 1)]
struct C {
    id: charpente::core::table::Id<C>,
}

fn main() {}
```

- [ ] **Step 2: Run trybuild to see it fail**

Run: `cargo test -p charpente-macros --test trybuild 2>&1 | tee /tmp/t2-red.log; grep -E "table_(global_and_scoped_by|duplicate_global|global_with_version)" /tmp/t2-red.log`

Expected: all three FAIL. The `compile_pass` case fails with a syn parse error at `global` (the parser expects `ident = value`); the two `compile_fail` cases fail because their stderr does not match a `.stderr` that does not exist yet.

- [ ] **Step 3: Teach the parser `global`**

In `crates/charpente-macros/src/lib.rs`, the `TableArgs` block (lines 88-100):

```rust
/// `#[table]`'s parsed argument list: `version = N` (default 1),
/// `scoped_by = <Type>` (`#[table(scoped_by = Org)]`) and the bare word
/// `global` (`#[table(global)]`), comma separated, any order — but each at
/// most once (`route`'s `auth`/`emits` and `job`'s `max`/`backoff` refuse a
/// repeat by name rather than last-write-wins; this follows the same
/// precedent), and never both `scoped_by` and `global`.
struct TableArgs {
    version: u32,
    scoped_by: Option<syn::Path>,
    /// The explicit declaration that no row belongs to a tenant
    /// (default-secure spec §3). Changes nothing in expansion — a `global`
    /// table is what a bare `#[table]` was before absence became a refusal.
    global: bool,
}

enum TableArg {
    Version(u32, proc_macro2::Span),
    ScopedBy(syn::Path, proc_macro2::Span),
    Global(proc_macro2::Span),
}
```

`impl Parse for TableArg` — `global` is a bare identifier, so it is recognised before the `=`:

```rust
impl syn::parse::Parse for TableArg {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<TableArg> {
        let ident: syn::Ident = input.parse()?;
        if ident == "global" {
            return Ok(TableArg::Global(ident.span()));
        }
        input.parse::<syn::Token![=]>()?;
        if ident == "version" {
            let lit: syn::LitInt = input.parse()?;
            let version: u32 = lit.base10_parse()?;
            Ok(TableArg::Version(version, lit.span()))
        } else if ident == "scoped_by" {
            let path: syn::Path = input.parse()?;
            let span = path.span();
            Ok(TableArg::ScopedBy(path, span))
        } else {
            Err(syn::Error::new(
                ident.span(),
                "takes only `version = N`, `scoped_by = <Type>` or `global` (or no arguments)",
            ))
        }
    }
}
```

(The "(or no arguments)" tail stays true until Task 4 removes it.)

`parse_table_args`: the empty-args early return gains `global: false`; the loop gains a `let mut global: Option<proc_macro2::Span> = None;` and a third arm; after the loop, the contradiction check:

```rust
            TableArg::Global(span) => {
                if global.is_some() {
                    return Err(syn::Error::new(
                        span,
                        "`global` given twice — write it once",
                    ));
                }
                global = Some(span);
            }
        }
    }
    if let (Some(span), Some(path)) = (global, &scoped_by) {
        let scope = &path.segments.last().expect("a parsed path has a segment").ident;
        return Err(syn::Error::new(
            span,
            format!("`global` and `scoped_by = {scope}` on one table — a table is one or the other; drop one"),
        ));
    }
    Ok(TableArgs {
        version: version.unwrap_or(1),
        scoped_by,
        global: global.is_some(),
    })
```

`pub fn table` (line 22) must read the new field, or rustc warns "field `global` is never read" and clippy's `-D warnings` goes red. The refusal that consumes it lands in Task 4; until then, read it explicitly with the reason on the line:

```rust
    let args = match parse_table_args(args) {
        Ok(a) => a,
        Err(e) => return e.to_compile_error().into(),
    };
    // Read here so the declaration is part of the parse contract from this
    // commit on; the refusal of its absence lands with the sweep (spec §7).
    let _declared_global = args.global;
```

- [ ] **Step 4: Generate and review the pins**

Run: `TRYBUILD=overwrite cargo test -p charpente-macros --test trybuild 2>&1 | tee /tmp/t2-gen.log; git status --short crates/charpente-macros/tests/`

Expected new/changed: `table_global_and_scoped_by.stderr`, `table_duplicate_global.stderr` (new), `table_unknown_arg.stderr` (wording). Check:
- `table_global_and_scoped_by.stderr` first line is exactly `` error: `global` and `scoped_by = Org` on one table — a table is one or the other; drop one ``, caret under `global`.
- `table_duplicate_global.stderr` first line is exactly `` error: `global` given twice — write it once ``, caret under the second `global`.
- `table_unknown_arg.stderr` first line is exactly `` error: #[table] takes only `version = N`, `scoped_by = <Type>` or `global` (or no arguments) ``.
- Nothing else changed.

- [ ] **Step 5: Run the macros package green, then fmt/clippy**

Run: `cargo test -p charpente-macros --no-fail-fast 2>&1 | tee /tmp/t2.log; grep -c "^test result: ok" /tmp/t2.log; grep -c "FAILED" /tmp/t2.log; cargo fmt --all --check && cargo clippy -p charpente-macros --all-targets -- -D warnings`

Expected: ok-count equals total, `0` FAILED, fmt and clippy silent.

- [ ] **Step 6: Commit**

```bash
git add crates/charpente-macros/src/lib.rs crates/charpente-macros/tests/compile_fail/table_global_and_scoped_by.rs crates/charpente-macros/tests/compile_fail/table_global_and_scoped_by.stderr crates/charpente-macros/tests/compile_fail/table_duplicate_global.rs crates/charpente-macros/tests/compile_fail/table_duplicate_global.stderr crates/charpente-macros/tests/compile_pass/table_global_with_version.rs crates/charpente-macros/tests/compile_fail/table_unknown_arg.stderr
git commit -m "macros(table): \`global\` is a word — the explicit spelling of a table with no tenant, one of scoped_by/global, never both

Accepted only; absence is not yet refused. Composes with version = N in either
order. Two compile-fail pins, one compile-pass control covering the grammar."
```

---

### Task 3: The sweep — every route says `public`, every table says `global`

**Files:**
- Modify: every `.rs` under `crates/` and `examples/` with a bare `#[table]`, `#[table(version = N)]`, or a `#[route(...)]` lacking `auth`/`public` (≈151 lines; the commands below find them — do not hand-list)
- Modify: `docs/GUIDE.md` (inside ```rust and ```rust,ignore blocks only), `README.md` (its one ```rust block)
- Modify: `crates/charpente-cli/templates/lib.rs:13,51` (covered by the `.rs` glob)
- Modify: 13 `crates/charpente-macros/tests/compile_fail/*.stderr` whose snippet line or caret column shifts

**Interfaces:**
- Consumes: `public` (Task 1) and `global` (Task 2) accepted by the parsers.
- Produces: a tree where Task 4's refusal finds nothing to refuse.

- [ ] **Step 1: Sweep the `.rs` files**

Both substitutions are anchored to a line that is nothing but the attribute (optionally indented), so comments, doc-comments and string literals that mention `#[table]` are not touched. The route substitution inserts `, public` directly after the path literal and skips any line already carrying `auth` or `public`.

```zsh
cd "$(git rev-parse --show-toplevel)"
perl -pi -e 's/^(\s*)#\[table\]$/$1#[table(global)]/; s/^(\s*#\[table\(version = \d+)\)\]$/$1, global)]/' $(grep -rlE '^\s*#\[table(\]|\(version = \d+\)\])$' --include='*.rs' crates examples)
perl -pi -e 's/^(\s*#\[route\((?:GET|POST|PUT|PATCH|DELETE|SSE) "[^"]*")(?=[,)])/$1, public/ unless /\b(auth|public)\b/' $(grep -rlE '^\s*#\[route\(' --include='*.rs' crates examples)
```

- [ ] **Step 2: Sweep the docs' Rust blocks**

Same substitutions, applied only between a ` ```rust ` (or ` ```rust,ignore `) fence and its closing fence — never inside a ` ```text ` block, which quote error messages that must keep showing the bare form:

```zsh
perl -pi -e 'if (/^```rust/) { $in = 1 } elsif (/^```/) { $in = 0 } if ($in) { s/^(\s*)#\[table\]$/$1#[table(global)]/; s/^(\s*#\[table\(version = \d+)\)\]$/$1, global)]/; s/^(\s*#\[route\((?:GET|POST|PUT|PATCH|DELETE|SSE) "[^"]*")(?=[,)])/$1, public/ unless /\b(auth|public)\b/ }' docs/GUIDE.md README.md
```

- [ ] **Step 3: Prove the sweep found every attribute line**

```zsh
grep -rEn '^\s*#\[table(\]|\(version = \d+\)\])$' --include='*.rs' crates examples | wc -l
grep -rEn '^\s*#\[route\(' --include='*.rs' crates examples | grep -vE '\b(auth|public)\b' | wc -l
awk '/^```rust/{in_=1;next} /^```/{in_=0} in_ && /^\s*#\[table(\]|\(version = [0-9]+\)\])$/ {print FILENAME":"NR": "$0}' docs/GUIDE.md README.md | wc -l
awk '/^```rust/{in_=1;next} /^```/{in_=0} in_ && /^\s*#\[route\(/ && !/auth|public/ {print FILENAME":"NR": "$0}' docs/GUIDE.md README.md | wc -l
git diff --stat | tail -1
```

Expected: four `0`s. The diff stat should touch on the order of 60–70 files. Spot-check three hunks by eye: `crates/charpente/src/http/app.rs` (the built-in `Session` table now `#[table(global)]`), `crates/charpente-cli/templates/lib.rs` (`#[table(global)]` and `#[route(POST "/notes/:id", public)]`), and `docs/GUIDE.md` around line 5300 (§16's `Org` is `#[table(global)]`, `Memo` still `scoped_by = Org`, `count_memos` still `auth`).

Also confirm no ` ```text ` block moved: `git diff docs/GUIDE.md | grep '^[-+]' | grep -vE '^(\+\+\+|---)' | grep -vE '#\[(table|route)' | wc -l` → `0`.

- [ ] **Step 4: Regenerate the shifted `.stderr` pins and review the diff**

Run: `TRYBUILD=overwrite cargo test -p charpente-macros --test trybuild 2>&1 | tee /tmp/t3-gen.log; git diff --stat crates/charpente-macros/tests/compile_fail/`

Expected changed `.stderr` files, exactly these thirteen: `across_tenants_without_capability`, `cross_tenant_token_cannot_be_forged`, `route_bare_u64_body`, `route_bare_u64_param`, `route_duplicate_emits_clause`, `route_duplicate_emits_name`, `route_emits_empty`, `route_emits_no_parens`, `route_emits_on_non_sse`, `route_missing_tx`, `route_not_a_result`, `route_unknown_flag`, `route_unknown_method`.

Review `git diff crates/charpente-macros/tests/compile_fail/` hunk by hunk. The ONLY permitted changes are (a) the quoted snippet line now containing `, public` or `(global)`, (b) a caret column moved right by the inserted text, and (c) in `across_tenants_without_capability.stderr` only, the message line's two reconstructed attributes now read `#[route(GET "/admin/revenue", public)]` and `#[route(GET "/admin/revenue", public, cross_tenant)]`. Any other change to a message line is a regression — stop and find out why.

- [ ] **Step 5: Compile everything, run the packages the sweep touched most**

```zsh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tee /tmp/t3-clippy.log | tail -3
cargo build -p guide-examples 2>&1 | tail -2
cargo test -p charpente-macros -p charpente-cli -p guide-examples -p ts-fixture-app -p invoicing --no-fail-fast 2>&1 | tee /tmp/t3.log; grep -c "^test result: ok" /tmp/t3.log; grep -c "^test result:" /tmp/t3.log; grep -c "FAILED" /tmp/t3.log
```

Expected: fmt silent; clippy ends `Finished` with no `error`; guide-examples builds (every GUIDE block still compiles); the two counts equal; `0` FAILED. `charpente-cli`'s `new` tests prove the template still matches GUIDE §1 and still builds; `ts-fixture-app`'s `drift`/`samples` prove `client.ts` did not move; `invoicing`'s `describe_golden` proves `describe` did not move.

- [ ] **Step 6: Commit**

```bash
git add -A crates examples docs/GUIDE.md README.md
git commit -m "sweep: every route says auth or public, every table says scoped_by or global

Syntax only — public and global expand to what the bare attribute did. The
.rs tree, GUIDE's rust blocks, README's sample and the charpente new template;
thirteen compile-fail pins re-generated for the shifted snippet/caret and
reviewed line by line (one message line moved: the undeclared-capability
reconstruction now carries the route's stance). The refusal that proves this
sweep complete is the next commit."
```

---

### Task 4: The flip — absence is a compile error, with its docs

**Files:**
- Modify: `crates/charpente-macros/src/route.rs:148-160,172-185`
- Modify: `crates/charpente-macros/src/lib.rs:22-31,119-124`
- Create: `crates/charpente-macros/tests/compile_fail/helpdesk_route_neither_auth_nor_public.rs` (+ generated `.stderr`)
- Create: `crates/charpente-macros/tests/compile_fail/helpdesk_table_no_tenancy.rs` (+ generated `.stderr`)
- Modify: `crates/charpente-macros/tests/compile_fail/table_unknown_arg.stderr`
- Modify: `docs/GUIDE.md` §2 (`:145-160`, `:305-310`), §5 (`:638-643`, `:889-893`, `:1200-1203`), §9 (`:2576-2592`, `:2612-2650`), §16 (`:5286-5290`, `:5392-5400`, `:5647-5648`)
- Modify: `crates/guide-examples/tests/doc_truth.rs:47` (`PINNED_FLOOR`)
- Modify: `DESIGN.md:553-556,575,1421,1460,1522`
- Modify: `README.md:33-50`

**Interfaces:**
- Consumes: `RouteArgs.public`, `TableArgs.global`.
- Produces: the two refusals, and GUIDE §5/§2 pinned blocks that `doc_truth` holds to the two format strings below — so the strings must stay byte-identical to the doc lines modulo `{name}`/`{method}`/`{path}`.

- [ ] **Step 1: Write the two helpdesk fixtures**

`crates/charpente-macros/tests/compile_fail/helpdesk_route_neither_auth_nor_public.rs`:

```rust
// The helpdesk oracle, Critical 1 (docs/experiments/2026-09-helpdesk/README.md;
// default-secure spec §5). The route that let anyone join any org, quoted
// from `8913038:examples/helpdesk/src/lib.rs:471-474` — attribute, doc
// comment and signature verbatim; the body reduced to what keeps the
// signature's types in use. It declared neither `auth` nor `public`, and
// until this refusal that was a working route.
//
// The types are real (`Wire` on the body and view) so the ONE error in the
// pin is the refusal: if the refusal were removed, this file would compile.
//
// Positive controls: compile_pass/route_auth_flag.rs and
// compile_pass/route_public_flag.rs.
#![allow(dead_code)]
use charpente::http::ApiError;
use charpente::{Id, Tx, Wire, route, table};
use serde::{Deserialize, Serialize};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

#[table(global)]
struct Agent {
    id: Id<Agent>,
    org: Id<Org>,
    subject: String,
}

#[derive(Wire, Serialize)]
struct AgentView {
    id: Id<Agent>,
    org: Id<Org>,
    subject: String,
}

#[derive(Wire, Serialize, Deserialize)]
struct CreateAgentBody {
    org: Id<Org>,
    subject: String,
}

/// Bootstrap route: registers an agent (user) into an org. Also no `auth`,
/// for the same reason as `create_org` — see FINDINGS.md.
#[route(POST "/agents/:id")]
fn create_agent(tx: &mut Tx, id: Id<Agent>, body: CreateAgentBody) -> Result<AgentView, ApiError> {
    let _ = tx;
    Ok(AgentView {
        id,
        org: body.org,
        subject: body.subject,
    })
}

fn main() {}
```

`crates/charpente-macros/tests/compile_fail/helpdesk_table_no_tenancy.rs`:

```rust
// The helpdesk oracle, Critical 2 (docs/experiments/2026-09-helpdesk/README.md;
// default-secure spec §5). The declaration the cross-tenant existence oracle
// cascades from: `Ticket`, quoted from `8913038:examples/helpdesk/src/lib.rs:134-143`
// with its enum fields reduced to the `Codec` primitives they encode as. It
// carries an `org: Id<Org>` and declares no tenancy, so every
// `tx.get(id)?.is_some()` on it (lib.rs:542) ran unscoped. Once it says
// `scoped_by = Org`, each of those reads becomes GUIDE §16's E0277 at the
// call line — the error the feature exists to produce.
//
// Positive controls: compile_pass/table_global_with_version.rs and
// compile_pass/table_scoped_by_with_tenant_scope_impl.rs.
#![allow(dead_code)]
use charpente::{Id, table};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

#[table(version = 2)]
struct Ticket {
    id: Id<Ticket>,
    org: Id<Org>,
    state: u8,
    opener_subject: String,
    title: String,
    body: String,
    pending_since_hour: Option<u64>,
    priority: u8,
}

fn main() {}
```

- [ ] **Step 2: Run trybuild to see both fixtures fail for the wrong reason**

Run: `cargo test -p charpente-macros --test trybuild 2>&1 | tee /tmp/t4-red.log; grep -E "helpdesk_" /tmp/t4-red.log`

Expected: both FAIL with trybuild's "expected compile error, but it compiled" (or equivalent) — they compile today, which is the defect.

- [ ] **Step 3: Refuse a route with neither flag**

In `crates/charpente-macros/src/route.rs`, `expand` (line 172) — the stance check goes first, before the `emits` check, because it is the first question a reviewer asks:

```rust
pub(crate) fn expand(args: RouteArgs, func: ItemFn) -> syn::Result<TokenStream> {
    if !args.auth && !args.public {
        let name = &func.sig.ident;
        let method = &args.method;
        let path = args.path.value();
        return Err(Error::new(
            args.path.span(),
            format!(
                "`{name}` declares neither `auth` nor `public` — a route says which callers \
                 reach it: write `#[route({method} \"{path}\", auth)]` to require a principal \
                 (`ctx.principal()` is `Some`), or `#[route({method} \"{path}\", public)]` to \
                 serve anonymous callers. Neither is the default."
            ),
        ));
    }
    if !args.emits.is_empty() && args.method != "SSE" {
```

`undeclared_capability` (line 151): the empty branch is now unreachable — `expand` refused before either `expand_*` could call this. Replace the three-way with:

```rust
    // `expand` refused a route with neither flag before reaching here.
    let stance = if args.auth { ", auth" } else { ", public" };
```

- [ ] **Step 4: Refuse a table with neither word**

In `crates/charpente-macros/src/lib.rs`, `pub fn table` (line 22): delete the `let _declared_global = args.global;` line from Task 2 and replace the body after `parse_macro_input!` with:

```rust
    let input = parse_macro_input!(input as DeriveInput);
    if args.scoped_by.is_none() && !args.global {
        let name = &input.ident;
        return Error::new(
            proc_macro2::Span::call_site(),
            format!(
                "`{name}` declares no tenancy — a table says which: write \
                 `#[table(scoped_by = Org)]` if every row belongs to one tenant (the key \
                 becomes `(scope, id)` and a bare `Tx` cannot read it), or `#[table(global)]` \
                 if its rows belong to no tenant. Neither is the default."
            ),
        )
        .to_compile_error()
        .into();
    }
    match table::expand(input, args.version, args.scoped_by) {
```

And the argument-list message in `impl Parse for TableArg` loses its now-false tail:
```rust
                "takes `version = N`, `scoped_by = <Type>` or `global`",
```

- [ ] **Step 5: Generate and review the pins**

Run: `TRYBUILD=overwrite cargo test -p charpente-macros --test trybuild 2>&1 | tee /tmp/t4-gen.log; git status --short crates/charpente-macros/tests/`

Expected new/changed: the two `helpdesk_*.stderr` (new) and `table_unknown_arg.stderr`. Check:
- `helpdesk_route_neither_auth_nor_public.stderr` is ONE error whose first line is exactly `` error: `create_agent` declares neither `auth` nor `public` — a route says which callers reach it: write `#[route(POST "/agents/:id", auth)]` to require a principal (`ctx.principal()` is `Some`), or `#[route(POST "/agents/:id", public)]` to serve anonymous callers. Neither is the default. `` and the caret is under `"/agents/:id"`. No warnings follow it.
- `helpdesk_table_no_tenancy.stderr` is ONE error whose first line is exactly `` error: `Ticket` declares no tenancy — a table says which: write `#[table(scoped_by = Org)]` if every row belongs to one tenant (the key becomes `(scope, id)` and a bare `Tx` cannot read it), or `#[table(global)]` if its rows belong to no tenant. Neither is the default. `` and the caret spans `#[table(version = 2)]`. No warnings follow it.
- `table_unknown_arg.stderr` first line is exactly `` error: #[table] takes `version = N`, `scoped_by = <Type>` or `global` ``.

If either helpdesk pin carries a second error or a warning, fix the FIXTURE (the pin must isolate the refusal), not the message.

- [ ] **Step 6: GUIDE — the rule, the examples, the pinned blocks, the catalog**

All edits in `docs/GUIDE.md`. Line numbers are pre-sweep and may have moved by a few lines; anchor on the quoted text.

**§2, "The `#[table]` contract" (line 145):** add a first bullet:
```
- It declares its tenancy: `#[table(scoped_by = Org)]` if every row belongs
  to one tenant (§16), or `#[table(global)]` if its rows belong to none. A
  bare `#[table]` is a compile error naming both — the platform never guesses
  which a table is, and the built-in tables (`Session`, `ApiKey`, …) are
  `global` like any other.
```

**§2, "What the macro rejects" (line 305):** before the existing ```rust,ignore block, add a pinned block:
```
The first rejection is the one every table meets once — a declaration with
no tenancy stance:

<!-- pinned -->
```text
`<table>` declares no tenancy — a table says which: write `#[table(scoped_by = Org)]` if every row belongs to one tenant (the key becomes `(scope, id)` and a bare `Tx` cannot read it), or `#[table(global)]` if its rows belong to no tenant. Neither is the default.
`global` and `scoped_by = <scope>` on one table — a table is one or the other; drop one
`global` given twice — write it once
```

`<table>` is the struct's name. The other six rejections are about the
struct's shape:
```
(`<table>` and `<scope>` are lowercase-initial so `doc_truth` treats them as placeholders — the code renders the struct and scope names there.)

**§5, "The `#[route]` argument convention" (line 640):** replace the parenthetical "the trailing `, auth` flag is optional and covered under Sessions below" with:
```
exactly one of `, auth` / `, public` follows the path — a route says which
callers reach it, and a route that says neither is a compile error naming
both; `auth` is covered under Sessions below
```

**§5, the auth semantics paragraph (line 889):** replace "Without `auth`, the route always runs and" with "With `public`, the route always runs and".

**§5, "What `#[route]` rejects" (line 1200):** after "Every rejection is a compile error naming the contract and the fix:" and before the ```rust,ignore block, add:
```
The first is the one every route meets once:

<!-- pinned -->
```text
`<handler>` declares neither `auth` nor `public` — a route says which callers reach it: write `#[route(<method> "<path>", auth)]` to require a principal (`ctx.principal()` is `Some`), or `#[route(<method> "<path>", public)]` to serve anonymous callers. Neither is the default.
`auth` and `public` on one route — a route is one or the other; drop one
`public` given twice — write it once
```

The rest are about the handler's shape:
```

**§9, the `#[table]` block (line 2580):** add the three table lines (same text as the §2 pinned block, with `<table>`/`<scope>`) at the top of the existing ```text block, change "Six ways" to "Nine ways", and prepend to the "In order:" paragraph: "No tenancy declared — write `scoped_by = …` or `global`. Both at once — drop one. `global` twice — once."

**§9, the `#[route]` block (line 2612):** add the three route lines at the top of the block; replace the existing FLAGS line with the new wording (`` a route flag is `auth`, `public`, `cross_tenant` or `emits(Ev, …)` — write `#[route(GET "/path", auth)]`, `#[route(GET "/path", public)]`, or `#[route(SSE "/path", emits(NoteAdded, NoteRemoved))]` ``); replace the `revenue` line's two attributes with `#[route(GET "/admin/revenue", public)]` and `#[route(GET "/admin/revenue", public, cross_tenant)]` (the pin now carries the stance); and in the prose after the block, "a flag after the path is neither `auth` nor `emits(...)`" becomes "a flag after the path is none of `auth`, `public`, `cross_tenant`, `emits(...)`", and prepend "the route declares neither `auth` nor `public`; it declares both; `public` appears twice;" to the "The rest, in order:" list.

**§16 (line 5290):** the bullet "`#[table(scoped_by = Org)]` on every table that belongs to a tenant;" becomes "`#[table(scoped_by = Org)]` on every table that belongs to a tenant, `#[table(global)]` on the scope type and every table that belongs to none — a bare `#[table]` does not compile;". **Line 5392:** "**Unscoped tables stay reachable.**" → "**Global tables stay reachable.**"; the comment on line 5400 `// unscoped: Region has no tenant` → `// global: Region has no tenant`. **Line 5647:** the non-goal bullet "**Retrofitting existing tables.** Scoping is opt-in per table and no migration is forced on any app — but see the in-place refusal above." becomes "**Retrofitting existing tables.** Every table declares `scoped_by` or `global`, but no migration is forced: `global` is what a bare table always was, and a table cannot gain a scope in place — see the refusal above."

**`crates/guide-examples/tests/doc_truth.rs:47`:** `const PINNED_FLOOR: usize = 16;` (two new markers).

- [ ] **Step 7: DESIGN and README**

`DESIGN.md:553` (§3 "Tables" bullet): after "Schema *is* the Rust definition." add: "Every table declares its tenancy — `scoped_by = Org` or `global` — and a bare `#[table]` is a compile error naming both (spec 2026-09-16-default-secure-surfaces)."

`DESIGN.md:575`: the grammar `#[route(METHOD "/path/:param"[, auth][, cross_tenant])]` becomes `#[route(METHOD "/path/:param", auth | public[, cross_tenant])]`, and append to that bullet: "A route declares exactly one of `auth`/`public`; neither is a compile error naming both."

`DESIGN.md:1273` (§7, the `describe` contract list): "whether it requires auth," → "whether it declared `auth` (`false` is a declared `public`, never an omission),".

`DESIGN.md:1421` (§8): "and `#[route(…, auth)]` gates on it." → "and `#[route(…, auth)]` gates on it — a route that declares neither `auth` nor `public` does not compile, so an open route is a declaration, never an omission (default-secure, 2026-09-16)."

`DESIGN.md:1522` (§8 tenancy paragraph): after the sentence ending "the app's own `ApiError`." add: "A table that declares neither `scoped_by` nor `global` does not compile: which tables are tenant data is the app's declaration at every struct, never the platform's inference (default-secure, 2026-09-16; the field-type inference was weighed and rejected — the membership table must be `global` and must hold `Id<Org>`)."

`README.md:33-50`, the "What it looks like" block — the honest twelve lines are the declared ones:
```rust
#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

#[table(scoped_by = Org)]
struct Invoice {
    id: Id<Invoice>,
    amount_cents: i64,
    state: InvoiceState,
}

#[route(POST "/orgs/:org/invoices/:id/finalize", auth)]
fn finalize(tx: &mut Tx, org: Id<Org>, id: Id<Invoice>, ctx: &Ctx) -> Result<Invoice, ApiError> {
    let mut tx = tx.scope::<Org>(org, ctx)?;                // the one auditable line
    let mut inv = tx.get(id)?.ok_or(ApiError::NotFound)?;   // typed row, no SQL string
    inv.state = InvoiceState::Final;
    tx.put(&inv);
    tx.enqueue(SendReceipt { invoice: id });   // job enqueued IN the same transaction
    tx.emit(InvoiceFinalized { invoice: id }); // realtime event, same transaction
    Ok(inv)
}
```
and add one sentence to the paragraph below it: "Note also what's declared: the route says `auth`, the table says which tenant owns its rows, and a route or table that says neither does not compile."

(`tx.enqueue` on the `ScopedTx` reaches `Tx::enqueue` through the deref — there is no inherent `enqueue` on `ScopedTx` to shadow it, unlike `scan`; `tx.emit` is `ScopedTx`'s own and is keyed by the scope.)

- [ ] **Step 8: The full gate — this is where the sweep is proven**

```zsh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tee /tmp/t4-clippy.log | tail -3
cargo build -p guide-examples 2>&1 | tail -2
```
Expected: silent, `Finished`, `Finished`. Any "declares neither" or "declares no tenancy" error here names a site the sweep missed — add the word there (it is a real site; it just was not matched by the line-anchored regex) and re-run. Record every such site in the commit message.

Then the workspace suite, in the background to a log:
```zsh
( cargo test --workspace --no-fail-fast > /tmp/t4-ws.log 2>&1; echo "EXIT=$?" >> /tmp/t4-ws.log ) &
```
Poll `tail -2 /tmp/t4-ws.log` until `EXIT=` appears, then:
```zsh
grep -c "^test result: ok" /tmp/t4-ws.log; grep -c "^test result:" /tmp/t4-ws.log; grep -c "FAILED" /tmp/t4-ws.log; grep "^EXIT=" /tmp/t4-ws.log
```
Expected: the two counts equal, `0`, `EXIT=0`. `doc_truth` (in `guide-examples`) proves the two new pinned blocks against the two format strings; `describe_contract` and `ts-fixture-app` prove nothing on the wire moved.

- [ ] **Step 9: Commit**

```bash
git add crates/charpente-macros/src/route.rs crates/charpente-macros/src/lib.rs crates/charpente-macros/tests/compile_fail/helpdesk_route_neither_auth_nor_public.rs crates/charpente-macros/tests/compile_fail/helpdesk_route_neither_auth_nor_public.stderr crates/charpente-macros/tests/compile_fail/helpdesk_table_no_tenancy.rs crates/charpente-macros/tests/compile_fail/helpdesk_table_no_tenancy.stderr crates/charpente-macros/tests/compile_fail/table_unknown_arg.stderr docs/GUIDE.md crates/guide-examples/tests/doc_truth.rs DESIGN.md README.md
git commit -m "macros: a route that says neither auth nor public, and a table that says neither scoped_by nor global, do not compile

The two refusals name both choices and the fix. Pinned by the helpdesk oracle
quoted verbatim (8913038 lib.rs:473 and :134) plus the four grammar pins from
the two previous commits. GUIDE §2/§5 carry the rule and two new doc_truth
pinned blocks (floor 14 -> 16), §9 the catalog lines, §16 says global; DESIGN
§3/§8 say declared, not opt-in; README's sample is the declared shape."
```
(Append a line naming any sweep-missed sites Step 8 found, or "Sweep-missed sites: none.")

---

### Task 5: DECISIONS §13, the superseded non-goal, PLAN's status line

**Files:**
- Modify: `DECISIONS.md` (append after §12, which ends the file)
- Modify: `docs/superpowers/specs/2026-08-30-tenant-scoping-design.md:385-386`
- Modify: `PLAN.md:212-217`

- [ ] **Step 1: Append DECISIONS §13**

Append to `DECISIONS.md`:

```markdown

## 13. No surface has a permissive default (2026-09-16)

M3 sub-project 2 (spec `docs/superpowers/specs/2026-09-16-default-secure-surfaces-design.md`)
made a `#[route]` that declares neither `auth` nor `public`, and a `#[table]` that declares
neither `scoped_by = X` nor `global`, compile errors naming both choices. This entry records
the reading that generalises, because it binds the next surface as much as these two.

**The ruling.** Where a declaration on a route, table or job selects between a guarded
behaviour and an open one, its absence is a compile error naming both — never a quiet
default to either. The framework is the reviewer, and a reviewer's first question is "is
this open?"; a default answers it silently, on the author's behalf, which is the one thing
the 2026-09-03 helpdesk build showed an agent will not notice. This extends §11's shape —
declared on the entry point, enumerable by `describe` — from capabilities to stances: `public`
and `global` are declarations `describe` already lists (`auth=false`, `scoped=false`), so an
audit of "what is open" is an inventory, not a grep.

**Why the platform declares rather than infers, stated so it stays decided.** Two inference
rules for "this table should be tenant-scoped" were weighed and rejected in the spec (§4):
keying off a declared scope type cannot fire on an app that declared none — the helpdesk —
and keying off a field of type `Id<scope>` flags the membership table first, which must be
`global` and must hold that id because `Tenant::authorize` reads it through a bare `&Tx`. An
inference with a mandatory override is a declaration with a heuristic in front of it. The
platform asks at every struct instead.

**What this binds.** A future `#[route]`, `#[table]` or `#[job]` word that gates a behaviour
arrives as a required choice between named alternatives, with a compile-fail pin for the
absent case in `crates/charpente-macros/tests/compile_fail/` and its line in GUIDE §9. A
config key, a builder default, or a "sensible" fallback in the macro is the thing this entry
refuses. **Reopen if:** a surface appears whose only honest default is "always on" with no
second choice worth naming — then there is nothing to declare, and this entry does not
apply to it.
```

- [ ] **Step 2: Supersede the tenant-scoping non-goal**

In `docs/superpowers/specs/2026-08-30-tenant-scoping-design.md`, replace lines 385-386:
```
- **Retrofitting existing tables.** Scoping is opt-in per table; no migration is forced on
  any app, and none of the fixtures is converted except as an oracle needs one.
```
with:
```
- **Retrofitting existing tables.** Scoping is opt-in per table; no migration is forced on
  any app, and none of the fixtures is converted except as an oracle needs one.
  *Superseded 2026-09-16* (`2026-09-16-default-secure-surfaces-design.md`, Noah's 2026-09-13
  ruling): opt-in ended — every table now declares `scoped_by` or `global`. The
  no-forced-migration half stands: `global` is what a bare table always was.
```

- [ ] **Step 3: Mark sub-project 2 in PLAN.md**

`PLAN.md:212`: "2. **Default-secure surfaces** (own brainstorm and spec; a DESIGN §7/§8 change)." → "2. **Default-secure surfaces** [shipped 2026-09-16; spec `docs/superpowers/specs/2026-09-16-default-secure-surfaces-design.md`]." Leave the rest of the bullet as the record of what was asked.

- [ ] **Step 4: Verify docs-only, commit**

Run: `cargo test -p guide-examples --no-fail-fast 2>&1 | tee /tmp/t5.log; grep -c "^test result: ok" /tmp/t5.log; grep -c "FAILED" /tmp/t5.log`
Expected: ok-count equals `grep -c "^test result:"`, `0` FAILED (`doc_inventory` reads PLAN/CLAUDE; `describe_contract` reads the invoicing fixture).

```bash
git add DECISIONS.md docs/superpowers/specs/2026-08-30-tenant-scoping-design.md PLAN.md
git commit -m "decisions: §13 no surface has a permissive default; tenant-scoping's opt-in non-goal superseded; PLAN marks M3 sub-project 2 shipped"
```

---

### Task 6: The PLAN oracle, run literally — the rebased helpdesk fails at both Critical sites

**Files:**
- Create (gitignored, working state): `.superpowers/sdd/2026-09-16-default-secure-surfaces/oracle-run.txt`
- No tracked file changes; the ledger (Task 8) quotes the result.

This is a one-off, witnessed run. The branch `docs-only-helpdesk` is evidence and MUST NOT be rewritten: work on a detached HEAD in a sibling worktree, never `git rebase` while the branch is checked out.

- [ ] **Step 1: Detached worktree at the helpdesk tip, rebased onto main**

```zsh
ROOT="$(git rev-parse --show-toplevel)"
git worktree add --detach "$ROOT/../charpente-helpdesk-oracle" 8913038
cd "$ROOT/../charpente-helpdesk-oracle"
git rebase --onto main 6827d75
```
Expected: conflicts are likely in `Cargo.toml` (the `members` list) and `Cargo.lock`, possibly `CLAUDE.md`. Resolve each as: take main's version, then re-add `"examples/helpdesk"` to `members`; for `Cargo.lock` take main's (`git checkout --ours Cargo.lock` — during a rebase, `--ours` is the branch being rebased ONTO, i.e. main); `git add` and `git rebase --continue` until it ends. If a conflict is in `examples/helpdesk/` itself, keep the helpdesk side unchanged — it is the oracle.

Confirm the branch is untouched: `cd "$ROOT" && git rev-parse docs-only-helpdesk` → `8913038…`.

- [ ] **Step 2: Check it, capture everything**

```zsh
cd "$ROOT/../charpente-helpdesk-oracle"
cargo check -p helpdesk 2>&1 | tee "$ROOT/.superpowers/sdd/2026-09-16-default-secure-surfaces/oracle-run.txt" | grep -cE "declares (neither|no tenancy)"
```
(This worktree has its own `target/`; the first check builds the platform cold and takes minutes.)

Expected count: **11** — eight tables (`Org`, `Agent`, `Ticket`, `Comment`, `Survey`, `SlaReminder`, `AppClock`, `IdCounter`) and three routes (`create_org`, `create_agent`, `login`). Other errors may follow (the helpdesk was written against an older API); they are not the oracle and are not counted.

Then the two Critical sites specifically:
```zsh
grep -nE "create_agent. declares neither|Ticket. declares no tenancy" -A 2 "$ROOT/.superpowers/sdd/2026-09-16-default-secure-surfaces/oracle-run.txt"
```
Expected: both present, the first pointing at `examples/helpdesk/src/lib.rs:473`, the second at `examples/helpdesk/src/lib.rs:134`.

- [ ] **Step 3: Remove the worktree**

```zsh
cd "$ROOT"
git worktree remove --force "$ROOT/../charpente-helpdesk-oracle"
git worktree prune
git worktree list
```
Expected: only the main worktree listed. (`remove --force` deletes the worktree's multi-GB `target/`; if it exceeds the tool timeout, `rm -rf` the directory first and then `git worktree prune`.)

No commit in this task; `oracle-run.txt` is quoted by Task 8.

---

### Task 7: The mutation run over the two parse files

**Files:**
- Create (gitignored): `.superpowers/sdd/2026-09-16-default-secure-surfaces/mutants-run.txt`

Every task before this is committed (`git status --short` must be empty before starting — a mutation run over a dirty tree can leave a fix silently reverted).

- [ ] **Step 1: List the mutants so the regex is known, not guessed**

```zsh
cargo mutants --list -f crates/charpente-macros/src/route.rs -f crates/charpente-macros/src/lib.rs 2>/dev/null | grep -E "RouteArgs|route::expand|undeclared_capability|TableArg|parse_table_args|charpente_macros::table\b" | tee /tmp/t7-list.txt | wc -l
```
Expected: a few dozen lines naming mutants inside `<impl Parse for RouteArgs>::parse`, `route::expand`, `undeclared_capability`, `<impl Parse for TableArg>::parse`, `parse_table_args` and `table`. Read the list: every mutant should be in one of those functions; if the grep caught something outside them, tighten the regex below.

- [ ] **Step 2: Run the instrument, backgrounded**

```zsh
ROOT="$(git rev-parse --show-toplevel)"
( scripts/mutants.sh --file crates/charpente-macros/src/route.rs --file crates/charpente-macros/src/lib.rs --re 'RouteArgs|route::expand|undeclared_capability|TableArg|parse_table_args|charpente_macros::table\b' --timeout 1200 > "$ROOT/.superpowers/sdd/2026-09-16-default-secure-surfaces/mutants-run.txt" 2>&1; echo "EXIT=$?" >> "$ROOT/.superpowers/sdd/2026-09-16-default-secure-surfaces/mutants-run.txt" ) &
```
Each mutant rebuilds `charpente-macros` and runs its tests including the ~90-case trybuild matrix, so budget minutes per mutant. Poll `tail -3` of the file until `EXIT=` appears.

- [ ] **Step 3: Read the verb's verdict, never cargo-mutants' own exit code**

```zsh
tail -15 "$ROOT/.superpowers/sdd/2026-09-16-default-secure-surfaces/mutants-run.txt"
```
Expected: the verb's summary with `caught` equal to the viable count, `missed 0`, `timeout 0`, `EXIT=0`. A `MISSED` is a one-sided property: find which fixture should have gone red under that mutant and write the missing pin (a new compile_fail/compile_pass case), commit it as its own commit, and re-run. A `TIMEOUT` is not a catch — raise `--timeout` and re-run that mutant. Do not add to `.cargo/mutants.toml`.

Record the printed counts (`caught`, `missed`, `timeout`, `unviable`) for Task 8.

---

### Task 8: Ledger entry and the final gate

**Files:**
- Modify: `docs/LEDGER.md` (append)

- [ ] **Step 1: The final triple, counted, on the committed tree**

`git status --short` must be empty. Then:
```zsh
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -1
( cargo test --workspace --no-fail-fast > /tmp/t8-ws.log 2>&1; echo "EXIT=$?" >> /tmp/t8-ws.log ) &
```
Poll until `EXIT=`; then `grep -c "^test result: ok" /tmp/t8-ws.log; grep -c "^test result:" /tmp/t8-ws.log; grep -c "FAILED" /tmp/t8-ws.log; grep "^EXIT=" /tmp/t8-ws.log`. Expected: equal counts, `0`, `EXIT=0`. Note the ok-count for the ledger.

- [ ] **Step 2: Append the ledger entry**

Append to `docs/LEDGER.md`, filling each bracketed slot from the runs above (the counts are the run's own numbers; the error list is `oracle-run.txt`'s matching lines):

```markdown

## 2026-09-16 — default-secure surfaces: a route says auth or public, a table says scoped_by or global, and neither is a default

Spec `docs/superpowers/specs/2026-09-16-default-secure-surfaces-design.md`, plan
`docs/superpowers/plans/2026-09-16-default-secure-surfaces.md`, eight tasks. M3 sub-project 2.
Workspace `.superpowers/sdd/2026-09-16-default-secure-surfaces/`.

**What landed.** `#[route]` gained `public`; `#[table]` gained `global`; each is a no-op in
expansion. A route with neither `auth` nor `public`, and a table with neither `scoped_by`
nor `global`, are compile errors naming both choices (GUIDE §5, §2, §9). The sweep added the
words at [N .rs lines from Task 3's diff stat] sites across `crates/`, `examples/`, GUIDE's
rust blocks, README and the `charpente new` template; the flip commit found [none / the
sites named in its commit message] the line-anchored sweep had missed. `describe`'s wire is
unchanged; `describe_contract` and the TS client did not move. DESIGN §3/§8 say declared, not
opt-in; DECISIONS §13 records the generalising ruling; the tenant-scoping spec's opt-in
non-goal carries its superseded line.

**The oracle, run literally.** `docs-only-helpdesk` (`8913038`) rebased onto [the flip
commit's sha] in a detached sibling worktree, `cargo check -p helpdesk`: 11 refusals — eight
tables and three routes — and the two Critical sites among them:

```text
[paste the `create_agent` error and its `--> examples/helpdesk/src/lib.rs:473` line]
[paste the `Ticket` error and its `--> examples/helpdesk/src/lib.rs:134` line]
```

The branch is untouched (`git rev-parse docs-only-helpdesk` = `8913038…`) and stays unmerged.

**Pins.** Six new compile-fail cases (`helpdesk_route_neither_auth_nor_public`,
`helpdesk_table_no_tenancy`, `route_auth_and_public`, `route_duplicate_public`,
`table_global_and_scoped_by`, `table_duplicate_global`), three compile-pass controls
(`route_public_flag`, `route_public_cross_tenant`, `table_global_with_version`), thirteen
existing `.stderr` re-generated for the shifted snippet and reviewed. Two `doc_truth` pinned
blocks added (floor 14 → 16).

**Mutation run** (`scripts/mutants.sh --file route.rs --file lib.rs --re …`, Task 7):
[caught] caught, [missed] missed, [timeout] timeouts, [unviable] unviable; no new entry in
`.cargo/mutants.toml`. [If a MISSED was found: name the pin written for it and its commit.]

**Workspace gate** on [final sha]: fmt clean, clippy clean, `cargo test --workspace
--no-fail-fast` [ok-count] of [total] `test result: ok`, zero FAILED.

**Open, deliberately.** `public` + `cross_tenant` stays expressible (DECISIONS §11); the
field-type second oracle is deferred to a real leak through a `global` table (spec §4); the
typed `&Principal` parameter waits for the third docs-only build to ask for it (spec §8). Next
is M3 sub-project 3, the third docs-only build, which now builds against a platform that
asks.
```

- [ ] **Step 3: Commit**

```bash
git add docs/LEDGER.md
git commit -m "ledger(default-secure): the oracle run, the pins, the mutants counts, and the workspace gate"
```

If work was on a `default-secure` branch: `git checkout main && git merge --no-ff default-secure -m "merge: default-secure surfaces — a route says auth or public, a table says scoped_by or global"`, re-run Step 1's triple on the merge result, and record its sha in a one-line ledger addendum as the publish-path entry did.
