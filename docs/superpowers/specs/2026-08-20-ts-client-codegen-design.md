# M2 sub-project — TS client codegen: design

Date: 2026-08-20. Noah-approved scope and architecture (this session), section by section.

## Context

`charpente describe` has emitted a machine-readable `Contract` since the M1
introspection sub-project — assembled by a runtime walk of the constructed `App`,
because "a static parse cannot see what `main` actually performs (conditional mounts,
config-driven schedules)". That contract carries *topology*: which routes exist, which
jobs, which tables, which cron schedules.

It carries no types. `RouteInfo` is `{ method, pattern, name, auth, stream }` — a route's
request body, response, and error types are discarded. `TableInfo` has no field types.
There is no `EventInfo` at all.

So the headline of this sub-project is misleading. Emitting TypeScript is the easy half;
the work is **giving the `Contract` a type system**. Everything below follows from that.

The realtime sub-project (2026-07-14) declared both WebSocket transport and TS codegen
explicit non-goals, scoping codegen as "its own later project, over routes + events
together". PLAN.md's M2 bullet bundles WebSockets and codegen into one line; this design
**splits them**. They are independent: a generated client targets the SSE surface that
shipped in M1, and WebSocket transport is a later swap behind the same client API.
Doing WS first would build a transport with no typed client to consume it.

## Why this is the thesis, not a convenience

MANIFESTO and CLAUDE.md put it plainly: compile-time contracts are the oracle for
business logic — "a schema change that breaks a query must be a compile error, not a
500". Inside Rust that guarantee already holds. It stops at the process boundary. Today
a Rust type change ships a silently incompatible API to every client, and the failure
surfaces as a runtime decode error in someone's browser.

This sub-project extends the existing guarantee across that boundary. The success
condition is mechanical: **change a Rust wire type, and a TypeScript build goes red.**

## Scope

In: a `TypeSchema` IR and a `types` registry on `Contract`; `#[derive(Wire)]`; type
capture in `#[route]`; a describe-time event registry behind `#[event]`; an `emits(...)`
clause on stream routes; a TS emitter producing types, route functions, and an event
client with its own fetch-based runtime; `charpente gen-client` and `charpente
check-client`; a `ts-fixture/` dogfood target; the serde-fidelity, brand-negative,
emits-drift and staleness oracles; DESIGN/GUIDE/PLAN updates.

Out (explicit non-goals): **WebSocket transport** (its own later project — this client
targets SSE, and WS is a transport swap behind the same API); typed query parameters
beyond what routes already parse; pagination helpers; retry/backoff in the client
runtime; any React or framework binding; codegen for languages other than TypeScript;
making `Contract` a published stable format; typed auth (see §3).

## §1 The type IR, and what `u64` / `Id<T>` become

The IR lives in `charpente::wire`, and `Contract` (in `charpente::describe`) gains
`types: Vec<TypeDef>` so slots hold references rather than inline structures — which is
what makes reuse and recursive types expressible:

```rust
enum TypeSchema {
    Bool, I8, I16, I32, U8, U16, U32, F32, F64, String,
    Int64 { signed: bool },         // ALWAYS string-encoded on the wire; see below
    Option(Box<TypeSchema>),        // `T | null`, and optional in a struct position
    Seq(Box<TypeSchema>),           // T[]
    Map(Box<TypeSchema>),           // Record<string, V>
    Struct { fields: Vec<Field> },  // Field.name is the SERDE name, never the Rust name
    Enum(EnumRepr),                 // unit variants -> string union; data -> serde tagging
    Id(&'static str),               // a typed row id; the str is the table name
    Ref(&'static str),              // into Contract.types
}
```

**Note what is not a plain primitive: 64-bit integers. That is the load-bearing decision.**

Ids are `u64` (`Id::<T>::from_raw(u64)`), and app code unwraps them at the view
boundary — `examples/invoicing`'s `CustomerView { pub id: u64 }` is built by
`c.id.raw()`. So ids cross the wire as raw JSON numbers today, and **any id above 2^53
already loses precision in every JavaScript client**. The hazard is live, not
hypothetical, and it is silent: the value arrives, type-checks, and is wrong.

The obvious repair does not work. Mapping `u64` to TS `bigint` and parsing with a
`JSON.parse` reviver is **unsound** — a reviver receives the already-parsed value, so
the precision is gone before it is ever called. Lossless 64-bit integers over JSON
require either the wire to carry a string, or a hand-written JSON tokenizer in the
client runtime. The tokenizer is too much machinery to carry for one integer type.

Therefore:

1. **`Id<T>` gains a serde impl that reads and writes a JSON string.** It has none
   today. In TypeScript it generates a *branded* type:
   ```ts
   type CustomerId = string & { readonly __table: "Customer" };
   ```
   This restores across the wire the guarantee DESIGN §3 makes inside Rust — "an
   `Id<Invoice>` is not an `Id<Org>`, and neither is a bare u64". Passing an
   `InvoiceId` where a `CustomerId` belongs becomes a `tsc` error.
2. **A bare `u64` in a wire position is a compile error.** Per hard rule 5 it names the
   contract, the site, and the fix:
   ```
   `CustomerView.id: u64` cannot cross the wire — values above 2^53 lose precision in
   JavaScript clients, silently. Use `Id<Customer>` for a row id, or `WireU64` to carry
   it as a string.
   ```
3. **`i64` is rejected on exactly the same terms as `u64`**, and gets the same
   diagnostic. The precision ceiling is 2^53 in both directions; treating the signed
   case as "usually fine" would be the unenforceable half-rule this design exists to
   remove. `WireU64` and `WireI64` are the sanctioned carriers.
4. `u8`/`u16`/`u32` and `i8`/`i16`/`i32` map to `number` and are safe by construction —
   their full range fits inside the safe-integer range, so no guard is needed.

**What the carriers generate.** `WireU64`/`WireI64` serialize as JSON strings and
generate TS `bigint`: because the wire carries a string, `BigInt(str)` is lossless, which
is the same round trip that was unsound via `JSON.parse`. `Id<T>` also serializes as a
string but generates a *branded string*, not a `bigint` — a row id is an opaque handle,
never an arithmetic value, and giving it a numeric type would invite arithmetic on it.

**Cost, stated rather than buried:** this breaks existing app code. `examples/invoicing`
uses `pub id: u64` in every view type and must change. That example is a
docs-provenance artifact ("agent-built from docs alone"), so the edit is not free —
but the alternative is shipping a client generator that faithfully reproduces a wire
bug. Noah accepted this trade when approving §1.

## §2 Capture: the derive, the macro, and a second event registry

**The seam.** `#[derive(Wire)]` generates `fn schema() -> TypeSchema` and
`fn type_name() -> &'static str`. The `#[route]` macro already classifies handler
parameters in order to generate dispatch; it reuses that classification to emit
registration code calling `<Body as Wire>::schema()` for each typed position — path
params, body, response, error.

So types resolve **statically** (the macro knows which types are involved) and are
collected **at runtime**, during the same `App` walk that already collects routes. One
mechanism, not two, and the introspection spec's runtime-walk decision is preserved
rather than relitigated. A missing `#[derive(Wire)]` fails the trait bound at the route
site — a compile error naming the handler, not a gap discovered at generation time.

**Events are not enumerable, and this is the design's second real problem.**
`Subscription::of::<Ev>()` executes *inside a handler body* at subscribe time, so the
`App` walk cannot observe it. The realtime spec deliberately refused a runtime event
registry: delivery encodes through "the encoder each `Subscription::of::<Ev>()` captured
statically at the subscribe site — there is no runtime event registry, so 'unregistered
event at delivery' is inexpressible, not an error case."

That property must not be broken to make events describable. The resolution is that
these are two different registries for two different purposes:

- **Delivery** keeps the statically captured encoder. Unchanged. "Unregistered event at
  delivery" remains inexpressible.
- **Describe** gains a registry populated by `#[event]` at `App` construction, exactly
  as `#[table]` already populates schema. It is read by `App::describe` and by nothing
  on the delivery path.

`Contract` gains `events: Vec<EventInfo { name, payload, keyed: bool }>`.

**Correction (task 6 review, Ruling 37):** this line first said `payload: TypeRef`.
`TypeRef` is a spec-level notion with no Rust type behind it — task 5 realized it two
ways, by position: `TypeSchema` where a route's request/response/error needs a full
shape, and a bare `&'static str` wire NAME where a name is all a consumer needs
(`Route.response_name`). `EventInfo.payload` is the second kind and shipped as
`&'static str` (`Wire::WIRE_NAME`, an entry in `Contract.types`). It sits right beside
`RouteInfo.emits: Vec<&'static str>` in the rendered contract, and the two have the
same Rust type. The code is correct; this sentence was stale — same discharge as
Ruling 10, where the IR shipped in `charpente::wire` rather than `charpente::describe`.

**Correction (task 7 review, Ruling 39):** the sentence above originally continued
"…matching `RouteInfo.emits`, which task 7 fills with exactly this kind of value". That
prediction was wrong, and it contradicted amendment B1, which task 7 implemented. The
two fields carry the same Rust type but NOT the same kind of value:

- `EventInfo.payload` is `Wire::WIRE_NAME` — `"WidgetAdded"` — and IS a key into
  `Contract.types`.
- `RouteInfo.emits[i]` is `<Ev as Event>::NAME` — `"widget_added"` — the name an SSE
  frame's `event:` field carries. It is **not** a key into `Contract.types`.

`Event::NAME` is correct and stays. §7's acceptance criterion 6 is "an `emits(...)`
declaration that disagrees with its handler's `Subscription` fails", and a
`Subscription`'s entries hold `Event::NAME`; filling `emits` with the same value is what
makes that criterion a direct comparison rather than one with a lookup inside it. It is
also the value a client must switch on, since it is what arrives on the wire. And
because both sides of the oracle now derive from `Ev::NAME`, the check detects
**type-set** drift while name drift becomes inexpressible rather than merely detected.

**Ruling 40 (task 7 review) — the join is Task 9's to design.** The emitter must resolve
every `emits` entry to a payload type. Nothing here prescribes how. The constraint it has
to satisfy: a stream route may declare an event the app never passes to `mount_events`,
and that is a supported state, not a mistake — task 7 registers a declared payload into
`Contract.types` from the ROUTE, and
`crates/charpente/tests/describe.rs::a_declared_emits_payload_reaches_the_closure_without_mount_events`
pins it. So a resolution that only works for mounted events is insufficient. Task 9
picks the mechanism with sight of the emitter.

**Binding events to streams.** `RouteInfo.stream` is a bare bool; nothing records which
events a given stream carries, so a generated client could only offer "subscribe to
anything". Stream routes therefore declare it:

```rust
#[route(SSE "/feed", emits(InvoicePaid, InvoiceVoided))]
```

which lets the emitted client give `feed.subscribe()` a discriminated union of exactly
those two payloads. The risk is drift — the declaration says one thing, the handler's
`Subscription::of::<>()` another. That is handled as an oracle rather than a rule; see
§5.

## §3 The generated client

Emitted: every `TypeDef` as a TS type (ids branded per §1); one function per route,
named from `RouteInfo.name`; one subscribe function per stream route; and a small
runtime we own.

**Route calls return a discriminated union, not a thrown error.**

```ts
const r = await api.createCustomer(id, body);
if (!r.ok) { /* r.error : { error: string } — the ApiError envelope, not a per-variant union */ }
```

**Correction (task 5 fix round 1, F1):** `r.error` is the fixed `ApiError` envelope
`{ error: string }`, not a discriminated union of `ApiError`'s Rust variants.
`ApiError::response()` always emits that one shape regardless of which variant a
handler returned — the variant distinction never reaches the wire — so `Contract.types["ApiError"]`
describes the envelope (a hand-written `Wire` impl, not the derive), and this emitter
must type every route's error branch from it. The status code (which the fetch response
already carries) is what discriminates 400/401/404/500 for a caller that needs to, not
the error body's shape.

The route's error type is still part of its declared Rust signature, so the ENVELOPE
belongs in the return type even though it is not itself a per-variant type — a caller
still cannot silently ignore a failure the server declares. Throwing would erase it
entirely — `catch` binds `unknown`, discarding even the message. The cost is verbosity
at every call site; the purchase is that `tsc` refuses to let a caller skip past `!r.ok`
before reading `r.error`. Transport-level faults (socket died, body did not parse) still
throw: those are not in the contract.

**The client runtime uses fetch-based SSE, not `EventSource`.** `EventSource` is
browser-native and handles `Last-Event-ID` resume for free, but **it cannot set request
headers** — so every `auth` stream route would have to put its token in a query string,
where it lands in logs and referrer headers. A fetch-based reader sets headers properly
and manages `Last-Event-ID` itself. The cost is that we lose the browser's automatic
reconnect and therefore owe it explicitly.

**The honesty frames must not be swallowed.** DESIGN §6 defines two, and both exist to
prevent silent wrongness:

- a terminal `event: charpente.gap` frame when a tail falls behind retention;
- a 409 at subscribe naming either the retention floor or a **cross-timeline
  divergence** (the case the cursor's seq half was added to detect).

A client that treated these as "connection dropped, reconnect" would reconnect straight
past a divergence — converting a deliberate, named refusal into exactly the silent
misattribution §6 was built to make impossible. Both surface as distinct typed
conditions the caller must handle, and the reconnect path **must refuse to auto-resume
across a divergence**.

**Auth** is a token held by the client, supplied at construction. `RouteInfo.auth` is in
the contract, so making auth routes type-require a session is available as a later
tightening; it is out of scope here.

## §4 The gate

- `charpente gen-client [--dir PATH] [--out PATH]` — builds the app, runs the existing
  describe path, emits TypeScript to `--out`.
- `charpente check-client [--dir PATH]` — regenerates to a temp path, compares against
  the committed file, and fails with a diff if they differ.

Exit codes follow the existing CLI convention: 0 green, 1 failure, 2 usage/launch error
(the same shape as `charpente test --sim`).

**Generation must be byte-deterministic**, or the gate flaps and people learn to ignore
a red check — which is worse than having no check. Everything is sorted by name; no
timestamps; no version string in the emitted body; no `HashMap` iteration order anywhere
in the emitter. This is the same discipline the pinned fixtures already carry.

The generated client is **checked in**, so a schema change appears in review as changed
client code rather than as drift someone forgets to regenerate.

**Cost to weigh:** this puts Node in CI. GHA minutes are limited here, which is why the
documented workflow is local-first verification with a push after green. The job is
modest (`check-client` plus `tsc --noEmit`) but it is a new toolchain in the pipeline.

## §5 Testing and the oracles

**Serde fidelity — the oracle that carries the design.** The wire shape is decided by
serde, not by the Rust struct. If the IR does not reproduce `#[serde(rename_all)]`, a
tagged enum, `flatten` or `skip_serializing_if` exactly, the generated TypeScript is
wrong *and still type-checks*, which is the worst failure available here.

This is not proven, it is **tested, at compile time**. For each `Wire` type, a sample
value is serialized through `serde_json` and emitted as a TypeScript literal in a
generated test file:

```ts
const _customerView = { id: "42", name: "a", email: "b" } satisfies CustomerView;
```

The literal is the real wire shape, because serde produced it. If a type carries
`rename_all = "camelCase"` and the IR missed it, the literal says `unitAmountCents`
while the generated type says `unit_amount_cents`, and `tsc` fails. No runtime
validator, no TS-side parsing machinery, and it runs inside the `tsc --noEmit` the
fixture needs anyway.

The rest:

- **Brand negatives** — `@ts-expect-error` assertions that passing an `InvoiceId` where
  a `CustomerId` belongs does **not** compile. Without these, §1's branding is
  decorative.
- **Emits drift** — a test asserting a stream route's declared `emits(...)` set equals
  the set its handler's `Subscription` actually names (§2).
- **Staleness** — `check-client` in CI (§4).
- **The dogfood target** — `ts-fixture/`: package.json, tsconfig, the checked-in
  generated client, a handful of real call sites, one event subscription. Deliberately
  small, in the spirit of `crates/sim-fixture`, which exists to pin macro-path expansion
  rather than to be an application.

**No sim coverage, and that is correct rather than an omission.** None of this is in the
deterministic core: the emitter is a pure function of the `Contract`, and the client
runtime lives in the browser. `describe`'s own determinism — a fresh store yielding an
identical contract — is already a property of the introspection sub-project.

## §6 What changes where

| file | change |
|---|---|
| `crates/charpente/src/wire.rs` **(new)** | `TypeSchema`, `TypeDef`, `Field`, `EnumRepr`, `TypeRegistry`, the `Wire` trait and its primitive impls |
| `crates/charpente/src/describe.rs` | `Contract.types`; `Contract.events`; `RouteInfo` gains request/response/error `TypeSchema`s and `emits`. Stays the assembly point only. |
| `crates/charpente-core/src/table.rs` | `Id<T>`: serde impls (string-encoded) |
| `crates/charpente-macros` | `#[derive(Wire)]`; `#[route]` type capture + `emits(...)`; `#[event]` describe-time registration; the bare-`u64` diagnostic |
| new codegen crate | `Contract` -> TypeScript emitter (pure, deterministic) |
| `crates/charpente-cli` | `gen-client`, `check-client` |
| new `ts-fixture/` | dogfood target + generated tests |
| `examples/invoicing` | view types move off bare `u64` (§1) |
| `DESIGN.md` §3, §7 | the typed surface crosses the process boundary; describe carries types |
| `docs/GUIDE.md` | client generation workflow |
| `PLAN.md` | split the M2 "WebSockets + TS client codegen" bullet in two |

## §7 Exit criteria

1. `charpente gen-client` against `ts-fixture/` produces a client whose `tsc --noEmit`
   is clean, including the generated serde-fidelity literals and the brand negatives.
2. Changing a Rust wire type — renaming a field, changing `u32` to `String`, adding a
   required field — makes that `tsc` run **fail**, demonstrated per case, not argued.
3. `check-client` fails on a stale committed client and passes on a fresh one.
4. Generation is byte-identical across repeated runs and across machines (no map
   ordering, no timestamps).
5. A bare `u64` **and** a bare `i64` in a wire position each fail to compile, with the
   §1 message pinned by a test against its text (hard rule 5). `WireU64`/`WireI64`
   round-trip a value above 2^53 losslessly, proven by a value that would corrupt as a
   JSON number.
6. An `emits(...)` declaration that disagrees with its handler's `Subscription` fails.
7. `examples/invoicing` builds and its demo still passes after the §1 migration.

## §8 Non-goals, restated so they stay refused

WebSocket transport (separate project; this client targets SSE). Framework bindings.
Typed query parameters beyond current parsing. Client-side retry, backoff, or caching.
Codegen for other languages — though the `TypeSchema` IR is deliberately language-neutral,
nothing here promises a second backend. A stable, published `Contract` format:
`Contract` remains an internal surface that may change shape freely.
