# Job tenancy — design

**Date:** 2026-09-27. **Status:** approach D approved in chat (Noah, 2026-09-27); the written
spec, §14 included, approved 2026-09-27; spec of record for the plan
`docs/superpowers/plans/2026-09-27-job-tenancy.md`.
**Predecessor:** M3 sub-project 3's verdict (PASS, fix before publish; record
`docs/experiments/2026-09-statuspage/`). This is the second of the two pre-publish platform
fixes, PLAN.md M3 item 3 (2): "a job cannot open a tenant scope the way GUIDE §16 documents,
and `authorize` sees neither the operation nor the caller kind". The first, C-1, shipped as
`2026-09-24-payload-shapes-design.md`. Every citation was re-verified against `main` at
`6354cd8`; the design research behind this spec was not committed and nothing depends on it.

## Context

`Tenant::authorize(tx, who: Option<&Principal>, scope)` says it answers "may this caller act
as this tenant?" (`charpente-core/src/db.rs:1249-1250`). In practice it answers for three
kinds of caller. `None` means an anonymous request *and* any direct
`tx.open_scope(scope, None)` (`db.rs:1276-1279`). A job has no `Ctx`
(`kernel/dispatch.rs:83`), so `open_scope(org, None)` is its only route to a scoped row. An
app whose jobs write tenant data must therefore admit `None`, and that admits every
anonymous caller on every `public` route. GUIDE §16's remedy for code outside a handler, "go
through a transaction and `tx.scope::<Org>(org, ctx)?`", names a `Ctx` that no job and no
operator code holds (`http/app.rs:79-94`, `:812`). DECISIONS §12 and
`charpente/src/tenant.rs:30-32` repeat it.

**The statuspage shipped exactly that** (`../statuspage-run/statuspage@6037704`).
`Org::authorize` returns `Ok(())` for `None` unconditionally (`src/lib.rs:42-45`), because
three public operations (`public.rs:41`, `:90`, `:127`) and both jobs (`notify.rs:24`,
`maintenance.rs:149`) open their scope with the undocumented `open_scope(org, None)`. The
review (`attempt-1/review.md`) found two consequences. I-1: flipping all seven write routes
to `public` leaves the sim batch byte-identical, and anonymous callers can then resolve
incidents. C-2 edit (b): rewriting four `auth` write routes to use `open_scope(org, None)`
also leaves it byte-identical, while alpha's key writes beta's data
(`attempt-1/probes/takeover.rs`).

`who` is also forgeable. `Principal`'s fields are `pub` (`auth/ladder.rs:26-29`), so a
handler can pass `Some(&forged)`. DECISIONS §12 seals `CrossTenant` against handlers, but
nothing seals `who`.

**Two independent builds needed the same two job shapes.** P1 is a follow-up in the
enqueuer's tenant: the statuspage's `NotifySubscribers`, the helpdesk's `SendSurvey`
(`docs-only-helpdesk@8913038`), and README's own example (`README.md:47-54`), which enqueues
`SendReceipt` inside `tx.scope` and is correct code only if the job inherits that scope. P2
is a cron sweep that reads every tenant and fans out writes: `MaintenanceSweep` →
`ApplyWindow`, and `HourlyTick`. `cross_tenant` is read-only (`db.rs:1508-1516`), so P2 has
no declared way to write.

## §1 Decisions already made

- **2026-09-24 (Noah, verdict ruling):** fix C-2 before the 0.1.0 publish.
- **2026-09-27 (Noah): approach D.** `authorize` is only ever asked about a real,
  unforgeable principal. Every other way into a tenant is a declaration on the entry point,
  which `describe` lists. A scoped job carries the tenant it was enqueued from.
- **Sequencing.** The plan starts from a `main` that contains the 2026-09-27 pre-publish
  follow-ups (`docs/superpowers/plans/2026-09-27-prepublish-followups.md`). That plan's Task
  2 changes `fingerprint.rs` and its Task 3 changes `dispatch.rs`, and this spec changes
  both files too.

## §2 Rulings

1. **D, not B ("tell `authorize` everything").** *Basis:* DECISIONS §12's "a check in name
   only", and DESIGN §8's "*which tenant*, never *what may this principal do*". Under B, a
   job's admission would be app code (`Entry::Job => Ok(())`), yet a job has no request and
   no principal: its authority came from whoever enqueued it. Handing `authorize` the
   operation also invites per-operation policy, which is permissions. So `authorize` is not
   told the operation. PLAN's "sees neither the operation nor the caller kind" is answered by
   removing the need rather than by supplying either. `authorize` sees only principals, and
   the route's declared stance is the only operation-level granularity tenancy owns.
2. **`authorize(tx, who: &Principal, scope)`; `Principal`'s fields become private** (§3).
   This is a smaller migration than a witness type that only `Ctx` mints. Every
   `impl Tenant` keeps `Who = Principal` and changes only `Option<&Principal>` to
   `&Principal`, and `TenantScope`'s bound is untouched (only its note's text changes, §3). Every construction already lives inside
   `charpente` (`ladder.rs:97`, `:112`, `:119`; `http/app.rs:832`), so the fields become
   `pub(crate)` and the forgery is removed at its source. The seal holds because
   `TenantScope` fixes `Who = Principal`, and every `#[table(scoped_by)]`,
   `#[job(scoped_by)]` and `public(S)` const-asserts `TenantScope`. Core's `Tenant` stays
   generic over `Who`.
3. **A route's stance is a required choice among `auth | public | public(Org)`**, reusing
   the flag grammar `emits(...)` has. `public(Org)` admits every caller, credentialed ones
   included, into an `Org` scope by declaration, with no `authorize` call. *Basis:* DECISIONS
   §11, "the declaration is the authorization"; the statuspage needed another org's operator
   to read a public page (`attempt-1/FINDINGS.md` L76-81). On plain `public`, a credentialed
   caller at a scope line runs `authorize`, and an anonymous caller gets §4's named 401.
   **This complies with DECISIONS §13**, which asks for "a required choice between named
   alternatives, with a compile-fail pin for the absent case". The stance is still required,
   and its absence is still a compile error, which now names all three answers. Plain
   `public` is a written answer, not a quiet default: anonymous callers, no tenant by
   declaration. A handler that contradicts it fails closed, by name, at the scope line, and
   `describe` lists `public(Org)`.
4. **Jobs declare `#[job(scoped_by = Org)]` or `#[job(global)]`; neither is a default**
   (§5). *Basis:* DECISIONS §13's "What this binds" names `#[job]`, and the bare case gets a
   compile-fail pin. A scoped job is enqueued only from inside its tenant, or by a declared
   fan-out, and its `run` receives the `ScopedTx` it was enqueued from. Cron accepts only
   `global` jobs, because a cron tick has no tenant.
5. **Fan-out rides `cross_tenant`:** `tx.enqueue_in::<S>(org, job, cap)`. *Basis:* DECISIONS
   §11's "declared, enumerable, tenant visible". One inventory, `cross_tenant=true`, keeps
   answering "what reaches every tenant here". Before 0.1.0 no external app's declaration
   silently widens. GUIDE's "the cross-tenant handle is read-only" becomes "reads, plus
   fan-out enqueue". `AllTenants` stays read-only.
6. **The operator opens a writable scope with a `Group`-minted `TenantAdmission`,** sealed as
   `CrossTenant` is (§7). *Basis:* DECISIONS §12, where the `Db` holder is the operator.
   §12's "What an app is told" remedy is corrected, along with its twins in `tenant.rs` and
   GUIDE §16. The DECISIONS half is a prose correction and is listed in §14. The §12 ruling
   itself is unchanged.
7. **The tenant rides as a `JobRow` column on a new record tag (β), not as a payload prefix
   (α)** (§6). Both keep one log and one commit point, because the scope is part of the same
   enqueue effect in the same record. β wins three ways. *Precedent:* tenant scoping carries
   a scope as an explicit record field on tags the declaration chooses (`0x05`-`0x07`,
   presence-not-value), and its spec §2 named reading an id out of a prefix of opaque bytes
   (`row_id_prefix`) as a sharp edge. *Mutation coverage:* under β the dispatcher reads the
   tenant from a typed column, in source cargo-mutants mutates. Under α it is parsed either
   in macro-emitted glue, which cargo-mutants never mutates, or out of a field that
   everywhere else means "the app's `Codec` bytes". *Timing:* what α saves (a tag, a
   `JobRow` field, a `PROTO_VERSION` bump) is cheapest before 0.1.0, when no deployed
   follower or archive can be stranded, and C-1 paid exactly this three days ago. Both need
   §6's fingerprint mixing, so either way a changed tenancy is refused by name, never
   misdecoded. **Cost if wrong:** 8 bytes per scoped `JobRow`, one tag and one protocol
   bump, carried for nothing, none of which can misdecode. The opposite error, α and then
   needing the tenant outside the glue after 0.1.0, costs a record-format change plus a
   drain-before-upgrade on deployed nodes.
8. **`describe` lists scope type names, not booleans,** for routes (`public_scope`) and jobs
   (`scoped_by`). *Basis:* DECISIONS §13 wants an exact inventory, and a multi-scope app
   must see which scope a route admits strangers to. `TableInfo` is unchanged.
9. **Out of scope, and staying out:** the membership table's voluntary scope line (the
   review's C-2 edit (a), which `docs/LEDGER.md` 2026-09-24 ruled "no claim, an agent bug —
   a one-sided property"); permissions; and the louder `App::mount` notch, whose DECISIONS
   §11/§12 trigger has not fired. This spec makes the inventory more complete, not less.
10. **Tenant-scoping ruling 3 ("the platform never interprets the scoping key") reads: the
    platform carries an app-named `Id<S>` and hands it back as the same type, and never
    derives one.** *Precedent:* `AllTenants::scan` (`db.rs:1537-1540`). A job's tenant is
    named at the enqueuer's `tx.scope` line or in `enqueue_in`'s argument, and is restored as
    that `Id<S>`. Nothing maps a principal, header or field to a tenant.

## §3 `authorize` and the principal

```rust
// charpente-core::db
pub trait Tenant: Sized + 'static {
    type Who: ?Sized;
    type Error;
    fn authorize(tx: &Tx<'_>, who: &Self::Who, scope: Id<Self>) -> Result<(), Self::Error>;
}
impl<'a> Tx<'a> {
    pub fn open_scope<S: Tenant>(&mut self, scope: Id<S>, who: &S::Who)
        -> Result<ScopedTx<'_, 'a, S>, S::Error>;                       // runs authorize
    pub fn admit_scope<S: Tenant>(&mut self, scope: Id<S>, admission: &TenantAdmission)
        -> ScopedTx<'_, 'a, S>;                                          // runs none
}
#[derive(Clone)]
pub struct TenantAdmission(());               // only constructor: Group::tenant_admission
impl Group<'_> { pub fn tenant_admission(&self) -> TenantAdmission; }
// charpente::auth — fields become pub(crate)
impl Principal { pub fn subject(&self) -> &str; pub fn via(&self) -> Via; }
```

Both constructors share one private body. It checks the one-scope contract first, with the
existing text. A `TenantAdmission` proves that a declaration or the operator admitted this
transaction. It has exactly three holders: the operator (through a `Group`), the platform's
`Ctx` on a `public(S)` route (private, never exposed), and the dispatcher.

`TenantScope`'s note changes in two fragments only. Its opening becomes "a tenant scope needs
the app's own membership check; add:", and its signature line becomes
`fn authorize(tx: &Tx, who: &Principal, scope: Id<Self>) -> Result<(), ApiError> { … }`.

## §4 Routes: `auth | public | public(Org)`

**Parse.** `public` may carry one parenthesised type path. `Route` gains
`pub public_scope: Option<PublicScope>`, where `PublicScope { pub name: &'static str, pub
type_id: fn() -> TypeId }` and `name` is the path's last segment, as the table refusals
render it. The macro emits `#[table]`'s `TenantScope` const assert, so `public(Foo)` with
no `impl Tenant for Foo` gets §3's diagnostic. `auth` with `public(Org)` gets the existing
one-or-the-other refusal; `public` twice, in any form, the existing given-twice refusal;
`undeclared_capability`'s stance renders `public(Org)`. New and changed texts:

```text
`create_agent` declares neither `auth` nor `public` — a route says which callers reach it: write `#[route(POST "/agents/:id", auth)]` to require a principal (`ctx.principal()` is `Some`), `#[route(POST "/agents/:id", public)]` to serve anonymous callers, or `#[route(POST "/agents/:id", public(Org))]` to also let every caller act inside the `Org` tenant it opens. None is the default.
a route flag is `auth`, `public`, `public(<Scope>)`, `cross_tenant` or `emits(Ev, …)` — write `#[route(GET "/path", auth)]`, `#[route(GET "/path", public)]`, `#[route(GET "/orgs/:org/status", public(Org))]`, or `#[route(SSE "/path", emits(NoteAdded, NoteRemoved))]`
`public(...)` names exactly one tenant scope type — write `public(Org)`, or plain `public` if anonymous callers enter no tenant here
write `#[route(METHOD "/path", auth | public | public(<Scope>))]` — e.g. #[route(GET "/notes/:id", auth)]
```

**Semantics.** `stage_or_subscribe` mints `group.tenant_admission()` for a route that
declared `public(S)`, and only for one. It does so beside the `CrossTenant` it already mints
(`http/app.rs:733`). `build_ctx` stores three things privately in `Ctx`: that admission,
`S`'s `TypeId`, and the route's site (the method word, `SSE` for a stream, plus the
pattern). `Scoped::scope` then applies three rules, in order: (1) if the route declared
`public(S)` for this `S`, it calls `admit_scope` and no `authorize` runs; (2) otherwise, with
a principal present, it calls `open_scope(scope, principal)` and `authorize` runs once; (3)
otherwise it returns `ApiError::AnonymousScope(text)`, a new variant answered as **401**, and
nothing below the scope line runs. An `auth` route never reaches rule 3, because
`build_ctx` refuses the anonymous caller first (`http/app.rs:828-830`). A `public(Org)` route that opens a different scope type falls
through to rule 2 or 3, and the text then names `public(Org)` as its stance. The text names
the contract, the site, and both fixes, the caller's and the author's:

```text
authentication required — `POST /orgs/:org/incidents` is declared `public` and opened the tenant scope `Org` for a caller with no credential; only a route declared `public(Org)` admits one. Send "Authorization: Bearer <token>" or the charpente_session cookie, or, if every caller may act inside this tenant here, declare `#[route(POST "/orgs/:org/incidents", public(Org))]`
```

The scope is `S`'s type name cut to its last segment, as `decode_body` renders a type. The
`emits(...)` drift probe honours `public(S)` exactly as it honours `cross_tenant`
(`http/app.rs:551-558`). On any other route, the probe's credential-free `Ctx` meets rule 3,
which the probe already reports as not probeable. The TypeScript generator reads only
`RouteInfo.auth`, so `client.ts` does not change.

## §5 Jobs: `global | scoped_by = Org`

**Parse.** A job takes exactly one of `global` or `scoped_by = <Path>`, plus optional
`cross_tenant` and `retry(...)`. The words may come in any order, each at most once (the
table parser's precedent). `scoped_by` emits `type Scope = Org` and the `TenantScope` const
assert; `global` emits `type Scope = Unscoped`. `scoped_by` composes with `cross_tenant`.

```text
`NotifySubscribers` declares no tenancy — a job says which: write `#[job(scoped_by = Org)]` if it acts inside one tenant (it is enqueued from inside that tenant's scope, and its `run` receives that tenant's `ScopedTx`), or `#[job(global)]` if it belongs to no tenant (the only kind cron can schedule). Neither is the default.
`global` and `scoped_by = Org` on one job — a job is one or the other; drop one
`global` given twice — write it once
`scoped_by` given twice — write it once
`retry(...)` given twice — write it once
a job declares `global` or `scoped_by = <Type>`, and may add `cross_tenant` and `retry(max = <int>, backoff = "<n><unit>")` (unit is one of ms, s, m, h), in any order — e.g. `#[job(scoped_by = Org, retry(max = 3, backoff = "10s"))]` or `#[job(global, cross_tenant)]`
```

The first refusal spans the attribute (`Span::call_site()`), as `#[table]`'s does. The last
replaces `ACCEPTED_FORMS`. That also retires GUIDE §9's `#[job]` block, which has already
drifted: it omits `cross_tenant`, which `job.rs:16-18` accepts.

**Bounds.** `Job` gains `type Scope: 'static` with no default. The 14 hand-written
`impl Job` blocks in Rust, and GUIDE's one, state `type Scope = Unscoped`. Beside
`NoTenant`/`UnscopedRow` (`table.rs:340-407`) sits the same sealed pair for jobs, for the
same reason: the diagnostic must hang on a trait the scope type either implements or does
not.

```rust
pub trait NoJobTenant<J>: sealed::NotATenant {}      // impl only for Unscoped
pub trait UnscopedJob: Job + sealed::NotScopedJob {} // J::Scope: NoJobTenant<J>
impl Tx<'_> {
    pub fn enqueue<J: UnscopedJob>(&mut self, job: J);
    pub fn enqueue_keyed<J: UnscopedJob>(&mut self, key: impl Into<String>, job: J);
    pub fn enqueue_in<S: Tenant, J: Job<Scope = S>>(&mut self, scope: Id<S>, job: J, cap: &CrossTenant);
}
impl<S: 'static> ScopedTx<'_, '_, S> {  // shadows Tx::enqueue*, as ScopedTx::emit shadows emit
    pub fn enqueue<J: Job<Scope = S>>(&mut self, job: J);
    pub fn enqueue_keyed<J: Job<Scope = S>>(&mut self, key: impl Into<String>, job: J);
}
// charpente: App::schedule<J: UnscopedJob + 'static>(…)
```

```text
error[E0277]: `NotifySubscribers` is a tenant-scoped job (`#[job(scoped_by = Org)]`) and a bare `Tx` names no tenant to run it in
   = note: enqueue it from inside the tenant it acts in, and it runs in that tenant:
               let mut tx = tx.scope::<Org>(scope, ctx)?;
               tx.enqueue(job);
           or fan it out from a `cross_tenant` route or job, naming the tenant:
               tx.enqueue_in::<Org>(scope, job, cap);
           cron (`App::schedule`) enqueues on a bare `Tx`, so it schedules `global` jobs only
```

The label is "`NotifySubscribers` is scoped by `Org`". A `global` job enqueued inside a scope
is written `(*tx).enqueue(job)`, the explicit-deref idiom GUIDE §16 already teaches. rustc's
own E0271 for the undereferenced call is pinned so GUIDE can show it. A job has no `ctx`, so
the table marker's note gains a job case:

```text
= note: open the tenant scope first — in a handler, `Org`'s own `Tenant::authorize` runs there for a credentialed caller:
            let tx = tx.scope::<Org>(scope, ctx)?;
            tx.scan::<Invoice>()?
        in a job, declare `#[job(scoped_by = Org)]` and read through the `ScopedTx` its `run` receives
```

**Dispatch.** `JobDef` gains `pub scoped_by: Option<&'static str>`, and `JobGlue` becomes
`fn(&mut Tx, &[u8], Option<&CrossTenant>, Option<JobScope<'_>>) -> Result<(), JobError>`.
`charpente::kernel::JobScope` holds the carried id and a `&TenantAdmission`, both private.
Only the dispatcher constructs one, and its one method,
`open<S: TenantScope>(self, tx: &'t mut Tx<'a>) -> ScopedTx<'t, 'a, S>`, opens that id. So a
glue, generated or hand-written, can open only its enqueuer's tenant. `run_due_into` mints
`group.tenant_admission()` beside `cap` (`dispatch.rs:201-203`). After the shape assert
(`:229-238`) it asserts `row.scope.is_some() == def.scoped_by.is_some()`, which is hard rule
4: §6's fingerprint mixing makes a mismatch unreachable. A scoped job's glue decodes as
today, opens its `JobScope`, and calls `run` through a fn-pointer binding,
`let run: fn(Job, &mut ScopedTx<'_, '_, Org>) -> Result<(), JobError> = Job::run;`. A method
call would deref-coerce `&mut ScopedTx` to `&mut Tx` and let a scoped job whose `run` takes
a bare `Tx` compile; the binding makes that a pinned compile error. `global` glue is
unchanged. `SessionSweep` (`auth/sweep.rs:30`) becomes `#[job(global)]` and keeps its
fingerprint, per DESIGN §6's rename rule.

## §6 How the tenant rides with the job

**The record.** Tag `0x0a` carries `[job][payload][key: Option<String>][shape: u64 le]
[scope: u64 le]`. The job's `Job::Scope` chooses it: `ScopedTx::enqueue*` and `enqueue_in`
write it, and `Tx::enqueue*` cannot. The scope's value never chooses it, because tenant 0 is
a real tenant. `Effect::Enqueue` gains `scope: Option<u64>`, with the presence-not-value
convention of `Put`/`Delete`. An older binary refuses `0x0a` by name through
`decode_record`'s unknown-tag error.

**The row.** `JobRow` gains a trailing `scope: Option<u64>`, encoded after `shape` and only
when `Some`. Apply fills it from the record, so followers, replay and replication's
fingerprint oracle see identical rows. A scoped row always has a shape, so the tail after
`last_error` is 0, 8 or 16 bytes. Any other length is a named error, and the existing pinned
text becomes:

```text
row decode: 3 bytes after a JobRow's last_error — the shape and the tenant scope after it are 8 bytes each, present or absent, so the stored row is corrupt
```

**The protocol.** `PROTO_VERSION` goes from 3 to 4, on C-1's amendment A3 reasoning. A
version-3 follower would refuse `0x0a` at apply, by name; the bump moves that refusal to
`Hello`, before any frame ships.

**The fingerprint.** A `global` job's is exactly the fold a bare `#[job]` computes on the
base commit, so every existing job keeps its shape, `SessionSweep` included. A `scoped_by`
job's is that fold followed by `#scoped_by\0<scope path tokens>\0`, rendered by the field
renderer; no identifier can begin with `#`, so no field list collides with it. `cross_tenant`
is not mixed in, because it changes neither the stored bytes nor decode. A tenancy change is
therefore a shape change, which the shipped refusals catch by name at startup, at promote and
at enqueue. The startup refusal's fix clause gains the tenancy half:

```text
… — this binary will not decode them; run the binary that enqueued them until they are Done or Dead, then deploy this one, or restore SendReceipt's old fields and its `global`/`scoped_by` declaration and give the new shape its own #[job] type under a new name (GUIDE §6)
```

## §7 The operator path

Some operator code must act inside one tenant: seeding, a test oracle, an admin step in
`main`. It calls `db.begin_group()`, mints `group.tenant_admission()`, opens the scope inside
`group.stage` with `tx.admit_scope::<Org>(org, &admission)`, and then calls `commit`. This
follows the precedent `CrossTenant` set for operators (`charpente/tests/tenant.rs:979-984`).
No `authorize` runs, because the operator has no principal to ask about and is already
trusted (DECISIONS §12). There is no `Db`-minted token and no admitting `Db::transact`, so
the seal has exactly one constructor. The eight platform-test sites that call
`open_scope(…, None)` today move to this form.

## §8 `describe`

`RouteInfo` gains `public_scope: Option<&'static str>`, and `JobInfo` gains
`scoped_by: Option<&'static str>`. In JSON each is a name or `null`. The human dump follows
`fingerprint=none`'s precedent:

```text
  GET /orgs/:org/status name=status auth=false public_scope=Org cross_tenant=false stream=false
  NotifySubscribers max=5 backoff_nanos=30000000000 scoped_by=Org cross_tenant=false builtin=false
```

One dump answers the audit: `public_scope=none` on an `auth=false` route is plain public,
`public_scope=Org` admits every caller to `Org`, `cross_tenant=true` reaches every tenant (by
reads and by fan-out), and `scoped_by` names the tenant a job acts in. `TableInfo` does not
change. `genclient`'s mirror structs and `charpente-tsgen`'s test literals gain the fields,
which the compiler forces. `ts-fixture-app`'s staleness diff does not move.

## §9 Oracles

Red before green (hard rule 3): each behavioural test is seen failing against the base
commit, and that failure is recorded.

1. **Compile-fail pins** (`crates/charpente-macros/tests/compile_fail/`, `.stderr` pinned):

   | Fixture | Pins |
   |---|---|
   | `statuspage_job_no_tenancy.rs` | `NotifySubscribers` verbatim, citing `6037704:src/notify.rs:10-17` |
   | `open_scope_without_a_principal.rs` | C-2 edit (b): `open_scope::<Org>(org, None)` in a handler |
   | `principal_cannot_be_forged.rs`, `tenant_admission_cannot_be_forged.rs` | forging either from app code |
   | `job_bare.rs` | bare `#[job]`, and `#[job(retry(...))]` with no stance |
   | `job_global_and_scoped_by.rs`, `job_duplicate_global.rs`, `job_duplicate_scoped_by.rs` | one or the other; given twice |
   | `job_scoped_by_missing_tenant_scope.rs` | `scoped_by = Org` with no `impl Tenant` |
   | `scoped_job_enqueued_on_bare_tx.rs`, `scoped_job_scheduled_by_cron.rs` | the `NoJobTenant` note, at both sites |
   | `scoped_tx_enqueues_another_scopes_job.rs`, `scoped_tx_enqueue_of_a_global_job.rs` | the two E0271s |
   | `enqueue_in_without_capability.rs` | fan-out from an undeclared route |
   | `scoped_job_run_takes_bare_tx.rs` | the fn-pointer binding |
   | `route_public_scope_arity.rs`, `route_public_scope_missing_tenant_scope.rs` | `public()` / `public(A, B)`; `public(Foo)` with no `impl Tenant` |

   Re-pinned for changed text: `helpdesk_route_neither_auth_nor_public`,
   `route_unknown_flag`, `route_missing_method`, `job_bad_retry`,
   `scoped_read_from_bare_tx`, `table_scoped_by_missing_tenant_scope` and
   `unscoped_row_impl_from_an_app`, plus every fixture whose line numbers shift. New
   compile-pass fixtures: a scoped job enqueued inside its scope, a `global, cross_tenant`
   fan-out, and a `public(Org)` route.
2. **Runtime pins** in `crates/charpente/tests/tenant.rs`, counted with `AUTHORIZE_CALLS` and
   `BODIES_ENTERED`:
   - (a) An anonymous caller on plain `public` gets §4's 401, byte-exact. No body runs and
     `authorize` is never called.
   - (b) and (c) `public(Org)` admits anonymous and credentialed callers alike, with zero
     calls.
   - (d) A credentialed caller on plain `public` makes exactly one call.
   - (e) An `auth` route makes exactly one call with that principal. A refusing scope
     answers the app's own error and no body runs. This is today's
     `a_refusing_authorize_is_the_answer…`, re-expressed on `auth` routes.
   - (f) A `public(Org)` route that opens another scope type anonymously gets a 401 naming
     `public(Org)`.
3. **The statuspage regression.** A fixture shaped like the statuspage's incident routes.
   I-1, a write route flipped from `auth` to `public` and driven anonymously, gets the 401
   and leaves the store unchanged. C-2 edit (b) is covered by item 1's compile error. §13
   criterion 2 is the one-off rebased run.
4. **Isolation, extended to jobs.** A new swarm test in `crates/charpente/tests/kernel.rs`
   with its own workload, so no existing universe is re-keyed. *Workload:* N tenants with
   colliding ids and tenant-derived markers (the `ledger_marker` idiom). An `auth` scoped
   route and a `public(Org)` route each enqueue a `scoped_by` job that writes a marked row
   and sometimes a scoped follow-up. A cron `global, cross_tenant` sweep fans out with
   `enqueue_in`. A failing first attempt forces retries, and every write is replayed with its
   credential stripped. The full fault matrix applies, with crash and restart through
   dispatch. *Oracle:* read through the operator path, every job-written marker lies in the
   tenant it was found under, and every stripped write on a route not declared `public(Org)`
   got the 401 and wrote nothing. *`require:`* `tenant:job_multi_tenant_write`,
   `tenant:job_run_after_crash`, `tenant:job_fan_out`, `tenant:anonymous_refused`, because a
   single-tenant universe cannot detect a leak at all (tenant spec §5). *Naive victims,* each
   of which must fail the shared oracle by name beside a control that passes on the same
   store: a `JobRow` whose carried scope is not its enqueuer's; every job's rows written
   under tenant 0; a fan-out that lands every enqueue in the first tenant. N is set after
   timing one batch. The commit and LEDGER state N and the master seed, and any failing seed
   is pinned with its reason asserted.
5. **Mutation.** `scripts/mutants.sh` covers every new or changed function: in `db.rs`, the
   scope constructors, the enqueue family, the `0x0a` codec and `materialize_job`; `job.rs`'s
   `JobRow` codec; `tenant.rs`; in `http/app.rs`, `build_ctx`, `stage_or_subscribe` and the
   probe; `kernel/dispatch.rs`; `describe.rs`; and the macros' `job.rs`, `route.rs` and
   `fingerprint.rs`. Pass `--test-package` for the crate whose suites exercise each file
   (`charpente` outside core and sim, `charpente-macros` for the parsers), and check each
   `--re` against `cargo mutants --list` first. The bar is zero MISSED; a timeout is not a
   catch, and no `.cargo/mutants.toml` entry may be added. `Scoped::scope`'s three arms and
   the dispatcher's scope binding are also inverted by hand, one operand at a time, in an
   isolated worktree.
6. **Goldens.** Re-pin `examples/invoicing/tests/describe_golden.rs`, `charpente-cli`'s
   describe golden, `describe_contract` (GUIDE §13's JSON) and `doc_truth`'s pinned blocks.
   `PINNED_FLOOR` rises by §10's three new blocks.
7. **Replay.** Every pre-existing pinned seed reproduces, because nothing an unscoped app
   writes changes. `global` enqueues stay `0x08` and keep their fingerprints; a test pins
   `SessionSweep`'s fingerprint, and one app job's, to literals recorded on the base commit.
   `authorize` writes nothing, and `kernel.rs:1435` moves to `public(Org)` and stops calling
   it. The protocol bump changes one `Hello` byte's value, not its length. So no seed is
   predicted to move. That is a prediction, and the plan measures it.
8. **The full gate.** `cargo fmt --all --check`,
   `cargo clippy --workspace --all-targets -- -D warnings`, and
   `cargo test --workspace --no-fail-fast`, confirmed by counting `test result: ok` lines.

## §10 Docs, in the same commits (hard rule 6)

- **GUIDE** §5 (the three stances), §6 (the `#[job]` forms, the scoped `run`,
  `ScopedTx::enqueue`, "What `#[job]` rejects"), §7 (cron is `global`-only; the fan-out
  pattern), §9 (both catalogs; the job block rewritten), §11 (the public feed at
  `:3464-3471` becomes `auth`, keeping its "authorizes the subscriber" comment true), §12 (a
  tenancy change is a shape change), §13 (the describe JSON), and §16 throughout
  (`&Principal`, the stances, scoped jobs, fan-out, "reads, plus fan-out enqueue", and "Where
  the guarantee stops" rebuilt around §7). Three new pinned blocks: the 401, the bare-`#[job]`
  refusal, the `NoJobTenant` note.
- **DESIGN** §3 (the Scope, Routes and Jobs bullets), §6 (Queue: the scoped `run` and
  `0x0a`; Cron: `global` only), §7 (the describe fields), §8 (a tenant is entered by
  membership or by declaration, never by `None`).
- **DECISIONS §12**: §14's dated correction. `tenant.rs`'s module doc gets the same
  correction.
- **Earlier specs** get dated "superseded by this spec" lines:
  `2026-09-16-default-secure-surfaces-design.md` §2's "`authorize` admits `None`" sentence,
  and its §8 items "Any change to `Tenant::authorize`'s `Option<&Principal>`" and "A
  `#[job]` change". `2026-08-30-tenant-scoping-design.md` ruling 3 gets R10's reading.
- **README** (`:62-64`, `:140-143`): jobs declare `scoped_by` or `global` too. The example
  at `:47-54` becomes correct as written. **PLAN**: M3 item 3 (2) is fixed. **LEDGER**: the
  entry, with seeds, counts and the rebased statuspage's errors.

## §11 Migration

The order is the verification, and every commit is green (default-secure §7). First, the job
words are accepted as no-ops, `global` is swept onto every `#[job]`, and only then is the
bare form refused, which is where the compiler proves the sweep complete. Second, the
`authorize` signature, `Principal`'s privacy, `TenantAdmission`, `public(Org)` and the 401 land
together, because the signature change is compile-forced. Then scoped jobs, then
`describe`, then the property. The inventory:

| What | Where |
|---|---|
| `authorize` impls | `crates/charpente-macros/tests/` (8 in 7 compile-fail and 5 compile-pass fixtures); 10 in `crates/charpente/tests/{tenant,archive,kernel,realtime,typed_tables,describe,replication}.rs`; 2 in `charpente-core/tests/engine.rs`, with its 15 `open_scope` calls; `examples/invoicing/src/bin/tenant_node.rs:64-79`; GUIDE's two compiled examples |
| `Principal` field readers | `crates/charpente/tests/{kernel,http,auth}.rs`; `examples/invoicing/src/routes.rs`; `tenant_node.rs:69`; GUIDE `:888`, `:966`, `:979`, `:5613` |
| Public routes that open scopes, moving to `public(Org)` | `tenant.rs:886`, `:898`; `realtime.rs:4613`, `:4623`, `:4849`; `kernel.rs:1435` |
| Public routes that open scopes, moving to `auth` | `tenant.rs:91`, `:105`, which then keep testing refusal and gain `public` and `public(Org)` twins for oracle 2 |
| Operator `open_scope(…, None)`, moving to §7's form | `tenant.rs:450`, `:524`, `:580`, `:684`, `:988`, `:1046`; `archive.rs:8261`; `replication.rs:1229` |
| `#[job]` sites, all becoming `global` (28 in Rust, 18 in GUIDE) | `bench-harness`; `charpente-cli/templates/lib.rs`, held byte-identical to GUIDE §1; `auth/sweep.rs`; `launch.rs`; `sim-fixture`; `examples/invoicing/src/jobs.rs`; the macro fixtures; `crates/charpente/tests/{kernel,group_commit,describe,archive,replication}.rs` |
| Platform code | `charpente-core/src/{db,job,table}.rs`; `charpente/src/{tenant,describe}.rs`, `auth/ladder.rs`, `http/{app,router,mod}.rs`, `kernel/{dispatch,cron}.rs`, `repl/mod.rs`; `charpente-macros/src/{job,route,fingerprint,lib}.rs`; `charpente-cli/src/genclient.rs`; `charpente-tsgen/tests/` |

## §12 Refused, so the plan does not grow them

- **B** (R1), **α** (R7), and **C**, which reads a scoped job's tenant from its payload and
  so lets a job act in a tenant its enqueuer was never admitted to.
- **`auth` with `public(Org)`.** "Any authenticated caller enters any tenant" is the
  fail-open for any stranger with a login.
- **A compile-time refusal of `tx.scope` on plain `public`.** The 401 is structural, named and
  pinned; a typed `Ctx` per stance would reshape every handler signature to answer earlier.
- **Cron scheduling a scoped job, or an admitting mode on the drift probe.** Neither has a
  tenant to name, and a `Ctx` that admits any scope is one bug away from a request.
- **`enqueue_keyed_in`**, until the first fan-out needs an external idempotency key; **a
  `Db`-minted admission or an admitting `Db::transact`** (§7); **inferring a job's tenancy**
  from what `run` touches (DECISIONS §13: the platform asks); **R9's three items**; and **any
  `Principal → Org` resolver** (R10).

## §13 Exit criteria

1. The three CI commands are green and counted. Oracle 1's fixtures are pinned, and existing
   fixtures keep their meaning.
2. The statuspage at `6037704`, rebased onto the finished `main`, fails to compile at
   `lib.rs:42-45`; at every `open_scope(org, None)` (`public.rs:41`, `:90`, `:127`;
   `notify.rs:24`; `maintenance.rs:149`); and at every bare `#[job]` (`notify.rs:10`;
   `maintenance.rs:109`, `:139`). The full error list is recorded in the LEDGER.
3. Oracles 2 and 3 are green, and each was red on the base commit.
4. Oracle 4 is green over the stated N, with all four `require:` keys credited and every
   naive victim failing by name.
5. Oracle 5 catches every viable mutant, its counts are in the LEDGER, and no exclusion is
   added.
6. Every pre-existing pinned seed reproduces, and `SessionSweep`'s fingerprint is unchanged.
7. `describe` lists every `public(S)` route and every `scoped_by` job by scope name.
8. §10 lands in the same commits as the code it describes, and `charpente new` still
   compiles and matches GUIDE §1.

## §14 Decisions for Noah

One item touches DECISIONS.md text.

1. **DECISIONS §12, "What an app is told": a prose correction. The ruling is unchanged.** The
   remedy clause currently reads "code outside a handler that wants the guarantee should go
   through a transaction and `tx.scope::<Org>(org, ctx)?` exactly as a handler does". That
   names a `Ctx` that no code outside a handler holds. Proposed replacement for that clause:

   > with the remedy: code outside a handler never takes a scope on its own word. A job
   > declares `#[job(scoped_by = Org)]` and runs in the tenant its enqueuer opened; operator
   > code that must act inside one tenant — seeding, a test oracle — opens it with a
   > `TenantAdmission` minted from a `Group`, the seal `CrossTenant` already carries, which is
   > the operator's standing made explicit rather than an exception to the guarantee.
   > *(Corrected 2026-09-27, job-tenancy spec: the earlier remedy named a `Ctx` that no code
   > outside a handler holds.)*

R3's reading of §13 needs no sign-off. The stance is still a required choice whose absence is
a compile error naming every answer, and R3 records why `public(Org)` is one of those
answers, not an optional word.

## Amendments (implementation plan)

- **A1 — the bare `#[job]` refusal lands with `scoped_by`, not before it.** Spec §11 accepts the job words as no-ops, sweeps `global` onto every `#[job]`, and then refuses the bare form. The refusal's text tells the reader to write `#[job(scoped_by = Org)]`, so the refusal lands in the same commit that makes that advice work (Task 6). Task 1 accepts `global`, sweeps it, and proves the sweep with a grep. Task 6's refusal is where the compiler proves it.
- **A2 — `Who = str` scope types keep `open_scope` and pass a name.** Four test scope types implement core's generic `Tenant` with `Who = str`, not the platform's `TenantScope`:
  - `ConvOrg` (`charpente/tests/archive.rs`);
  - `SeedOrg` (`charpente/tests/replication.rs`);
  - `TestOrg` and `AuditOrg` (`charpente-core/tests/engine.rs`).

  Their `open_scope(…, None)` calls become `open_scope(…, "operator")`, or `"member"` in engine.rs (A6). Their `authorize` still runs and still writes nothing. §7's form applies to `Who = Principal` sites: `tenant.rs`'s six. So spec §7's "eight platform-test sites" are six moved to `admit_scope` and two kept on `open_scope` with a name. Core's `Tenant` stays generic over `Who` (spec §2 R2), so these calls remain the honest spelling at that layer.
- **A3 — `build_ctx` takes the route's `Stance`.** `Stance` is private to `charpente::http` and holds three things:
  - the method word (`SSE` for a stream route) and the pattern;
  - whether the route said `auth`;
  - for a `public(S)` route, `S`'s `PublicScope` and the `TenantAdmission` minted for it.

  It is built in two steps. `Declared::of(route)` copies the route's declaration out of the router. `.admitted(&group)` then mints the admission, for a `public(S)` route only.
  - The split exists for the drift probe, which must stop borrowing the router before `App::begin_group` borrows the whole `App`. That is why the probe already reads `found.cross_tenant` first.
  - The request path and the probe build it the same way, so the probe honours `public(S)` exactly as a request does (spec §4).
- **A4 — `tenant.rs`'s counters become thread-locals.** Oracle 2 has seven tests that read them, and `cargo test` runs them on parallel threads of one binary. A request is handled on the thread that called `App::handle`, so a thread-local counts exactly one test's requests. `AUTHORIZE_CALLS` now records the subject of each call, so test (e) can assert *which* principal was asked about.
- **A5 — oracle 4 correlates replies by body.** Every write route returns its request's nonce, so a 200 names the request that produced it. The 401s carry fixed text and no nonce. "Wrote nothing" is therefore checked on the store and the job queue, where every row and job carries its nonce, and on the 200s. It is not checked by matching each 401 to its request.
- **A6 — core's `engine.rs` fixtures pass `"member"` where they passed `None`, and `"stranger"` where they passed `Some("stranger")`.**
- **A7 — Tasks 3 and 4 split spec §11's "land together" group.** Task 3 lands the stances, the three rules and the 401 while `authorize` still takes `Option<&Principal>`; rule 2 passes `Some`. Task 4 then lands the signature and `Principal`'s privacy. Both commits are green. The split lets a reviewer judge the semantic change apart from the compile-forced migration. In the commit between them, nothing the platform does passes `None`; only direct `open_scope(…, None)` calls still can.
- **A8 — the NoJobTenant pinned block quotes the diagnostic's template.** The block uses `<job>` and `<scope>` placeholders, because the attribute source spells `{J}` and `{Self}` and `doc_truth` matches source text. GUIDE §16 also shows the rendered rustc output, unpinned; the `.stderr` fixture pins that. The 401 pinned block quotes oracle 3's concrete instance, which its test pins byte-exact. `PINNED_FLOOR` goes 21 → 22 (Task 3) → 24 (Task 6).
- **A9 — the fingerprint pins are computed, then verified at base.** `SessionSweep` (`before: u64`) is FNV-1a over `"before\0u64\0"` = `0x7e72_9c20_6b79_cbad`. `kernel.rs`'s `CountUp` (`slot: u64`) is FNV-1a over `"slot\0u64\0"` = `0x766b_8ba4_18db_4d08`. Task 1 runs the pin at `d363baf` before changing anything. If it fails there, the literal is wrong, not the code: stop and report.
- **A10 — oracle 4's stripped replays are on all three routes.** A stripped replay on the `public(Shop)` route legitimately succeeds, so the oracle refuses a stripped write only on `auth` and plain `public`. That is exactly spec §9 item 4's "not declared `public(Org)`".
- **A11 — `Stance` records `auth` too.** The drift probe does not enforce `auth`. An `auth` stream route that opens a scope therefore meets rule 3 under the probe, and the probe's `Err` quotes the rule-3 text. With `auth` recorded, that text names the route's real stance.
- **A12 — Task 2 lands `TenantAdmission` and `Tx::admit_scope` before §11's second group.** Both are additive and neither is compile-forced, so the operator path exists before any caller needs it and every commit stays green.
