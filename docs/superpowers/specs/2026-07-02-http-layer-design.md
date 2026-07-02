# M0 step 5 — HTTP layer: design

Status: awaiting Noah's review. The boundary decision marked **[FLAGGED]** was made on
the recommended option after the question went unanswered (Noah away); everything else
follows README/DESIGN/PLAN/DECISIONS directly.

## Goal

PLAN.md step 5: `#[route]` over a hyper-based server; typed params/body/response;
sessions (minimal auth). The deliverable that matters for the thesis: every line of
HTTP behavior charpente owns — routing, decoding, auth, transact-wrapping, error
mapping — runs deterministically in the sim under the fault matrix, and a request
acknowledged with 200 provably survives kill-at-any-point (PLAN's M0 exit criterion,
expressed at the request level).

## The boundary decision **[FLAGGED — recommended option taken]**

**A deterministic app kernel + a hyper edge adapter.** `charpente::http` (deny-list
fenced, no ambient IO) owns typed `Request`/`Response`, the router, param/body decode,
sessions, and transact-wrapping; the sim drives `App::handle(request)` directly.
`charpente::serve` (module-wide allow, like `prod.rs`) runs hyper/tokio and only
translates wire↔struct, feeding the single-threaded kernel through a channel.

Rationale: the only option consistent with all three governing docs — README's
"entire application in the simulator" (the application's HTTP surface is the kernel),
PLAN's "hyper-based server", and DECISIONS §10's "tokio-flavored async stays outside
the deterministic boundary". Rejected: (a) writing our own HTTP/1.1 parser over `Conn`
byte streams for wire-level sim coverage — a large security/correctness surface that
contradicts PLAN and delays the milestone; (b) axum-style hyper-everywhere — leaves
routing/decode/session logic outside the sim, breaking MANIFESTO §6.

**Deterministic executor: deferred to step 6.** Step 5's kernel loop has exactly one
event source (incoming requests); serialization comes from `&mut App` (which owns
`Db`), not from a scheduler. DECISIONS §10 anticipated the executor "when HTTP/queue
land (M0 steps 5–6)" — the need materializes at 6, when multiple task kinds first
compete. *Post-implementation correction (final review):* the shipped loop is a single
blocking `recv` on the request channel — step 6 must restructure it (recv-with-timeout
or the deterministic executor) to add queue/cron wakeups. What actually leaves the
room is `App` owning the `Db` behind `&mut self` entry points; the loop itself will
reshape.

## Module layout

```
charpente::http   (deterministic, sim-tested)
├── Method, Request, Response      — plain structs; headers as BTreeMap<String, String>
│                                    (lowercased keys), body Vec<u8>
├── Router / Route / routes![]     — sorted table; exact + :param segments;
│                                    mount-time conflict panic; 404/405
├── App / AppConfig                — owns Db + Box<dyn Entropy>; handle(&mut self,
│                                    Request) -> Response; body_cap (default 1 MiB)
├── Ctx                            — session: Option<Session>, fresh token;
│                                    create_session / revoke
├── Session                        — platform #[table]: id, #[index] token, subject
└── ApiError                       — BadRequest(String) | NotFound | Unauthorized |
                                     Internal(String); From<io::Error>; JSON body
charpente::serve  (edge, ambient-IO allowed)
└── serve(addr, app) — hyper 1.x on a minimal tokio rt; per-request: build
    http::Request, send (req, oneshot) over mpsc to the kernel thread, await reply
charpente-macros
└── #[route(METHOD "/path/:param"[, auth])] — typed extraction glue + Route descriptor
```

## The app surface

```rust
#[route(POST "/invoices/:id/finalize", auth)]
fn finalize(tx: &mut Tx, id: Id<Invoice>, body: FinalizeReq, ctx: &Ctx) -> Result<Invoice, ApiError> { … }

let mut app = App::open(disk, schema, entropy, AppConfig::default())?  // registers the
    .mount(routes![finalize, create_invoice, list_invoices]);          // Session table itself
let response = app.handle(request);   // &mut self — serializable by construction
```

**Argument convention (macro-enforced, errors are product surface):**
1. `tx: &mut Tx` — required first argument.
2. One argument per `:segment` in the path, matched **by name**, in path order. Types
   implement `FromParam` (provided: `u64`, `String`, `Id<T>`). A `:segment` with no
   matching argument (or vice versa) is a compile error naming both lists.
3. At most one remaining "body" argument, any `serde::Deserialize` type. Present ⇒
   the request must carry a JSON body of that shape.
4. Optional trailing `ctx: &Ctx`.
Return type: `Result<T: serde::Serialize, ApiError>`. `Ok` ⇒ 200 with JSON body.

The macro emits a `Route` descriptor (method, pattern, auth flag, glue fn) alongside
the untouched original fn; `routes![a, b, c]` collects descriptors explicitly —
distributed/registry magic (inventory/linkme) rejected as a nondeterminism source and
an unnecessary dependency.

## Dispatch pipeline (`App::handle`)

1. **Route match.** Segment-by-segment over a sorted table; a literal segment takes
   precedence over a `:param` capture at the same position. Unknown path → 404;
   known path, wrong method → 405. Mounting two routes with the
   same method+pattern panics at mount with both route names.
2. **Body cap.** `body.len() > config.body_cap` (default 1 MiB) → 413 *before* any
   decode. This fences the `MAX_RECORD_BYTES` assert in `Db::transact` from network
   input. (An app whose handler amplifies a small body past 16 MiB of effects can
   still hit the assert — that remains a contract violation by the app, documented on
   the assert. Revisit at M1 if real apps hit it.)
3. **Decode.** Path params via `FromParam`, body via `serde_json` — failures → 400
   with a JSON body naming the parameter/field and the expected type (hard rule 5;
   tested against message text).
4. **One transaction.** `db.transact(|tx| { session lookup; auth gate; handler })`:
   the session token (from `Authorization: Bearer <t>`, else cookie
   `charpente_session=<t>`) is looked up via `Session::by_token` *inside* the same
   transaction the handler runs in; `auth` routes with no valid session return 401
   without invoking the handler.
5. **Respond.** `Ok(T)` → 200 `application/json`; `Err(ApiError)` →
   400/404/401/500 with `{"error": …}`; `TxError::Io` → 500 (the record may be
   durable-but-unacknowledged — same contract as `Db::transact`; clients retry with
   idempotency at M1's queue surface). The 200 is only produced after `transact`
   returns `Ok`, i.e. after the WAL sync: **ack-after-durable at the HTTP level.**

## Sessions (minimal auth, inside the commit point — DESIGN §8)

- `#[table] Session { id: Id<Session>, #[index] token: String, subject: String }` —
  a platform-registered table (`App::new` registers it in the Schema alongside the
  app's tables).
- Before each transaction, `App` draws 32 bytes from `Entropy` (hex-encoded) — the
  request's fresh-token candidate; the first 8 bytes double as the `Id<Session>` raw
  value. Deterministic in sim, OS entropy in prod, recorded in effects like any row.
- `Ctx::session` — the authenticated session, if any. `Ctx::create_session(tx,
  subject) -> &str` inserts the row with the fresh token and returns it (login).
  `Ctx::revoke(tx)` deletes the current session (logout). Login/logout are ordinary
  app routes; credential verification is app logic.
- Tokens stored raw in M0; M1 ("auth, properly") hashes them and adds expiry. Session
  cookie emitted by the app returning the token in its response body — no Set-Cookie
  automation in M0.

## Edge adapter (`charpente::serve`)

- New deps in the `charpente` crate only (core untouched): `hyper` 1.x,
  `hyper-util`, `http-body-util`, `tokio` (features limited to `rt`, `net`, `sync`),
  `serde`, `serde_json`. Deny-list entries for tokio's ambient APIs
  (`tokio::spawn`, `tokio::time::sleep`, `tokio::fs::*`, `tokio::net::TcpListener::bind`,
  `tokio::net::TcpStream::connect`) land in the same commit (CLAUDE.md hard rule 1).
- Shape: `serve(addr, app)` — the kernel thread owns `App` and drains an
  `std::sync::mpsc` of `(Request, reply: oneshot)`; hyper tasks translate and await.
  Body reads capped at `body_cap` during collection (don't buffer what will be 413'd).
- CLI `serve` wiring is step 7; step 5 ships the library fn + a loopback smoke test.

## Testing (sim-first, same PR — hard rule 3)

`crates/charpente/tests/http.rs` (sim) + a prod smoke test:

1. **Unit (FaultConfig::none):** routing (404/405, param capture, conflict panic),
   decode errors (400 naming field/type), body cap (413), auth gate (401),
   session create → authed request → revoke → 401, handler App-error mapping,
   read-only GET consumes no seq.
2. **Request-level crash property (`run_universes!`):** seeded client issues typed
   requests through `App::handle` under the fault matrix with crash/restart rounds;
   oracle: every request answered 200 is present after recovery (with its enqueues —
   reusing the step-4 dual-write oracle shape); 500-answered requests may or may not
   be durable (either is legal); the store never invents state. This is the M0 exit
   criterion at the request level.
3. **Determinism:** same seed + same request sequence → identical status codes,
   bodies, and durable fingerprint.
4. **Error-quality tests** on the 400/401/404/413 response bodies. Macro compile
   errors are written to the hard-rule-5 bar but not ui-tested in step 5 (trybuild
   adds a heavy dev-dependency; the step-8 agent-authored demo app is the acceptance
   test for macro error quality — same policy as `#[table]`).
5. **Prod smoke test:** boot `serve` on `127.0.0.1:0`, drive one route with
   hand-written HTTP/1.1 bytes over a std TcpStream (in the sanctioned prod test
   module), assert status line + JSON body.

## Scope cuts (deferred, each additive)

- Query-string params (path + JSON body suffice for the demo; add with `FromParam`
  reuse when needed).
- Streaming/multipart bodies; response streaming. `Vec<u8>` + cap only.
- TLS (terminate in front; revisit with deployment story).
- OpenAPI derivation (types shaped for it; `charpente describe` is step 7).
- SSE/WebSocket (M1 realtime, DESIGN §6).
- Set-Cookie automation, token hashing, expiry (M1 auth).
- Deterministic edge executor (step 6, with the queue — first real multi-source
  scheduling need).

## Docs updated in the implementation commits (hard rule 6)

- DESIGN §3 Routes bullet: mark v0 implemented; note the argument convention and
  that OpenAPI derivation is pending.
- DESIGN §0/§1: one sentence locating the HTTP kernel inside the deterministic
  boundary and hyper at the edge.
- CLAUDE.md current-state: step 5 done + step 6 next (final commit, after evidence).
- README: no change needed — its example already matches this surface.
