# M1 sub-project 4 — auth: design

Date: 2026-07-15. Noah-approved scope and architecture (this session). Fourth of M1's
six sub-projects (decomposition in 2026-07-07-wal-segment-management-design.md).

## Context

M0 shipped minimal auth: a `Session { id, token: String #[index], subject }` platform
row inside the commit point, raw tokens, no expiry, `bearer_or_cookie` extraction, and
`#[route(…, auth)]` gating on session existence. The code carries the IOU verbatim
("raw in M0 — M1 hashes them and adds expiry", http/app.rs). PLAN M1 names "Auth,
properly: OIDC, API keys, tenant scoping as typed key prefixes"; DESIGN §8 commits to
built-in typed tables and route guards, differentiated by living *inside* the commit
point. Separately, the replication and realtime reviews accumulated a pile of
pre-auth-hardening notes whose real answer is this sub-project: the replication port
accepts any TCP peer.

## Scope (Noah rulings, 2026-07-15)

In: hashed session tokens + expiry + sweep; API keys; replication-port peer auth;
OIDC in static-JWKS form. Crypto via RustCrypto (`sha2`, `rsa`, `base64ct` for
base64url) — pure Rust, no IO/threads/time inside the boundary, deny-list notes in
the dependency commit per CLAUDE.md's rule.

Out (explicit non-goals): passwords / user tables / login flows (app domain);
dynamic JWKS fetch and any outbound IO (waits for the external-IO surface); API-key
scopes/permissions (subject-only v1 — a permissions story earns its own design);
TLS on the replication wire (M2 conversation — see §5's threat model); tenant
scoping as typed key prefixes (PLAN names it; deferred to its own design — it is a
table-layer type-system feature, and the session-subject pattern covers scoping
until then, as the realtime demo shows); key-management CLI/UI.

## 1. Data model (platform tables — breaking pre-release, no migration)

- `Session { id: Id<Session>, token_hash: String #[index], subject: String,
  expires_at: u64 }`. `token_hash` is lowercase-hex SHA-256 of the raw token; the
  raw token exists only in the mint response. `expires_at` is `Clock` nanos —
  wall-based in prod, virtual in sim; restart-stable (the same clock shape cron's
  catch-up already proves).
- `ApiKey { id: Id<ApiKey>, key_hash: String #[index], subject: String,
  created_at: u64 }`. Minted via `Ctx::create_api_key(tx, subject) -> String` (raw
  key returned once, `ck_`-prefixed so leaked keys are grep-able/scanner-friendly);
  revoked by `tx.delete::<ApiKey>(id)` — revocation is an ordinary transaction
  inside the commit point, durable and replicated like any other write.
- `Ctx::create_session(tx, subject)` keeps its signature; `expires_at = now +
  session_ttl` from app config (§8 constants). One-per-request assert unchanged.
- Both credential kinds are fed by the SINGLE existing pre-transaction entropy
  draw (the `fresh_token` pattern) — **zero new draws**, so every pinned seed's
  entropy interleaving is structurally untouched (the frozen replay contract
  holds by construction, not by re-verification). Consequence, enforced by
  extending the existing once-assert: **one credential mint per request**, either
  `create_session` OR `create_api_key`, never both — the second mint in one
  request panics with the named contract message. (Handlers cannot draw entropy —
  `Tx` has no `Entropy` by design — and an unconditional second pre-handler draw
  would shift every pinned universe; one-mint-per-request is the honest shape.)

## 2. The resolution ladder (pre-transaction, pure)

One entry point replaces `bearer_or_cookie`, producing
`Option<Principal { subject: String, via: Via }>` with
`enum Via { Session(Id<Session>), ApiKey(Id<ApiKey>), Oidc }`:

1. `Authorization: Bearer <t>` where `<t>` contains exactly two `.` — OIDC verify
   (§3). Verification failure is a named 401 (§6), never a fall-through: a
   JWT-shaped credential is judged as a JWT.
2. Any other bearer — SHA-256 it, exact-match the `ApiKey` index, then the
   `Session` index (expiry-checked against `clock.now()`).
3. `charpente_session` cookie — session lookup as in 2.
4. Nothing presented — `principal = None`; `#[route(…, auth)]` answers the
   no-credential 401.

`Ctx::principal() -> Option<&Principal>` is the new surface; `#[route(…, auth)]`
gates on its presence (existing apps recompile unchanged in behavior).
`Ctx::session()` survives as a compatibility accessor returning the `Session` row
only when `via` is `Session(id)` (looked up during resolution; `None` for
ApiKey/Oidc principals). `create_session`/`revoke` unchanged in shape.

Timing note (stated, not hand-waved): lookups are hash-then-index-equality. The
compared values are SHA-256 outputs; an attacker controls neither stored preimages
nor learns anything exploitable from index-lookup timing on hashes. The repl secret
compare (§5) is the one place equality runs on secret-derived data both sides
control, and it compares hashes for constant-time behavior.

## 3. OIDC, static-JWKS

- App-mount config (the `App` builder): `OidcConfig { issuer: String, audience:
  String, jwks_json: String }`. JWKS parsed at mount; a bad key, unsupported `alg`,
  or malformed document is a named mount error (which key, what is wrong, the fix)
  — startup, never request time. RS256 only in v1 (`rsa` crate); ES256 joins when
  someone needs it.
- Verify: split the compact JWT; base64url-decode header/payload/signature
  (`base64ct`); header `alg` must be RS256 and `kid` (when present) must match a
  configured key, else try all configured keys; RSA-PKCS1-v1.5-SHA256 over the
  signing input; claims: `iss` string-equals issuer, `aud` (string or array)
  contains audience, `exp > now - skew`, `nbf <= now + skew` (`JWT_CLOCK_SKEW`,
  §8). Subject = the `sub` claim (required).
- Success yields an ephemeral `Principal { subject: sub, via: Oidc }` — no row, no
  state, nothing to revoke (revocation story for OIDC = short `exp`, stated in
  GUIDE).
- Pure computation on the kernel thread (~100µs/verify); deterministic in sim — the
  test suite checks in a fixed RSA keypair and mints tokens in-test with the same
  `rsa` crate, so the claim matrix replays byte-identically.

## 4. Built-in expiry sweep

A platform cron entry (the existing cron machinery — recurring enqueuer, catch-up
semantics for free) runs daily and deletes expired `Session` rows in one
transaction per tick. Lazy resolution (§2) is the correctness mechanism — the sweep
is storage hygiene only, so its cadence is not load-bearing. Expired rows are
invisible from the moment `now` passes `expires_at`, restart or not.

## 5. Replication peer auth

- `ReplConfig.peer_secret: Option<String>`. When `Some` on a node, every inbound
  session must prove it: `Hello` gains `secret: Option<String>`, and each single-
  message control conn (`CtrlPromote`, `CtrlSwitchover`) carries the same field.
  Wrong or missing proof → `Msg::Refuse` with the named text (§6) and close, before
  any state is touched (no ladder entry, no pin, no seed). Comparison is
  constant-time: compare SHA-256 of both sides.
- `None` (unconfigured) preserves today's open behavior — each side enforces its
  own config, independently: a node with `peer_secret: Some` refuses any inbound
  Hello/Ctrl without the matching proof; a node with `None` accepts everything
  inbound and ignores (gracefully, not errors on) a proof it did not require.
  Outbound, a configured node always attaches its proof. So a half-configured
  pair works in exactly one direction — the configured node's inbound is guarded,
  the unconfigured node's is open — which is visible, debuggable, and the
  rollout-friendly middle state (configure the followers first, then the
  primary).
- The CLI verbs gain `--secret <s>` (absent = send `None`); the existing exit-code
  contract stands (a `Refuse` maps to exit 1 with the forwarded text).
- **Threat model, stated honestly (GUIDE + DESIGN):** the replication wire is
  plaintext TCP. `peer_secret` authenticates peers — it closes the
  anyone-can-connect hole (the AwaitHello-pins-GC / empty-Frames class, already
  hardened, now access-controlled) — but an eavesdropper on the path can read
  frames and the secret. The deployment assumption is a private network; transport
  encryption is an M2 conversation. This is documented, not implied.

## 6. Error surface (product surface, all pinned by tests against text)

Precision over vagueness — the repo ethos, and the leak profile is acceptable
(possession of an expired token is already proof of past authorization; JWT claims
are attacker-visible by construction):

- No credential on an `auth` route: 401 `authentication required — send
  "Authorization: Bearer <token>" or the charpente_session cookie`.
- Unrecognized bearer/cookie: 401 `unrecognized credential — it may have been
  revoked; obtain a new one`.
- Expired session: 401 `session expired — log in again`.
- OIDC, one named 401 per failed check: bad signature; token expired (`exp`);
  not yet valid (`nbf`); wrong issuer (got vs configured); wrong audience (got vs
  configured); unsupported `alg`; malformed token. Each names the check and the
  fix.
- Mount-time OIDC config errors: name the offending key/field and the fix.
- Repl refusal: `peer secret missing or mismatched — set ReplConfig.peer_secret to
  match this node (or run the CLI verb with --secret)`.
- Exact final texts are fixed at implementation and pinned by tests; the plan
  carries them verbatim once written (the T5-realtime convention).

## 7. The sim story

- **Flagship negative-space property:** after a swarm workload that mints sessions
  and API keys and runs the repl link with a `peer_secret`, the raw token, raw key,
  and raw secret byte-strings appear NOWHERE in any durable file — scan every disk
  in the universe (store + WAL + role files), assert absence. This is the
  hashed-at-rest guarantee as a mechanical property, fault matrix and all. (The
  WAL nuance is the interesting part: mint transactions must commit the HASH, not
  the raw credential — i.e. hashing happens before the effects record is built.
  The property is what proves nobody regresses that.)
- Expiry: virtual-time jump past TTL → the exact expired-401; across crash/restart;
  the sweep cron's tick deletes exactly the expired rows (existing cron test
  patterns); resolution correctness never depends on the sweep having run.
- The ladder matrix: {JWT, api key, session bearer, session cookie, garbage,
  nothing} × {valid, expired, revoked, wrong} → exact status + text, deterministic.
- The OIDC claim matrix: each §3 check violated in isolation → its named 401; the
  valid token accepted; `kid` present/absent; `aud` as string and as array.
- Repl peer auth: universes with {both configured matching, mismatched, one-sided}
  × {data plane, promote, switchover via the real CLI binary} → named refusals,
  zero state touched on refusal (pin/GC/seed counters unmoved), and the T7/T8
  suites still green with secrets configured.
- Meta-test (hard rule 3): a naive resolver victim — compares raw tokens instead
  of hashes (equivalently: skips the expiry check) — must fail the ladder matrix /
  negative-space property by name.
- Determinism check unchanged; entropy discipline: the added API-key draw is
  unconditional per request (§1), so request-shape determinism and every pinned
  seed hold — the frozen-contract proof is the existing suites green.

## 8. What changes where

- `charpente-core`: nothing (auth is http/kernel/repl-layer; tables ride the
  existing `#[table]` machinery). Dependencies `sha2`/`base64ct` land in
  `charpente` (and `rsa` likewise) — not core.
- `charpente` (http): `Session` reshape, `ApiKey`, `Principal`/`Via`, the ladder,
  `Ctx::{principal, create_api_key}`, OIDC config + verify module, the 401 family.
- `charpente` (kernel): none beyond what http resolution already runs pre-Tx; the
  sweep cron entry registers platform-side like `charpente.jobs`' machinery.
- `charpente` (repl): `Hello`/Ctrl secret field (wire change, pre-release), the
  refusal, `ReplConfig.peer_secret`.
- `charpente-cli`: `--secret` on promote/switchover.
- Docs: DESIGN §8 from sketch to mechanism; GUIDE auth section rewritten (login,
  keys, OIDC config, the threat model); invoicing demo migrates (its `/login`
  keeps the free-subject shape — passwords stay app domain — but gains TTL and the
  hashed store for free; FINDINGS-style notes if friction appears).
- Constants (deterministic): `SESSION_TTL` default 30 days (app config, nanos);
  `JWT_CLOCK_SKEW = 60s`; sweep cadence daily. Token formats: sessions as today
  (64 hex chars), API keys `ck_` + 64 hex.

## 9. Evidence bar

Every task lands with its suites green over fresh seeds; sim-found bugs land with
seeds pinned. Close-out sweep: fresh batches ×2 for the auth property file and the
four core suites plus realtime + replication (the Session reshape and the repl wire
change ripple both); pinned-seed replays including the two release-mode CI seeds;
invoicing 1024-universe batch; full workspace + clippy + fmt; a recorded live demo
— mint a key, call with it, revoke it in a transaction, watch the next call get the
named 401; login, expire (config with a short TTL), see the expired text; a wrong
`--secret` promote refused by name.
