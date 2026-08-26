//! The part of a generated client that never varies with the contract: the
//! `Result` union, `ApiBase` (base URL + token, `request`, `stream`), and the
//! two honesty conditions DESIGN §6 defines.
//!
//! It is a `&'static str` rather than something assembled at runtime because
//! it has nothing to assemble from — which also makes this half of the output
//! byte-identical by construction (spec §4).

/// Reserved TypeScript names this text declares at module scope. `lib.rs`
/// refuses a contract that would redeclare any of them, because two
/// `export type Result` in one file is a `tsc` "Duplicate identifier" landing
/// in an app author's build, pointing at generated code they did not write.
///
/// Sorted, and kept in sync by `RUNTIME_DECLARES_EXACTLY_THE_RESERVED_NAMES`
/// in `lib.rs` — a name added to the TypeScript below without being added
/// here would go unprotected.
pub const RESERVED: &[&str] = &[
    "Api",
    "ApiBase",
    "CursorDiverged",
    "Result",
    "StreamEnd",
    "StreamRejected",
    "SubscribeOptions",
    "UnknownEvent",
];

/// Members `ApiBase` defines. A route whose generated method name matched one
/// would override it — `tsc` rejects an incompatible override, and a
/// compatible one would silently replace the transport.
pub const APIBASE_MEMBERS: &[&str] = &["constructor", "headers", "request", "setToken", "stream"];

pub const RUNTIME: &str = r##"
// --------------------------------------------------------------------------
// Runtime. Hand-written and identical in every generated client — nothing
// below varies with the contract.
// --------------------------------------------------------------------------

/**
 * A route's outcome. Routes RETURN this instead of throwing: the error type is
 * half of the route's declared Rust signature, and `catch` binds `unknown`,
 * which would discard exactly the information this client exists to carry.
 */
export type Result<T, E> =
  | { ok: true; data: T }
  | { ok: false; error: E };

/**
 * Why a subscribed stream stopped.
 *
 * `gap` is DESIGN §6's terminal `charpente.gap` frame: the tail fell behind
 * retention, so events the server no longer holds were skipped. It is a
 * DELIBERATE, NAMED refusal, not a dropped socket — a client that reconnected
 * on it would resume past the hole and deliver a history with a silent
 * discontinuity in it.
 *
 * `closed` is the ordinary end of the response body.
 *
 * A REFUSED CURSOR is deliberately not a member here. It happens before any
 * frame arrives, so nothing has ended, and handling it in the same branch as a
 * normal end is the mistake; it is raised as `CursorDiverged` instead. Every
 * member of this union has a producer in `ApiBase.stream` below.
 */
export type StreamEnd =
  | { kind: "gap"; detail: string }
  | { kind: "closed" };

/**
 * The server refused the cursor as cross-timeline: HTTP 409 (DESIGN §6). The
 * rule is in the message on purpose — it is what a developer reads at 3am.
 */
export class CursorDiverged extends Error {
  constructor(readonly detail: string) {
    super(
      "cursor diverged: " + detail + " — a client must not auto-resume across a " +
      "divergence: the history behind that cursor came from another primary, so " +
      "resuming would deliver events from another timeline under ids this client " +
      "has already used. That silent misattribution is what the cursor's seq half " +
      "exists to prevent. Re-sync current state through reads, then reconnect " +
      "without a cursor.",
    );
  }
}

/**
 * A subscribe the server rejected before any frame: 401 with no credential, or
 * 400 on a cursor it could not even parse. Distinct from `CursorDiverged`,
 * which is a parseable cursor from a history this node does not have.
 */
export class StreamRejected extends Error {
  constructor(readonly status: number, readonly detail: string) {
    super("subscribe rejected with HTTP " + status + ": " + detail);
  }
}

/**
 * The event shape of a stream route that declared no `emits(...)`. The
 * contract genuinely does not know what such a route carries, so this is the
 * honest type rather than a fabricated union. To get a discriminated union
 * here, add `emits(...)` to that route: `#[route(SSE "/path", emits(Ev, …))]`.
 */
export type UnknownEvent = { event: string; data: unknown };

export interface SubscribeOptions<Ev> {
  /** A `Cursor::wire` string from a previous `onEvent`, to resume from. */
  cursor?: string;
  onEvent: (ev: Ev, cursor: string) => void;
  onEnd: (end: StreamEnd) => void;
  signal?: AbortSignal;
}

export class ApiBase {
  constructor(protected baseUrl: string, protected token?: string) {}

  setToken(token: string | undefined) {
    this.token = token;
  }

  protected headers(json: boolean): Record<string, string> {
    const h: Record<string, string> = {};
    if (json) h["content-type"] = "application/json";
    if (this.token) h["authorization"] = "Bearer " + this.token;
    return h;
  }

  protected async request<T, E>(
    method: string,
    path: string,
    body?: unknown,
  ): Promise<Result<T, E>> {
    const res = await fetch(this.baseUrl + path, {
      method,
      headers: this.headers(body !== undefined),
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    const text = await res.text();
    // A non-JSON body is a protocol violation, not an `E`: this throws rather
    // than coercing whatever arrived into the route's declared error type.
    const parsed = text.length ? JSON.parse(text) : null;
    return res.ok
      ? { ok: true, data: parsed as T }
      : { ok: false, error: parsed as E };
  }

  // Fetch-based, NOT EventSource: EventSource cannot set an Authorization
  // header, and the alternative is a token in a query string, where it lands
  // in logs and referrers. The cost is that reconnect is owed explicitly.
  protected async stream<Ev extends { event: string; data: unknown }>(
    path: string,
    opts: SubscribeOptions<Ev>,
  ): Promise<void> {
    const headers = this.headers(false);
    if (opts.cursor) headers["last-event-id"] = opts.cursor;
    const res = await fetch(this.baseUrl + path, { headers, signal: opts.signal });
    if (res.status === 409) {
      // The server refused the cursor. Surfaced, never retried: see
      // CursorDiverged.
      throw new CursorDiverged(await res.text());
    }
    if (!res.ok) throw new StreamRejected(res.status, await res.text());
    if (!res.body) {
      throw new StreamRejected(res.status, "the subscribe response carried no body");
    }
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buf = "";
    for (;;) {
      const chunk = await reader.read();
      if (chunk.done) {
        opts.onEnd({ kind: "closed" });
        return;
      }
      buf += decoder.decode(chunk.value, { stream: true });
      let idx: number;
      while ((idx = buf.indexOf("\n\n")) >= 0) {
        const frame = buf.slice(0, idx);
        buf = buf.slice(idx + 2);
        let name = "message";
        let id = "";
        let data: string | undefined;
        for (const line of frame.split("\n")) {
          // A line starting with ':' is an SSE comment. charpente sends `:hb`
          // every 15s on an idle connection; reading it as a field would hand
          // JSON.parse an empty string on every heartbeat.
          if (line.startsWith(":")) continue;
          if (line.startsWith("event:")) name = sseValue(line, 6);
          else if (line.startsWith("id:")) id = sseValue(line, 3);
          else if (line.startsWith("data:")) {
            const v = sseValue(line, 5);
            data = data === undefined ? v : data + "\n" + v;
          }
        }
        // No data field at all (a lone comment, a keepalive): nothing to
        // dispatch, and nothing has gone wrong.
        if (data === undefined) continue;
        if (name === "charpente.gap") {
          opts.onEnd({ kind: "gap", detail: data });
          return;
        }
        opts.onEvent({ event: name, data: JSON.parse(data) } as Ev, id);
      }
    }
  }
}

/** One SSE field value: the text after the colon, minus one optional leading space. */
function sseValue(line: string, at: number): string {
  const v = line.slice(at);
  return v.startsWith(" ") ? v.slice(1) : v;
}
"##;
