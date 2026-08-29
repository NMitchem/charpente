// The SSE half of the inbound `Int64` revive (chunk B1c). `request()`'s side
// landed in B1b; `stream()` still did one untyped `JSON.parse` per frame, so a
// declared event's `Int64` payload arrived as a decimal STRING under a type
// that says `bigint` — the same class of lie chunk B0's oracle exists to catch,
// on the one path B1b did not reach.
//
// Every frame below is a REAL `serde_json` document from `generated/samples.ts`
// where one exists (`invoicePaid`, `invoiceVoided`), never a hand-written
// stand-in: the three-way tie (Rust serialization <-> committed literal <->
// client runtime behaviour) is what makes these oracles rather than assertions
// about a stub. The two frames that deliberately are NOT samples are the
// UNDECLARED ones — the contract has no such event by construction, so no
// generated literal for one can exist.

import { test } from "node:test";
import * as assert from "node:assert/strict";
import { Api, UndeclaredEvent } from "./generated/client";
import type { UnknownEvent } from "./generated/client";
import { invoicePaid, invoiceVoided } from "./generated/samples";
import { stubSseResponse } from "./testing/fetch-stub";

/** A frame name `/feed` did not declare, with an `Int64`-SHAPED value at the
 * key `InvoicePaid` uses. The shape is deliberate: it is exactly what a
 * revive-everything dispatch would convert, and the client has no schema for
 * an undeclared event, so converting it would be inventing one. */
const UNDECLARED = {
  event: "invoice_refunded",
  data: { invoice: "7", amount_cents: "9007199254740993" },
};

test("an SSE frame's Int64 revives to a bigint and supports arithmetic", async () => {
  const { restore } = stubSseResponse([
    { event: "invoice_paid", data: invoicePaid, id: "12.3" },
  ]);
  const seen: string[] = [];
  let kind: string | undefined;
  let doubled: bigint | undefined;
  let cursor: string | undefined;
  try {
    const api = new Api("http://fixture.invalid");
    await api.feed({
      onEvent: (ev, at) => {
        seen.push(ev.event);
        if (ev.event !== "invoice_paid") return;
        cursor = at;
        kind = typeof ev.data.amount_cents;
        // The load-bearing line. `String(x)` would compile and pass for a
        // string; `x * 2n` throws TypeError ("Cannot mix BigInt and other
        // types") on anything that is not really a bigint, which is what this
        // path handed callers before B1c.
        doubled = ev.data.amount_cents * 2n;
      },
      onEnd: () => {},
    });
  } finally {
    restore();
  }
  // The floor: an `onEvent` that never fired leaves every assertion below
  // unexecuted, and a test that asserts only inside a callback is green when
  // nothing dispatches at all.
  assert.deepEqual(seen, ["invoice_paid"]);
  assert.equal(kind, "bigint");
  // u64 above 2^53 — 9007199254740993 is the value serde wrote, and the one a
  // JSON number could not have carried back.
  assert.equal(doubled, 18014398509481986n);
  assert.equal(cursor, "12.3");
});

// A1 (chunk-b1c-brief.md): the dispatch map's key set is the DECLARED event
// list, independent of which payloads need reviving. `invoice_voided` is this
// fixture's declared event whose payload carries no `Int64` at all — an
// emitter that skipped a map entry for it would turn a declared event into an
// undeclared one and raise `UndeclaredEvent` on a frame the route declared.
// `crates/ts-fixture-app/tests/drift.rs` carries the coverage floor keeping
// such an event in the contract.
test("a declared event whose payload carries no Int64 still dispatches", async () => {
  const { restore } = stubSseResponse([
    { event: "invoice_voided", data: invoiceVoided },
  ]);
  const seen: string[] = [];
  let payload: unknown;
  try {
    const api = new Api("http://fixture.invalid");
    await api.feed({
      onEvent: (ev) => {
        seen.push(ev.event);
        payload = ev.data;
      },
      onEnd: () => {},
    });
  } finally {
    restore();
  }
  assert.deepEqual(seen, ["invoice_voided"]);
  // Passed through verbatim: nothing to revive means nothing touched, not
  // "walked by an identity function that might have dropped a field".
  assert.deepEqual(payload, invoiceVoided);
});

test("an undeclared frame still raises UndeclaredEvent when no onUnknown is given", async () => {
  const { restore } = stubSseResponse([UNDECLARED]);
  const seen: string[] = [];
  let caught: unknown;
  try {
    const api = new Api("http://fixture.invalid");
    await api.feed({
      onEvent: (ev) => {
        seen.push(ev.event);
      },
      onEnd: () => {},
    });
  } catch (e) {
    caught = e;
  } finally {
    restore();
  }
  assert.ok(
    caught instanceof UndeclaredEvent,
    `an undeclared frame must reject the subscribe promise: ${String(caught)}`,
  );
  assert.equal((caught as UndeclaredEvent).event, UNDECLARED.event);
  // Never cast into the union on the way out: the refusal is by NAME, before
  // anything reaches `onEvent`.
  assert.deepEqual(seen, []);
});

// C5: the revive runs between the declared-list guard and the union cast, so a
// frame that fails the guard is handed to `onUnknown` exactly as `JSON.parse`
// produced it. Reviving it would mean applying a schema the client does not
// have for that event — which is precisely what makes it unknown.
//
// What this test observes is the VALUE `onUnknown` receives, not the source
// order. Measured: moving the walker above the guard leaves this test green,
// because the guard and the walker read the same map and `get(name)` on a name
// `has(name)` just rejected is `undefined`. The source order is pinned instead
// by `an_undeclared_event_name_is_refused_by_name_rather_than_cast_into_the_union`
// in `crates/charpente-tsgen/tests/routes.rs`. This test reddens on any OTHER
// mechanism that would revive an undeclared payload — a walker chosen by shape
// rather than by name, say, or a blanket pass over every string that parses as
// an integer.
test("an undeclared frame goes to onUnknown un-revived when one is supplied", async () => {
  const { restore } = stubSseResponse([{ ...UNDECLARED, id: "42.7" }]);
  const unknown: UnknownEvent[] = [];
  const seen: string[] = [];
  let at: string | undefined;
  let ended: string | undefined;
  try {
    const api = new Api("http://fixture.invalid");
    await api.feed({
      onEvent: (ev) => {
        seen.push(ev.event);
      },
      onUnknown: (ev, cursor) => {
        unknown.push(ev);
        at = cursor;
      },
      onEnd: (end) => {
        ended = end.kind;
      },
    });
  } finally {
    restore();
  }
  assert.deepEqual(seen, []);
  assert.equal(unknown.length, 1);
  assert.equal(unknown[0].event, UNDECLARED.event);
  assert.deepEqual(unknown[0].data, UNDECLARED.data);
  assert.equal(
    typeof (unknown[0].data as { amount_cents: unknown }).amount_cents,
    "string",
    "an undeclared frame carries no schema this client can revive against",
  );
  assert.equal(at, "42.7");
  // Tolerated, not fatal: the stream runs to its ordinary end.
  assert.equal(ended, "closed");
});

// C3 / exit criterion 10. `raw_feed` declares no `emits(...)`, so the generated
// subscribe passes NO third argument at all — a case distinct from "declared a
// list and this name is not in it". An empty dispatch map would be TRUTHY and
// would convert this route from *accepts everything* to *rejects everything*.
test("a route that declared no events accepts an arbitrary frame name", async () => {
  const { restore } = stubSseResponse([
    { event: "whatever_the_server_sends", data: { anything: 1 }, id: "9.1" },
  ]);
  const seen: UnknownEvent[] = [];
  let ended: string | undefined;
  try {
    const api = new Api("http://fixture.invalid");
    // No `onUnknown` on purpose: a route that declared nothing must not need
    // one, and this rejects the promise if any frame is refused.
    await api.rawFeed({
      onEvent: (ev) => {
        seen.push(ev);
      },
      onEnd: (end) => {
        ended = end.kind;
      },
    });
  } finally {
    restore();
  }
  assert.equal(seen.length, 1);
  assert.equal(seen[0].event, "whatever_the_server_sends");
  assert.deepEqual(seen[0].data, { anything: 1 });
  assert.equal(ended, "closed");
});
