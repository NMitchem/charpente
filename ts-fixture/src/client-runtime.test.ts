// Proves the harness itself: `fetch-stub.ts` + the real `Api` client + real
// `generated/samples.ts` documents, exercised end to end, expected to stay
// GREEN. This is what chunk-b0-brief.md means by "a separate trivially-true
// [test], alongside" the RED Int64 test — except bound to real samples rather
// than a throwaway assertion, so a broken stub (not a broken client) is also
// caught here rather than surfacing as a confusing failure in int64.test.ts.
//
// Neither test here does bigint arithmetic, so neither observes the Int64 gap
// int64.test.ts (request path) and int64-sse.test.ts (stream path) pin — those
// two do the arithmetic that only a real bigint survives. What the SSE round
// trip below adds is the CONVERSION at the boundary: `amount_cents` comes off
// the wire as a decimal string and must reach the caller as `BigInt(that)`, so
// it is compared against the sample's own value converted, never against the
// raw literal (which is what it was before chunk B1c revived this path, and
// what would silently be true again if the revive were dropped).
//
// The JSON round trip also asserts on the stub's recorded call (`calls[0]`) —
// proof that request-body capture itself works, on a real body
// (`createCustomerBody`), not just that responses can be served.

import { test } from "node:test";
import * as assert from "node:assert/strict";
import { Api } from "./generated/client";
import type { CustomerId, StreamEnd } from "./generated/client";
import {
  createCustomerBody,
  customerView,
  invoicePaid,
  invoiceVoided,
} from "./generated/samples";
import { stubJsonResponse, stubSseResponse } from "./testing/fetch-stub";

test("JSON round trip: a real sample document survives request()", async () => {
  const { restore, calls } = stubJsonResponse(customerView);
  try {
    const api = new Api("http://fixture.invalid");
    const result = await api.createCustomer(
      "c-1" as CustomerId,
      createCustomerBody,
    );
    assert.equal(result.ok, true);
    if (!result.ok) throw new Error("unreachable: stub always returns 200");
    assert.equal(result.data.id, customerView.id);
    assert.equal(result.data.displayName, customerView.displayName);
    assert.equal(result.data.emailAddress, customerView.emailAddress);
    assert.equal(result.data.status, customerView.status);

    // The stub captured what `request()` actually sent — proof the capture
    // itself works, on a real request body.
    assert.equal(calls.length, 1);
    assert.equal(calls[0].method, "POST");
    assert.deepEqual(
      JSON.parse(calls[0].body as string),
      createCustomerBody,
    );
  } finally {
    restore();
  }
});

test("SSE round trip: real sample frames dispatch through stream()", async () => {
  const { restore } = stubSseResponse([
    { event: "invoice_paid", data: invoicePaid },
    { event: "invoice_voided", data: invoiceVoided },
  ]);
  const seen: string[] = [];
  let ended: string | undefined;
  try {
    const api = new Api("http://fixture.invalid");
    await api.feed({
      onEvent: (ev) => {
        seen.push(ev.event);
        if (ev.event === "invoice_paid") {
          assert.equal(ev.data.invoice, invoicePaid.invoice);
          assert.equal(ev.data.amount_cents, BigInt(invoicePaid.amount_cents));
        } else {
          assert.equal(ev.data.invoice, invoiceVoided.invoice);
        }
      },
      onEnd: (end) => {
        ended = end.kind;
      },
    });
  } finally {
    restore();
  }
  assert.deepEqual(seen, ["invoice_paid", "invoice_voided"]);
  assert.equal(ended, "closed");
});

test("a reshaped frame ends the stream by name, and nothing after it is delivered", async () => {
  const detail = {
    event: "invoice_paid",
    id: 1,
    fix: "re-sync current state through reads, then reconnect without a cursor",
  };
  const { restore } = stubSseResponse([
    { event: "invoice_paid", data: invoicePaid, id: "0.1" },
    { event: "charpente.reshaped", data: detail, id: "1.2" },
    { event: "invoice_voided", data: invoiceVoided, id: "2.3" },
  ]);
  const seen: string[] = [];
  let ended: StreamEnd | undefined;
  try {
    await new Api("http://fixture.invalid").feed({
      onEvent: (ev) => {
        seen.push(ev.event);
      },
      onEnd: (end) => {
        ended = end;
      },
    });
  } finally {
    restore();
  }
  assert.deepEqual(seen, ["invoice_paid"]);
  assert.equal(ended?.kind, "reshaped");
  assert.deepEqual(JSON.parse((ended as { detail: string }).detail), detail);
});

// The `id` reset to "" every frame (runtime.rs) is what makes a resume
// cursor absent for `charpente.error`, which carries no `id:` line on the
// wire (spec A7). That guarantee only holds if this frame never reaches
// `onEvent`/`onUnknown` at all — a caller cannot store an id it was never
// handed a callback with. `onUnknown` below records every call it gets so
// this test can assert there are none, rather than relying on `seen`'s
// shape alone to notice a regression.
test("an error frame ends the stream by name, and nothing after it is delivered", async () => {
  const detail = { event: "invoice_paid", id: 1, error: "event invoice_paid failed JSON encoding" };
  const { restore } = stubSseResponse([
    { event: "invoice_paid", data: invoicePaid, id: "0.1" },
    { event: "charpente.error", data: detail },
    { event: "invoice_voided", data: invoiceVoided, id: "2.3" },
  ]);
  const seen: string[] = [];
  const unknown: string[] = [];
  let ended: StreamEnd | undefined;
  try {
    await new Api("http://fixture.invalid").feed({
      onEvent: (ev) => {
        seen.push(ev.event);
      },
      onUnknown: (ev) => {
        unknown.push(ev.event);
      },
      onEnd: (end) => {
        ended = end;
      },
    });
  } finally {
    restore();
  }
  assert.deepEqual(seen, ["invoice_paid"]);
  assert.deepEqual(unknown, []);
  assert.equal(ended?.kind, "error");
  assert.deepEqual(JSON.parse((ended as { detail: string }).detail), detail);
});
