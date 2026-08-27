// RED until chunk B1 — see chunk-b1-brief.md.
//
// `InvoiceView.totalCents` is typed `bigint` (generated/client.ts:39) but the
// wire carries a decimal STRING (generated/samples.ts:40 — a real serde_json
// document, "totalCents":"9007199254740993", 2^53+1: the value a JSON number
// would corrupt). Nothing in the generated client converts it — runtime.rs's
// `request()` does `JSON.parse(text) as T` with no revival — so at runtime
// this field holds a string while the type promises a bigint.
//
// This test IS that gap, executing: it feeds the real `Api` client the real
// sample document through a stubbed `fetch`, then does arithmetic the
// generated type says is legal. It throws.
//
// DO NOT fix this test, weaken it, or mark it skipped. It is chunk B1's
// starting point (chunk-b0-brief.md A1) — wiring it green is B1's job.

import { test } from "node:test";
import * as assert from "node:assert/strict";
import { Api } from "./generated/client";
import type { InvoiceId } from "./generated/client";
import { invoiceView } from "./generated/samples";
import { stubJsonResponse } from "./testing/fetch-stub";

test("Int64 arithmetic on a real client response (RED until B1)", async () => {
  const restore = stubJsonResponse(invoiceView);
  try {
    const api = new Api("http://fixture.invalid");
    const result = await api.getInvoice(invoiceView.id as InvoiceId);
    assert.equal(result.ok, true);
    if (!result.ok) throw new Error("unreachable: stub always returns 200");

    // `totalCents` is declared `bigint`. At runtime it is still the wire
    // string "9007199254740993", because nothing revives it — so this throws
    // `TypeError: Cannot mix BigInt and other types, use explicit conversions`
    // instead of returning 18014398509481986n.
    assert.equal(result.data.totalCents * 2n, 18014398509481986n);
  } finally {
    restore();
  }
});
