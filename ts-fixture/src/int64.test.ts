// The Int64 gap's original oracle (chunk-b0-brief.md A1), RED from B0
// through B1a, GREEN as of chunk B1b (the inbound walker,
// crates/charpente-tsgen/src/revive.rs).
//
// `InvoiceView.totalCents` is typed `bigint` (generated/client.ts) and the
// wire carries a decimal STRING (generated/samples.ts — a real serde_json
// document, "totalCents":"9007199254740993", 2^53+1: the value a JSON number
// would corrupt). `getInvoice`'s generated method now revives it before
// returning (`reviveInvoiceView`, called from the method body, not from
// `request()` itself — B1a's decision) — so the arithmetic below is real,
// not thrown away.
//
// This test feeds the real `Api` client the real sample document through a
// stubbed `fetch`, then does arithmetic the generated type says is legal.
// DO NOT weaken it or mark it skipped — it is what going green LOOKS like
// for the whole sub-project's Int64 gap.

import { test } from "node:test";
import * as assert from "node:assert/strict";
import { Api } from "./generated/client";
import type { InvoiceId } from "./generated/client";
import { invoiceView } from "./generated/samples";
import { stubJsonResponse } from "./testing/fetch-stub";

test("Int64 arithmetic on a real client response", async () => {
  const { restore } = stubJsonResponse(invoiceView);
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
