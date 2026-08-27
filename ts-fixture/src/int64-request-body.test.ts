// Chunk B1a's oracle: the OUTBOUND half of the Int64 gap (chunk-b1a-brief.md).
//
// `AdjustInvoiceBody` (generated/client.ts) has one field, `deltaCents:
// bigint`. Before the replacer at charpente-tsgen/src/runtime.rs's
// `request()`, `JSON.stringify(body)` throws synchronously on that bigint —
// `fetch` is never even reached, so this test's stub only needs to exist to
// prove the request never gets that far. See chunk-b1a-report.md for the
// verbatim RED output this test produced against the unfixed emitter.
//
// This is deliberately a DIFFERENT file from int64.test.ts, which pins the
// INBOUND gap (chunk B1b's target) and must stay red until that chunk lands.

import { test } from "node:test";
import * as assert from "node:assert/strict";
import { Api } from "./generated/client";
import type { InvoiceId } from "./generated/client";
import { adjustInvoiceBody, invoiceView } from "./generated/samples";
import { stubJsonResponse } from "./testing/fetch-stub";

test("an Int64 in a request body serializes to its decimal string, not a thrown TypeError", async () => {
  const { restore, calls } = stubJsonResponse(invoiceView);
  try {
    const api = new Api("http://fixture.invalid");
    const result = await api.adjustInvoice(invoiceView.id as InvoiceId, {
      deltaCents: 9_007_199_254_740_993n,
    });
    assert.equal(result.ok, true);

    // Which of these three assertions actually catches THIS bug: only the
    // `deepEqual`. Under the unfixed emitter `JSON.stringify` throws before
    // `fetch` runs, so the two `assert.equal` lines are never reached -- the
    // test fails by uncaught exception, and they are regression guards for
    // other bugs (a double-send, a broken `res.ok` ternary), not oracles for
    // this one. The `deepEqual` is the line that survives the near-miss fix:
    // a replacer using `Number(v)` instead of `v.toString()` would NOT throw,
    // would reach here, and would fail on the JSON-number-vs-string mismatch.
    //
    // The exact bytes that reached `fetch`: not the JS number
    // 9007199254740993 (which would silently corrupt any value above 2^53,
    // the whole reason Int64 is a string on the wire) and not corrupted some
    // other way -- the SAME decimal string serde produces server-side for the
    // identical value (`adjustInvoiceBody`, a real serde_json document).
    assert.equal(calls.length, 1);
    assert.deepEqual(JSON.parse(calls[0].body as string), adjustInvoiceBody);
  } finally {
    restore();
  }
});
