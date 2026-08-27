// Exit criterion 5 (chunk-b1b-brief.md): an `Int64` in an ERROR position
// must revive the same way a response position does. `withdrawInvoice`'s
// generated method wraps BOTH `r.data` and `r.error` in the same
// `r.ok ? {...} : {...}` shape (crates/charpente-tsgen/src/routes.rs), so an
// error-position `Int64` that never revived would be a real gap this test
// closes, not a decorative mirror of int64.test.ts.
//
// `limitExceeded` (generated/samples.ts) is a real serde_json document for
// `LimitExceeded { max_cents: WireI64 }` — see that struct's doc comment in
// crates/ts-fixture-app/src/lib.rs for what this specimen does and does not
// prove about a real running server (the route macro's glue always sends
// `ApiError`'s envelope in practice; this fixture never invokes a handler
// for real, so that gap is orthogonal to what's under test here).

import { test } from "node:test";
import * as assert from "node:assert/strict";
import { Api } from "./generated/client";
import type { InvoiceId } from "./generated/client";
import { limitExceeded } from "./generated/samples";
import { stubJsonResponse } from "./testing/fetch-stub";

test("Int64 arithmetic on a real client ERROR response", async () => {
  const { restore } = stubJsonResponse(limitExceeded, 400);
  try {
    const api = new Api("http://fixture.invalid");
    const result = await api.withdrawInvoice("invoice-1" as InvoiceId);
    assert.equal(result.ok, false);
    if (result.ok) throw new Error("unreachable: stub always returns 400");

    // `maxCents` is declared `bigint` on `LimitExceeded`. `withdrawInvoice`'s
    // generated method revives the error position too (`reviveLimitExceeded`,
    // called on `r.error`), so this is real arithmetic on a real `bigint` --
    // the assertion this test exists for is that error positions are not a
    // gap: had they been, this would throw `TypeError: Cannot mix BigInt and
    // other types, use explicit conversions` instead of returning
    // 18014398509481986n.
    assert.equal(result.error.maxCents * 2n, 18014398509481986n);
  } finally {
    restore();
  }
});
