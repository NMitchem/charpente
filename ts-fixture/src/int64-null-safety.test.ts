// Exit criterion 2 (chunk-b1b-brief.md): `Option(Int64)` and an optional
// (`skip_serializing_if`) `Int64` field must both round-trip a null/absent
// value WITHOUT throwing. This is A1's landmine, executing: `BigInt(null)`
// and `BigInt(undefined)` both throw `TypeError`, so a walker that called
// `BigInt()` unconditionally would break the client on `invoiceView` itself
// — a document the server was correct to send.
//
// `invoiceView` (generated/samples.ts, a real serde_json document) already
// carries BOTH shapes on its ordinary path, not a contrived one:
// `adjustmentCents` is `null` (an `Option<WireI64>` with no adjustment —
// the key is present, the value is JSON `null`), and `lateFeeCents` is
// OMITTED entirely (`skip_serializing_if` on a bare, non-Option `WireI64`
// that happens to be zero) — a different runtime shape from `null`: property
// access on an omitted key hands the revive walker `undefined`. A third,
// `creditNoteCents`, combines the two (`skip_serializing_if` over an
// `Option<WireI64>`) and is the only shape that reaches the walker's
// conditional-spread branch.
//
// The test completing at all (no uncaught TypeError) IS the primary oracle:
// under a walker that skipped the null-guard, `getInvoice` would throw
// synchronously inside `reviveInvoiceView`, before this file's `assert.equal`
// calls ever ran. The equality checks pin the exact resulting values on top
// of that.

import { test } from "node:test";
import * as assert from "node:assert/strict";
import { Api } from "./generated/client";
import type { InvoiceId } from "./generated/client";
import { invoiceView } from "./generated/samples";
import { stubJsonResponse } from "./testing/fetch-stub";

test("Option(Int64) and an optional bare Int64 both round-trip null/absent without throwing", async () => {
  const { restore } = stubJsonResponse(invoiceView);
  try {
    const api = new Api("http://fixture.invalid");
    const result = await api.getInvoice(invoiceView.id as InvoiceId);
    assert.equal(result.ok, true);
    if (!result.ok) throw new Error("unreachable: stub always returns 200");

    // `Option<WireI64>`, present key, JSON `null`.
    assert.equal(result.data.adjustmentCents, null);
    // `skip_serializing_if` on a bare `WireI64` (no `Option` wrapper):
    // `late_fee_cents: WireI64(0)` in samples.rs omits the key entirely, so
    // this reads `undefined`, not `null` and not `0n`.
    assert.equal(result.data.lateFeeCents, undefined);

    // `skip_serializing_if` over an `Option<WireI64>` -- the ONE shape that
    // makes the walker emit its conditional spread, and the shape that had no
    // executing coverage at all until the fix-round-1 re-review found it (N1).
    // `in` rather than `== undefined` on purpose: the bug it pins is the key
    // being MATERIALIZED, so a walker that spread `{creditNoteCents: null}`
    // over an omitted key passes an equality check against `null` while
    // reporting "the server sent null" for "the server sent nothing" -- the
    // exact distinction `skip_serializing_if` exists to express.
    assert.ok(
      !("creditNoteCents" in result.data),
      "an omitted key must stay omitted, not be materialized on the object (as null or as undefined)",
    );

    // The type still promises `bigint`, so a caller doing arithmetic on the
    // ORDINARY (present) Int64 positions on the SAME document must still
    // work — this null-safety fix must not have broken the common case.
    assert.equal(result.data.totalCents * 2n, 18014398509481986n);
  } finally {
    restore();
  }
});
