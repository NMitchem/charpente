// Fix round 1, F6: `EnumRepr::Internal` (`Payment::Card.limit_cents`) and
// `EnumRepr::Adjacent` (`Notification::Progress.bytes_done`) carrying an
// `Int64` each had coverage only from a `charpente-tsgen` Rust-mechanical
// test asserting on generated SOURCE TEXT -- never compiled by `tsc`, never
// executed. `invoiceView.payment`/`.notice` (see `Payment`/`Notification`'s
// doc comments in crates/ts-fixture-app/src/lib.rs) are real serde
// documents carrying both; this file compiles and executes both revivers.

import { test } from "node:test";
import * as assert from "node:assert/strict";
import { Api } from "./generated/client";
import type { InvoiceId } from "./generated/client";
import { invoiceView } from "./generated/samples";
import { stubJsonResponse } from "./testing/fetch-stub";

test("Internal-tagged and Adjacent-tagged enum bodies both revive their Int64 field", async () => {
  const { restore } = stubJsonResponse(invoiceView);
  try {
    const api = new Api("http://fixture.invalid");
    const result = await api.getInvoice(invoiceView.id as InvoiceId);
    assert.equal(result.ok, true);
    if (!result.ok) throw new Error("unreachable: stub always returns 200");

    // EnumRepr::Internal: the tag ("type") merges into the same object as
    // the field the revive must convert -- no `content` wrapper to guard.
    const payment = result.data.payment;
    assert.equal(payment.type, "Card");
    if (payment.type !== "Card") throw new Error("unreachable: stub is always Card");
    assert.equal(payment.limit_cents * 2n, 18014398509481986n);

    // EnumRepr::Adjacent: the tag ("kind-of") and the payload ("payload")
    // are two SEPARATE keys -- the revive must reach through the nested
    // `payload` object to convert `bytes_done`.
    const notice = result.data.notice;
    assert.equal(notice["kind-of"], "Progress");
    if (notice["kind-of"] !== "Progress") throw new Error("unreachable: stub is always Progress");
    assert.equal(notice.payload.bytes_done * 2n, 18014398509481986n);
  } finally {
    restore();
  }
});
