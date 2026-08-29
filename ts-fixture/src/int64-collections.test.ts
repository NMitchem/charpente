// Fix round 1, F6: four `TypeSchema` shapes -- `Seq(Int64)`, `Map` value
// `= Int64`, `Option(Seq(Int64))`, `Seq(Option(Int64))` -- had coverage only
// from `charpente-tsgen`'s Rust-mechanical tests, which assert on GENERATED
// SOURCE TEXT (a string the emitter produced, compared against a string the
// same author wrote). Nothing anywhere compiled that output with `tsc` or
// executed it. `invoiceView`'s new `installmentCents`/`feesByCategory`/
// `refundBatchesCents`/`disputedLineCents` fields (see `InvoiceView`'s doc
// comment in crates/ts-fixture-app/src/lib.rs) close that gap: this file
// compiles the generated walker for real (`npm run check`) and executes it
// against a real serde document (`npm run test`).

import { test } from "node:test";
import * as assert from "node:assert/strict";
import { Api } from "./generated/client";
import type { InvoiceId } from "./generated/client";
import { invoiceView } from "./generated/samples";
import { stubJsonResponse } from "./testing/fetch-stub";

test("Seq(Int64), Map value, Option(Seq), and Seq(Option) all revive on a real client response", async () => {
  const { restore } = stubJsonResponse(invoiceView);
  try {
    const api = new Api("http://fixture.invalid");
    const result = await api.getInvoice(invoiceView.id as InvoiceId);
    assert.equal(result.ok, true);
    if (!result.ok) throw new Error("unreachable: stub always returns 200");
    // A local alias, not just style: narrowing on a PROPERTY (`result.data`)
    // does not reliably survive an intervening function call the way
    // narrowing on a plain local variable does -- `data` below is the latter.
    const data = result.data;

    // Seq(Int64): every element converts, not just the first.
    assert.equal(data.installmentCents[0] * 2n, 18014398509481986n);
    assert.equal(data.installmentCents[1] * 2n, -1000n);

    // Map value = Int64: the VALUE converts; the key stays a plain string
    // (a Map key is never part of the schema -- `types.rs` renders
    // `Record<string, T>` -- so there is nothing to revive on it).
    assert.equal(data.feesByCategory["late"] * 2n, 18014398509481986n);

    // Option(Seq(Int64)): present here, so the inner Seq must still map
    // every element once the outer null-guard lets it through.
    const refundBatches = data.refundBatchesCents;
    if (refundBatches === null) throw new Error("unreachable: stub always sends a batch");
    assert.equal(refundBatches[0] * 2n, 18014398509481986n);

    // Seq(Option(Int64)): one real value, one null element in the same
    // array -- the inner guard must apply PER ELEMENT, not once for the
    // whole array.
    const [disputedFirst, disputedSecond] = data.disputedLineCents;
    if (disputedFirst === null) throw new Error("unreachable: stub's first element is a value");
    assert.equal(disputedFirst * 2n, 18014398509481986n);
    assert.equal(disputedSecond, null);
  } finally {
    restore();
  }
});
