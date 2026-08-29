// Fix round 1, F6: `EnumRepr::External`'s two dangerous shapes -- a
// newtype variant carrying an `Int64` (`Ack`), and the coexisting UNIT
// variant that is a bare JSON string (`Ping`) rather than an object, which
// the revive's `typeof v === "object"` guard exists to avoid throwing on
// (`"X" in v` throws when `v` is a primitive string) -- had coverage only
// from a `charpente-tsgen` Rust-mechanical test asserting on generated
// SOURCE TEXT. Every route that reaches `FeedItem` before this one nested it
// inside `InvoiceView.lastItem`, always constructed as the `Message`
// variant, so neither dangerous branch had ever actually run.
//
// `GET /invoices/:id/last-item` (crates/ts-fixture-app/src/lib.rs) returns a
// BARE `FeedItem`, so this file can drive both branches directly with the
// two existing specimens `feedItemAck`/`feedItemPing` (generated/samples.ts)
// without touching `invoiceView`'s own shared `Message` value.

import { test } from "node:test";
import * as assert from "node:assert/strict";
import { Api } from "./generated/client";
import type { InvoiceId } from "./generated/client";
import { feedItemAck, feedItemPing } from "./generated/samples";
import { stubJsonResponse } from "./testing/fetch-stub";

test("an externally-tagged newtype variant carrying Int64 revives and supports arithmetic", async () => {
  const { restore } = stubJsonResponse(feedItemAck);
  try {
    const api = new Api("http://fixture.invalid");
    const result = await api.lastItem("invoice-1" as InvoiceId);
    assert.equal(result.ok, true);
    if (!result.ok) throw new Error("unreachable: stub always returns 200");
    assert.ok(result.data !== "Ping" && "Ack" in result.data);
    if (result.data === "Ping" || !("Ack" in result.data)) {
      throw new Error("unreachable: stub is always Ack");
    }
    assert.equal(result.data.Ack * 2n, 18014398509481986n);
  } finally {
    restore();
  }
});

test("a coexisting unit variant (a bare JSON string) does not throw through the object guard", async () => {
  const { restore } = stubJsonResponse(feedItemPing);
  try {
    const api = new Api("http://fixture.invalid");
    const result = await api.lastItem("invoice-1" as InvoiceId);
    assert.equal(result.ok, true);
    if (!result.ok) throw new Error("unreachable: stub always returns 200");
    // If `reviveFeedItem` ever ran `"Ack" in v` unconditionally (no
    // `typeof v === "object"` guard first), THIS is where it would throw
    // `TypeError: Cannot use 'in' operator to search for 'Ack' in Ping` --
    // reaching the equality check below is the proof it does not.
    assert.equal(result.data, "Ping");
  } finally {
    restore();
  }
});
