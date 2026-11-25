// A generated write sends an id the server needs (GUIDE §5): a fresh one per
// call, or the caller's own so a retry is the same write.

import { test } from "node:test";
import * as assert from "node:assert/strict";
import { Api } from "./generated/client";
import type { CustomerId, InvoiceId } from "./generated/client";
import { createCustomerBody, customerView, invoiceView } from "./generated/samples";
import { stubJsonResponse } from "./testing/fetch-stub";

const id = customerView.id as CustomerId;

test("a write sends an idempotency-key of 32 lowercase hex characters", async () => {
  const { restore, calls } = stubJsonResponse(customerView);
  try {
    await new Api("http://fixture.invalid").createCustomer(id, createCustomerBody);
    assert.ok(/^[0-9a-f]{32}$/.test(calls[0]!.headers["idempotency-key"] ?? ""));
  } finally {
    restore();
  }
});

test("two writes without an intent send different keys", async () => {
  const { restore, calls } = stubJsonResponse(customerView);
  try {
    const api = new Api("http://fixture.invalid");
    await api.createCustomer(id, createCustomerBody);
    await api.createCustomer(id, createCustomerBody);
    assert.ok(calls[0]!.headers["idempotency-key"] !== calls[1]!.headers["idempotency-key"]);
  } finally {
    restore();
  }
});

test("two writes with the same intent send exactly that key", async () => {
  const { restore, calls } = stubJsonResponse(customerView);
  try {
    const api = new Api("http://fixture.invalid");
    await api.createCustomer(id, createCustomerBody, "retry-me-1");
    await api.createCustomer(id, createCustomerBody, "retry-me-1");
    assert.equal(calls[0]!.headers["idempotency-key"], "retry-me-1");
    assert.equal(calls[1]!.headers["idempotency-key"], "retry-me-1");
  } finally {
    restore();
  }
});

test("a GET sends no key", async () => {
  const { restore, calls } = stubJsonResponse(invoiceView);
  try {
    await new Api("http://fixture.invalid").getInvoice(invoiceView.id as InvoiceId);
    assert.equal(calls[0]!.headers["idempotency-key"], undefined);
  } finally {
    restore();
  }
});
