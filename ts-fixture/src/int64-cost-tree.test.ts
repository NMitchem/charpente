// Fix round 1, F6: a `Ref` CYCLE (`CostNode.children: CostNode[]`) and the
// sibling-`Ref` shape (`CostNode.price`/`.discount`, both `Ref("Money")`)
// each had coverage only from a `charpente-tsgen` Rust-mechanical test
// asserting on generated SOURCE TEXT -- never compiled, never executed on a
// genuinely recursive document. `costTree()`'s real route
// (`GET /cost-tree`, crates/ts-fixture-app/src/lib.rs) and its sample (one
// child, not zero -- see `samples::cost_tree`'s doc comment) close that gap.

import { test } from "node:test";
import * as assert from "node:assert/strict";
import { Api } from "./generated/client";
import { costTree } from "./generated/samples";
import { stubJsonResponse } from "./testing/fetch-stub";

test("a self-referential type's sibling Ref fields both revive, at every recursion depth", async () => {
  const { restore } = stubJsonResponse(costTree);
  try {
    const api = new Api("http://fixture.invalid");
    const result = await api.costTree();
    assert.equal(result.ok, true);
    if (!result.ok) throw new Error("unreachable: stub always returns 200");

    // Sibling Ref, at the root: `price` and `discount` are two DIFFERENT
    // fields of the SAME named type (`Money`) -- both must convert
    // independently, not just the first one a naive per-type memo might
    // stop at.
    assert.equal(result.data.price.cents * 2n, 18014398509481986n);
    assert.equal(result.data.discount.cents * 2n, -1000n);

    // The Ref cycle: `children` is `CostNode[]`, so reviving the root must
    // recurse into the SAME reviver function for each child, not stop one
    // level down. Checked on the child's OWN sibling-Ref pair too, proving
    // the recursion is real, not a coincidence of the root's fields lining
    // up with the top-level object.
    assert.equal(result.data.children.length, 1);
    assert.equal(result.data.children[0].price.cents * 2n, 2000n);
    assert.equal(result.data.children[0].discount.cents * 2n, -200n);
  } finally {
    restore();
  }
});
