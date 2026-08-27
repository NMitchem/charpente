//! The serde-fidelity literals (TS-codegen spec §5).
//!
//! **What this is an oracle for.** The wire shape is serde's to decide, not
//! the Rust struct's. If `#[derive(Wire)]`'s schema and serde's real output
//! ever disagree — a `rename_all` rule applied to the wrong table, a
//! `skip_serializing_if` that changes presence, a tagged enum's key — the
//! generated TypeScript is wrong AND still type-checks, which spec §5 calls
//! the worst failure available here. Each literal below is a REAL
//! `serde_json` document: `tsc` checking it against the generated type is
//! that disagreement made into a compile error.
//!
//! **Why the specimens are hand-written and not derived.** They cannot go
//! stale silently: adding a required field, renaming one, or changing its
//! type stops THIS crate compiling. The compiler is the anti-drift oracle,
//! so a `sample()` on the `Wire` derive would buy nothing that a struct
//! literal does not already give — at the cost of a new method on a trait
//! two sessions share. (The serde-vs-schema question itself is covered
//! independently, and far more thoroughly, by the 35 `fidelity_*` tests in
//! `crates/charpente/tests/wire.rs`.)
//!
//! Regenerate with:
//!
//! ```text
//! cargo run -p ts-fixture-app --example gen-samples > ts-fixture/src/generated/samples.ts
//! ```
//!
//! `tests/samples.rs` fails if the committed file is not what serde produces
//! today, and names that command.

use std::collections::BTreeMap;

use charpente::Id;
use charpente::serde::Serialize;
use charpente::wire::{WireI64, WireU64};

use crate::{
    AdjustInvoiceBody, CostNode, CreateCustomerBody, Customer, CustomerView, FeedItem, Invoice,
    InvoicePaid, InvoiceView, InvoiceVoided, LimitExceeded, LineItem, Money, Notification, Payment,
    Status,
};

/// The relative path this file is rendered to, named once so the generator,
/// the freshness test and the failure message cannot drift apart.
pub const OUT_PATH: &str = "ts-fixture/src/generated/samples.ts";

/// The command that rewrites `OUT_PATH`. Part of the failure message, per
/// hard rule 5 — a stale-file error that does not name its own fix is a
/// puzzle, not a diagnostic.
pub const REGEN_CMD: &str =
    "cargo run -p ts-fixture-app --example gen-samples > ts-fixture/src/generated/samples.ts";

/// One specimen: the TypeScript const name, the generated type it must
/// satisfy, and serde's document for it.
struct Specimen {
    ident: &'static str,
    ts_type: &'static str,
    json: String,
}

fn specimen<T: Serialize>(ident: &'static str, ts_type: &'static str, value: &T) -> Specimen {
    Specimen {
        ident,
        ts_type,
        // `to_string`, not `to_string_pretty`: this is the document as it
        // goes on the wire, and one line per specimen keeps a drift in the
        // committed file to one changed line.
        json: serde_json::to_string(value).expect("a wire type must serialize"),
    }
}

/// A fully-populated `InvoiceView`. Shared with the route handlers in
/// `lib.rs` deliberately: the specimen and the value the app actually
/// returns are then the same shape by construction, not by coincidence.
pub fn invoice_view(id: Id<Invoice>, customer: Id<Customer>) -> InvoiceView {
    InvoiceView {
        id,
        customer,
        // Above 2^53: as a JSON number this would come back off by one in
        // any JavaScript client, which is exactly why `Int64` is a string.
        total_cents: WireI64(9_007_199_254_740_993),
        sequence: WireU64(18_446_744_073_709_551_615),
        line_items: vec![
            LineItem {
                unit_amount_cents: WireI64(-250),
                quantity: 3,
                note: None,
            },
            LineItem {
                unit_amount_cents: WireI64(1_000),
                quantity: 1,
                note: Some("rush".to_string()),
            },
        ],
        tags: vec![Some("urgent".to_string()), None],
        labels: BTreeMap::from([("region".to_string(), "eu".to_string())]),
        paid: true,
        // Exactly representable in binary floating point, so the decimal
        // serde writes is not a rounding artefact.
        ratio: 0.5,
        ratio32: 0.25,
        tiny: -8,
        small: -16,
        medium: -32,
        ubyte: 8,
        ushort: 16,
        payment: Payment::Card {
            card_number: "4242".to_string(),
            limit_cents: WireI64(9_007_199_254_740_993),
        },
        notice: Notification::Progress {
            percent: 42,
            bytes_done: WireU64(9_007_199_254_740_993),
        },
        last_item: FeedItem::Message {
            body: "hello".to_string(),
        },
        // Both null/absent -- the A1 landmine's two shapes (chunk B1b), on
        // the specimen every existing route/test already exercises: an
        // invoice with no adjustment and no late fee is the ordinary case,
        // not an edge case, which is exactly why it belongs on the value
        // shared everywhere rather than a one-off.
        adjustment_cents: None,
        late_fee_cents: WireI64(0),
        // The four collection shapes below (fix round 1, F6) all carry the
        // same precision-breaking value already used elsewhere in this
        // specimen, plus at least one element that exercises the OTHER half
        // of each shape (a negative signed value, an absent map key never
        // applies here since `Map`'s key is always `string`, a `None`
        // element in the `Seq(Option(_))` case) -- present, not absent,
        // since the null/absent halves of `Option`/`Field.optional` are
        // already the dedicated point of `late_fee_cents`/`adjustment_cents`
        // above and their own Rust-mechanical tests; this specimen's job is
        // to prove the PRESENT case executes correctly end to end.
        installment_cents: vec![WireI64(9_007_199_254_740_993), WireI64(-500)],
        fees_by_category: BTreeMap::from([("late".to_string(), WireI64(9_007_199_254_740_993))]),
        refund_batches_cents: Some(vec![WireI64(9_007_199_254_740_993)]),
        disputed_line_cents: vec![Some(WireI64(9_007_199_254_740_993)), None],
    }
}

/// A small recursive tree (fix round 1, F6): `children` reaches `CostNode`
/// itself (the `Ref` cycle), and each node's `price`/`discount` are two
/// SIBLING fields of the same `Int64`-bearing `Ref` type (`Money`). One
/// child, not zero, so the recursive case is a real document a test can
/// assert on, not just a leaf.
pub fn cost_tree() -> CostNode {
    CostNode {
        label: "root".to_string(),
        price: Money {
            cents: WireI64(9_007_199_254_740_993),
        },
        discount: Money {
            cents: WireI64(-500),
        },
        children: vec![CostNode {
            label: "child".to_string(),
            price: Money {
                cents: WireI64(1_000),
            },
            discount: Money {
                cents: WireI64(-100),
            },
            children: vec![],
        }],
    }
}

/// Every specimen, in emission order. Enums get one specimen per variant —
/// a union arm nothing constructs is a union arm nothing checks.
fn specimens() -> Vec<Specimen> {
    let customer = Id::<Customer>::from_raw(9_007_199_254_740_993);
    let invoice = Id::<Invoice>::from_raw(7);
    vec![
        specimen(
            "createCustomerBody",
            "CreateCustomerBody",
            &CreateCustomerBody {
                display_name: "Ada".to_string(),
                email_address: Some("ada@example.com".to_string()),
            },
        ),
        // The same precision-breaking value as `invoiceView.totalCents` below,
        // but in a REQUEST body — chunk B1a's coverage gap (A2): before this
        // specimen existed nothing exercised the client's outbound
        // serialization of an `Int64`.
        specimen(
            "adjustInvoiceBody",
            "AdjustInvoiceBody",
            &AdjustInvoiceBody {
                delta_cents: WireI64(9_007_199_254_740_993),
            },
        ),
        specimen(
            "customerView",
            "CustomerView",
            &CustomerView {
                id: customer,
                display_name: "Ada".to_string(),
                // The `Option` half: `null` is present in the document, as
                // opposed to `LineItem.note`'s absent key below.
                email_address: None,
                status: Status::OnHold,
            },
        ),
        specimen("statusActive", "Status", &Status::Active),
        specimen("statusOnHold", "Status", &Status::OnHold),
        specimen(
            "lineItemWithNote",
            "LineItem",
            &LineItem {
                unit_amount_cents: WireI64(1_000),
                quantity: 1,
                note: Some("rush".to_string()),
            },
        ),
        // `skip_serializing_if` omits the key entirely — the document that
        // makes `note?:` (optional) different from `note: string | null`.
        specimen(
            "lineItemWithoutNote",
            "LineItem",
            &LineItem {
                unit_amount_cents: WireI64(-250),
                quantity: 3,
                note: None,
            },
        ),
        specimen(
            "invoiceView",
            "InvoiceView",
            &invoice_view(invoice, customer),
        ),
        // All four externally-tagged variant shapes, which are four
        // genuinely different JSON documents.
        specimen("feedItemPing", "FeedItem", &FeedItem::Ping),
        specimen(
            "feedItemAck",
            "FeedItem",
            // Above 2^53, same reasoning as `invoiceView.totalCents`: a
            // newtype variant carrying an `Int64` alongside `Ping` (a unit
            // variant, a bare JSON string) is the mixed shape chunk B1b's
            // `EnumRepr::External` revive has to guard without throwing.
            &FeedItem::Ack(WireU64(9_007_199_254_740_993)),
        ),
        specimen(
            "feedItemMessage",
            "FeedItem",
            &FeedItem::Message {
                body: "hello".to_string(),
            },
        ),
        specimen("feedItemInProgress", "FeedItem", &FeedItem::InProgress {}),
        specimen("paymentCash", "Payment", &Payment::Cash),
        specimen(
            "paymentCard",
            "Payment",
            &Payment::Card {
                card_number: "4242".to_string(),
                limit_cents: WireI64(9_007_199_254_740_993),
            },
        ),
        specimen(
            "notificationStarted",
            "Notification",
            &Notification::Started,
        ),
        specimen(
            "notificationProgress",
            "Notification",
            &Notification::Progress {
                percent: 42,
                bytes_done: WireU64(9_007_199_254_740_993),
            },
        ),
        specimen(
            "invoicePaid",
            "InvoicePaid",
            &InvoicePaid {
                invoice,
                amount_cents: WireU64(9_007_199_254_740_993),
            },
        ),
        specimen("invoiceVoided", "InvoiceVoided", &InvoiceVoided { invoice }),
        // Exit criterion 5 (chunk B1b): an `Int64` in an ERROR position.
        // Every other error in this fixture is `ApiError`'s fixed
        // `{error: string}` envelope, which carries no `Int64` anywhere --
        // see `LimitExceeded`'s doc comment in `lib.rs` for what this
        // specimen does and does not prove about a real running server.
        specimen(
            "limitExceeded",
            "LimitExceeded",
            &LimitExceeded {
                max_cents: WireI64(9_007_199_254_740_993),
            },
        ),
        // Fix round 1, F6: the `Ref`-cycle and sibling-`Ref` standing oracle
        // -- see `cost_tree`'s and `CostNode`'s own doc comments.
        specimen("costTree", "CostNode", &cost_tree()),
    ]
}

const PREAMBLE: &str = r#"// GENERATED by `cargo run -p ts-fixture-app --example gen-samples`. Do not edit.
//
// Each literal below is a REAL serde_json document, produced by serializing a
// Rust specimen value in crates/ts-fixture-app/src/samples.rs. `satisfies`
// proves the generated type accepts exactly what the server actually sends —
// so a serde rename the type IR missed is a `tsc` error here rather than a
// generated type that is wrong and compiles (TS-codegen spec §5).
//
// Nothing in APPLICATION code imports this file directly — but as of chunk
// B0, the `*.test.ts` files DO import these literals as runtime values, to
// feed the fetch stub real serde documents end to end. "Existing is the whole
// job" stopped being true the moment a test started running against it;
// correcting it here (b0-review Minor 4) rather than in the generated output,
// which regenerates from this string.

"#;

/// The TypeScript rewrite the literals are checked THROUGH, and the two
/// relaxations it makes.
///
/// Both are representation conventions the generated client states
/// deliberately, and neither weakens what the literals check: key names,
/// optionality, nesting, array shape and every enum tagging are compared
/// unchanged.
const WIRE_OF: &str = r#"
/**
 * The wire view of a generated type: what `JSON.parse` of the server's own
 * document is allowed to look like. Exactly two leaf types are rewritten.
 *
 * 1. A branded id (`CustomerId` = `string & { __table: "Customer" }`) is a
 *    plain `string` on the wire. The brand is a compile-time fiction over a
 *    runtime string — every operation the branded type permits works on the
 *    string — but TypeScript will not infer a brand for a string literal, so
 *    without this a correct document would fail to satisfy a correct type.
 * 2. An `Int64` field is typed `bigint` and carried as a decimal STRING,
 *    because a JSON number loses precision above 2^53. This rewrite
 *    describes the WIRE shape, not what the client hands back: chunk B1b's
 *    generated client revives an Int64 position to a real `bigint` at
 *    runtime, but that conversion happens in `request()`'s caller, after
 *    `JSON.parse` -- these literals are the server's raw `serde_json`
 *    documents, exactly the string this rewrite still describes, and that
 *    is unchanged by anything the CLIENT does with them afterward.
 */
type WireOf<T> =
  T extends string & { readonly __table: string } ? string
  : T extends bigint ? string
  : T extends readonly (infer E)[] ? WireOf<E>[]
  : T extends object ? { [K in keyof T]: WireOf<T[K]> }
  : T;
"#;

/// The whole `samples.ts` file.
pub fn render() -> String {
    let all = specimens();

    let mut type_names: Vec<&str> = all.iter().map(|s| s.ts_type).collect();
    type_names.sort_unstable();
    type_names.dedup();

    let mut out = String::from(PREAMBLE);
    out.push_str(&format!(
        "import type {{ {} }} from \"./client\";\n",
        type_names.join(", ")
    ));
    out.push_str(WIRE_OF);
    out.push('\n');
    for s in &all {
        out.push_str(&format!(
            "export const {} = {} satisfies WireOf<{}>;\n",
            s.ident, s.json, s.ts_type
        ));
    }
    out
}
