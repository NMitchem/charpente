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
    CreateCustomerBody, Customer, CustomerView, FeedItem, Invoice, InvoicePaid, InvoiceView,
    InvoiceVoided, LineItem, Notification, Payment, Status,
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
        },
        notice: Notification::Progress { percent: 42 },
        last_item: FeedItem::Message {
            body: "hello".to_string(),
        },
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
        specimen("feedItemAck", "FeedItem", &FeedItem::Ack(7)),
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
            &Notification::Progress { percent: 42 },
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
// Nothing here is imported by application code; existing is the whole job.

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
 *    because a JSON number loses precision above 2^53. See the KNOWN GAP
 *    block in ../negatives.ts: the generated client does not yet convert it,
 *    so at runtime the value is still the string this rewrite describes.
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
