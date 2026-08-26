// The assertions that fail when something WRONGLY compiles.
//
// `@ts-expect-error` is an error if the line below it type-checks, so every
// block here is a real assertion about what the generated client refuses.
// Without them, §1's id branding is decorative — a `CustomerId` that were
// secretly `string` would pass every test in `calls.ts`.

import type {
  CustomerId,
  InvoiceId,
  InvoiceView,
  LineItem,
} from "./generated/client";

declare const customer: CustomerId;
declare const invoice: InvoiceId;
declare const view: InvoiceView;

// @ts-expect-error an InvoiceId is not a CustomerId — DESIGN §3's typed-id
// guarantee has to survive the process boundary, or the brand is theatre.
export const wrongIdKind: CustomerId = invoice;

// @ts-expect-error a bare string is not a branded id: the brand is what makes
// `createCustomer(someInvoiceId)` a compile error rather than a 404 at 3am.
export const bareStringIsNotAnId: CustomerId = "42";

// The POSITIVE direction, and it is load-bearing. The two assertions above
// would also hold if the brands were `never`, or if the whole file failed to
// declare them — this line fails in either case, so it is what makes the two
// above evidence about branding rather than about breakage.
export const brandedIdIsStillAString: string = customer;

// Declared rather than written inline: `@ts-expect-error` covers exactly the
// ONE line after it, and inside a multi-line object literal the error lands
// on the offending property's line instead — which reports the directive as
// unused and passes for the wrong reason.
declare const numericNote: {
  unitAmountCents: bigint;
  quantity: number;
  note: number;
};
// @ts-expect-error `note` is optional, not untyped — an optional field is
// still checked when it IS present.
export const wrongOptionalType: LineItem = numericNote;

declare const camelCasedCard: { type: "Card"; cardNumber: string };
// @ts-expect-error the internally-tagged variant's renamed field is
// `"card-number"`, not `cardNumber` — a serde rename the emitter dropped
// would make this line compile.
export const wrongTaggedField: InvoiceView["payment"] = camelCasedCard;

// --------------------------------------------------------------------------
// KNOWN GAP — task 11 finding 1, recorded as a tripwire rather than a note.
//
// `InvoiceView.totalCents` is typed `bigint`, and `WireI64` really is carried
// as the decimal STRING "9007199254740993" (that is the point: a JSON number
// loses precision above 2^53). But the generated client's `request()` does
// `JSON.parse(text) as T` with no revival — nothing in
// charpente-tsgen/src/runtime.rs ever calls `BigInt(...)` — so at runtime this
// field holds a string while the type promises a bigint, and
// `inv.totalCents * 2n` throws TypeError. That is a generated type which is
// wrong AND type-checks, which spec §5 calls the worst failure available here.
//
// The line below pins the CURRENT state. If the emitter starts rendering
// `Int64` as a string (or as a branded string), this assignment compiles, the
// `@ts-expect-error` fires, and whoever made the change is sent here. Note
// what it does NOT catch: the other fix — reviving with `BigInt(...)` in the
// runtime — leaves the type as `bigint` and this line unchanged. So deleting
// this block is part of that fix, not a side effect of it.
// --------------------------------------------------------------------------

// @ts-expect-error see KNOWN GAP above: the type still claims bigint
export const int64StillClaimsBigint: string = view.totalCents;
