// The assertions that fail when something WRONGLY compiles.
//
// `@ts-expect-error` is an error if the line below it type-checks, so every
// block here is a real assertion about what the generated client refuses.
// Without them, §1's id branding is decorative — a `CustomerId` that were
// secretly `string` would pass every test in `calls.ts`.

import type {
  CustomerId,
  FeedEvent,
  InvoiceId,
  InvoiceView,
  LineItem,
  SubscribeOptions,
  UnknownEvent,
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
// `exactOptionalPropertyTypes` (chunk B2). The flag is ON in `tsconfig.json`,
// and the emitter answers `?: T` versus `?: T | undefined` DIFFERENTLY on the
// two sides of the wire. Both answers are pinned here, because both are
// decisions a later change could quietly reverse and no other gate would say
// so.
// --------------------------------------------------------------------------

declare const explicitlyUndefined: { lateFeeCents: undefined };
// @ts-expect-error a WIRE type keeps `?: T`. `InvoiceView.lateFeeCents` is a
// `skip_serializing_if` over a bare `WireI64`: the server either sends a value
// or omits the key, and never sends `undefined` — so "present with the value
// `undefined`" is a document that does not exist, and the type says so. This
// is the assertion that goes red if someone ever answers the walker's TS2375
// by widening the wire type instead of fixing the walker; the walker's own
// half is `int64-null-safety.test.ts`'s `!("lateFeeCents" in result.data)`.
//
// It also pins the FLAG: with `exactOptionalPropertyTypes` off, the line below
// compiles and `tsc` reports this directive as unused (TS2578). So turning the
// flag back off is itself a build error, not a silent loss of the oracle.
export const wireOptionalRefusesUndefined: Pick<InvoiceView, "lateFeeCents"> =
  explicitlyUndefined;

declare const maybeCursor: string | undefined;
declare const maybeSignal: AbortSignal | undefined;
declare const maybeOnUnknown:
  | ((ev: UnknownEvent, cursor: string) => void)
  | undefined;

// The POSITIVE direction, and the opposite answer. `SubscribeOptions` is an
// INPUT option bag, where absent and `undefined` mean the same thing (nothing
// is serialized from the presence of a key), so every optional member is
// `?: T | undefined` and this ordinary pass-through compiles. Under `?: T` it
// does not — which is the shape that shipped before chunk B2, and the reason
// `calls.ts`'s `tail(api, cursor?: string)` could not be type-checked at this
// strictness.
//
// Written here rather than left to `calls.ts` because `calls.ts` passes
// `cursor` only, and would stop exercising even that the moment someone made
// its parameter required — a floor, not a duplicate.
export const optionalInputsAcceptUndefined: SubscribeOptions<FeedEvent> = {
  cursor: maybeCursor,
  onEvent: () => {},
  onEnd: () => {},
  onUnknown: maybeOnUnknown,
  signal: maybeSignal,
};
