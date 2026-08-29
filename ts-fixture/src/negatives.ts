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
