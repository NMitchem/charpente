// Real call sites against the generated client. Nothing here runs — `tsc
// --noEmit` is the test, and every line exists because it fails if a Rust
// route or wire type changes and the client is regenerated (TS-codegen spec
// §7, exit criterion 2).
//
// Written against the GENERATED FILE, not against the plan: the event union
// is discriminated by the frame's `event:` name with the payload under
// `data:` (ruling 46), and `SubscribeOptions` carries `onUnknown`.

import {
  Api,
  CursorRefused,
  StreamRejected,
  UndeclaredEvent,
} from "./generated/client";
import type {
  CustomerId,
  FeedEvent,
  InvoiceView,
  Status,
  WidgetId,
} from "./generated/client";

/** A route's outcome NARROWS: `ok` picks the data, `!ok` the typed error. */
export async function createThenRead(
  api: Api,
  id: CustomerId,
): Promise<string | undefined> {
  const created = await api.createCustomer(id, {
    displayName: "Ada",
    emailAddress: null,
  });
  if (!created.ok) {
    // The error is TYPED — `ApiError`, not `unknown`. `catch` would bind
    // `unknown`, which is why routes return this instead of throwing.
    const message: string = created.error.error;
    return message;
  }
  // A branded id is still a string: assignable OUT of the brand, never in.
  const back: string = created.data.id;
  const status: Status = created.data.status;
  // `Option<String>` is `string | null`, so the null case must be handled.
  const email = created.data.emailAddress ?? "(none)";
  return `${back} ${status} ${email}`;
}

/** `-> Result<(), ApiError>`: the success payload is the JSON literal null. */
export async function removeWidget(api: Api, id: WidgetId): Promise<boolean> {
  const gone = await api.deleteWidget(id);
  if (!gone.ok) return false;
  const nothing: null = gone.data;
  return nothing === null;
}

/** Every enum representation, narrowed the way its tagging requires. */
export function describeInvoice(inv: InvoiceView): string[] {
  const parts: string[] = [];

  // Internally tagged: the tag key merges into the payload object, and the
  // renamed field needs a quoted access.
  if (inv.payment.type === "Card") parts.push(inv.payment["card-number"]);

  // Adjacently tagged, under a kebab-case tag key.
  if (inv.notice["kind-of"] === "Progress") {
    parts.push(String(inv.notice.payload.percent));
  }

  // Externally tagged: a unit variant is a bare string, a braced one an
  // object keyed by the variant name.
  if (inv.lastItem === "Ping") parts.push("ping");
  else if ("Message" in inv.lastItem) parts.push(inv.lastItem.Message.body);
  else if ("Ack" in inv.lastItem) parts.push(String(inv.lastItem.Ack));

  // `skip_serializing_if` makes `note` optional, so it can be absent.
  //
  // `units` does ARITHMETIC on `quantity`, and that is load-bearing rather
  // than decorative. `String(item.quantity)` alone would compile for a
  // string, a number, a bigint or a boolean — so a call site that only
  // stringifies a numeric field does not depend on its numeric-ness, and a
  // Rust `u32 -> String` change would regenerate cleanly and leave `tsc`
  // green. `tsc` can only redden where a hand-written site DEPENDS on the
  // shape that changed; this is the site that depends on this one.
  let units = 0;
  for (const item of inv.lineItems) {
    if (item.note != null) parts.push(item.note);
    units += item.quantity;
  }
  parts.push(`${units} units`);
  // `Seq(Option(_))` is `(string | null)[]`, not `string | null[]`.
  for (const tag of inv.tags) if (tag !== null) parts.push(tag);
  // `Map` is a `Record`, keyed by string.
  for (const [key, value] of Object.entries(inv.labels)) {
    parts.push(`${key}=${value}`);
  }
  if (inv.paid) parts.push("paid");
  return parts;
}

/** The declared stream: the union must discriminate and be exhaustive. */
export function tail(api: Api, cursor?: string): Promise<void> {
  return api.feed({
    cursor,
    onEvent: (ev: FeedEvent, at: string) => {
      switch (ev.event) {
        case "invoice_paid":
          // `#[event]` applies no `rename_all`, so this field is snake_case
          // on the wire and snake_case here. That is the honest rendering.
          console.log(ev.data.invoice, ev.data.amount_cents, at);
          break;
        case "invoice_voided":
          console.log(ev.data.invoice, at);
          break;
        default: {
          // Exhaustiveness: an arm added to the union stops assigning here.
          const unreachable: never = ev;
          return unreachable;
        }
      }
    },
    // `onEnd` fires only for the two ways the SERVER ends a stream. It is
    // not a cleanup hook — a refusal rejects the promise instead.
    onEnd: (end) => {
      if (end.kind === "gap") console.warn("gap:", end.detail);
      else console.warn("closed");
    },
    // Without this, a frame the route never declared raises UndeclaredEvent
    // rather than being cast into the union or dropped.
    onUnknown: (ev, at) => console.warn("skew:", ev.event, ev.data, at),
  });
}

/** A stream with no `emits(...)`: the honest `UnknownEvent`, not a fiction. */
export function tailRaw(api: Api): Promise<void> {
  return api.rawFeed({
    onEvent: (ev) => {
      const name: string = ev.event;
      const payload: unknown = ev.data;
      console.log(name, payload);
    },
    onEnd: () => {},
  });
}

/** The three refusals this client raises, each carrying its own detail. */
export async function tailStrict(api: Api, cursor: string): Promise<string> {
  try {
    await api.feed({ cursor, onEvent: () => {}, onEnd: () => {} });
    return "ended";
  } catch (e) {
    if (e instanceof CursorRefused) return `refused: ${e.detail}`;
    if (e instanceof StreamRejected) return `rejected: ${e.status} ${e.detail}`;
    if (e instanceof UndeclaredEvent) return `skew: ${e.event}`;
    throw e;
  }
}
