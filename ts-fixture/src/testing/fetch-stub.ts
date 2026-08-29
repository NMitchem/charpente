// Stubs `globalThis.fetch` so `Api`'s real `request`/`stream` methods run
// end-to-end against real `serde_json` documents from `generated/samples.ts`.
//
// Deliberately NOT a hand-written JSON string: `ApiBase.request`/`ApiBase.stream`
// are `protected` and call the global `fetch` (runtime.rs:180, :206), so an
// executing test must stub it or subclass `Api`. A stub fed by a hand-written
// literal would be an oracle about the stub, not about the client — feeding it
// `generated/samples.ts` instead keeps the three-way tie the sub-project's other
// oracles have: real Rust serialization <-> the committed literal <-> the
// client's actual runtime behaviour (chunk-b0-brief.md A2).

/** One call this stub served, exactly as `ApiBase` passed it to `fetch`. `body`
 * is `init.body` verbatim (a JSON string for `request()`, `undefined` for
 * `stream()`, which sends none) — not re-parsed here, so a test can observe a
 * serialization failure as readily as a value. Added after B0 landed: chunk
 * B1 needs to assert an `Int64` in a REQUEST body serializes correctly
 * instead of throwing, which requires seeing what `request()` actually handed
 * to `fetch`, not just what the stub served back. */
export interface RecordedCall {
  url: string;
  method: string;
  body: BodyInit | null | undefined;
}

export interface Stubbed {
  restore: () => void;
  /** Every call this stub has served, in call order. */
  readonly calls: readonly RecordedCall[];
}

function install(respond: (init: RequestInit | undefined) => Promise<Response>): Stubbed {
  const original = globalThis.fetch;
  const calls: RecordedCall[] = [];
  globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    calls.push({
      url: String(input),
      method: init?.method ?? "GET",
      body: init?.body,
    });
    return respond(init);
  }) as typeof fetch;
  return {
    calls,
    restore: () => {
      globalThis.fetch = original;
    },
  };
}

/** One JSON response body, as `ApiBase.request` receives it: `res.text()` then
 * `JSON.parse`. No revival happens here — this is the wire exactly as the real
 * server would send it, which is the point. */
export function stubJsonResponse(body: unknown, status = 200): Stubbed {
  return install(async () => new Response(JSON.stringify(body), { status }));
}

/** One SSE frame, `ApiBase.stream`'s vocabulary: an `event:` name and a `data:`
 * payload (already-serialized, i.e. a real `samples.ts` literal). */
export interface SseFrame {
  event: string;
  data: unknown;
  id?: string;
}

/** An SSE response body built from real frames, formatted the way charpente's
 * wire does (DESIGN §6): one `data:` line per frame, blank-line terminated. */
export function stubSseResponse(frames: readonly SseFrame[]): Stubbed {
  const text = frames
    .map((f) => {
      const id = f.id !== undefined ? `id: ${f.id}\n` : "";
      return `event: ${f.event}\n${id}data: ${JSON.stringify(f.data)}\n\n`;
    })
    .join("");
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(new TextEncoder().encode(text));
      controller.close();
    },
  });
  return install(async () => new Response(body, { status: 200 }));
}
