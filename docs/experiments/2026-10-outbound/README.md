# The docs-only outbound experiment (2026-10)

Outbound-effects sub-project (spec §16(5)). An agent is given `BRIEF.md`, `README.md` and
`docs/GUIDE.md` — nothing else — and asked to build a small feedback-triage backend that
sends email and calls a language-model service, in its own repository scaffolded by
`charpente new` and reaching the platform only through the brief's `[patch.crates-io]`
lines. The point is not the app. It is whether the docs lead a stranger to minted keys,
read-only `call` and atomic `settle`, and whether the outbound oracles catch what they
claim. The brief names the services' behaviour and never the platform's surface.

**Status: attempt 1 in flight.**

## Attempt 1

| | |
|---|---|
| Brief sha256 | `73692af03241326ac9f903b113d9142d90d712eaae4a85011f3493f7fc0340b9` (71 lines, committed before any attempt) |
| Pinned sha | `0506e22c` (main at preflight; the CLI on the builder's PATH is installed from this tree) |
| Dependency form | `[patch.crates-io]`, the brief's lines unmodified (proven by preflight at `0506e22c`) |
