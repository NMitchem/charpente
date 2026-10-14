# triage — product brief for a docs-only build (requirements, never implementation)

You are building a small backend on the charpente platform, from its documentation alone.
Your ONLY sources are this brief, `docs/README.md` and `docs/GUIDE.md` in the directory
you are in. Do not read anything outside this directory — not the platform's source, not
its other docs, not its examples. If the docs do not tell you how to do something, record
that in `FINDINGS.md` and choose the simplest thing the docs DO support — never patch the
framework. Every commit touches `triage/` only.

## Setup
The `charpente` CLI is on your PATH. Run `charpente new triage` in this directory, then
`git init` inside `triage/` — the scaffold does neither. Append these lines to the
scaffold's `Cargo.toml` exactly as written; they are the only lines you may add that name
a path outside this directory:

    [patch.crates-io]
    charpente = { path = "../../charpente/crates/charpente" }
    charpente-sim = { path = "../../charpente/crates/charpente-sim" }

## Domain
- Orgs. Operators belong to exactly one org and authenticate by session login and API keys
  as GUIDE §5 describes. Each org has an on-call email address its operators can change.
- Anyone, with no credential, can submit a feedback item (a short text) to an org by the
  org's id in the path. Operators read and list their own org's items.
- Every item is classified by an outside language-model service into one of: bug, praise,
  urgent, unsure. An item's label is written once.
- An item labelled urgent pages the org: one email to the on-call address as it was when
  the item was labelled.

## The two outside services
- The classifier, host `api.llm.example`. `POST /v1/classify` with JSON `{"text": "…"}`,
  authenticated by `authorization: Bearer <key>` where the key is in the environment
  variable `LLM_API_KEY`. It answers 200 with `{"label": "bug" | "praise" | "urgent" |
  "unsure"}`, sometimes 429 or 503 when overloaded, and is slow at the tail. It does NOT
  deduplicate: every request is classified and billed, and two requests for one text may
  answer differently.
- The mailer, host `api.mail.example`. `POST /v1/send` with JSON `{"to", "subject",
  "body"}`, authenticated by the header `x-mail-token` from the environment variable
  `MAIL_TOKEN`. It honours an `Idempotency-Key` request header: a repeated key returns the
  first response without sending again. It answers 200 with `{"id": "…"}`, 422 for an
  address it will never accept, and sometimes 503.

## Behaviours
- Submitting an item enqueues its classification in the same transaction.
- An urgent label and the page it causes are decided together: the label is never written
  without its page being scheduled, and a page is never sent for an item that is not urgent.
- An address the mailer refuses with 422 is recorded on the item as a failed page; it is
  not retried.
- Nothing the outside services return may cross into another org's rows.

## Proof (the part that matters)
`tests/sim.rs` using the platform's simulation harness, with your own models of both
services and properties for:
- acked writes are durable across crashes; replay is identical from a seed;
- every urgent item causes exactly one email the mailer actually sent, and no other item
  causes one — under the full fault matrix, including the outside services' faults;
- an item's label is written exactly once, however often the classifier was asked;
- no org ever observes another org's items, labels or pages.

`charpente test --sim --universes 10000` must be green; record the master seed it prints.
Then run `charpente test --mutants` (GUIDE §8) over the files carrying the two jobs, with
`--test-package triage`, and record its full output in `FINDINGS.md`. Every mutant caught
or unviable is the target; a MISSED is a one-sided property to fix before you are done.

## Deliverables
- `triage/` as its own git repository, building with `cargo test` and
  `cargo clippy --all-targets -- -D warnings`.
- `FINDINGS.md` in the repository: a friction ledger in your own words, dated as you go;
  how many times you restarted an approach; every GUIDE section you had to re-read and why;
  what you could not find in the docs; the master seed; the `--mutants` output.
- Commit as you go, small commits, named paths.
