# helpdesk — product brief for a docs-only build (requirements, never implementation)

You are building a small backend on the charpente platform, from its documentation alone.
Your ONLY sources are this brief, `README.md`, and `docs/GUIDE.md` in the repo you are
in. Do not read anything under `crates/`, `examples/`, `DESIGN.md`, `PLAN.md`, `docs/`
other than GUIDE.md, or `.superpowers/`. If the docs do not tell you how to do something,
record that in `FINDINGS.md` and choose the simplest thing the docs DO support — never
patch the framework. Every commit touches `examples/helpdesk/` and the workspace `members`
line only.

## Domain
- Orgs. Agents (users) belong to exactly one org. Tickets belong to an org and move
  Open → Pending → Closed; any other transition is an application error with a message an
  operator could act on. Comments belong to a ticket.
- Every ticket, comment and agent route requires authentication (session login and API
  keys as GUIDE §5 describes) and is scoped to the caller's org: no route may ever return,
  modify or count another org's rows. Use the platform's tenant-scoping surface if GUIDE
  documents one; otherwise derive the org from the authenticated subject and filter
  explicitly. Record in FINDINGS.md which of the two you did and why.

## Behaviours
- Closing a ticket, in ONE transaction: marks it Closed, enqueues a "send survey" job, and
  emits a `TicketClosed` event. The job records a survey row (a table; no external IO).
- A realtime SSE route that streams `TicketOpened`, `TicketClosed`, `CommentAdded` for the
  caller's org, with exact `Last-Event-ID` resume (GUIDE §11).
- `Ticket` ships at `version = 1`. A second, separate commit bumps it to `version = 2`,
  adding a `priority` field with a migration (GUIDE §12). Old rows read back with a
  default priority.
- An hourly cron tick enqueues an SLA-breach reminder (a row) for every Pending ticket
  older than 4 hours; a ticket gets at most one reminder per hour.
- The TypeScript client: `charpente gen-client` output committed; `charpente check-client`
  exits 0 (GUIDE §14).

## Proof (the part that matters)
`tests/sim.rs` using the platform's simulation harness, with properties for:
- acked writes are durable across crashes; replay is identical from a seed;
- surveys ≡ closed tickets, exactly, under the full fault matrix;
- no org ever observes another org's ticket, comment, agent, or event (the tenancy
  invariant), under the fault matrix;
- SLA reminders: at most one per ticket-hour, none for non-Pending tickets, over virtual
  weeks.
`charpente test --sim --dir examples/helpdesk --universes 10000` must be green; record the
master seed it prints.

## Deliverables
- `examples/helpdesk/` as a workspace member, building with `cargo test -p helpdesk`.
- `FINDINGS.md` in the example: friction ledger in your own words, dated as you go; how
  many times you restarted an approach; every GUIDE section you had to re-read and why;
  what you could not find in the docs.
- Commit as you go, small commits, named paths.
