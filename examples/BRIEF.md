# Brief: invoicing on charpente

Build a minimal invoicing SaaS as the Rust crate `examples/invoicing` in this
workspace (add it to the root Cargo.toml members). Your only references are
README.md and docs/GUIDE.md. Requirements:

- Customers; invoices with line items and a Draft → Final → Paid state
  machine. Illegal transitions are rejected as app errors, not panics.
- Finalizing an invoice marks it Final, enqueues a send-receipt job, and
  emits an invoice-finalized event — in one transaction. "Sending" a receipt
  means writing a receipt row and emitting an event (there is no external IO).
- A daily cron tick enqueues one overdue-reminder job per overdue Final
  invoice (a reminder is a reminders-table row). Missed days are not
  backfilled beyond the platform's own catch-up behavior.
- Session-authenticated HTTP routes: customer create/get/list, invoice
  create/get/list, finalize, record-payment. Plus whatever session-creation
  route your design needs.
- Sim tests in tests/sim.rs covering at least: no acknowledged write is lost
  under crash injection; the app replays deterministically; receipts match
  finalized invoices exactly (exactly-once) under the full fault matrix; and
  overdue-reminder behavior over simulated months.
- Keep a running log of every point of confusion — missing documentation,
  unclear errors, API surprises — in examples/invoicing/FINDINGS.md as you
  work. Candor there is a deliverable, not a complaint.

Schema, route shapes, and test structure are your design. Definition of done:
`cargo test --workspace` green and `cargo run -p charpente-cli -- test --sim
--dir examples/invoicing` green.
