# `Drive::recover` runs the platform checks

Status: approved by Noah, 2026-11-26. A small change, so this is the short design. It ships
in 0.7.0, before the publish.

## Why

The platform's own rules are checked today only when an app's sim test remembers to call
the checks for them. `examples/double-charge/tests/sim.rs:62-73` spends twelve lines
calling `keys_are_per_effect`, `outbound_settles_by_horizon` and `calls_have_durable_jobs`
and routing a durable-corruption give-up. An app that forgets them gets no verdict on those
rules, and the agent that wrote the app is the one who has to remember. This change makes
the checks a verdict the app can read but neither writes nor skips (MANIFESTO §6, and the
2026-11-26 twin review, which parked the fault-free twin and named this as the cheaper step
that comes first).

## Goals, non-goals, anti-goals

Goals:
- Every app whose sim drives recovery through `charpente_sim::drive::Drive` is judged on
  three platform rules after the fault-free tail, with no call from the app.
- Each failure is the check's own text, which already names the rule.

Non-goals:
- Apps that drive recovery by hand (`examples/invoicing`, `crates/outbound-fixture`,
  `crates/sim-fixture`'s `acked_implies_durable` check). Shipped examples and the scaffold
  already use `Drive` (`tests/driver_census.rs`); a hand driver keeps calling the checks
  itself.
- Acked durability. `acked_implies_durable` drives its own rounds and does not fit after a
  tail.

Anti-goals:
- An opt-out (hard rule 2).
- Moving a universe. The checks only read the recovered store and the sim's record of
  deliveries, so no seed may replay differently because of this change.

## Approaches

1. **In `Drive::recover`, after the tail** (picked). `recover` already runs one platform
   check itself, `mark_dead_coverage`, so the others go beside it, and an app cannot skip a
   check that runs before `recover` returns.
2. In the sim runner, after each check returns. `Drive` would have to record the end state
   on the `Sim` for the runner to judge. That is more machinery for no gain, since an app
   already cannot skip approach 1.
3. One `checks::platform(...)` call for apps to make. That is opt-in again, which is the gap
   this change closes.

## The design

After the tail settles and before `mark_dead_coverage` (which stays last, as GUIDE §8
says), `recover` reads one snapshot of the tail kernel's store and runs:
- `keys_are_per_effect(&snap)`: a key carries one request, and two live jobs never share
  one.
- `calls_have_durable_jobs(sim, &snap)`: every call that reached a remote carries a durable
  job's key. When it fails and `durable_corruption_fired(sim)` holds, `recover` gives the
  universe up with `sim.give_up("injected corruption cut an enqueue after its call reached
  the remote", <the check's text>)` and returns `Ok(None)`, as it does for its other
  give-ups. Otherwise it returns the check's `Err`. *(Revised while planning: the reason is
  pinned here because the double-charge GOLDEN and README quote it; see Evidence.)*
- `dead_hooks_succeeded(&snap)` *(call: in; Noah approved)*. A hook that returns `Err`
  leaves the job Dead and its compensation undone. An app that means to skip the
  compensation returns `Ok`, so no business outcome becomes unreachable.

A failing check is returned as `recover`'s `Err`, in the check's own words.

`outbound_settles_by_horizon` is **not** added. `recover` returns only once `unsettled`
(`drive.rs`) finds no Pending outbound row that is due at `recovered.wall`, which is the
condition the check tests (`checks.rs`), so after `recover` it cannot fail. Its copies after
`recover` are removed as dead code. It stays in the library for hand drivers.

## What changes elsewhere

- The copies after `recover` are removed: `examples/double-charge/tests/sim.rs`,
  `crates/charpente/examples/modelled_remote.rs`, GUIDE §8's outbound example (near "Calling
  the outside world in a sim"), and the scaffold's `dead_hooks_succeeded` call
  (`crates/charpente-cli/templates/sim.rs.tmpl`). Hand drivers keep theirs.
  `crates/charpente/examples/api.rs` keeps its calls, since it shows the library's
  signatures; its comment about what `Drive::recover` runs is updated.
- `charpente-sim`'s own `tests/drive.rs` assertions of `outbound_settles_by_horizon` after
  `recover` are the builder's to keep or drop.
- GUIDE §8: the drive section says what `recover` checks, and the checker list says which
  checks `recover` runs itself and that `outbound_settles_by_horizon` is for hand drivers.
- CHANGELOG 0.7.0, under breaking changes: a `Drive` workload that broke one of the three
  rules without calling its check now fails, and a `Drive` run whose call lost its durable
  job under durable corruption is now given up.

## Evidence before it lands

- **Red first, per rule.** For each of the three checks, a `Drive` workload over an app
  that breaks the rule fails through `recover` alone, with the check's text, and its test
  calls no check. Deleting that check's call in `recover` makes the test fail, which proves
  the wiring is what catches it.
- **The give-up.** The double-charge GOLDEN already names a natural seed for it:
  `0x11c23979e56d3608` under master `0x434841524745`, where injected corruption cut an
  enqueue after its call reached the gateway, and the app gave the run up itself. After
  this change `recover` gives that same universe up first. A constructed case (an orphan
  call plus a corruption mark) also pins the branch in `charpente-sim`'s own tests.
- **No universe moves.** Every recorded batch stays byte-identical, with one named
  exception *(revised while planning)*: in the double-charge GOLDEN
  (`crates/charpente-cli/tests/demo.rs`) and README's quote of it (`README.md:119`), the
  give-up line for `0x11c23979e56d3608` carries `recover`'s reason in place of the app's
  own ("…reached the remote" for "…reached the gateway"). Its count, its first seed and
  every other line stay the same. Any other change means the checks drew something, and
  that is a bug in this change, not a re-pin.
- The workspace suite green by count, fmt and clippy clean, and `charpente test --sim` over
  fresh master seeds for sim-fixture, invoicing, outbound-fixture, a fresh scaffold and the
  double-charge copy, with the seed recorded.

## Left to the builder

- Whether `recover` reads one snapshot for all three checks or each check reads its own.
- The `tests/drive.rs` assertions noted above.
