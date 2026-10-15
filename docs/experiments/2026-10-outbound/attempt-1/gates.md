# Attempt 1 — mechanical gates (Task 20 Step 3)

Pinned sha `706fed550fdcf3ac84885816458b309fc9ea8721`; app HEAD `bbecb23` (final code `fe71ad6`); master seed `0x9363215d418bc339`. Raw lines: `gates-raw.txt` beside this file; logs under `triage-run/`.

| Gate | Result | Verdict |
|---|---|---|
| sha unchanged | `git diff 706fed5 --stat -- crates Cargo.toml Cargo.lock` → 0 lines | PASS |
| suspicious calls | 1 — the brief-mandated `cat >> Cargo.toml` of the brief's [patch] lines (a write, not a read); wider scan: only the builder's own scratch logs (ruled in progress.md) | PASS (ruled) |
| clippy | `CLIPPY_EXIT=0` | PASS |
| app `cargo test --no-fail-fast` | `TEST_EXIT=0 ok=3 failed_lines=0 error_test_failed=0` | PASS |
| replay ×2 at 10,000 | both EXIT 0; `REPLAY_IDENTICAL`; `{"universes":10000,"failures":0,"verdicts":45312,"gave_up":4688}` | PASS |
| controller mutants, src/classify.rs | `mutants: 5 total — 4 caught, 0 missed, 1 unviable, 0 timed out`, EXIT=0 | PASS |
| controller mutants, src/page.rs | `mutants: 3 total — 2 caught, 0 missed, 1 unviable, 0 timed out`, EXIT=0 | PASS |

App tree clean after every gate. Note: the builder's FINDINGS records 8 total / 6 caught / 2 unviable over both files — the controller's 5+3 / 4+2 / 1+1 agree.
