# Research base

Evidence backing the manifesto's claims, from an adversarially-verified deep-research pass
(2026-07-01: 104 agents, 22 sources fetched, 93 claims extracted, 25 claims put through
3-vote adversarial verification — 19 confirmed, 6 refuted). Confidence labels are from
that process. Re-verify anything load-bearing before quoting publicly; the field moves fast.

## Feasibility: agents can build platform-grade systems software

**The Carlini compiler experiment (Feb 2026)** — confirmed high-confidence against
Anthropic's primary source:

- 16 parallel Claude Opus 4.6 agents, largely unsupervised after setup, built a
  ~100,000-line Rust C compiler over ~2,000 Claude Code sessions in two weeks, for just
  under $20,000 (2B input / 140M output tokens).
- Result passes 99% of the GCC torture suite, builds a bootable Linux 6.9 kernel on
  x86/ARM/RISC-V, compiles PostgreSQL, SQLite, Redis, FFmpeg, QEMU; runs Doom.
- Sources: https://www.anthropic.com/engineering/building-c-compiler ·
  https://www.infoq.com/news/2026/02/claude-built-c-compiler/ ·
  https://www.theregister.com/2026/02/09/claude_opus_46_compiler/

**The ceiling — equally verified, and it IS the charpente thesis:**

- The same compiler is explicitly non-production-grade: codegen less efficient than GCC
  with optimizations *disabled*; buggy agent-written assembler/linker; no 16-bit backend;
  Anthropic's own words: Rust quality "reasonable… nowhere near the quality of what an
  expert Rust programmer might produce."
- Read the pattern: **99% where a dense oracle existed (torture suite); weak exactly where
  no oracle existed** (codegen efficiency, assembler, code quality). Agent output quality
  tracks oracle density. This is the empirical core of MANIFESTO §2 and the reason
  charpente's product is the oracle (types + simulation), not the code.

Caveat (from the verify pass): single experiment, ~5 months old at time of research. One
data point of feasibility, not a trend line. Several *overclaimed* secondary-source
variants of these claims were refuted 3-vote — the press runs ahead of the verified facts.

## The verification-scarcity frame is institutionally corroborated

- **DARPA TRACTOR** funds automated C→Rust translation with an explicit quality bar of
  "the same quality and style that a skilled Rust developer would produce" — i.e., a major
  funder treats *expert-quality automated systems code* as valuable and unsolved (as of the
  program's 2024–2026 run). https://www.darpa.mil/research/programs/translating-all-c-to-rust
- **Microsoft** research goal: eliminate C/C++ by 2030, "1 engineer, 1 month, 1M lines" —
  publicly walked back from any claim that Windows is being rewritten (medium confidence;
  based on an engineer's post + clarification, not a formal program page).
- State of the art in that adjacent field (all confirmed vs. primary arXiv sources):
  C2Rust = correct but heavily `unsafe`; C2SaferRust (neurosymbolic) cuts unsafe by only
  ~24–25% on coreutils; Scylla reaches fully-safe Rust via *non-AI* formal methods and
  needs a human to hand-refactor the C first; ORBIT (2026 agentic orchestration) hits 100%
  compile / 91.7% test success but only on a 24-program, ~1,600-LOC-average benchmark.
  (ORBIT's "near-zero unsafe" claim was REFUTED in verification — don't repeat it.)

## The white space (a verified negative result)

The research pass found **no primary-source evidence of serious 2025–2026 agentic attempts**
at: databases, hypervisors, network stacks, browser engines, distributed consensus systems,
verified TLS, JIT runtimes — or anything shaped like charpente. The verified activity
concentrates on (a) the one compiler demo and (b) C→Rust transpilation research. The
agent-native application platform quadrant was empty as of mid-2026.

## Supporting evidence

- **Human-baseline for foundational rewrites:** uutils (Rust coreutils) took years of
  community effort to reach ~82–88% GNU compatibility (83/106 "Done" in a 2021 LWN
  snapshot; ~87.75% GNU-test compat cited mid-2026). Elite labor was genuinely the
  bottleneck; that labor is now cheap. https://lwn.net/Articles/857599/
- **FFI-boundary hazard (why single-binary, not incremental hybrid):** rewriting
  *components* of a C/C++ system in Rust relocates memory-safety risk to the FFI boundary
  rather than eliminating it (UCSD HotOS'23 position paper, confirmed 3-0; corroborated by
  CVE-2025-68260, the first Rust CVE in the Linux kernel, at exactly such a boundary).
  https://goto.ucsd.edu/~rjhala/hotos-ffi.pdf — supports the whole-system, one-language,
  one-process shape.
- Martin Kleppmann on AI + formal verification (Dec 2025) — the "AI makes verification
  economical" argument from the DDIA author:
  https://martin.kleppmann.com/2025/12/08/ai-formal-verification.html

## Origin note

Charpente emerged from a broader "golden opportunities in the agent era" analysis
(2026-07-01). The candidate list it won from, and why, is in DECISIONS.md §1.
