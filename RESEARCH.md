# Research base

This is the evidence behind the manifesto's claims, from an adversarially verified
deep-research pass (2026-07-01: 104 agents, 22 sources fetched, 93 claims extracted, and 25
claims put through 3-vote adversarial verification, of which 19 were confirmed and 6
refuted). The confidence labels come from that process. The field moves fast, so re-verify
anything an argument rests on before quoting it publicly.

## Feasibility: agents can build platform-grade systems software

**The Carlini compiler experiment (Feb 2026)**, confirmed with high confidence against
Anthropic's primary source:

- 16 parallel Claude Opus 4.6 agents, largely unsupervised after setup, built a
  ~100,000-line Rust C compiler over ~2,000 Claude Code sessions in two weeks, for just
  under $20,000 (2B input / 140M output tokens).
- The result passes 99% of the GCC torture suite, builds a bootable Linux 6.9 kernel on
  x86/ARM/RISC-V, compiles PostgreSQL, SQLite, Redis, FFmpeg and QEMU, and runs Doom.
- Sources: https://www.anthropic.com/engineering/building-c-compiler ·
  https://www.infoq.com/news/2026/02/claude-built-c-compiler/ ·
  https://www.theregister.com/2026/02/09/claude_opus_46_compiler/

**The ceiling, equally verified, is the charpente thesis:**

- The same compiler is explicitly not production-grade. Its codegen is less efficient than
  GCC's even with GCC's optimizations disabled, the agent-written assembler and linker are
  buggy, and it has no 16-bit backend. In Anthropic's own words, its Rust quality is
  "reasonable… nowhere near the quality of what an expert Rust programmer might produce."
- The pattern: 99% where a dense oracle existed (the torture suite), and weak wherever no
  oracle existed (codegen efficiency, assembler, code quality). Agent output quality tracks
  oracle density. This is the empirical core of MANIFESTO §2, and it's why charpente's
  product is the oracle (types plus simulation) and not the code.

Caveat from the verify pass: this is a single experiment, ~5 months old at the time of
research, so it's one data point for feasibility, not a trend line. Several overclaimed
secondary-source variants of these claims were refuted 3-vote: the press runs ahead of the
verified facts.

## The verification-scarcity frame is institutionally corroborated

- DARPA TRACTOR funds automated C→Rust translation with an explicit quality bar of "the
  same quality and style that a skilled Rust developer would produce", which means a major
  funder treats expert-quality automated systems code as valuable and unsolved (as of the
  program's 2024 to 2026 run). https://www.darpa.mil/research/programs/translating-all-c-to-rust
- A Microsoft research goal is to eliminate C/C++ by 2030 ("1 engineer, 1 month, 1M
  lines"). It has been publicly walked back from any claim that Windows is being rewritten
  (medium confidence: based on an engineer's post and a clarification, not a formal program
  page).
- The state of the art in that adjacent field, all confirmed against primary arXiv sources:
  C2Rust is correct but heavily `unsafe`. C2SaferRust (neurosymbolic) cuts unsafe by only
  ~24 to 25% on coreutils. Scylla reaches fully safe Rust through formal methods that use no
  AI, and it needs a human to hand-refactor the C first. ORBIT (2026 agentic orchestration)
  hits 100% compile and 91.7% test success, but only on a 24-program benchmark averaging
  ~1,600 LOC. (ORBIT's "near-zero unsafe" claim was refuted in verification, so don't
  repeat it.)

## The white space (a verified negative result)

The research pass found no primary-source evidence of serious agentic attempts in 2025 or
2026 at databases, hypervisors, network stacks, browser engines, distributed consensus
systems, verified TLS or JIT runtimes, or at anything shaped like charpente. The verified
activity is concentrated in (a) the one compiler demo and (b) C→Rust transpilation research.
The agent-native application platform quadrant was empty as of mid-2026.

## Supporting evidence

- **Human baseline for foundational rewrites:** uutils (Rust coreutils) took years of
  community effort to reach ~82 to 88% GNU compatibility (83/106 "Done" in a 2021 LWN
  snapshot, and ~87.75% GNU-test compatibility cited in mid-2026). Elite labor really was
  the bottleneck, and that labor is now cheap. https://lwn.net/Articles/857599/
- **FFI-boundary hazard (why single-binary, not incremental hybrid):** rewriting
  components of a C/C++ system in Rust moves the memory-safety risk to the FFI boundary
  rather than eliminating it (UCSD HotOS'23 position paper, confirmed 3-0,
  https://goto.ucsd.edu/~rjhala/hotos-ffi.pdf). CVE-2025-68260, the first Rust CVE in the
  Linux kernel, sits at just such a boundary and corroborates it. The finding supports the
  whole-system, one-language, one-process shape.
- Martin Kleppmann on AI and formal verification (Dec 2025), the "AI makes verification
  economical" argument from the DDIA author:
  https://martin.kleppmann.com/2025/12/08/ai-formal-verification.html

## Origin note

Charpente came out of a broader "golden opportunities in the agent era" analysis
(2026-07-01). DECISIONS.md §1 has the list of candidates it won out over, and why.
