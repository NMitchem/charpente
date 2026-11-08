# The launch demo: an open judge for agent-built backends

Status: approved by Noah 2026-11-08. Brainstormed in conversation 2026-11-06. The judge
repo's name and the launch signals (§10, §12) are still his to set.

## 1. Intent

charpente has been published since 0.1.0 but never announced: the repo is public with one
star, and the crates' download counts are mostly mirrors and our own smoke runs. This
project is the announcement, and it doubles as the first test of the thesis that has a
control arm.

The sentence Noah wants a reader to come away with: **"wow, the new era of software
engineering is here."** The idea to show is the passage from his essay
[Nobody Has Actually Built a Software Factory](https://noahmitchem.com/notes/nobody-built-a-software-factory):

> Every stack you have worked on was built to fit inside a human's head. Code a human can
> understand is code a human can approve, and that is the review gate, written into the
> source. The floor I am describing gives that up on purpose. No engineer can hold it, and
> none needs to, because the simulator explores more states overnight than a reviewer will
> consider in a career. Only the agent can say no.

**Unread code is not the news** (Noah, in conversation): nobody reads agent code any more,
on any stack. The news is that on charpente something can still say no. The essay ends
on "Ask which number the agent is allowed to fail, and who owns it", and this project
builds that number for application code and makes it public.

The claims to show (Noah's): charpente apps are **fast, durable, and have no bugs**, and
**any bug or design fault is fixed once, and its maintenance cost is zero**.

## 2. What gets built

Four parts. Each one depends on the parts before it.

1. **The judge:** a stack-neutral, black-box harness that drives any implementation of a
   task's HTTP API, injects faults, and scores the result. It lives in its own public repo,
   separate from charpente, so it reads as neutral.
2. **Two runs of a head-to-head:** the same agent builds the same task twice, once on
   charpente and once on a strong conventional stack, unattended. An unannounced rehearsal comes
   first, then a public attempt on a fresh task.
3. **The open challenge ("bring your stack"):** the task briefs, the judge and a
   leaderboard, so any developer can pit their stack and agent against charpente's entry.
   Noah's framing: developers pitting their stack against charpente. (Labs training agents
   against the judge is a possible later audience. It is not this project.)
4. **The writeup:** a long-form interactive post in the format of Cursor's
   [Git at any scale](https://cursor.com/blog/git-at-any-scale), written after the public
   attempt from its real numbers.

## 3. Goals, non-goals, anti-goals

**Goals**

- A number the agent can fail and cannot edit, for application code, that anyone can run.
- A head-to-head whose scoring is fixed and committed before either arm runs.
- A post built on that result, with every figure traceable to a seed, a log, or
  docs/BENCH.md.
- An open call that keeps producing data after launch day, including data where charpente
  loses.

**Non-goals (for launch)**

- A hosted runner. Submitters run the judge themselves; we rerun the claims we list.
- More than one conventional arm of our own. Other stacks arrive through the open call.
- More tasks than the two this needs (the rehearsal task and the public task).
- An RL environment for labs, and any hosted charpente platform (PLAN keeps the hosted
  control plane off the roadmap).

**Anti-goals (a failure even if everything technically works)**

- **A judge shaped around charpente.** Dropping a fault class because charpente fails it,
  or adding one only because the conventional arm fails it. Fault classes are fixed by
  §5's list before the rehearsal. Any change afterwards gets a written reason, and both
  arms are re-judged under it.
- **A strawman control.** The conventional arm uses the strongest common choice for this
  kind of service (§6), and the brief tells both agents the same properties.
- **Re-rolling.** The public attempt is attempt 1 and is published whatever it shows, the
  way docs/BENCH.md keeps a refused run as attempt 1 and never re-rolls it.
- **Overclaiming.** "No bugs" means none in the judged classes (§5). The post says plainly
  that charpente cannot stop an agent from misreading a brief and building the wrong
  thing.
- **A number with nothing behind it.** Every figure must trace to a seed, a log, or BENCH.md.
- **Hype copy.** The post states its limits up front and argues from the result.

## 4. Tasks

A task is a brief describing a small multi-tenant backend, with a **fixed HTTP API**
(routes, bodies, status codes) so the judge can drive any implementation. The brief also
states in prose the properties that will be judged (§5). Kaggle tells you the metric, and
so does this.

- **Task 0, used only for the rehearsal:** appointments with reminders. Tenants (orgs), staff
  credentials, bookings with a start time, cancel and check-in, emailed reminders before
  the start, and a reaper on a schedule that marks no-shows. It is chosen because it leans
  on the bug classes charpente deletes: acknowledged writes, timed side effects, external
  calls with retries, tenants and auth. Outside agent A's 0.4.0 evaluation built nearly
  this app.
- **Task 1, the public attempt and the first leaderboard task:** a different domain that
  exercises the same classes. A fresh domain means anything we change in charpente or its
  docs after the rehearsal can't be tuned to the public task. The builder picks the domain
  (§10).

Every task brief must:

- **Bootstrap tenants without a deploy-time secret.** For example, a public `POST /signup`
  creates an org and returns its owner credential. A secret read at boot would push the
  charpente arm into the open first-credential gap (PLAN v1.x, trigger met), which is a
  platform gap rather than something this demo should measure.
- **Make every timed obligation fall due within one judge run,** which lasts minutes. It
  also says what happens to an obligation whose time has already passed when it is
  created.
- **Name the email provider's host,** its idempotency-key header, and the longest provider
  outage an app must survive, so every implementation can size its retries.

## 5. The judge

**Shape.** One command, run locally:

```
judge run --task <task> --target <dir>
```

- **The submission contract.** The target directory is a deployment the judge can start,
  kill and restart: a compose project with named services, the app's port, environment
  variables for the email base URL and CA certificate, and data volumes. A charpente
  entry is one service. A conventional entry is several (app, database, queue if separate).
  The judge treats the whole deployment as the system under test. The judge's fake email
  provider is reachable at a fixed IP address (or loopback) and presents a certificate for
  the brief's email host, issued by a CA the judge hands the deployment. This is
  required: charpente's `--outbound-base` accepts only an IP address or `localhost` as a
  base, never a DNS name, and TLS still verifies the declared host (GUIDE §5).
- **A smoke check.** `judge check --target <dir>` starts the deployment fault-free and
  confirms that it boots, answers the brief's routes, and reaches the fake provider. It
  says nothing about the scored classes. Agents may run it while they build, the way
  Kaggle validates a submission's format, so an entry doesn't lose on packaging alone.
- **Fresh faults every run.** Each run draws a new fault schedule from a printed seed, so
  publishing the judge does not let anyone fit to it. The only way to pass is to be
  correct. A black-box run over real processes is not byte-for-byte replayable. The seed
  reproduces the schedule, and the log is the evidence. That honesty contract is stated in
  the README, the same one the edge harness's proxy states.

**Fault classes (fixed before the rehearsal):**

1. kill -9 on any service of the deployment at drawn points, including mid-write, followed
   by a restart;
2. client-to-app connection faults: severed connections and refused connects. A request
   left unanswered is re-sent, the way a production client retries;
3. email provider faults: dropped replies after the effect landed, delays, 5xx and 429
   answers, outages up to the brief's bound;
4. clock steps, **if** one mechanism can step the clock for every stack without the app's
   cooperation. If no such mechanism holds up, v1 ships without this class, and the README
   says so before the rehearsal. It is not dropped after seeing a result. Steps are bounded
   by a size the brief states, and every run's fault-free tail outlasts the largest step.
   charpente stops a node by name when its clock goes back more than 60 s
   (`CLOCK_TOLERANCE`, GUIDE §7) and comes back once the clock catches up. The tail rule
   applies to every stack alike; it is not an exemption for charpente.

**What gets scored (the preregistered scoring, per task):**

| Class | A violation |
|---|---|
| Lost acknowledged write | A write that got a 2xx is missing or reverted after recovery |
| Missing side effect | A timed obligation that was still owed gets no effect at the email provider by its due time plus the brief's grace, once faults have stopped |
| Duplicate side effect | More than one effect for one obligation. Sends sharing an idempotency key count as one effect, the way a real provider that honours keys would |
| Effect after cancel | An effect for an obligation whose cancel was acknowledged before it came due |
| Double apply | A re-sent request changes state a second time |
| Cross-tenant access | One tenant's credential reads or changes another tenant's data |
| Unauthenticated access | A route the brief does not declare public answers 2xx without credentials |

Each violation is reported with its class, the seed, the fault schedule, and the log lines
that show it.

**A run that tested nothing fails.** As written, the seven classes can only fire on a 2xx,
an owed obligation or a state change, so a deployment that refuses everything would score
clean. The judge's version of 0.4.0's "a check that tested nothing fails" closes that:

- A run fails outright when its fault-free warm-up and tail reach fewer acknowledged
  writes and owed obligations than a floor fixed per task.
- A run also fails when the deployment does not come back within the tail after its last
  fault.
- Beside the violation counts, every run reports what it exercised (acknowledged writes,
  owed obligations, kills survived) and its availability: the share of requests answered
  during faults and during the tail. Refusing during faults is a design choice an entry
  may make. charpente makes it when IO fails or the clock steps back. The cost shows in
  availability; it is not hidden by a clean violation count.

**The verdict unit.** One claim is N judge runs, each with a fresh seed, each M minutes
long. N and M are fixed in the judge repo before the rehearsal, sized to the judge's
cost.

- **The headline:** violations per class summed over the N runs, plus how many runs had
  any violation, with every run required to pass the vacuity gate.
- **Leaderboard order:** total violations, fewest first. Ties go to availability.
- **A violation is a fact and a clean run is a sample.** When a verification rerun
  disagrees with an earlier run, every violation from any verified run counts. We list our
  rerun's result and show the submitter's beside it.
- **Claims in the post** take this exact shape: "zero violations in N runs of M minutes
  under these fault classes," never a bare "no bugs."

**Speed** is measured by the judge at a fixed load on the same machine, for each arm, and
reported apart from correctness. Its role in the post is secondary. Any figure about
charpente is quoted only after docs/BENCH.md records it with its method (repo rule), and
the post says no bare-metal Linux run exists yet.

**Proving the judge.** For every scored class, a known-bad victim app that the judge must
catch by name, plus a known-good reference app it must pass, plus a do-nothing app (one
that answers 500 to everything) that the vacuity gate must fail. Writing the victims comes
first, the way charpente's own oracles come with naive victims that fail them. A judge
that stays green on a buggy app would sink the whole project, so this proof is a gate
before the rehearsal.

**Reuse.** charpente's `crates/edge-harness` already has process control (`child.rs`), a TCP
fault proxy (`proxy.rs`) and an HTTP client. The outbound fixture has a TLS fake remote,
and `launch`'s `--outbound-base` and `--outbound-ca` flags point a charpente app at a fake
provider with no platform change. Those pieces can be copied into the judge repo, which
must not depend on the charpente workspace.

## 6. The head-to-head

- **Agent:** Claude Code run headless, latest Opus, identical for both arms: same tools,
  same wall-clock budget (an overnight run, about eight hours), and no human input after
  the start. It runs confined and logged, as the launch run was (a `./box` wrapper, a
  transcript of every command).
- **Charpente arm:** the brief plus charpente's README and GUIDE, with charpente from
  crates.io: the docs-only setup every earlier build used. It tests its work however it
  sees fit. In practice that means sim properties and `--mutants`.
- **Conventional arm:** the brief, with the stack fixed as **TypeScript on Node, Postgres,
  and a Postgres-backed job queue that can enqueue inside the same transaction**
  (pg-boss or graphile-worker, the agent's pick), with any HTTP framework. This is the
  strongest common choice for this service, and it heads off the "you used Redis"
  objection. It tests its work however it sees fit.
- **Equal footing:** both arms get the same network policy (package registries, no web
  search) and the same box image apart from the toolchains. Both are told the judged
  properties (§4), and neither sees the judge, its code or its schedules.
- **Each agent writes its own deployment** (Dockerfile and compose project) against the
  contract, because shipping the deployment is part of the job. Each box gets a container
  runtime that runs inside the box, so every command still goes through the logged
  wrapper. The agent can run its stack the way a developer would (the conventional arm can
  test against Postgres in containers) and can run `judge check`. Neither box gets the
  host's Docker socket, and we write none of the settings that bear on durability, such as
  Postgres config or restart policy. Those are the agent's.
- **Done** means the agent says the service is production-ready, or the budget runs out.
  Whatever exists at that point is judged. A deployment that doesn't start fails every
  run through the vacuity gate.

**Rehearsal (unannounced).** Task 0, charpente as currently published (0.4.0). Purposes:
1. prove the judge against real agent output (no false positives);
2. see whether a gap between the arms exists;
3. find platform defects, which get fixed red-first as usual.

0.4.0 has no transaction time (PLAN: "a transaction has no time"), so the charpente arm
will build a clock that cron advances, and that clock lags by every downtime (GUIDE §7).
Its timed obligations may come due late under kill -9 for that known reason. The
rehearsal runs anyway. Its findings are evidence for 0.5.0, as agreed in conversation.

- Timed-class violations are labelled with their cause in the record.
- The go/no-go reads the gap mainly on the classes 0.5.0 doesn't touch: lost writes,
  duplicates, double apply, tenancy and auth.
- Task 0's API uses absolute timestamps, because the domain does.
- Its grace windows come from the domain ("a reminder within a minute of its due time is
  on time") and are frozen with the fault classes before any arm runs. They are never
  sized to absorb the lag.

**Unannounced, not hidden.** The rehearsal's brief, both arms' code, the transcripts
(scrubbed) and the judge output are committed to the judge repo when it finishes, the way
every earlier run's records went into `docs/experiments/`. Nobody is pointed at them before
launch, and the post may cite them. A rehearsal failure that 0.5.0 then makes impossible
is the post's best "fixed once" material. Published records also rule out a charge that
the rehearsal was the real attempt 1, re-rolled.

Afterwards Noah decides whether there is a story. If the gap isn't there, that's a finding
about the thesis, and nothing gets written.

**Public attempt.** Task 1, after 0.5.0 ships. Before either arm starts, one commit in the
judge repo fixes the task brief, the scoring, and the judge version. The ledger records
that commit's hash, and the post cites it. Attempt 1 is published whatever it shows.

## 7. The open challenge

- **The judge repo holds the briefs, the judge, the submission contract and a leaderboard
  by task × stack × agent.** charpente's public-attempt entry is the first row, and the
  conventional arm is the second.
- **Submissions come in as pull requests** carrying the deployment, the build transcript,
  and the judge's output. Before a claim is listed we rerun it with a fresh seed, the
  way SWE-bench verifies submissions.
- **A public bug list.** Every technical bug found in a charpente entry, by the judge or by
  anyone, is listed with its fix and its pinned seed. A bug in charpente itself also gets
  a charpente fix, so every app inherits it. This is the "fixed once" claim made visible.
- **The gym:** the same judge, run during a build. Any stack can practice against it.
  charpente's point is that its gym is built in: the agent hears "no" from the simulator
  at every step, where other stacks hear it from the judge only at the end.

## 8. The writeup

- **Where:** a note on noahmitchem.com. The site is Vite + React and already renders React
  figures inside notes (for example `InvestigationFigure.jsx`), so each interactive piece
  is a component there.
- **Voice:** Noah's, per his blog style guide (the "essay machine"). Cursor's post supplies
  the format: about 4,000 words, plain section headings, a table of contents, and each
  interactive piece right after the paragraph that sets it up.
- **Angle:** what replaces reading. Lead with the judge and the result, with charpente as
  the stack that produced it. A reader who will never write Rust should still want the
  scoreboard.
- **Interactive pieces, all built from recorded runs:**
  1. *The scale:* states the simulator explored overnight next to what a reviewer reads
     in a career. The reviewer figure needs a cited source.
  2. *The night:* a scrubbable timeline of the charpente arm's build, where each red mark
     is a seed the simulator rejected, followed by its fix and the green rerun.
  3. *One universe, step by step:* request, commit, crash, restart, replay, with Back and
     Next buttons, from one of the night's real seeds.
  4. *Plant a bug:* the reader picks a line to break from a menu and sees the real sim run
     that caught it. The options come from the arm's mutation run.
  5. *Kill it yourself:* both arms killed mid-booking, and what each did on restart, from
     judge logs.
  6. *The scoreboard:* both arms by scored class, each violation linked to its log.
- **"Fixed once,"** from history the repo already holds: outside agents' bugs that later
  releases made impossible to write. For example, the spent-App shape that 0.4.0 turned
  into a compile error, so agent A's old code stopped compiling (E0599), and the check
  that tested nothing, which now fails the batch. The public bug list (§7) carries the
  story forward.
- **Concessions:** a named section covering what the judge doesn't score (building the
  wrong thing), Rust only, greenfield only, one box, breaking changes between 0.x
  releases, and no bare-metal benchmark.
- **The ending:** an instruction: bring your stack, with the one-sentence prompt the
  launch run showed gets a stranger's agent to a red seed.
- **Written after the public attempt.** No figure in it predates the run that produced
  it.

## 9. Order of work

1. **charpente fixes found by the outside agents** (small, on main):
   - plain `charpente test --mutants` inside an app crate either defaults to the app's
     package or refuses with a message naming `--test-package`;
   - `charpente new`'s usage line says three sim properties, pinned by a test that counts
     the template's checks;
   - GUIDE §8 says what Drive re-sending "at the tail's start" means for a time-sensitive
     check.
2. **The judge:** core, task 0 oracles, victims and the reference app, passing the §5
   proof.
3. **The rehearsal**, then Noah's go/no-go.
4. **0.5.0 ships:** `tx.now()`, the Dead hook, and the re-issue key. A task with timed
   emails needs both time and a visible Dead. holdqueue's notification went Dead during an
   outage and nobody was told. 0.5.0 is a dependency with its own spec and plan, as every
   release has had, and is not designed here. Its spec can start in parallel with steps
   2 and 3, and this project's public attempt waits on its publish.
5. **Task 1:** brief, oracles and victims, then the preregistration commit.
6. **The public attempt.**
7. **The writeup, the leaderboard and the open call,** then launch.

## 10. Left to the builder

- Task 1's domain, within §4's constraints, and both briefs' exact APIs.
- The judge's implementation language and packaging. My suggestion is Rust, shipped as one
  binary and a container image, which allows reuse from the edge harness.
- The compose contract's exact names, the fault schedule's distributions, the run length,
  the grace windows, and the fixed load for speed.
- The clock-step mechanism, or the documented decision to ship v1 without it (§5).
- How the leaderboard page is generated and hosted.
- The judge repo's name. Noah picks it; it is not a builder call.

## 11. Constraints

- charpente-side work follows CLAUDE.md: the hard rules, red-first fixes with pinned seeds,
  commits straight to main with no Co-Authored-By trailer, and docs/BENCH.md as the only
  source of quotable performance numbers.
- Outward contact (the judge repo, the User-Agent strings, the post) uses
  noah@noahmitchem.com.
- No employer, past or present, is named or alluded to anywhere: repo, post, transcripts
  or history.
- Agent transcripts are scrubbed before they're committed. 0.4.0's headless builder
  copied the account email from its harness into the box's git config, so set git config
  in the box up front.

## 12. Calls made in drafting (not discussed; check these)

- The conventional stack (TypeScript, Postgres, a transactional Postgres queue) and one
  conventional arm at launch.
- An overnight budget of about eight hours per arm.
- Task 0 as the rehearsal only, with a fresh domain for the public attempt.
- The seven scored classes, and speed reported apart from them.
- The writeup on noahmitchem.com rather than a charpente site.
- The 0.5.0 release lands before the public attempt.
- Clock steps conditional on a uniform mechanism.
- A vacuity gate (a run that exercised too little, or whose deployment never came back,
  fails), with availability reported beside the violation counts.
- The verdict unit: N runs of M minutes fixed before the rehearsal, a violation in any
  verified run counts, and leaderboard order by total violations.
- Each agent writes its own deployment, with a container runtime inside its box and a
  `judge check` smoke command. We write no durability settings for either arm.
- The rehearsal's records are committed publicly when it finishes but not announced.

**For Noah to set before launch:** the signals that count as the thesis landing. For
example, a number of submissions from strangers, issues from strangers, or apps built on
charpente by strangers within some window. Stars measure the post, not the thesis.
