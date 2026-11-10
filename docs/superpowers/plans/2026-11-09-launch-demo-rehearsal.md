# The launch demo's rehearsal: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run spec §9 step 3. Two unattended agents each build Task 0 overnight, one on charpente 0.4.0 and one on TypeScript + Postgres + a transactional Postgres queue. The frozen judge scores both under one master seed. Every violation is traced to its cause, and the records go into the judge repo. The plan stops at Noah's go/no-go.

**Architecture:** Each arm gets a fresh **box**: a privileged Debian container that runs its own Docker daemon (no host socket). The agent on the Mac reaches the box only through the logged `./box` wrapper, plus Read/Write/Edit on a `work/` directory that the box mounts at `/work`. The box sits on an internal Docker network. Its only ways out are a squid proxy holding a fixed registry allowlist and a pull-through Docker Hub mirror. A launch script runs headless Claude Code under an 8-hour budget, with a fixed resume rule. A collect script freezes, copies and scrubs the evidence. The judge, frozen at the tag `prereg-rehearsal`, then scores each arm on the Mac, as in the §5 proof.

**Tech Stack:** bash (tooling, as in the launch run's `tools/`), Docker Engine 29.7.2 / Compose v5.5.1 inside the box, squid, `registry:2` (pull-through mirror), Rust 1.99.0 (the judge), Claude Code 2.1.x headless.

**Spec:** `docs/superpowers/specs/2026-11-06-launch-demo-design.md`, chiefly §5 (the judge, preregistration), §6 (the head-to-head and the rehearsal) and §11 (constraints). Two other documents are binding: the judge's own `README.md` (submission and honesty contracts, preregistration log) and the judge plan's ledger, `.superpowers/sdd/2026-11-08-launch-demo-judge/progress.md` (rulings J1–J52).

**Where the work lives:** code in `~/Documents/GitHub/judge` (dir `h2h/`, plus Task 1 in `crates/judge`); run directories at `~/Documents/GitHub/h2h-<arm>-<n>/`, outside every repo, as earlier builds' were; the record in the judge repo at `rehearsal/2026-11-task0/`; the charpente-side record in `docs/LEDGER.md` and PLAN.md; the SDD ledger at `.superpowers/sdd/2026-11-09-launch-demo-rehearsal/progress.md`.

## Global Constraints

- **Agent:** Claude Code headless with the same flags for both arms, model pinned by id: `--model claude-opus-5-5` (the latest Opus; 0.4.0's builder resolved `opus` to this id). No human input after the start (spec §6).
- **Budget:** 8 h of wall clock per arm (`28800` s), counted from launch. Whatever is in `/work/app` when the agent stops or the budget ends is judged (spec §6 "Done").
- **Charpente arm:** the brief, plus charpente's README and GUIDE from the published **0.4.0** crates, with charpente from crates.io (spec §6, "Rehearsal": "charpente as currently published (0.4.0)").
- **Conventional arm:** the brief, with the stack fixed as **TypeScript on Node, Postgres, and a Postgres-backed job queue that can enqueue inside the same transaction (pg-boss or graphile-worker, the agent's pick), any HTTP framework** (spec §6, verbatim).
- **Equal footing (spec §6):**
  - Both arms get the same network policy (package registries, no web search) and the same box image apart from the toolchains.
  - Both are told the judged properties, which BRIEF.md's "What the judge checks" does.
  - Neither sees the judge, its code or its schedules. Each can run `judge-check` only.
  - Neither box gets the host's Docker socket.
  - We write none of the settings that bear on durability: no Postgres config, no restart policy, nothing in the prompt about either.
- **The judge is frozen** at the annotated tag `prereg-rehearsal` before the first arm launches.
  - From then on, the preregistration rules in the judge README bind: a change to `crates/judge/src/score.rs`, a fault class, or a value in `crates/judge/src/task0/{mod,workload}.rs` or `crates/judge/src/schedule.rs` gets a dated entry with its reason, and both arms are judged again under it (spec §3 anti-goals).
  - N = 10 runs (`RUNS`), M = 7 min 50 s, grace windows as they stand. They are never resized to absorb the charpente arm's clock lag (spec §6).
- **Records:** committed to the judge repo when the rehearsal finishes, unannounced but not hidden (spec §6). Transcripts are scrubbed before they are committed.
  - The operator's account email must not appear anywhere: repo, plan, ledger or record. This plan names it only as "the account email".
  - No employer, past or present, is named or alluded to (spec §11).
  - The box's git config is set up front to `builder` / `builder@box.invalid` (spec §11).
  - Outward contact is noah@noahmitchem.com.
- **Commits:** straight to `main` in both repos, with no Co-Authored-By trailer (CLAUDE.md, judge ledger precedent). **Nothing is pushed.** The judge repo has no remote; its name and remote are Noah's (spec §10).
- **Shell:** scripts are `#!/usr/bin/env bash`. The controller's shell is zsh, so use `${pipestatus[1]}`, never `PIPESTATUS`. Count `test result: ok` lines rather than trusting an absent `FAILED`. Bound runs with perl `alarm`, since `timeout` is absent.
- **One heavy job at a time on Docker Desktop** (12 CPUs, 7.7 GiB) while an arm runs or a batch is judged: no test suites, no proofs, no second box.
- **Not in this plan:** speed (spec §5, "measured by the judge at a fixed load"). The judge has no fixed-load mode, and spec §6 states the rehearsal's purposes without speed. Speed is owed before the public attempt, and Task 12 reports it as open.

## Review Focus

1. **A deployment that works in its box but not on the judging host.** Examples: an image built by hand in the box with no `build:` section, a `build:` context outside `/work/app`, a bind mount to a box path. A reasonable reader expects the record to say whether a host `never_ready` is the agent's packaging or our move between the box and the host. Task 9 runs `judge check` on the host first. If it fails at `boots`, Task 9 runs `judge-check` in a fresh box with an empty image cache against a copy of the same tree, and records both.
2. **The box's proxy variables reaching the deployment.** Docker's client config injects `HTTPS_PROXY` into every container in the box, so an email client that honours it (reqwest does, and so does axios) would send mail to squid. Squid refuses that, so the email step would fail in the box only. A reasonable agent expects `judge-check` in the box to behave as it does anywhere else. Task 3 runs the reference fixture, whose reqwest client honours proxy variables, through `judge-check` in the box, and requires `ok   email`.
3. **The session dying mid-night, or the budget ending mid-command.** A reasonable reader expects the rule for both to be fixed and the same for both arms: an infrastructure error gets a fixed resume text at most 3 times, and the budget ends in a kill and then a frozen box. Task 4 pins both with a fake `claude`.
4. **The account email surviving the scrub in another spelling:** JSON-escaped (`@`), URL-encoded (`%40`), or in other letter case. A reasonable reader expects a published transcript to contain none of them. Task 5's selftest feeds each spelling and requires the check to fail on any survivor.
5. **Restricted mode's permission rules falling unevenly on the two stacks.** 0.4.0's builder had 12 Bash calls refused for a line starting with `#`, which every Rust attribute is. Restricted mode may also refuse writes to "tool-configuration files". A reasonable reader expects neither arm to be pushed into heredoc workarounds the other is spared. Task 6's pilot has each agent Write its manifest, compose file, Dockerfile and a `#`-led file, and requires no denial.

---

### Task 1: a check-only judge binary for the boxes

The agents may run `judge check`. They must not be able to run `judge run`, whose output would show the fault schedule's effects. The box gets a binary that only checks.

**Files:**
- Create: `crates/judge/src/bin/judge-check.rs`
- Modify: `crates/judge/src/cli.rs` (add `CHECK_USAGE`, `parse_check_only`)
- Modify: `crates/judge/src/report.rs` (add `render_check`)
- Modify: `crates/judge/src/main.rs` (its `check` prints through `render_check`)
- Modify: `crates/judge/Cargo.toml` (`[[bin]] name = "judge-check"`)
- Modify: `README.md` (a `judge-check` paragraph under "Quick start", and the README:217 replay clause parked in the judge ledger's "Final: parked" list: each kill's target also depends on the services running when the fault phase starts)
- Test: `crates/judge/tests/cli.rs`

**Interfaces:**
- Produces: the binary `judge-check`, with usage `usage: judge-check --task task0 --target DIR`, exit codes identical to `judge check` (0 all ok, 1 a step failed, 2 the check could not be carried out or a usage error).
- Produces: `pub fn parse_check_only(args: &[String]) -> Result<CheckArgs, UsageError>` and `pub const CHECK_USAGE: &str` in `cli.rs`; `pub fn render_check(r: &run::CheckReport) -> (String, i32)` in `report.rs`, returning the printed lines and the exit code.

- [ ] **Step 1: Write the failing tests** in `crates/judge/tests/cli.rs`

```rust
fn judge_check(args: &[&str]) -> (i32, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_judge-check")).args(args).output().unwrap();
    (out.status.code().unwrap(), String::from_utf8_lossy(&out.stderr).into_owned())
}

#[test]
fn judge_check_without_arguments_prints_its_own_usage() {
    let (code, err) = judge_check(&[]);
    assert_eq!(code, 2);
    assert!(err.contains("usage: judge-check --task task0 --target DIR"), "{err}");
    assert!(!err.contains("judge run"), "{err}");
}

#[test]
fn judge_check_refuses_run_and_score_by_name() {
    for verb in ["run", "score"] {
        let (code, err) = judge_check(&[verb, "--task", "task0", "--target", "."]);
        assert_eq!(code, 2, "{verb}");
        assert!(err.contains("judge-check only checks a deployment; it has no `run` or `score`"), "{verb}: {err}");
    }
}

#[test]
fn judge_check_names_the_contract_as_judge_check_does() {
    let dir = std::env::temp_dir().join(format!("judge-check-empty-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let d = dir.to_str().unwrap();
    let (c1, e1) = judge_check(&["--task", "task0", "--target", d]);
    let (c2, e2) = judge(&["check", "--task", "task0", "--target", d]);
    assert_eq!((c1, e1.lines().next()), (c2, e2.lines().next()));
}
```

Add a unit test in `report.rs`: `render_check` on a report with steps `[("boots: …", true), ("signup: …\n  more", false)]` returns `"ok   boots: …\nFAIL signup: …\n       more\n"` (the continuation line indented 5 spaces, as `main.rs` prints today) and exit `1`; with every step ok, exit `0`.

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p judge --test cli judge_check && cargo test -p judge --lib render_check`
Expected: compile error: no `CARGO_BIN_EXE_judge-check` and no `render_check`.

- [ ] **Step 3: Implement**

`parse_check_only` refuses `run` or `score` as the first argument with the message from the test. It parses everything else as `check`'s arguments. The binary's `main` installs the rustls provider and the panic hook exactly as `main.rs` does, then calls `run::check`, then `render_check`. Move `main.rs`'s printing loop into `render_check` unchanged, so `judge check`'s output does not move by one byte.

- [ ] **Step 4: Run the tests to verify they pass, then the judge's non-Docker suites**

Run: `cargo test -p judge --lib && cargo test -p judge --test cli && cargo test -p judge --test brief_truth`
Expected: each prints `test result: ok`; count three.

- [ ] **Step 5: Confirm no scored file moved**

Run: `git diff --stat 833abcb -- crates/judge/src/score.rs crates/judge/src/schedule.rs crates/judge/src/task0`
Expected: empty output.

- [ ] **Step 6: Commit**

```bash
git add crates/judge README.md
git commit -m "judge-check: a binary that only checks, for the head-to-head's boxes — the agents may smoke-test a deployment but never run a scored batch"
```

---

### Task 2: the box — image, network, proxy, mirror and the logged wrapper

**Files:**
- Create: `h2h/box/Dockerfile` (stages `base`, `charpente`, `conventional`)
- Create: `h2h/box/box-init` (the entrypoint)
- Create: `h2h/box/build.sh` (builds `judge-check` for Linux, then both images)
- Create: `h2h/proxy/squid.conf`, `h2h/proxy/allowlist.txt`
- Create: `h2h/box.template` (copied from charpente's `docs/experiments/2026-10-launch-run/tools/box.template`, with a header line naming its source)
- Create: `h2h/mkbox.sh`, `h2h/teardown.sh`
- Create: `h2h/selftest-box.sh`
- Modify: `.gitignore` (`/h2h/box/judge-check`, `/h2h/scrub.patterns`)

**Interfaces:**
- Produces: images `h2h-box:charpente` and `h2h-box:conventional`.
- Produces: `h2h/mkbox.sh <name> <image> <run-dir> <log>`, which creates the network `<name>-net` (`--internal`), the proxy `<name>-proxy`, the mirror `<name>-mirror`, the volume `<name>-docker` and the box `<name>`, and writes `<run-dir>/box`. It exits 0 once the box's own `docker info` answers, and 2 with a reason otherwise.
- Produces: `h2h/teardown.sh <name>`, which removes the box, proxy, mirror, network and volume, and nothing else.
- Produces: inside the box, `/usr/local/bin/judge-check`, `docker` and `docker compose`, `git` with global config `builder` / `builder@box.invalid`, and `/work` mounted from `<run-dir>/work`.

- [ ] **Step 1: Write `h2h/selftest-box.sh`** (it makes one box from each image and tears both down on exit)

Port cases 1–12 of charpente's `docs/experiments/2026-10-launch-run/tools/selftest-box.sh` unchanged in intent: verbatim logging, exit and streams kept apart, cwd persistence, a killed call on record, parallel seqs, non-UTF-8 output, usage refusal, a name in use refused, a dead `./box` killing its whole tree, an exiting command's cwd, background jobs, reaping. Add:

| # | Case | Passes when |
|---|---|---|
| 13 | the box's own Docker | `./box 'docker run --rm alpine:3 true'` exits 0, and `docker logs <name>-mirror` shows a request for `library/alpine` |
| 14 | not the host's daemon | `./box 'docker info --format {{.Name}}'` differs from the host's `docker info --format {{.Name}}`, and `docker inspect <name>` shows no bind of `/var/run/docker.sock` |
| 15 | `/work` both ways | a file written on the Mac at `<run>/work/a` reads back in the box at `/work/a`, and one written in the box at `/work/b` reads on the Mac |
| 16 | git identity | `./box 'git config --global user.email'` prints `builder@box.invalid` |
| 17 | judge-check present | `./box 'judge-check'` exits 2 and prints `usage: judge-check` |
| 18 | toolchains | in `h2h-box:charpente`, `rustc --version`, `cargo clippy --version` and `cargo fmt --version` succeed and `node` is absent. In `h2h-box:conventional`, `node --version` and `npm --version` succeed and `rustc` is absent |
| 19 | no durability settings of ours | neither image contains `/etc/postgresql` and `box-init` writes no `restart` key; a `grep -r restart /etc/docker` in the box prints nothing |

- [ ] **Step 2: Run it to verify it fails**

Run: `bash h2h/selftest-box.sh`
Expected: `mkbox.sh: No such file` (nothing exists yet).

- [ ] **Step 3: Write the image**

Pin `debian:13` by the digest it has on the build day, in the `FROM` line.

- **`base` stage:**
  - Packages: `ca-certificates curl git jq procps iptables build-essential pkg-config`.
  - Docker Engine, containerd and the Compose plugin come from Docker's apt repository for Debian 13. Use the host's versions, Engine 29.7.2 and Compose v5.5.1. If the repository lacks either exact version, take its newest 29.x and v5.x, and print what was installed in `build.sh`'s output.
  - `git config --system` sets the builder identity (`--system`, so it holds for any user).
  - `COPY judge-check /usr/local/bin/`.
- **`charpente` stage:** rustup, `--profile minimal`, the stable toolchain current on the build day, plus `clippy` and `rustfmt`.
- **`conventional` stage:** the Node LTS line current on the build day, from the official `linux-arm64` tarball, checked against its `SHASUMS256.txt`.
- Neither stage installs anything else. charpente-cli, cargo-mutants, TypeScript and Postgres are the agents' to install.

`box-init` is the entrypoint. It writes two files from the environment (`H2H_PROXY`, `H2H_MIRROR`, `H2H_NO_PROXY`), starts `dockerd` with its log at `/var/log/dockerd.log`, then `exec sleep infinity`.

`/etc/docker/daemon.json`:

```json
{"bip": "10.200.0.1/24", "host-gateway-ip": "10.200.0.1",
 "default-address-pools": [{"base": "10.201.0.0/16", "size": 24}],
 "registry-mirrors": ["$H2H_MIRROR"], "insecure-registries": ["<mirror host:port>"],
 "proxies": {"http-proxy": "$H2H_PROXY", "https-proxy": "$H2H_PROXY", "no-proxy": "$H2H_NO_PROXY"}}
```

`/root/.docker/config.json`: `{"proxies": {"default": {"httpProxy": …, "httpsProxy": …, "noProxy": …}}}`.

The inner pools are `10.200.0.0/24` and `10.201.0.0/16` so that no inner network can shadow the route to the proxy, whose network Docker Desktop allocates from 172.x or 192.168.x.

`build.sh`:
1. Builds `judge-check` in `rust:1.99.0-trixie` with the judge repo mounted read-only and `--target-dir` in a container path, using `cargo build --release --bin judge-check`, then `strip`s it.
2. Copies it to `h2h/box/judge-check`.
3. Runs `docker build --target charpente -t h2h-box:charpente h2h/box` and the same for `conventional`.
4. Prints each image id and each toolchain's `--version`.

- [ ] **Step 4: Write the proxy and the mirror**

`allowlist.txt`, exactly, one squid `dstdomain` per line:

```
.crates.io
static.rust-lang.org
registry.npmjs.org
registry.yarnpkg.com
repo.yarnpkg.com
.docker.io
production.cloudflare.docker.com
deb.debian.org
security.debian.org
dl-cdn.alpinelinux.org
```

The rule behind the list: package registries and the hosts their installers fetch from on their own. No documentation sites, no GitHub, no vendor binary CDNs. A host the agent needs and is denied is testimony, recorded per arm in Task 12. It is never added mid-rehearsal.

`squid.conf`:
- listens on 3128;
- `acl allowed dstdomain "/etc/squid/allowlist.txt"`;
- `http_access allow allowed`, then `http_access deny all`;
- `access_log stdio:/dev/stdout` with ISO-8601 times (`logformat` `%{%Y-%m-%dT%H:%M:%S}tl.%03tu %>a %Ss/%03>Hs %rm %ru`).

Pin the image as `ubuntu/squid` at its digest on the build day. The mirror is `registry:2` at its digest, with `REGISTRY_PROXY_REMOTEURL=https://registry-1.docker.io`.

- [ ] **Step 5: Write `mkbox.sh` and `teardown.sh`**

`mkbox.sh`:
1. Refuses an existing container or network of the name (exit 2, "every attempt gets a fresh box").
2. Creates `<name>-net` as `--internal`.
3. Starts the proxy and the mirror, each attached to the default `bridge` and to `<name>-net`.
4. Reads their IPs on `<name>-net` with `docker inspect`.
5. Starts the box: `docker run -d --init --privileged --label h2h.role=box --name <name> --network <name>-net -v <name>-docker:/var/lib/docker -v <run-dir>/work:/work`. Set `HTTP_PROXY`, `HTTPS_PROXY`, `NO_PROXY` and their lowercase twins, plus `H2H_PROXY=http://<proxy-ip>:3128`, `H2H_MIRROR=http://<mirror-ip>:5000` and `H2H_NO_PROXY=localhost,127.0.0.1,::1,10.200.0.1,10.200.0.0/24,10.201.0.0/16`, the same value in all three no-proxy forms.
6. Writes `/root` to `/tmp/.box-cwd`, as the old `mkbox.sh` did.
7. Writes `<run-dir>/box` from the template.
8. Waits up to 60 s for `docker exec <name> docker info`.

`teardown.sh` removes exactly the five named objects. It prints each one it removed, and refuses a name that does not start with `h2h-` or `selftest-`.

- [ ] **Step 6: Build the images and run the selftest**

Run: `bash h2h/box/build.sh && bash h2h/selftest-box.sh`
Expected: `selftest-box: 19 passed, 0 failed` for each image (two summary lines, since cases 13–19 run per image).

- [ ] **Step 7: Commit**

```bash
git add h2h .gitignore
git commit -m "h2h: the head-to-head's box — a Docker daemon of its own, an internal network whose only ways out are a registry allowlist and a Hub mirror, and the launch run's logged ./box"
```

---

### Task 3: the network policy holds, and `judge-check` works inside a box

**Files:**
- Create: `h2h/selftest-net.sh`
- Modify: `h2h/mkbox.sh` or `h2h/box/box-init`, only if a case below fails

**Interfaces:**
- Consumes: Task 2's `mkbox.sh`, both images, `teardown.sh`.
- Produces: the proof, at the commit that the freeze tags, that the policy is the one the record will state.

- [ ] **Step 1: Write `h2h/selftest-net.sh <image>`** (it makes a box, runs every case, prints a count, and tears down)

| # | Case | Passes when |
|---|---|---|
| 1 | crates.io allowed | `./box 'curl -fsS https://index.crates.io/config.json'` exits 0 |
| 2 | npm allowed | `./box 'curl -fsS -o /dev/null https://registry.npmjs.org/pg'` exits 0 |
| 3 | docs denied | `./box 'curl -sS https://docs.rs/'` does not print a 200 page, and the proxy log has `TCP_DENIED` with `docs.rs` |
| 4 | search denied | the same for `https://www.google.com/` |
| 5 | GitHub denied | the same for `https://github.com/` |
| 6 | no way around the proxy | `./box "curl --noproxy '*' --connect-timeout 5 -sS https://registry.npmjs.org/"` fails |
| 7 | a build reaches the registries | in the box, a Dockerfile `FROM debian:13-slim`, `RUN apt-get update && apt-get install -y curl`, `RUN curl -fsS -o /dev/null https://registry.npmjs.org/pg` builds with `docker build` |
| 8 | `judge-check` in the box, and the proxy variables do not capture its mail | copy the judge repo's `fixtures/task0-app` and `fixtures/targets/reference` to `<run>/work/fixtures/` preserving the layout (the target's `build: ../../task0-app` must resolve). Then `./box 'judge-check --task task0 --target /work/fixtures/targets/reference'` exits 0 and prints the seven `ok` lines of the judge README's "Quick start", among them `ok   email: reminder_early reached mail.judge.test over TLS` |

- [ ] **Step 2: Run it against both images**

Run: `bash h2h/selftest-net.sh h2h-box:charpente && bash h2h/selftest-net.sh h2h-box:conventional`
Expected: `selftest-net: 8 passed, 0 failed`, twice. If case 8 fails at `email`, the reference's reqwest client went through squid. Fix it by correcting `H2H_NO_PROXY` or the inner `host-gateway-ip` (Task 2's files), never by changing the fixture or the judge.

- [ ] **Step 3: Commit**

```bash
git add h2h
git commit -m "h2h: the network policy's selftest — registries reachable, docs, search and GitHub refused, no route around the proxy, and judge-check passing the reference inside a box"
```

---

### Task 4: the launch — prompts, budget, resume rule, run layout

**Files:**
- Create: `h2h/launch.sh`
- Create: `h2h/prompts/machine.md`, `h2h/prompts/charpente.md`, `h2h/prompts/conventional.md`, `h2h/prompts/resume.txt`
- Create: `h2h/fetch-charpente-docs.sh`
- Create: `h2h/test/fake-claude`, `h2h/test/fake-mkbox`
- Create: `h2h/selftest-launch.sh`

**Interfaces:**
- Consumes: Task 2's `mkbox.sh` (replaceable by `H2H_MKBOX` in tests).
- Produces: `h2h/launch.sh <arm> <attempt>`, where `<arm>` is one of `charpente`, `conventional`, `pilot-charpente`, `pilot-conventional`. It writes these beside the run directory `<root>/h2h-<arm>-<attempt>/` (`<root>` defaults to `~/Documents/GitHub`, overridable by `H2H_RUNS_ROOT`):
  - `<run>.prompt.txt`;
  - `<run>.launch.jsonl`;
  - `<run>.stream-<k>.jsonl` and `<run>.err-<k>`, one pair per session segment, `k` from 1;
  - `<run>.box.jsonl`;
  - `<run>.final/app`: a full copy (`rsync -a`) of `work/app` taken while the box is paused. **This copy is what gets judged and collected.**

  It exits 0 once an `end` event is written, and 2 on a refusal before launch.
- Environment:
  - `CLAUDE_BIN`, default `claude`;
  - `H2H_BUDGET_SECS`, default `28800`;
  - `H2H_RUNS_ROOT`;
  - `H2H_DOCS_DIR`, a directory holding charpente's `README.md` and `GUIDE.md`, required by the `charpente` and `pilot-charpente` arms;
  - `H2H_JUDGE_REPO`, default the script's own repo;
  - `H2H_MKBOX`, default `h2h/mkbox.sh`.
- Produces: `<run>.launch.jsonl` events, one JSON object per line:
  - `{"event":"launch","utc","arm","attempt","judge_head","prereg_tag","image","image_id","model","budget_secs","brief_sha256","docs":{name:sha256},"docker_cpus","docker_mem"}`;
  - `{"event":"resume","utc","segment","session_id","remaining_secs"}`;
  - `{"event":"end","utc","reason":"finished"|"budget"|"gave_up_after_3_resumes","segments","resumes","elapsed_secs"}`;
  - `{"event":"frozen","utc","box","final_files"}` (the number of files in `<run>.final/app`);
  - `{"event":"stopped","utc"}`.
- Produces: `h2h/fetch-charpente-docs.sh <version> <dest>`. It downloads `charpente-cli-<version>.crate` and `charpente-<version>.crate` from `https://static.crates.io/crates/<name>/<name>-<version>.crate`, extracts `README.md` from the first and the file named `GUIDE.md` from the second, and prints both sha256s.

- [ ] **Step 1: Write the prompts.** The text is the experiment, so it is exact. `{RUN}` is the run directory's absolute path. The first line of the prompt is the arm's paragraph, then a blank line, then `machine.md`.

`h2h/prompts/charpente.md`:

```
Your directory is {RUN}. Read BRIEF.md there and build what it describes, on charpente, a Rust backend platform, from crates.io: `cargo install charpente-cli` installs its command. Its README.md and GUIDE.md are in your directory. Test your work however you see fit.
```

`h2h/prompts/conventional.md`:

```
Your directory is {RUN}. Read BRIEF.md there and build what it describes in TypeScript on Node, with Postgres as the database and a Postgres-backed job queue that can enqueue inside the same transaction as your writes (pg-boss or graphile-worker, your pick), and any HTTP framework. Test your work however you see fit.
```

`h2h/prompts/machine.md`:

```
How this machine works, the same for every builder:

- `./box '<command>'` runs a command on a Linux machine, in the directory the previous call left. It is your only way to run commands. The machine has Docker with Compose, and git.
- Your directory's `work/` folder is `/work` on that machine. Read, Write and Edit work on the files in your directory, so you can edit /work's files from here.
- Your deployment goes in `/work/app`, with `compose.yaml` at its root. That directory is what gets judged, on another machine, with `docker compose up --build`, so it must build from its own files alone.
- `judge-check --task task0 --target /work/app`, run on the machine, starts your deployment without faults and checks that it boots, answers the brief's routes and reaches the email provider. It says nothing about the judged properties. Run it as often as you like.
- The machine reaches package registries (crates.io, npm, Docker Hub, Debian and Alpine packages, rustup) and nothing else on the internet. You have no web search.
- One tool call may not run longer than ten minutes, and a ./box call counts. Run anything long in the background, for example ./box 'nohup <command> > <log> 2>&1 &', then check on it with further ./box calls until it has finished, and read the whole log before concluding anything.
- You have eight hours from now. Nobody will answer questions or look at your work before then. When the service is production-ready, say so in your final message and stop. If the time runs out first, what is in /work/app then is judged.
```

`h2h/prompts/resume.txt`:

```
Your session stopped on an infrastructure error, not on anything you did. Continue where you left off.
```

The pilot arms use Task 6's pilot brief and the same `machine.md`, with these differences:
- The arm paragraph is replaced by `Your directory is {RUN}. Read BRIEF.md there and do what it says.`
- The last bullet of `machine.md` reads `You have twenty minutes.` in place of `eight hours`.

- [ ] **Step 2: Write `h2h/selftest-launch.sh`.** It uses `CLAUDE_BIN=h2h/test/fake-claude`, `H2H_MKBOX=h2h/test/fake-mkbox` and a temporary `H2H_RUNS_ROOT`. The fake mkbox writes a `box` script and makes no container. The fake claude records its argv and cwd to a file. It behaves by `FAKE_MODE`: `ok`, `err-once`, `always-err` or `sleep`.

Cases:

| # | Case | Passes when |
|---|---|---|
| 1 | layout, charpente | the run dir holds exactly `BRIEF.md GUIDE.md README.md box work`, `work` is empty, and `launch.jsonl`'s `launch` event names both docs' sha256 |
| 2 | layout, conventional | exactly `BRIEF.md box work` |
| 3 | the command line | the fake's argv is exactly `-p <prompt> --model claude-opus-5-5 --restricted --tools Bash,Read,Write,Edit --permission-mode dontAsk --allowedTools Read Write Edit Bash(./box:*) --strict-mcp-config --output-format stream-json --verbose`, and its cwd is the run dir |
| 4 | finished | `ok`: one segment; `end` reason `finished`, `resumes` 0 |
| 5 | one resume | `err-once`: two segments; the second argv carries `--resume fake-session-1` and `-p` with `resume.txt`'s text; `end` `finished`, `resumes` 1 |
| 6 | resumes are bounded | `always-err`: four segments, then `end` reason `gave_up_after_3_resumes` |
| 7 | budget | `sleep` with `H2H_BUDGET_SECS=3`: `end` reason `budget` with `elapsed_secs` between 3 and 40, and the fake's pid is gone afterwards |
| 8 | a fresh directory every attempt | a second launch of the same arm and attempt exits 2 with `exists: every attempt gets a fresh directory` |
| 9 | the frozen judge | with `H2H_JUDGE_REPO` pointing at a temporary repo whose `crates/judge/src/score.rs` differs from its tag `prereg-rehearsal`, launching `charpente` exits 2 naming `crates/judge/src/score.rs`. A `pilot-*` arm is not held to the tag |
| 10 | the judged copy | a fake that writes `work/app/x` and then finishes leaves `<run>.final/app/x` with the same bytes, and the events end `end`, `frozen` (`final_files` 1), `stopped` |

A segment that ended with a `result` event whose `is_error` is `false` is finished. Anything else is an infrastructure error: a non-zero exit, no `result` event, or `is_error` `true`. `session_id` comes from segment 1's `init` event. The fake's `init` event carries `session_id` `fake-session-1`.

- [ ] **Step 3: Run it to verify it fails**

Run: `bash h2h/selftest-launch.sh`
Expected: FAIL at case 1 (`launch.sh` missing).

- [ ] **Step 4: Write `launch.sh`**

Preflight, each refusal exit 2 with its reason:
1. The judge repo (`H2H_JUDGE_REPO`, default the script's repo) is clean.
2. For `charpente` and `conventional`, the tag `prereg-rehearsal` exists and `git diff --quiet prereg-rehearsal -- crates/judge/src tasks` holds; otherwise name each changed path.
3. The run dir does not exist.
4. The image exists, or `H2H_MKBOX` is set.
5. No container labelled `h2h.role=box` has status `running`, unless `H2H_MKBOX` is set. A previous arm's box was killed at its end, and its proxy and mirror were stopped.
6. Docker's disk has at least 100 GB free (`docker run --rm alpine:3 df -Pk /`), skipped under `H2H_MKBOX`.

Then lay out the run directory (Step 1's layout; the docs come from `H2H_DOCS_DIR`) and call mkbox with `<run>.box.jsonl` as the log. Write the prompt and the `launch` event. Run segments.

The budget is a deadline fixed at launch. A watchdog sends `TERM` to the segment's `claude` at the deadline, then `KILL` 30 s later. A resume is allowed only while more than 60 s remain.

After the `end` event:
1. `docker pause <box>`.
2. `rsync -a <run>/work/app/ <run>.final/app/`.
3. Write the `frozen` event.
4. `docker kill <box>`, then `docker stop <box>-proxy <box>-mirror`.
5. Write the `stopped` event.

The `docker` steps are skipped when no such container exists. The judged tree is then exactly `/work/app` at the end, whatever was still running in the box. Nothing of a finished arm holds memory or CPU while the next arm runs or a batch is judged. Task 12 removes the stopped containers, the network and the volume.

- [ ] **Step 5: Run the selftest**

Run: `bash h2h/selftest-launch.sh`
Expected: `selftest-launch: 9 passed, 0 failed`.

- [ ] **Step 6: Commit**

```bash
git add h2h
git commit -m "h2h: the launch — both arms' prompts word for word, an 8-hour deadline, three resumes on an infrastructure error with a fixed text, and a box frozen at the end"
```

---

### Task 5: collect and scrub

**Files:**
- Create: `h2h/collect.sh`, `h2h/scrub.sh`
- Create: `h2h/selftest-collect.sh` and its fixtures under `h2h/test/`

**Interfaces:**
- Consumes: Task 4's run layout and `launch.jsonl`.
- Produces: `h2h/scrub.sh <patterns-file> <in> <out>`, which exits 0 on a clean output, 1 naming the file and line of a survivor, and 2 on a missing or empty patterns file.
- Produces: `h2h/collect.sh <run-dir> <dest>`, which fills `<dest>` with:
  - `app/`: `<run>.final/app` without `target/`, `node_modules/` or `.git/`;
  - `app.git-log.txt` and `app.bundle`, when `<run>.final/app` is a git repo with commits;
  - `work-listing.txt`;
  - `agent.jsonl`: every segment's stream, in order;
  - `box.jsonl`, `proxy.log`, `mirror.log`, `launch.jsonl` and `prompt.txt`;
  - `MANIFEST.sha256`: the sha256 of each original and of each output.

  Every text file passes through `scrub.sh` with `h2h/scrub.patterns`. That file is gitignored, holds one literal and its replacement per line, tab-separated, and is written by the controller.

- [ ] **Step 1: Write the selftest**

Its fixture run dir carries a fake account address `alex.example@mail.invalid`, written five ways: plain, uppercase, JSON-escaped `alex.example@mail.invalid`, URL-encoded `alex.example%40mail.invalid`, and inside a base64-free JSON string of a tool result. It also carries the home path `/Users/alexample`. The patterns file maps the address to `<operator-email>` and the home path to `/Users/operator`.

| # | Case | Passes when |
|---|---|---|
| 1 | every spelling scrubbed | `grep -ric 'alex.example' <dest>` prints 0 for every file, and `<operator-email>` appears 5 times in `agent.jsonl` |
| 2 | a survivor fails | with the patterns file lacking the address, `collect.sh` exits 1 and names `agent.jsonl` |
| 3 | a key fails | a fixture line holding `sk-ant-` makes `scrub.sh` exit 1 |
| 4 | no patterns, no collect | a missing or empty patterns file exits 2 before anything is written |
| 5 | what ships | `app/` has no `target/`, `node_modules/` or `.git/`; `app.bundle` verifies with `git bundle verify`; `MANIFEST.sha256` names every output file |

For a literal containing `@`, `scrub.sh` also fails if its local part survives, in any case.

- [ ] **Step 2: Run it to verify it fails, implement, run it to pass**

Run: `bash h2h/selftest-collect.sh`
Expected first: FAIL (`collect.sh` missing). After implementing: `selftest-collect: 5 passed, 0 failed`.

- [ ] **Step 3: Commit**

```bash
git add h2h
git commit -m "h2h: collect and scrub — the evidence copied out of a frozen box, and no operator address in any spelling survives into the record"
```

---

### Task 6: the pilots — the whole pipeline once per image, before the freeze (controller)

A short real session per image proves the pipeline before a night is spent on it. It checks the flags, Write on manifest files, the proxy, the mirror, `judge-check`, collect and scrub. Pilots are infrastructure checks, not rehearsal attempts. The record lists them as such.

**Files:**
- Create: `h2h/pilot/BRIEF.md`
- Create (record, Task 12): `rehearsal/2026-11-task0/pilots/`

- [ ] **Step 1: Write `h2h/pilot/BRIEF.md`**, exactly:

```
# Pilot

This checks the machine; it is not a task. Do exactly this, then stop.

1. With the Write tool, create these files in work/app (the machine's /work/app):
   - compose.yaml: one service named `app`, built from the Dockerfile beside it, serving HTTP on 0.0.0.0:8080 inside its container and publishing no ports;
   - a Dockerfile and the source of a server that answers GET /health with 200 and every other request with 404, written in the language your directory's instructions name, using at least one package from that language's registry, with its manifest (Cargo.toml or package.json);
   - NOTES.md, whose first line is `# Notes`.
2. Run `judge-check --task task0 --target /work/app` on the machine and put its whole output in your final message. Its steps after `boots` will fail; that is expected.
```

The pilot's arm paragraph names the language. For `pilot-charpente` it is `Rust, with packages from crates.io`, and for `pilot-conventional` it is `TypeScript on Node, with packages from npm`. Each is appended to the pilot paragraph as `Use <language>.`

- [ ] **Step 2: Run both pilots, one after the other**

Write `h2h/scrub.patterns` with the account email and the operator's home directory first.

Run: `H2H_BUDGET_SECS=1200 caffeinate -is bash h2h/launch.sh pilot-charpente 1`, then the same for `pilot-conventional`. The controller runs them in the background and waits for each to end.

- [ ] **Step 3: Check each pilot**

Each must pass all of these:
- the stream's `init` event has `model` `claude-opus-5-5`, and `tools` is exactly the set {Bash, Read, Write, Edit};
- no `tool_result` in the stream reports a permission denial for a Write or Edit, including `Cargo.toml`/`package.json`, `compose.yaml`, `Dockerfile` and `NOTES.md`;
- the agent's final message contains `ok   boots: GET /health answered 200`;
- `proxy.log` has at least one `TCP_TUNNEL/200` to the arm's registry, and `mirror.log` has the Dockerfile's base image;
- `launch.jsonl` ends with reason `finished`, then `frozen`;
- `h2h/collect.sh` into a scratch directory exits 0.

A failed check is fixed in Tasks 2–5's files, with that task's selftest re-run, and the pilot runs again as attempt `n+1`. A fix to the launch flags changes both arms alike.

- [ ] **Step 4: Tear the pilot boxes down, and log the outcome**

Run: `bash h2h/teardown.sh h2h-pilot-charpente-<n>` and the same for the conventional pilot, after the collect.
Write each pilot's checks and outcome to the SDD ledger.

---

### Task 7: the freeze, then the charpente arm (controller)

**Files:**
- No code. Evidence goes into `~/Documents/GitHub/h2h-charpente-1*` and the SDD ledger.

- [ ] **Step 1: Freeze the judge**
  - The judge repo is clean, and `cargo test -p judge --lib && cargo test -p judge --test cli && cargo test -p judge --test brief_truth` gives three `test result: ok`.
  - `git diff --stat 833abcb..HEAD -- crates/judge/src/score.rs crates/judge/src/schedule.rs crates/judge/src/task0` is empty, so the §5 proof at `432df09` still certifies the scored code.
  - Tag it: `git tag -a prereg-rehearsal -m "the rehearsal's judge: scoring, fault classes and task 0 values as of this commit"`.
  - Record the tag's commit in the SDD ledger.
- [ ] **Step 2: Build what the arms and the judging use**
  - At the tag, `cargo build --release`, then record `shasum -a 256 target/release/judge`.
  - `bash h2h/box/build.sh` (images now carry the tag's `judge-check`), then record both image ids and the toolchain versions it prints.
  - `bash h2h/fetch-charpente-docs.sh 0.4.0 ~/Documents/GitHub/h2h-docs-0.4.0` and record both sha256s.
- [ ] **Step 3: Launch**

Nothing else heavy should be running on Docker Desktop.

Run (background): `H2H_DOCS_DIR=~/Documents/GitHub/h2h-docs-0.4.0 caffeinate -is bash h2h/launch.sh charpente 1`
Expected: a `launch` event, then about 8 h of session, then `end` and `frozen`. The controller does not message, steer or resume the agent by hand. A stall in the harness itself is recorded and diagnosed, never answered inside the session.
- [ ] **Step 4: Collect**

Run: `bash h2h/collect.sh ~/Documents/GitHub/h2h-charpente-1 <judge repo>/rehearsal/2026-11-task0/charpente`
Expected: exit 0, and `launch.jsonl` ends `end`, `frozen`, `stopped`.
- [ ] **Step 5: Ledger**

Record the start and end times, end reason, segments, resumes, `./box` call count (the `end` lines in `box.jsonl`), the agent's final message, and the denied hosts in `proxy.log`.

---

### Task 8: the conventional arm (controller)

Same as Task 7's Steps 3–5 with `conventional`. Launch it once Task 7's collect has finished. It uses the same tag, images and budget. Its `launch` event must show the same `prereg_tag` commit and the same `model` as the charpente arm's, and the controller checks both before the collect.

- [ ] **Step 1: Launch** — `caffeinate -is bash h2h/launch.sh conventional 1` (background)
- [ ] **Step 2: Collect** into `rehearsal/2026-11-task0/conventional`
- [ ] **Step 3: Ledger**, as Task 7 Step 5, plus the two `launch` events' `prereg_tag` and `model` compared

---

### Task 9: judging (controller)

**Files:**
- Create (record): `rehearsal/2026-11-task0/<arm>/judge/` (the `--out` directory), `judge.txt`, `judge.err`, `check.txt`

- [ ] **Step 1: Draw one master seed for both arms, before either batch**

Run: `printf '0x%016x\n' $(od -An -N8 -tu8 /dev/urandom)`, then write the result to the SDD ledger before the first batch starts. The same master seed for both arms gives both the same fault schedules.
- [ ] **Step 2: Smoke each target on the host**

Run: `./target/release/judge check --task task0 --target ~/Documents/GitHub/h2h-<arm>-1.final/app > <record>/<arm>/check.txt`

If it fails at `boots`, copy `<run>.final/app` into the `work/` directory of a fresh box, `h2h-diag-<arm>`, made by `mkbox.sh`, whose image cache starts empty. Run `judge-check` there, record its output beside `check.txt` as `check-fresh-box.txt`, and tear that box down. This is Review Focus 1. The batch runs whatever the smoke shows, because a deployment that does not start fails through the gate (spec §6).
- [ ] **Step 3: Judge each arm, one batch at a time**

Run: `caffeinate -is ./target/release/judge run --task task0 --target ~/Documents/GitHub/h2h-<arm>-1.final/app --runs 10 --master-seed <M> --out <record>/<arm>/judge > <record>/<arm>/judge.txt 2> <record>/<arm>/judge.err`, then record the exit code.
Expected: a verdict line from the judge README's table, about 85 min per arm.
- [ ] **Step 4: VOID and ERROR runs**

Run each `rerun` command a void or error run printed, once, with `--out <record>/<arm>/judge-rerun-<seed>`, and record both results. A FAIL is a fact and is never re-run. A run that could not be carried out twice is reported as such.

---

### Task 10: the violation audit — no false positive, and every timed violation's cause

Spec §6's first purpose is to prove the judge against real agent output. A judge false positive found here halts the plan. The fix then gets a preregistration log entry in the judge README with its date and reason, and **both** arms are judged again under it with the same master seed.

**Files:**
- Create (record): `rehearsal/2026-11-task0/<arm>/audit.md`

- [ ] **Step 1: Dispatch one fresh reviewer per arm** (opus; each writes its file before replying)

Each gets the arm's `judge/` directory, `app/`, the judge README's "Scoring" section and `crates/judge/src/score.rs` at the tag. It must:
- Group every violation into clusters by mechanism. Each cluster gets a name, a count, the run-and-request ids, and the app code path that produced it (file:line).
- Trace at least one instance per cluster end to end through `runlog.json` and `compose.log`.
- Give a mechanical check showing every member of the cluster shares the traced signature: a `jq` query over the runlogs whose count equals the cluster's.
- Mark each cluster `true positive` or `false positive`.
- Label each `missing_effect` and `effect_not_owed` cluster with its cause (spec §6: "Timed-class violations are labelled with their cause in the record"). For the charpente arm, say explicitly whether the cause is the hand-built clock's lag after a kill, the known 0.4.0 limit that 0.5.0's `tx.now()` addresses, or something else.
- Report every gate failure with its cause the same way.
- [ ] **Step 2: Verify the audit (controller)**
  - Re-run each cluster's `jq` query and compare its count.
  - `judge score --log <run>/runlog.json --format json` for every run must reproduce the batch's totals.
  - Read one traced instance per arm in full.

  A reviewer's claim the controller has not re-derived stays marked unverified in `audit.md`.

---

### Task 11: the charpente arm's review, and both arms' testimony

Spec §6's third purpose: find platform defects. Every finding gets a disposition: **fixed** (with the commit), a **PLAN v1.x row** (with its trigger), or **refused** (with the reason). That is PLAN M3 sub-project 3's rule for every docs-only or stranger build.

**Files:**
- Create (record): `rehearsal/2026-11-task0/charpente/review.md`, `rehearsal/2026-11-task0/testimony.md`

- [ ] **Step 1: Dispatch the charpente arm's review** (opus, fresh; writes its file first)

The reviewer checks these, and records any framework patch, `[patch]` or `path` dependency as a Critical:
- every charpente crate in `Cargo.lock` comes from crates.io at 0.4.0;
- the sim properties the agent wrote, and the outcome of the last `charpente test --sim` in `box.jsonl`;
- whether the agent ran `--mutants`, and its result;
- each of the agent's own reported frictions (`agent.jsonl`'s final message and any FINDINGS-like file in `app/`), triaged as platform or app;
- every audit cluster from Task 10 whose cause lies in the platform or its docs.
- [ ] **Step 2: Census the testimony from both arms**

List the agent's final claims for each arm: production-ready, properties tested, durability, idempotency, tenancy. Rule each claim right, wrong or unproven against the judge's result and the code, citing evidence the way the launch-run README's census does.
- [ ] **Step 3: Assign dispositions (controller)**

Assign one to every charpente finding. The rehearsal ran on 0.4.0, and the order of work puts 0.5.0 next (spec §9 step 4). So a finding 0.5.0 already covers cites its PLAN row: the transaction's own time, the Dead hook, or the re-issue key. A new platform defect gets a new PLAN row whose trigger reads "fix red-first before or with 0.5.0", unless it is refused with a reason. This plan writes no charpente code.

---

### Task 12: the record, the ledgers, and the go/no-go packet (controller)

**Files:**
- Create: `rehearsal/2026-11-task0/README.md` (judge repo)
- Modify: judge `README.md`, "Preregistration log": one line, `The rehearsal's judge is the tag prereg-rehearsal (<commit>), <date>.`, followed by `No entries.` or the entries Task 10 forced
- Modify (charpente): `docs/LEDGER.md` (a dated entry), `PLAN.md` (Task 11's new rows and the table heading's count)

- [ ] **Step 1: Write the record README.** It is a fact sheet with no adjectives, in the launch-run README's shape:
  - **Setup:**
    - the judge tag and commit, and the release binary's sha256;
    - both image ids and toolchain versions;
    - the model id, the budget and the allowlist;
    - the master seed;
    - the brief's sha256, and the charpente docs' sha256s.
  - **Pilots:** each pilot's checks and outcome.
  - **Attempts table, one row per arm:**
    - start and end;
    - end reason, segments, resumes;
    - turns and `./box` calls;
    - denied hosts;
    - the verdict line and exit code.
  - **Scoreboard:**
    - per-class totals and runs with a violation for each arm;
    - gate failures;
    - per-run exercised and availability figures, copied from `judge.txt`.
  - **The five classes 0.5.0 does not touch, side by side:** lost writes, duplicates, double apply, tenancy and auth (spec §6: the go/no-go reads the gap mainly on these).
  - **Timed classes,** with Task 10's cause labels.
  - **Audit verdict:** any false positive, and what it forced.
  - **Testimony census** (Task 11).
  - **Platform findings** with their dispositions (Task 11).
- [ ] **Step 2: Scrub check over the whole record**

Run `h2h/scrub.sh`'s survivor check over every file under `rehearsal/` and the planned commit. It must report zero survivors. Also run `git -C <judge> diff --cached | grep -ci '<operator-email local part>'`, typing the local part at the prompt and never into a file; it must print `0`.
- [ ] **Step 3: Commit the record in the judge repo**

```bash
git add rehearsal README.md
git commit -m "rehearsal: task 0, charpente 0.4.0 against TypeScript + Postgres + <queue>, judged at prereg-rehearsal under master seed <M> — the record, the audit and the testimony"
```
- [ ] **Step 4: Ledger and PLAN in charpente.** Write a LEDGER entry headed `## 2026-11-<dd> — the launch demo's rehearsal` that cites the judge commit, the master seed, the scoreboard's two totals, the audit verdict and the findings' dispositions. Add the PLAN rows from Task 11, and update the table heading's count phrase. Commit:

```bash
git add docs/LEDGER.md PLAN.md
git commit -m "records: the launch demo's rehearsal — <one-clause result>, its platform findings' dispositions, and the go/no-go that is Noah's"
```
- [ ] **Step 5: Tear down**
  - Run `bash h2h/teardown.sh h2h-charpente-1` and the same for `h2h-conventional-1`, plus any `h2h-diag-*`.
  - Copy each run dir's ungitted files to `~/Documents/GitHub/h2h-archive/`: the raw streams and `err` files, kept because the record holds only scrubbed copies.
  - Record their sha256s in the SDD ledger.
- [ ] **Step 6: The go/no-go packet. Stop here.** Report to Noah:
  - the scoreboard;
  - the gap on the five classes 0.5.0 does not touch;
  - the timed classes' causes;
  - whether the judge produced any false positive;
  - the platform findings and their dispositions;
  - the judge ledger's rulings J47 and J49, carried for this decision (J49: the proof certifies one stack shape);
  - speed, still unmeasured and owed before the public attempt.

  Two things are his alone: the go/no-go (spec §6: "Noah decides whether there is a story"), and the judge repo's name and remote, which publishing the record needs (spec §10). The push of both repos is his.
