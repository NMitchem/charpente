//! The report is the artifact (spec §5-§6): every honesty-contract field is a
//! field here, not prose elsewhere. JSON is hand-rolled (no deps) and its
//! shape is pinned byte-exact by test, the sim-harness discipline.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

#[derive(Debug, Clone)]
pub struct Report {
    pub hardware: String,
    pub os: String,
    pub filesystem: String,
    pub cores: usize,
    pub sync_semantics: &'static str,
    // T5 review, M-7 / Noah ruling #3: every number in the review, the
    // implementer's report and the ledger came from an unoptimized dev
    // build (`cargo run`/`cargo test`, no `--release`) — this barely
    // matters for the fsync-bound write path and matters a great deal for
    // `reads_per_sec` and the kernel/wire layers. "dev" or "release" from
    // `cfg!(debug_assertions)` (spec §5).
    pub build_profile: &'static str,
    pub warmup_secs: f64,
    pub duration_secs: f64,
    pub device: Option<DeviceReport>,
    pub core: Option<CoreReport>,
    pub kernel: Option<KernelReport>,
    pub wire: Vec<WireReport>,
    pub invalid: Vec<String>,
}
#[derive(Debug, Clone)]
pub struct DeviceReport {
    pub syncs_per_sec: f64,
    pub appends_per_sec: f64,
}
#[derive(Debug, Clone)]
pub struct CoreReport {
    pub acked_writes_per_sec: f64,
    pub log_records_per_sec: f64,
    pub reads_per_sec: f64,
    // 2026-08-10 group-commit interlude, T1 fix wave, I-4: the device-ratio
    // gate's NUMERATOR, published for the same reason
    // `device_ratio_denominator` below was added for the denominator (T5
    // review C-1 / Noah ruling #1) — so `device_ratio` is reconstructable
    // from the artifact instead of implicit. Today equals
    // `acked_writes_per_sec`/`log_records_per_sec` exactly (one transaction,
    // one fsync, no batching); a group-commit primary path is where the two
    // diverge.
    pub syncs_per_sec: f64,
    pub device_ratio: f64,
    // T5 review, C-1 / Noah ruling #1: state in the artifact itself which
    // device operand the ratio divides by, rather than leaving it implicit
    // in spec §5's prose.
    pub device_ratio_denominator: &'static str,
    // T5 review, C-1 / Noah ruling #1: independent device-sync samples
    // bracketing the measured write window (before and after), so the
    // ratio's error bar is visible in the artifact instead of invisible —
    // a single sample per operand cannot distinguish a real >1x reading
    // from cross-window noise.
    pub device_appends_per_sec_before: f64,
    pub device_appends_per_sec_after: f64,
    // Task 3 (2026-08-19 phase-attribution plan): the measured write
    // window's phase partition — see `tracing_disk`/`phase_accounting`.
    // `None` means the attribution was refused (the refusal strings are
    // among this run's `invalid` reasons), never a zeroed `PhaseTotals` —
    // that would be indistinguishable from a genuinely fast run.
    //
    // Task 5: now rendered by `render_json`/`render_human`, alongside
    // `phases_counted` and `phases_absent_reason` — see those fields' docs
    // for why `None` alone is not enough provenance.
    pub phases: Option<crate::phase_accounting::PhaseTotals>,
    // Task 5: whether this arm was RECORDING at all (`TracingDisk`'s trace
    // was `Trace::recording`, not `Trace::passthrough`) — mirrors
    // `SweepPoint::syncs_counted`'s shape: a fact about how the measurement
    // was taken, published beside a number that may or may not be there.
    // `false` only when the arm was not recording in the first place, which
    // is a DIFFERENT fact from "recorded, but this window's attribution was
    // refused" — `phases_absent_reason` is what tells those two apart.
    pub phases_counted: bool,
    // Task 5 (Ruling 2 / Task 3's Ruling 14): `phases: None` conflates THREE
    // causes a reader must be able to tell apart without parsing prose out
    // of `invalid` — see `PhasesAbsentReason`'s doc. `Some` exactly when
    // `phases` is `None`; `None` needs no reason.
    pub phases_absent_reason: Option<PhasesAbsentReason>,
    // The measured write window's own length in nanoseconds
    // (`window_end_nanos - window_start_nanos`), the SAME window `phases`
    // was attributed against — not `write.elapsed`. The two are stamped
    // from different clock reads a handful of nanoseconds apart, so a
    // caller checking `phases.total() == window_nanos` against the wrong
    // one would see a flaky mismatch nobody could reproduce.
    pub window_nanos: u64,
    // Task 5: the number of ops `Trace::record` actually pushed for this
    // arm's WHOLE run (not just the in-window subset `phases` describes) —
    // `overhead_gate`'s own `op_count`, published so a caller wiring the
    // gate against this run does not have to re-derive it. 0 whenever
    // `!phases_counted` (`Trace::record` early-returns before touching the
    // event vec in passthrough mode).
    pub events_recorded: u64,
}
#[derive(Debug, Clone)]
pub struct KernelReport {
    pub acked_writes_per_sec: f64,
    pub log_records_per_sec: f64,
    // T6 review, I-4 / Noah ruling: this layer reads neither --warmup nor
    // --duration, so the artifact's methodology header describes a window it
    // never used. Publish the one it did measure.
    pub window_secs: f64,
    // 2026-08-10 group-commit interlude, T1 fix wave, I-4: the device-ratio
    // gate's NUMERATOR, published so `device_ratio` is reconstructable from
    // the artifact (mirrors `CoreReport::syncs_per_sec` — the same standing
    // ruling applied symmetrically to the numerator). Fix round I-1,
    // correcting this doc's own old claim: it used to say this equals
    // `log_records_per_sec` exactly ("one commit, one fsync, no batching,
    // two syncs per request") — false since T6/T7's batching landed. Both
    // the request arm (T6) and the job arm (T7) now batch (`Group::commit`
    // -> `Engine::submit_all`), and this is COUNTED at the `Disk` boundary,
    // never derived from `log_records_per_sec` — the two are no longer the
    // same number in general.
    pub syncs_per_sec: f64,
    // `syncs_per_sec / max(device_before, device_after)` (2026-08-10
    // group-commit interlude, T1) — the numerator is SYNCS, not the log
    // record rate; see `kernel_accounting` for why. Fix round I-1: this used
    // to say the two are "the same number, one commit, one sync" — that was
    // true only pre-batching. Both arms batch now, so `syncs_per_sec` is
    // genuinely counted rather than a copy of `log_records_per_sec`, and
    // `syncs_per_sec` beside it is what lets a reader check the two AGAINST
    // each other from the artifact itself (they diverge under batching),
    // rather than assume they still coincide.
    pub device_ratio: f64,
    // 2026-08-10 interlude §4.2: this layer brackets its own batch window, as
    // core and wire already did. The shipped refusal message used to name the
    // absence of exactly these two fields as the likeliest cause of its own
    // firing.
    pub device_appends_per_sec_before: f64,
    pub device_appends_per_sec_after: f64,
    pub requests_total: u64,
    pub jobs_done: u64,
    // Task 3 (2026-08-19 phase-attribution plan): the measured batch
    // window's phase partition — mirrors `CoreReport::phases`. `None` means
    // the attribution was refused; the refusal strings are among this run's
    // `invalid` reasons.
    //
    // Task 5: now rendered — see `CoreReport::phases`'s doc, which this
    // mirrors exactly, for `phases_counted`/`phases_absent_reason`.
    pub phases: Option<crate::phase_accounting::PhaseTotals>,
    // Task 5: mirrors `CoreReport::phases_counted`.
    pub phases_counted: bool,
    // Task 5: mirrors `CoreReport::phases_absent_reason`.
    pub phases_absent_reason: Option<PhasesAbsentReason>,
    // The measured batch window's own length in nanoseconds — mirrors
    // `CoreReport::window_nanos`. This layer already publishes
    // `window_secs`; this is the nanosecond-precision figure `phases` was
    // attributed against.
    pub window_nanos: u64,
    // Task 5: mirrors `CoreReport::events_recorded`. This is the SAME count
    // `run_with_trace_mode`'s third return element already carries (Task
    // 4); `run`'s two-element wrapper drops it, so this is the first place
    // an ordinary caller of `run` can see it.
    pub events_recorded: u64,
}
#[derive(Debug, Clone)]
pub struct WireReport {
    pub target: String,
    pub points: Vec<SweepPoint>,
}
#[derive(Debug, Clone)]
pub struct SweepPoint {
    pub connections: usize,
    pub throughput_per_sec: f64,
    pub log_records_per_sec: f64,
    // 2026-08-10 group-commit interlude, T1 fix wave, I-4/C-1: the
    // device-ratio gate's ACTUAL subject. Published so the gate's own
    // numerator is reconstructable from the artifact rather than implicit,
    // mirroring the standing ruling that added `device_ratio_denominator` for
    // the DENOMINATOR (T5 review C-1 / Noah ruling #1).
    //
    // **2026-08-14: this may now be COUNTED, and the comment that used to say
    // "NOT a counted quantity — a client outside the process cannot see the
    // server's log" is superseded.** That was true of the log, and remains
    // true of `log_records_per_sec` below; it stopped being true of syncs the
    // moment a target began reporting its own. bench-app now does, over
    // `/__syncs`.
    //
    // **T13 (Noah ruling (b), 2026-08-19): `None` where the rate would have
    // been DERIVED, and the number is withdrawn rather than published.** The
    // derivation is `Target::syncs_per_request() * throughput`, which assumes
    // one fsync per commit — precisely what group commit removed. A quantity
    // known false a priori is not made safe by labelling it, so it is no
    // longer emitted: `null` in the artifact, `n/a (derived, unsound under
    // batching)` in the human render. `Some` is a real fsync tally from inside
    // the server and carries the device-ratio gate exactly as before. Read
    // `syncs_counted` for the provenance; read this for the number, if there
    // is one.
    pub syncs_per_sec: Option<f64>,
    /// Whether the sync rate for this point was a real server-reported count
    /// (`true`) or the one-fsync-per-commit derivation (`false`). Published
    /// because two numbers of different evidentiary standing must not appear in
    /// one table looking alike — the same reason `device_ratio_denominator` is
    /// published.
    ///
    /// Since T13 this also predicts `syncs_per_sec`: `true` ⇒ `Some`, `false` ⇒
    /// `None`. Both are still published because they answer different
    /// questions — this one says whether the harness COULD count, which stays
    /// true of a target even at a point where there is no number to show.
    pub syncs_counted: bool,
    // T7 rev. 4 (Noah ruling, 2026-08-09), SUPERSEDED at the wire layer by
    // the 2026-08-10 group-commit interlude's T1 fix wave (C-1): this field
    // is still published-but-not-gated-on — it stays the log-record-rate
    // derivation, which is a DIFFERENT quantity from the gate's numerator
    // now that `syncs_per_sec` exists as its own field — but the gate itself
    // moved OFF the counted acked-request rate onto `syncs_per_sec` above,
    // reversing this comment's own "a derived quantity cannot carry a gate"
    // claim for that specific numerator. See wire_accounting's module doc for
    // the full reasoning and why it is not a regression to the false-alarm
    // behaviour rev. 4 fixed.
    pub device_ratio: f64,
    // T7 rev. 3 (Noah ruling, 2026-08-09): each point brackets its own
    // ceiling (before/after, child not running) and both ratios divide by the
    // max — published here, named exactly as CoreReport's equivalents, so the
    // ratio's error bar is visible in the artifact. Bracketing replaced a
    // single cross-window device.syncs_per_sec sample and removed real drift;
    // rev. 4 then measured a bracket-stable ceiling and found the false
    // alarms came from the DERIVED numerator, not this denominator. NOT
    // comparable to CoreReport::device_ratio or KernelReport::device_ratio —
    // different denominators (see wire_accounting's module doc).
    pub device_appends_per_sec_before: f64,
    pub device_appends_per_sec_after: f64,
    pub p50_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub client_busy_fraction: f64,
    pub saturated: bool,
}

/// Why `phases` is `None`, when it is (Task 5, Ruling 2 / carried from Task
/// 3's Ruling 14). `None` alone conflates three causes, and a reader must be
/// able to tell them apart WITHOUT parsing prose out of `invalid` — the
/// refusal strings that land there are the raw material this type is built
/// from, not the interface a consumer reads.
///
/// The first two mean an attempt was made and rejected; the third means no
/// attempt was made at all — a different fact, and the one
/// `phases_counted` also names on its own (this type is the finer-grained
/// sibling: `phases_counted == false` implies `NotRecording` here, and
/// nothing else can).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhasesAbsentReason {
    /// `phase_accounting::attribute` refused this window's trace (an
    /// overlapping, malformed, or boundary-straddling event) — the refusal
    /// strings themselves are in `invalid`, `"contract:"`-prefixed.
    AttributionRefused,
    /// `Trace::capacity_grew_reason` detected a reallocation of the event
    /// buffer during recording — a regrow's stall lands in whichever gap
    /// bucket happens to surround it, indistinguishable after the fact from
    /// genuine idle time, so the shares are withheld rather than published
    /// diluted.
    TraceRegrew,
    /// This arm was not recording at all (`tracing_disk::TraceMode::
    /// Passthrough`) — there is no trace to attribute in the first place,
    /// which is a different fact from a trace that was attributed and
    /// refused.
    NotRecording,
}

impl PhasesAbsentReason {
    /// The JSON tag. Stable, lowercase, snake_case — a consumer matches on
    /// this string, not on prose.
    pub fn as_str(self) -> &'static str {
        match self {
            PhasesAbsentReason::AttributionRefused => "attribution_refused",
            PhasesAbsentReason::TraceRegrew => "trace_regrew",
            PhasesAbsentReason::NotRecording => "not_recording",
        }
    }

    /// A short phrase for the human render — not a full sentence; the full
    /// explanation is in `invalid` already (except for `NotRecording`, which
    /// names nothing in `invalid` because there was no attempt to refuse).
    fn human_phrase(self) -> &'static str {
        match self {
            PhasesAbsentReason::AttributionRefused => "attribution refused, see invalid reasons",
            PhasesAbsentReason::TraceRegrew => "trace buffer regrew, see invalid reasons",
            PhasesAbsentReason::NotRecording => "this arm was not recording",
        }
    }
}

pub const CEILING_REFUSAL: &str = "a number without its device ceiling is not publishable \
output — run the device layer in the same invocation (spec §5)";

fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

/// NaN/Infinity are not JSON numbers (fix round 1, review I-1): a single non-finite
/// field would otherwise make the entire artifact unparseable, silently, with
/// `render_json` still returning `Ok`. Ruled (Noah, 2026-08-07): render the field as
/// `null` and name the reason in `invalid`, rather than refuse — a non-finite number
/// usually co-occurs with an already-INVALID run, and refusing would destroy the very
/// artifact carrying the explanation (spec §5).
fn non_finite_reason(field: &str, v: f64) -> String {
    format!(
        "{field} is not finite ({v}): NaN/Infinity is not a JSON number (typically a rate \
         divided by a zero-length interval) — recorded as null and this run is marked \
         INVALID (spec §5)"
    )
}

fn push_if_non_finite(out: &mut Vec<String>, field: &str, v: f64) {
    if !v.is_finite() {
        out.push(non_finite_reason(field, v));
    }
}

/// Scans every float field for non-finite values, in a fixed field order. Both
/// renderers call this so `render_json`'s `invalid` array and `render_human`'s `!!`
/// lines always agree about what is wrong with a run.
fn collect_non_finite(r: &Report) -> Vec<String> {
    let mut out = Vec::new();
    push_if_non_finite(&mut out, "warmup_secs", r.warmup_secs);
    push_if_non_finite(&mut out, "duration_secs", r.duration_secs);
    if let Some(d) = &r.device {
        push_if_non_finite(&mut out, "device.syncs_per_sec", d.syncs_per_sec);
        push_if_non_finite(&mut out, "device.appends_per_sec", d.appends_per_sec);
    }
    if let Some(c) = &r.core {
        push_if_non_finite(
            &mut out,
            "core.acked_writes_per_sec",
            c.acked_writes_per_sec,
        );
        push_if_non_finite(&mut out, "core.log_records_per_sec", c.log_records_per_sec);
        push_if_non_finite(&mut out, "core.reads_per_sec", c.reads_per_sec);
        push_if_non_finite(&mut out, "core.syncs_per_sec", c.syncs_per_sec);
        push_if_non_finite(&mut out, "core.device_ratio", c.device_ratio);
        push_if_non_finite(
            &mut out,
            "core.device_appends_per_sec_before",
            c.device_appends_per_sec_before,
        );
        push_if_non_finite(
            &mut out,
            "core.device_appends_per_sec_after",
            c.device_appends_per_sec_after,
        );
    }
    if let Some(k) = &r.kernel {
        push_if_non_finite(
            &mut out,
            "kernel.acked_writes_per_sec",
            k.acked_writes_per_sec,
        );
        push_if_non_finite(
            &mut out,
            "kernel.log_records_per_sec",
            k.log_records_per_sec,
        );
        push_if_non_finite(&mut out, "kernel.window_secs", k.window_secs);
        push_if_non_finite(&mut out, "kernel.syncs_per_sec", k.syncs_per_sec);
        push_if_non_finite(&mut out, "kernel.device_ratio", k.device_ratio);
        // 2026-08-10 interlude: scanned by the SAME scan as core's and wire's
        // equivalents — a published field this scan skips is exactly how the
        // two renderers drift apart (T7's Step 1 finding, one layer over).
        push_if_non_finite(
            &mut out,
            "kernel.device_appends_per_sec_before",
            k.device_appends_per_sec_before,
        );
        push_if_non_finite(
            &mut out,
            "kernel.device_appends_per_sec_after",
            k.device_appends_per_sec_after,
        );
    }
    for w in &r.wire {
        for (pi, p) in w.points.iter().enumerate() {
            let prefix = format!("wire[{}].points[{pi}]", w.target);
            push_if_non_finite(
                &mut out,
                &format!("{prefix}.throughput_per_sec"),
                p.throughput_per_sec,
            );
            push_if_non_finite(
                &mut out,
                &format!("{prefix}.log_records_per_sec"),
                p.log_records_per_sec,
            );
            // T13: only a published rate can be non-finite. A withdrawn one
            // renders `null` for a reason that is not a NaN, and scanning
            // `None` would have to invent a value to scan.
            if let Some(syncs) = p.syncs_per_sec {
                push_if_non_finite(&mut out, &format!("{prefix}.syncs_per_sec"), syncs);
            }
            push_if_non_finite(&mut out, &format!("{prefix}.device_ratio"), p.device_ratio);
            push_if_non_finite(
                &mut out,
                &format!("{prefix}.device_appends_per_sec_before"),
                p.device_appends_per_sec_before,
            );
            push_if_non_finite(
                &mut out,
                &format!("{prefix}.device_appends_per_sec_after"),
                p.device_appends_per_sec_after,
            );
            push_if_non_finite(
                &mut out,
                &format!("{prefix}.client_busy_fraction"),
                p.client_busy_fraction,
            );
        }
    }
    out
}

/// Every reason this run is INVALID: `r.invalid` (accumulated by the
/// measurement layers as they ran) plus `collect_non_finite`'s rendering-time
/// findings, in that order. This is the ONE place "is this run publishable"
/// should be answered from (T4 review, I-3) — gating on `r.invalid` alone, as
/// `main`'s exit code did before this fix, misses a non-finite value that is
/// discovered only at render time (Task 3's ruling forbids a renderer
/// mutating the caller's `Report`, so it can never land in `r.invalid`
/// itself) and can report a run valid when its own rendered artifact already
/// says INVALID.
pub fn invalid_reasons(r: &Report) -> Vec<String> {
    let mut reasons = r.invalid.clone();
    reasons.extend(collect_non_finite(r));
    reasons
}

fn num2(v: f64) -> String {
    if v.is_finite() {
        format!("{v:.2}")
    } else {
        "null".to_string()
    }
}

fn num3(v: f64) -> String {
    if v.is_finite() {
        format!("{v:.3}")
    } else {
        "null".to_string()
    }
}

/// `num2` for a rate that may have been WITHDRAWN rather than measured (T13).
/// Collapses to the same `null` a non-finite value renders as, deliberately:
/// the artifact's contract is "this key carries no number", and a reader who
/// needs to know WHICH kind of absence this is reads `syncs_counted` beside it.
fn num2_opt(v: Option<f64>) -> String {
    v.map_or_else(|| "null".to_string(), num2)
}

/// Negative space (hard rule 4), shared by both renderers below: the four
/// phase-provenance fields must never contradict each other. Called once per
/// layer per render rather than trusted from construction, because a struct
/// literal's fields are independently settable and this is the one place
/// both renderers agree the combination has to make sense.
fn assert_phases_consistent(
    phases: &Option<crate::phase_accounting::PhaseTotals>,
    counted: bool,
    reason: Option<PhasesAbsentReason>,
) {
    assert_eq!(
        phases.is_none(),
        reason.is_some(),
        "phases_absent_reason must be Some exactly when phases is None — got phases.is_some()={} \
         reason={reason:?}",
        phases.is_some()
    );
    if !counted {
        assert_eq!(
            reason,
            Some(PhasesAbsentReason::NotRecording),
            "an arm that was not recording must report NotRecording, not {reason:?}"
        );
    }
}

/// The `phases`/`phases_counted`/`phases_absent_reason`/`window_nanos`/
/// `events_recorded` fragment shared by `core` and `kernel` — both objects
/// publish the identical shape (Task 5), so this is written once rather than
/// risking the two renderers drifting apart the way this file's own history
/// warns about repeatedly.
fn phases_json(
    phases: &Option<crate::phase_accounting::PhaseTotals>,
    counted: bool,
    reason: Option<PhasesAbsentReason>,
    window_nanos: u64,
    events_recorded: u64,
) -> String {
    assert_phases_consistent(phases, counted, reason);
    let mut s = String::new();
    match phases {
        Some(t) => {
            s.push_str(&format!(
                ",\"phases\":{{\"wal_append_io\":{},\"wal_read_io\":{},\"wal_sync_io\":{},\
                 \"store_write_io\":{},\"store_read_io\":{},\"store_sync_io\":{},\
                 \"other_io\":{},\"gap_within_wal\":{},\"gap_wal_to_store\":{},\
                 \"gap_within_store\":{},\"gap_store_to_wal\":{},\"gap_other\":{},\
                 \"lead_in\":{},\"tail\":{}}}",
                t.wal_append_io,
                t.wal_read_io,
                t.wal_sync_io,
                t.store_write_io,
                t.store_read_io,
                t.store_sync_io,
                t.other_io,
                t.gap_within_wal,
                t.gap_wal_to_store,
                t.gap_within_store,
                t.gap_store_to_wal,
                t.gap_other,
                t.lead_in,
                t.tail,
            ));
        }
        None => s.push_str(",\"phases\":null"),
    }
    s.push_str(&format!(",\"phases_counted\":{counted}"));
    match reason {
        Some(reason) => s.push_str(&format!(
            ",\"phases_absent_reason\":\"{}\"",
            esc(reason.as_str())
        )),
        None => s.push_str(",\"phases_absent_reason\":null"),
    }
    s.push_str(&format!(
        ",\"window_nanos\":{window_nanos},\"events_recorded\":{events_recorded}"
    ));
    s
}

/// The human phase table shared by `core`/`kernel` (Task 5) — mirrors
/// `phases_json`'s reasoning for being one function rather than two.
/// `label` names the layer ("core"/"kernel") for the absent-case line, which
/// is the only line that does not already carry it from context.
fn phases_human(
    label: &str,
    phases: &Option<crate::phase_accounting::PhaseTotals>,
    counted: bool,
    reason: Option<PhasesAbsentReason>,
    window_nanos: u64,
) -> String {
    assert_phases_consistent(phases, counted, reason);
    let Some(t) = phases else {
        let reason = reason.expect("assert_phases_consistent just proved this is Some");
        return format!("  {label} phases: n/a ({})\n", reason.human_phrase());
    };
    let pct = |v: u64| {
        if window_nanos == 0 {
            0.0
        } else {
            v as f64 / window_nanos as f64 * 100.0
        }
    };
    let mut s = format!("  {label} phases (window {window_nanos} ns, counted):\n");
    for (name, v) in [
        ("wal_append_io", t.wal_append_io),
        ("wal_read_io", t.wal_read_io),
        ("wal_sync_io", t.wal_sync_io),
        ("store_write_io", t.store_write_io),
        ("store_read_io", t.store_read_io),
        ("store_sync_io", t.store_sync_io),
        ("other_io", t.other_io),
        ("gap_within_wal", t.gap_within_wal),
        ("gap_wal_to_store", t.gap_wal_to_store),
        ("gap_within_store", t.gap_within_store),
        ("gap_store_to_wal", t.gap_store_to_wal),
        ("gap_other", t.gap_other),
        ("lead_in", t.lead_in),
        ("tail", t.tail),
    ] {
        s.push_str(&format!("    {name:<16} {v:>14} ns ({:>5.1}%)\n", pct(v)));
    }
    s
}

pub fn render_json(r: &Report) -> Result<String, String> {
    let has_layers = r.core.is_some() || r.kernel.is_some() || !r.wire.is_empty();
    if has_layers && r.device.is_none() {
        return Err(CEILING_REFUSAL.to_string());
    }
    let all_invalid = invalid_reasons(r);
    let mut s = String::new();
    s.push_str(&format!(
        "{{\"bench\":{{\"hardware\":\"{}\",\"os\":\"{}\",\"filesystem\":\"{}\",\"cores\":{},\
         \"sync_semantics\":\"{}\",\"build_profile\":\"{}\",\"warmup_secs\":{},\
         \"duration_secs\":{}",
        esc(&r.hardware),
        esc(&r.os),
        esc(&r.filesystem),
        r.cores,
        esc(r.sync_semantics),
        esc(r.build_profile),
        num2(r.warmup_secs),
        num2(r.duration_secs)
    ));
    if let Some(d) = &r.device {
        s.push_str(&format!(
            ",\"device\":{{\"syncs_per_sec\":{},\"appends_per_sec\":{}}}",
            num2(d.syncs_per_sec),
            num2(d.appends_per_sec)
        ));
    }
    if let Some(c) = &r.core {
        s.push_str(&format!(
            ",\"core\":{{\"acked_writes_per_sec\":{},\"log_records_per_sec\":{},\
             \"reads_per_sec\":{},\"syncs_per_sec\":{},\"device_ratio\":{},\
             \"device_ratio_denominator\":\"{}\",\
             \"device_appends_per_sec_before\":{},\"device_appends_per_sec_after\":{}",
            num2(c.acked_writes_per_sec),
            num2(c.log_records_per_sec),
            num2(c.reads_per_sec),
            num2(c.syncs_per_sec),
            num3(c.device_ratio),
            esc(c.device_ratio_denominator),
            num2(c.device_appends_per_sec_before),
            num2(c.device_appends_per_sec_after)
        ));
        s.push_str(&phases_json(
            &c.phases,
            c.phases_counted,
            c.phases_absent_reason,
            c.window_nanos,
            c.events_recorded,
        ));
        s.push('}');
    }
    if let Some(k) = &r.kernel {
        s.push_str(&format!(
            ",\"kernel\":{{\"acked_writes_per_sec\":{},\"log_records_per_sec\":{},\
             \"window_secs\":{},\"syncs_per_sec\":{},\"device_ratio\":{},\
             \"device_appends_per_sec_before\":{},\"device_appends_per_sec_after\":{},\
             \"requests_total\":{},\"jobs_done\":{}",
            num2(k.acked_writes_per_sec),
            num2(k.log_records_per_sec),
            num2(k.window_secs),
            num2(k.syncs_per_sec),
            num3(k.device_ratio),
            num2(k.device_appends_per_sec_before),
            num2(k.device_appends_per_sec_after),
            k.requests_total,
            k.jobs_done
        ));
        s.push_str(&phases_json(
            &k.phases,
            k.phases_counted,
            k.phases_absent_reason,
            k.window_nanos,
            k.events_recorded,
        ));
        s.push('}');
    }
    if !r.wire.is_empty() {
        s.push_str(",\"wire\":[");
        for (wi, w) in r.wire.iter().enumerate() {
            if wi > 0 {
                s.push(',');
            }
            s.push_str(&format!("{{\"target\":\"{}\",\"points\":[", esc(&w.target)));
            for (pi, p) in w.points.iter().enumerate() {
                if pi > 0 {
                    s.push(',');
                }
                s.push_str(&format!(
                    "{{\"connections\":{},\"throughput_per_sec\":{},\
                     \"log_records_per_sec\":{},\"syncs_per_sec\":{},\"syncs_counted\":{},\
                     \"device_ratio\":{},\
                     \"device_appends_per_sec_before\":{},\"device_appends_per_sec_after\":{},\
                     \"p50_us\":{},\"p99_us\":{},\"p999_us\":{},\"client_busy_fraction\":{},\
                     \"saturated\":{}}}",
                    p.connections,
                    num2(p.throughput_per_sec),
                    num2(p.log_records_per_sec),
                    num2_opt(p.syncs_per_sec),
                    p.syncs_counted,
                    num3(p.device_ratio),
                    num2(p.device_appends_per_sec_before),
                    num2(p.device_appends_per_sec_after),
                    p.p50_us,
                    p.p99_us,
                    p.p999_us,
                    num3(p.client_busy_fraction),
                    p.saturated
                ));
            }
            s.push_str("]}");
        }
        s.push(']');
    }
    s.push_str(",\"invalid\":[");
    for (i, reason) in all_invalid.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!("\"{}\"", esc(reason)));
    }
    s.push_str("]}}");
    Ok(s)
}

pub fn render_human(r: &Report) -> String {
    let mut s = String::new();
    // T5 review, I-4: the bare `{:.0}` printed "warmup 0s" for a real 0.5s
    // warmup (round-half-to-even) and "measure 0s" for a real 0.4s window —
    // false values for the two most load-bearing methodological fields in a
    // benchmark artifact. `num2` matches the JSON renderer's precision and
    // non-finite handling. T5 review, M-1: "(per window)" states that a
    // layer running multiple measured windows (core: write, then read) uses
    // this SAME pair for each one, not once for the whole invocation.
    s.push_str(&format!(
        "bench: {} | {} | {} | {} cores | {} build | sync: {} | warmup {}s, measure {}s \
         (per window)\n",
        r.hardware,
        r.os,
        r.filesystem,
        r.cores,
        r.build_profile,
        r.sync_semantics,
        num2(r.warmup_secs),
        num2(r.duration_secs)
    ));
    if let Some(d) = &r.device {
        s.push_str(&format!(
            // 2026-08-10 interlude: no longer "(the ceiling)". Since §4.2 no
            // layer divides by these; each brackets its own window. This line
            // is the artifact's methodology header, not a gate input.
            "  device: {:.0} syncs/s, {:.0} appends/s (reference; layers bracket their own)\n",
            d.syncs_per_sec, d.appends_per_sec
        ));
    }
    if let Some(c) = &r.core {
        s.push_str(&format!(
            // 2026-08-10 group-commit interlude, T1 fix wave, M-3: the ratio
            // is SYNCS' ratio, not acked writes' — print the syncs rate
            // beside it rather than juxtaposing the ratio with a different
            // published number.
            "  core:   {:.0} acked writes/s, {:.0} durable syncs/s ({:.2}x {}, bracket \
             {:.0}/{:.0} appends/s before/after), {:.0} log records/s, {:.0} reads/s\n",
            c.acked_writes_per_sec,
            c.syncs_per_sec,
            c.device_ratio,
            c.device_ratio_denominator,
            c.device_appends_per_sec_before,
            c.device_appends_per_sec_after,
            c.log_records_per_sec,
            c.reads_per_sec
        ));
        s.push_str(&phases_human(
            "core",
            &c.phases,
            c.phases_counted,
            c.phases_absent_reason,
            c.window_nanos,
        ));
    }
    if let Some(k) = &r.kernel {
        s.push_str(&format!(
            // T6 review, I-4: name this layer's OWN window here. The header
            // above carries --warmup/--duration, which this layer never read.
            // 2026-08-10 group-commit interlude, T1 fix wave, M-2/M-3: print
            // the syncs rate the ratio actually divides by, rather than
            // asserting in prose that it equals the log rate (a claim with a
            // shelf life that nothing pinned).
            "  kernel: {:.0} acked writes/s, {:.0} log records/s, {:.0} durable syncs/s ({} \
             requests, {} jobs; {:.2}s batch window, no warmup; device_ratio {:.3}, bracket \
             {:.0}/{:.0} appends/s before/after)\n",
            k.acked_writes_per_sec,
            k.log_records_per_sec,
            k.syncs_per_sec,
            k.requests_total,
            k.jobs_done,
            k.window_secs,
            k.device_ratio,
            k.device_appends_per_sec_before,
            k.device_appends_per_sec_after
        ));
        s.push_str(&phases_human(
            "kernel",
            &k.phases,
            k.phases_counted,
            k.phases_absent_reason,
            k.window_nanos,
        ));
    }
    for w in &r.wire {
        s.push_str(&format!("  wire [{}]:\n", w.target));
        for p in &w.points {
            // T10a fix, two defects the T10 measurement run's instrumentation
            // review found: (1) `syncs_counted` was published nowhere, so a
            // reader could not tell a real fsync tally from the
            // syncs-per-request-times-throughput derivation apart — now
            // named right after the rate it qualifies. (2) `device_ratio` is
            // the RECORDS ratio (`log_records_per_sec` / ceiling,
            // published-but-ungated — see wire_accounting's module doc), NOT
            // the ratio the gate actually divides on (`syncs_per_sec` /
            // ceiling); the old comment here claimed the line printed the
            // latter and it printed the former, so a passing row read as
            // confirming the ceiling claim while showing the wrong number.
            // Both ratios are shown now, each named for what it is, the sync
            // one computed from fields already on this same line rather than
            // published as a new field (nothing judged here changes). Since T13
            // the sync half appears on counted points only — see the next
            // paragraph.
            //
            // T13 (Noah ruling (b)): a point whose rate was WITHDRAWN prints
            // the withdrawal in place of both the number and its ratio, rather
            // than printing a number qualified by a word. "derived" as a tag
            // beside a figure still reads as a measurement to anyone skimming;
            // `n/a` cannot. The records ratio stays on the line in both arms —
            // it is the same published-but-ungated quantity it always was.
            let sync_cell = match p.syncs_per_sec {
                Some(syncs) => {
                    let sync_ratio = syncs
                        / p.device_appends_per_sec_before
                            .max(p.device_appends_per_sec_after);
                    format!("{syncs:>8.0} sync/s (counted)  {sync_ratio:.3}x dev (sync, gated)")
                }
                None => "n/a (derived, unsound under batching)".to_string(),
            };
            s.push_str(&format!(
                "    {:>4} conns: {:>8.0} req/s  {:>8.0} rec/s  {}  {:.3}x dev (records, \
                 ungated)  (bracket {:.0}/{:.0} appends/s \
                 before/after)  p50 {:>6}us  p99 {:>6}us  p99.9 {:>6}us  client busy {:.0}%{}\n",
                p.connections,
                p.throughput_per_sec,
                p.log_records_per_sec,
                sync_cell,
                p.device_ratio,
                p.device_appends_per_sec_before,
                p.device_appends_per_sec_after,
                p.p50_us,
                p.p99_us,
                p.p999_us,
                p.client_busy_fraction * 100.0,
                if p.saturated {
                    "  CLIENT-SATURATED (point invalid)"
                } else {
                    ""
                }
            ));
        }
    }
    for reason in invalid_reasons(r) {
        s.push_str(&format!("  !! {reason}\n"));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixed_report() -> Report {
        Report {
            hardware: "Test \"Box\" 1".to_string(),
            os: "TestOS 1.0 arm64".to_string(),
            filesystem: "TestFS".to_string(),
            cores: 8,
            sync_semantics: "F_FULLFSYNC via sync_all",
            build_profile: "dev",
            warmup_secs: 5.0,
            duration_secs: 30.0,
            device: Some(DeviceReport {
                syncs_per_sec: 1500.25,
                appends_per_sec: 1400.5,
            }),
            core: Some(CoreReport {
                acked_writes_per_sec: 1450.0,
                log_records_per_sec: 1450.0,
                reads_per_sec: 800000.0,
                syncs_per_sec: 1450.0,
                device_ratio: 0.97,
                device_ratio_denominator: "the device bracket max",
                device_appends_per_sec_before: 1490.0,
                device_appends_per_sec_after: 1510.0,
                // T5: the base fixture models a RECORDING arm whose window's
                // attribution was refused for some unrelated reason (the
                // refusal string itself is not this fixture's concern) —
                // `phases: Some(...)` and the three-cause split each get
                // their own dedicated fixtures below rather than living here.
                phases: None,
                phases_counted: true,
                phases_absent_reason: Some(PhasesAbsentReason::AttributionRefused),
                window_nanos: 0,
                events_recorded: 94_562,
            }),
            kernel: Some(KernelReport {
                acked_writes_per_sec: 1300.0,
                log_records_per_sec: 2600.0,
                window_secs: 38.46,
                syncs_per_sec: 2600.0,
                device_ratio: 0.867,
                device_appends_per_sec_before: 2980.0,
                device_appends_per_sec_after: 3020.0,
                requests_total: 50000,
                jobs_done: 50000,
                phases: None,
                phases_counted: true,
                phases_absent_reason: Some(PhasesAbsentReason::TraceRegrew),
                window_nanos: 0,
                events_recorded: 1_336_846,
            }),
            wire: vec![WireReport {
                target: "bench-app".to_string(),
                points: vec![
                    SweepPoint {
                        connections: 4,
                        throughput_per_sec: 1200.0,
                        log_records_per_sec: 2400.0,
                        syncs_per_sec: Some(2400.0),
                        syncs_counted: true,
                        device_ratio: 0.685,
                        device_appends_per_sec_before: 3500.0,
                        device_appends_per_sec_after: 3600.0,
                        p50_us: 800,
                        p99_us: 2100,
                        p999_us: 5000,
                        client_busy_fraction: 0.12,
                        saturated: false,
                    },
                    // T13: the WITHDRAWN arm. Added to this fixture rather than
                    // pinned in a test of its own because the shape pin below
                    // is the artifact's contract, and a contract that describes
                    // only the counted arm is how a shape change ships
                    // unnoticed — which is exactly what happened here: making
                    // `syncs_per_sec` an `Option` broke this fixture at COMPILE
                    // time but would have left `json_shape_is_pinned` green,
                    // since its only point was a counted one.
                    SweepPoint {
                        connections: 16,
                        throughput_per_sec: 1800.0,
                        log_records_per_sec: 1800.0,
                        syncs_per_sec: None,
                        syncs_counted: false,
                        device_ratio: 0.514,
                        device_appends_per_sec_before: 3500.0,
                        device_appends_per_sec_after: 3600.0,
                        p50_us: 1100,
                        p99_us: 2600,
                        p999_us: 6000,
                        client_busy_fraction: 0.20,
                        saturated: false,
                    },
                ],
            }],
            invalid: vec![],
        }
    }

    #[test]
    fn json_shape_is_pinned() {
        let json = render_json(&fixed_report()).expect("renders");
        assert_eq!(
            json,
            "{\"bench\":{\"hardware\":\"Test \\\"Box\\\" 1\",\"os\":\"TestOS 1.0 arm64\",\
             \"filesystem\":\"TestFS\",\"cores\":8,\
             \"sync_semantics\":\"F_FULLFSYNC via sync_all\",\"build_profile\":\"dev\",\
             \"warmup_secs\":5.00,\
             \"duration_secs\":30.00,\"device\":{\"syncs_per_sec\":1500.25,\
             \"appends_per_sec\":1400.50},\"core\":{\"acked_writes_per_sec\":1450.00,\
             \"log_records_per_sec\":1450.00,\"reads_per_sec\":800000.00,\
             \"syncs_per_sec\":1450.00,\"device_ratio\":0.970,\
             \"device_ratio_denominator\":\"the device bracket max\",\
             \"device_appends_per_sec_before\":1490.00,\"device_appends_per_sec_after\":1510.00,\
             \"phases\":null,\"phases_counted\":true,\
             \"phases_absent_reason\":\"attribution_refused\",\"window_nanos\":0,\
             \"events_recorded\":94562},\
             \"kernel\":{\"acked_writes_per_sec\":1300.00,\
             \"log_records_per_sec\":2600.00,\"window_secs\":38.46,\
             \"syncs_per_sec\":2600.00,\
             \"device_ratio\":0.867,\"device_appends_per_sec_before\":2980.00,\
             \"device_appends_per_sec_after\":3020.00,\
             \"requests_total\":50000,\"jobs_done\":50000,\
             \"phases\":null,\"phases_counted\":true,\
             \"phases_absent_reason\":\"trace_regrew\",\"window_nanos\":0,\
             \"events_recorded\":1336846},\
             \"wire\":[{\"target\":\"bench-app\",\"points\":[{\"connections\":4,\
             \"throughput_per_sec\":1200.00,\"log_records_per_sec\":2400.00,\
             \"syncs_per_sec\":2400.00,\"syncs_counted\":true,\
             \"device_ratio\":0.685,\"device_appends_per_sec_before\":3500.00,\
             \"device_appends_per_sec_after\":3600.00,\"p50_us\":800,\"p99_us\":2100,\
             \"p999_us\":5000,\
             \"client_busy_fraction\":0.120,\"saturated\":false},\
             {\"connections\":16,\
             \"throughput_per_sec\":1800.00,\"log_records_per_sec\":1800.00,\
             \"syncs_per_sec\":null,\"syncs_counted\":false,\
             \"device_ratio\":0.514,\"device_appends_per_sec_before\":3500.00,\
             \"device_appends_per_sec_after\":3600.00,\"p50_us\":1100,\"p99_us\":2600,\
             \"p999_us\":6000,\
             \"client_busy_fraction\":0.200,\"saturated\":false}]}],\"invalid\":[]}}"
        );
    }

    // T5 re-review, N-5 / Noah ruling #5: build_profile's release-mode value
    // now names the workspace's deliberate redb debug-assertions override
    // (bin/bench.rs's build_profile()) — pin that the renderer emits it
    // byte-exact, parens and comma included, through the ordinary `esc` path
    // (neither character needs escaping, and nothing bypasses that path).
    #[test]
    fn json_pin_covers_the_release_build_profile_label() {
        let mut r = fixed_report();
        r.build_profile = "release (redb debug-assertions on, workspace override)";
        let json = render_json(&r).expect("renders");
        assert!(
            json.contains(
                "\"build_profile\":\"release (redb debug-assertions on, workspace override)\""
            ),
            "got: {json}"
        );
    }

    #[test]
    fn a_layer_without_the_device_ceiling_refuses_to_render() {
        let mut r = fixed_report();
        r.device = None;
        let err = render_json(&r).unwrap_err();
        assert_eq!(err, CEILING_REFUSAL);
        r.core = None;
        r.kernel = None;
        r.wire.clear();
        assert!(
            render_json(&r).is_ok(),
            "a device-less report with no layers is fine"
        );
    }

    #[test]
    fn invalid_reasons_survive_into_the_artifact() {
        let mut r = fixed_report();
        r.invalid
            .push("INVALID: heartbeat observed 1 ticks".to_string());
        let json = render_json(&r).expect("renders");
        assert!(json.contains("\"invalid\":[\"INVALID: heartbeat observed 1 ticks\"]"));
        let human = render_human(&r);
        // fix round 1 (review M-1): assert on render_human's own "!! " marker plus
        // the exact reason, not merely `.contains("INVALID")` — that passed only
        // because the fixture's own string happens to start with "INVALID: ", and
        // would keep passing even if the `!!` marker were deleted entirely.
        assert!(
            human.contains("  !! INVALID: heartbeat observed 1 ticks\n"),
            "human render must shout invalidity with its own !! marker, not merely \
             echo text the fixture happened to supply; got: {human}"
        );
    }

    // fix round 1 (review I-1, Noah-ruled 2026-08-07): a non-finite float must not
    // silently produce an unparseable artifact. It renders as JSON `null` and the
    // reason is named in `invalid`, in both renderers, without making render_json
    // refuse (CEILING_REFUSAL stays the only Err arm).
    #[test]
    fn a_non_finite_float_renders_as_null_and_marks_the_run_invalid() {
        let mut r = fixed_report();
        r.core.as_mut().unwrap().device_ratio = f64::NAN;
        r.wire[0].points[0].client_busy_fraction = f64::INFINITY;

        let json = render_json(&r).expect("non-finite floats are recorded, not refused");
        assert!(
            json.contains("\"device_ratio\":null"),
            "non-finite core.device_ratio must render as JSON null, got: {json}"
        );
        assert!(
            json.contains("\"client_busy_fraction\":null"),
            "non-finite client_busy_fraction must render as JSON null, got: {json}"
        );
        assert!(
            json.contains("core.device_ratio is not finite (NaN)"),
            "the invalid array must name the field and the offending value, got: {json}"
        );
        assert!(
            json.contains("wire[bench-app].points[0].client_busy_fraction is not finite (inf)"),
            "the invalid array must name the wire site, got: {json}"
        );

        let human = render_human(&r);
        assert!(
            human.contains("!! core.device_ratio is not finite (NaN)"),
            "render_human must surface the same reason, got: {human}"
        );
        assert!(
            human.contains("!! wire[bench-app].points[0].client_busy_fraction is not finite (inf)"),
            "render_human must surface the wire reason too, got: {human}"
        );

        // every other float is still finite: only the two perturbed fields are null.
        assert!(json.contains("\"acked_writes_per_sec\":1450.00"));
        assert!(json.contains("\"throughput_per_sec\":1200.00"));
    }

    // T5 review, C-1 / Noah ruling #1: the two new bracket-sample fields
    // must be scanned by the SAME central non-finite path as every other
    // float — do not hand-roll a separate check for them (Task 3's
    // standing note to T4-T8).
    #[test]
    fn the_bracket_sample_fields_are_scanned_for_non_finite() {
        let mut r = fixed_report();
        r.core.as_mut().unwrap().device_appends_per_sec_before = f64::NAN;
        r.core.as_mut().unwrap().device_appends_per_sec_after = f64::INFINITY;
        let json = render_json(&r).expect("non-finite floats are recorded, not refused");
        assert!(
            json.contains("\"device_appends_per_sec_before\":null"),
            "got: {json}"
        );
        assert!(
            json.contains("\"device_appends_per_sec_after\":null"),
            "got: {json}"
        );
        assert!(
            json.contains("core.device_appends_per_sec_before is not finite (NaN)"),
            "got: {json}"
        );
        assert!(
            json.contains("core.device_appends_per_sec_after is not finite (inf)"),
            "got: {json}"
        );
    }

    // T7, plan Step 1: `SweepPoint::device_ratio` must be scanned by the SAME
    // central non-finite path as every other float — a zero device ceiling
    // makes this ratio `inf` and wire_accounting must not grow its own
    // non-finite guard (interlude spec §4's standing rule).
    #[test]
    fn a_non_finite_wire_device_ratio_is_collected_by_name() {
        let mut r = fixed_report();
        r.wire[0].points[0].device_ratio = f64::INFINITY;
        let json = render_json(&r).expect("non-finite floats are recorded, not refused");
        assert!(
            json.contains(
                "\"wire\":[{\"target\":\"bench-app\",\"points\":[{\"connections\":4,\
             \"throughput_per_sec\":1200.00,\"log_records_per_sec\":2400.00,\
             \"syncs_per_sec\":2400.00,\"syncs_counted\":true,\"device_ratio\":null"
            ),
            "non-finite wire device_ratio must render as JSON null, got: {json}"
        );
        assert!(
            json.contains("wire[bench-app].points[0].device_ratio is not finite (inf)"),
            "the invalid array must name the wire site, got: {json}"
        );
        let human = render_human(&r);
        assert!(
            human.contains("!! wire[bench-app].points[0].device_ratio is not finite (inf)"),
            "render_human must surface the same reason, got: {human}"
        );
    }

    // T7 rev. 3: `SweepPoint::device_appends_per_sec_before`/`_after` must be
    // scanned by the SAME central non-finite path as every other float — the
    // `CoreReport` twin above (`the_bracket_sample_fields_are_scanned_for_
    // non_finite`) is the precedent this mirrors. `wire_accounting`'s own
    // `a_non_finite_bracket_sample_is_not_locally_guarded_and_flows_through`
    // proves the accounting layer does not catch it locally; this proves
    // this layer does.
    #[test]
    fn the_wire_bracket_sample_fields_are_scanned_for_non_finite() {
        let mut r = fixed_report();
        r.wire[0].points[0].device_appends_per_sec_before = f64::NAN;
        r.wire[0].points[0].device_appends_per_sec_after = f64::INFINITY;
        let json = render_json(&r).expect("non-finite floats are recorded, not refused");
        assert!(
            json.contains("\"device_appends_per_sec_before\":null"),
            "got: {json}"
        );
        assert!(
            json.contains("\"device_appends_per_sec_after\":null"),
            "got: {json}"
        );
        assert!(
            json.contains(
                "wire[bench-app].points[0].device_appends_per_sec_before is not finite (NaN)"
            ),
            "got: {json}"
        );
        assert!(
            json.contains(
                "wire[bench-app].points[0].device_appends_per_sec_after is not finite (inf)"
            ),
            "got: {json}"
        );
        let human = render_human(&r);
        assert!(
            human.contains(
                "!! wire[bench-app].points[0].device_appends_per_sec_before is not finite (NaN)"
            ),
            "render_human must surface the same reason, got: {human}"
        );
    }

    // T5 re-review, N-3: I-4's routing explicitly asked for a byte-exact pin
    // on render_human's header and `core:` lines so the format string cannot
    // regress silently — fix round 1 shipped, then hand-caught, a doubled
    // "device.device.syncs_per_sec" prefix bug in precisely this unpinned
    // line, found only by eyeballing a manual run. This pin would have
    // reddened it in CI.
    #[test]
    fn render_human_header_and_core_line_are_pinned() {
        let human = render_human(&fixed_report());
        let lines: Vec<&str> = human.lines().collect();
        assert_eq!(
            lines[0],
            "bench: Test \"Box\" 1 | TestOS 1.0 arm64 | TestFS | 8 cores | dev build | \
             sync: F_FULLFSYNC via sync_all | warmup 5.00s, measure 30.00s (per window)",
            "got: {human}"
        );
        assert_eq!(
            lines[2],
            "  core:   1450 acked writes/s, 1450 durable syncs/s (0.97x the device bracket \
             max, bracket 1490/1510 appends/s before/after), 1450 log records/s, 800000 \
             reads/s",
            "got: {human}"
        );
    }

    /// 2026-08-10 interlude, spec §6.6. Two human lines this file changed were
    /// pinned NOWHERE, which is how an edit to both passed a full green suite:
    /// the `device:` line (whose parenthetical stopped being true the moment
    /// §4.2 stopped any layer dividing by it) and the `kernel:` line (which
    /// gained the bracket pair). The N-3 pin above exists because exactly this
    /// class of silent renderer regression shipped once already.
    #[test]
    fn render_human_device_and_kernel_lines_are_pinned() {
        let human = render_human(&fixed_report());
        let lines: Vec<&str> = human.lines().collect();
        assert_eq!(
            lines[1],
            // 1400.50 renders as 1400, not 1401: `{:.0}` rounds half to even.
            "  device: 1500 syncs/s, 1400 appends/s (reference; layers bracket their own)",
            "got: {human}"
        );
        // Task 5: `core:`'s line index is unchanged (N-3's pin above still
        // reads lines[2]) but a new `core phases:` line now sits between it
        // and `kernel:`, since `fixed_report()`'s core arm has `phases:
        // None`.
        assert_eq!(
            lines[3], "  core phases: n/a (attribution refused, see invalid reasons)",
            "got: {human}"
        );
        assert_eq!(
            lines[4],
            "  kernel: 1300 acked writes/s, 2600 log records/s, 2600 durable syncs/s (50000 \
             requests, 50000 jobs; 38.46s batch window, no warmup; device_ratio 0.867, \
             bracket 2980/3020 appends/s before/after)",
            "got: {human}"
        );
        assert_eq!(
            lines[5], "  kernel phases: n/a (trace buffer regrew, see invalid reasons)",
            "got: {human}"
        );
    }

    // T10a fix: this line was pinned NOWHERE before this test, which is
    // exactly the class of silent renderer regression the note above
    // documents one layer over — a reviewer of that fix could not have
    // caught either defect it fixed (`syncs_counted` never printed;
    // `device_ratio` — the RECORDS ratio, published-but-ungated — juxtaposed
    // right after the sync rate as if it were the ratio the gate divides on)
    // by running the suite, only by eyeballing a manual run. Pins both the
    // provenance tag and that the sync-gated ratio (0.667 = 2400/3600, the
    // worse of the two brackets) and the records ratio (0.685, unchanged)
    // are printed as two distinctly labeled numbers, not one ambiguous one.
    #[test]
    fn render_human_wire_line_is_pinned() {
        let human = render_human(&fixed_report());
        let lines: Vec<&str> = human.lines().collect();
        // Task 5: shifted by 2 from `wire [bench-app]:`'s pre-Task-5 index —
        // `core phases:` and `kernel phases:` each add one line above it.
        assert_eq!(lines[6], "  wire [bench-app]:", "got: {human}");
        assert_eq!(
            lines[7],
            "       4 conns:     1200 req/s      2400 rec/s      2400 sync/s (counted)  \
             0.667x dev (sync, gated)  0.685x dev (records, ungated)  (bracket 3500/3600 \
             appends/s before/after)  p50    800us  p99   2100us  p99.9   5000us  client \
             busy 12%",
            "got: {human}"
        );
        // T13, and the reason this arm is pinned rather than trusted: the point
        // of withdrawing a derived rate is that a reader CANNOT mistake it for
        // a measurement, and that property lives entirely in this string. A
        // renderer regression that printed a number here — or printed
        // "0.000x dev (sync, gated)" from a `None` — would defeat the ruling
        // while every other test stayed green. Note there is no sync ratio on
        // this line at all: the ratio was the gate's subject, and there is no
        // gate on this arm.
        assert_eq!(
            lines[8],
            "      16 conns:     1800 req/s      1800 rec/s  n/a (derived, unsound under \
             batching)  0.514x dev (records, ungated)  (bracket 3500/3600 appends/s \
             before/after)  p50   1100us  p99   2600us  p99.9   6000us  client busy 20%",
            "got: {human}"
        );
    }

    // fix round 1 (review I-2): esc's `\\`, `\n`, and `<0x20` arms were exercised by
    // no test — `fixed_report()`'s only escape-triggering value hits the `"` arm.
    #[test]
    fn esc_escapes_every_control_character_arm() {
        assert_eq!(esc("plain"), "plain");
        assert_eq!(esc("a\"b"), "a\\\"b");
        assert_eq!(esc("a\\b"), "a\\\\b");
        assert_eq!(esc("a\nb"), "a\\nb");
        assert_eq!(esc("a\tb"), "a\\u0009b");
        assert_eq!(esc("a\rb"), "a\\u000db");
        assert_eq!(esc("a\u{0}b"), "a\\u0000b");
    }

    // fix round 1 (T4 review, I-3): `main`'s exit-code gate must see
    // rendering-time non-finite reasons too, not just `Report.invalid` — this
    // is the ONE function that answers "is this run publishable", so its
    // result must include what `collect_non_finite` finds even though that
    // never touches the caller's struct (Task 3's ruling forbids the
    // renderer mutating the caller's `Report`).
    #[test]
    fn invalid_reasons_includes_rendering_time_non_finite_reasons() {
        let mut r = fixed_report();
        r.core.as_mut().unwrap().device_ratio = f64::NAN;
        assert!(
            r.invalid.is_empty(),
            "the struct field alone must still be empty — this is exactly the gap"
        );
        let reasons = invalid_reasons(&r);
        assert!(
            !reasons.is_empty(),
            "invalid_reasons must see the rendering-time non-finite reason"
        );
        assert!(
            reasons
                .iter()
                .any(|s| s.contains("core.device_ratio is not finite")),
            "got: {reasons:?}"
        );
    }

    #[test]
    fn invalid_reasons_carries_pre_existing_reasons_too() {
        let mut r = fixed_report();
        r.invalid.push("INVALID: something else".to_string());
        let reasons = invalid_reasons(&r);
        assert_eq!(reasons, vec!["INVALID: something else".to_string()]);
    }

    // --- Task 5 (2026-08-19 phase-attribution plan): publishing phases ---

    /// Every field distinct and nonzero, mirroring `phase_accounting`'s own
    /// fixture discipline (`every_nanosecond_of_the_window_is_attributed`):
    /// a fixture with a repeated or zeroed value cannot catch a swap or a
    /// dropped field in the renderer either.
    fn healthy_phases() -> crate::phase_accounting::PhaseTotals {
        crate::phase_accounting::PhaseTotals {
            wal_append_io: 100,
            wal_read_io: 200,
            wal_sync_io: 5_000,
            store_write_io: 300,
            store_read_io: 50,
            store_sync_io: 400,
            other_io: 60,
            gap_within_wal: 10,
            gap_wal_to_store: 20,
            gap_within_store: 30,
            gap_store_to_wal: 40,
            gap_other: 15,
            lead_in: 5,
            tail: 25,
        }
    }
    const HEALTHY_PHASES_WINDOW_NANOS: u64 = 6_255; // healthy_phases().total()

    /// Step 1: a recording run whose attribution succeeded publishes
    /// `phases_counted: true` and the phase object, byte-exact.
    #[test]
    fn a_recording_run_publishes_phases_counted_true_and_the_phase_object() {
        let mut r = fixed_report();
        let c = r.core.as_mut().unwrap();
        c.phases = Some(healthy_phases());
        c.phases_counted = true;
        c.phases_absent_reason = None;
        c.window_nanos = HEALTHY_PHASES_WINDOW_NANOS;
        c.events_recorded = 777;

        let json = render_json(&r).expect("renders");
        assert!(json.contains("\"phases_counted\":true"), "got: {json}");
        assert!(
            json.contains(
                "\"phases\":{\"wal_append_io\":100,\"wal_read_io\":200,\"wal_sync_io\":5000,\
                 \"store_write_io\":300,\"store_read_io\":50,\"store_sync_io\":400,\
                 \"other_io\":60,\"gap_within_wal\":10,\"gap_wal_to_store\":20,\
                 \"gap_within_store\":30,\"gap_store_to_wal\":40,\"gap_other\":15,\
                 \"lead_in\":5,\"tail\":25}"
            ),
            "got: {json}"
        );
        assert!(
            json.contains("\"phases_absent_reason\":null"),
            "a healthy attribution needs no reason: got: {json}"
        );
        assert!(json.contains("\"window_nanos\":6255"), "got: {json}");
        assert!(json.contains("\"events_recorded\":777"), "got: {json}");

        let human = render_human(&r);
        assert!(
            human.contains("core phases (window 6255 ns, counted):"),
            "got: {human}"
        );
        assert!(
            human.contains("wal_sync_io "),
            "every phase name must appear: got: {human}"
        );
        // 5000/6255 = 79.9%
        assert!(human.contains("79.9%"), "got: {human}");
    }

    /// Step 1's own wording: `phases` is ABSENT (not zeroed) on a passthrough
    /// run — the cause `phase_accounting`'s own attribute() can never
    /// itself report, since an empty passthrough trace attributes cleanly
    /// to an all-`lead_in` `PhaseTotals` that LOOKS like a healthy, fast
    /// run. A zeroed phase block is indistinguishable from a genuinely fast
    /// one (`SweepPoint::syncs_per_sec`'s own `Option` precedent); this
    /// proves the JSON key is `null`, never `{"lead_in": <window>, ...}`.
    #[test]
    fn phases_is_absent_not_zeroed_on_a_not_recording_arm() {
        let mut r = fixed_report();
        let k = r.kernel.as_mut().unwrap();
        k.phases = None;
        k.phases_counted = false;
        k.phases_absent_reason = Some(PhasesAbsentReason::NotRecording);
        k.window_nanos = 999_999;
        k.events_recorded = 0;

        let json = render_json(&r).expect("renders");
        assert!(
            json.contains("\"phases\":null"),
            "absence must be `null`, not a zeroed object: got: {json}"
        );
        assert!(
            !json.contains("\"lead_in\":999999"),
            "the whole window must not have been folded into lead_in: got: {json}"
        );
        assert!(json.contains("\"phases_counted\":false"), "got: {json}");
        assert!(
            json.contains("\"phases_absent_reason\":\"not_recording\""),
            "got: {json}"
        );

        let human = render_human(&r);
        assert!(
            human.contains("kernel phases: n/a (this arm was not recording)"),
            "got: {human}"
        );
    }

    /// Step 1: the three causes behind `phases: None` must be distinguishable
    /// in the OUTPUT — without a consumer parsing prose out of `invalid`
    /// (Ruling 2 / Task 3's Ruling 14). Each fixture below carries an EMPTY
    /// `invalid` list, proving the distinction lives in `phases_absent_
    /// reason` itself, not in text this test happens to also be able to grep.
    #[test]
    fn the_three_phase_absence_causes_are_distinguishable_without_parsing_invalid() {
        let reasons = [
            (
                PhasesAbsentReason::AttributionRefused,
                "attribution_refused",
                "attribution refused, see invalid reasons",
            ),
            (
                PhasesAbsentReason::TraceRegrew,
                "trace_regrew",
                "trace buffer regrew, see invalid reasons",
            ),
            (
                PhasesAbsentReason::NotRecording,
                "not_recording",
                "this arm was not recording",
            ),
        ];
        let mut seen_json_tags = std::collections::HashSet::new();
        for (reason, json_tag, human_phrase) in reasons {
            let mut r = fixed_report();
            let c = r.core.as_mut().unwrap();
            c.phases = None;
            c.phases_counted = reason != PhasesAbsentReason::NotRecording;
            c.phases_absent_reason = Some(reason);
            r.invalid.clear(); // no prose to parse — the field alone must carry it

            let json = render_json(&r).expect("renders");
            assert!(
                json.contains(&format!("\"phases_absent_reason\":\"{json_tag}\"")),
                "reason {reason:?} got: {json}"
            );
            seen_json_tags.insert(json_tag);

            let human = render_human(&r);
            assert!(
                human.contains(&format!("core phases: n/a ({human_phrase})")),
                "reason {reason:?} got: {human}"
            );
        }
        assert_eq!(
            seen_json_tags.len(),
            3,
            "all three causes must produce three DISTINCT JSON tags, not two \
             colliding onto one"
        );
    }

    /// Hard rule 4 (assert invariants and negative space): `phases.is_none()`
    /// and `phases_absent_reason.is_some()` must never disagree — a
    /// construction site that sets one without the other is a bug this
    /// module refuses to render silently.
    #[test]
    #[should_panic(expected = "phases_absent_reason must be Some exactly when phases is None")]
    fn a_healthy_phases_block_with_a_stray_absent_reason_panics_rather_than_renders() {
        let mut r = fixed_report();
        let c = r.core.as_mut().unwrap();
        c.phases = Some(healthy_phases());
        c.phases_absent_reason = Some(PhasesAbsentReason::AttributionRefused);
        let _ = render_json(&r);
    }

    #[test]
    #[should_panic(expected = "an arm that was not recording must report NotRecording")]
    fn an_uncounted_arm_with_a_different_reason_panics_rather_than_renders() {
        let mut r = fixed_report();
        let c = r.core.as_mut().unwrap();
        c.phases = None;
        c.phases_counted = false;
        c.phases_absent_reason = Some(PhasesAbsentReason::TraceRegrew);
        let _ = render_json(&r);
    }
}
