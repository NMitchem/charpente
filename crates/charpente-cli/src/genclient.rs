//! `charpente gen-client` / `charpente check-client` (TS-codegen spec §4).
//!
//! The contract comes from the same app-build-and-exec path `describe` uses
//! (`describe::contract_json`); the emitter is a pure function; so the only
//! IO here is writing or reading one file.
//!
//! `check-client` is the oracle, not the convenience. A stale client exits 1,
//! names the command that fixes it, and prints the FIRST differing line — CI
//! must show what drifted without dumping a generated file into the log.

const KNOWN_FLAGS: &[&str] = &["--dir", "--out", "--bin"];

pub const USAGE: &str = "usage: charpente gen-client [--dir PATH] [--out PATH] [--bin NAME]\n\
       charpente check-client [--dir PATH] [--out PATH] [--bin NAME]\n\
       --dir PATH   the app crate directory to build and read the contract from (default: .)\n\
       --out PATH   the generated TypeScript client, committed to the repo (default: ./client.ts)\n\
       --bin NAME   which binary to read the contract from when the crate has several \
(default: the sole binary, else the one named like the package)";

struct Parsed {
    dir: String,
    out: String,
    bin: Option<String>,
}

/// Both verbs take the same flags, so they share one parser; `verb` is only
/// in the diagnostics. Every flag takes exactly one value.
fn parse(verb: &str, args: &[String]) -> Result<Parsed, i32> {
    let mut dir = ".".to_string();
    let mut out = "./client.ts".to_string();
    let mut bin = None;

    let mut iter = args.iter();
    while let Some(flag) = iter.next() {
        if !KNOWN_FLAGS.contains(&flag.as_str()) {
            eprintln!(
                "error: charpente {verb}: unknown flag \"{flag}\" — valid flags: {}\n\n{USAGE}",
                KNOWN_FLAGS.join(", ")
            );
            return Err(2);
        }
        let Some(value) = iter.next() else {
            eprintln!("error: charpente {verb}: {flag} requires a value\n\n{USAGE}");
            return Err(2);
        };
        match flag.as_str() {
            "--dir" => dir = value.clone(),
            "--out" => out = value.clone(),
            "--bin" => bin = Some(value.clone()),
            _ => unreachable!("KNOWN_FLAGS and this match must stay in sync"),
        }
    }

    Ok(Parsed { dir, out, bin })
}

/// Builds the app at `--dir`, generates its client, writes it to `--out`.
/// 0 on success, 2 on any build/launch/write fault, 101 if
/// `charpente_tsgen::emit_client` panics on a contract it refuses to
/// represent (Rust's default panic exit code — the panic message names the
/// Rust route or type and the fix, hard rules 4 and 5; there is no `Result`
/// to map to a code of our own choosing here). There is no failure verdict
/// to report, so 1 is not among this verb's codes.
pub fn generate(args: &[String]) -> i32 {
    let parsed = match parse("gen-client", args) {
        Ok(p) => p,
        Err(code) => return code,
    };
    let ts = match client_for("gen-client", &parsed.dir, parsed.bin.as_deref()) {
        Ok(ts) => ts,
        Err(code) => return code,
    };
    // Writing the generated client is edge tooling, outside the
    // deterministic core (DESIGN.md §1) — nothing inside charpente-core
    // touches the filesystem directly.
    #[allow(clippy::disallowed_methods)]
    let written = std::fs::write(&parsed.out, ts);
    match written {
        Ok(()) => {
            eprintln!("charpente gen-client: wrote {}", parsed.out);
            0
        }
        Err(e) => {
            eprintln!(
                "error: charpente gen-client: cannot write {}: {e}",
                parsed.out
            );
            2
        }
    }
}

/// Regenerates the client for the app at `--dir` and compares it against the
/// committed file at `--out`. 0 identical, 1 stale or missing, 2 build or
/// launch fault, 101 if `charpente_tsgen::emit_client` panics on a contract
/// it refuses to represent (same as `generate` — a fresh client is generated
/// here too, before the comparison).
///
/// The 1-vs-2 split is the whole point: 1 means "the committed client does
/// not match this app's contract", 2 means "nothing could be built to
/// compare it against". Collapsing them would tell CI a client is stale when
/// the truth is that the build broke.
pub fn check(args: &[String]) -> i32 {
    let parsed = match parse("check-client", args) {
        Ok(p) => p,
        Err(code) => return code,
    };
    let fresh = match client_for("check-client", &parsed.dir, parsed.bin.as_deref()) {
        Ok(ts) => ts,
        Err(code) => return code,
    };
    // Same boundary as `gen` above.
    #[allow(clippy::disallowed_methods)]
    let committed = std::fs::read_to_string(&parsed.out);
    let committed = match committed {
        Ok(s) => s,
        Err(e) => {
            eprintln!(
                "error: charpente check-client: cannot read {} ({e}) — the generated client is \
                 committed to the repo, so a missing one means it was never generated\n  fix: \
                 charpente gen-client --dir {} --out {}\n  then commit the result.",
                parsed.out, parsed.dir, parsed.out
            );
            return 1;
        }
    };
    if fresh == committed {
        return 0;
    }
    eprintln!(
        "error: charpente check-client: {} is stale — it does not match what this app's \
         contract generates right now\n  fix: charpente gen-client --dir {} --out {}\n  then \
         commit the result.",
        parsed.out, parsed.dir, parsed.out
    );
    for line in first_difference(&committed, &fresh) {
        eprintln!("{line}");
    }
    1
}

/// Build the app, read its contract, emit the client. Every error is already
/// reported when this returns `Err`. `verb` is only in the diagnostics, same
/// reason as `parse`'s.
fn client_for(verb: &str, dir: &str, bin: Option<&str>) -> Result<String, i32> {
    let json = crate::describe::contract_json(verb, dir, bin)?;
    let contract = match contract::parse(&json) {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "error: charpente {verb}: the contract the app at `{dir}` printed is not one \
                 this charpente understands: {e} — the app and this CLI must be built from the \
                 same charpente version"
            );
            return Err(2);
        }
    };
    Ok(charpente_tsgen::emit_client(&contract))
}

/// The first differing line with both sides — enough to see WHAT drifted
/// without printing an entire generated file into a CI log.
///
/// Splits on `\n` via plain `split`, NOT `str::lines()` or
/// `split_terminator`. Both of those normalize away a difference that can be
/// the ONLY difference between two byte-distinct strings, which reopens F1's
/// self-contradiction under a different input each time:
/// - `lines()` strips a trailing `\r`, so CRLF vs LF with the same content
///   compares equal line-by-line (round 1 of this fix).
/// - `split_terminator` still treats "ends with one `\n`" and "does not end
///   with `\n`" as the same split, so `"a\nb"` and `"a\nb\n"` ALSO compare
///   equal line-by-line (round 2 — caught in review of round 1).
///
/// Both then fall through to the length branch, which reports the SAME
/// count for both sides — a self-contradiction, since the inputs are
/// guaranteed different whenever this function is called (every caller
/// checks `a != b` first).
///
/// Plain `split('\n')` closes every door at once rather than one input at a
/// time: `v.join("\n") == original` holds for ANY string's `split('\n')`
/// result, with no normalization. So if `a != b`, their split vectors cannot
/// be equal (equal vectors would join back to equal strings) — the
/// self-contradiction becomes unreachable by construction, not by patching
/// the next input someone thinks of.
fn first_difference(a: &str, b: &str) -> Vec<String> {
    let (av, bv): (Vec<&str>, Vec<&str>) = (a.split('\n').collect(), b.split('\n').collect());
    for (i, (x, y)) in av.iter().zip(bv.iter()).enumerate() {
        if x != y {
            return vec![
                format!("  first difference at line {}:", i + 1),
                format!("    committed: {x}"),
                format!("    generated: {y}"),
            ];
        }
    }
    vec![format!(
        "  same prefix, different length: committed {} lines, generated {} lines",
        av.len(),
        bv.len()
    )]
}

/// JSON -> `charpente::describe::Contract`.
///
/// **Why this module exists.** `Contract` and everything under it derive
/// `Serialize` only, and every name in them is a `&'static str` — they are
/// built from `#[table]`/`#[route]` expansions, where every name is a
/// literal. Nothing in the workspace deserializes a contract, and
/// `&'static str` cannot be deserialized from a runtime `String`. So the
/// app's JSON is parsed into the owned mirror below and converted, leaking
/// the strings to reach `&'static str`.
///
/// **Why the leak is defensible.** `charpente gen-client` is a one-shot
/// process: it parses exactly one contract, emits one file and exits, so the
/// leaked bytes are bounded by that contract's own size and are reclaimed by
/// process exit. That rationale is a property of THIS process, which is why
/// the mirror lives here and not in `charpente-tsgen` — a library that
/// leaked once per call would be a landmine for a long-running caller.
///
/// **Why a mirror rather than `Deserialize` on `Contract` itself.** That is a
/// public-API change to `charpente` (hard rule 6 drags the docs in), it would
/// put the same leak inside a library where the rationale above is false, and
/// it reaches across a crate two sessions are working in. The mirror reaches
/// into `charpente` not at all.
///
/// **What keeps the mirror from drifting.** `into_contract` builds the real
/// types field by field, so a field added to `Contract` is a compile error
/// here, not a silent omission. `round_trip_preserves_every_shape` below
/// pins the serde spelling: a `#[serde(rename)]` or a changed variant shape
/// on the real types makes the round trip differ, which the compiler cannot
/// see on its own.
mod contract {
    use charpente::describe as real;
    use charpente::http::EmittedEvent;
    use charpente::wire as rw;
    use serde::Deserialize;

    pub fn parse(json: &str) -> Result<real::Contract, serde_json::Error> {
        serde_json::from_str::<Contract>(json).map(into_contract)
    }

    /// See the module doc: bounded by one contract in a one-shot process.
    fn leak(s: String) -> &'static str {
        String::leak(s)
    }

    fn leak_all(v: Vec<String>) -> Vec<&'static str> {
        v.into_iter().map(leak).collect()
    }

    #[derive(Deserialize)]
    struct Contract {
        charpente_version: String,
        tables: Vec<TableInfo>,
        routes: Vec<RouteInfo>,
        events: Vec<EventInfo>,
        types: Vec<TypeDef>,
        jobs: Vec<JobInfo>,
        cron: Vec<CronInfo>,
        config: ConfigInfo,
        telemetry: TelemetryInfo,
    }

    #[derive(Deserialize)]
    struct TableInfo {
        name: String,
        version: u32,
        fingerprint: Option<u64>,
        indexes: Vec<String>,
        migrations: Vec<MigrationInfo>,
        scoped: bool,
        builtin: bool,
    }

    #[derive(Deserialize)]
    struct MigrationInfo {
        from: u32,
        to: u32,
    }

    #[derive(Deserialize)]
    struct RouteInfo {
        method: String,
        pattern: String,
        name: String,
        auth: bool,
        cross_tenant: bool,
        stream: bool,
        params: Vec<ParamInfo>,
        request: Option<TypeSchema>,
        response: TypeSchema,
        error: TypeSchema,
        emits: Vec<Emitted>,
    }

    #[derive(Deserialize)]
    struct ParamInfo {
        name: String,
        schema: TypeSchema,
    }

    #[derive(Deserialize)]
    struct Emitted {
        event: String,
        payload: String,
    }

    #[derive(Deserialize)]
    struct EventInfo {
        name: String,
        payload: String,
        keyed: bool,
    }

    #[derive(Deserialize)]
    struct TypeDef {
        name: String,
        schema: TypeSchema,
    }

    #[derive(Deserialize)]
    enum TypeSchema {
        Bool,
        I8,
        I16,
        I32,
        U8,
        U16,
        U32,
        F32,
        F64,
        String,
        Null,
        Int64 { signed: bool },
        Id(String),
        Option(Box<TypeSchema>),
        Seq(Box<TypeSchema>),
        Map(Box<TypeSchema>),
        Struct { fields: Vec<Field> },
        Enum(EnumRepr),
        Ref(String),
    }

    #[derive(Deserialize)]
    struct Field {
        name: String,
        schema: TypeSchema,
        optional: bool,
    }

    #[derive(Deserialize)]
    enum EnumRepr {
        Unit {
            variants: Vec<String>,
        },
        External {
            variants: Vec<(String, Option<TypeSchema>)>,
        },
        Internal {
            tag: String,
            variants: Vec<(String, TypeSchema)>,
        },
        Adjacent {
            tag: String,
            content: String,
            variants: Vec<(String, Option<TypeSchema>)>,
        },
    }

    #[derive(Deserialize)]
    struct JobInfo {
        name: String,
        retry: RetryInfo,
        cross_tenant: bool,
        builtin: bool,
    }

    #[derive(Deserialize)]
    struct RetryInfo {
        max: u32,
        backoff_nanos: u64,
    }

    #[derive(Deserialize)]
    struct CronInfo {
        name: String,
        interval_nanos: u64,
        builtin: bool,
    }

    #[derive(Deserialize)]
    struct ConfigInfo {
        body_cap: usize,
        session_ttl: u64,
        store_path: String,
        retain_segments: usize,
        checkpoint_every: u64,
        event_retention: u64,
        migration_batch_size: u64,
        oidc: bool,
    }

    #[derive(Deserialize)]
    struct TelemetryInfo {}

    fn into_contract(c: Contract) -> real::Contract {
        real::Contract {
            charpente_version: leak(c.charpente_version),
            tables: c.tables.into_iter().map(into_table).collect(),
            routes: c.routes.into_iter().map(into_route).collect(),
            events: c.events.into_iter().map(into_event).collect(),
            types: c.types.into_iter().map(into_type_def).collect(),
            jobs: c.jobs.into_iter().map(into_job).collect(),
            cron: c.cron.into_iter().map(into_cron).collect(),
            config: into_config(c.config),
            telemetry: into_telemetry(c.telemetry),
        }
    }

    /// `TelemetryInfo` is deliberately empty until M2 fills it in
    /// (charpente/src/describe.rs). The mirror still carries the field
    /// rather than skipping it, because serde then REQUIRES the key: a
    /// contract with no `telemetry` at all is refused here instead of
    /// silently generating a client from a document that is not a contract.
    fn into_telemetry(_: TelemetryInfo) -> real::TelemetryInfo {
        real::TelemetryInfo {}
    }

    fn into_table(t: TableInfo) -> real::TableInfo {
        real::TableInfo {
            name: leak(t.name),
            version: t.version,
            fingerprint: t.fingerprint,
            indexes: leak_all(t.indexes),
            migrations: t
                .migrations
                .into_iter()
                .map(|m| real::MigrationInfo {
                    from: m.from,
                    to: m.to,
                })
                .collect(),
            scoped: t.scoped,
            builtin: t.builtin,
        }
    }

    fn into_route(r: RouteInfo) -> real::RouteInfo {
        real::RouteInfo {
            method: leak(r.method),
            pattern: leak(r.pattern),
            name: leak(r.name),
            auth: r.auth,
            cross_tenant: r.cross_tenant,
            stream: r.stream,
            params: r
                .params
                .into_iter()
                .map(|p| real::ParamInfo {
                    name: leak(p.name),
                    schema: into_schema(p.schema),
                })
                .collect(),
            request: r.request.map(into_schema),
            response: into_schema(r.response),
            error: into_schema(r.error),
            emits: r
                .emits
                .into_iter()
                .map(|e| EmittedEvent {
                    event: leak(e.event),
                    payload: leak(e.payload),
                })
                .collect(),
        }
    }

    fn into_event(e: EventInfo) -> real::EventInfo {
        real::EventInfo {
            name: leak(e.name),
            payload: leak(e.payload),
            keyed: e.keyed,
        }
    }

    fn into_type_def(d: TypeDef) -> rw::TypeDef {
        rw::TypeDef {
            name: leak(d.name),
            schema: into_schema(d.schema),
        }
    }

    fn into_job(j: JobInfo) -> real::JobInfo {
        real::JobInfo {
            name: leak(j.name),
            retry: real::RetryInfo {
                max: j.retry.max,
                backoff_nanos: j.retry.backoff_nanos,
            },
            cross_tenant: j.cross_tenant,
            builtin: j.builtin,
        }
    }

    fn into_cron(c: CronInfo) -> real::CronInfo {
        real::CronInfo {
            name: leak(c.name),
            interval_nanos: c.interval_nanos,
            builtin: c.builtin,
        }
    }

    fn into_config(c: ConfigInfo) -> real::ConfigInfo {
        real::ConfigInfo {
            body_cap: c.body_cap,
            session_ttl: c.session_ttl,
            store_path: c.store_path,
            retain_segments: c.retain_segments,
            checkpoint_every: c.checkpoint_every,
            event_retention: c.event_retention,
            migration_batch_size: c.migration_batch_size,
            oidc: c.oidc,
        }
    }

    fn into_schema(s: TypeSchema) -> rw::TypeSchema {
        match s {
            TypeSchema::Bool => rw::TypeSchema::Bool,
            TypeSchema::I8 => rw::TypeSchema::I8,
            TypeSchema::I16 => rw::TypeSchema::I16,
            TypeSchema::I32 => rw::TypeSchema::I32,
            TypeSchema::U8 => rw::TypeSchema::U8,
            TypeSchema::U16 => rw::TypeSchema::U16,
            TypeSchema::U32 => rw::TypeSchema::U32,
            TypeSchema::F32 => rw::TypeSchema::F32,
            TypeSchema::F64 => rw::TypeSchema::F64,
            TypeSchema::String => rw::TypeSchema::String,
            TypeSchema::Null => rw::TypeSchema::Null,
            TypeSchema::Int64 { signed } => rw::TypeSchema::Int64 { signed },
            TypeSchema::Id(t) => rw::TypeSchema::Id(leak(t)),
            TypeSchema::Option(i) => rw::TypeSchema::Option(Box::new(into_schema(*i))),
            TypeSchema::Seq(i) => rw::TypeSchema::Seq(Box::new(into_schema(*i))),
            TypeSchema::Map(i) => rw::TypeSchema::Map(Box::new(into_schema(*i))),
            TypeSchema::Struct { fields } => rw::TypeSchema::Struct {
                fields: fields.into_iter().map(into_field).collect(),
            },
            TypeSchema::Enum(e) => rw::TypeSchema::Enum(into_enum(e)),
            TypeSchema::Ref(n) => rw::TypeSchema::Ref(leak(n)),
        }
    }

    fn into_field(f: Field) -> rw::Field {
        rw::Field {
            name: f.name,
            schema: into_schema(f.schema),
            optional: f.optional,
        }
    }

    fn into_enum(e: EnumRepr) -> rw::EnumRepr {
        match e {
            EnumRepr::Unit { variants } => rw::EnumRepr::Unit { variants },
            EnumRepr::External { variants } => rw::EnumRepr::External {
                variants: variants
                    .into_iter()
                    .map(|(n, s)| (n, s.map(into_schema)))
                    .collect(),
            },
            EnumRepr::Internal { tag, variants } => rw::EnumRepr::Internal {
                tag,
                variants: variants
                    .into_iter()
                    .map(|(n, s)| (n, into_schema(s)))
                    .collect(),
            },
            EnumRepr::Adjacent {
                tag,
                content,
                variants,
            } => rw::EnumRepr::Adjacent {
                tag,
                content,
                variants: variants
                    .into_iter()
                    .map(|(n, s)| (n, s.map(into_schema)))
                    .collect(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every `TypeSchema` variant, every `EnumRepr` variant, an optional
    /// field, a null fingerprint, a bodyless route, a stream route with
    /// `emits`, a scoped table beside an unscoped one — hand-written, so it
    /// is not a copy of whatever the emitter happens to produce today.
    const EVERY_SHAPE: &str = r#"{
      "charpente_version": "0.0.0",
      "tables": [
        { "name": "Widget", "version": 2, "fingerprint": 42, "indexes": ["by_name"],
          "migrations": [{"from":1,"to":2}], "scoped": true, "builtin": false },
        { "name": "Session", "version": 1, "fingerprint": null, "indexes": [],
          "migrations": [], "scoped": false, "builtin": true }
      ],
      "routes": [
        { "method": "POST", "pattern": "/widgets/:id", "name": "create_widget",
          "auth": true, "stream": false, "cross_tenant": false,
          "params": [{ "name": "id", "schema": { "Id": "Widget" } }],
          "request": { "Ref": "CreateWidget" }, "response": { "Ref": "WidgetView" },
          "error": "String", "emits": [] },
        { "method": "GET", "pattern": "/feed", "name": "feed", "auth": false,
          "stream": true, "cross_tenant": true,
          "params": [], "request": null, "response": "Null",
          "error": "String",
          "emits": [{ "event": "widget_added", "payload": "WidgetAdded" }] }
      ],
      "events": [{ "name": "widget_added", "payload": "WidgetAdded", "keyed": false }],
      "types": [
        { "name": "AdjacentEnum", "schema": { "Enum": { "Adjacent": {
            "tag": "t", "content": "c",
            "variants": [["Unit", null], ["Full", { "Ref": "WidgetView" }]] } } } },
        { "name": "CreateWidget", "schema": { "Struct": { "fields": [] } } },
        { "name": "Every", "schema": { "Struct": { "fields": [
            {"name":"b","schema":"Bool","optional":false},
            {"name":"i8","schema":"I8","optional":false},
            {"name":"i16","schema":"I16","optional":false},
            {"name":"i32","schema":"I32","optional":false},
            {"name":"u8","schema":"U8","optional":false},
            {"name":"u16","schema":"U16","optional":false},
            {"name":"u32","schema":"U32","optional":false},
            {"name":"f32","schema":"F32","optional":false},
            {"name":"f64","schema":"F64","optional":false},
            {"name":"s","schema":"String","optional":false},
            {"name":"n","schema":"Null","optional":false},
            {"name":"big","schema":{"Int64":{"signed":true}},"optional":false},
            {"name":"ubig","schema":{"Int64":{"signed":false}},"optional":false},
            {"name":"wid","schema":{"Id":"Widget"},"optional":false},
            {"name":"opt","schema":{"Option":"String"},"optional":true},
            {"name":"seq","schema":{"Seq":{"Ref":"WidgetView"}},"optional":false},
            {"name":"map","schema":{"Map":"U32"},"optional":false}
        ] } } },
        { "name": "ExternalEnum", "schema": { "Enum": { "External": { "variants": [
            ["Bare", null],
            ["Braced", { "Struct": { "fields": [] } }],
            ["Payload", { "Ref": "WidgetView" }]] } } } },
        { "name": "InternalEnum", "schema": { "Enum": { "Internal": { "tag": "type",
            "variants": [["One", { "Struct": { "fields": [
              {"name":"x","schema":"U32","optional":false}] } }]] } } } },
        { "name": "UnitEnum", "schema": { "Enum": { "Unit": { "variants": ["A","B"] } } } },
        { "name": "WidgetAdded", "schema": { "Struct": { "fields": [] } } },
        { "name": "WidgetView", "schema": { "Struct": { "fields": [] } } }
      ],
      "jobs": [{ "name": "SendMail", "retry": { "max": 3, "backoff_nanos": 1000 },
                 "cross_tenant": true, "builtin": false }],
      "cron": [{ "name": "charpente.session_sweep", "interval_nanos": 60000000000,
                 "builtin": true }],
      "config": { "body_cap": 1024, "session_ttl": 3600, "store_path": "/tmp/x",
                  "retain_segments": 4, "checkpoint_every": 100, "event_retention": 1000,
                  "migration_batch_size": 500, "oidc": false },
      "telemetry": {}
    }"#;

    /// The anti-drift oracle for the mirror in `mod contract`.
    ///
    /// The compiler already catches a field ADDED to `Contract` (the
    /// conversion would not build). What it cannot catch is a serde spelling
    /// change — a `#[serde(rename)]`, a variant that stops being a newtype,
    /// a field that starts skipping when empty. This parses a contract, runs
    /// it back out through `charpente::describe::to_json` — the SAME
    /// function the app binary printed it with — and requires the document
    /// to come back unchanged. Any spelling the mirror gets wrong either
    /// fails to parse or fails to round-trip.
    ///
    /// Compared as parsed JSON, not as text: only the document is the
    /// contract here, and `to_string_pretty`'s whitespace is not.
    #[test]
    fn round_trip_preserves_every_shape() {
        let parsed = contract::parse(EVERY_SHAPE).expect("EVERY_SHAPE must parse");
        let out = charpente::describe::to_json(&parsed);
        let before: serde_json::Value = serde_json::from_str(EVERY_SHAPE).unwrap();
        let after: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(
            before, after,
            "the owned mirror in `mod contract` has drifted from \
             charpente::describe's serde shape"
        );
    }

    /// The mirror is not a lenient reader: a contract this CLI cannot
    /// represent must be refused, not silently defaulted, or `gen-client`
    /// would emit a client for a contract the app does not have.
    #[test]
    fn a_contract_missing_a_field_is_refused_rather_than_defaulted() {
        let broken = EVERY_SHAPE.replace(r#""stream": true,"#, "");
        let err = contract::parse(&broken)
            .err()
            .expect("a missing field must not parse");
        assert!(
            err.to_string().contains("stream"),
            "the parse error must name the missing field: {err}"
        );
    }

    #[test]
    fn first_difference_names_the_line_and_both_sides() {
        let out = first_difference("a\nb\nc\n", "a\nB\nc\n");
        assert_eq!(
            out,
            vec![
                "  first difference at line 2:".to_string(),
                "    committed: b".to_string(),
                "    generated: B".to_string(),
            ]
        );
    }

    /// Neither input ends with `\n`, so plain `split('\n')` and the old
    /// `split_terminator('\n')` agree here — this exercises the length
    /// branch on a genuine difference (a real third line was appended), not
    /// on a trailing-newline artifact. `first_difference_is_not_blind_to_a_
    /// trailing_newline_only_drift` below covers the case where a trailing
    /// `\n` IS the only difference, which used to reach this same branch
    /// with a self-contradicting message.
    #[test]
    fn a_common_prefix_of_different_lengths_reports_the_lengths() {
        let out = first_difference("a\nb", "a\nb\nc");
        assert_eq!(
            out,
            vec!["  same prefix, different length: committed 2 lines, generated 3 lines"]
        );
    }

    /// F1 round 1: `str::lines()` splits on `\n` and strips a trailing `\r`,
    /// so a CRLF file and an LF file with the same content used to compare
    /// equal line-by-line, falling through to a length branch that then
    /// printed "committed 2 lines, generated 2 lines" — the same number
    /// twice. This pins that the drift is now a real difference AT the
    /// first line, not an invisible one that surfaces as a self-
    /// contradiction two branches later.
    #[test]
    fn first_difference_is_not_blind_to_a_crlf_vs_lf_drift() {
        let crlf = "a\r\nb\r\n";
        let lf = "a\nb\n";
        assert_eq!(
            first_difference(crlf, lf),
            vec![
                "  first difference at line 1:".to_string(),
                "    committed: a\r".to_string(),
                "    generated: a".to_string(),
            ]
        );
    }

    /// F1 round 2 (review of round 1): `split_terminator('\n')` fixed CRLF
    /// but shares the exact property that caused the original bug —
    /// `"a\nb"` and `"a\nb\n"` both split to `["a", "b"]`, so a
    /// trailing-newline-only drift STILL compared equal line-by-line and
    /// STILL fell through to the length branch, printing "committed 2
    /// lines, generated 2 lines" for inputs that are not the same length in
    /// bytes. Plain `split('\n')` does not normalize this away: `"a\nb\n"`
    /// splits to `["a", "b", ""]` (3 elements), so the length branch now
    /// reports 2 vs 3 — different numbers, no contradiction.
    #[test]
    fn first_difference_is_not_blind_to_a_trailing_newline_only_drift() {
        let without_trailing_newline = "a\nb";
        let with_trailing_newline = "a\nb\n";
        assert_eq!(
            first_difference(without_trailing_newline, with_trailing_newline),
            vec!["  same prefix, different length: committed 2 lines, generated 3 lines"]
        );
    }

    #[test]
    fn parse_defaults_dir_and_out() {
        let p = parse("gen-client", &[]).unwrap();
        assert_eq!(p.dir, ".");
        assert_eq!(p.out, "./client.ts");
        assert_eq!(p.bin, None);
    }

    #[test]
    fn parse_reads_both_flags() {
        let args = ["--dir", "/app", "--out", "web/client.ts"].map(String::from);
        let p = parse("gen-client", &args).unwrap();
        assert_eq!(p.dir, "/app");
        assert_eq!(p.out, "web/client.ts");
    }

    /// F2: the plumbing a two-binary crate needs. `describe.rs`'s ambiguous-
    /// binary error names `--bin NAME` as the fix; before this, `gen-client`
    /// and `check-client` had no such flag, so that named fix did not exist
    /// for either verb's own user (only `charpente describe` had `--bin`).
    #[test]
    fn parse_reads_the_bin_flag() {
        let args = ["--bin", "worker"].map(String::from);
        let p = parse("gen-client", &args).unwrap();
        assert_eq!(p.bin, Some("worker".to_string()));
    }

    #[test]
    fn parse_rejects_an_unknown_flag_as_a_usage_error() {
        let args = ["--frobnicate", "x"].map(String::from);
        assert_eq!(parse("gen-client", &args).err(), Some(2));
    }

    /// 2, not 1: a flag with no value is the operator's mistake, and 1 would
    /// tell CI the committed client is stale.
    #[test]
    fn parse_rejects_a_valueless_flag_as_a_usage_error() {
        let args = ["--out"].map(String::from);
        assert_eq!(parse("check-client", &args).err(), Some(2));
    }
}
