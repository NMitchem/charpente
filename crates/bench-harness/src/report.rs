//! The report is the artifact (spec §5-§6): every honesty-contract field is a
//! field here, not prose elsewhere. JSON is hand-rolled (no deps) and its
//! shape is pinned byte-exact by test, the sim-harness discipline.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

pub struct Report {
    pub hardware: String,
    pub os: String,
    pub filesystem: String,
    pub cores: usize,
    pub sync_semantics: &'static str,
    pub warmup_secs: f64,
    pub duration_secs: f64,
    pub device: Option<DeviceReport>,
    pub core: Option<CoreReport>,
    pub kernel: Option<KernelReport>,
    pub wire: Vec<WireReport>,
    pub invalid: Vec<String>,
}
pub struct DeviceReport {
    pub syncs_per_sec: f64,
    pub appends_per_sec: f64,
}
pub struct CoreReport {
    pub acked_writes_per_sec: f64,
    pub log_records_per_sec: f64,
    pub reads_per_sec: f64,
    pub device_ratio: f64,
}
pub struct KernelReport {
    pub acked_writes_per_sec: f64,
    pub log_records_per_sec: f64,
    pub requests_total: u64,
    pub jobs_done: u64,
}
pub struct WireReport {
    pub target: String,
    pub points: Vec<SweepPoint>,
}
pub struct SweepPoint {
    pub connections: usize,
    pub throughput_per_sec: f64,
    pub log_records_per_sec: f64,
    pub p50_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub client_busy_fraction: f64,
    pub saturated: bool,
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

pub fn render_json(r: &Report) -> Result<String, String> {
    let has_layers = r.core.is_some() || r.kernel.is_some() || !r.wire.is_empty();
    if has_layers && r.device.is_none() {
        return Err(CEILING_REFUSAL.to_string());
    }
    let mut s = String::new();
    s.push_str(&format!(
        "{{\"bench\":{{\"hardware\":\"{}\",\"os\":\"{}\",\"filesystem\":\"{}\",\"cores\":{},\
         \"sync_semantics\":\"{}\",\"warmup_secs\":{:.2},\"duration_secs\":{:.2}",
        esc(&r.hardware),
        esc(&r.os),
        esc(&r.filesystem),
        r.cores,
        esc(r.sync_semantics),
        r.warmup_secs,
        r.duration_secs
    ));
    if let Some(d) = &r.device {
        s.push_str(&format!(
            ",\"device\":{{\"syncs_per_sec\":{:.2},\"appends_per_sec\":{:.2}}}",
            d.syncs_per_sec, d.appends_per_sec
        ));
    }
    if let Some(c) = &r.core {
        s.push_str(&format!(
            ",\"core\":{{\"acked_writes_per_sec\":{:.2},\"log_records_per_sec\":{:.2},\
             \"reads_per_sec\":{:.2},\"device_ratio\":{:.3}}}",
            c.acked_writes_per_sec, c.log_records_per_sec, c.reads_per_sec, c.device_ratio
        ));
    }
    if let Some(k) = &r.kernel {
        s.push_str(&format!(
            ",\"kernel\":{{\"acked_writes_per_sec\":{:.2},\"log_records_per_sec\":{:.2},\
             \"requests_total\":{},\"jobs_done\":{}}}",
            k.acked_writes_per_sec, k.log_records_per_sec, k.requests_total, k.jobs_done
        ));
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
                    "{{\"connections\":{},\"throughput_per_sec\":{:.2},\
                     \"log_records_per_sec\":{:.2},\"p50_us\":{},\
                     \"p99_us\":{},\"p999_us\":{},\"client_busy_fraction\":{:.3},\
                     \"saturated\":{}}}",
                    p.connections,
                    p.throughput_per_sec,
                    p.log_records_per_sec,
                    p.p50_us,
                    p.p99_us,
                    p.p999_us,
                    p.client_busy_fraction,
                    p.saturated
                ));
            }
            s.push_str("]}");
        }
        s.push(']');
    }
    s.push_str(",\"invalid\":[");
    for (i, reason) in r.invalid.iter().enumerate() {
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
    s.push_str(&format!(
        "bench: {} | {} | {} | {} cores | sync: {} | warmup {:.0}s, measure {:.0}s\n",
        r.hardware, r.os, r.filesystem, r.cores, r.sync_semantics, r.warmup_secs, r.duration_secs
    ));
    if let Some(d) = &r.device {
        s.push_str(&format!(
            "  device: {:.0} syncs/s, {:.0} appends/s (the ceiling)\n",
            d.syncs_per_sec, d.appends_per_sec
        ));
    }
    if let Some(c) = &r.core {
        s.push_str(&format!(
            "  core:   {:.0} acked writes/s ({:.2}x device), {:.0} log records/s, {:.0} reads/s\n",
            c.acked_writes_per_sec, c.device_ratio, c.log_records_per_sec, c.reads_per_sec
        ));
    }
    if let Some(k) = &r.kernel {
        s.push_str(&format!(
            "  kernel: {:.0} acked writes/s, {:.0} log records/s ({} requests, {} jobs)\n",
            k.acked_writes_per_sec, k.log_records_per_sec, k.requests_total, k.jobs_done
        ));
    }
    for w in &r.wire {
        s.push_str(&format!("  wire [{}]:\n", w.target));
        for p in &w.points {
            s.push_str(&format!(
                "    {:>4} conns: {:>8.0} req/s  {:>8.0} rec/s  p50 {:>6}us  p99 {:>6}us  \
                 p99.9 {:>6}us  client busy {:.0}%{}\n",
                p.connections,
                p.throughput_per_sec,
                p.log_records_per_sec,
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
    for reason in &r.invalid {
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
                device_ratio: 0.97,
            }),
            kernel: Some(KernelReport {
                acked_writes_per_sec: 1300.0,
                log_records_per_sec: 2600.0,
                requests_total: 50000,
                jobs_done: 50000,
            }),
            wire: vec![WireReport {
                target: "bench-app".to_string(),
                points: vec![SweepPoint {
                    connections: 4,
                    throughput_per_sec: 1200.0,
                    log_records_per_sec: 2400.0,
                    p50_us: 800,
                    p99_us: 2100,
                    p999_us: 5000,
                    client_busy_fraction: 0.12,
                    saturated: false,
                }],
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
             \"sync_semantics\":\"F_FULLFSYNC via sync_all\",\"warmup_secs\":5.00,\
             \"duration_secs\":30.00,\"device\":{\"syncs_per_sec\":1500.25,\
             \"appends_per_sec\":1400.50},\"core\":{\"acked_writes_per_sec\":1450.00,\
             \"log_records_per_sec\":1450.00,\"reads_per_sec\":800000.00,\
             \"device_ratio\":0.970},\"kernel\":{\"acked_writes_per_sec\":1300.00,\
             \"log_records_per_sec\":2600.00,\"requests_total\":50000,\"jobs_done\":50000},\
             \"wire\":[{\"target\":\"bench-app\",\"points\":[{\"connections\":4,\
             \"throughput_per_sec\":1200.00,\"log_records_per_sec\":2400.00,\
             \"p50_us\":800,\"p99_us\":2100,\"p999_us\":5000,\
             \"client_busy_fraction\":0.120,\"saturated\":false}]}],\"invalid\":[]}}"
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
        assert!(
            human.contains("INVALID"),
            "human render must shout invalidity"
        );
    }
}
