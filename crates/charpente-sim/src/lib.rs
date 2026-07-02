//! Simulation implementations of the core IO traits (DESIGN.md §1, PLAN.md M0
//! step 1): one seed drives everything — virtual time, latency, entropy, and
//! the fault matrix (torn writes, failed syncs, crash-restart between any two
//! disk operations, partitions). A failure prints its seed; the seed is the
//! reproduction.
//!
//! The world is single-threaded and every container with order-dependent
//! iteration is a BTree — replaying a seed must be byte-identical, forever.

mod disk;
mod net;
pub mod rng;

use std::cell::{Cell, RefCell};
use std::io;
use std::rc::Rc;
use std::time::Duration;

use charpente_core::io::{Clock, Conn, Disk, Entropy, File, Instant, Listener, Net};

use crate::disk::{DiskWorld, PendingOp};
use crate::net::{ConnState, NetWorld, host_of, normalize};
use crate::rng::{Rng, splitmix64};

/// Fault-injection probabilities and latency ranges. The defaults are the
/// standard matrix; `none()` turns everything off for logic-only tests.
#[derive(Clone, Debug)]
pub struct FaultConfig {
    /// P(a `write_at` returns an injected IO error).
    pub write_error: f64,
    /// P(a `sync` fails, leaving the durability of prior writes unknown).
    pub sync_error: f64,
    /// P(an unsynced op is fully durable after a crash).
    pub crash_apply: f64,
    /// P(an unsynced write survives a crash torn); the rest are lost entirely.
    pub crash_torn: f64,
    pub disk_latency: (Duration, Duration),
    pub net_latency: (Duration, Duration),
}

impl Default for FaultConfig {
    fn default() -> FaultConfig {
        FaultConfig {
            write_error: 0.02,
            sync_error: 0.02,
            crash_apply: 0.4,
            crash_torn: 0.3,
            disk_latency: (Duration::from_micros(20), Duration::from_millis(2)),
            net_latency: (Duration::from_micros(100), Duration::from_millis(20)),
        }
    }
}

impl FaultConfig {
    pub fn none() -> FaultConfig {
        FaultConfig {
            write_error: 0.0,
            sync_error: 0.0,
            crash_apply: 0.0,
            crash_torn: 0.0,
            disk_latency: (Duration::ZERO, Duration::ZERO),
            net_latency: (Duration::ZERO, Duration::ZERO),
        }
    }

    fn validate(&self) {
        for (name, p) in [
            ("write_error", self.write_error),
            ("sync_error", self.sync_error),
            ("crash_apply", self.crash_apply),
            ("crash_torn", self.crash_torn),
        ] {
            assert!(
                (0.0..=1.0).contains(&p),
                "FaultConfig.{name} = {p} — must be in [0, 1]"
            );
        }
        assert!(
            self.crash_apply + self.crash_torn <= 1.0,
            "FaultConfig: crash_apply + crash_torn must not exceed 1 (the rest is the drop probability)"
        );
        assert!(
            self.disk_latency.0 <= self.disk_latency.1,
            "disk_latency range inverted"
        );
        assert!(
            self.net_latency.0 <= self.net_latency.1,
            "net_latency range inverted"
        );
    }
}

struct World {
    seed: u64,
    cfg: FaultConfig,
    /// Virtual time, nanoseconds since universe start.
    now: Cell<u64>,
    /// Independent streams so app-visible entropy draws don't perturb fault
    /// decisions and vice versa.
    faults: RefCell<Rng>,
    entropy: RefCell<Rng>,
    latency: RefCell<Rng>,
    disk: RefCell<DiskWorld>,
    net: RefCell<NetWorld>,
}

impl World {
    fn advance(&self, (lo, hi): (Duration, Duration)) {
        let lo = u64::try_from(lo.as_nanos()).expect("latency exceeds u64 nanos");
        let hi = u64::try_from(hi.as_nanos()).expect("latency exceeds u64 nanos");
        if hi == 0 {
            return;
        }
        let step = self.latency.borrow_mut().between(lo, hi);
        self.advance_by(step);
    }

    fn advance_by(&self, nanos: u64) {
        let now = self
            .now
            .get()
            .checked_add(nanos)
            .expect("virtual clock overflowed u64 nanos");
        self.now.set(now);
    }

    /// Charged by every disk operation: the injected-crash countdown ticks
    /// here, so a crash can land between any two operations.
    fn disk_op(&self) -> io::Result<()> {
        {
            let mut d = self.disk.borrow_mut();
            if d.crashed {
                return Err(crashed_error());
            }
            if let Some(countdown) = d.crash_countdown {
                if countdown == 0 {
                    d.crashed = true;
                    return Err(crashed_error());
                }
                d.crash_countdown = Some(countdown - 1);
            }
        }
        self.advance(self.cfg.disk_latency);
        Ok(())
    }
}

fn crashed_error() -> io::Error {
    io::Error::other("simulated crash: the disk is gone until Sim::restart()")
}

pub struct Sim {
    world: Rc<World>,
}

impl Sim {
    pub fn new(seed: u64) -> Sim {
        Sim::with_faults(seed, FaultConfig::default())
    }

    pub fn with_faults(seed: u64, cfg: FaultConfig) -> Sim {
        cfg.validate();
        let mut sm = seed;
        let faults = Rng::new(splitmix64(&mut sm));
        let entropy = Rng::new(splitmix64(&mut sm));
        let latency = Rng::new(splitmix64(&mut sm));
        Sim {
            world: Rc::new(World {
                seed,
                cfg,
                now: Cell::new(0),
                faults: RefCell::new(faults),
                entropy: RefCell::new(entropy),
                latency: RefCell::new(latency),
                disk: RefCell::new(DiskWorld::default()),
                net: RefCell::new(NetWorld::default()),
            }),
        }
    }

    pub fn seed(&self) -> u64 {
        self.world.seed
    }

    pub fn now(&self) -> Instant {
        Instant::from_nanos(self.world.now.get())
    }

    pub fn clock(&self) -> SimClock {
        SimClock {
            world: self.world.clone(),
        }
    }

    pub fn entropy(&self) -> SimEntropy {
        SimEntropy {
            world: self.world.clone(),
        }
    }

    pub fn disk(&self) -> SimDisk {
        SimDisk {
            world: self.world.clone(),
        }
    }

    /// The network endpoint for a named simulated host.
    pub fn net(&self, host: &str) -> SimNet {
        SimNet {
            world: self.world.clone(),
            host: host.to_string(),
        }
    }

    /// Injects a crash on the `ops`-th subsequent disk operation (0 = the
    /// very next one). The op fails, and the disk is gone until `restart()`.
    pub fn crash_disk_after(&self, ops: u64) {
        self.world.disk.borrow_mut().crash_countdown = Some(ops);
    }

    pub fn crash_disk_now(&self) {
        self.world.disk.borrow_mut().crashed = true;
    }

    /// Power-cycles the disk: every unsynced write independently survives,
    /// tears, or vanishes (see `disk::collapse`), open handles die, and any
    /// pending crash countdown is cleared. Called after a crash — or without
    /// one; a clean power cycle collapses the page cache the same way.
    pub fn restart(&self) {
        let mut d = self.world.disk.borrow_mut();
        let mut rng = self.world.faults.borrow_mut();
        for file in d.files.values_mut() {
            disk::collapse(file, &mut rng, &self.world.cfg);
        }
        d.epoch += 1;
        d.crashed = false;
        d.crash_countdown = None;
    }

    pub fn partition(&self, a: &str, b: &str) {
        self.world
            .net
            .borrow_mut()
            .partitions
            .insert(normalize(a, b));
    }

    pub fn repair(&self, a: &str, b: &str) {
        self.world
            .net
            .borrow_mut()
            .partitions
            .remove(&normalize(a, b));
    }

    /// FNV-1a over all durable file contents — for determinism assertions:
    /// the same seed must produce the same fingerprint, forever.
    pub fn durable_fingerprint(&self) -> u64 {
        let d = self.world.disk.borrow();
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        let mut eat = |bytes: &[u8]| {
            for &b in bytes {
                hash = (hash ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3);
            }
        };
        for (name, file) in &d.files {
            eat(name.as_bytes());
            eat(&(file.durable.len() as u64).to_le_bytes());
            eat(&file.durable);
        }
        hash
    }
}

// ---------------------------------------------------------------- Clock

pub struct SimClock {
    world: Rc<World>,
}

impl Clock for SimClock {
    fn now(&self) -> Instant {
        Instant::from_nanos(self.world.now.get())
    }

    fn sleep(&self, duration: Duration) {
        let nanos = u64::try_from(duration.as_nanos()).expect("sleep exceeds u64 nanos");
        self.world.advance_by(nanos);
    }
}

// ---------------------------------------------------------------- Entropy

pub struct SimEntropy {
    world: Rc<World>,
}

impl Entropy for SimEntropy {
    fn fill(&self, buf: &mut [u8]) {
        self.world.entropy.borrow_mut().fill(buf);
    }
}

// ---------------------------------------------------------------- Disk

pub struct SimDisk {
    world: Rc<World>,
}

impl Disk for SimDisk {
    fn open(&self, path: &str) -> io::Result<Box<dyn File>> {
        self.world.disk_op()?;
        let mut d = self.world.disk.borrow_mut();
        d.files.entry(path.to_string()).or_default();
        Ok(Box::new(SimFile {
            world: self.world.clone(),
            path: path.to_string(),
            epoch: d.epoch,
        }))
    }

    fn remove(&self, path: &str) -> io::Result<()> {
        self.world.disk_op()?;
        let mut d = self.world.disk.borrow_mut();
        if d.files.remove(path).is_none() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("remove(\"{path}\"): no such simulated file"),
            ));
        }
        Ok(())
    }

    fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        self.world.disk_op()?;
        let d = self.world.disk.borrow();
        Ok(d.files
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }
}

pub struct SimFile {
    world: Rc<World>,
    path: String,
    epoch: u64,
}

impl SimFile {
    fn check_epoch(&self) -> io::Result<()> {
        let d = self.world.disk.borrow();
        if d.epoch != self.epoch {
            return Err(io::Error::other(format!(
                "file handle for \"{}\" predates a simulated crash — contract: reopen files after Sim::restart()",
                self.path
            )));
        }
        Ok(())
    }

    fn with_state<T>(&self, f: impl FnOnce(&mut disk::FileState) -> T) -> T {
        let mut d = self.world.disk.borrow_mut();
        let state = d.files.get_mut(&self.path).unwrap_or_else(|| {
            panic!(
                "simulated file \"{}\" removed while a handle was open",
                self.path
            )
        });
        f(state)
    }
}

impl File for SimFile {
    fn len(&self) -> io::Result<u64> {
        self.check_epoch()?;
        self.world.disk_op()?;
        Ok(self.with_state(|s| s.view().len() as u64))
    }

    fn read_at(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.check_epoch()?;
        self.world.disk_op()?;
        self.with_state(|s| {
            let view = s.view();
            let end = offset
                .checked_add(out.len() as u64)
                .expect("read_at range overflowed u64");
            if end > view.len() as u64 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!(
                        "read_at past end of \"{}\": requested [{offset}, {end}) but len is {} — contract: read_at reads exactly out.len() bytes inside the file",
                        self.path,
                        view.len()
                    ),
                ));
            }
            out.copy_from_slice(&view[offset as usize..end as usize]);
            Ok(())
        })
    }

    fn write_at(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.check_epoch()?;
        self.world.disk_op()?;
        if data.is_empty() {
            return Ok(());
        }
        if self
            .world
            .faults
            .borrow_mut()
            .chance(self.world.cfg.write_error)
        {
            return Err(io::Error::other(
                "simulated disk write error (injected fault)",
            ));
        }
        self.with_state(|s| {
            s.pending.push(PendingOp::Write {
                offset,
                data: data.to_vec(),
            });
        });
        Ok(())
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.check_epoch()?;
        self.world.disk_op()?;
        self.with_state(|s| s.pending.push(PendingOp::SetLen { len }));
        Ok(())
    }

    fn sync(&self) -> io::Result<()> {
        self.check_epoch()?;
        self.world.disk_op()?;
        if self
            .world
            .faults
            .borrow_mut()
            .chance(self.world.cfg.sync_error)
        {
            // Real fsync failure semantics: nothing can be assumed. The
            // buffer stays volatile; a later crash collapses it as usual.
            return Err(io::Error::other(
                "simulated fsync failure — durability of writes since the last successful sync is unknown",
            ));
        }
        self.with_state(|s| {
            s.durable = s.view();
            s.pending.clear();
        });
        Ok(())
    }
}

// ---------------------------------------------------------------- Net

pub struct SimNet {
    world: Rc<World>,
    host: String,
}

impl Net for SimNet {
    fn listen(&self, addr: &str) -> io::Result<Box<dyn Listener>> {
        assert_eq!(
            host_of(addr),
            self.host,
            "a simulated host can only listen on its own name"
        );
        let mut n = self.world.net.borrow_mut();
        if n.listeners.contains_key(addr) {
            return Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                format!("\"{addr}\" already has a simulated listener"),
            ));
        }
        n.listeners.insert(addr.to_string(), Default::default());
        Ok(Box::new(SimListener {
            world: self.world.clone(),
            addr: addr.to_string(),
        }))
    }

    fn connect(&self, addr: &str) -> io::Result<Box<dyn Conn>> {
        let target = host_of(addr).to_string();
        let mut n = self.world.net.borrow_mut();
        if n.partitioned(&self.host, &target) {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                format!(
                    "simulated partition between \"{}\" and \"{target}\"",
                    self.host
                ),
            ));
        }
        if !n.listeners.contains_key(addr) {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                format!("nothing is listening on \"{addr}\" in this simulation"),
            ));
        }
        let id = n.next_conn;
        n.next_conn += 1;
        n.conns.insert(
            id,
            ConnState {
                host_a: self.host.clone(),
                host_b: target,
                listen_addr: addr.to_string(),
                to_a: Default::default(),
                to_b: Default::default(),
                open_a: true,
                open_b: true,
            },
        );
        n.listeners
            .get_mut(addr)
            .expect("checked above")
            .push_back(id);
        Ok(Box::new(SimConn {
            world: self.world.clone(),
            id,
            is_a: true,
        }))
    }
}

pub struct SimListener {
    world: Rc<World>,
    addr: String,
}

impl Listener for SimListener {
    fn accept(&self) -> io::Result<Option<Box<dyn Conn>>> {
        let mut n = self.world.net.borrow_mut();
        let backlog = n
            .listeners
            .get_mut(&self.addr)
            .expect("listener state removed while the handle was open");
        match backlog.pop_front() {
            Some(id) => Ok(Some(Box::new(SimConn {
                world: self.world.clone(),
                id,
                is_a: false,
            }))),
            None => Ok(None),
        }
    }

    fn local_addr(&self) -> String {
        self.addr.clone()
    }
}

impl Drop for SimListener {
    fn drop(&mut self) {
        self.world.net.borrow_mut().listeners.remove(&self.addr);
    }
}

pub struct SimConn {
    world: Rc<World>,
    id: u64,
    is_a: bool,
}

impl SimConn {
    fn with_state<T>(&self, f: impl FnOnce(&mut ConnState, &World) -> T) -> T {
        let mut n = self.world.net.borrow_mut();
        let state = n
            .conns
            .get_mut(&self.id)
            .expect("connection state removed while a handle was open");
        f(state, &self.world)
    }
}

impl Conn for SimConn {
    fn send(&self, data: &[u8]) -> io::Result<()> {
        let deliver_at = {
            let (lo, hi) = self.world.cfg.net_latency;
            let lo = u64::try_from(lo.as_nanos()).expect("latency exceeds u64 nanos");
            let hi = u64::try_from(hi.as_nanos()).expect("latency exceeds u64 nanos");
            let latency = if hi == 0 {
                0
            } else {
                self.world.latency.borrow_mut().between(lo, hi)
            };
            self.world.now.get() + latency
        };
        let partitioned = {
            let n = self.world.net.borrow();
            let state = n.conns.get(&self.id).expect("connection state removed");
            n.partitioned(&state.host_a, &state.host_b)
        };
        self.with_state(|state, _| {
            let (my_open, peer_open) = if self.is_a {
                (state.open_a, state.open_b)
            } else {
                (state.open_b, state.open_a)
            };
            assert!(my_open, "send on a connection this side already closed");
            if !peer_open {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "simulated peer closed the connection",
                ));
            }
            if partitioned {
                // Partition semantics: the send succeeds locally and the
                // packet is lost, like the real thing.
                return Ok(());
            }
            let queue = if self.is_a {
                &mut state.to_b
            } else {
                &mut state.to_a
            };
            queue.push_back((deliver_at, data.to_vec()));
            Ok(())
        })
    }

    fn recv(&self, out: &mut [u8]) -> io::Result<Option<usize>> {
        let now = self.world.now.get();
        self.with_state(|state, _| {
            let (queue, peer_open) = if self.is_a {
                (&mut state.to_a, state.open_b)
            } else {
                (&mut state.to_b, state.open_a)
            };
            match queue.front() {
                Some((deliver_at, data)) if *deliver_at <= now => {
                    assert!(
                        out.len() >= data.len(),
                        "recv buffer ({}) smaller than message ({}) — sim Net is message-oriented; size the buffer to the protocol's largest message",
                        out.len(),
                        data.len()
                    );
                    let n = data.len();
                    out[..n].copy_from_slice(data);
                    queue.pop_front();
                    Ok(Some(n))
                }
                Some(_) => Ok(None), // in flight; virtual time hasn't reached it
                None if !peer_open => Ok(Some(0)),
                None => Ok(None),
            }
        })
    }

    fn peer_addr(&self) -> String {
        self.with_state(|state, _| {
            if self.is_a {
                state.listen_addr.clone()
            } else {
                state.host_a.clone()
            }
        })
    }
}

impl Drop for SimConn {
    fn drop(&mut self) {
        let mut n = self.world.net.borrow_mut();
        if let Some(state) = n.conns.get_mut(&self.id) {
            if self.is_a {
                state.open_a = false;
            } else {
                state.open_b = false;
            }
        }
    }
}

// ---------------------------------------------------------------- Harness

/// Runs `universes` seeded universes from a fresh base seed. A failure panics
/// with the seed and a one-command repro (DESIGN.md §7: the failure message
/// IS the reproduction). Set `CHARPENTE_SEED=0x…` to replay one universe.
pub fn run_universes(test_file: &str, universes: u64, f: impl Fn(&mut Sim) -> Result<(), String>) {
    run_universes_from(test_file, ambient_base_seed(), universes, f);
}

/// `run_universes` with a caller-chosen base seed, for tests that must be
/// deterministic in CI (e.g. the harness-catches-the-bug meta-tests).
pub fn run_universes_from(
    test_file: &str,
    base: u64,
    universes: u64,
    f: impl Fn(&mut Sim) -> Result<(), String>,
) {
    if let Ok(seed_var) = std::env::var("CHARPENTE_SEED") {
        let seed = parse_seed(&seed_var);
        eprintln!("charpente-sim [{test_file}]: replaying seed {seed:#018x}");
        run_one(test_file, seed, &f);
        return;
    }
    eprintln!("charpente-sim [{test_file}]: {universes} universes from base seed {base:#018x}");
    let mut state = base;
    for _ in 0..universes {
        run_one(test_file, splitmix64(&mut state), &f);
    }
}

fn run_one(test_file: &str, seed: u64, f: &impl Fn(&mut Sim) -> Result<(), String>) {
    let mut sim = Sim::new(seed);
    if let Err(msg) = f(&mut sim) {
        panic!("{}", failure_report(test_file, seed, &msg));
    }
}

fn failure_report(test_file: &str, seed: u64, msg: &str) -> String {
    format!(
        "\nsim failure in {test_file}: {msg}\n  seed:  {seed:#018x}\n  repro: CHARPENTE_SEED={seed:#x} cargo test -p charpente-sim --test {test_file} -- --nocapture\n"
    )
}

fn parse_seed(raw: &str) -> u64 {
    let hex = raw
        .strip_prefix("0x")
        .or_else(|| raw.strip_prefix("0X"))
        .unwrap_or(raw);
    u64::from_str_radix(hex, 16).unwrap_or_else(|_| {
        panic!("CHARPENTE_SEED must be a hex seed like 0xdeadbeef — got \"{raw}\"")
    })
}

// The single sanctioned wall-clock read in the workspace: universes must be
// fresh each run (the evidence bar is "green over N fresh seeds"), and a
// failure prints its exact seed, so freshness costs no reproducibility.
#[allow(clippy::disallowed_methods, clippy::disallowed_types)]
fn ambient_base_seed() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is set before 1970")
        .as_nanos();
    let mut state = nanos as u64;
    splitmix64(&mut state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn virtual_time_advances_only_by_simulation() {
        let sim = Sim::with_faults(1, FaultConfig::none());
        assert_eq!(sim.now(), Instant::ZERO);
        sim.clock().sleep(Duration::from_secs(3600));
        assert_eq!(sim.now(), Instant::from_nanos(3_600_000_000_000));
    }

    #[test]
    fn disk_ops_advance_virtual_time() {
        let sim = Sim::new(2);
        let disk = sim.disk();
        let before = sim.now();
        disk.open("f").unwrap();
        assert!(sim.now() > before, "a disk op must cost virtual latency");
    }

    #[test]
    fn synced_data_survives_restart_and_handles_die() {
        let sim = Sim::with_faults(3, FaultConfig::none());
        let disk = sim.disk();
        let file = disk.open("f").unwrap();
        file.write_at(0, b"durable").unwrap();
        file.sync().unwrap();
        file.write_at(7, b" volatile").unwrap();

        sim.restart();

        let err = file.len().unwrap_err();
        assert!(
            err.to_string()
                .contains("reopen files after Sim::restart()"),
            "{err}"
        );

        let file = disk.open("f").unwrap();
        // FaultConfig::none() drops every unsynced write at restart.
        assert_eq!(file.len().unwrap(), 7);
        let mut out = [0u8; 7];
        file.read_at(0, &mut out).unwrap();
        assert_eq!(&out, b"durable");
    }

    #[test]
    fn crash_countdown_kills_the_nth_op() {
        let sim = Sim::with_faults(4, FaultConfig::none());
        let disk = sim.disk();
        let file = disk.open("f").unwrap();
        sim.crash_disk_after(2);
        file.write_at(0, b"a").unwrap(); // countdown 2 -> 1
        file.write_at(1, b"b").unwrap(); // countdown 1 -> 0
        let err = file.sync().unwrap_err(); // countdown 0 -> crash
        assert!(err.to_string().contains("simulated crash"), "{err}");
        // Everything after the crash fails too.
        assert!(file.len().is_err());
        assert!(disk.open("g").is_err());
    }

    #[test]
    fn same_seed_same_fingerprint() {
        let run = |seed| {
            let sim = Sim::new(seed);
            let disk = sim.disk();
            let file = disk.open("f").unwrap();
            let mut payload = [0u8; 64];
            sim.entropy().fill(&mut payload);
            let _ = file.write_at(0, &payload);
            let _ = file.sync();
            sim.restart();
            (sim.durable_fingerprint(), sim.now())
        };
        assert_eq!(run(99), run(99));
    }

    #[test]
    fn failure_report_contains_seed_and_repro() {
        let report = failure_report("crash_recovery", 0xABCD, "record 3 corrupt");
        assert!(report.contains("seed:  0x000000000000abcd"), "{report}");
        assert!(
            report.contains(
                "CHARPENTE_SEED=0xabcd cargo test -p charpente-sim --test crash_recovery"
            ),
            "{report}"
        );
    }
}
