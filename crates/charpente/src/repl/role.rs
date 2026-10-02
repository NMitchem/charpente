//! The durable role file (spec docs/superpowers/specs/2026-07-08-
//! replication-design.md §2): startup config declares an initial role, but
//! the *effective* role is whatever the "role" file (a flat `Disk` name)
//! durably says. A role flip is written with `write_at` + `set_len` +
//! `sync` + `sync_dir` — every byte, and the file's own directory entry,
//! durable before the call returns — which is what makes the three-state
//! switchover (`Primary` → `HandingOff` → `Follower`) crash-safe: a node
//! restarting mid-flip resolves against whatever it actually wrote, never
//! a config default.
//!
//! The file carries its own CRC (`<role line>\n<crc32 hex>`): a role write
//! whose sync failed can be TORN by a later crash, and a torn role line can
//! still parse — "follower:p:1" torn to "follower:p:\0" is a valid form
//! naming a ghost primary (found by the replication T6 harness, seed
//! 0x51dacce702b667c8; the unparseable-tear sibling is seed
//! 0x23b5e7e7f2132438). Integrity failure is a named hard error, never a
//! parse of whatever bytes survived — and never a silent fall back to
//! config: a corrupt file is indistinguishable from a real role flip, and
//! the durable role must win over config (spec §2), so only the operator
//! can safely decide (delete the file to re-init from config on a node
//! that never finished first boot; restore the node otherwise).

use std::io;

use charpente_core::crc::Crc32;
use charpente_core::io::Disk;

const ROLE_FILE: &str = "role";

const EXPECTED_FORMS: &str = "primary / handing-off:<addr> / follower:<addr> / backup:<addr>";

/// The node's effective role — durable state, not just config (spec §2).
#[derive(Debug, Clone, PartialEq)]
pub enum Role {
    Primary,
    HandingOff { to: String },
    Follower { primary: String },
    Backup { primary: String },
}

fn role_to_line(role: &Role) -> String {
    match role {
        Role::Primary => "primary".to_string(),
        Role::HandingOff { to } => format!("handing-off:{to}"),
        Role::Follower { primary } => format!("follower:{primary}"),
        Role::Backup { primary } => format!("backup:{primary}"),
    }
}

fn parse_role(line: &str) -> io::Result<Role> {
    if line == "primary" {
        return Ok(Role::Primary);
    }
    if let Some(to) = line.strip_prefix("handing-off:") {
        return Ok(Role::HandingOff { to: to.to_string() });
    }
    if let Some(primary) = line.strip_prefix("follower:") {
        return Ok(Role::Follower {
            primary: primary.to_string(),
        });
    }
    if let Some(primary) = line.strip_prefix("backup:") {
        return Ok(Role::Backup {
            primary: primary.to_string(),
        });
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!("role file: \"{line}\" is not a valid role — expected one of: {EXPECTED_FORMS}"),
    ))
}

fn describe(role: &Role) -> String {
    match role {
        Role::Primary => "primary".to_string(),
        Role::HandingOff { to } => format!("handing off to {to}"),
        Role::Follower { primary } => format!("follower of {primary}"),
        Role::Backup { primary } => format!("backup of {primary}"),
    }
}

/// Names both roles and, when the durable role is `Primary` but config
/// says `Follower` (a demote), the re-seed rule (spec §2): a primary
/// demoted by config may have a log that diverged from whatever is
/// primary now while it still thought it was one, so the only safe rejoin
/// is a fresh data dir — re-seed as a follower, never resume in place.
fn mismatch_notice(durable: &Role, config: &Role) -> String {
    let mut notice = format!(
        "role file says {}, but config says {} — the durable role file always wins over \
         config, never the reverse",
        describe(durable),
        describe(config),
    );
    if let (Role::Primary, Role::Follower { primary }) = (durable, config) {
        notice.push_str(&format!(
            "; demoting a primary requires a fresh data dir (re-seed) — a demoted primary's log \
             may have diverged, and re-seeding is the only safe rejoin; to re-seed, {}",
            reseed_steps(primary)
        ));
    }
    notice
}

/// The re-seed steps, in one sentence with the hazard they carry, for every
/// text that prescribes a re-seed (S5 R2-I1). The data dir holds this role
/// file, which outranks config, and the witness set. Delete them and config
/// alone decides the role, so a cleared node started with a primary config
/// comes up as a second writable primary that no witness gate sees. Found
/// for a switchover-demoted former primary, whose role file says follower
/// while its startup config still says primary: wiped and restarted as
/// configured, it acked a write beside the real primary.
pub(crate) fn reseed_steps(primary: &str) -> String {
    format!(
        "stop it, delete its data dir, and restart it configured as a follower of {primary} so \
         it snapshot-seeds from it, never with a primary config: the durable role file that \
         outranks config is deleted with the data dir, so a cleared node started as a primary \
         comes up as a second writable primary"
    )
}

fn line_crc(line: &str) -> u32 {
    let mut crc = Crc32::new();
    crc.update(line.as_bytes());
    crc.finish()
}

/// Wraps a raw disk-IO failure touching the role file with the file's name
/// and a fix (hard rule 5). A corrupt or unparseable role is already named
/// by the callers below before it ever reaches here — this only ever sees a
/// genuine IO failure (permissions, a full disk, a torn mount), which really
/// can clear on a restart, unlike those.
fn role_io_error(action: &str, e: io::Error) -> io::Error {
    io::Error::new(
        e.kind(),
        format!(
            "{action} the role file \"{ROLE_FILE}\": {e} — fix the underlying disk problem and \
             restart"
        ),
    )
}

/// Writes the durable role file — `<role line>\n<crc32 hex>` — with
/// `write_at` + `set_len` + `sync` + `sync_dir`: a role flip must survive
/// any crash before it takes effect (spec §2's three-state switchover
/// safety), and a flip a crash TORE must fail `load_role`'s integrity check
/// by name rather than parse as some other role (the module doc's torn-write
/// finding).
pub fn store_role(disk: &dyn Disk, role: &Role) -> io::Result<()> {
    let line = role_to_line(role);
    let content = format!("{line}\n{:08x}", line_crc(&line));
    let bytes = content.as_bytes();
    let file = disk
        .open(ROLE_FILE)
        .map_err(|e| role_io_error("writing", e))?;
    file.write_at(0, bytes)
        .map_err(|e| role_io_error("writing", e))?;
    file.set_len(bytes.len() as u64)
        .map_err(|e| role_io_error("writing", e))?;
    file.sync().map_err(|e| role_io_error("writing", e))?;
    disk.sync_dir(ROLE_FILE)
        .map_err(|e| role_io_error("writing", e))?;
    Ok(())
}

/// The corrupt-role-file hard error: names the failure and both exits. Not
/// classified as fault fallout — an operator (or a first-boot caller that
/// KNOWS no flip ever happened) must decide, never an automatic fallback.
fn corrupt_role_error(reason: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "role file failed its integrity check ({reason}) — a torn write or corruption; if \
             this node never finished first boot, delete the \"role\" file to re-initialize it \
             from config; otherwise restore this node from a replica or backup (expected \
             contents: one of {EXPECTED_FORMS}, then its crc32 on a second line)"
        ),
    )
}

/// Reads the role file; absent → writes `initial` and returns it (first
/// boot). Present-and-different → the durable role wins; returns `(role,
/// Some(mismatch_notice))` where the notice names both roles and, for the
/// Primary-file/Follower-config case, the demote rule (spec §2).
///
/// The file's name and bytes are made durable before either is trusted
/// (DESIGN §2, "readable is not durable"). A flip whose fsync failed, or
/// that a killed process never synced, is still in the page cache after a
/// restart; booting as that role and then losing it to a power cycle would
/// revert a node that already served as primary. A sync that fails fails the
/// boot, by name.
pub fn load_role(disk: &dyn Disk, initial: &Role) -> io::Result<(Role, Option<String>)> {
    disk.sync_dir(ROLE_FILE)
        .map_err(|e| role_io_error("reading", e))?;
    if !disk
        .list(ROLE_FILE)
        .map_err(|e| role_io_error("reading", e))?
        .iter()
        .any(|name| name == ROLE_FILE)
    {
        store_role(disk, initial)?;
        return Ok((initial.clone(), None));
    }
    let file = disk
        .open(ROLE_FILE)
        .map_err(|e| role_io_error("reading", e))?;
    file.sync().map_err(|e| role_io_error("reading", e))?;
    let len = file.len().map_err(|e| role_io_error("reading", e))?;
    let mut buf = vec![0u8; len as usize];
    file.read_at(0, &mut buf)
        .map_err(|e| role_io_error("reading", e))?;
    let text = String::from_utf8(buf).map_err(|_| corrupt_role_error("not valid UTF-8"))?;
    let (line, stored_crc) = text
        .split_once('\n')
        .ok_or_else(|| corrupt_role_error("missing the crc line"))?;
    if format!("{:08x}", line_crc(line)) != stored_crc.trim_end_matches('\n') {
        return Err(corrupt_role_error("crc mismatch"));
    }
    let durable = parse_role(line)?;
    if durable == *initial {
        return Ok((durable, None));
    }
    let notice = mismatch_notice(&durable, initial);
    Ok((durable, Some(notice)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    use super::*;
    use charpente_core::io::Entropy;
    use charpente_sim::{FaultConfig, Sim};

    fn disk() -> charpente_sim::SimDisk {
        Sim::with_faults(0x5eed_5eed, FaultConfig::none()).disk()
    }

    /// A disk whose store fails its own sync (`file`) or barrier (`dir`):
    /// the arm fires only after a write, so a read ahead of it is untouched.
    fn failing(sim: &Sim, file: bool, dir: bool) -> crate::repl::FailNextSync {
        crate::repl::FailNextSync {
            inner: sim.disk(),
            arm: Arc::new(AtomicBool::new(file)),
            arm_dir: Arc::new(AtomicBool::new(dir)),
            after_write: true,
            wrote: Arc::new(AtomicBool::new(false)),
        }
    }

    /// A disk whose very next `File::sync` fails: a boot's own read.
    fn failing_read(sim: &Sim) -> crate::repl::FailNextSync {
        crate::repl::FailNextSync {
            after_write: false,
            ..failing(sim, true, false)
        }
    }

    /// Readable is not durable, for the role file (D1 review I3; DESIGN §2).
    /// A promote whose role fsync failed returned `Err`, and the error says to
    /// restart. The restarted process reads `Primary` back from the page cache;
    /// acting on that without making it durable lets the next power loss revert
    /// the file to `Follower` under a node that already served as primary, its
    /// log now diverged. `load_role` syncs before it trusts, so the role a node
    /// boots as is the role it boots as after the next power loss too.
    #[test]
    fn a_role_a_restarted_node_boots_as_survives_the_next_power_loss() {
        let sim = Sim::with_faults(0xD1_1301, FaultConfig::none());
        let follower = Role::Follower {
            primary: "a:1".to_string(),
        };
        load_role(&sim.disk(), &follower).unwrap(); // first boot: a durable Follower
        let err = store_role(&failing(&sim, true, false), &Role::Primary).unwrap_err();
        assert!(err.to_string().contains("injected fsync failure"), "{err}");
        sim.restart_process(); // the operator restarts, as the error says to
        let (booted, _) = load_role(&sim.disk(), &follower).unwrap();
        assert_eq!(
            booted,
            Role::Primary,
            "the restarted node reads the failed promote back from the page cache — if not, \
             this test no longer reaches the shape it pins"
        );
        sim.restart(); // power loss
        let (after, _) = load_role(&sim.disk(), &follower).unwrap();
        assert_eq!(
            after, booted,
            "the role a restarted node booted as reverted at the next power loss — load_role \
             trusted role bytes it never made durable"
        );
    }

    /// The name is trusted too: whether the file exists decides first boot. A
    /// first boot whose directory barrier failed leaves a role file only the
    /// page cache knows about; a node restarted under a changed config boots
    /// as that file's role ("the durable role file always wins"), and a power
    /// loss then drops the name, so the next boot is a first boot under the
    /// new config. `load_role` barriers the name before it reads the listing.
    #[test]
    fn a_role_file_name_a_restarted_node_trusts_survives_the_next_power_loss() {
        // A power cycle loses every directory entry that was never barriered.
        let sim = Sim::with_faults(
            0xD1_1302,
            FaultConfig {
                dir_entry_loss: 1.0,
                ..FaultConfig::none()
            },
        );
        let err = load_role(&failing(&sim, false, true), &Role::Primary).unwrap_err();
        assert!(
            err.to_string().contains("injected directory fsync failure"),
            "{err}"
        );
        sim.restart_process();
        let config = Role::Follower {
            primary: "b:1".to_string(),
        };
        let (booted, _) = load_role(&sim.disk(), &config).unwrap();
        assert_eq!(
            booted,
            Role::Primary,
            "the restarted node finds the first boot's role file by a name only the page cache \
             holds — if not, this test no longer reaches the shape it pins"
        );
        sim.restart(); // power loss
        let (after, _) = load_role(&sim.disk(), &config).unwrap();
        assert_eq!(
            after, booted,
            "the role a restarted node booted as reverted at the next power loss — load_role \
             trusted a role file name it never made durable"
        );
    }

    /// A role file whose sync fails at boot is not acted on: the boot fails by
    /// name, and after the next power loss the node still holds the last role
    /// that was durable (fsyncgate — a failed sync guarantees nothing).
    #[test]
    fn a_role_file_whose_sync_fails_at_boot_is_not_trusted() {
        let sim = Sim::with_faults(0xD1_1303, FaultConfig::none());
        let follower = Role::Follower {
            primary: "a:1".to_string(),
        };
        load_role(&sim.disk(), &follower).unwrap();
        store_role(&failing(&sim, true, false), &Role::Primary).unwrap_err();
        sim.restart_process();
        let err = load_role(&failing_read(&sim), &follower)
            .expect_err("a boot acted on a role file whose sync failed");
        let msg = err.to_string();
        assert!(msg.contains("reading the role file \"role\""), "{msg}");
        assert!(msg.contains("injected fsync failure"), "{msg}");
        assert!(msg.contains("restart"), "{msg}");
        sim.restart(); // power loss
        assert_eq!(
            load_role(&sim.disk(), &follower).unwrap().0,
            follower,
            "the last durable role is the one the node holds after the power loss"
        );
    }

    /// A raw disk-IO failure under `Repl::open`'s call to `load_role` must
    /// name the role file and the fix (hard rule 5), not just surface the
    /// bare OS error. Driven through a real `ProdDisk` (sim faults never
    /// produce an `IsADirectory`-shaped error): "role" pre-exists as a
    /// directory, so `load_role`'s first-boot branch finds no plain file by
    /// that name and its `store_role` write fails on the real filesystem.
    #[test]
    // Builds a real directory to provoke a real filesystem error — the one
    // thing a `SimDisk` cannot produce.
    #[allow(clippy::disallowed_methods)]
    fn a_role_file_that_is_a_directory_names_the_file_and_the_fix() {
        let mut suffix = [0u8; 8];
        crate::prod::ProdEntropy::new().unwrap().fill(&mut suffix);
        let root = std::env::temp_dir().join(format!(
            "charpente-role-io-{:016x}",
            u64::from_le_bytes(suffix)
        ));
        std::fs::create_dir_all(root.join(ROLE_FILE)).unwrap();
        let disk = crate::prod::ProdDisk::new(&root).unwrap();

        let err = load_role(&disk, &Role::Primary).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("writing the role file \"role\""), "{msg}");
        assert!(msg.contains("restart"), "{msg}");

        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn every_variant_roundtrips_through_a_sim_disk() {
        let disk = disk();
        for role in [
            Role::Primary,
            Role::HandingOff {
                to: "b:9000".to_string(),
            },
            Role::Follower {
                primary: "a:9000".to_string(),
            },
            Role::Backup {
                primary: "a:9000".to_string(),
            },
        ] {
            store_role(&disk, &role).unwrap();
            let (loaded, notice) = load_role(&disk, &role).unwrap();
            assert_eq!(loaded, role);
            assert!(notice.is_none(), "same role as config must not notice");
        }
    }

    #[test]
    fn absent_file_first_boot_writes_and_returns_initial() {
        let disk = disk();
        let initial = Role::Follower {
            primary: "primary:9000".to_string(),
        };
        let (role, notice) = load_role(&disk, &initial).unwrap();
        assert_eq!(role, initial);
        assert!(notice.is_none());

        // The write from first boot is now durable: a second load (any
        // config) sees it and agrees when the config matches.
        let (again, notice2) = load_role(&disk, &initial).unwrap();
        assert_eq!(again, initial);
        assert!(notice2.is_none());
    }

    #[test]
    fn demoted_primary_mismatch_names_role_file_config_and_reseed() {
        let disk = disk();
        store_role(&disk, &Role::Primary).unwrap();
        let config = Role::Follower {
            primary: "new-primary:9000".to_string(),
        };
        let (role, notice) = load_role(&disk, &config).unwrap();
        assert_eq!(role, Role::Primary, "the durable role wins over config");
        let notice = notice.expect("a demote mismatch must produce a notice");
        // Byte-exact (error-suite T8): reconstructed from `describe`'s own
        // shape for these two scenario-known roles, not by calling
        // `mismatch_notice` again (that would just check the function
        // against itself).
        assert_eq!(
            notice,
            "role file says primary, but config says follower of new-primary:9000 — the \
             durable role file always wins over config, never the reverse; demoting a primary \
             requires a fresh data dir (re-seed) — a demoted primary's log may have diverged, \
             and re-seeding is the only safe rejoin; to re-seed, stop it, delete its data dir, \
             and restart it configured as a follower of new-primary:9000 so it snapshot-seeds \
             from it, never with a primary config: the durable role file that outranks config \
             is deleted with the data dir, so a cleared node started as a primary comes up as a \
             second writable primary"
        );
    }

    #[test]
    fn non_demote_mismatch_names_both_roles_without_the_reseed_clause() {
        let disk = disk();
        store_role(
            &disk,
            &Role::Backup {
                primary: "a:9000".to_string(),
            },
        )
        .unwrap();
        let config = Role::Backup {
            primary: "b:9000".to_string(),
        };
        let (role, notice) = load_role(&disk, &config).unwrap();
        assert_eq!(
            role,
            Role::Backup {
                primary: "a:9000".to_string()
            }
        );
        let notice = notice.expect("a mismatch must produce a notice");
        // Byte-exact (error-suite T8): non-demote mismatches carry no re-seed
        // clause at all — the reconstruction proves that by its absence, not
        // just a negative `contains` check.
        assert_eq!(
            notice,
            "role file says backup of a:9000, but config says backup of b:9000 — the durable \
             role file always wins over config, never the reverse"
        );
    }

    #[test]
    fn garbage_file_errs_naming_the_file_and_expected_forms() {
        let disk = disk();
        let file = disk.open(ROLE_FILE).unwrap();
        file.write_at(0, b"not-a-role").unwrap();
        file.set_len(b"not-a-role".len() as u64).unwrap();
        file.sync().unwrap();
        disk.sync_dir(ROLE_FILE).unwrap();

        let err = load_role(&disk, &Role::Primary).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("role file"), "{msg}");
        assert!(msg.contains(EXPECTED_FORMS), "{msg}");
    }

    /// The torn-write finding (module doc; replication T6 harness, seed
    /// 0x51dacce702b667c8): a torn role line can still PARSE —
    /// "follower:p:1" torn to "follower:p:\0" names a ghost primary — so
    /// integrity must fail by name, never yield a parsed role. Constructed
    /// deterministically: durable bytes emulate exactly what the sim's
    /// crash collapse produced (a prefix of the write, zero-extended to the
    /// set_len length).
    #[test]
    fn a_torn_but_parseable_role_line_fails_integrity_by_name() {
        let disk = disk();
        store_role(
            &disk,
            &Role::Follower {
                primary: "p:1".to_string(),
            },
        )
        .unwrap();
        let file = disk.open(ROLE_FILE).unwrap();
        // Tear the tail: the last byte of the line and the whole crc line
        // survive as zeros.
        let torn = b"follower:p:\0";
        file.write_at(0, torn).unwrap();
        file.set_len(torn.len() as u64).unwrap();
        file.sync().unwrap();

        let err = load_role(
            &disk,
            &Role::Follower {
                primary: "p:1".to_string(),
            },
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("integrity check"), "{msg}");
        assert!(msg.contains("delete the \"role\" file"), "{msg}");
    }

    #[test]
    fn a_crc_mismatch_fails_integrity_by_name() {
        let disk = disk();
        store_role(&disk, &Role::Primary).unwrap();
        let file = disk.open(ROLE_FILE).unwrap();
        // Flip one byte of the role line; the stored crc no longer matches.
        file.write_at(0, b"Primary").unwrap();
        file.sync().unwrap();

        let err = load_role(&disk, &Role::Primary).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("integrity check (crc mismatch)"), "{msg}");
    }
}
