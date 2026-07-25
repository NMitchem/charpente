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
    if matches!(durable, Role::Primary) && matches!(config, Role::Follower { .. }) {
        notice.push_str(
            "; demoting a primary requires a fresh data dir (re-seed) — a demoted \
             primary's log may have diverged, and re-seeding is the only safe rejoin",
        );
    }
    notice
}

fn line_crc(line: &str) -> u32 {
    let mut crc = Crc32::new();
    crc.update(line.as_bytes());
    crc.finish()
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
    let file = disk.open(ROLE_FILE)?;
    file.write_at(0, bytes)?;
    file.set_len(bytes.len() as u64)?;
    file.sync()?;
    disk.sync_dir(ROLE_FILE)?;
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
pub fn load_role(disk: &dyn Disk, initial: &Role) -> io::Result<(Role, Option<String>)> {
    if !disk.list(ROLE_FILE)?.iter().any(|name| name == ROLE_FILE) {
        store_role(disk, initial)?;
        return Ok((initial.clone(), None));
    }
    let file = disk.open(ROLE_FILE)?;
    let len = file.len()?;
    let mut buf = vec![0u8; len as usize];
    file.read_at(0, &mut buf)?;
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
    use super::*;
    use charpente_sim::{FaultConfig, Sim};

    fn disk() -> charpente_sim::SimDisk {
        Sim::with_faults(0x5eed_5eed, FaultConfig::none()).disk()
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
             and re-seeding is the only safe rejoin"
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
