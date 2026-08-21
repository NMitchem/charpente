use std::process::Command;

// Process spawning is edge tooling here, outside the deterministic core
// (DESIGN.md §1) — this test drives the built `charpente` binary as a real
// subprocess, exactly as a user would (duplicated from tests/cli.rs:
// integration tests are separate binaries).
#[allow(clippy::disallowed_methods)]
fn charpente() -> Command {
    Command::new(env!("CARGO_BIN_EXE_charpente"))
}

fn invoicing_dir() -> String {
    format!("{}/../../examples/invoicing", env!("CARGO_MANIFEST_DIR"))
}

/// A real lib-only crate (no `[[bin]]`, no `src/main.rs`, no build script) —
/// `cargo build` there produces zero non-null `"executable"` artifacts, the
/// real zero-bins case, no synthetic fixture needed.
fn zero_bin_dir() -> String {
    format!("{}/../charpente-core", env!("CARGO_MANIFEST_DIR"))
}

const USAGE: &str = "usage: charpente describe [--dir PATH] [--format json|human] [--bin NAME]\n\
       --dir PATH   the app crate directory to build and describe (default: .)\n\
       --bin NAME   which binary to describe when the crate has several (default: the sole binary, else the one named like the package)";

// The SAME literal as examples/invoicing/tests/describe_golden.rs's
// `GOLDEN` (describe T5 step 1b) — that file's sim-disk construction and
// this CLI's ephemeral-disk `--dir`/`build`-then-exec path must render
// byte-identical, so `happy_path_json_exits_zero_and_prints_the_contract`
// asserts full equality against this copy rather than a substring. Kept in
// sync by hand: this crate cannot depend on the `invoicing` example crate.
const GOLDEN: &str = r#"{
  "charpente_version": "0.0.0",
  "tables": [
    {
      "name": "ApiKey",
      "version": 1,
      "fingerprint": 11308777448940268015,
      "indexes": [
        "key_hash"
      ],
      "migrations": [],
      "builtin": true
    },
    {
      "name": "AppClock",
      "version": 1,
      "fingerprint": 1422665991556096363,
      "indexes": [],
      "migrations": [],
      "builtin": false
    },
    {
      "name": "CronRow",
      "version": 1,
      "fingerprint": 13750309978683829658,
      "indexes": [
        "name"
      ],
      "migrations": [],
      "builtin": true
    },
    {
      "name": "Customer",
      "version": 1,
      "fingerprint": 12205929389820576530,
      "indexes": [],
      "migrations": [],
      "builtin": false
    },
    {
      "name": "IdCounter",
      "version": 1,
      "fingerprint": 17814272651750401892,
      "indexes": [],
      "migrations": [],
      "builtin": false
    },
    {
      "name": "Invoice",
      "version": 2,
      "fingerprint": 16282652804679416816,
      "indexes": [
        "customer"
      ],
      "migrations": [
        {
          "from": 1,
          "to": 2
        }
      ],
      "builtin": false
    },
    {
      "name": "LineItem",
      "version": 1,
      "fingerprint": 261905951631396626,
      "indexes": [
        "invoice"
      ],
      "migrations": [],
      "builtin": false
    },
    {
      "name": "Receipt",
      "version": 1,
      "fingerprint": 18357282543607550790,
      "indexes": [
        "invoice"
      ],
      "migrations": [],
      "builtin": false
    },
    {
      "name": "Reminder",
      "version": 1,
      "fingerprint": 13477227825039761833,
      "indexes": [
        "invoice"
      ],
      "migrations": [],
      "builtin": false
    },
    {
      "name": "Session",
      "version": 1,
      "fingerprint": 11676348087434866846,
      "indexes": [
        "token_hash"
      ],
      "migrations": [],
      "builtin": true
    },
    {
      "name": "charpente.events",
      "version": 1,
      "fingerprint": null,
      "indexes": [],
      "migrations": [],
      "builtin": true
    },
    {
      "name": "charpente.jobs",
      "version": 1,
      "fingerprint": null,
      "indexes": [],
      "migrations": [],
      "builtin": true
    },
    {
      "name": "charpente.schema_versions",
      "version": 1,
      "fingerprint": null,
      "indexes": [],
      "migrations": [],
      "builtin": true
    }
  ],
  "routes": [
    {
      "method": "POST",
      "pattern": "/login",
      "name": "login",
      "auth": false,
      "stream": false,
      "params": [],
      "request": "String",
      "response": "String",
      "error": {
        "Ref": "ApiError"
      },
      "emits": []
    },
    {
      "method": "POST",
      "pattern": "/customers/:id",
      "name": "create_customer",
      "auth": true,
      "stream": false,
      "params": [
        {
          "name": "id",
          "schema": {
            "Id": "Customer"
          }
        }
      ],
      "request": {
        "Ref": "CreateCustomerBody"
      },
      "response": {
        "Ref": "CustomerView"
      },
      "error": {
        "Ref": "ApiError"
      },
      "emits": []
    },
    {
      "method": "GET",
      "pattern": "/customers/:id",
      "name": "get_customer",
      "auth": true,
      "stream": false,
      "params": [
        {
          "name": "id",
          "schema": {
            "Id": "Customer"
          }
        }
      ],
      "request": null,
      "response": {
        "Ref": "CustomerView"
      },
      "error": {
        "Ref": "ApiError"
      },
      "emits": []
    },
    {
      "method": "GET",
      "pattern": "/customers",
      "name": "list_customers",
      "auth": true,
      "stream": false,
      "params": [],
      "request": null,
      "response": {
        "Seq": {
          "Ref": "CustomerView"
        }
      },
      "error": {
        "Ref": "ApiError"
      },
      "emits": []
    },
    {
      "method": "POST",
      "pattern": "/invoices/:id",
      "name": "create_invoice",
      "auth": true,
      "stream": false,
      "params": [
        {
          "name": "id",
          "schema": {
            "Id": "Invoice"
          }
        }
      ],
      "request": {
        "Ref": "CreateInvoiceBody"
      },
      "response": {
        "Ref": "InvoiceView"
      },
      "error": {
        "Ref": "ApiError"
      },
      "emits": []
    },
    {
      "method": "GET",
      "pattern": "/invoices/:id",
      "name": "get_invoice",
      "auth": true,
      "stream": false,
      "params": [
        {
          "name": "id",
          "schema": {
            "Id": "Invoice"
          }
        }
      ],
      "request": null,
      "response": {
        "Ref": "InvoiceView"
      },
      "error": {
        "Ref": "ApiError"
      },
      "emits": []
    },
    {
      "method": "GET",
      "pattern": "/invoices",
      "name": "list_invoices",
      "auth": true,
      "stream": false,
      "params": [],
      "request": null,
      "response": {
        "Seq": {
          "Ref": "InvoiceView"
        }
      },
      "error": {
        "Ref": "ApiError"
      },
      "emits": []
    },
    {
      "method": "POST",
      "pattern": "/invoices/:id/finalize",
      "name": "finalize_invoice",
      "auth": true,
      "stream": false,
      "params": [
        {
          "name": "id",
          "schema": {
            "Id": "Invoice"
          }
        }
      ],
      "request": null,
      "response": {
        "Ref": "InvoiceView"
      },
      "error": {
        "Ref": "ApiError"
      },
      "emits": []
    },
    {
      "method": "POST",
      "pattern": "/invoices/:id/record-payment",
      "name": "record_payment",
      "auth": true,
      "stream": false,
      "params": [
        {
          "name": "id",
          "schema": {
            "Id": "Invoice"
          }
        }
      ],
      "request": null,
      "response": {
        "Ref": "InvoiceView"
      },
      "error": {
        "Ref": "ApiError"
      },
      "emits": []
    },
    {
      "method": "GET",
      "pattern": "/events/feed",
      "name": "events_feed",
      "auth": true,
      "stream": true,
      "params": [],
      "request": null,
      "response": "Null",
      "error": {
        "Ref": "ApiError"
      },
      "emits": []
    },
    {
      "method": "POST",
      "pattern": "/keys",
      "name": "create_key",
      "auth": true,
      "stream": false,
      "params": [],
      "request": null,
      "response": "String",
      "error": {
        "Ref": "ApiError"
      },
      "emits": []
    },
    {
      "method": "DELETE",
      "pattern": "/keys/:id",
      "name": "revoke_key",
      "auth": true,
      "stream": false,
      "params": [
        {
          "name": "id",
          "schema": {
            "Id": "ApiKey"
          }
        }
      ],
      "request": null,
      "response": "String",
      "error": {
        "Ref": "ApiError"
      },
      "emits": []
    }
  ],
  "types": [
    {
      "name": "ApiError",
      "schema": {
        "Enum": {
          "External": {
            "variants": [
              [
                "BadRequest",
                "String"
              ],
              [
                "Unauthorized",
                null
              ],
              [
                "UnrecognizedCredential",
                null
              ],
              [
                "SessionExpired",
                null
              ],
              [
                "Oidc",
                {
                  "Ref": "OidcReject"
                }
              ],
              [
                "NotFound",
                null
              ],
              [
                "Internal",
                "String"
              ]
            ]
          }
        }
      }
    },
    {
      "name": "CreateCustomerBody",
      "schema": {
        "Struct": {
          "fields": [
            {
              "name": "name",
              "schema": "String",
              "optional": false
            },
            {
              "name": "email",
              "schema": "String",
              "optional": false
            }
          ]
        }
      }
    },
    {
      "name": "CreateInvoiceBody",
      "schema": {
        "Struct": {
          "fields": [
            {
              "name": "customer",
              "schema": {
                "Id": "Customer"
              },
              "optional": false
            },
            {
              "name": "due_day",
              "schema": {
                "Int64": {
                  "signed": false
                }
              },
              "optional": false
            },
            {
              "name": "line_items",
              "schema": {
                "Seq": {
                  "Ref": "LineItemInput"
                }
              },
              "optional": false
            }
          ]
        }
      }
    },
    {
      "name": "CustomerView",
      "schema": {
        "Struct": {
          "fields": [
            {
              "name": "id",
              "schema": {
                "Id": "Customer"
              },
              "optional": false
            },
            {
              "name": "name",
              "schema": "String",
              "optional": false
            },
            {
              "name": "email",
              "schema": "String",
              "optional": false
            }
          ]
        }
      }
    },
    {
      "name": "InvoiceView",
      "schema": {
        "Struct": {
          "fields": [
            {
              "name": "id",
              "schema": {
                "Id": "Invoice"
              },
              "optional": false
            },
            {
              "name": "customer",
              "schema": {
                "Id": "Customer"
              },
              "optional": false
            },
            {
              "name": "state",
              "schema": "String",
              "optional": false
            },
            {
              "name": "due_day",
              "schema": {
                "Int64": {
                  "signed": false
                }
              },
              "optional": false
            },
            {
              "name": "line_items",
              "schema": {
                "Seq": {
                  "Ref": "LineItemView"
                }
              },
              "optional": false
            },
            {
              "name": "total_cents",
              "schema": {
                "Int64": {
                  "signed": true
                }
              },
              "optional": false
            }
          ]
        }
      }
    },
    {
      "name": "LineItemInput",
      "schema": {
        "Struct": {
          "fields": [
            {
              "name": "description",
              "schema": "String",
              "optional": false
            },
            {
              "name": "unit_amount_cents",
              "schema": {
                "Int64": {
                  "signed": true
                }
              },
              "optional": false
            },
            {
              "name": "quantity",
              "schema": {
                "Int64": {
                  "signed": true
                }
              },
              "optional": false
            }
          ]
        }
      }
    },
    {
      "name": "LineItemView",
      "schema": {
        "Struct": {
          "fields": [
            {
              "name": "description",
              "schema": "String",
              "optional": false
            },
            {
              "name": "unit_amount_cents",
              "schema": {
                "Int64": {
                  "signed": true
                }
              },
              "optional": false
            },
            {
              "name": "quantity",
              "schema": {
                "Int64": {
                  "signed": true
                }
              },
              "optional": false
            }
          ]
        }
      }
    },
    {
      "name": "OidcReject",
      "schema": {
        "Enum": {
          "External": {
            "variants": [
              [
                "Malformed",
                "String"
              ],
              [
                "UnsupportedAlg",
                "String"
              ],
              [
                "BadSignature",
                null
              ],
              [
                "Expired",
                null
              ],
              [
                "NotYetValid",
                null
              ],
              [
                "WrongIssuer",
                {
                  "Struct": {
                    "fields": [
                      {
                        "name": "got",
                        "schema": "String",
                        "optional": false
                      },
                      {
                        "name": "configured",
                        "schema": "String",
                        "optional": false
                      }
                    ]
                  }
                }
              ],
              [
                "WrongAudience",
                {
                  "Struct": {
                    "fields": [
                      {
                        "name": "got",
                        "schema": "String",
                        "optional": false
                      },
                      {
                        "name": "configured",
                        "schema": "String",
                        "optional": false
                      }
                    ]
                  }
                }
              ]
            ]
          }
        }
      }
    }
  ],
  "jobs": [
    {
      "name": "SessionSweep",
      "retry": {
        "max": 5,
        "backoff_nanos": 30000000000
      },
      "builtin": true
    },
    {
      "name": "SendReceipt",
      "retry": {
        "max": 5,
        "backoff_nanos": 30000000000
      },
      "builtin": false
    },
    {
      "name": "SendOverdueReminder",
      "retry": {
        "max": 5,
        "backoff_nanos": 30000000000
      },
      "builtin": false
    },
    {
      "name": "AdvanceDay",
      "retry": {
        "max": 5,
        "backoff_nanos": 30000000000
      },
      "builtin": false
    }
  ],
  "cron": [
    {
      "name": "charpente.session_sweep",
      "interval_nanos": 86400000000000,
      "builtin": true
    },
    {
      "name": "daily-overdue-check",
      "interval_nanos": 86400000000000,
      "builtin": false
    }
  ],
  "config": {
    "body_cap": 1048576,
    "session_ttl": 2592000000000000,
    "store_path": "tables.redb",
    "retain_segments": 2,
    "checkpoint_every": 4096,
    "event_retention": 65536,
    "migration_batch_size": 1000,
    "oidc": false
  },
  "telemetry": {}
}
"#;

#[test]
fn bad_format_value_exits_two_and_pins_the_error_text() {
    let out = charpente()
        .args(["describe", "--format", "bogus"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(out.stdout.is_empty(), "stdout: {:?}", out.stdout);
    assert_eq!(
        String::from_utf8(out.stderr).unwrap(),
        format!("error: --format must be json or human — got 'bogus'\n\n{USAGE}\n")
    );
}

#[test]
fn unknown_flag_exits_two_and_pins_the_error_text() {
    let out = charpente()
        .args(["describe", "--frobnicate", "x"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(out.stdout.is_empty(), "stdout: {:?}", out.stdout);
    assert_eq!(
        String::from_utf8(out.stderr).unwrap(),
        format!(
            "error: unknown flag \"--frobnicate\" — valid flags: --dir, --format, --bin\n\n{USAGE}\n"
        )
    );
}

#[test]
fn dir_with_no_cargo_toml_exits_two_with_the_cargo_error_verbatim() {
    // The system temp dir: it exists, and (in any sane environment) neither
    // it nor any of its parents is inside a cargo workspace — cargo's own
    // "could not find `Cargo.toml`" is the oracle here, forwarded verbatim,
    // never wrapped in our own text.
    let dir = std::env::temp_dir();
    let out = charpente()
        .args(["describe", "--dir", dir.to_str().unwrap()])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(out.stdout.is_empty(), "stdout: {:?}", out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("could not find `Cargo.toml`"),
        "expected cargo's own error verbatim:\n{stderr}"
    );
}

#[test]
fn zero_binaries_exits_two_and_pins_the_error_text() {
    let dir = zero_bin_dir();
    let out = charpente()
        .args(["describe", "--dir", &dir])
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(2),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(out.stdout.is_empty(), "stdout: {:?}", out.stdout);
    assert_eq!(
        String::from_utf8(out.stderr).unwrap(),
        format!(
            "error: no binary target in {dir} — charpente describe needs the app crate to \
             build an executable (a `[[bin]]` in Cargo.toml, or src/main.rs) that calls \
             charpente::launch\n"
        )
    );
}

#[test]
fn happy_path_json_exits_zero_and_prints_the_contract() {
    let out = charpente()
        .args(["describe", "--dir", &invoicing_dir(), "--format", "json"])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("\"charpente_version\""), "{stdout}");
    // The FULL golden pin lives in T5's invoicing test — this survives
    // route additions with only that golden updating.
    assert!(stdout.contains("\"pattern\": \"/events/feed\""), "{stdout}");
    // Byte-for-byte cross-check (describe T5 step 1b): the CLI's
    // ephemeral-disk stdout and examples/invoicing/tests/describe_golden.rs's
    // sim-disk `GOLDEN` are the SAME literal — this closes the sim-disk vs
    // ephemeral-disk equivalence claim with an assert instead of an
    // argument. Cross-referenced the other way in that file's `GOLDEN` doc
    // comment. Duplicated by hand, not shared via `include!`: this crate
    // cannot depend on the `invoicing` example crate.
    assert_eq!(stdout, GOLDEN);
}

// Edge T10 interplay: `examples/invoicing` gained a SECOND bin
// (`src/bin/node.rs`, the edge-harness's process-control vehicle) — this
// pins that `describe` (called with no `--bin`) still disambiguates to the
// `invoicing` bin via the package-name default (describe T4's `choose_bin`),
// rather than erroring "several binaries ... pass --bin". The two
// `happy_path_*` tests above already exercise this path implicitly (they
// pass no `--bin` either); this test names the guarantee explicitly now that
// a real second bin exists to be ambiguous with.
#[test]
fn describe_still_resolves_to_the_invoicing_bin_by_package_name_now_that_node_exists() {
    let out = charpente()
        .args(["describe", "--dir", &invoicing_dir(), "--format", "json"])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        out.stderr.is_empty(),
        "describe must not report bin ambiguity now that node.rs exists: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("\"charpente_version\""), "{stdout}");
}

#[test]
fn happy_path_human_exits_zero_and_prints_the_contract() {
    let out = charpente()
        .args(["describe", "--dir", &invoicing_dir()])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.starts_with("tables:\n"), "{stdout}");
}
