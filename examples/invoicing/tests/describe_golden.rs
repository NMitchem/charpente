//! The invoicing worked example for `charpente describe` (describe T5, spec
//! docs/superpowers/specs/2026-07-23-introspection-edge-harness-design.md
//! §1). This is the SAME golden GUIDE §13 quotes verbatim and
//! crates/charpente-cli/tests/describe.rs's `happy_path_json_...` test
//! cross-checks byte-for-byte against the CLI's ephemeral-disk stdout (T5
//! step 1b) — one literal, two callers, so a route/table/job addition here
//! updates a single pin.
//!
//! App construction mirrors tests/sim.rs's `build` fn exactly (sim
//! `Disk`/`Entropy`, `FaultConfig::none()` since describe is a pure registry
//! walk with no fault-injection concern of its own — the T2 fixture's own
//! rationale).

use charpente::describe::to_json;
use charpente::http::App;
use charpente_sim::{FaultConfig, Sim};

fn fixture_app() -> App {
    let sim = Sim::with_faults(0x2EC5_0001, FaultConfig::none());
    invoicing::build_app(Box::new(sim.disk()), Box::new(sim.entropy()))
        .expect("fixture app must build under FaultConfig::none()")
}

// 13 tables (7 app: AppClock, Customer, IdCounter, Invoice, LineItem,
// Receipt, Reminder; 6 builtin: ApiKey, CronRow, Session, charpente.events,
// charpente.jobs, charpente.schema_versions), 12 routes (build_app's
// `routes_macro![...]` list, in registration order), 2 events (task 6:
// build_app's `events_macro![InvoiceFinalized, ReceiptSent]`, rendered in
// event-NAME order — invoice_finalized, receipt_sent — not registration
// order), 4 jobs (SessionSweep builtin + the app's
// SendReceipt/SendOverdueReminder/AdvanceDay), 2 cron
// (charpente.session_sweep builtin + the app's daily-overdue-check) — every
// count reviewed field-by-field against invoicing's actual surface (T5
// report).
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
      "scoped": false,
      "builtin": true
    },
    {
      "name": "AppClock",
      "version": 1,
      "fingerprint": 1422665991556096363,
      "indexes": [],
      "migrations": [],
      "scoped": false,
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
      "scoped": false,
      "builtin": true
    },
    {
      "name": "Customer",
      "version": 1,
      "fingerprint": 12205929389820576530,
      "indexes": [],
      "migrations": [],
      "scoped": false,
      "builtin": false
    },
    {
      "name": "IdCounter",
      "version": 1,
      "fingerprint": 17814272651750401892,
      "indexes": [],
      "migrations": [],
      "scoped": false,
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
      "scoped": false,
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
      "scoped": false,
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
      "scoped": false,
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
      "scoped": false,
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
      "scoped": false,
      "builtin": true
    },
    {
      "name": "charpente.events",
      "version": 1,
      "fingerprint": null,
      "indexes": [],
      "migrations": [],
      "scoped": false,
      "builtin": true
    },
    {
      "name": "charpente.jobs",
      "version": 1,
      "fingerprint": null,
      "indexes": [],
      "migrations": [],
      "scoped": false,
      "builtin": true
    },
    {
      "name": "charpente.schema_versions",
      "version": 1,
      "fingerprint": null,
      "indexes": [],
      "migrations": [],
      "scoped": false,
      "builtin": true
    }
  ],
  "routes": [
    {
      "method": "POST",
      "pattern": "/login",
      "name": "login",
      "auth": false,
      "cross_tenant": false,
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
      "cross_tenant": false,
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
      "cross_tenant": false,
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
      "cross_tenant": false,
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
      "cross_tenant": false,
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
      "cross_tenant": false,
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
      "cross_tenant": false,
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
      "cross_tenant": false,
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
      "cross_tenant": false,
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
      "cross_tenant": false,
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
      "cross_tenant": false,
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
      "cross_tenant": false,
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
  "events": [
    {
      "name": "invoice_finalized",
      "payload": "InvoiceFinalized",
      "keyed": false
    },
    {
      "name": "receipt_sent",
      "payload": "ReceiptSent",
      "keyed": false
    }
  ],
  "types": [
    {
      "name": "ApiError",
      "schema": {
        "Struct": {
          "fields": [
            {
              "name": "error",
              "schema": "String",
              "optional": false
            }
          ]
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
      "name": "InvoiceFinalized",
      "schema": {
        "Struct": {
          "fields": [
            {
              "name": "invoice",
              "schema": {
                "Id": "Invoice"
              },
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
      "name": "ReceiptSent",
      "schema": {
        "Struct": {
          "fields": [
            {
              "name": "invoice",
              "schema": {
                "Id": "Invoice"
              },
              "optional": false
            },
            {
              "name": "amount_cents",
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
    }
  ],
  "jobs": [
    {
      "name": "SessionSweep",
      "retry": {
        "max": 5,
        "backoff_nanos": 30000000000
      },
      "cross_tenant": false,
      "builtin": true
    },
    {
      "name": "SendReceipt",
      "retry": {
        "max": 5,
        "backoff_nanos": 30000000000
      },
      "cross_tenant": false,
      "builtin": false
    },
    {
      "name": "SendOverdueReminder",
      "retry": {
        "max": 5,
        "backoff_nanos": 30000000000
      },
      "cross_tenant": false,
      "builtin": false
    },
    {
      "name": "AdvanceDay",
      "retry": {
        "max": 5,
        "backoff_nanos": 30000000000
      },
      "cross_tenant": false,
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
fn contract_json_shape_is_pinned() {
    let app = fixture_app();
    let json = to_json(&app.describe());
    // The FULL golden, as a raw string literal, assert_eq! — not substrings.
    // Cross-checked byte-for-byte against the CLI's stdout in
    // crates/charpente-cli/tests/describe.rs's `happy_path_json_...` test.
    assert_eq!(json, GOLDEN);
}

#[test]
fn describe_is_deterministic_across_runs_and_reopens() {
    let a = to_json(&fixture_app().describe());
    let b = to_json(&fixture_app().describe());
    assert_eq!(a, b, "two fresh constructions must render byte-identical");
}
