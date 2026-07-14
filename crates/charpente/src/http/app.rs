//! The app kernel: sessions inside the commit point (DESIGN §8), and `App` —
//! the owner of the `Db` that turns each request into one transaction.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::io;
use std::time::Duration;

use charpente_core::db::{Db, DbConfig, Job, Schema, Tx, TxError};
use charpente_core::io::{Disk, Entropy};
use charpente_core::table::Id;
use charpente_macros::table;

use crate::http::router::{RouteMatch, Router};
use crate::http::{ApiError, Request, Response, Route, hex};
use crate::kernel::JobDef;
use crate::kernel::cron::{CronEntry, CronRow};

// ---------------------------------------------------------------- session

/// Minimal auth: a session row inside the commit point. Tokens are stored
/// raw in M0 — M1 ("auth, properly") hashes them and adds expiry.
#[table]
pub struct Session {
    pub id: Id<Session>,
    #[index]
    pub token: String,
    pub subject: String,
}

// ---------------------------------------------------------------- ctx

/// Per-request context handed to handlers: the authenticated session (if
/// any) and one fresh entropy-derived token for session creation.
pub struct Ctx {
    session: Option<Session>,
    fresh_token: String,
    created: Cell<bool>,
}

impl Ctx {
    pub fn session(&self) -> Option<&Session> {
        self.session.as_ref()
    }

    /// Creates a session for `subject` and returns its token (login). One
    /// per request — the fresh token is drawn before the transaction, so a
    /// second call could only reuse it.
    pub fn create_session(&self, tx: &mut Tx, subject: impl Into<String>) -> String {
        assert!(
            !self.created.replace(true),
            "Ctx::create_session called twice in one request — one session per request"
        );
        let id = u64::from_str_radix(&self.fresh_token[..16], 16).expect("token is hex");
        tx.put(&Session {
            id: Id::from_raw(id),
            token: self.fresh_token.clone(),
            subject: subject.into(),
        });
        self.fresh_token.clone()
    }

    /// Deletes the current session (logout). A no-op without one.
    pub fn revoke(&self, tx: &mut Tx) {
        if let Some(session) = &self.session {
            tx.delete::<Session>(session.id);
        }
    }
}

// ---------------------------------------------------------------- app

pub struct AppConfig {
    /// Bodies over the cap get 413 before any decoding — this also fences
    /// `Db::transact`'s MAX_RECORD_BYTES assert from network input.
    pub body_cap: usize,
    pub db: DbConfig,
}

impl Default for AppConfig {
    fn default() -> AppConfig {
        AppConfig {
            body_cap: 1 << 20,
            db: DbConfig::default(),
        }
    }
}

/// The deterministic application kernel. Owns the `Db`; `handle` takes
/// `&mut self`, so requests are serializable by construction. The sim drives
/// `handle` directly; `crate::serve` feeds it from hyper in production.
pub struct App {
    /// `Option` only so the replication reseed swap can `take` the `Db`, hand
    /// it to `Repl::run_seed` (which consumes and returns a rebuilt one), and
    /// `put` it back — the slot is `Some` at every other observable moment
    /// (`db()`/`db_mut()` expect it).
    db: Option<Db>,
    entropy: Box<dyn Entropy>,
    config: AppConfig,
    router: Router,
    job_defs: Vec<JobDef>,
    cron_entries: Vec<CronEntry>,
}

impl App {
    /// Opens the Db with the app's schema plus the platform's `Session` and
    /// `CronRow` tables — registered here so recovery can always replay
    /// logins and schedules.
    pub fn open(
        disk: Box<dyn Disk>,
        schema: Schema,
        entropy: Box<dyn Entropy>,
        config: AppConfig,
    ) -> io::Result<App> {
        let db = Db::open(
            disk,
            config.db.clone(),
            schema.table::<Session>().table::<CronRow>(),
        )?;
        Ok(App {
            db: Some(db),
            entropy,
            config,
            router: Router::new(),
            job_defs: Vec::new(),
            cron_entries: Vec::new(),
        })
    }

    pub fn mount(mut self, routes: Vec<Route>) -> App {
        self.router.mount(routes);
        self
    }

    /// Registers `#[job]` descriptors (mount-time duplicate-name panic — each
    /// job type is mounted once, like routes).
    pub fn mount_jobs(mut self, defs: Vec<JobDef>) -> App {
        for def in defs {
            assert!(
                !self.job_defs.iter().any(|d| d.name == def.name),
                "job \"{}\" registered twice — each #[job] type is mounted once",
                def.name
            );
            self.job_defs.push(def);
        }
        self
    }

    /// Registers a cron schedule (mount-time duplicate-name panic — each
    /// name is scheduled once). `make` builds the job enqueued on each tick.
    pub fn schedule<J: Job + 'static>(
        mut self,
        name: &'static str,
        every: Duration,
        make: impl Fn() -> J + Send + 'static,
    ) -> App {
        assert!(
            !self.cron_entries.iter().any(|e| e.name == name),
            "cron schedule \"{name}\" scheduled twice — schedule each name once"
        );
        self.cron_entries.push(CronEntry {
            name,
            interval: every,
            enqueue: Box::new(move |tx| tx.enqueue(make())),
        });
        self
    }

    /// Snapshot/checkpoint access for the kernel loop and tests — handlers
    /// never see it.
    pub fn db(&self) -> &Db {
        self.db
            .as_ref()
            .expect("db taken for reseed — a kernel bug left it out")
    }

    pub fn config(&self) -> &AppConfig {
        &self.config
    }

    pub(crate) fn db_mut(&mut self) -> &mut Db {
        self.db
            .as_mut()
            .expect("db taken for reseed — a kernel bug left it out")
    }

    /// Lifts the `Db` out for the replication reseed swap (the kernel's
    /// `Ready::Repl` arm): `Repl::run_seed` consumes it and returns a rebuilt
    /// one that `put_db` restores. Between the two calls `db()`/`db_mut()`
    /// would panic — the arm holds them adjacent, so no other code runs.
    pub(crate) fn take_db(&mut self) -> Db {
        self.db
            .take()
            .expect("db taken twice — the reseed swap is not re-entrant")
    }

    pub(crate) fn put_db(&mut self, db: Db) {
        self.db = Some(db);
    }

    pub(crate) fn entropy(&self) -> &dyn Entropy {
        &*self.entropy
    }

    pub(crate) fn take_job_defs(&mut self) -> Vec<JobDef> {
        std::mem::take(&mut self.job_defs)
    }

    /// Test-only equivalent of `take_job_defs` — integration tests cannot see
    /// `pub(crate)`; the kernel itself uses `take_job_defs`.
    #[doc(hidden)]
    pub fn take_job_defs_for_tests(&mut self) -> Vec<JobDef> {
        self.take_job_defs()
    }

    pub(crate) fn take_cron_entries(&mut self) -> Vec<CronEntry> {
        std::mem::take(&mut self.cron_entries)
    }

    /// Test-only equivalent of `take_cron_entries` — integration tests cannot
    /// see `pub(crate)`; the kernel itself uses `take_cron_entries`.
    #[doc(hidden)]
    pub fn take_cron_entries_for_tests(&mut self) -> Vec<CronEntry> {
        self.take_cron_entries()
    }

    /// Test-only equivalent of `db_mut` — integration tests cannot see
    /// `pub(crate)`; the kernel itself uses `db_mut`.
    #[doc(hidden)]
    pub fn db_for_tests(&mut self) -> &mut Db {
        self.db_mut()
    }

    /// Test-only equivalents of `take_db`/`put_db` — integration tests cannot
    /// see `pub(crate)`; the kernel itself uses the unsuffixed pair.
    #[doc(hidden)]
    pub fn take_db_for_tests(&mut self) -> Db {
        self.take_db()
    }

    #[doc(hidden)]
    pub fn put_db_for_tests(&mut self, db: Db) {
        self.put_db(db);
    }

    pub fn handle(&mut self, request: Request) -> Response {
        let (route, captures) = match self.router.find(request.method, &request.path) {
            RouteMatch::Found(route, captures) => (route, captures),
            RouteMatch::PathNotFound => return Response::error(404, "not found"),
            RouteMatch::MethodNotAllowed => {
                return Response::error(405, "method not allowed for this path");
            }
        };
        if request.body.len() > self.config.body_cap {
            return Response::error(
                413,
                &format!(
                    "request body is {} bytes but the cap is {} — raise AppConfig.body_cap \
                     or send less",
                    request.body.len(),
                    self.config.body_cap
                ),
            );
        }
        let mut fresh = [0u8; 32];
        self.entropy.fill(&mut fresh);
        let fresh_token = hex(&fresh);
        let token = bearer_or_cookie(&request.headers);
        let (auth_required, glue) = (route.auth, route.glue);

        let result = self.db_mut().transact(|tx| {
            let session = match &token {
                Some(token) => Session::by_token(tx, token)?.into_iter().next(),
                None => None,
            };
            if auth_required && session.is_none() {
                return Err(ApiError::Unauthorized);
            }
            let ctx = Ctx {
                session,
                fresh_token,
                created: Cell::new(false),
            };
            glue(tx, &ctx, &captures, &request.body)
        });
        match result {
            Ok(response) => response,
            Err(TxError::App(e)) => e.response(),
            Err(TxError::Io(e)) => Response::error(
                500,
                &format!(
                    "internal storage failure — the request may or may not have \
                     committed; retry after the server recovers: {e}"
                ),
            ),
            Err(TxError::NotPrimary { primary }) => Response::error(
                421,
                &format!(
                    "this node is a read-only follower — writes go to the primary at \
                     {primary}; retry there (the request was not committed)"
                ),
            ),
        }
    }
}

/// `Authorization: Bearer <t>` wins; else the `charpente_session` cookie.
fn bearer_or_cookie(headers: &BTreeMap<String, String>) -> Option<String> {
    if let Some(auth) = headers.get("authorization")
        && let Some(token) = auth.strip_prefix("Bearer ")
    {
        return Some(token.trim().to_string());
    }
    for pair in headers.get("cookie")?.split(';') {
        if let Some((name, value)) = pair.trim().split_once('=')
            && name == "charpente_session"
        {
            return Some(value.to_string());
        }
    }
    None
}
