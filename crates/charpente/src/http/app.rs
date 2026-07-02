//! The app kernel: sessions inside the commit point (DESIGN §8), and `App` —
//! the owner of the `Db` that turns each request into one transaction.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::io;

use charpente_core::db::{Db, DbConfig, Schema, Tx, TxError};
use charpente_core::io::{Disk, Entropy};
use charpente_core::table::Id;
use charpente_macros::table;

use crate::http::router::{RouteMatch, Router};
use crate::http::{ApiError, Request, Response, Route, hex};

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
    db: Db,
    entropy: Box<dyn Entropy>,
    config: AppConfig,
    router: Router,
}

impl App {
    /// Opens the Db with the app's schema plus the platform's `Session`
    /// table — registered here so recovery can always replay logins.
    pub fn open(
        disk: Box<dyn Disk>,
        schema: Schema,
        entropy: Box<dyn Entropy>,
        config: AppConfig,
    ) -> io::Result<App> {
        let db = Db::open(disk, config.db.clone(), schema.table::<Session>())?;
        Ok(App {
            db,
            entropy,
            config,
            router: Router::new(),
        })
    }

    pub fn mount(mut self, routes: Vec<Route>) -> App {
        self.router.mount(routes);
        self
    }

    /// Snapshot/checkpoint access for the kernel loop and tests — handlers
    /// never see it.
    pub fn db(&self) -> &Db {
        &self.db
    }

    pub fn config(&self) -> &AppConfig {
        &self.config
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

        let result = self.db.transact(|tx| {
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
