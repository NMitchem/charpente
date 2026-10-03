//! `tenant_node`: the tenancy wire vehicle (M2 tenant-scoping sub-project,
//! task 12) — `node.rs`'s sibling, and for the same reason: `edge-harness`
//! drives REAL built binaries, so a black-box wire oracle for tenant scoping
//! needs a stock, committed repo artifact whose tables are actually scoped.
//! The invoicing app itself is not one (its `#[table]`s predate `scoped_by`
//! and stay byte-identical on purpose), so this is a second, self-contained
//! app in a second bin rather than a mode of the first — exactly the split
//! `node.rs` made from `main.rs`.
//!
//! Two tenants, one store: `Doc` is `#[table(scoped_by = Org)]`, so every
//! read of it goes through `tx.scope::<Org>(org, ctx)?` and every write lands
//! under a 16-byte `(scope, id)` key. Both tenants deliberately use the SAME
//! document ids — separation is the scope's job, never the id's.
//!
//! ```text
//! tenant_node --addr A --data-dir D
//! tenant_node --addr A --data-dir D --repl-listen R --role primary [--secret S]
//! tenant_node --addr A --data-dir D --repl-listen R --role follower --primary P [--secret S]
//! ```
//!
//! `--legacy-unscoped` serves the schema this binary had BEFORE `Doc` was
//! scoped: the same table name, the same fields, no `scoped_by`. It exists
//! for one scenario — a mixed-version replication pair, where the primary
//! writes scoped records (tag `0x05`) and an older follower must refuse them
//! BY NAME rather than misread a 16-byte key as an 8-byte one. It is not a
//! mode of the platform and nothing about the guarantee is optional: it is a
//! second app, and the only thing it can do with a scoped record is refuse
//! it. That pair is reachable at all because the schema digest covers a
//! table's fields and version, not its scopedness — which is precisely why
//! the record-level refusal has to exist.
//!
//! Bad or missing flags print the usage to stderr and exit 2.

use charpente::auth::Principal;
use charpente::http::{ApiError, App, AppConfig, Ctx, Cursor, Subscription};
use charpente::prod::{ProdDisk, ProdEntropy};
use charpente::repl::ReplConfig;
use charpente::repl::role::Role;
use charpente::{
    Id, Schema, Scoped, Subscribe, Tenant, Tx, View, Wire as WireDerive, event, route, routes,
    serve, table,
};
use serde::{Deserialize, Serialize};

const USAGE: &str = "usage: tenant_node --addr A --data-dir D [--legacy-unscoped]\n   \
     or: tenant_node --addr A --data-dir D --repl-listen R --role primary [--secret S]\n   \
     or: tenant_node --addr A --data-dir D --repl-listen R --role follower --primary P \
     [--secret S] [--legacy-unscoped]";

// ------------------------------------------------------------------- model

/// The scope type. A `#[table]` so `Id<Org>` can be a path parameter
/// (`Id<R>`'s `Wire` impl is bounded on `R: Row`); no `Org` row is ever
/// written — a tenant type owes a `NAME` (its identity, unique in the app) and
/// `authorize`.
#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

/// The membership check, written the way an app writes one: the session
/// subject names the org it belongs to (`org-1` is a member of org 1 and of
/// nothing else), so a caller asking for another tenant's rows is refused
/// here, in app code, before any scoped read exists.
impl Tenant for Org {
    const NAME: &'static str = "Org";
    type Who = Principal;
    type Error = ApiError;

    fn authorize(_tx: &Tx, who: &Principal, scope: Id<Org>) -> Result<(), ApiError> {
        let subject = who.subject();
        if subject == format!("org-{}", scope.raw()) {
            Ok(())
        } else {
            Err(ApiError::BadRequest(format!(
                "principal \"{subject}\" is not a member of org {}",
                scope.raw()
            )))
        }
    }
}

#[table(scoped_by = Org)]
struct Doc {
    id: Id<Doc>,
    #[index]
    tag: String,
    text: String,
}

/// The pre-scoping shape of the SAME table: same name on the wire and in the
/// store, same fields, no `scoped_by`. Only `--legacy-unscoped` mounts it.
mod legacy {
    use charpente::{Id, table};

    #[table(global)]
    pub struct Doc {
        pub id: Id<Doc>,
        #[index]
        pub tag: String,
        pub text: String,
    }
}

/// Scoped by `Org`: `create_doc` emits it on its org's `ScopedTx`, and only
/// that org's `feed` receives it (tenancy-completion spec §6). The wire
/// oracle (`edge-harness`'s `tenant_wire`) checks exactly that over TCP.
#[event(scoped_by = Org)]
struct DocCreated {
    doc: Id<Doc>,
    /// The tenant-unique marker the wire oracle keys on. Both tenants use the
    /// same document ids on purpose, so the id alone cannot say which tenant
    /// a frame came from — the text can.
    text: String,
}

// ------------------------------------------------------------------ routes

#[derive(WireDerive, Serialize, Deserialize)]
struct CreateDocBody {
    tag: String,
    text: String,
}

#[derive(WireDerive, Serialize)]
struct DocView {
    id: Id<Doc>,
    tag: String,
    text: String,
}

impl From<Doc> for DocView {
    fn from(d: Doc) -> DocView {
        DocView {
            id: d.id,
            tag: d.tag,
            text: d.text,
        }
    }
}

#[route(POST "/login", public)]
fn login(tx: &mut Tx, subject: String, ctx: &Ctx) -> Result<String, ApiError> {
    Ok(ctx.create_session(tx, subject))
}

#[route(POST "/orgs/:org/docs/:id", auth)]
fn create_doc(
    tx: &mut Tx,
    org: Id<Org>,
    id: Id<Doc>,
    body: CreateDocBody,
    ctx: &Ctx,
) -> Result<DocView, ApiError> {
    let mut tx = tx.scope::<Org>(org, ctx)?;
    let row = Doc {
        id,
        tag: body.tag,
        text: body.text,
    };
    tx.put(&row);
    // `ScopedTx::emit`, so the event is recorded in this tenant, apart from
    // any key (tenancy-completion spec §6).
    tx.emit(DocCreated {
        doc: row.id,
        text: row.text.clone(),
    });
    Ok(row.into())
}

#[route(GET "/orgs/:org/docs", auth)]
fn list_docs(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<Vec<DocView>, ApiError> {
    let tx = tx.scope::<Org>(org, ctx)?;
    Ok(tx.scan::<Doc>()?.into_iter().map(Into::into).collect())
}

/// The index half: a `#[index]` lookup is prefixed by the scope too, so two
/// tenants sharing a tag value do not share an index entry.
#[route(GET "/orgs/:org/docs/tagged/:tag", auth)]
fn list_docs_by_tag(
    tx: &mut Tx,
    org: Id<Org>,
    tag: String,
    ctx: &Ctx,
) -> Result<Vec<DocView>, ApiError> {
    let tx = tx.scope::<Org>(org, ctx)?;
    Ok(Doc::by_tag(&tx, &tag)?
        .into_iter()
        .map(Into::into)
        .collect())
}

/// One tenant's event feed, built from the scope the subscriber just proved
/// membership of: it carries that tenant, and delivers only events recorded
/// in it.
#[route(SSE "/orgs/:org/feed", auth)]
fn feed(
    tx: &mut Tx,
    org: Id<Org>,
    ctx: &Ctx,
    cursor: Option<Cursor>,
) -> Result<Subscription, ApiError> {
    let tx = tx.scope::<Org>(org, ctx)?;
    Ok(tx.subscription::<DocCreated>().cursor(cursor).into())
}

/// `--legacy-unscoped`'s only route: every `Doc` row this node holds, with no
/// tenant anywhere in the question — which is the point. An older follower
/// that MISREAD a scoped record instead of refusing it would answer this with
/// a row; refusing means it stays empty.
#[route(GET "/legacy/docs", public)]
fn legacy_list_docs(tx: &mut Tx) -> Result<Vec<String>, ApiError> {
    Ok(tx
        .scan::<legacy::Doc>()?
        .into_iter()
        .map(|d| d.text)
        .collect())
}

// -------------------------------------------------------------------- args

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RoleKind {
    Primary,
    Follower,
}

#[derive(Debug)]
struct ReplArgs {
    listen: String,
    role: RoleKind,
    primary: Option<String>,
    secret: Option<String>,
}

#[derive(Debug)]
struct Args {
    addr: String,
    data_dir: String,
    legacy: bool,
    repl: Option<ReplArgs>,
}

fn parse(args: &[String]) -> Result<Args, String> {
    let mut addr = None;
    let mut data_dir = None;
    let mut legacy = false;
    let mut repl_listen = None;
    let mut role = None;
    let mut primary = None;
    let mut secret = None;

    let mut iter = args.iter();
    while let Some(flag) = iter.next() {
        if flag == "--legacy-unscoped" {
            legacy = true;
            continue;
        }
        let Some(value) = iter.next() else {
            return Err(format!("error: {flag} requires a value"));
        };
        match flag.as_str() {
            "--addr" => addr = Some(value.clone()),
            "--data-dir" => data_dir = Some(value.clone()),
            "--repl-listen" => repl_listen = Some(value.clone()),
            "--role" => role = Some(value.clone()),
            "--primary" => primary = Some(value.clone()),
            "--secret" => secret = Some(value.clone()),
            other => return Err(format!("error: unknown flag \"{other}\"")),
        }
    }

    let addr = addr.ok_or_else(|| "error: --addr is required".to_string())?;
    let data_dir = data_dir.ok_or_else(|| "error: --data-dir is required".to_string())?;

    let repl = match (repl_listen, role) {
        (None, None) => None,
        (Some(_), None) => return Err("error: --repl-listen requires --role".to_string()),
        (None, Some(_)) => return Err("error: --role requires --repl-listen".to_string()),
        (Some(listen), Some(role_str)) => {
            let role = match role_str.as_str() {
                "primary" => RoleKind::Primary,
                "follower" => RoleKind::Follower,
                other => {
                    return Err(format!(
                        "error: --role must be primary or follower — got '{other}'"
                    ));
                }
            };
            if role == RoleKind::Follower && primary.is_none() {
                return Err("error: --role follower requires --primary".to_string());
            }
            Some(ReplArgs {
                listen,
                role,
                primary,
                secret,
            })
        }
    };

    Ok(Args {
        addr,
        data_dir,
        legacy,
        repl,
    })
}

// -------------------------------------------------------------------- main

fn build(args: &Args) -> std::io::Result<App> {
    let disk = Box::new(ProdDisk::new(&args.data_dir)?);
    let entropy = Box::new(ProdEntropy::new()?);
    if args.legacy {
        return Ok(App::open(
            disk,
            Schema::new().table::<legacy::Doc>(),
            entropy,
            AppConfig::default(),
        )?
        .mount(routes![login, legacy_list_docs]));
    }
    Ok(App::open(
        disk,
        Schema::new().table::<Doc>(),
        entropy,
        AppConfig::default(),
    )?
    .mount(routes![
        login,
        create_doc,
        list_docs,
        list_docs_by_tag,
        feed
    ]))
}

fn main() -> std::io::Result<()> {
    let raw: Vec<String> = std::env::args().skip(1).collect();
    let args = match parse(&raw) {
        Ok(a) => a,
        Err(msg) => {
            eprintln!("{msg}\n\n{USAGE}");
            std::process::exit(2);
        }
    };

    let app = build(&args)?;
    let server = match &args.repl {
        None => serve::serve(&args.addr, app)?,
        Some(repl) => {
            let initial_role = match repl.role {
                RoleKind::Primary => Role::Primary,
                RoleKind::Follower => Role::Follower {
                    primary: repl.primary.clone().expect("checked in parse"),
                },
            };
            let cfg = ReplConfig {
                listen: Some(repl.listen.clone()),
                initial_role,
                peer_secret: repl.secret.clone(),
                strict_witnesses: false,
            };
            serve::serve_replicated(
                &args.addr,
                app,
                Box::new(ProdDisk::new(&args.data_dir)?),
                cfg,
            )?
        }
    };

    // A stopped kernel ends the process with its code (spec §5).
    std::process::exit(server.wait());
}
