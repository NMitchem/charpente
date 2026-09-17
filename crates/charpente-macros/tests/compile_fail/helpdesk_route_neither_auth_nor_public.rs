// The helpdesk oracle, Critical 1 (docs/experiments/2026-09-helpdesk/README.md;
// default-secure spec §5). The route that let anyone join any org, quoted
// from `8913038:examples/helpdesk/src/lib.rs:471-474` — attribute, doc
// comment and signature verbatim; the body reduced to what keeps the
// signature's types in use. It declared neither `auth` nor `public`, and
// until this refusal that was a working route.
//
// The types are real (`Wire` on the body and view) so the ONE error in the
// pin is the refusal: if the refusal were removed, this file would compile.
//
// Positive controls: compile_pass/route_auth_flag.rs and
// compile_pass/route_public_flag.rs.
#![allow(dead_code, unused_imports)]
use charpente::http::ApiError;
use charpente::{Id, Tx, Wire, route, table};
use serde::{Deserialize, Serialize};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

#[table(global)]
struct Agent {
    id: Id<Agent>,
    org: Id<Org>,
    subject: String,
}

#[derive(Wire, Serialize)]
struct AgentView {
    id: Id<Agent>,
    org: Id<Org>,
    subject: String,
}

#[derive(Wire, Serialize, Deserialize)]
struct CreateAgentBody {
    org: Id<Org>,
    subject: String,
}

/// Bootstrap route: registers an agent (user) into an org. Also no `auth`,
/// for the same reason as `create_org` — see FINDINGS.md.
#[route(POST "/agents/:id")]
fn create_agent(tx: &mut Tx, id: Id<Agent>, body: CreateAgentBody) -> Result<AgentView, ApiError> {
    let _ = tx;
    Ok(AgentView {
        id,
        org: body.org,
        subject: body.subject,
    })
}

fn main() {}
