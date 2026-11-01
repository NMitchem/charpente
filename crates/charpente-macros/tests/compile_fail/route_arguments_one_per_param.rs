// The HTTP arity refusal: one argument per :param, plus at most one body.
// This handler takes `id` and leaves out `rev`, so the path's two :params
// meet one argument. The text names both counts and the rule. Everything the
// refused handler names is named by path: a refused route emits nothing, so
// an import would come back as an unused-import warning in the pin below.
use charpente::{route, table};

#[table(global)]
struct Note {
    id: charpente::Id<Note>,
}

#[route(GET "/notes/:id/:rev", public)]
fn note_revision(
    tx: &mut charpente::Tx,
    id: charpente::Id<Note>,
) -> Result<(), charpente::http::ApiError> {
    let _ = (tx, id);
    Ok(())
}

fn main() {}
