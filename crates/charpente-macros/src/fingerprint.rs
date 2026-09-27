//! Shared by `#[table]`, `#[row_shape]`, `#[event]` and `#[job]`: the
//! structural fingerprint over a struct's `(field_name, type tokens)`
//! sequence, computed at expansion time (spec §1). A deliberate, exact
//! duplicate of `charpente_core::migration::fnv1a_hash` — proc-macros run in
//! the host compiler and cannot call core's compiled code.
use proc_macro2::{Delimiter, TokenStream, TokenTree};
use quote::ToTokens;
use syn::Type;

fn fnv1a(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    let mut hash = OFFSET;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// NUL-separated fold so ("ab","c") cannot collide with ("a","bc"). Each type
/// is rendered by `render`, not by `TokenStream`'s `Display`: that printer's
/// spacing is the compiler's and is documented as subject to change, and
/// these hashes are persisted.
pub(crate) fn compute(field_idents: &[syn::Ident], field_types: &[Type]) -> u64 {
    let mut buf = String::new();
    for (ident, ty) in field_idents.iter().zip(field_types) {
        buf.push_str(&ident.to_string());
        buf.push('\0');
        let mut words = Vec::new();
        render(ty.to_token_stream(), &mut words);
        buf.push_str(&words.join(" "));
        buf.push('\0');
    }
    fnv1a(buf.as_bytes())
}

/// Every token as its own word: an identifier, one punctuation character, a
/// literal's source text, or a group's delimiters around its contents. An
/// invisible (`Delimiter::None`) group contributes only its contents.
fn render(tokens: TokenStream, words: &mut Vec<String>) {
    for tree in tokens {
        match tree {
            TokenTree::Group(g) => {
                let (open, close) = match g.delimiter() {
                    Delimiter::Parenthesis => ("(", ")"),
                    Delimiter::Brace => ("{", "}"),
                    Delimiter::Bracket => ("[", "]"),
                    Delimiter::None => ("", ""),
                };
                if !open.is_empty() {
                    words.push(open.to_string());
                }
                render(g.stream(), words);
                if !close.is_empty() {
                    words.push(close.to_string());
                }
            }
            TokenTree::Ident(i) => words.push(i.to_string()),
            TokenTree::Punct(p) => words.push(p.as_char().to_string()),
            TokenTree::Literal(l) => words.push(l.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    #[test]
    fn compute_is_deterministic_and_order_sensitive() {
        let idents: Vec<syn::Ident> = vec![parse_quote!(a), parse_quote!(b)];
        let types: Vec<Type> = vec![parse_quote!(u32), parse_quote!(String)];
        let fp = compute(&idents, &types);
        assert_eq!(fp, compute(&idents, &types));
        let ridents: Vec<syn::Ident> = vec![parse_quote!(b), parse_quote!(a)];
        let rtypes: Vec<Type> = vec![parse_quote!(String), parse_quote!(u32)];
        assert_ne!(fp, compute(&ridents, &rtypes));
    }

    #[test]
    fn compute_distinguishes_type_changes() {
        let idents: Vec<syn::Ident> = vec![parse_quote!(a)];
        assert_ne!(
            compute(&idents, &[parse_quote!(u32)]),
            compute(&idents, &[parse_quote!(u64)])
        );
    }

    #[test]
    fn render_flattens_invisible_groups_and_keeps_braces() {
        use proc_macro2::{Group, Ident, Punct, Spacing, Span};

        // `Vec < (None-group: u8) >` — the invisible group around `u8` must
        // contribute only its contents, not a pair of delimiter words.
        let none_group = TokenTree::Group(Group::new(
            Delimiter::None,
            TokenStream::from(TokenTree::Ident(Ident::new("u8", Span::call_site()))),
        ));
        let vec_stream: TokenStream = vec![
            TokenTree::Ident(Ident::new("Vec", Span::call_site())),
            TokenTree::Punct(Punct::new('<', Spacing::Alone)),
            none_group,
            TokenTree::Punct(Punct::new('>', Spacing::Alone)),
        ]
        .into_iter()
        .collect();
        let mut words = Vec::new();
        render(vec_stream, &mut words);
        assert_eq!(words, vec!["Vec", "<", "u8", ">"]);

        // A brace group (unlike `None`) renders its own delimiter words.
        let brace_group = TokenTree::Group(Group::new(Delimiter::Brace, TokenStream::new()));
        let mut brace_words = Vec::new();
        render(TokenStream::from(brace_group), &mut brace_words);
        assert_eq!(brace_words, vec!["{", "}"]);
    }

    #[test]
    fn compute_hashes_the_rendered_tokens_not_a_printer() {
        let idents: Vec<syn::Ident> = vec![parse_quote!(a), parse_quote!(b)];
        let types: Vec<Type> = vec![
            parse_quote!(std::collections::BTreeMap<String, Vec<(u8, [u16; 2])>>),
            parse_quote!(&'static str),
        ];
        let expected = "a\0std : : collections : : BTreeMap < String , Vec < ( u8 , [ u16 ; 2 ] ) > >\0\
                        b\0& ' static str\0";
        assert_eq!(compute(&idents, &types), fnv1a(expected.as_bytes()));
    }
}
