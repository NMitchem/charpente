//! Shared by `#[table]` and `#[row_shape]`: the structural fingerprint
//! over a struct's `(field_name, type_name)` sequence, computed at
//! expansion time (spec §1). A deliberate, exact duplicate of
//! `charpente_core::migration::fnv1a_hash` — proc-macros run in the host
//! compiler and cannot call core's compiled code.
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

/// NUL-separated fold so ("ab","c") cannot collide with ("a","bc").
pub(crate) fn compute(field_idents: &[syn::Ident], field_types: &[Type]) -> u64 {
    let mut buf = String::new();
    for (ident, ty) in field_idents.iter().zip(field_types) {
        buf.push_str(&ident.to_string());
        buf.push('\0');
        buf.push_str(&quote::quote!(#ty).to_string());
        buf.push('\0');
    }
    fnv1a(buf.as_bytes())
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
}
