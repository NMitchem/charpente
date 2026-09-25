//! `#[job]` / `#[job([cross_tenant, ]retry(max = N, backoff = "30s"))]`
//! (DESIGN.md §6, spec
//! 2026-07-02-queue-cron-design.md): turns a struct into a typed job
//! payload — a field-wise `Codec` impl (mirrors `#[table]`'s row codec, minus
//! the id: a job payload has no primary key), `impl Job` carrying the name,
//! the payload's structural fingerprint (`fingerprint::compute`) and the
//! retry policy, and the `JobEntry` glue `jobs![]` collects. Errors name
//! the accepted forms (hard rule 5).

use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned;
use syn::{Data, DeriveInput, Error, Fields, Ident, LitInt, LitStr, Token, parenthesized};

const ACCEPTED_FORMS: &str = "write `#[job]`, `#[job(cross_tenant)]` or \
     `#[job([cross_tenant, ]retry(max = <int>, backoff = \"<n><unit>\"))]` \
     (unit is one of ms, s, m, h) — e.g. `#[job(retry(max = 3, backoff = \"10s\"))]`";

// ---------------------------------------------------------------- args

pub(crate) struct JobArgs {
    pub max: u32,
    pub backoff_millis: u64,
    /// `cross_tenant` — the same declared capability a route carries
    /// (tenant-scoping spec §3), for the background rollups that need it.
    /// A declared job's `run` is called with the token as a second argument.
    pub cross_tenant: bool,
}

impl Default for JobArgs {
    fn default() -> JobArgs {
        JobArgs {
            max: 5,
            backoff_millis: 30_000,
            cross_tenant: false,
        }
    }
}

enum RetryField {
    Max(LitInt),
    Backoff(LitStr),
}

impl Parse for RetryField {
    fn parse(input: ParseStream) -> syn::Result<RetryField> {
        let ident: Ident = input
            .parse()
            .map_err(|e| Error::new(e.span(), ACCEPTED_FORMS))?;
        input
            .parse::<Token![=]>()
            .map_err(|e| Error::new(e.span(), ACCEPTED_FORMS))?;
        match ident.to_string().as_str() {
            "max" => Ok(RetryField::Max(input.parse()?)),
            "backoff" => Ok(RetryField::Backoff(input.parse()?)),
            other => Err(Error::new(
                ident.span(),
                format!("unknown retry field `{other}` — {ACCEPTED_FORMS}"),
            )),
        }
    }
}

impl Parse for JobArgs {
    fn parse(input: ParseStream) -> syn::Result<JobArgs> {
        if input.is_empty() {
            return Ok(JobArgs::default());
        }
        let mut cross_tenant = false;
        let mut kw: Ident = input
            .parse()
            .map_err(|e| Error::new(e.span(), ACCEPTED_FORMS))?;
        if kw == "cross_tenant" {
            cross_tenant = true;
            if input.is_empty() {
                let defaults = JobArgs::default();
                return Ok(JobArgs {
                    cross_tenant,
                    ..defaults
                });
            }
            input
                .parse::<Token![,]>()
                .map_err(|e| Error::new(e.span(), ACCEPTED_FORMS))?;
            kw = input
                .parse()
                .map_err(|e| Error::new(e.span(), ACCEPTED_FORMS))?;
            if kw == "cross_tenant" {
                return Err(Error::new(
                    kw.span(),
                    "`cross_tenant` given twice — write it once",
                ));
            }
        }
        if kw != "retry" {
            return Err(Error::new(kw.span(), ACCEPTED_FORMS));
        }
        let content;
        parenthesized!(content in input);
        let fields = content.parse_terminated(RetryField::parse, Token![,])?;
        if !input.is_empty() {
            return Err(Error::new(input.span(), ACCEPTED_FORMS));
        }

        let mut max = None;
        let mut backoff_millis = None;
        for field in fields {
            match field {
                RetryField::Max(lit) => {
                    if max.is_some() {
                        return Err(Error::new(lit.span(), "`max` given twice — write it once"));
                    }
                    max = Some(lit.base10_parse::<u32>()?);
                }
                RetryField::Backoff(lit) => {
                    if backoff_millis.is_some() {
                        return Err(Error::new(
                            lit.span(),
                            "`backoff` given twice — write it once",
                        ));
                    }
                    backoff_millis = Some(parse_backoff(&lit)?);
                }
            }
        }
        let defaults = JobArgs::default();
        Ok(JobArgs {
            max: max.unwrap_or(defaults.max),
            backoff_millis: backoff_millis.unwrap_or(defaults.backoff_millis),
            cross_tenant,
        })
    }
}

/// `"10s"` -> 10_000, `"500ms"` -> 500, `"5m"` -> 300_000, `"2h"` -> 7_200_000.
/// Checked in order `ms, s, m, h` — `ms` first, since it also ends in `s`.
fn parse_backoff(lit: &LitStr) -> syn::Result<u64> {
    let raw = lit.value();
    let (digits, millis_per_unit) = if let Some(d) = raw.strip_suffix("ms") {
        (d, 1u64)
    } else if let Some(d) = raw.strip_suffix('s') {
        (d, 1_000u64)
    } else if let Some(d) = raw.strip_suffix('m') {
        (d, 60_000u64)
    } else if let Some(d) = raw.strip_suffix('h') {
        (d, 3_600_000u64)
    } else {
        return Err(Error::new(
            lit.span(),
            format!("backoff \"{raw}\" has no unit — {ACCEPTED_FORMS}"),
        ));
    };
    let count: u64 = digits.parse().map_err(|_| {
        Error::new(
            lit.span(),
            format!("backoff \"{raw}\": \"{digits}\" is not a whole number — {ACCEPTED_FORMS}"),
        )
    })?;
    count.checked_mul(millis_per_unit).ok_or_else(|| {
        Error::new(
            lit.span(),
            format!("backoff \"{raw}\" overflows u64 milliseconds — {ACCEPTED_FORMS}"),
        )
    })
}

// ---------------------------------------------------------------- expand

pub(crate) fn expand(args: JobArgs, input: DeriveInput) -> syn::Result<TokenStream> {
    let name = &input.ident;
    let name_str = name.to_string();

    if !input.generics.params.is_empty() {
        return Err(Error::new(
            input.generics.span(),
            format!(
                "#[job] structs cannot be generic — a job is one concrete payload shape; make `{name_str}` non-generic"
            ),
        ));
    }
    let Data::Struct(data) = &input.data else {
        return Err(Error::new(
            input.span(),
            "#[job] applies to structs — enums and unions cannot be job payloads",
        ));
    };
    let Fields::Named(fields) = &data.fields else {
        return Err(Error::new(
            input.span(),
            format!("#[job] needs named fields — `{name_str}` must be a braced struct"),
        ));
    };

    let field_idents: Vec<_> = fields
        .named
        .iter()
        .map(|f| f.ident.clone().expect("named fields have idents"))
        .collect();
    let field_types: Vec<_> = fields.named.iter().map(|f| f.ty.clone()).collect();
    let fingerprint = crate::fingerprint::compute(&field_idents, &field_types);

    let max = args.max;
    let backoff_millis = args.backoff_millis;
    let cross_tenant = args.cross_tenant;
    // A declared job's `run` takes the token; an undeclared one's does not,
    // so `#[job]` alone still calls `run(tx)` exactly as before. `expect` is
    // an invariant: the dispatcher passes the token from the SAME
    // `JobDef.cross_tenant` this expansion sets.
    let run_call = if cross_tenant {
        quote! {
            job.run(tx, cap.expect(
                "job declares cross_tenant but the dispatcher passed no CrossTenant — \
                 the flag on JobDef and the token come from the same expansion"
            ))
        }
    } else {
        quote! { job.run(tx) }
    };
    // The glue decodes only a row whose recorded shape is this type's (the
    // dispatcher asserts it first), so a decode failure has two possible
    // causes — corrupt bytes, or a change inside a type a field holds, which
    // the fingerprint hashes by name and cannot see (GUIDE §12) — and the
    // text names both, with both fixes.
    let undecodable = "its recorded shape is this binary's, so either a type one of its fields \
                       holds changed, which the fingerprint cannot see (GUIDE §12), or the stored \
                       payload is corrupt; it retries, then dead-letters — if such a type changed, \
                       restore that type's old definition and put the new one in a new #[job] type \
                       under a new name (GUIDE §6); if the bytes are corrupt and other rows are \
                       damaged too, restore this node from its archive (GUIDE §15)";

    Ok(quote! {
        #input

        impl ::charpente::core::table::Codec for #name {
            fn encode(&self, out: &mut ::std::vec::Vec<u8>) {
                #(::charpente::core::table::Codec::encode(&self.#field_idents, out);)*
            }

            fn decode(input: &mut &[u8]) -> ::std::io::Result<Self> {
                #(let #field_idents =
                    <#field_types as ::charpente::core::table::Codec>::decode(input)?;)*
                ::std::result::Result::Ok(Self { #(#field_idents,)* })
            }
        }

        impl ::charpente::core::db::Job for #name {
            const NAME: &'static str = #name_str;
            const FINGERPRINT: u64 = #fingerprint;
            const RETRY: ::charpente::core::job::RetryPolicy = ::charpente::core::job::RetryPolicy {
                max: #max,
                backoff: ::std::time::Duration::from_millis(#backoff_millis),
            };
        }

        impl ::charpente::kernel::JobEntry for #name {
            fn def() -> ::charpente::kernel::JobDef {
                fn __charpente_job_glue(
                    tx: &mut ::charpente::Tx,
                    payload: &[u8],
                    cap: ::std::option::Option<&::charpente::CrossTenant>,
                ) -> ::std::result::Result<(), ::charpente::kernel::JobError> {
                    let _ = cap; // an undeclared job is handed None and uses none of it
                    let input = &mut &payload[..];
                    let job = <#name as ::charpente::core::table::Codec>::decode(input)
                        .map_err(|e| ::charpente::kernel::JobError::App(::std::format!(
                            "job decode for {}: {e} — {}", #name_str, #undecodable
                        )))?;
                    if !input.is_empty() {
                        return ::std::result::Result::Err(::charpente::kernel::JobError::App(
                            ::std::format!(
                                "job decode: {} trailing bytes after a {} payload — {}",
                                input.len(), #name_str, #undecodable,
                            ),
                        ));
                    }
                    #run_call
                }
                ::charpente::kernel::JobDef {
                    name: <#name as ::charpente::core::db::Job>::NAME,
                    retry: <#name as ::charpente::core::db::Job>::RETRY,
                    cross_tenant: #cross_tenant,
                    fingerprint: <#name as ::charpente::core::db::Job>::FINGERPRINT,
                    glue: __charpente_job_glue,
                }
            }
        }
    })
}
