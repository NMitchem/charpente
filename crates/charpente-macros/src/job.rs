//! `#[job(global | scoped_by = Org[, cross_tenant][, retry(max = N, backoff = "30s")])]`,
//! the words in any order, each at most once, and exactly one of `global` /
//! `scoped_by` (DESIGN.md §6, specs 2026-07-02-queue-cron-design.md and
//! 2026-09-27-job-tenancy-design.md §5):
//! turns a struct into a typed job
//! payload — a field-wise `Codec` impl (mirrors `#[table]`'s row codec, minus
//! the id: a job payload has no primary key), `impl Job` carrying the name,
//! the payload's structural fingerprint (`fingerprint::compute`, continued over
//! the tenant type's `NAME` by `charpente_core::db::scoped_fingerprint` for a
//! `scoped_by` job), the scope type and the retry policy, and the
//! `JobEntry` glue `jobs![]` collects. Errors name the accepted forms (hard
//! rule 5).

use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned;
use syn::{Data, DeriveInput, Error, Fields, Ident, LitInt, LitStr, Token, parenthesized};

const ACCEPTED_FORMS: &str = "a job declares `global` or `scoped_by = <Type>`, and may add \
     `cross_tenant` and `retry(max = <int>, backoff = \"<n><unit>\")` (unit is one of ms, s, \
     m, h), in any order — e.g. `#[job(scoped_by = Org, retry(max = 3, backoff = \"10s\"))]` \
     or `#[job(global, cross_tenant)]`";

// ---------------------------------------------------------------- args

/// A job's answer to "which tenant does this act in" (job-tenancy spec §5).
pub(crate) enum Tenancy {
    Global,
    ScopedBy(syn::Path),
}

pub(crate) struct JobArgs {
    pub max: u32,
    pub backoff_millis: u64,
    /// `cross_tenant` — the same declared capability a route carries
    /// (tenant-scoping spec §3), for the background rollups that need it.
    /// A declared job's `run` is called with the token as a second argument.
    pub cross_tenant: bool,
    /// `global` or `scoped_by = Org`. `None` only while parsing: `expand`
    /// refuses a job that gave neither, by name, before anything else.
    pub tenancy: Option<Tenancy>,
}

impl Default for JobArgs {
    fn default() -> JobArgs {
        JobArgs {
            max: 5,
            backoff_millis: 30_000,
            cross_tenant: false,
            tenancy: None,
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

/// One word of the attribute, parsed one at a time so each can come at most
/// once and in any order — `#[table]`'s `TableArg` is the precedent, for the
/// same reason: a declaration repeated is a typo or a bad merge, never an
/// override.
enum JobArg {
    Global(proc_macro2::Span),
    ScopedBy(proc_macro2::Span, syn::Path),
    CrossTenant(proc_macro2::Span),
    Retry(proc_macro2::Span, Vec<RetryField>),
}

impl Parse for JobArg {
    fn parse(input: ParseStream) -> syn::Result<JobArg> {
        let ident: Ident = input
            .parse()
            .map_err(|e| Error::new(e.span(), ACCEPTED_FORMS))?;
        match ident.to_string().as_str() {
            "global" => Ok(JobArg::Global(ident.span())),
            "scoped_by" => {
                input
                    .parse::<Token![=]>()
                    .map_err(|e| Error::new(e.span(), ACCEPTED_FORMS))?;
                let path: syn::Path = input
                    .parse()
                    .map_err(|e| Error::new(e.span(), ACCEPTED_FORMS))?;
                Ok(JobArg::ScopedBy(ident.span(), path))
            }
            "cross_tenant" => Ok(JobArg::CrossTenant(ident.span())),
            "retry" => {
                // Peeked, so `retry` without its parentheses gets the house
                // message rather than syn's "expected parentheses".
                if !input.peek(syn::token::Paren) {
                    return Err(Error::new(ident.span(), ACCEPTED_FORMS));
                }
                let content;
                parenthesized!(content in input);
                let fields = content.parse_terminated(RetryField::parse, Token![,])?;
                Ok(JobArg::Retry(ident.span(), fields.into_iter().collect()))
            }
            _ => Err(Error::new(ident.span(), ACCEPTED_FORMS)),
        }
    }
}

impl Parse for JobArgs {
    fn parse(input: ParseStream) -> syn::Result<JobArgs> {
        let mut global: Option<proc_macro2::Span> = None;
        let mut scoped_by: Option<syn::Path> = None;
        let mut cross_tenant: Option<proc_macro2::Span> = None;
        let mut retry: Option<Vec<RetryField>> = None;
        while !input.is_empty() {
            match input.parse::<JobArg>()? {
                JobArg::Global(span) => {
                    if global.is_some() {
                        return Err(Error::new(span, "`global` given twice — write it once"));
                    }
                    if let Some(path) = &scoped_by {
                        return Err(Error::new(span, global_and_scoped_by(path)));
                    }
                    global = Some(span);
                }
                JobArg::ScopedBy(span, path) => {
                    if scoped_by.is_some() {
                        return Err(Error::new(span, "`scoped_by` given twice — write it once"));
                    }
                    if global.is_some() {
                        return Err(Error::new(span, global_and_scoped_by(&path)));
                    }
                    scoped_by = Some(path);
                }
                JobArg::CrossTenant(span) => {
                    if cross_tenant.is_some() {
                        return Err(Error::new(
                            span,
                            "`cross_tenant` given twice — write it once",
                        ));
                    }
                    cross_tenant = Some(span);
                }
                JobArg::Retry(span, fields) => {
                    if retry.is_some() {
                        return Err(Error::new(span, "`retry(...)` given twice — write it once"));
                    }
                    retry = Some(fields);
                }
            }
            if input.is_empty() {
                break;
            }
            input
                .parse::<Token![,]>()
                .map_err(|e| Error::new(e.span(), ACCEPTED_FORMS))?;
        }

        let mut max = None;
        let mut backoff_millis = None;
        for field in retry.unwrap_or_default() {
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
            cross_tenant: cross_tenant.is_some(),
            tenancy: match (global, scoped_by) {
                (_, Some(path)) => Some(Tenancy::ScopedBy(path)),
                (Some(_), None) => Some(Tenancy::Global),
                (None, None) => None,
            },
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

/// `global` and `scoped_by` on one job, reported at whichever came second.
/// Quotes the scope path as written (plan amendment A8): a tenant type's
/// `NAME` is a constant of the compiled type, which expansion cannot read.
fn global_and_scoped_by(path: &syn::Path) -> String {
    format!(
        "`global` and `scoped_by = {}` on one job — a job is one or the other; drop one",
        crate::fingerprint::path_as_written(path)
    )
}

// ---------------------------------------------------------------- expand

pub(crate) fn expand(args: JobArgs, input: DeriveInput) -> syn::Result<TokenStream> {
    let name = &input.ident;
    let name_str = name.to_string();

    let Some(tenancy) = &args.tenancy else {
        // Spanned at the attribute, as `#[table]`'s neither-refusal is: the
        // fix is a word the attribute lacks, so there is no token to point
        // at inside it.
        return Err(Error::new(
            proc_macro2::Span::call_site(),
            format!(
                "`{name_str}` declares no tenancy — a job says which: write \
                 `#[job(scoped_by = Org)]` if it acts inside one tenant (it is enqueued from \
                 inside that tenant's scope, and its `run` receives that tenant's `ScopedTx`), \
                 or `#[job(global)]` if it belongs to no tenant (the only kind cron can \
                 schedule). Neither is the default."
            ),
        ));
    };

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
    // A `global` job's fingerprint is its field fold's hash. A scoped job's
    // continues that fold over its tenant type's `NAME`, in
    // `charpente_core::db::scoped_fingerprint` (tenancy-completion spec §4):
    // a constant the compiler evaluates, because expansion cannot read `NAME`.
    // `tenant_name`'s bound is the one `JobDef.scoped_by` already carries at
    // this span, so a scope type with no `Tenant` impl gains no error.
    let fold = crate::fingerprint::compute(&field_idents, &field_types);
    let fingerprint = match tenancy {
        Tenancy::Global => quote! { #fold },
        Tenancy::ScopedBy(path) => quote! {
            ::charpente::core::db::scoped_fingerprint(
                #fold,
                ::charpente::tenant::tenant_name::<#path>(),
            )
        },
    };

    let max = args.max;
    let backoff_millis = args.backoff_millis;
    let cross_tenant = args.cross_tenant;
    // A declared job's `run` takes the token; an undeclared one's does not.
    // `expect` is an invariant: the dispatcher passes the token from the SAME
    // `JobDef.cross_tenant` this expansion sets.
    let cap_expect = quote! {
        cap.expect(
            "job declares cross_tenant but the dispatcher passed no CrossTenant — \
             the flag on JobDef and the token come from the same expansion"
        )
    };
    // A `global` job's glue is today's. A scoped job's opens the tenant the
    // dispatcher's `JobScope` carries, then calls `run` through a fn-pointer
    // binding rather than a method call (job-tenancy spec §5): `job.run(&mut
    // tx)` would deref-coerce `&mut ScopedTx` to `&mut Tx`, so a scoped job
    // whose `run` takes a bare `Tx` would compile and run in no tenant. The
    // binding makes that a compile error, pinned by
    // `compile_fail/scoped_job_run_takes_bare_tx.rs`.
    let (scope_ty, scoped_by_name, run_call) = match tenancy {
        Tenancy::Global => {
            let call = if cross_tenant {
                quote! { job.run(tx, #cap_expect) }
            } else {
                quote! { job.run(tx) }
            };
            (
                quote! { ::charpente::core::table::Unscoped },
                quote! { ::std::option::Option::None },
                quote! {
                    let _ = scope; // a global job is handed no tenant
                    #call
                },
            )
        }
        Tenancy::ScopedBy(path) => {
            let (cap_ty, cap_arg) = if cross_tenant {
                (
                    quote! { , &::charpente::CrossTenant },
                    quote! { , #cap_expect },
                )
            } else {
                (quote! {}, quote! {})
            };
            (
                quote! { #path },
                quote! {
                    ::std::option::Option::Some(::charpente::tenant::tenant_name::<#path>())
                },
                quote! {
                    let scope = scope.expect(
                        "job declares scoped_by but the dispatcher passed no JobScope — the \
                         dispatcher asserts a row carries a tenant exactly when JobDef.scoped_by \
                         is set"
                    );
                    let mut tx = scope.open::<#path>(tx);
                    let run: fn(
                        #name,
                        &mut ::charpente::ScopedTx<'_, '_, #path>
                        #cap_ty
                    ) -> ::std::result::Result<(), ::charpente::kernel::JobError> = #name::run;
                    run(job, &mut tx #cap_arg)
                },
            )
        }
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
                       damaged too, restore this node from its archive (GUIDE §15) or re-seed it \
                       from its primary (GUIDE §10)";

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
            type Scope = #scope_ty;
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
                    scope: ::std::option::Option<::charpente::kernel::JobScope<'_>>,
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
                    scoped_by: #scoped_by_name,
                    scope_type: ::std::any::TypeId::of::<
                        <#name as ::charpente::core::db::Job>::Scope,
                    >(),
                    fingerprint: <#name as ::charpente::core::db::Job>::FINGERPRINT,
                    glue: __charpente_job_glue,
                }
            }
        }
    })
}
