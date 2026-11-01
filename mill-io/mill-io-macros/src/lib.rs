//! The `#[mill_io::main]` and `#[mill_io::test]` attributes. Use them through
//! the `mill-io` crate, which re-exports them.

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote;
use syn::{
    meta::ParseNestedMeta, parse::Parser, spanned::Spanned, FnArg, Item, ItemFn, LitInt, LitStr,
    Type,
};

/// Runs `main` with a mill-io event loop.
///
/// The loop is built from the options, runs on a background thread named
/// `mill-io-reactor`, and is entered on the main thread, so the free functions
/// (`mill_io::spawn`, `post`, `after`, ...) work in the body. When the body
/// returns, including with `Err` or by panicking, the loop is stopped and
/// joined. The function may return `()` or a `Result`, and may take one
/// parameter, `&Arc<EventLoop>`, `&EventLoop` or `Arc<EventLoop>`, to reach
/// the loop itself.
///
/// Options, all optional: `workers = N` (I/O pool), `compute_workers = N`,
/// `low_latency` (handlers on the reactor thread), `events = N` (events per
/// poll) and `poll_timeout_ms = N`.
///
/// ```rust
/// use std::time::Duration;
///
/// #[mill_io::main(compute_workers = 4)]
/// fn main() -> Result<(), mill_io::JoinError> {
///     let answer = mill_io::spawn(|| 6 * 7);
///     mill_io::after(Duration::from_millis(1), || println!("timer fired"));
///     assert_eq!(answer.join()?, 42);
///     Ok(())
/// }
/// ```
#[proc_macro_attribute]
pub fn main(args: TokenStream, item: TokenStream) -> TokenStream {
    attribute(Kind::Main, args, item)
}

/// Runs a test with a mill-io event loop, as `#[mill_io::main]` does for `main`.
///
/// Takes the options of `#[mill_io::main]` plus:
/// - `modes = "pool" | "direct" | "both"` (default `"pool"`): dispatch handlers
///   on the I/O pool, on the reactor thread, or run the body once per mode,
///   each on a fresh loop. A failure names its mode.
/// - `timeout = "10s"` (units `ms`, `s` or `m`): a body still running then
///   fails the test instead of hanging it.
///
/// The body may return `()` or `Result<(), E: Debug>`.
///
/// ```rust
/// #[mill_io::test(modes = "both", timeout = "5s")]
/// fn posts_run_on_the_reactor() {
///     let (tx, rx) = std::sync::mpsc::channel();
///     let handle = mill_io::current();
///     mill_io::post(move || tx.send(handle.is_reactor_thread()).unwrap());
///     assert!(rx.recv().unwrap());
/// }
/// # fn main() {}
/// ```
#[proc_macro_attribute]
pub fn test(args: TokenStream, item: TokenStream) -> TokenStream {
    attribute(Kind::Test, args, item)
}

fn attribute(kind: Kind, args: TokenStream, item: TokenStream) -> TokenStream {
    let item = TokenStream2::from(item);
    match expand(kind, args.into(), item.clone()) {
        Ok(expanded) => expanded.into(),
        Err(error) => {
            // Keeping the item avoids follow-on errors such as a missing `main`.
            let mut tokens = error.into_compile_error();
            tokens.extend(item);
            tokens.into()
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Main,
    Test,
}

impl Kind {
    fn attribute(self) -> &'static str {
        match self {
            Kind::Main => "#[mill_io::main]",
            Kind::Test => "#[mill_io::test]",
        }
    }

    fn option_names(self) -> &'static str {
        match self {
            Kind::Main => "workers, compute_workers, low_latency, events, poll_timeout_ms",
            Kind::Test => {
                "workers, compute_workers, low_latency, events, poll_timeout_ms, modes, timeout"
            }
        }
    }
}

#[derive(Default)]
struct Options {
    workers: Option<usize>,
    compute_workers: Option<usize>,
    events: Option<usize>,
    poll_timeout_ms: Option<u64>,
    low_latency: Option<Span>,
    modes: Option<(Vec<&'static str>, Span)>,
    timeout_ms: Option<u64>,
}

fn set<T>(slot: &mut Option<T>, value: T, meta: &ParseNestedMeta) -> syn::Result<()> {
    if slot.is_some() {
        let name = meta
            .path
            .get_ident()
            .map(|i| i.to_string())
            .unwrap_or_default();
        return Err(meta.error(format!("duplicate option `{name}`")));
    }
    *slot = Some(value);
    Ok(())
}

fn int<T>(meta: &ParseNestedMeta) -> syn::Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    meta.value()?.parse::<LitInt>()?.base10_parse()
}

fn parse_timeout(lit: &LitStr) -> syn::Result<u64> {
    let text = lit.value();
    let split = text
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(text.len());
    let (number, unit) = text.split_at(split);
    let millis_per_unit = match unit {
        "ms" => 1,
        "s" => 1_000,
        "m" => 60_000,
        _ => 0,
    };
    match number.parse::<u64>() {
        Ok(number) if millis_per_unit > 0 => number
            .checked_mul(millis_per_unit)
            .ok_or_else(|| syn::Error::new(lit.span(), "timeout is too large")),
        _ => Err(syn::Error::new(
            lit.span(),
            "expected a timeout such as \"500ms\", \"10s\" or \"2m\"",
        )),
    }
}

fn parse_modes(lit: &LitStr) -> syn::Result<Vec<&'static str>> {
    match lit.value().as_str() {
        "pool" => Ok(vec!["Pool"]),
        "direct" => Ok(vec!["Direct"]),
        "both" => Ok(vec!["Pool", "Direct"]),
        _ => Err(syn::Error::new(
            lit.span(),
            "expected modes = \"pool\", \"direct\" or \"both\"",
        )),
    }
}

fn parse_options(kind: Kind, args: TokenStream2) -> syn::Result<Options> {
    let mut options = Options::default();
    let parser = syn::meta::parser(|meta| {
        let path = &meta.path;
        let name = quote!(#path).to_string().replace(' ', "");
        match (name.as_str(), kind) {
            ("workers", _) => set(&mut options.workers, int(&meta)?, &meta),
            ("compute_workers", _) => set(&mut options.compute_workers, int(&meta)?, &meta),
            ("events", _) => set(&mut options.events, int(&meta)?, &meta),
            ("poll_timeout_ms", _) => set(&mut options.poll_timeout_ms, int(&meta)?, &meta),
            ("low_latency", _) => {
                if !meta.input.is_empty() && !meta.input.peek(syn::Token![,]) {
                    return Err(meta.error("`low_latency` takes no value"));
                }
                set(&mut options.low_latency, meta.path.span(), &meta)
            }
            ("modes", Kind::Test) => {
                let lit: LitStr = meta.value()?.parse()?;
                set(&mut options.modes, (parse_modes(&lit)?, lit.span()), &meta)
            }
            ("timeout", Kind::Test) => {
                let lit: LitStr = meta.value()?.parse()?;
                set(&mut options.timeout_ms, parse_timeout(&lit)?, &meta)
            }
            _ => Err(meta.error(format!(
                "unknown {} option `{name}`; expected one of: {}",
                kind.attribute(),
                kind.option_names()
            ))),
        }
    });
    parser.parse2(args)?;
    Ok(options)
}

fn builder_calls(options: &Options, kind: Kind) -> TokenStream2 {
    let mut calls = TokenStream2::new();
    if let Some(n) = options.workers {
        calls.extend(quote!(.io_workers(#n)));
    }
    if let Some(n) = options.compute_workers {
        calls.extend(quote!(.compute_workers(#n)));
    }
    if let Some(n) = options.events {
        calls.extend(quote!(.events_capacity(#n)));
    }
    if let Some(ms) = options.poll_timeout_ms {
        calls.extend(quote!(.poll_timeout(::std::time::Duration::from_millis(#ms))));
    }
    // A test's modes decide dispatch; `low_latency` there means `modes = "direct"`.
    if options.low_latency.is_some() && kind == Kind::Main {
        calls.extend(quote!(.low_latency()));
    }
    calls
}

fn error(tokens: impl quote::ToTokens, kind: Kind, message: &str) -> syn::Error {
    syn::Error::new_spanned(tokens, format!("{} {message}", kind.attribute()))
}

fn expand(kind: Kind, args: TokenStream2, item: TokenStream2) -> syn::Result<TokenStream2> {
    let options = parse_options(kind, args)?;
    let function = match syn::parse2::<Item>(item)? {
        Item::Fn(function) => function,
        other => return Err(error(other, kind, "can only be applied to a function")),
    };
    let ItemFn {
        attrs,
        vis,
        sig,
        block,
    } = function;

    if let Some(asyncness) = sig.asyncness {
        return Err(error(asyncness, kind, "functions cannot be async"));
    }
    if !sig.generics.params.is_empty() || sig.generics.where_clause.is_some() {
        return Err(error(&sig.generics, kind, "functions cannot be generic"));
    }
    if sig.inputs.len() > 1 {
        return Err(error(
            &sig.inputs,
            kind,
            "functions take no parameter, or one for the loop: \
             `&Arc<EventLoop>`, `&EventLoop` or `Arc<EventLoop>`",
        ));
    }
    let argument = match sig.inputs.first() {
        None => quote!(),
        Some(FnArg::Receiver(receiver)) => {
            return Err(error(receiver, kind, "cannot be applied to a method"))
        }
        Some(FnArg::Typed(typed)) => match *typed.ty {
            Type::Reference(_) => quote!(__mill_io_loop),
            _ => quote!(::std::sync::Arc::clone(__mill_io_loop)),
        },
    };
    if kind == Kind::Test {
        if let Some(attr) = attrs.iter().find(|a| a.path().is_ident("test")) {
            return Err(error(
                attr,
                kind,
                "already declares the test; remove #[test]",
            ));
        }
    }

    let name = &sig.ident;
    let output = &sig.output;
    let inputs = &sig.inputs;
    let calls = builder_calls(&options, kind);
    let builder = quote!(::mill_io::EventLoop::builder() #calls);
    let body = quote! {
        fn __mill_io_body(#inputs) #output #block
    };
    let loop_param = quote!(__mill_io_loop: &::std::sync::Arc<::mill_io::EventLoop>);

    if kind == Kind::Main {
        return Ok(quote! {
            #(#attrs)*
            #vis fn #name() #output {
                #body
                ::mill_io::__private::run_main(#builder, |#loop_param| __mill_io_body(#argument))
            }
        });
    }

    let modes = match (&options.modes, options.low_latency) {
        (Some((_, span)), Some(_)) => {
            return Err(syn::Error::new(
                *span,
                "#[mill_io::test] takes either `modes` or `low_latency`, not both",
            ))
        }
        (Some((modes, _)), None) => modes.clone(),
        (None, Some(_)) => vec!["Direct"],
        (None, None) => vec!["Pool"],
    };
    let modes = modes.iter().map(|mode| {
        let mode = syn::Ident::new(mode, Span::call_site());
        quote!(::mill_io::__private::Mode::#mode)
    });
    let timeout = match options.timeout_ms {
        Some(ms) => {
            quote!(::core::option::Option::Some(::std::time::Duration::from_millis(#ms)))
        }
        None => quote!(::core::option::Option::None),
    };
    Ok(quote! {
        #[::core::prelude::v1::test]
        #(#attrs)*
        #vis fn #name() {
            #body
            ::mill_io::__private::run_test(
                || #builder,
                &[#(#modes),*],
                #timeout,
                |#loop_param| {
                    ::mill_io::__private::TestResult::into_test_result(__mill_io_body(#argument))
                },
            )
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{expand, parse_timeout, Kind};
    use proc_macro2::{Span, TokenStream as TokenStream2};
    use quote::quote;
    use syn::LitStr;

    fn compact(tokens: TokenStream2) -> String {
        tokens.to_string().split_whitespace().collect()
    }

    fn expanded(kind: Kind, args: TokenStream2) -> String {
        compact(
            expand(
                kind,
                args,
                quote!(
                    fn body() {}
                ),
            )
            .unwrap(),
        )
    }

    #[test]
    fn main_applies_every_option() {
        let out = expanded(
            Kind::Main,
            quote!(
                workers = 3,
                compute_workers = 5,
                low_latency,
                events = 64,
                poll_timeout_ms = 25
            ),
        );
        let expected = compact(quote!(::mill_io::EventLoop::builder()
            .io_workers(3usize)
            .compute_workers(5usize)
            .events_capacity(64usize)
            .poll_timeout(::std::time::Duration::from_millis(25u64))
            .low_latency()));
        assert!(out.contains(&expected), "{out}");
        assert!(out.contains("run_main"), "{out}");
    }

    #[test]
    fn main_without_options_uses_the_default_builder() {
        let out = expanded(Kind::Main, quote!());
        let expected = compact(quote!(::mill_io::__private::run_main(
            ::mill_io::EventLoop::builder(),
        )));
        assert!(out.contains(&expected[..expected.len() - 1]), "{out}");
    }

    #[test]
    fn test_applies_modes_timeout_and_loop_options() {
        let out = expanded(
            Kind::Test,
            quote!(
                modes = "both",
                timeout = "2s",
                workers = 2,
                poll_timeout_ms = 7
            ),
        );
        let expected = compact(quote!(
            || ::mill_io::EventLoop::builder()
                .io_workers(2usize)
                .poll_timeout(::std::time::Duration::from_millis(7u64)),
            &[
                ::mill_io::__private::Mode::Pool,
                ::mill_io::__private::Mode::Direct
            ],
            ::core::option::Option::Some(::std::time::Duration::from_millis(2000u64)),
        ));
        assert!(out.contains(&expected), "{out}");
        assert!(out.contains("#[::core::prelude::v1::test]"), "{out}");
    }

    #[test]
    fn test_low_latency_means_direct_mode_only() {
        let out = expanded(Kind::Test, quote!(low_latency));
        assert!(
            out.contains("&[::mill_io::__private::Mode::Direct]"),
            "{out}"
        );
        assert!(!out.contains("low_latency()"), "{out}");
        let out = expanded(Kind::Test, quote!());
        assert!(out.contains("&[::mill_io::__private::Mode::Pool]"), "{out}");
        assert!(out.contains("Option::None"), "{out}");
    }

    #[test]
    fn timeouts_parse_with_units() {
        let parse = |text: &str| parse_timeout(&LitStr::new(text, Span::call_site()));
        assert_eq!(parse("250ms").unwrap(), 250);
        assert_eq!(parse("10s").unwrap(), 10_000);
        assert_eq!(parse("2m").unwrap(), 120_000);
        for bad in ["", "10", "s", "1.5s", "10h", "-1s"] {
            assert!(parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn loop_parameter_is_passed_by_reference_or_cloned() {
        let by_ref = compact(
            expand(
                Kind::Main,
                quote!(),
                quote!(
                    fn main(event_loop: &Arc<EventLoop>) {}
                ),
            )
            .unwrap(),
        );
        assert!(
            by_ref.contains("__mill_io_body(__mill_io_loop)"),
            "{by_ref}"
        );
        let owned = compact(
            expand(
                Kind::Main,
                quote!(),
                quote!(
                    fn main(event_loop: Arc<EventLoop>) {}
                ),
            )
            .unwrap(),
        );
        assert!(owned.contains("Arc::clone(__mill_io_loop)"), "{owned}");
    }
}
