use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    braced,
    parse::{Parse, ParseStream},
    parse_macro_input, FnArg, Ident, Pat, ReturnType, Token, TraitItemFn, Type,
};

/// Module-level macro for defining an RPC service.
///
/// Generates a module containing `Service` trait, `Client` struct,
/// `server()` wrapper function, and all request/response types.
///
/// By default, both server and client code are generated.
/// Use `#[server]` or `#[client]` to generate only one side.
///
/// # Examples
///
/// ```ignore
/// // Generate both server and client (default)
/// mill_rpc::service! {
///     service Calculator {
///         fn add(a: i32, b: i32) -> i32;
///         fn divide(a: f64, b: f64) -> f64;
///     }
/// }
///
/// // Server only
/// mill_rpc::service! {
///     #[server]
///     service Calculator {
///         fn add(a: i32, b: i32) -> i32;
///     }
/// }
///
/// // Client only (e.g. in a separate client crate)
/// mill_rpc::service! {
///     #[client]
///     service Calculator {
///         fn add(a: i32, b: i32) -> i32;
///     }
/// }
/// ```
#[proc_macro]
pub fn service(input: TokenStream) -> TokenStream {
    let def = parse_macro_input!(input as ServiceDef);
    match generate_service_module(def) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GenerateMode {
    Both,
    ServerOnly,
    ClientOnly,
}

struct ServiceDef {
    mode: GenerateMode,
    name: Ident,
    methods: Vec<MethodDef>,
}

struct MethodDef {
    name: Ident,
    args: Vec<(Ident, Type)>,
    return_type: Type,
}

impl Parse for ServiceDef {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        // Parse optional #[server] or #[client]
        let mode = if input.peek(Token![#]) {
            input.parse::<Token![#]>()?;
            let content;
            syn::bracketed!(content in input);
            let attr_name: Ident = content.parse()?;
            match attr_name.to_string().as_str() {
                "server" => GenerateMode::ServerOnly,
                "client" => GenerateMode::ClientOnly,
                other => {
                    return Err(syn::Error::new_spanned(
                        attr_name,
                        format!(
                            "Unknown attribute `{}`, expected `server` or `client`",
                            other
                        ),
                    ))
                }
            }
        } else {
            GenerateMode::Both
        };

        // Parse `service Name { ... }`
        let service_kw: Ident = input.parse()?;
        if service_kw != "service" {
            return Err(syn::Error::new_spanned(service_kw, "Expected `service`"));
        }

        let name: Ident = input.parse()?;

        let content;
        braced!(content in input);

        let mut methods = Vec::new();
        while !content.is_empty() {
            let method: TraitItemFn = content.parse()?;

            let method_name = method.sig.ident.clone();

            let mut args = Vec::new();
            for arg in &method.sig.inputs {
                match arg {
                    FnArg::Typed(pat_type) => {
                        let ident = match &*pat_type.pat {
                            Pat::Ident(pi) => pi.ident.clone(),
                            other => {
                                return Err(syn::Error::new_spanned(
                                    other,
                                    "Expected a simple identifier for argument name",
                                ))
                            }
                        };
                        args.push((ident, (*pat_type.ty).clone()));
                    }
                    FnArg::Receiver(_) => {
                        return Err(syn::Error::new_spanned(
                            arg,
                            "Service methods should not have `self` parameter",
                        ))
                    }
                }
            }

            let return_type = match &method.sig.output {
                ReturnType::Default => syn::parse_quote!(()),
                ReturnType::Type(_, ty) => (**ty).clone(),
            };

            methods.push(MethodDef {
                name: method_name,
                args,
                return_type,
            });
        }

        Ok(ServiceDef {
            mode,
            name,
            methods,
        })
    }
}

fn generate_service_module(def: ServiceDef) -> syn::Result<proc_macro2::TokenStream> {
    let mod_name = format_ident!("{}", to_snake_case(&def.name.to_string()));
    let service_name_str = def.name.to_string();
    let method_count = def.methods.len() as u16;

    let gen_server = def.mode != GenerateMode::ClientOnly;
    let gen_client = def.mode != GenerateMode::ServerOnly;

    let method_consts: Vec<_> = def
        .methods
        .iter()
        .enumerate()
        .map(|(idx, m)| {
            let const_name = format_ident!("{}", m.name.to_string().to_uppercase());
            let id = idx as u16;
            quote! { pub const #const_name: u16 = #id; }
        })
        .collect();

    // Request / Response types
    let type_defs: Vec<_> = def
        .methods
        .iter()
        .map(|m| {
            let req_name = format_ident!("{}Request", to_pascal_case(&m.name.to_string()));
            let resp_name = format_ident!("{}Response", to_pascal_case(&m.name.to_string()));
            let ret_ty = &m.return_type;

            let field_names: Vec<_> = m.args.iter().map(|(n, _)| n).collect();
            let field_types: Vec<_> = m.args.iter().map(|(_, t)| t).collect();

            let req_struct = if m.args.is_empty() {
                quote! {
                    #[derive(::serde::Serialize, ::serde::Deserialize, Debug)]
                    pub(super) struct #req_name;
                }
            } else {
                quote! {
                    #[derive(::serde::Serialize, ::serde::Deserialize, Debug)]
                    pub(super) struct #req_name {
                        #( pub #field_names: #field_types, )*
                    }
                }
            };

            quote! {
                #req_struct

                #[derive(::serde::Serialize, ::serde::Deserialize, Debug)]
                pub(super) struct #resp_name(pub #ret_ty);
            }
        })
        .collect();

    let server_trait = if gen_server {
        let trait_methods: Vec<_> = def
            .methods
            .iter()
            .map(|m| {
                let name = &m.name;
                let ret_ty = &m.return_type;
                let arg_names: Vec<_> = m.args.iter().map(|(n, _)| n).collect();
                let arg_types: Vec<_> = m.args.iter().map(|(_, t)| t).collect();
                quote! {
                    fn #name(&self, ctx: &::mill_rpc_core::RpcContext, #( #arg_names: #arg_types ),*) -> #ret_ty;
                }
            })
            .collect();

        let dispatch_arms: Vec<_> = def
            .methods
            .iter()
            .map(|m| {
                let name = &m.name;
                let const_name = format_ident!("{}", m.name.to_string().to_uppercase());
                let req_name = format_ident!("{}Request", to_pascal_case(&m.name.to_string()));
                let resp_name = format_ident!("{}Response", to_pascal_case(&m.name.to_string()));

                let call_args = if m.args.is_empty() {
                    quote! {}
                } else {
                    let field_names: Vec<_> = m.args.iter().map(|(n, _)| n).collect();
                    let args: Vec<_> = field_names.iter().map(|n| quote! { req.#n }).collect();
                    quote! { , #( #args ),* }
                };

                quote! {
                    methods::#const_name => {
                        let req: types::#req_name = codec.deserialize(args)?;
                        let result = svc.#name(ctx #call_args);
                        codec.serialize(&types::#resp_name(result))
                    }
                }
            })
            .collect();

        quote! {
            /// Server trait — implement this to handle RPC calls for this service.
            pub trait Service: Send + Sync + 'static {
                #( #trait_methods )*
            }

            /// Internal dispatcher that bridges `Service` impl to `ServiceDispatch`.
            struct Dispatcher<T: Service>(T);

            impl<T: Service> ::mill_rpc_core::ServiceDispatch for Dispatcher<T> {
                fn dispatch(
                    &self,
                    ctx: &::mill_rpc_core::RpcContext,
                    method_id: u16,
                    args: &[u8],
                    codec: &::mill_rpc_core::Codec,
                ) -> Result<Vec<u8>, ::mill_rpc_core::RpcError> {
                    let svc = &self.0;
                    match method_id {
                        #( #dispatch_arms, )*
                        _ => Err(::mill_rpc_core::RpcError::method_not_found(method_id)),
                    }
                }
            }

            /// Wrap a `Service` implementation for server registration.
            ///
            /// # Example
            /// ```ignore
            /// RpcServer::builder()
            ///     .service(calculator::server(MyCalc))
            ///     .build(&event_loop)?;
            /// ```
            pub fn server<T: Service>(implementation: T) -> impl ::mill_rpc_core::ServiceDispatch {
                Dispatcher(implementation)
            }
        }
    } else {
        quote! {}
    };

    let client_code = if gen_client {
        let client_methods: Vec<_> = def
            .methods
            .iter()
            .map(|m| {
                let name = &m.name;
                let ret_ty = &m.return_type;
                let const_name = format_ident!("{}", m.name.to_string().to_uppercase());
                let req_name = format_ident!("{}Request", to_pascal_case(&m.name.to_string()));
                let resp_name = format_ident!("{}Response", to_pascal_case(&m.name.to_string()));

                let arg_names: Vec<_> = m.args.iter().map(|(n, _)| n).collect();
                let arg_types: Vec<_> = m.args.iter().map(|(_, t)| t).collect();

                let req_construct = if m.args.is_empty() {
                    quote! { types::#req_name }
                } else {
                    quote! { types::#req_name { #( #arg_names, )* } }
                };

                quote! {
                    pub fn #name(&self, #( #arg_names: #arg_types ),*) -> Result<#ret_ty, ::mill_rpc_core::RpcError> {
                        let req = #req_construct;
                        let payload = self.codec.serialize(&req)?;
                        let resp_bytes = self.transport.call(
                            self.service_id,
                            methods::#const_name,
                            payload,
                        )?;
                        let resp: types::#resp_name = self.codec.deserialize(&resp_bytes)?;
                        Ok(resp.0)
                    }
                }
            })
            .collect();

        quote! {
            /// Generated RPC client for this service.
            pub struct Client {
                transport: ::std::sync::Arc<dyn ::mill_rpc_core::RpcTransport>,
                codec: ::mill_rpc_core::Codec,
                service_id: u16,
            }

            impl Client {
                /// Create a new client.
                ///
                /// - `transport`: the RPC transport (typically an `RpcClient`)
                /// - `codec`: serialization codec (must match the server)
                /// - `service_id`: the ID assigned to this service on the server
                ///   (matches registration order, starting from 0)
                pub fn new(
                    transport: ::std::sync::Arc<dyn ::mill_rpc_core::RpcTransport>,
                    codec: ::mill_rpc_core::Codec,
                    service_id: u16,
                ) -> Self {
                    Self { transport, codec, service_id }
                }

                #( #client_methods )*
            }
        }
    } else {
        quote! {}
    };

    let output = quote! {
        pub mod #mod_name {
            #![allow(unused_imports)]
            use super::*;

            /// Method ID constants.
            pub mod methods {
                #( #method_consts )*
            }

            /// Service metadata.
            pub const SERVICE_NAME: &str = #service_name_str;
            pub const METHOD_COUNT: u16 = #method_count;

            /// Internal request/response types (not part of the public API).
            mod types {
                use super::super::*;
                #( #type_defs )*
            }

            #server_trait

            #client_code
        }
    };

    Ok(output)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn to_snake_case(s: &str) -> String {
    let mut result = String::new();
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() {
            if i > 0 {
                result.push('_');
            }
            result.push(ch.to_lowercase().next().unwrap());
        } else {
            result.push(ch);
        }
    }
    result
}

fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                None => String::new(),
                Some(c) => c.to_uppercase().to_string() + chars.as_str(),
            }
        })
        .collect()
}
