use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{Attribute, Data, DeriveInput, Fields, Ident, Path, Type, parse_macro_input};

#[proc_macro_derive(TTLState, attributes(timestamp_type))]
pub fn ttl_state_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let timestamp_type = match input
        .attrs
        .iter()
        .find(|attr| attr.path().is_ident("timestamp_type"))
    {
        Some(attr) => {
            let path = attr
                .parse_args::<Path>()
                .expect("Expected a type as argument");
            let ty = Type::Path(syn::TypePath { qself: None, path });
            ty
        }
        None => panic!(
            "Missing `timestamp_type` attribute. Usage: `#[derive(TTLState)]
            #[timestamp_type(T)]`"
        ),
    };

    // Process each field
    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => panic!("Only structs with named fields are supported"),
        },
        _ => panic!("Only structs are supported"),
    };

    // Generate the new fields as Option<(FieldType, T)>
    let new_fields = fields.iter().map(|field| {
        let name = &field.ident;
        let ty = &field.ty;
        quote! {
            #name: std::option::Option<(#ty, #timestamp_type)>
        }
    });

    let default_fields = fields.iter().map(|field| {
        let name = &field.ident;
        quote! {#name: None}
    });
    
    let expire_stmts = fields.iter().map(|field| {
        let name = &field.ident;
        quote! {self.#name.take_if(|(_, expiry)| *expiry <= *epoch);}
    });
    
    let is_empty_stmts = fields.iter().map(|field| {
        let name = &field.ident;
        quote! {self.#name.is_none()}
    });

    // Generate getters and setters for each field
    let getters = fields.iter().map(|field| {
        let name = &field.ident.clone().unwrap();
        let ty = &field.ty;
        let fn_name = format_ident!("get_{}", name);
        quote! {
            pub fn #fn_name(&self) -> std::option::Option<&#ty> {
                self.#name.as_ref().map(|(val, _)| val)
            }
        }
    });

    let setters = fields.iter().map(|field| {
        let name = &field.ident.clone().unwrap();
        let ty = &field.ty;
        let fn_name = format_ident!("set_{}", name);
        quote! {
            pub fn #fn_name(&mut self, value: #ty, ttl: #timestamp_type) {
                self.#name = Some((value, ttl));
            }
        }
    });

    let struct_fields = new_fields.clone();

    // Generate the new struct definition and trait implementation
    let structname = format_ident!("TTL{}", name);
    let expanded = quote! {
        #[derive(::serde::Serialize, ::serde::Deserialize)]
        struct #structname {
            #(#struct_fields),*
        }

        impl #structname {
            #(#getters)*
            #(#setters)*
        }

        impl std::default::Default for #structname {
            fn default() -> Self {
                #structname {
                    #(#default_fields),*
                }
            }
        }

        impl ::malstrom::operators::TTLState for #structname {
            type Timestamp = #timestamp_type;

            fn expire(&mut self, epoch: &Self::Timestamp) {
                #(#expire_stmts)*
            }
            fn is_empty(&self) -> bool {
                #(#is_empty_stmts) & *
            }
        }
    };

    TokenStream::from(expanded)
}
