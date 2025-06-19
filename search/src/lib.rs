wit_bindgen::generate!({
    path: "../wit/golem-search",
    world: "search-library",
    generate_all,
    generate_unused_types: true,
    additional_derives: [PartialEq, golem_rust::FromValueAndType, golem_rust::IntoValue],
    pub_export_macro: true,
});

// Re-export the generated export macro so that provider crates can
// expose their implementations with a stable name (`export_search!`).
// The generated macro by `wit-bindgen` has an internal name derived
// from the world (e.g. `__export_search_library_impl`).
#[allow(non_upper_case_globals)]
pub use __export_search_library_impl as export_search;