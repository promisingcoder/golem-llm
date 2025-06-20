//! WASM component implementation for the `golem:search` interface.
//!
//! This module is only included when the crate is compiled for the
//! `wasm32-wasi` target which matches Golem's execution environment (WASI 0.23).
//! When compiled on the host for ordinary `cargo test` runs the module is
//! excluded so that missing WIT files or `wit-bindgen` codegen artefacts do not
//! interfere with normal CI pipelines.

#![cfg(target_arch = "wasm32")]

use crate::{ElasticSearchClient, Doc, map_status};
use log::trace;

// Generate bindings for the search interface. The exact WIT location will be
// finalised once the `golem:search` specification is merged upstream.
wit_bindgen::generate!({
    path: "../search/wit",      // relative to the crate root at compile time
    world: "search-library",
    generate_all,
    generate_unused_types: true,
    additional_derives: [PartialEq, golem_rust::FromValueAndType, golem_rust::IntoValue],
    pub_export_macro: true,
});

pub use self::exports::golem as golem;

use golem::search::search::{Config, Document, Error, ErrorCode, Guest, Hit};

struct ElasticSearchComponent;

impl ElasticSearchComponent {
    fn client() -> Result<ElasticSearchClient, Error> {
        ElasticSearchClient::new().map_err(|e| Error {
            code: ErrorCode::InternalError,
            message: e.to_string(),
            provider_error_json: None,
        })
    }

    fn map_error(err: crate::SearchError) -> Error {
        match err {
            crate::SearchError::IndexNotFound => Error {
                code: ErrorCode::IndexNotFound,
                message: "index not found".to_string(),
                provider_error_json: None,
            },
            crate::SearchError::InvalidQuery(m) => Error {
                code: ErrorCode::InvalidQuery,
                message: m,
                provider_error_json: None,
            },
            crate::SearchError::Unsupported => Error {
                code: ErrorCode::Unsupported,
                message: "unsupported operation".to_string(),
                provider_error_json: None,
            },
            crate::SearchError::Timeout => Error {
                code: ErrorCode::Timeout,
                message: "timeout".to_string(),
                provider_error_json: None,
            },
            crate::SearchError::RateLimited => Error {
                code: ErrorCode::RateLimited,
                message: "rate limited".to_string(),
                provider_error_json: None,
            },
            crate::SearchError::Internal(m) => Error {
                code: ErrorCode::InternalError,
                message: m,
                provider_error_json: None,
            },
        }
    }
}

impl Guest for ElasticSearchComponent {
    fn create_index(name: String, mapping_json: Option<String>, _config: Config) -> Result<golem::search::search::Unit, Error> {
        let client = Self::client()?;
        let mapping = mapping_json.and_then(|s| serde_json::from_str(&s).ok());
        client
            .create_index(&name, mapping)
            .map(|_| golem::search::search::Unit {})
            .map_err(|e| Self::map_error(crate::SearchError::Internal(e.to_string())))
    }

    fn delete_index(name: String, _config: Config) -> Result<golem::search::search::Unit, Error> {
        let client = Self::client()?;
        client
            .delete_index(&name)
            .map(|_| golem::search::search::Unit {})
            .map_err(|e| Self::map_error(crate::SearchError::Internal(e.to_string())))
    }

    fn upsert(index: String, doc: Document, _config: Config) -> Result<golem::search::search::Unit, Error> {
        let client = Self::client()?;
        let internal_doc = Doc { id: doc.id.clone(), content: doc.json.clone() };
        client
            .upsert_document(&index, internal_doc)
            .map(|_| golem::search::search::Unit {})
            .map_err(|e| Self::map_error(crate::SearchError::Internal(e.to_string())))
    }

    fn delete(index: String, id: String, _config: Config) -> Result<golem::search::search::Unit, Error> {
        let client = Self::client()?;
        client
            .delete_document(&index, &id)
            .map(|_| golem::search::search::Unit {})
            .map_err(|e| Self::map_error(crate::SearchError::Internal(e.to_string())))
    }

    fn get(index: String, id: String, _config: Config) -> Result<Option<Document>, Error> {
        let client = Self::client()?;
        match client.get_document(&index, &id) {
            Ok(Some(value)) => Ok(Some(Document { id, json: value.to_string() })),
            Ok(None) => Ok(None),
            Err(e) => Err(Self::map_error(crate::SearchError::Internal(e.to_string()))),
        }
    }

    fn search(index: String, query: String, from: Option<u64>, size: Option<u64>, _config: Config) -> Result<Vec<Hit>, Error> {
        let client = Self::client()?;
        match client.search_documents(&index, &query, from, size) {
            Ok(values) => Ok(values.into_iter().map(|v| Hit { doc: Document { id: String::new(), json: v.to_string() }, score: None }).collect()),
            Err(e) => Err(Self::map_error(crate::SearchError::Internal(e.to_string()))),
        }
    }

    // The rest of the interface defaults to "unsupported" for now. These will
    // be incrementally implemented in follow-up tasks.
}

// For the initial build we export the raw component directly without durability wrappers.
golem_llm::export_llm!(ElasticSearchComponent with_types_in golem_llm);