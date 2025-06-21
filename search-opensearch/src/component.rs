//! WASM component implementation for the `golem:search` interface specific to OpenSearch.
//!
//! All functions are currently implemented as no-ops that return an `Unsupported` error so the
//! component can be compiled and linked while higher-level integration work progresses.
//! Concrete logic will be introduced in follow-up tasks.

#![cfg(target_arch = "wasm32")]

use crate::{OpenSearchClient, SearchError};
use log::trace;

// Generate bindings for the golem:search interface. The actual WIT package path will be updated
// once the canonical specification lands upstream. For now we rely on the same placeholder path
// used by the ElasticSearch component so that both crates continue to compile together.
wit_bindgen::generate!({
    path: "../../search/wit",      // relative to this file at compile time
    world: "search-library",
    generate_all,
    generate_unused_types: true,
    additional_derives: [PartialEq, golem_rust::FromValueAndType, golem_rust::IntoValue],
    pub_export_macro: true,
});

use self::golem::search::search::{Config, Document, Error, ErrorCode, Guest, Hit};

struct OpenSearchComponent;

impl OpenSearchComponent {
    fn client() -> Result<OpenSearchClient, Error> {
        OpenSearchClient::new().map_err(|e| Error {
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

    fn unsupported() -> Error {
        Error {
            code: ErrorCode::Unsupported,
            message: "operation not yet supported".to_string(),
            provider_error_json: None,
        }
    }
}

impl Guest for OpenSearchComponent {
    fn create_index(_name: String, _schema_json: Option<String>, _config: Config) -> Result<(), Error> {
        trace!("create_index called but not implemented");
        Err(Self::unsupported())
    }

    fn delete_index(_name: String, _config: Config) -> Result<(), Error> {
        trace!("delete_index called but not implemented");
        Err(Self::unsupported())
    }

    fn list_indexes(_config: Config) -> Result<Vec<String>, Error> {
        trace!("list_indexes called but not implemented");
        Err(Self::unsupported())
    }

    fn upsert(_index: String, _doc: Document, _config: Config) -> Result<(), Error> {
        trace!("upsert called but not implemented");
        Err(Self::unsupported())
    }

    fn upsert_many(_index: String, _docs: Vec<Document>, _config: Config) -> Result<(), Error> {
        trace!("upsert_many called but not implemented");
        Err(Self::unsupported())
    }

    fn delete(_index: String, _id: String, _config: Config) -> Result<(), Error> {
        trace!("delete called but not implemented");
        Err(Self::unsupported())
    }

    fn delete_many(_index: String, _ids: Vec<String>, _config: Config) -> Result<(), Error> {
        trace!("delete_many called but not implemented");
        Err(Self::unsupported())
    }

    fn get(_index: String, _id: String, _config: Config) -> Result<Option<Document>, Error> {
        trace!("get called but not implemented");
        Err(Self::unsupported())
    }

    fn search(_index: String, _query_json: String, _config: Config) -> Result<Vec<Hit>, Error> {
        let client = match Self::client() {
            Ok(c) => c,
            Err(e) => return Err(e),
        };

        // For now interpret `query_json` as a raw query string. Advanced mapping
        // from structured JSON into the OpenSearch DSL will be added in later tasks.
        match client.search_documents(&_index, &_query_json, None, None) {
            Ok(values) => Ok(values
                .into_iter()
                .map(|v| Hit {
                    doc: Document { id: String::new(), json: v.to_string() },
                    score: None,
                })
                .collect()),
            Err(e) => match e.downcast::<crate::SearchError>() {
                Ok(boxed_se) => Err(Self::map_error(*boxed_se)),
                Err(other) => Err(Error { code: ErrorCode::InternalError, message: other.to_string(), provider_error_json: None }),
            },
        }
    }

    fn stream_search(_index: String, _query_json: String, _config: Config) -> Result<golem_rust::wasm_rpc::Pollable, Error> {
        trace!("stream_search called but not implemented");
        Err(Self::unsupported())
    }

    fn get_schema(_index: String, _config: Config) -> Result<String, Error> {
        trace!("get_schema called but not implemented");
        Err(Self::unsupported())
    }

    fn update_schema(_index: String, _schema_json: String, _config: Config) -> Result<(), Error> {
        trace!("update_schema called but not implemented");
        Err(Self::unsupported())
    }
}

type DurableOpenSearchComponent = golem_llm::durability::DurableComponent<OpenSearchComponent>;

golem_llm::export_llm!(DurableOpenSearchComponent with_types_in self::golem);