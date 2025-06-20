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
    path: "../../search/wit",      // relative to this file at compile time
    world: "search-library",
    generate_all,
    generate_unused_types: true,
    additional_derives: [PartialEq, golem_rust::FromValueAndType, golem_rust::IntoValue],
    pub_export_macro: true,
});

use self::golem::search::search::{Config, Document, Error, ErrorCode, Guest, Hit};

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
    fn create_index(name: String, mapping_json: Option<String>, _config: Config) -> Result<(), Error> {
        let client = Self::client()?;
        let mapping = mapping_json.and_then(|s| serde_json::from_str(&s).ok());
        client.create_index(&name, mapping).map_err(|e| Self::map_error(crate::SearchError::Internal(e.to_string())))
    }

    fn delete_index(name: String, _config: Config) -> Result<(), Error> {
        let client = Self::client()?;
        client.delete_index(&name).map_err(|e| Self::map_error(crate::SearchError::Internal(e.to_string())))
    }

    fn upsert(index: String, doc: Document, _config: Config) -> Result<(), Error> {
        let client = Self::client()?;
        let internal_doc = Doc { id: doc.id.clone(), content: doc.json.clone() };
        client.upsert_document(&index, internal_doc).map_err(|e| Self::map_error(crate::SearchError::Internal(e.to_string())))
    }

    fn delete(index: String, id: String, _config: Config) -> Result<(), Error> {
        let client = Self::client()?;
        client.delete_document(&index, &id).map_err(|e| Self::map_error(crate::SearchError::Internal(e.to_string())))
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

    fn list_indexes(_config: Config) -> Result<Vec<String>, Error> {
        let client = Self::client()?;
        client
            .list_indices()
            .map_err(|e| Self::map_error(crate::SearchError::Internal(e.to_string())))
    }

    fn upsert_many(index: String, docs: Vec<Document>, _config: Config) -> Result<(), Error> {
        let client = Self::client()?;
        let internal_docs: Vec<Doc> = docs
            .into_iter()
            .map(|d| Doc { id: d.id, content: d.json })
            .collect();
        client
            .upsert_documents(&index, &internal_docs)
            .map_err(|e| Self::map_error(crate::SearchError::Internal(e.to_string())))
    }

    fn delete_many(index: String, ids: Vec<String>, _config: Config) -> Result<(), Error> {
        let client = Self::client()?;
        let id_refs: Vec<&str> = ids.iter().map(|s| s.as_str()).collect();
        client
            .delete_documents(&index, &id_refs)
            .map_err(|e| Self::map_error(crate::SearchError::Internal(e.to_string())))
    }

    fn get_schema(index: String, _config: Config) -> Result<String, Error> {
        let client = Self::client()?;
        client
            .get_schema(&index)
            .map(|v| v.to_string())
            .map_err(|e| Self::map_error(crate::SearchError::Internal(e.to_string())))
    }

    fn update_schema(index: String, schema_json: String, _config: Config) -> Result<(), Error> {
        let client = Self::client()?;
        let schema_value: serde_json::Value = serde_json::from_str(&schema_json).map_err(|e| Self::map_error(crate::SearchError::InvalidQuery(e.to_string())))?;
        client
            .update_schema(&index, schema_value)
            .map_err(|e| Self::map_error(crate::SearchError::Internal(e.to_string())))
    }

    fn stream_search(index: String, query: String, _config: Config) -> Result<Vec<Hit>, Error> {
        // Fallback implementation: fetch first 1000 documents using scroll
        let client = Self::client()?;
        let mut scroll = match client.start_scroll_search(&index, &query, 1000) {
            Ok(s) => s,
            Err(e) => return Err(Self::map_error(crate::SearchError::Internal(e.to_string()))),
        };
        let mut hits: Vec<Hit> = Vec::new();
        while let Ok(Some(batch)) = scroll.next_page() {
            for doc in batch {
                hits.push(Hit {
                    doc: Document { id: String::new(), json: doc.to_string() },
                    score: None,
                });
            }
        }
        Ok(hits)
    }

    // The rest of the interface defaults to "unsupported" for now. These will
    // be incrementally implemented in follow-up tasks.
}

type DurableElasticSearchComponent = golem_llm::durability::DurableComponent<ElasticSearchComponent>;

golem_llm::export_llm!(DurableElasticSearchComponent with_types_in self::golem);