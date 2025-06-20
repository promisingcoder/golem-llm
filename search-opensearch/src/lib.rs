//! Basic module skeleton for the OpenSearch search component.
//!
//! This file intentionally contains only the types and helpers required to
//! make the crate build for both the host (native) target and the `wasm32-wasi`
//! target used by Golem components. The actual `golem:search` interface
//! implementation will be fleshed out in follow-up tasks.

use opensearch::{http::transport::Transport, OpenSearch};
use std::env;
use std::error::Error;
use log::{debug, LevelFilter};
use thiserror::Error;
use serde_json::Value;

/// Initialise the global logger once. Subsequent calls are no-ops.
///
/// Even though logging is not strictly required at this stage, setting up the
/// logger early makes debugging the forthcoming implementation tasks easier.
pub fn init() {
    // Ignore the result because re-initialising the logger is harmless and will
    // simply return an error we can discard.
    let _ = env_logger::Builder::from_default_env()
        .filter_level(LevelFilter::Info)
        .is_test(cfg!(test))
        .try_init();
}

/// Mirror of the `search-error` variant from the WIT definition that will be
/// used once the interface bindings are generated. Keeping it here allows
/// other modules (tests, component stub) to reference a unified error type.
#[derive(Error, Debug)]
pub enum SearchError {
    #[error("index not found")]
    IndexNotFound,
    #[error("invalid query: {0}")]
    InvalidQuery(String),
    #[error("unsupported operation")]
    Unsupported,
    #[error("internal error: {0}")]
    Internal(String),
    #[error("timeout")]
    Timeout,
    #[error("rate limited")]
    RateLimited,
}

/// Represents a document as defined in the `golem:search` WIT interface.
#[derive(Debug, Clone)]
pub struct Doc {
    pub id: String,
    /// A JSON encoded string representing the document contents.
    pub content: String,
}

/// Thin wrapper around the official `opensearch-rs` client. Only the minimal
/// constructor is implemented for now – CRUD helpers will be added later.
#[derive(Clone)]
pub struct OpenSearchClient {
    pub client: OpenSearch,
    timeout_secs: u64,
    max_retries: usize,
}

impl OpenSearchClient {
    /// Creates a new client instance using common environment variables defined
    /// in the project-wide conventions.
    pub fn new() -> Result<Self, Box<dyn Error>> {
        init();

        // Common configuration parameters (shared with other providers).
        let endpoint = env::var("SEARCH_PROVIDER_ENDPOINT")
            .unwrap_or_else(|_| "http://localhost:9200".to_string());

        let timeout_secs: u64 = env::var("SEARCH_PROVIDER_TIMEOUT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(30);

        let max_retries: usize = env::var("SEARCH_PROVIDER_MAX_RETRIES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(3);

        debug!(
            "OpenSearch client config — endpoint: {}, timeout: {}s, retries: {}",
            endpoint, timeout_secs, max_retries
        );

        let transport = Transport::single_node(&endpoint)?;
        Ok(Self {
            client: OpenSearch::new(transport),
            timeout_secs,
            max_retries,
        })
    }

    /// Creates an index with an optional schema/mapping definition.
    #[allow(unused_variables)]
    pub fn create_index(&self, index: &str, mapping: Option<Value>) -> Result<(), Box<dyn Error>> {
        Err(Box::new(SearchError::Unsupported))
    }

    /// Deletes an index, succeeding even if the index does not exist.
    #[allow(unused_variables)]
    pub fn delete_index(&self, index: &str) -> Result<(), Box<dyn Error>> {
        Err(Box::new(SearchError::Unsupported))
    }

    /// Returns the names of all indices.
    pub fn list_indices(&self) -> Result<Vec<String>, Box<dyn Error>> {
        Err(Box::new(SearchError::Unsupported))
    }

    /// Upserts a single document.
    #[allow(unused_variables)]
    pub fn upsert_document(&self, index: &str, doc: Doc) -> Result<(), Box<dyn Error>> {
        Err(Box::new(SearchError::Unsupported))
    }

    /// Upserts a batch of documents.
    #[allow(unused_variables)]
    pub fn upsert_documents(&self, index: &str, docs: &[Doc]) -> Result<(), Box<dyn Error>> {
        Err(Box::new(SearchError::Unsupported))
    }

    /// Deletes a single document by id.
    #[allow(unused_variables)]
    pub fn delete_document(&self, index: &str, id: &str) -> Result<(), Box<dyn Error>> {
        Err(Box::new(SearchError::Unsupported))
    }

    /// Deletes a list of document ids.
    #[allow(unused_variables)]
    pub fn delete_documents(&self, index: &str, ids: &[&str]) -> Result<(), Box<dyn Error>> {
        Err(Box::new(SearchError::Unsupported))
    }

    /// Retrieves a single document by id.
    #[allow(unused_variables)]
    pub fn get_document(&self, index: &str, id: &str) -> Result<Option<Value>, Box<dyn Error>> {
        Err(Box::new(SearchError::Unsupported))
    }

    /// Executes a basic full-text search.
    #[allow(unused_variables)]
    pub fn search_documents(
        &self,
        index: &str,
        query: &str,
        from: Option<u64>,
        size: Option<u64>,
    ) -> Result<Vec<Value>, Box<dyn Error>> {
        Err(Box::new(SearchError::Unsupported))
    }

    /// Executes an advanced search that supports filters, sorting, highlights and facets.
    #[allow(unused_variables)]
    pub fn advanced_search(
        &self,
        index: &str,
        query: &str,
        filter_json: Option<Value>,
        sort_json: Option<Value>,
        highlight_fields: &[&str],
        facet_fields: &[&str],
        from: Option<u64>,
        size: Option<u64>,
    ) -> Result<(Vec<Value>, Option<Value>, Option<Value>), Box<dyn Error>> {
        Err(Box::new(SearchError::Unsupported))
    }

    /// Retrieves the raw schema (mapping) of an index.
    #[allow(unused_variables)]
    pub fn get_schema(&self, index: &str) -> Result<Value, Box<dyn Error>> {
        Err(Box::new(SearchError::Unsupported))
    }

    /// Updates (merges) the schema for an existing index.
    #[allow(unused_variables)]
    pub fn update_schema(&self, index: &str, mapping: Value) -> Result<(), Box<dyn Error>> {
        Err(Box::new(SearchError::Unsupported))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_initialises_with_defaults() {
        // Ensure no env vars are set to test default fallbacks.
        env::remove_var("SEARCH_PROVIDER_ENDPOINT");
        env::remove_var("SEARCH_PROVIDER_TIMEOUT");
        env::remove_var("SEARCH_PROVIDER_MAX_RETRIES");

        let client = OpenSearchClient::new().expect("client should initialise");
        assert_eq!(client.timeout_secs, 30);
        assert_eq!(client.max_retries, 3);
    }
}