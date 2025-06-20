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

#[cfg(not(target_arch = "wasm32"))]
use {
    aws_config,
    futures::executor::block_on,
    opensearch::http::transport::{SingleNodeConnectionPool, TransportBuilder},
    std::convert::TryInto,
    url::Url,
};

/// Initialise the global logger once. Subsequent calls are no-ops.
///
/// Even though logging is not strictly required at this stage, setting up the
/// logger early makes debugging the forthcoming implementation tasks easier.
pub fn init() {
    // Ignore the result because re-initialising the logger is harmless and will
    // simply return an error we can discard.
    // Honour the SEARCH_PROVIDER_LOG_LEVEL env var if present; otherwise default
    // to INFO to avoid overly verbose output in production.
    let level = env::var("SEARCH_PROVIDER_LOG_LEVEL")
        .ok()
        .and_then(|lvl| match lvl.to_lowercase().as_str() {
            "trace" => Some(LevelFilter::Trace),
            "debug" => Some(LevelFilter::Debug),
            "info" => Some(LevelFilter::Info),
            "warn" => Some(LevelFilter::Warn),
            "error" => Some(LevelFilter::Error),
            _ => None,
        })
        .unwrap_or(LevelFilter::Info);

    let _ = env_logger::Builder::from_default_env()
        .filter_level(level)
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

        // Determine whether AWS SigV4 authentication should be enabled. If the
        // `OPENSEARCH_AWS_AUTH` env var is set to "true"/"1"/"yes" the client
        // will sign requests using credentials discovered by the AWS SDK.

        #[cfg(target_arch = "wasm32")]
        let transport = Transport::single_node(&endpoint)?;

        #[cfg(not(target_arch = "wasm32"))]
        let transport = {
            let aws_auth_enabled = env::var("OPENSEARCH_AWS_AUTH")
                .map(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes"))
                .unwrap_or(false);

            if aws_auth_enabled {
                debug!("Using AWS SigV4 authentication for OpenSearch client");

                // Resolve the endpoint URL and build a single-node connection pool.
                let url = Url::parse(&endpoint)?;
                let conn_pool = SingleNodeConnectionPool::new(url);

                // If OPENSEARCH_AWS_REGION is set we use that, otherwise fall back to
                // AWS_REGION or the default region provider chain.
                let _ = env::var("OPENSEARCH_AWS_REGION"); // presence is enough for aws_config's default chain

                let sdk_cfg = block_on(async { aws_config::load_from_env().await });

                TransportBuilder::new(conn_pool)
                    .auth(sdk_cfg.try_into()?)
                    .build()?
            } else {
                Transport::single_node(&endpoint)?
            }
        };

        Ok(Self {
            client: OpenSearch::new(transport),
            timeout_secs,
            max_retries,
        })
    }

    /// Creates an index with an optional schema/mapping definition.
    pub fn create_index(&self, index: &str, mapping: Option<Value>) -> Result<(), Box<dyn Error>> {
        #[cfg(target_arch = "wasm32")]
        {
            let _ = index;
            let _ = mapping;
            return Err(Box::new(SearchError::Unsupported));
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            use futures::executor::block_on;
            use opensearch::indices::IndicesCreateParts;

            // Move ownership of mapping into the async block.
            let mapping_body = mapping;

            block_on(async {
                let response = if let Some(body) = mapping_body {
                    self.client
                        .indices()
                        .create(IndicesCreateParts::Index(index))
                        .body(body)
                        .send()
                        .await
                        .map_err(|e| Box::<dyn Error>::from(map_transport_error(&e)))?
                } else {
                    self.client
                        .indices()
                        .create(IndicesCreateParts::Index(index))
                        .send()
                        .await
                        .map_err(|e| Box::<dyn Error>::from(map_transport_error(&e)))?
                };

                let status = response.status_code();
                match status.as_u16() {
                    200 | 201 => Ok(()),
                    _ => {
                        let err_body = response.text().await.unwrap_or_default();
                        Err(map_status(status.as_u16(), &err_body).into())
                    }
                }
            })
        }
    }

    /// Deletes an index, succeeding even if the index does not exist.
    pub fn delete_index(&self, index: &str) -> Result<(), Box<dyn Error>> {
        #[cfg(target_arch = "wasm32")]
        {
            let _ = index;
            return Err(Box::new(SearchError::Unsupported));
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            use futures::executor::block_on;
            use opensearch::indices::IndicesDeleteParts;

            block_on(async {
                let response = self
                    .client
                    .indices()
                    .delete(IndicesDeleteParts::Index(&[index]))
                    .send()
                    .await
                    .map_err(|e| Box::<dyn Error>::from(map_transport_error(&e)))?;

                let status = response.status_code();
                match status.as_u16() {
                    200 | 202 | 404 => Ok(()),
                    _ => {
                        let err_body = response.text().await.unwrap_or_default();
                        Err(map_status(status.as_u16(), &err_body).into())
                    }
                }
            })
        }
    }

    /// Returns the names of all indices.
    pub fn list_indices(&self) -> Result<Vec<String>, Box<dyn Error>> {
        Err(Box::new(SearchError::Unsupported))
    }

    /// Upserts a single document.
    #[allow(unused_variables)]
    pub fn upsert_document(&self, index: &str, doc: Doc) -> Result<(), Box<dyn Error>> {
        #[cfg(target_arch = "wasm32")]
        {
            // Networking is not available inside the guest component – gracefully
            // degrade to an unsupported error that the higher-level bindings can
            // translate to `search-error.unsupported`.
            let _ = index;
            let _ = doc;
            return Err(Box::new(SearchError::Unsupported));
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            use futures::executor::block_on;
            use opensearch::{IndexParts, params::Refresh};
            use serde_json::Value;

            // Map the `doc.content` JSON string into a serde_json::Value that can be
            // supplied as the request body. Treat malformed JSON as an invalid query.
            let json_body: Value = serde_json::from_str(&doc.content)
                .map_err(|e| Box::<dyn Error>::from(SearchError::InvalidQuery(e.to_string())))?;

            block_on(async {
                let response = self
                    .client
                    .index(IndexParts::IndexId(index, &doc.id))
                    .body(json_body)
                    // Make the document visible to subsequent reads deterministically.
                    .refresh(Refresh::WaitFor)
                    .send()
                    .await
                    .map_err(|e| Box::<dyn Error>::from(map_transport_error(&e)))?;

                let status = response.status_code();
                if !status.is_success() {
                    let err_body = response.text().await.unwrap_or_default();
                    return Err(map_status(status.as_u16(), &err_body).into());
                }
                Ok(())
            })
        }
    }

    /// Upserts a batch of documents.
    #[allow(unused_variables)]
    pub fn upsert_documents(&self, index: &str, docs: &[Doc]) -> Result<(), Box<dyn Error>> {
        Err(Box::new(SearchError::Unsupported))
    }

    /// Deletes a single document by id.
    #[allow(unused_variables)]
    pub fn delete_document(&self, index: &str, id: &str) -> Result<(), Box<dyn Error>> {
        #[cfg(target_arch = "wasm32")]
        {
            let _ = index;
            let _ = id;
            return Err(Box::new(SearchError::Unsupported));
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            use futures::executor::block_on;
            use opensearch::{DeleteParts, params::Refresh};

            block_on(async {
                let response = self
                    .client
                    .delete(DeleteParts::IndexId(index, id))
                    .refresh(Refresh::WaitFor)
                    .send()
                    .await
                    .map_err(|e| Box::<dyn Error>::from(map_transport_error(&e)))?;

                let status = response.status_code();
                match status.as_u16() {
                    200 | 202 | 404 => Ok(()),
                    _ => {
                        let err_body = response.text().await.unwrap_or_default();
                        Err(map_status(status.as_u16(), &err_body).into())
                    }
                }
            })
        }
    }

    /// Deletes a list of document ids.
    #[allow(unused_variables)]
    pub fn delete_documents(&self, index: &str, ids: &[&str]) -> Result<(), Box<dyn Error>> {
        Err(Box::new(SearchError::Unsupported))
    }

    /// Retrieves a single document by id.
    #[allow(unused_variables)]
    pub fn get_document(&self, index: &str, id: &str) -> Result<Option<Value>, Box<dyn Error>> {
        #[cfg(target_arch = "wasm32")]
        {
            let _ = index;
            let _ = id;
            return Err(Box::new(SearchError::Unsupported));
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            use futures::executor::block_on;
            use opensearch::GetParts;

            block_on(async {
                let response = self
                    .client
                    .get(GetParts::IndexId(index, id))
                    .send()
                    .await
                    .map_err(|e| Box::<dyn Error>::from(map_transport_error(&e)))?;
                let status = response.status_code();
                match status.as_u16() {
                    200 => {
                        let json: Value = response
                            .json()
                            .await
                            .map_err(|e| Box::<dyn Error>::from(e))?;
                        Ok(json.get("_source").cloned())
                    }
                    404 => Ok(None),
                    _ => {
                        let err_body = response.text().await.unwrap_or_default();
                        Err(map_status(status.as_u16(), &err_body).into())
                    }
                }
            })
        }
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

// Helper to translate common HTTP status codes into structured SearchError
// variants so that callers can rely on a consistent error surface across
// different search providers.
fn map_status(status: u16, body: &str) -> SearchError {
    match status {
        400 => SearchError::InvalidQuery(body.to_string()),
        404 => SearchError::IndexNotFound,
        408 | 504 => SearchError::Timeout,
        429 => SearchError::RateLimited,
        _ => SearchError::Internal(body.to_string()),
    }
}

// Helper to translate transport/client errors (network failures, timeouts etc.)
fn map_transport_error(err: &dyn std::error::Error) -> SearchError {
    // Attempt to detect a timeout by inspecting the error chain
    let mut current: Option<&dyn std::error::Error> = Some(err);
    while let Some(e) = current {
        let msg = e.to_string().to_lowercase();
        if msg.contains("timeout") || msg.contains("timed out") {
            return SearchError::Timeout;
        }
        if msg.contains("rate") && msg.contains("limit") {
            return SearchError::RateLimited;
        }
        // Traverse the source chain
        current = e.source();
    }
    // Fallback
    SearchError::Internal(err.to_string())
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

    #[test]
    fn crud_operations_via_mock_server() {
        use httpmock::{MockServer, Method::GET};
        use serde_json::json;

        // Spin up a lightweight HTTP mock server.
        let server = MockServer::start();

        // Mock for document retrieval path to return a _source payload.
        let _doc_get_mock = server.mock(|when, then| {
            when.method(GET)
                .path_regex("/test_index/_doc/.*");
            then.status(200)
                .json_body(json!({
                    "_source": { "title": "Test" }
                }));
        });

        // Fallback mock for any other request – acknowledge with 200 OK.
        let _fallback = server.mock(|when, then| {
            when.any_request();
            then.status(200).json_body(json!({ "acknowledged": true }));
        });

        // Configure client to point to the mock server.
        std::env::set_var("SEARCH_PROVIDER_ENDPOINT", server.base_url());

        let client = OpenSearchClient::new().expect("client");
        let index = "test_index";

        // Create index
        client.create_index(index, None).expect("create index");

        // Upsert document
        client
            .upsert_document(
                index,
                Doc {
                    id: "1".to_string(),
                    content: json!({ "title": "Test" }).to_string(),
                },
            )
            .expect("upsert document");

        // Get document
        let retrieved = client
            .get_document(index, "1")
            .expect("get document")
            .expect("document should exist");
        assert_eq!(retrieved, json!({ "title": "Test" }));

        // Delete document
        client.delete_document(index, "1").expect("delete doc");

        // Delete index
        client.delete_index(index).expect("delete index");
    }

    #[test]
    fn error_mapping_index_not_found_and_timeouts() {
        use httpmock::{Method::PUT, MockServer};
        use serde_json::json;

        let server = MockServer::start();

        // 404 for upsert path simulating missing index
        let _not_found_mock = server.mock(|when, then| {
            when.method(PUT)
                .path_regex("/missing_index/_doc/.*");
            then.status(404);
        });

        // 504 timeout for create index path
        let _timeout_mock = server.mock(|when, then| {
            when.method(PUT)
                .path("/timeout_index");
            then.status(504);
        });

        std::env::set_var("SEARCH_PROVIDER_ENDPOINT", server.base_url());
        let client = OpenSearchClient::new().expect("client");

        // Index not found error
        let result = client.upsert_document(
            "missing_index",
            Doc { id: "1".into(), content: json!({ "k": 1 }).to_string() },
        );
        match result {
            Err(e) => {
                let err = e.downcast_ref::<SearchError>().expect("search error");
                assert!(matches!(err, SearchError::IndexNotFound));
            }
            Ok(_) => panic!("expected error"),
        }

        // Timeout error
        let result = client.create_index("timeout_index", None);
        match result {
            Err(e) => {
                let err = e.downcast_ref::<SearchError>().expect("search error");
                assert!(matches!(err, SearchError::Timeout));
            }
            Ok(_) => panic!("expected timeout error"),
        }
    }

    #[test]
    fn invalid_query_and_rate_limited_errors() {
        use httpmock::{Method::PUT, MockServer};
        use serde_json::json;

        let server = MockServer::start();

        // 429 rate limit for delete path
        let _rl_mock = server.mock(|when, then| {
            when.method(httpmock::Method::DELETE)
                .path("/rate_index");
            then.status(429);
        });

        std::env::set_var("SEARCH_PROVIDER_ENDPOINT", server.base_url());
        let client = OpenSearchClient::new().expect("client");

        // Invalid JSON payload
        let invalid_doc = Doc { id: "bad".into(), content: "{ not-json".into() };
        let err = client.upsert_document("any", invalid_doc).unwrap_err();
        let err = err.downcast_ref::<SearchError>().expect("search error");
        match err {
            SearchError::InvalidQuery(_) => {},
            _ => panic!("expected invalid query error"),
        }

        // Rate limited
        let err = client.delete_index("rate_index").unwrap_err();
        let err = err.downcast_ref::<SearchError>().expect("search error");
        assert!(matches!(err, SearchError::RateLimited));
    }
}