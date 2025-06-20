use elasticsearch::{http::transport::Transport, DeleteParts, Elasticsearch, GetParts, IndexParts};
use elasticsearch::params::Refresh;
use futures::executor::block_on;
use serde_json::Value;
use std::env;
use std::error::Error;
use log::{debug, LevelFilter};
use thiserror::Error;
#[cfg(test)]
use httpmock::prelude::*;
use serde_json::json;

/// Represents a document as defined in the `golem:search` WIT interface.
#[derive(Debug, Clone)]
pub struct Doc {
    pub id: String,
    /// A JSON encoded string representing the document contents.
    pub content: String,
}

/// SearchError mirrors the `search-error` variant from the WIT definition and is
/// used as the local error representation until full component glue is
/// generated in later tasks.
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

/// A thin convenience wrapper around the official `elasticsearch-rs` client that
/// exposes synchronous helpers for basic CRUD operations required in task 1.3.
#[derive(Clone)]
pub struct ElasticSearchClient {
    client: Elasticsearch,
    timeout_secs: u64,
    max_retries: usize,
}

impl ElasticSearchClient {
    /// Creates a new client instance using the `SEARCH_PROVIDER_ENDPOINT` environment
    /// variable (defaults to `http://localhost:9200` when not present).
    pub fn new() -> Result<Self, Box<dyn Error>> {
        // Ensure the logger is initialised only once (subsequent calls are no-ops).
        init();

        // Common configuration parameters.
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

        // Provider-specific parameters (not fully wired yet, but parsed and logged).
        let cloud_id = env::var("ELASTIC_CLOUD_ID").ok();
        let password = env::var("ELASTIC_PASSWORD").ok();

        debug!(
            "ElasticSearch client config — endpoint: {}, timeout: {}s, retries: {}, cloud_id set: {}, password set: {}",
            endpoint,
            timeout_secs,
            max_retries,
            cloud_id.is_some(),
            password.is_some()
        );

        // NOTE: For the purposes of task 1.5 we keep the transport construction
        // simple. Advanced handling for Cloud ID and authentication will be
        // added in later tasks. The parsed values are stored so they can be
        // applied once a custom TransportBuilder is introduced.
        let transport = Transport::single_node(&endpoint)?;

        Ok(Self {
            client: Elasticsearch::new(transport),
            timeout_secs,
            max_retries,
        })
    }

    /// Creates or updates (upserts) a single document in the given index.
    ///
    /// Internally this maps the `doc.content` JSON string to the request body and
    /// performs an `index` operation with a fixed document id. If the document
    /// already exists it will be replaced; otherwise it will be created.
    pub fn upsert_document(&self, index: &str, doc: Doc) -> Result<(), Box<dyn Error>> {
        // Parse the JSON payload from the document content string.
        let json_body: Value = serde_json::from_str(&doc.content)?;

        block_on(async {
            let response = self
                .client
                .index(IndexParts::IndexId(index, &doc.id))
                .body(json_body)
                // `refresh=wait_for` is useful in tests to make the document immediately visible.
                .refresh(Refresh::WaitFor)
                .send()
                .await
                .map_err(|e| Box::<dyn Error>::from(e))?;

            let status = response.status_code();
            if !status.is_success() {
                let err_body = response.text().await.unwrap_or_default();
                return Err(map_status(status.as_u16(), &err_body).into());
            }
            Ok(())
        })
    }

    /// Retrieves a document by its ID. Returns `Ok(None)` when the document is
    /// not found.
    pub fn get_document(&self, index: &str, id: &str) -> Result<Option<Value>, Box<dyn Error>> {
        block_on(async {
            let response = self
                .client
                .get(GetParts::IndexId(index, id))
                .send()
                .await
                .map_err(|e| Box::<dyn Error>::from(e))?;
            let status = response.status_code();
            match status.as_u16() {
                200 => {
                    let json: Value = response
                        .json()
                        .await
                        .map_err(|e| Box::<dyn Error>::from(e))?;
                    // The actual document source resides under the `_source` field.
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

    /// Deletes a document by its ID. Deleting a non-existent document is treated
    /// as success (idempotent behaviour).
    pub fn delete_document(&self, index: &str, id: &str) -> Result<(), Box<dyn Error>> {
        block_on(async {
            let response = self
                .client
                .delete(DeleteParts::IndexId(index, id))
                // Use `refresh=wait_for` for deterministic behaviour in tests.
                .refresh(Refresh::WaitFor)
                .send()
                .await
                .map_err(|e| Box::<dyn Error>::from(e))?;

            let status = response.status_code();
            match status.as_u16() {
                200 | 202 | 404 => Ok(()), // 404 means document not found -> fine for delete.
                _ => {
                    let err_body = response.text().await.unwrap_or_default();
                    Err(map_status(status.as_u16(), &err_body).into())
                }
            }
        })
    }

    /// Creates a new index using the ElasticSearch Indices API. A caller may optionally
    /// provide a fully-formed ElasticSearch mappings/settings JSON payload. If `mapping`
    /// is `None` the index will be created with ElasticSearch defaults. Attempting to
    /// create an index that already exists results in an error from ElasticSearch which
    /// is forwarded to the caller.
    pub fn create_index(&self, index: &str, mapping: Option<Value>) -> Result<(), Box<dyn Error>> {
        block_on(async {
            use elasticsearch::indices::IndicesCreateParts;
            let response = if let Some(m) = mapping {
                self.client
                    .indices()
                    .create(IndicesCreateParts::Index(index))
                    .body(m)
                    .send()
                    .await
                    .map_err(|e| Box::<dyn Error>::from(e))?
            } else {
                self.client
                    .indices()
                    .create(IndicesCreateParts::Index(index))
                    .send()
                    .await
                    .map_err(|e| Box::<dyn Error>::from(e))?
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

    /// Deletes an index using the ElasticSearch Indices API. Deleting a non-existing
    /// index is treated as success to preserve idempotency.
    pub fn delete_index(&self, index: &str) -> Result<(), Box<dyn Error>> {
        block_on(async {
            use elasticsearch::indices::IndicesDeleteParts;
            let response = self
                .client
                .indices()
                .delete(IndicesDeleteParts::Index(&[index]))
                .send()
                .await
                .map_err(|e| Box::<dyn Error>::from(e))?;
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

// Retain the original dummy function and tests until they are replaced by
// comprehensive component tests in future tasks.
pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

pub fn init() {
    // Set up logging once using `SEARCH_PROVIDER_LOG_LEVEL` or default to INFO.
    let level = env::var("SEARCH_PROVIDER_LOG_LEVEL").unwrap_or_else(|_| "info".into());
    let level_filter = match level.to_ascii_lowercase().as_str() {
        "trace" => LevelFilter::Trace,
        "debug" => LevelFilter::Debug,
        "warn" => LevelFilter::Warn,
        "error" => LevelFilter::Error,
        _ => LevelFilter::Info,
    };

    // Ignore the result if the logger was already initialised (e.g. by tests).
    let _ = env_logger::Builder::from_default_env()
        .filter_level(level_filter)
        .try_init();
}

// Helper to map HTTP status codes (and optionally a response body snippet) to
// `SearchError` variants.
fn map_status(status: u16, body: &str) -> SearchError {
    match status {
        400 => SearchError::InvalidQuery(body.to_string()),
        404 => SearchError::IndexNotFound,
        408 | 504 => SearchError::Timeout,
        429 => SearchError::RateLimited,
        _ => SearchError::Internal(body.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use serial_test::serial;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }

    // These tests assume an ElasticSearch instance running on localhost:9200.
    // They are ignored by default as CI environments might not have ElasticSearch.
    // Run with `cargo test -- --ignored` to execute.

    #[ignore]
    #[test]
    fn upsert_get_delete_roundtrip() {
        std::env::set_var("SEARCH_PROVIDER_ENDPOINT", "http://localhost:9200");
        let client = ElasticSearchClient::new().expect("client");
        let index = "test_index_crud";
        let id = "1";
        let payload = json!({"title": "Test Document", "tags": ["rust", "wasm"]}).to_string();

        // Upsert
        client
            .upsert_document(index, Doc { id: id.to_string(), content: payload.clone() })
            .expect("upsert");

        // Get
        let retrieved = client
            .get_document(index, id)
            .expect("get")
            .expect("document should exist");
        assert_eq!(retrieved, serde_json::from_str::<Value>(&payload).unwrap());

        // Delete
        client.delete_document(index, id).expect("delete");

        // Ensure deletion
        let after_delete = client.get_document(index, id).expect("get after delete");
        assert!(after_delete.is_none());
    }

    #[ignore]
    #[test]
    fn create_delete_index_roundtrip() {
        std::env::set_var("SEARCH_PROVIDER_ENDPOINT", "http://localhost:9200");
        let client = ElasticSearchClient::new().expect("client");
        let index = "test_index_mgmt";

        // Ensure index doesn't exist (ignore potential errors).
        let _ = client.delete_index(index);

        // Create index (without explicit mapping).
        client.create_index(index, None).expect("create index");

        // Delete index.
        client.delete_index(index).expect("delete index");
    }

    // Existing env var and status mapping tests --------------------------------

    #[test]
    fn env_var_defaults_and_overrides() {
        use std::env;

        // Backup current env vars.
        let prev_timeout = env::var("SEARCH_PROVIDER_TIMEOUT").ok();
        let prev_retries = env::var("SEARCH_PROVIDER_MAX_RETRIES").ok();

        // Clear to test defaults.
        env::remove_var("SEARCH_PROVIDER_TIMEOUT");
        env::remove_var("SEARCH_PROVIDER_MAX_RETRIES");

        let client_default = ElasticSearchClient::new().expect("client");
        assert_eq!(client_default.timeout_secs, 30);
        assert_eq!(client_default.max_retries, 3);

        // Override values.
        env::set_var("SEARCH_PROVIDER_TIMEOUT", "42");
        env::set_var("SEARCH_PROVIDER_MAX_RETRIES", "7");

        let client_override = ElasticSearchClient::new().expect("client");
        assert_eq!(client_override.timeout_secs, 42);
        assert_eq!(client_override.max_retries, 7);

        // Restore original env vars.
        if let Some(v) = prev_timeout { env::set_var("SEARCH_PROVIDER_TIMEOUT", v); } else { env::remove_var("SEARCH_PROVIDER_TIMEOUT"); }
        if let Some(v) = prev_retries { env::set_var("SEARCH_PROVIDER_MAX_RETRIES", v); } else { env::remove_var("SEARCH_PROVIDER_MAX_RETRIES"); }
    }

    #[test]
    fn status_code_error_mapping() {
        use super::SearchError;

        // 400 -> InvalidQuery
        match super::map_status(400, "bad query") {
            SearchError::InvalidQuery(msg) => assert_eq!(msg, "bad query"),
            _ => panic!("expected InvalidQuery"),
        }

        // 404 -> IndexNotFound
        match super::map_status(404, "irrelevant") {
            SearchError::IndexNotFound => {},
            _ => panic!("expected IndexNotFound"),
        }

        // 429 -> RateLimited
        match super::map_status(429, "slow down") {
            SearchError::RateLimited => {},
            _ => panic!("expected RateLimited"),
        }
    }

    // New unit tests for Task 2.7 ------------------------------------------------

    /// Tests the `upsert_document`, `get_document`, and `delete_document` helpers
    /// without requiring a live ElasticSearch instance by mocking the HTTP layer.
    #[test]
    #[serial]
    fn upsert_get_delete_document_mock() {
        // Ensure a Tokio reactor is available during the synchronous helper calls.
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        let _guard = rt.enter();
        let server = MockServer::start();

        let index = "books";
        let id = "42";
        let doc_body = r#"{"title":"The Rust Book","tags":["rust","programming"]}"#;

        // Mock the INDEX (upsert) operation.
        let _m_upsert = server.mock(|when, then| {
            when.method(PUT)
                .path(format!("/{}/_doc/{}", index, id));
            then.status(201)
                .body(r#"{"result":"created"}"#);
        });

        // Some ElasticSearch client versions use POST instead of PUT for index
        // upsert operations. Provide an alternative mock to remain flexible.
        let _m_upsert_post = server.mock(|when, then| {
            when.method(POST)
                .path(format!("/{}/_doc/{}", index, id));
            then.status(201)
                .body(r#"{"result":"created"}"#);
        });

        // Mock the GET operation for an existing document.
        let _m_get_found = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/{}/_doc/{}", index, id));
            then.status(200)
                .body(format!("{{\"_source\":{}}}", doc_body));
        });

        // Mock DELETE operation.
        let _m_delete = server.mock(|when, then| {
            when.method(DELETE)
                .path(format!("/{}/_doc/{}", index, id));
            then.status(200)
                .body(r#"{"result":"deleted"}"#);
        });

        // Point the ElasticSearch client at the mock server.
        std::env::set_var("SEARCH_PROVIDER_ENDPOINT", &server.base_url());

        let client = ElasticSearchClient::new().expect("client creation");

        // Upsert
        client
            .upsert_document(index, Doc { id: id.to_string(), content: doc_body.to_string() })
            .expect("upsert document");

        // Retrieve and verify content
        let retrieved = client
            .get_document(index, id)
            .expect("get document")
            .expect("document exists");
        assert_eq!(retrieved, serde_json::from_str::<Value>(doc_body).unwrap());

        // Delete document
        client.delete_document(index, id).expect("delete document");

        // Validate that all mocks were hit as expected.
        assert_eq!(_m_upsert.hits() + _m_upsert_post.hits(), 1, "upsert mock should be hit exactly once");
        assert_eq!(_m_get_found.hits(), 1, "GET (found) mock should be hit once");
        _m_delete.assert();
    }

    /// Tests the `create_index` and `delete_index` helpers via mocked HTTP calls.
    #[test]
    #[serial]
    fn create_delete_index_mock() {
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        let _guard = rt.enter();
        let server = MockServer::start();
        let index = "unit_test_index";

        // Mock CREATE index
        let _m_create = server.mock(|when, then| {
            when.method(PUT).path(format!("/{}", index));
            then.status(200)
                .body(r#"{"acknowledged":true}"#);
        });

        // Mock DELETE index
        let _m_delete = server.mock(|when, then| {
            when.method(DELETE).path(format!("/{}", index));
            then.status(200)
                .body(r#"{"acknowledged":true}"#);
        });

        std::env::set_var("SEARCH_PROVIDER_ENDPOINT", &server.base_url());
        let client = ElasticSearchClient::new().expect("client creation");

        client.create_index(index, None).expect("create index");
        client.delete_index(index).expect("delete index");

        _m_create.assert();
        _m_delete.assert();
    }
}

// Placeholder module structure for the upcoming ElasticSearch component implementation.
// Full interface implementation will be added in future tasks.
