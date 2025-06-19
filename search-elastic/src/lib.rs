use elasticsearch::{http::transport::Transport, DeleteParts, Elasticsearch, GetParts, IndexParts};
use elasticsearch::params::Refresh;
use futures::executor::block_on;
use serde_json::Value;
use std::env;
use std::error::Error;

/// Represents a document as defined in the `golem:search` WIT interface.
#[derive(Debug, Clone)]
pub struct Doc {
    pub id: String,
    /// A JSON encoded string representing the document contents.
    pub content: String,
}

/// A thin convenience wrapper around the official `elasticsearch-rs` client that
/// exposes synchronous helpers for basic CRUD operations required in task 1.3.
#[derive(Clone)]
pub struct ElasticSearchClient {
    client: Elasticsearch,
}

impl ElasticSearchClient {
    /// Creates a new client instance using the `SEARCH_PROVIDER_ENDPOINT` environment
    /// variable (defaults to `http://localhost:9200` when not present).
    pub fn new() -> Result<Self, Box<dyn Error>> {
        // Read the endpoint from the environment or fall back to localhost.
        let endpoint = env::var("SEARCH_PROVIDER_ENDPOINT")
            .unwrap_or_else(|_| "http://localhost:9200".to_string());

        let transport = Transport::single_node(&endpoint)?;
        Ok(Self {
            client: Elasticsearch::new(transport),
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
                .await?;

            let status = response.status_code();
            if !status.is_success() {
                let err_body = response.text().await.unwrap_or_default();
                return Err(format!(
                    "Failed to upsert document (status: {}): {}",
                    status.as_u16(),
                    err_body
                )
                .into());
            }
            Ok(())
        })
    }

    /// Retrieves a document by its ID. Returns `Ok(None)` when the document is
    /// not found.
    pub fn get_document(&self, index: &str, id: &str) -> Result<Option<Value>, Box<dyn Error>> {
        block_on(async {
            let response = self.client.get(GetParts::IndexId(index, id)).send().await?;
            let status = response.status_code();
            match status.as_u16() {
                200 => {
                    let json: Value = response.json().await?;
                    // The actual document source resides under the `_source` field.
                    Ok(json.get("_source").cloned())
                }
                404 => Ok(None),
                _ => {
                    let err_body = response.text().await.unwrap_or_default();
                    Err(format!(
                        "Failed to get document (status: {}): {}",
                        status.as_u16(),
                        err_body
                    )
                    .into())
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
                .await?;

            let status = response.status_code();
            match status.as_u16() {
                200 | 202 | 404 => Ok(()), // 404 means document not found -> fine for delete.
                _ => {
                    let err_body = response.text().await.unwrap_or_default();
                    Err(format!(
                        "Failed to delete document (status: {}): {}",
                        status.as_u16(),
                        err_body
                    )
                    .into())
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
}

// Placeholder module structure for the upcoming ElasticSearch component implementation.
// Full interface implementation will be added in future tasks.

pub fn init() {
    // Initialize component (logging, env vars, etc.)
}
