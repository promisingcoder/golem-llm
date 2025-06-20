use elasticsearch::{http::transport::Transport, DeleteParts, Elasticsearch, GetParts, IndexParts, SearchParts};
use elasticsearch::params::Refresh;
use futures::executor::block_on;
use serde_json::Value;
use std::env;
use std::error::Error;
use log::{debug, LevelFilter};
use thiserror::Error;
use std::collections::HashMap;
use elasticsearch::http::{Method, request::JsonBody, response::Response};
use elasticsearch::http::headers::HeaderMap;
use std::time::Duration;
#[cfg(test)]
use testcontainers_modules::elastic_search::ElasticSearch as ElasticSearchContainer;
#[cfg(test)]
#[allow(unused_imports)]
use testcontainers_modules::testcontainers::{ImageExt, runners::SyncRunner};

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
#[derive(Error, Debug, Clone)]
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
    cloud_id: Option<String>,
    password: Option<String>,
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
            cloud_id,
            password,
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

    /// Upserts a batch of documents. This is currently implemented naïvely by
    /// delegating to `upsert_document` in a loop which is sufficient for test
    /// workloads. A future optimisation task will migrate this to the native
    /// ElasticSearch _bulk API for better performance and atomicity.
    pub fn upsert_documents(&self, index: &str, docs: &[Doc]) -> Result<(), Box<dyn Error>> {
        for doc in docs {
            self.upsert_document(index, doc.clone())?;
        }
        Ok(())
    }

    /// Deletes a batch of documents. Non-existing documents are ignored to keep
    /// the behaviour idempotent.
    pub fn delete_documents(&self, index: &str, ids: &[&str]) -> Result<(), Box<dyn Error>> {
        for id in ids {
            // Ignore individual errors for documents that disappear between
            // iterations so that the whole batch does not fail because of
            // racing deletes.
            let _ = self.delete_document(index, id);
        }
        Ok(())
    }

    /// Executes a simple full-text search with optional pagination. The query
    /// string is passed through ElasticSearch's `query_string` query which
    /// supports Lucene syntax. When `from`/`size` are not provided they default
    /// to ElasticSearch defaults (0/10).
    pub fn search_documents(
        &self,
        index: &str,
        query: &str,
        from: Option<u64>,
        size: Option<u64>,
    ) -> Result<Vec<Value>, Box<dyn Error>> {
        block_on(async {
            let mut body = serde_json::json!({
                "query": {
                    "query_string": { "query": query }
                }
            });
            if let Some(f) = from {
                body["from"] = serde_json::Value::from(f);
            }
            if let Some(s) = size {
                body["size"] = serde_json::Value::from(s);
            }

            let response = self
                .client
                .search(SearchParts::Index(&[index]))
                .body(body)
                .send()
                .await
                .map_err(|e| Box::<dyn Error>::from(e))?;

            let status = response.status_code();
            if !status.is_success() {
                let err_body = response.text().await.unwrap_or_default();
                return Err(map_status(status.as_u16(), &err_body).into());
            }

            let value: serde_json::Value = response.json().await?;
            let hits = value
                .get("hits")
                .and_then(|v| v.get("hits"))
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();

            // Extract the `_source` part from each hit.
            let docs: Vec<Value> = hits
                .into_iter()
                .filter_map(|hit| hit.get("_source").cloned())
                .collect();

            Ok(docs)
        })
    }

    /// Retrieves the schema for a given index.
    pub fn get_schema(&self, index: &str) -> Result<Value, Box<dyn Error>> {
        block_on(async {
            use elasticsearch::indices::IndicesGetMappingParts;
            let response = self
                .client
                .indices()
                .get_mapping(IndicesGetMappingParts::Index(&[index]))
                .send()
                .await?;
            let status = response.status_code();
            if !status.is_success() {
                let err_body = response.text().await.unwrap_or_default();
                return Err(map_status(status.as_u16(), &err_body).into());
            }
            let json: Value = response.json().await?;
            Ok(json)
        })
    }

    /// Advanced search supporting filter JSON, sorting, faceting and highlighting.
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
    ) -> Result<(Vec<Value>, Option<HashMap<String, Vec<String>>>, Option<Value>), Box<dyn Error>> {
        block_on(async {
            let mut body = serde_json::json!({
                "query": {
                    "query_string": { "query": query }
                }
            });
            if let Some(filter) = filter_json {
                body["post_filter"] = filter;
            }
            if let Some(sort) = sort_json {
                body["sort"] = sort;
            }
            if !highlight_fields.is_empty() {
                let fields: Value = highlight_fields
                    .iter()
                    .map(|f| (f.to_string(), serde_json::json!({})))
                    .collect::<serde_json::Map<_, _>>()
                    .into();
                body["highlight"] = serde_json::json!({ "fields": fields });
            }
            if !facet_fields.is_empty() {
                let mut aggs = serde_json::Map::new();
                for field in facet_fields {
                    aggs.insert(
                        format!("{field}_facet"),
                        serde_json::json!({
                            "terms": { "field": field }
                        }),
                    );
                }
                body["aggs"] = Value::Object(aggs);
            }
            if let Some(f) = from {
                body["from"] = Value::from(f);
            }
            if let Some(s) = size {
                body["size"] = Value::from(s);
            }

            let response = self
                .client
                .search(SearchParts::Index(&[index]))
                .body(body)
                .send()
                .await?;

            let status = response.status_code();
            if !status.is_success() {
                let err_body = response.text().await.unwrap_or_default();
                return Err(map_status(status.as_u16(), &err_body).into());
            }

            let value: Value = response.json().await?;
            let hits_arr = value
                .get("hits").and_then(|v| v.get("hits")).and_then(|v| v.as_array()).cloned()
                .unwrap_or_default();
            let mut highlights_map: HashMap<String, Vec<String>> = HashMap::new();
            let docs: Vec<Value> = hits_arr
                .into_iter()
                .filter_map(|hit| {
                    if let Some(obj) = hit.as_object() {
                        if let (Some(src), Some(id_val)) = (obj.get("_source"), obj.get("_id")) {
                            // Collect highlight
                            if let Some(highlight) = obj.get("highlight") {
                                let hl_vec = highlight
                                    .as_object()
                                    .unwrap_or(&serde_json::Map::new())
                                    .values()
                                    .flat_map(|v| v.as_array().unwrap_or(&Vec::new()).clone())
                                    .filter_map(|v| v.as_str().map(String::from))
                                    .collect::<Vec<String>>();
                                highlights_map.insert(id_val.as_str().unwrap_or_default().to_string(), hl_vec);
                            }
                            return Some(src.clone());
                        }
                    }
                    None
                })
                .collect();

            let facets = value.get("aggregations").cloned();

            Ok((docs, if highlights_map.is_empty() { None } else { Some(highlights_map) }, facets))
        })
    }

    /// Returns list of all indices in the cluster (names only).
    pub fn list_indices(&self) -> Result<Vec<String>, Box<dyn Error>> {
        block_on(async {
            let response: Response = self
                .client
                .transport()
                .send(
                    Method::Get,
                    "/_cat/indices?format=json",
                    HeaderMap::new(),
                    None::<&[(&str, &str)]>,
                    None::<&JsonBody<Value>>,
                    None::<Duration>,
                )
                .await?;
            let status = response.status_code();
            if !status.is_success() {
                let err_body = response.text().await.unwrap_or_default();
                return Err(map_status(status.as_u16(), &err_body).into());
            }
            let value: Value = response.json().await?;
            let indices = value
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|v| v.get("index").and_then(|i| i.as_str()).map(|s| s.to_string()))
                .collect();
            Ok(indices)
        })
    }

    /// Applies an updated mapping (schema) to an index using the put mapping API.
    pub fn update_schema(&self, index: &str, mapping: Value) -> Result<(), Box<dyn Error>> {
        block_on(async {
            use elasticsearch::indices::IndicesPutMappingParts;
            let response = self
                .client
                .indices()
                .put_mapping(IndicesPutMappingParts::Index(&[index]))
                .body(mapping)
                .send()
                .await?;
            let status = response.status_code();
            if !status.is_success() {
                let err_body = response.text().await.unwrap_or_default();
                return Err(map_status(status.as_u16(), &err_body).into());
            }
            Ok(())
        })
    }

    /// Starts a scrolling search returning a `SearchScroll` that can be used to
    /// iterate through result pages lazily. Uses ES scroll API under the hood.
    pub fn start_scroll_search(
        &self,
        index: &str,
        query: &str,
        page_size: u64,
    ) -> Result<SearchScroll, Box<dyn Error>> {
        block_on(async {
            let body = serde_json::json!({
                "size": page_size,
                "query": { "query_string": { "query": query } },
                "sort": ["_doc"]
            });
            let response = self
                .client
                .search(SearchParts::Index(&[index]))
                .scroll("1m")
                .body(body)
                .send()
                .await?;
            let status = response.status_code();
            if !status.is_success() {
                let err_body = response.text().await.unwrap_or_default();
                return Err(map_status(status.as_u16(), &err_body).into());
            }
            let value: Value = response.json().await?;
            let scroll_id = value["_scroll_id"].as_str().ok_or("missing scroll_id")?.to_string();
            let hits = value["hits"]["hits"].as_array().cloned().unwrap_or_default();
            Ok(SearchScroll {
                client: self.clone(),
                scroll_id,
                last_batch: Some(hits),
                finished: false,
            })
        })
    }
}

/// Represents a scrolling search session.
#[derive(Clone)]
pub struct SearchScroll {
    client: ElasticSearchClient,
    scroll_id: String,
    last_batch: Option<Vec<Value>>, // first batch already fetched
    finished: bool,
}

impl SearchScroll {
    /// Returns the next page of search hits. `Ok(None)` when finished.
    pub fn next_page(&mut self) -> Result<Option<Vec<Value>>, Box<dyn Error>> {
        if self.finished {
            return Ok(None);
        }
        if let Some(batch) = self.last_batch.take() {
            if batch.is_empty() {
                self.finished = true;
                self.clear_scroll();
                return Ok(None);
            }
            return Ok(Some(batch));
        }
        // fetch next batch via _search/scroll
        let scroll_id = self.scroll_id.clone();
        let client = self.client.clone();
        let result: Result<(Vec<Value>, String), Box<dyn Error>> = block_on(async move {
            let mut body_map = serde_json::Map::new();
            body_map.insert("scroll_id".into(), Value::String(scroll_id));
            let json_body = JsonBody::new(Value::Object(body_map));
            let response = client
                .client
                .transport()
                .send(
                    Method::Post,
                    "/_search/scroll",
                    HeaderMap::new(),
                    None::<&[(&str, &str)]>,
                    Some(&json_body),
                    None::<Duration>,
                )
                .await?;
            let status = response.status_code();
            if !status.is_success() {
                let err_body = response.text().await.unwrap_or_default();
                return Err(map_status(status.as_u16(), &err_body).into());
            }
            let value: Value = response.json().await?;
            let new_scroll_id = value["_scroll_id"].as_str().unwrap_or("").to_string();
            let hits = value["hits"]["hits"].as_array().cloned().unwrap_or_default();
            Ok((hits, new_scroll_id))
        });
        match result {
            Ok((hits, new_scroll_id)) => {
                self.scroll_id = new_scroll_id;
                if hits.is_empty() {
                    self.finished = true;
                    self.clear_scroll();
                    Ok(None)
                } else {
                    Ok(Some(hits))
                }
            }
            Err(e) => Err(e),
        }
    }

    fn clear_scroll(&self) {
        let scroll_id = self.scroll_id.clone();
        let client = self.client.clone();
        let _ = block_on(async move {
            let mut body_map = serde_json::Map::new();
            body_map.insert("scroll_id".into(), Value::String(scroll_id));
            let json_body = JsonBody::new(Value::Object(body_map));
            let _ = client
                .client
                .transport()
                .send(
                    Method::Delete,
                    "/_search/scroll",
                    HeaderMap::new(),
                    None::<&[(&str, &str)]>,
                    Some(&json_body),
                    None::<Duration>,
                )
                .await;
        });
    }

    /// Returns scroll id for persistence checkpoints.
    pub fn checkpoint(&self) -> String {
        self.scroll_id.clone()
    }

    /// Restores a scrolling search from a scroll id.
    pub fn from_checkpoint(client: ElasticSearchClient, scroll_id: String) -> Self {
        SearchScroll {
            client,
            scroll_id,
            last_batch: None,
            finished: false,
        }
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

    // These tests launch a real single-node Elasticsearch instance in a Docker
    // container using the `testcontainers-modules` crate. This means they can
    // run on any CI/host that has Docker available without requiring a
    // separately managed Elasticsearch service.
    //
    // Each test spins up its own container (which typically takes only a few
    // seconds) and configures the `SEARCH_PROVIDER_ENDPOINT` environment
    // variable so that the `ElasticSearchClient` under test talks to that
    // instance. When the test finishes the container is automatically
    // terminated and removed.
    //
    // NOTE: When Docker is not available these tests will fail to start. If you
    // need to temporarily disable them you can use Cargo's `--skip` flag.

    #[test]
    fn upsert_get_delete_roundtrip() {
        // Spin up a single-node Elasticsearch container.
        if !std::path::Path::new("/var/run/docker.sock").exists() {
            eprintln!("Docker not available – skipping upsert_get_delete_roundtrip");
            return;
        }
        let container = ElasticSearchContainer::default()
            .start()
            .expect("failed to start Elasticsearch container");
        let host_port = container
            .get_host_port_ipv4(9200)
            .expect("failed to get host port");
        let endpoint = format!("http://127.0.0.1:{host_port}");
        std::env::set_var("SEARCH_PROVIDER_ENDPOINT", &endpoint);

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

        // `container` is dropped here, shutting down Elasticsearch.
    }

    #[test]
    fn create_delete_index_roundtrip() {
        // Start fresh Elasticsearch container for this test.
        if !std::path::Path::new("/var/run/docker.sock").exists() {
            eprintln!("Docker not available – skipping create_delete_index_roundtrip");
            return;
        }
        let container = ElasticSearchContainer::default()
            .start()
            .expect("failed to start Elasticsearch container");
        let host_port = container
            .get_host_port_ipv4(9200)
            .expect("failed to get host port");
        let endpoint = format!("http://127.0.0.1:{host_port}");
        std::env::set_var("SEARCH_PROVIDER_ENDPOINT", &endpoint);

        let client = ElasticSearchClient::new().expect("client");
        let index = "test_index_mgmt";

        // Ensure index doesn't exist (ignore potential errors).
        let _ = client.delete_index(index);

        // Create index (without explicit mapping).
        client.create_index(index, None).expect("create index");

        // Delete index.
        client.delete_index(index).expect("delete index");

        // `container` drops here.
    }

    // New tests added for Task 1.5 ------------------------------------------

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
    fn cloud_id_and_password_env_vars() {
        use std::env;

        // Backup current vars
        let prev_cloud = env::var("ELASTIC_CLOUD_ID").ok();
        let prev_pwd = env::var("ELASTIC_PASSWORD").ok();

        env::set_var("ELASTIC_CLOUD_ID", "dummy-cloud-id");
        env::set_var("ELASTIC_PASSWORD", "secret");

        let client = ElasticSearchClient::new().expect("client");

        assert_eq!(client.cloud_id.as_deref(), Some("dummy-cloud-id"));
        assert_eq!(client.password.as_deref(), Some("secret"));

        // restore
        if let Some(v) = prev_cloud { env::set_var("ELASTIC_CLOUD_ID", v); } else { env::remove_var("ELASTIC_CLOUD_ID"); }
        if let Some(v) = prev_pwd { env::set_var("ELASTIC_PASSWORD", v); } else { env::remove_var("ELASTIC_PASSWORD"); }
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

    // New tests added for Task 1.6 — bulk operations and search ------------

    #[test]
    fn bulk_upsert_delete_and_search() {
        // Skip when Docker is unavailable.
        if !std::path::Path::new("/var/run/docker.sock").exists() {
            eprintln!("Docker not available – skipping bulk_upsert_delete_and_search");
            return;
        }
        let container = ElasticSearchContainer::default()
            .start()
            .expect("failed to start Elasticsearch container");
        let host_port = container
            .get_host_port_ipv4(9200)
            .expect("failed to get host port");
        let endpoint = format!("http://127.0.0.1:{host_port}");
        std::env::set_var("SEARCH_PROVIDER_ENDPOINT", &endpoint);

        let client = ElasticSearchClient::new().expect("client");
        let index = "test_bulk_ops";

        // Prepare documents.
        let docs: Vec<Doc> = (1..=5)
            .map(|i| Doc {
                id: i.to_string(),
                content: serde_json::json!({
                    "title": format!("Doc {i}"),
                    "category": if i % 2 == 0 { "even" } else { "odd" }
                })
                .to_string(),
            })
            .collect();

        // Bulk upsert.
        client
            .upsert_documents(index, &docs)
            .expect("bulk upsert");

        // Simple search for category:even.
        let results = client
            .search_documents(index, "category:even", None, Some(10))
            .expect("search");
        assert_eq!(results.len(), 2);

        // Bulk delete.
        let ids: Vec<&str> = docs.iter().map(|d| d.id.as_str()).collect();
        client
            .delete_documents(index, &ids)
            .expect("bulk delete");

        // Ensure documents gone.
        let after_delete = client
            .search_documents(index, "*", None, Some(10))
            .expect("search after delete");
        assert_eq!(after_delete.len(), 0);
    }

    #[test]
    fn schema_retrieval_and_faceted_highlight_search() {
        if !std::path::Path::new("/var/run/docker.sock").exists() {
            eprintln!("Docker not available – skipping schema_retrieval_and_faceted_highlight_search");
            return;
        }
        let container = ElasticSearchContainer::default()
            .start()
            .expect("failed to start Elasticsearch container");
        let host_port = container
            .get_host_port_ipv4(9200)
            .expect("failed to get host port");
        let endpoint = format!("http://127.0.0.1:{host_port}");
        std::env::set_var("SEARCH_PROVIDER_ENDPOINT", &endpoint);

        let client = ElasticSearchClient::new().expect("client");
        let index = "test_adv_search";

        // Define an explicit mapping with keyword field for category (needed for facets)
        let mapping = serde_json::json!({
            "mappings": {
                "properties": {
                    "title": { "type": "text" },
                    "category": { "type": "keyword" }
                }
            }
        });
        // Ensure index is fresh
        let _ = client.delete_index(index);
        client.create_index(index, Some(mapping.clone())).expect("create index");

        // Verify schema retrieval works
        let retrieved_schema = client.get_schema(index).expect("get schema");
        assert!(retrieved_schema.get(index).is_some(), "schema should contain index key");

        // Insert docs
        let docs: Vec<Doc> = (1..=4)
            .map(|i| Doc {
                id: i.to_string(),
                content: serde_json::json!({
                    "title": format!("Elasticsearch document {i}"),
                    "category": if i % 2 == 0 { "even" } else { "odd" }
                }).to_string(),
            })
            .collect();
        client.upsert_documents(index, &docs).expect("upsert docs");

        // Advanced search with highlight on title and facet on category
        let (hits, highlights_opt, facets_opt) = client
            .advanced_search(
                index,
                "Elasticsearch",
                None,
                None,
                &["title"],
                &["category"],
                None,
                Some(10),
            )
            .expect("advanced search");

        assert_eq!(hits.len(), 4);
        // Highlights present
        let highlights = highlights_opt.expect("highlights");
        assert_eq!(highlights.len(), 4);

        // Facets present with expected buckets
        let facets = facets_opt.expect("facets");
        let buckets = facets
            .get("category_facet")
            .and_then(|v| v.get("buckets"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        // Expect at least odd and even categories
        let mut seen = buckets
            .into_iter()
            .filter_map(|b| b.get("key").and_then(|k| k.as_str()).map(String::from))
            .collect::<Vec<String>>();
        seen.sort();
        assert_eq!(seen, vec!["even", "odd"]);
    }

    #[test]
    fn streaming_pagination_and_checkpoint() {
        if !std::path::Path::new("/var/run/docker.sock").exists() {
            eprintln!("Docker not available – skipping streaming_pagination_and_checkpoint");
            return;
        }
        let container = ElasticSearchContainer::default()
            .start()
            .expect("failed to start Elasticsearch container");
        let host_port = container
            .get_host_port_ipv4(9200)
            .expect("failed to get host port");
        let endpoint = format!("http://127.0.0.1:{host_port}");
        std::env::set_var("SEARCH_PROVIDER_ENDPOINT", &endpoint);

        let client = ElasticSearchClient::new().expect("client");
        let index = "test_stream_scroll";
        let _ = client.delete_index(index);
        client.create_index(index, None).expect("create index");

        // Insert 25 docs
        let docs: Vec<Doc> = (1..=25)
            .map(|i| Doc {
                id: i.to_string(),
                content: serde_json::json!({ "title": format!("Doc {i}") }).to_string(),
            })
            .collect();
        client.upsert_documents(index, &docs).expect("upsert");

        // Start scrolling search with page size 10
        let mut scroll = client
            .start_scroll_search(index, "Doc", 10)
            .expect("start scroll");

        let mut total = 0;
        // Fetch first page
        if let Some(batch1) = scroll.next_page().expect("next page 1") {
            total += batch1.len();
            assert_eq!(batch1.len(), 10);
        } else {
            panic!("expected first batch");
        }

        // Simulate checkpoint persistence & component restart by recreating SearchScroll
        let checkpoint = scroll.checkpoint();
        let mut restored = SearchScroll::from_checkpoint(client.clone(), checkpoint);

        // Continue fetching remaining pages via restored scroll
        while let Some(batch) = restored.next_page().expect("next page") {
            total += batch.len();
        }

        assert_eq!(total, 25);
    }
}

// Placeholder module structure for the upcoming ElasticSearch component implementation.
// Full interface implementation will be added in future tasks.

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

#[cfg(target_arch = "wasm32")]
mod component;
