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