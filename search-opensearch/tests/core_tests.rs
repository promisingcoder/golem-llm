// Integration tests for the non-WASM OpenSearch core implementation.
// These tests spin up a lightweight HTTP mock server to validate that the
// client produces the expected HTTP requests and correctly handles basic
// success and error scenarios.

#![cfg(not(target_arch = "wasm32"))]

use httpmock::{Method::*, MockServer};
use search_opensearch::{Doc, OpenSearchClient};
use serde_json::json;
use std::env;

fn setup_client(server: &MockServer) -> OpenSearchClient {
    // Ensure all client instances created by the test talk to the mock server.
    env::set_var("SEARCH_PROVIDER_ENDPOINT", server.url(""));
    // Disable AWS auth to avoid SDK initialization overhead in unit tests.
    env::set_var("OPENSEARCH_AWS_AUTH", "false");

    OpenSearchClient::new().expect("client should initialise against mock server")
}

#[test]
fn create_and_delete_index_happy_path() {
    let server = MockServer::start();

    // Mock for PUT /test-index
    let create_mock = server.mock(|when, then| {
        when.method(PUT).path("/test-index");
        then.status(200);
    });

    // Mock for DELETE /test-index
    let delete_mock = server.mock(|when, then| {
        when.method(DELETE).path("/test-index");
        then.status(200);
    });

    let client = setup_client(&server);

    // create_index should succeed with HTTP 200 response.
    client
        .create_index("test-index", None)
        .expect("create_index should succeed");

    // delete_index should also succeed.
    client
        .delete_index("test-index")
        .expect("delete_index should succeed");

    create_mock.assert();
    delete_mock.assert();
}

#[test]
fn upsert_get_delete_document_happy_path() {
    let server = MockServer::start();
    let client = setup_client(&server);

    // Upsert mock – PUT /test-index/_doc/1
    let upsert_mock = server.mock(|when, then| {
        when.method(PUT).path("/test-index/_doc/1");
        then.status(201);
    });

    // Get mock – GET /test-index/_doc/1
    let body = json!({ "_source": { "field": "value" }});
    let get_mock = server.mock(|when, then| {
        when.method(GET).path("/test-index/_doc/1");
        then.status(200).json_body(body.clone());
    });

    // Delete mock – DELETE /test-index/_doc/1
    let delete_mock = server.mock(|when, then| {
        when.method(DELETE).path("/test-index/_doc/1");
        then.status(200);
    });

    let doc = Doc {
        id: "1".to_string(),
        content: json!({ "field": "value" }).to_string(),
    };

    // Upsert document should succeed.
    client
        .upsert_document("test-index", doc.clone())
        .expect("upsert_document should succeed");

    // Getting the document should yield the expected JSON body.
    let fetched = client
        .get_document("test-index", "1")
        .expect("get_document should succeed")
        .expect("document should exist");
    assert_eq!(fetched, json!({ "field": "value" }));

    // Deleting the document should succeed.
    client
        .delete_document("test-index", "1")
        .expect("delete_document should succeed");

    // Ensure mocks were hit at least once.
    upsert_mock.assert();
    get_mock.assert();
    delete_mock.assert();
}