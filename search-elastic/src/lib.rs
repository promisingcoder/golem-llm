use golem_search::golem::search::core::{self as search_core};

// A minimal placeholder implementation of the `golem:search/core` interface for ElasticSearch.
// All functions currently return `search-error.unsupported` or sensible empty values.
// This is a compile-time stub that satisfies the interface contract so that
// further provider-specific logic can be implemented incrementally.
struct ElasticSearchComponent;

// Helper alias to make the code less verbose
use search_core::{SearchError as Error, Schema, Doc, SearchQuery, SearchResults, IndexName, DocumentId, SearchHit};

impl search_core::Guest for ElasticSearchComponent {
    // Index lifecycle
    fn create_index(_name: IndexName, _schema: Option<Schema>) -> Result<(), Error> {
        Err(Error::Unsupported)
    }

    fn delete_index(_name: IndexName) -> Result<(), Error> {
        Err(Error::Unsupported)
    }

    fn list_indexes() -> Result<Vec<IndexName>, Error> {
        Err(Error::Unsupported)
    }

    // Document operations
    fn upsert(_index: IndexName, _doc: Doc) -> Result<(), Error> {
        Err(Error::Unsupported)
    }

    fn upsert_many(_index: IndexName, _docs: Vec<Doc>) -> Result<(), Error> {
        Err(Error::Unsupported)
    }

    fn delete(_index: IndexName, _id: DocumentId) -> Result<(), Error> {
        Err(Error::Unsupported)
    }

    fn delete_many(_index: IndexName, _ids: Vec<DocumentId>) -> Result<(), Error> {
        Err(Error::Unsupported)
    }

    fn get(_index: IndexName, _id: DocumentId) -> Result<Option<Doc>, Error> {
        Err(Error::Unsupported)
    }

    // Query operations
    fn search(_index: IndexName, _query: SearchQuery) -> Result<SearchResults, Error> {
        Err(Error::Unsupported)
    }

    fn stream_search(_index: IndexName, _query: SearchQuery) -> Result<search_core::StreamSearch, Error> {
        // Currently streaming search is not supported in the placeholder implementation.
        Err(Error::Unsupported)
    }

    // Schema inspection
    fn get_schema(_index: IndexName) -> Result<Schema, Error> {
        Err(Error::Unsupported)
    }

    fn update_schema(_index: IndexName, _schema: Schema) -> Result<(), Error> {
        Err(Error::Unsupported)
    }
}

type DurableElasticComponent = ElasticSearchComponent;

// Expose the component to the host using the helper macro generated in the
// `golem-search` crate. This registers the `ElasticSearchComponent` as the
// guest implementation for the `search-library` world.
golem_search::export_search!(DurableElasticComponent with_types_in golem_search);