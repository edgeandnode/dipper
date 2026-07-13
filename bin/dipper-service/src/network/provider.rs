use reqwest::Url;
use thegraph_core::IndexerId;

use super::service;

/// An indexer.
pub struct Indexer {
    /// The indexer's ID (Eth address)
    pub id: IndexerId,
    /// The indexer's URL
    pub url: Url,
}

#[derive(Clone)]
pub struct NetworkProviderService {
    /// The indexer URLs service handle
    indexer_urls: service::indexer_urls::Handle,
}

impl NetworkProviderService {
    /// Creates a new network provider service instance.
    pub fn new(indexer_urls: service::indexer_urls::Handle) -> Self {
        Self { indexer_urls }
    }

    /// Get an indexer by its ID.
    pub fn get_indexer_by_id(&self, indexer_id: &IndexerId) -> Option<Indexer> {
        self.indexer_urls
            .get_indexer_url(indexer_id)
            .map(|url| Indexer {
                id: *indexer_id,
                url,
            })
    }
}
