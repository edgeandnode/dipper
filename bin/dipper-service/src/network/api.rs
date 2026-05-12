use reqwest::Url;
use thegraph_core::IndexerId;

/// An indexer.
pub struct Indexer {
    /// The indexer's ID (Eth address)
    pub id: IndexerId,
    /// The indexer's URL
    pub url: Url,
}

/// The network provider
///
/// Provides a set of methods to interact with the network provider abstracting the
/// access to the Graph network snapshot.
pub trait NetworkProvider {
    /// Get an indexer by its ID.
    fn get_indexer_by_id(&self, indexer_id: &IndexerId) -> Option<Indexer>;
}
