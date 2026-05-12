use thegraph_core::IndexerId;

use super::{
    api::{Indexer, NetworkProvider},
    service,
};

#[derive(Clone)]
pub struct NetworkProviderService {
    /// The network provider topology service handler
    topology: service::topology::Handle,
}

impl NetworkProviderService {
    /// Creates a new network provider service instance.
    pub fn new(topology: service::topology::Handle) -> Self {
        Self { topology }
    }
}

impl NetworkProvider for NetworkProviderService {
    fn get_indexer_by_id(&self, indexer_id: &IndexerId) -> Option<Indexer> {
        self.topology
            .snapshot()
            .get_indexer(indexer_id)
            .map(|indexer| Indexer {
                id: indexer.id,
                url: indexer.url.clone(),
            })
    }
}
