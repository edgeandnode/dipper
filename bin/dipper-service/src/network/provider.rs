use thegraph_core::{alloy::primitives::Address, DeploymentId, IndexerId};

use super::{api::NetworkProvider, service::ServiceHandle};
use crate::network::api::Indexer;

#[derive(Clone)]
pub struct NetworkProviderService {
    /// The network provider service handler.
    inner: ServiceHandle,
}

impl NetworkProviderService {
    /// Creates a new network provider service instance.
    pub fn new(inner: ServiceHandle) -> Self {
        Self { inner }
    }
}

impl NetworkProvider for NetworkProviderService {
    fn get_indexer_by_id(&self, indexer_id: &IndexerId) -> Option<Indexer> {
        self.inner
            .snapshot()
            .get_indexer(indexer_id)
            .map(|indexer| Indexer {
                id: indexer.id,
                url: indexer.url.clone(),
            })
    }

    fn get_indexers_not_indexing_a_deployment_id(
        &self,
        deployment_id: &DeploymentId,
    ) -> Vec<Indexer> {
        self.inner
            .snapshot()
            .indexers_iter()
            .filter(|indexer| !indexer.indexings.contains(deployment_id))
            .map(|indexer| Indexer {
                id: indexer.id,
                url: indexer.url.clone(),
            })
            .collect()
    }

    fn get_indexer_id_for_operator_address(
        &self,
        _operator_address: &Address,
    ) -> Option<IndexerId> {
        todo!()
    }
}
