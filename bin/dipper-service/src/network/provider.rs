use thegraph_core::{alloy::primitives::Address, DeploymentId, IndexerId};

use super::{
    api::{Deployment, Indexer, NetworkProvider},
    service::Handle,
};

#[derive(Clone)]
pub struct NetworkProviderService {
    /// The network provider service handler.
    inner: Handle,
}

impl NetworkProviderService {
    /// Creates a new network provider service instance.
    pub fn new(inner: Handle) -> Self {
        Self { inner }
    }
}

impl NetworkProvider for NetworkProviderService {
    fn get_deployment_by_id(&self, deployment_id: &DeploymentId) -> Option<Deployment> {
        self.inner
            .snapshot()
            .get_deployment(deployment_id)
            .map(|_deployment| Deployment {})
    }

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

    fn get_indexer_id_for_operator_address(&self, operator_address: &Address) -> Option<IndexerId> {
        self.inner
            .snapshot()
            .indexers_iter()
            .find(|indexer| indexer.operators.contains(operator_address))
            .map(|indexer| indexer.id)
    }
}
