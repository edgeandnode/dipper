use std::collections::BTreeSet;

use thegraph_core::{DeploymentId, IndexerId, alloy::primitives::Address};

use super::{
    api::{Indexer, NetworkProvider},
    service,
};

#[derive(Clone)]
pub struct NetworkProviderService {
    /// The network provider topology service handler
    topology: service::topology::Handle,

    /// The indexers allowlist.
    ///
    /// This list contains all the indexers that are allowed to interact with the
    /// DIPs Gateway. If the indexer is not contained in this list, it must not be
    /// considered as candidate. If the list is empty, all indexers are allowed.
    allowlist: BTreeSet<IndexerId>,
}

impl NetworkProviderService {
    /// Creates a new network provider service instance.
    pub fn new(
        topology: service::topology::Handle,
        allowlist: impl Into<BTreeSet<IndexerId>>,
    ) -> Self {
        Self {
            topology,
            allowlist: allowlist.into(),
        }
    }
}

impl NetworkProvider for NetworkProviderService {
    fn get_indexers_not_indexing_a_deployment_id(
        &self,
        deployment_id: &DeploymentId,
    ) -> Vec<Indexer> {
        self.topology
            .snapshot()
            .indexers_iter()
            // Filter out indexers that are not in the allowlist
            .filter(|indexer| self.allowlist.is_empty() || self.allowlist.contains(&indexer.id))
            // Filter out indexers that are already indexing the deployment
            .filter(|indexer| !indexer.indexings.contains(deployment_id))
            .map(|indexer| Indexer {
                id: indexer.id,
                url: indexer.url.clone(),
            })
            .collect()
    }

    fn get_indexer_by_id(&self, indexer_id: &IndexerId) -> Option<Indexer> {
        self.topology
            .snapshot()
            .get_indexer(indexer_id)
            .map(|indexer| Indexer {
                id: indexer.id,
                url: indexer.url.clone(),
            })
    }

    fn get_indexer_id_for_operator_address(&self, operator_address: &Address) -> Option<IndexerId> {
        self.topology
            .snapshot()
            .indexers_iter()
            .find(|indexer| indexer.operators.contains(operator_address))
            .map(|indexer| indexer.id)
    }
}
