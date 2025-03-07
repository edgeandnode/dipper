use std::collections::BTreeSet;

use thegraph_core::{AllocationId, DeploymentId, IndexerId, alloy::primitives::Address};

use super::{
    Allocation,
    api::{Deployment, Indexer, NetworkProvider},
    service,
};

#[derive(Clone)]
pub struct NetworkProviderService {
    /// The network provider epoch service handler
    epoch: service::epoch::Handle,

    /// The network provider topology service handler
    topology: service::topology::Handle,

    /// The network provider service handler.
    ///
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
        epoch: service::epoch::Handle,
        topology: service::topology::Handle,
        allowlist: impl Into<BTreeSet<IndexerId>>,
    ) -> Self {
        Self {
            epoch,
            topology,
            allowlist: allowlist.into(),
        }
    }
}

impl NetworkProvider for NetworkProviderService {
    fn get_deployment_by_id(&self, deployment_id: &DeploymentId) -> Option<Deployment> {
        self.topology
            .snapshot()
            .get_deployment(deployment_id)
            .map(|_| Deployment {})
    }

    fn get_allocation_by_id(&self, allocation_id: &AllocationId) -> Option<Allocation> {
        self.topology
            .snapshot()
            .get_allocation(allocation_id)
            .map(|allocation| Allocation {
                id: allocation.id,
                opened_at: allocation.created_at,
                closed_at: allocation.closed_at,
                indexer_id: allocation.indexer,
                deployment_id: allocation.deployment,
                subgraph_id: allocation.subgraph,
                allocated_tokens: allocation.allocated_tokens,
                proof_of_indexing: allocation.proof_of_indexing,
            })
    }

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

    fn get_indexer_id_for_operator_address(&self, operator_address: &Address) -> Option<IndexerId> {
        self.topology
            .snapshot()
            .indexers_iter()
            .find(|indexer| indexer.operators.contains(operator_address))
            .map(|indexer| indexer.id)
    }

    fn get_current_epoch(&self) -> u32 {
        self.epoch.snapshot().epoch().number
    }
}
