use std::collections::{BTreeMap, BTreeSet};

use reqwest::Url;
use thegraph_core::{
    alloy::primitives::{Address, BlockNumber},
    AllocationId, DeploymentId, IndexerId, ProofOfIndexing, SubgraphId,
};

use super::{epoches, indexer_operators, indexer_subgraphs};

/// A snapshot of the network state at a given point in time
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// The current network epoch
    ///
    /// See [Epoch] for more information
    epoch: Epoch,
    /// The indexers table
    ///
    /// See [Indexer] for more information
    indexers: BTreeMap<IndexerId, Indexer>,
    /// The subgraphs table
    ///
    /// See [Subgraph] for more information
    subgraphs: BTreeMap<SubgraphId, Subgraph>,
    /// The deployments table
    ///
    /// See [Deployment] for more information
    deployments: BTreeMap<DeploymentId, Deployment>,
    /// The active allocations table
    ///
    /// See [Allocation] for more information
    allocations: BTreeMap<AllocationId, Allocation>,
}

impl Snapshot {
    /// Create a new empty network snapshot with the current timestamp
    pub fn new(epoch: epoches::types::Epoch) -> Self {
        Self {
            epoch: Epoch {
                number: epoch.id.0,
                start_block: epoch.start_block,
                end_block: epoch.end_block,
            },
            indexers: Default::default(),
            subgraphs: Default::default(),
            deployments: Default::default(),
            allocations: Default::default(),
        }
    }

    /// Get the current network epoch
    pub fn epoch(&self) -> &Epoch {
        &self.epoch
    }

    /// Get an iterator over the indexers in the network snapshot
    ///
    /// As the indexers are stored in a BTreeMap-based table, the iterator
    /// will return the indexers in ascending order of their IDs.
    pub fn indexers_iter(&self) -> impl Iterator<Item = &Indexer> {
        self.indexers.values()
    }

    /// Get [Indexer] by [IndexerId]
    pub fn get_indexer(&self, id: &IndexerId) -> Option<&Indexer> {
        self.indexers.get(id)
    }

    /// Get [Indexer] operator addresses set by [IndexerId]
    pub fn get_indexer_operators(&self, id: &IndexerId) -> Option<&BTreeSet<Address>> {
        self.indexers.get(id).map(|indexer| &indexer.operators)
    }

    /// Get [Subgraph] by [SubgraphId]
    pub fn get_subgraph(&self, id: &SubgraphId) -> Option<&Subgraph> {
        self.subgraphs.get(id)
    }

    /// Get [Deployment] by [DeploymentId]
    pub fn get_deployment(&self, id: &DeploymentId) -> Option<&Deployment> {
        self.deployments.get(id)
    }

    /// Get [Allocation] by [AllocationId]
    pub fn get_allocation(&self, id: &AllocationId) -> Option<&Allocation> {
        self.allocations.get(id)
    }
}

impl Extend<indexer_subgraphs::types::Subgraph> for Snapshot {
    /// Extend the network snapshot with a list of subgraphs
    fn extend<T>(&mut self, iter: T)
    where
        T: IntoIterator<Item = indexer_subgraphs::types::Subgraph>,
    {
        for sub in iter {
            let subgraph_id = sub.id;

            // Add subgraph to the network snapshot
            self.subgraphs
                .entry(subgraph_id)
                .or_insert_with(|| Subgraph {
                    id: subgraph_id,
                    versions: Default::default(),
                });

            for sub_version in sub.versions {
                let deployment_id = sub_version.subgraph_deployment.id;
                let deployment_subgraph_id = subgraph_id;
                let deployment_version_num = sub_version.version;

                // Add subgraph version to the subgraph
                self.subgraphs.entry(subgraph_id).and_modify(|subgraph| {
                    subgraph.versions.push(SubgraphVersion {
                        num: deployment_version_num,
                        deployment: deployment_id,
                    });
                });

                // Add deployment to the network snapshot
                self.deployments
                    .entry(deployment_id)
                    .or_insert_with(|| Deployment {
                        id: deployment_id,
                        subgraph: deployment_subgraph_id,
                        version: deployment_version_num,
                        indexings: Default::default(),
                    });

                for allocation in sub_version.subgraph_deployment.allocations {
                    let allocation_id = allocation.id;
                    let allocation_created_at = allocation.created_at_epoch;
                    let allocation_closed_at = allocation.closed_at_epoch;
                    let allocation_allocated_tokens = allocation.allocated_tokens;
                    let allocation_proof_of_indexing = allocation.poi;
                    let indexer_id = allocation.indexer.id;
                    let indexer_staked_tokens = allocation.indexer.staked_tokens;

                    // Skip indexers without URL
                    let indexer_url = match allocation.indexer.url {
                        Some(url) => url,
                        None => continue,
                    };

                    // Parse indexer URL and check if it is valid, i.e., not empty,
                    // starts with "http://" (or "https://") and has a host part
                    let indexer_url = match indexer_url.parse::<Url>() {
                        Ok(url) if url.scheme().starts_with("http") && url.has_host() => url,
                        _ => continue,
                    };

                    // Add the indexer to the network snapshot indexers table
                    self.indexers
                        .entry(indexer_id)
                        .and_modify(|indexer| {
                            indexer.indexings.insert(deployment_id);
                        })
                        .or_insert_with(|| Indexer {
                            id: indexer_id,
                            url: indexer_url,
                            staked_tokens: indexer_staked_tokens,
                            indexings: BTreeSet::from([deployment_id]),
                            operators: Default::default(),
                        });

                    // Add the indexer to the deployment indexings
                    self.deployments
                        .entry(deployment_id)
                        .and_modify(|deployment| {
                            deployment.indexings.insert(indexer_id);
                        });

                    // Add the allocation to the network snapshot table
                    self.allocations
                        .entry(allocation_id)
                        .or_insert_with(|| Allocation {
                            id: allocation_id,
                            created_at: allocation_created_at,
                            closed_at: allocation_closed_at,
                            indexer: indexer_id,
                            deployment: deployment_id,
                            subgraph: deployment_subgraph_id,
                            allocated_tokens: allocation_allocated_tokens,
                            proof_of_indexing: allocation_proof_of_indexing,
                        });
                }
            }
        }
    }
}

impl Extend<indexer_operators::types::Indexer> for Snapshot {
    /// Extend the network snapshot with an indexer-operator set
    fn extend<T>(&mut self, iter: T)
    where
        T: IntoIterator<Item = indexer_operators::types::Indexer>,
    {
        let iter = iter.into_iter().flat_map(|indexer| {
            let indexer_id = indexer.id;
            indexer
                .account
                .operators
                .into_iter()
                .map(move |operator| (indexer_id, operator.id))
        });

        for (indexer_id, operator_address) in iter {
            // Insert the address into the indexer's operators addresses set
            self.indexers.entry(indexer_id).and_modify(|indexer| {
                indexer.operators.insert(operator_address);
            });
        }
    }
}

impl Snapshot {
    /// Add an indexer to the network snapshot.
    #[cfg(test)]
    pub fn add_indexer(&mut self, indexer: Indexer) {
        self.indexers.insert(indexer.id, indexer);
    }
}

/// An indexer in the network
#[derive(Debug, Clone)]
pub struct Indexer {
    /// The indexer ID
    ///
    /// The indexer ID is a unique identifier for the indexer and coincides with
    /// the Ethereum address of the indexer.
    pub id: IndexerId,
    /// The indexer URL
    pub url: Url,
    /// The staked tokens of the indexer
    pub staked_tokens: u128,
    /// The deployments that the indexer has allocations for and is indexing
    pub indexings: BTreeSet<DeploymentId>,
    /// Associated indexer operator account addresses
    pub operators: BTreeSet<Address>,
}

/// A subgraph in the network
#[derive(Debug, Clone)]
pub struct Subgraph {
    /// The subgraph ID
    pub id: SubgraphId,
    /// The versions of the subgraph
    ///
    /// See [SubgraphVersion] for more information
    pub versions: Vec<SubgraphVersion>,
}

/// A version of a [Subgraph]
#[derive(Debug, Clone)]
pub struct SubgraphVersion {
    /// The version number
    pub num: u32,
    /// The deployment ID
    pub deployment: DeploymentId,
}

/// A deployment of a [Subgraph] to the network
#[derive(Debug, Clone)]
pub struct Deployment {
    /// The deployment ID
    ///
    /// The deployment ID is a unique identifier for the deployment and coincides
    /// with the IPFS CID of the deployment manifest.
    pub id: DeploymentId,
    /// The subgraph ID
    ///
    /// The subgraph ID is the identifier of the subgraph that the deployment
    /// belongs to.
    pub subgraph: SubgraphId,
    /// The deployment version number
    ///
    /// The deployment version number represents the version of the subgraph the
    /// deployment belongs to.
    pub version: u32,
    /// The indexers that are indexing the deployment
    ///
    /// The indexers are stored in a BTreeSet to ensure that they are unique.
    pub indexings: BTreeSet<IndexerId>,
}

/// An allocation in the network
// TODO: Add epoch information to allocations
#[derive(Debug, Clone)]
pub struct Allocation {
    /// The allocation ID
    pub id: AllocationId,
    /// The epoch when the allocation was made
    pub created_at: u32,
    /// The epoch when the allocation was closed
    pub closed_at: Option<u32>,
    /// The indexer ID
    pub indexer: IndexerId,
    /// The deployment ID
    pub deployment: DeploymentId,
    /// The subgraph ID
    pub subgraph: SubgraphId,
    /// The amount of tokens staked by the indexer for the allocation
    pub allocated_tokens: u128,
    /// The proof of indexing for the allocation
    pub proof_of_indexing: Option<ProofOfIndexing>,
}

/// The network epoch information
///
/// The protocol economics are computed over time buckets called epochs that
/// are approximately 24 hours long. The active epoch is the one in which
/// Indexers are currently allocating stake and collecting query fees.
#[derive(Debug, Clone)]
pub struct Epoch {
    /// The epoch number
    pub number: u32,
    /// The start block number
    pub start_block: BlockNumber,
    /// The end block number
    pub end_block: BlockNumber,
}
