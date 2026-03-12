//! Network topology service for tracking indexers, subgraphs, deployments, and allocations.
//!
//! This module provides a service that periodically fetches network topology data from
//! a subgraph and maintains an up-to-date snapshot of the network state. The snapshot
//! includes information about:
//!
//! - Indexers: Entities that index subgraph data and provide query services
//! - Subgraphs: Collections of data sources and mappings that define how to index blockchain data
//! - Deployments: Specific versions of subgraphs deployed to the network
//! - Allocations: Staked tokens by indexers on specific deployments
//!
//! The service runs in the background and provides a handle for accessing the latest
//! network topology snapshot.

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    time::Duration,
};

use anyhow::Context;
use thegraph_core::{DeploymentId, IndexerId, SubgraphId, alloy::primitives::Address};
use tokio::{
    sync::{mpsc, watch, watch::Ref},
    time::MissedTickBehavior,
};
use url::Url;

use crate::network::fetch::{Client as SubgraphClient, indexer_operators, indexer_subgraphs};

/// Parse and validate an indexer URL.
///
/// Returns `Some(Url)` if the URL is present, parses successfully, uses an HTTP(S)
/// scheme, and has a host component. Returns `None` otherwise.
fn parse_indexer_url(raw: Option<String>) -> Option<Url> {
    let url = raw?.parse::<Url>().ok()?;
    (url.scheme().starts_with("http") && url.has_host()).then_some(url)
}

/// Fetches the latest network topology snapshot from the subgraph
pub async fn fetch_snapshot(client: &SubgraphClient) -> anyhow::Result<Snapshot> {
    let subgraphs = client
        .fetch_subgraphs()
        .await
        .context("failed to fetch subgraphs info")?;
    let operators = client
        .fetch_indexer_operators()
        .await
        .context("failed to fetch indexer operators info")?;

    let mut snapshot = Snapshot::new();
    snapshot.extend(subgraphs);
    snapshot.extend(operators);
    Ok(snapshot)
}

/// Handle for interacting with the network topology service.
///
/// This handle provides access to the current network topology snapshot
/// and allows stopping the service.
#[derive(Clone)]
pub struct Handle {
    /// The receiver for the service data
    rx_snapshot: watch::Receiver<Snapshot>,

    /// The stop signal for the service
    tx_stop: mpsc::Sender<()>,
}

impl Handle {
    /// Get the current network topology snapshot.
    ///
    /// Returns a reference to the latest snapshot of the network topology.
    pub fn snapshot(&self) -> Ref<'_, Snapshot> {
        self.rx_snapshot.borrow()
    }

    /// Stop the network topology service.
    ///
    /// This method sends a stop signal to the service and waits for it to shut down.
    /// If the service is already stopped, this method returns immediately.
    pub async fn stop(&self) {
        if self.tx_stop.is_closed() {
            return;
        }

        let _ = self.tx_stop.send(()).await;

        // Wait for the channel to close
        self.tx_stop.closed().await;
    }
}

/// Create a new network topology service that fetches data from the subgraph.
///
/// The service will fetch data from the subgraph at regular intervals and update the internal
/// state. It periodically queries for indexers, subgraphs, deployments, and allocations to
/// maintain an up-to-date view of the network topology.
pub fn new(
    client: SubgraphClient,
    update_interval: Duration,
    init: Snapshot,
) -> (Handle, impl Future<Output = anyhow::Result<()>>) {
    let (tx_stop, mut rx_stop) = mpsc::channel(1);
    let (tx_snapshot, rx_snapshot) = watch::channel(init);

    let service = async move {
        let mut timer = tokio::time::interval(update_interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = rx_stop.recv() => break,
                _ = timer.tick() => {},
            }

            // Create a new snapshot
            let mut snapshot = Snapshot::new();

            match client.fetch_subgraphs().await {
                Ok(data) if !data.is_empty() => {
                    snapshot.extend(data);
                }
                Ok(_) => {
                    tracing::warn!("empty network subgraph update");
                    continue;
                }
                Err(err) => {
                    tracing::warn!(error=%err, "failed to fetch network subgraph update");
                    continue;
                }
            };

            match client.fetch_indexer_operators().await {
                Ok(data) if !data.is_empty() => {
                    snapshot.extend(data);
                }
                Ok(_) => {
                    tracing::warn!("empty network indexer operator update");
                    continue;
                }
                Err(err) => {
                    tracing::warn!(error=%err, "failed to fetch network indexer operator update");
                    continue;
                }
            }

            // Send the snapshot to the receiver, if no listener is available, finish the service
            if let Err(err) = tx_snapshot.send(snapshot) {
                tracing::debug!(error = %err, "failed to send network subgraph update");
                break;
            }
        }

        tracing::debug!("network subgraph service stopped");

        Ok(())
    };

    (
        Handle {
            rx_snapshot,
            tx_stop,
        },
        service,
    )
}

/// A snapshot of the network topology state at a given point in time.
///
/// This structure contains the complete state of the network topology,
/// including indexers, subgraphs, deployments, and allocations.
#[derive(Debug, Clone)]
pub struct Snapshot {
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
}

impl Snapshot {
    /// Create a new empty network snapshot with the current timestamp.
    ///
    /// Returns an empty snapshot with no indexers, subgraphs, deployments, or allocations.
    pub fn new() -> Self {
        Self {
            indexers: Default::default(),
            subgraphs: Default::default(),
            deployments: Default::default(),
        }
    }

    /// Get an iterator over the indexers in the network snapshot.
    ///
    /// As the indexers are stored in a BTreeMap-based table, the iterator
    /// will return the indexers in ascending order of their IDs.
    ///
    /// # Returns
    ///
    /// An iterator over references to the indexers in the snapshot.
    pub fn indexers_iter(&self) -> impl Iterator<Item = &Indexer> {
        self.indexers.values()
    }

    /// Get an [Indexer] by its [IndexerId].
    pub fn get_indexer(&self, id: &IndexerId) -> Option<&Indexer> {
        self.indexers.get(id)
    }
}

impl Extend<indexer_subgraphs::types::Subgraph> for Snapshot {
    /// Extend the network snapshot with a list of subgraphs.
    ///
    /// This method processes subgraph data from the network and updates the snapshot
    /// with new subgraphs, deployments, indexers, and allocations.
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
                    let indexer_id = allocation.indexer.id;

                    let indexer_url = match parse_indexer_url(allocation.indexer.url) {
                        Some(url) => url,
                        None => continue,
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
                            indexings: BTreeSet::from([deployment_id]),
                            operators: Default::default(),
                        });

                    // Add the indexer to the deployment indexings
                    self.deployments
                        .entry(deployment_id)
                        .and_modify(|deployment| {
                            deployment.indexings.insert(indexer_id);
                        });
                }
            }
        }
    }
}

impl Extend<indexer_operators::types::Indexer> for Snapshot {
    /// Extend the network snapshot with indexer data and operator relationships.
    ///
    /// Creates indexer entries for any registered indexer with a valid URL,
    /// regardless of whether they have active allocations. This ensures idle
    /// indexers are visible to the proposal pipeline.
    fn extend<T>(&mut self, iter: T)
    where
        T: IntoIterator<Item = indexer_operators::types::Indexer>,
    {
        for indexer_data in iter {
            let indexer_id = indexer_data.id;

            let indexer_url = match parse_indexer_url(indexer_data.url) {
                Some(url) => url,
                None => continue,
            };

            let operators: BTreeSet<Address> = indexer_data
                .account
                .operators
                .into_iter()
                .map(|op| op.id)
                .collect();

            // Only create new entries for indexers that have operators,
            // since proposals require an operator to sign.
            if operators.is_empty() && !self.indexers.contains_key(&indexer_id) {
                continue;
            }

            self.indexers
                .entry(indexer_id)
                .and_modify(|indexer| {
                    indexer.operators.extend(operators.iter().copied());
                })
                .or_insert_with(|| Indexer {
                    id: indexer_id,
                    url: indexer_url,
                    indexings: BTreeSet::new(),
                    operators,
                });
        }
    }
}

/// An indexer in the network.
///
/// Indexers are entities that stake tokens on subgraph deployments and provide
/// query services for those deployments. They earn rewards for correctly indexing
/// and serving subgraph data.
#[derive(Debug, Clone)]
pub struct Indexer {
    /// The indexer ID
    ///
    /// The indexer ID is a unique identifier for the indexer and coincides with
    /// the Ethereum address of the indexer.
    pub id: IndexerId,
    /// The indexer URL
    ///
    /// The URL where the indexer's GraphQL API can be accessed.
    pub url: Url,
    /// The deployments that the indexer has allocations for and is indexing
    ///
    /// This set contains the IDs of all deployments the indexer is currently indexing.
    pub indexings: BTreeSet<DeploymentId>,
    /// Associated indexer operator account addresses
    ///
    /// These are Ethereum addresses that have permission to operate on behalf of the indexer.
    pub operators: BTreeSet<Address>,
}

/// A subgraph in the network.
///
/// Subgraphs define how to index and transform blockchain data into a structured GraphQL API.
/// They can have multiple versions, each corresponding to a specific deployment.
#[derive(Debug, Clone)]
pub struct Subgraph {
    /// The subgraph ID
    ///
    /// A unique identifier for the subgraph.
    pub id: SubgraphId,
    /// The versions of the subgraph
    ///
    /// Each version corresponds to a specific deployment of the subgraph.
    /// See [SubgraphVersion] for more information.
    pub versions: Vec<SubgraphVersion>,
}

/// A version of a [Subgraph].
///
/// Each subgraph can have multiple versions, each with a different deployment.
/// Newer versions typically contain improvements or fixes to the subgraph.
#[derive(Debug, Clone)]
pub struct SubgraphVersion {
    /// The version number
    ///
    /// A sequential number identifying the version, with higher numbers
    /// indicating newer versions.
    pub num: u32,
    /// The deployment ID
    ///
    /// The ID of the deployment corresponding to this version.
    pub deployment: DeploymentId,
}

/// A deployment of a [Subgraph] to the network.
///
/// A deployment represents a specific version of a subgraph that has been
/// published to the network and can be indexed by indexers.
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
