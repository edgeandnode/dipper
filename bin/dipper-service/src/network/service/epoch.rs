//! Network epoch service for tracking the network epoch
//!
//! This module provides a service that monitors network epochs, which are time periods
//! (approximately 24 hours) defined by block ranges. The service periodically fetches
//! the latest epoch information from a subgraph and makes it available to consumers.
//!
//! The main components are:
//! - `Snapshot`: Current state of the network epoch
//! - `Handle`: Interface for accessing epoch data and controlling the service
//! - `new()`: Function to create and start the epoch service

use std::{future::Future, time::Duration};

use tokio::{
    sync::{mpsc, watch, watch::Ref},
    time::MissedTickBehavior,
};

use crate::network::{fetch, fetch::Client as SubgraphClient};

/// Fetches the latest epoch snapshot from the subgraph client
///
/// Makes a network request to retrieve current epoch information and returns it as a Snapshot.
pub async fn fetch_snapshot(client: &SubgraphClient) -> anyhow::Result<Snapshot> {
    let epoch = client.fetch_latest_epoch().await?;
    Ok(Snapshot::new(epoch))
}

/// Handle for interacting with the epoch service
///
/// Provides access to the current epoch data and control over the service lifecycle.
#[derive(Clone)]
pub struct Handle {
    /// The receiver for the service data
    rx_snapshot: watch::Receiver<Snapshot>,

    /// The stop signal for the service
    tx_stop: mpsc::Sender<()>,
}

impl Handle {
    /// Get the current snapshot from the epoch service
    ///
    /// Returns a reference to the most recently fetched epoch data.
    pub fn snapshot(&self) -> Ref<'_, Snapshot> {
        self.rx_snapshot.borrow()
    }

    /// Stop the epoch service gracefully
    ///
    /// Sends a stop signal and waits for the service to shut down.
    pub async fn stop(&self) {
        if self.tx_stop.is_closed() {
            return;
        }

        let _ = self.tx_stop.send(()).await;

        // Wait for the channel to close
        self.tx_stop.closed().await;
    }
}

/// Create a new epoch service that fetches data from the subgraph
///
/// Initializes a service that periodically retrieves epoch information and makes it
/// available through the returned handle. Returns a handle and a future that must be
/// spawned on a runtime.
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

            // Fetch the current network epoch
            let snapshot = match fetch_snapshot(&client).await {
                Ok(snapshot) => snapshot,
                Err(err) => {
                    tracing::error!(error=%err, "failed to fetch network latest epoch update");
                    continue;
                }
            };

            // Send the snapshot to the receiver, if no listener is available, finish the service
            if let Err(err) = tx_snapshot.send(snapshot) {
                tracing::debug!(error = %err, "failed to send network subgraph epoch update");
                break;
            }
        }

        tracing::debug!("network subgraph epoch service stopped");

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

/// A snapshot of the network state at a given point in time
///
/// Contains the current epoch information retrieved from the network.
#[derive(Debug, Clone)]
pub struct Snapshot(Epoch);

impl Snapshot {
    /// Create a new snapshot from the fetched data
    #[inline]
    fn new(epoch: fetch::epochs::types::Epoch) -> Self {
        Self(Epoch { number: epoch.id })
    }

    /// Get the current network epoch from the snapshot
    ///
    /// Returns a reference to the epoch information.
    pub fn epoch(&self) -> &Epoch {
        &self.0
    }
}

/// The network epoch information
///
/// Represents a time bucket (approximately 24 hours) during which Indexers allocate
/// stake and collect query fees. Defined by a unique number and block range.
#[derive(Debug, Clone)]
pub struct Epoch {
    /// The epoch number
    pub number: u32,
}
