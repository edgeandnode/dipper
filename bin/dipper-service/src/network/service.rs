use std::{future::Future, time::Duration};

use tokio::{
    sync::{mpsc, watch, watch::Ref},
    time::MissedTickBehavior,
};

use super::fetch::{snapshot::Snapshot, Client as SubgraphClient};

#[derive(Clone)]
pub struct Handle {
    /// The receiver for the service data
    rx_snapshot: watch::Receiver<Snapshot>,

    /// The stop signal for the service
    tx_stop: mpsc::Sender<()>,
}

impl Handle {
    /// Wait for the service data to have changed
    ///
    /// If the underlying channel has been closed, this function will return an error.
    pub async fn wait_changed(&mut self) -> anyhow::Result<()> {
        self.rx_snapshot.changed().await.map_err(Into::into)
    }

    /// Get the current snapshot
    pub fn snapshot(&self) -> Ref<'_, Snapshot> {
        self.rx_snapshot.borrow()
    }

    /// Stop the service
    pub async fn stop(&self) {
        if self.tx_stop.is_closed() {
            return;
        }

        let _ = self.tx_stop.send(()).await;

        // Wait for the channel to close
        self.tx_stop.closed().await;
    }
}

/// Create a new service that fetches data from the subgraph
///
/// The service will fetch data from the subgraph at regular intervals and update the internal
/// state.
///
/// The service will return a handle that can be used to interact with the service.
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

            let mut snapshot = match client.fetch_subgraphs().await {
                Ok(data) if !data.is_empty() => {
                    let mut snapshot = Snapshot::new();
                    snapshot.extend(data);
                    snapshot
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
                }
                Err(err) => {
                    tracing::warn!(error=%err, "failed to fetch network indexer operator update");
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
