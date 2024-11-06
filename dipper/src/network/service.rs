use std::{future::Future, time::Duration};

use tokio::{
    sync::{watch, watch::Ref},
    time::MissedTickBehavior,
};

use super::subgraph::{client::Client as SubgraphClient, snapshot::Snapshot};

#[derive(Clone)]
pub struct ServiceHandle {
    rx: watch::Receiver<Snapshot>,
}

impl ServiceHandle {
    /// Wait for the service data to be ready
    ///
    /// This function will block until the service data is ready (not empty).
    ///
    /// If the underlying channel has been closed, this function will return an error.
    pub async fn wait_ready(&mut self) -> anyhow::Result<()> {
        let _ = self.rx.wait_for(|data| !data.is_empty()).await?;
        Ok(())
    }

    /// Wait for the service data to have changed
    ///
    /// If the underlying channel has been closed, this function will return an error.
    pub async fn wait_changed(&mut self) -> anyhow::Result<()> {
        self.rx.changed().await.map_err(Into::into)
    }

    /// Get the current snapshot
    pub fn snapshot(&self) -> Ref<'_, Snapshot> {
        self.rx.borrow()
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
) -> (ServiceHandle, impl Future<Output = ()>) {
    let (tx, rx) = watch::channel(Default::default());

    let handle = ServiceHandle { rx };
    let service = async move {
        let mut timer = tokio::time::interval(update_interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            timer.tick().await;

            let snapshot = match client.fetch().await {
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
                    tracing::warn!(error = %err, "failed to fetch network subgraph update");
                    continue;
                }
            };

            // Send the snapshot to the receiver, if no listener is available, finish the service
            if let Err(err) = tx.send(snapshot) {
                tracing::debug!(error = %err, "failed to send network subgraph update");
                break;
            }
        }

        tracing::debug!("network subgraph service stopped");
    };

    (handle, service)
}
