use anyhow::Context;
use async_trait::async_trait;
use pyo3::Python;
use thegraph_core::DeploymentId;
use tokio::sync::{mpsc, oneshot};

use super::{
    api::{CandidateSelection, Indexer},
    py::{
        iisa::{PyBigQueryProvider, PyDataManager, PyGeoipResolver, PyNetworkProvider},
        logging,
    },
};

/// The `Command` enum represents the commands that can be sent to the `IndexerSelectionService`.
enum Command {
    /// Instructs the `DataManager` to fetch, process and update the indexer performance
    /// history data.
    FetchAndUpdateHistoricalData {
        /// The response channel to send the result of the operation.
        tx: oneshot::Sender<anyhow::Result<()>>,
    },

    /// Set the latest network snapshot's indexers list to the `NetworkProvider`.
    UpdateNetworkIndexersList {
        /// The latest network snapshot's indexers list.
        indexers: Vec<Indexer>,

        /// The response channel to send the result of the operation.
        tx: oneshot::Sender<anyhow::Result<()>>,
    },

    /// Select one indexer from the given list of candidates.
    SelectOneIndexer {
        /// The deployment ID for which the indexer is being selected.
        deployment_id: DeploymentId,

        /// The list of candidates to select from.
        candidates: Vec<Indexer>,

        /// The response channel to send the result of the operation.
        tx: oneshot::Sender<anyhow::Result<Option<Indexer>>>,
    },

    /// Select indexers from the given list of candidates.
    SelectIndexers {
        /// The deployment ID for which the indexers are being selected.
        deployment_id: DeploymentId,

        /// The list of candidates to select from.
        candidates: Vec<Indexer>,

        /// The number of indexers to select.
        num_candidates: usize,

        /// The response channel to send the result of the operation.
        tx: oneshot::Sender<anyhow::Result<Vec<Indexer>>>,
    },
}

/// The `ServiceHandle` is a handle to the `IndexerSelectionService` that allows sending commands to
/// the service.
#[derive(Clone)]
pub struct ServiceHandle {
    tx: mpsc::UnboundedSender<Command>,
}

impl ServiceHandle {
    /// Instructs the `DataManager` to fetch, process and update the indexer performance
    /// history data.
    pub async fn fetch_and_update_historical_data(&self) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(Command::FetchAndUpdateHistoricalData { tx })?;
        rx.await?
    }

    /// Set the latest network snapshot's indexers list to the `NetworkProvider`.
    pub async fn update_network_indexers_list(&self, indexers: Vec<Indexer>) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::UpdateNetworkIndexersList { indexers, tx })?;
        rx.await?
    }
}

/// The `SelectionError` enum represents the errors that can occur during the candidate selection
/// process.
#[derive(Debug, thiserror::Error)]
pub enum SelectionError {
    /// Indexer Selection service is not available.
    ///
    /// An error occurred while sending a request to the IISA service.
    #[error("IISA service is not available")]
    IisaServiceUnavailable,

    /// An error occurred during the selection process.
    #[error(transparent)]
    Error(#[from] anyhow::Error),
}

#[async_trait]
impl CandidateSelection for ServiceHandle {
    type Error = SelectionError;

    async fn select_one(
        &self,
        deployment_id: DeploymentId,
        candidates: Vec<Indexer>,
    ) -> Result<Option<Indexer>, Self::Error> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::SelectOneIndexer {
                deployment_id,
                candidates,
                tx,
            })
            .map_err(|_| SelectionError::IisaServiceUnavailable)?;
        let res = rx
            .await
            .map_err(|_| SelectionError::IisaServiceUnavailable)?;

        res.map_err(SelectionError::Error)
    }

    async fn select(
        &self,
        deployment_id: DeploymentId,
        candidates: Vec<Indexer>,
        num_candidates: usize,
    ) -> Result<Vec<Indexer>, Self::Error> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::SelectIndexers {
                deployment_id,
                candidates,
                num_candidates,
                tx,
            })
            .map_err(|_| SelectionError::IisaServiceUnavailable)?;
        let res = rx
            .await
            .map_err(|_| SelectionError::IisaServiceUnavailable)?;

        res.map_err(SelectionError::Error)
    }
}

/// Creates a new `IndexerSelectionService` and returns a handle to it along with a function that
/// should be called to start the service.
pub fn new() -> (ServiceHandle, impl FnOnce() -> anyhow::Result<()>) {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let service_task = move || {
        // Register the Python logging to Rust log handler
        logging::register("dipper::indexer_selection").expect("Failed to register host logger");

        Python::with_gil(|py| {
            // Set up Python logging
            py.run_bound(
                indoc::indoc! {r#"
                import logging

                logging.basicConfig(level=logging.INFO)
                logging.captureWarnings(True)
                "#},
                None,
                None,
            )
            .context("Failed to set up Python logging")?;

            // Instantiate the data manager class
            let (data_manager, network_provider) = {
                let geoip_resolver = PyGeoipResolver::new(py)?;
                let network_provider = PyNetworkProvider::new(py, geoip_resolver)?;
                let bigquery_provider = PyBigQueryProvider::new(py, "graph-mainnet", "US")?;
                let data_manager =
                    PyDataManager::new(py, bigquery_provider, network_provider.clone())?;

                Ok::<_, anyhow::Error>((data_manager, network_provider))
            }?;

            // Start listening for commands
            loop {
                let cmd = match rx.blocking_recv() {
                    Some(cmd) => cmd,
                    None => {
                        tracing::info!("Service handle dropped, aborting service");
                        break;
                    }
                };

                match cmd {
                    Command::FetchAndUpdateHistoricalData { tx } => {
                        match data_manager.fetch_data_and_update() {
                            Ok(_) => {
                                if tx.send(Ok(())).is_err() {
                                    // Abort service if the response channel's receiver side has been dropped.
                                    return Err(anyhow::anyhow!(
                                        "Failed to send the result of the operation"
                                    ));
                                }
                            }
                            Err(err) => {
                                if tx
                                    .send(Err(anyhow::anyhow!(err)
                                        .context("Failed to fetch and update historical data")))
                                    .is_err()
                                {
                                    // Abort service if the response channel's receiver side has been dropped.
                                    return Err(anyhow::anyhow!(
                                        "Failed to send the result of the operation"
                                    ));
                                }
                            }
                        }
                    }

                    Command::UpdateNetworkIndexersList { tx, indexers } => {
                        match network_provider.set_snapshot(py, &indexers) {
                            Ok(_) => {
                                if tx.send(Ok(())).is_err() {
                                    // Abort service if the response channel's receiver side has been dropped.
                                    return Err(anyhow::anyhow!(
                                        "Failed to send the result of the operation"
                                    ));
                                }
                            }
                            Err(err) => {
                                if tx
                                    .send(Err(anyhow::anyhow!(err)
                                        .context("Failed to update network indexers list")))
                                    .is_err()
                                {
                                    // Abort service if the response channel's receiver side has been dropped.
                                    return Err(anyhow::anyhow!(
                                        "Failed to send the result of the operation"
                                    ));
                                }
                            }
                        }
                    }
                    Command::SelectOneIndexer { .. } => {
                        todo!("SelectOneIndexer command")
                    }
                    Command::SelectIndexers { .. } => {
                        todo!("SelectIndexers command")
                    }
                }
            }

            Ok(())
        })
    };

    (ServiceHandle { tx }, service_task)
}
