use anyhow::Context;
use async_trait::async_trait;
use pyo3::{ffi::c_str, Python};
use thegraph_core::DeploymentId;
use tokio::sync::{mpsc, oneshot};

use super::{
    api::{CandidateSelection, Indexer, SelectionError},
    py::{iisa, logging},
};

/// The IISA service configuration.
pub struct Config {
    /// The GeoIP resolver auth token.
    ///
    /// This token is used to authenticate the GeoIP resolver with the `ipinfo.io` service.
    pub geoip_auth: String,

    /// The BigQuery project ID.
    pub bigquery_project_id: String,

    /// The BigQuery region.
    pub bigquery_region: String,
}

/// The `Command` enum represents the commands that can be sent to the `IndexerSelectionService`.
enum Command {
    /// Instruct the IISA service to stop.
    Stop,

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

/// The `Handle` is a handle to the `IndexerSelectionService` that allows sending commands to
/// the service.
#[derive(Clone)]
pub struct Handle {
    /// A channel to communicate with the service.
    tx: mpsc::UnboundedSender<Command>,
}

impl Handle {
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

    /// Stop the service.
    pub async fn stop(&self) {
        if self.tx.is_closed() {
            return;
        }

        let _ = self.tx.send(Command::Stop);

        // Wait for the channel to close
        self.tx.closed().await;
    }
}

#[async_trait]
impl CandidateSelection for Handle {
    async fn select_one(
        &self,
        deployment_id: DeploymentId,
        candidates: Vec<Indexer>,
    ) -> Result<Option<Indexer>, SelectionError> {
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
    ) -> Result<Vec<Indexer>, SelectionError> {
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
pub fn new(config: Config) -> (Handle, impl FnOnce() -> anyhow::Result<()>) {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let service_task = move || {
        // Register the Python logging to Rust log handler
        logging::register().expect("Failed to register host logger");

        Python::with_gil(|py| {
            // Set up Python logging
            py.run(
                c_str!(indoc::indoc! {r#"
                import logging

                logging.basicConfig(
                    level=logging.INFO,
                    handlers=[
                        logging.HostLogHandler("dipper_iisa::service")
                    ]
                )
                logging.captureWarnings(True)
                "#}),
                None,
                None,
            )
            .context("Failed to set up Python logging")?;

            // Instantiate the data manager class
            let (data_manager, network_provider) = {
                let geoip_resolver = iisa::PyGeoipResolver::new(py, &config.geoip_auth)?;
                let network_provider = iisa::PyNetworkProvider::new(py, geoip_resolver)?;
                let bigquery_provider = iisa::PyBigQueryProvider::new(
                    py,
                    &config.bigquery_project_id,
                    &config.bigquery_region,
                )?;
                let data_manager =
                    iisa::PyDataManager::new(py, bigquery_provider, network_provider.clone())?;

                Ok::<_, anyhow::Error>((data_manager, network_provider))
            }?;

            // Start listening for commands
            loop {
                // Wait for the next command
                let cmd = match rx.blocking_recv() {
                    Some(cmd) => cmd,
                    None => {
                        tracing::debug!("Service handle dropped, aborting service");
                        break;
                    }
                };

                match cmd {
                    Command::Stop => {
                        tracing::debug!("Stopping IISA service");
                        break;
                    }

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
                    Command::SelectOneIndexer {
                        deployment_id,
                        candidates,
                        tx,
                    } => {
                        tracing::debug!(
                            %deployment_id,
                            "Selecting one indexer out of {} candidates", candidates.len()
                        );

                        // Skip if there are no candidates
                        if candidates.is_empty() {
                            let _ = tx.send(Ok(None));
                            continue;
                        }

                        let ids = candidates.iter().map(|indexer| &indexer.id);
                        match iisa::select_one(py, ids) {
                            Ok(None) => {
                                let _ = tx.send(Ok(None));
                            }
                            Ok(Some(id)) => {
                                let indexer =
                                    candidates.iter().find(|indexer| indexer.id == id).cloned();
                                let _ = tx.send(Ok(indexer));
                            }
                            Err(err) => {
                                let _ = tx
                                    .send(Err(anyhow::anyhow!(err)
                                        .context("Failed to select one indexer")));
                            }
                        }
                    }
                    Command::SelectIndexers {
                        deployment_id,
                        candidates,
                        num_candidates,
                        tx,
                    } => {
                        tracing::debug!(
                            %deployment_id,
                            "Selecting {} indexers out of {}", num_candidates, candidates.len()
                        );

                        // Skip if the desired number of indexers is zero,
                        // or the candidates list is empty
                        if candidates.is_empty() || num_candidates == 0 {
                            let _ = tx.send(Ok(Vec::new()));
                            continue;
                        }

                        let ids = candidates.iter().map(|indexer| &indexer.id);
                        match iisa::select_many(py, ids, num_candidates) {
                            Ok(res) => {
                                let indexers = candidates
                                    .iter()
                                    .filter(|indexer| res.contains(&indexer.id))
                                    .cloned()
                                    .collect();
                                let _ = tx.send(Ok(indexers));
                            }
                            Err(err) => {
                                let _ = tx.send(Err(
                                    anyhow::anyhow!(err).context("Failed to select indexers")
                                ));
                            }
                        }
                    }
                }
            }

            Ok(())
        })
    };

    (Handle { tx }, service_task)
}
