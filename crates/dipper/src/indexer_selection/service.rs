use std::time::Duration;

use pyo3::Python;
use tokio::sync::{mpsc, oneshot};

use crate::{indexer_selection::logging, network::Indexer};

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

    /// Update `DataProcessor` with the agreements list.
    UpdateAgreementsList {
        /// The agreements list.
        agreements: Vec<()>, // TODO: Add the agreement type.

        /// The response channel to send the result of the operation.
        tx: oneshot::Sender<anyhow::Result<()>>,
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

    /// Update `DataProcessor` with the agreements list.
    /// TODO: Add the agreement type.
    pub async fn update_agreements_list(&self, agreements: Vec<()>) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::UpdateAgreementsList { agreements, tx })?;
        rx.await?
    }
}

/// Creates a new `IndexerSelectionService` and returns a handle to it along with a function that
/// should be called to start the service.
pub fn new() -> (ServiceHandle, impl FnOnce() -> anyhow::Result<()>) {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let service_task = move || {
        // Register the Python logging to Rust log handler
        logging::register("dipper::indexer_selection").expect("Failed to register host logger");

        // Set up Python logging
        Python::with_gil(|py| {
            py.run_bound(
                indoc::indoc! {r#"
                import logging
                logging.basicConfig(level=logging.INFO)
                logging.captureWarnings(True)
                
                # Set up the logger for the indexer_selection service
                logger = logging.getLogger(__name__)
            "#},
                None,
                None,
            )
        })?;

        loop {
            let cmd = match rx.blocking_recv() {
                Some(cmd) => cmd,
                None => {
                    tracing::info!("Service handle dropped, aborting service.");
                    break;
                }
            };

            match cmd {
                Command::FetchAndUpdateHistoricalData { tx } => {
                    // TODO: Implement the logic to fetch, process and update the indexer performance
                    Python::with_gil(|py| {
                        py.run_bound(
                            r#"logger.info("fetching and updating historical data")"#,
                            None,
                            None,
                        )
                    })?;
                    std::thread::sleep(Duration::from_secs(2)); // Simulation

                    // Abort service if the response channel's receiver side has been dropped.
                    if tx.send(Ok(())).is_err() {
                        return Err(anyhow::anyhow!(
                            "Failed to send the result of the operation."
                        ));
                    }
                }

                Command::UpdateNetworkIndexersList { tx, .. } => {
                    // TODO: Implement the actual logic
                    Python::with_gil(|py| {
                        py.run_bound(
                            r#"logger.info("updating network indexers list")"#,
                            None,
                            None,
                        )
                    })?;
                    std::thread::sleep(Duration::from_millis(100)); // Simulation

                    // Abort service if the response channel's receiver side has been dropped.
                    if tx.send(Ok(())).is_err() {
                        return Err(anyhow::anyhow!(
                            "Failed to send the result of the operation."
                        ));
                    }
                }
                Command::UpdateAgreementsList { tx, .. } => {
                    // TODO: Implement the actual logic
                    Python::with_gil(|py| {
                        py.run_bound(r#"logger.info("updating agreements list")"#, None, None)
                    })?;
                    std::thread::sleep(Duration::from_millis(100)); // Simulation

                    // Abort service if the response channel's receiver side has been dropped.
                    if tx.send(Ok(())).is_err() {
                        return Err(anyhow::anyhow!(
                            "Failed to send the result of the operation."
                        ));
                    }
                }
            }
        }

        Ok(())
    };

    (ServiceHandle { tx }, service_task)
}
