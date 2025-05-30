use std::{collections::BTreeMap, env, path::PathBuf, sync::Arc};

use async_signal::{Signal, Signals};
use dipper_core::state::FromState;
use dipper_iisa as iisa;
use futures_lite::StreamExt;
use thegraph_core::alloy::{primitives::ChainId, signers::local::PrivateKeySigner};
use tokio::task::JoinSet;
use tracing_subscriber::EnvFilter;

use self::{
    context::{CtxBuilder, DEFAULT_MAX_CANDIDATES},
    registry::RegistryProvider,
    signing::{eip712::Eip712Signer, tap::ReceiptSigner},
    worker::{Worker, queue::QueueImpl},
};

mod admin_rpc_server;
mod config;
mod context;
mod db;
mod indexer_rpc_client;
mod indexer_rpc_server;
mod network;
mod registry;
mod signing;
mod worker;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    // Set up logging
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    // Load the configuration
    tracing::debug!("loading configuration");
    let conf_path = env::args()
        .nth(1)
        .expect("Missing argument for config path")
        .parse::<PathBuf>()
        .expect("Invalid path");
    let conf = config::load_from_file(&conf_path).expect("Failed to load config");
    tracing::debug!(conf=?conf, "configuration loaded");

    // Initialize the different components
    //- The signer component
    let signer = {
        let private_key_signer =
            PrivateKeySigner::from_signing_key(conf.signer.secret_key.as_ref().into());
        let private_key_signer_address = private_key_signer.address();
        let domain = dipper_rpc::admin::eip712_domain();

        Arc::new(Eip712Signer::new(
            private_key_signer,
            private_key_signer_address,
            conf.signer.chain_id,
            domain,
        ))
    };
    tracing::info!(address=%signer.address(), "Signer wallet imported");

    //- The TAP signer component
    let tap_signer = {
        let private_key_signer =
            PrivateKeySigner::from_signing_key(conf.tap_signer.secret_key.as_ref().into());

        Arc::new(ReceiptSigner::new(
            private_key_signer,
            conf.tap_signer.chain_id,
            conf.tap_signer.verifier,
        ))
    };
    tracing::info!(address=%tap_signer.address(), "TAP Signer wallet imported");

    //- DB connect and run migrations
    let db_conn = db::connect(&conf.db).await?;
    tracing::info!(db_url=%conf.db.url, "initialized DB connection pool");

    db::run_migrations(&db_conn).await?;
    tracing::info!("applied DB migrations");

    //- The worker and worker queue component
    let queue = QueueImpl::new(db_conn.clone());
    let worker = Worker::new(queue.clone());

    //- The registry component
    let registry = RegistryProvider::new(db_conn.clone());

    //- The indexer client component
    let indexer_client = indexer_rpc_client::DipsIndexerClient::new(signer.clone());

    //- The network services
    let (
        (network_epoch_handle, network_epoch_service),
        (network_topology_handle, network_topology_service),
    ) = {
        let network_subgraph_url = conf
            .network
            .gateway_url
            .join(&format!(
                "/api/deployments/id/{}",
                conf.network.deployment_id
            ))
            .expect("invalid network subgraph URL");

        let network_subgraph_client = network::fetch::Client::new(
            reqwest::Client::new(),
            network_subgraph_url,
            conf.network.api_key.into_inner(),
        );

        // Fetch the initial network snapshots, a successful fetch is required to start the service
        let epoch_init_snapshot =
            network::service::epoch::fetch_snapshot(&network_subgraph_client).await?;
        let topology_init_snapshot =
            network::service::topology::fetch_snapshot(&network_subgraph_client).await?;

        (
            network::service::epoch::new(
                network_subgraph_client.clone(),
                conf.network.update_interval,
                epoch_init_snapshot,
            ),
            network::service::topology::new(
                network_subgraph_client,
                conf.network.update_interval,
                topology_init_snapshot,
            ),
        )
    };
    tracing::info!("initialized Graph network service");

    //- The network provider component
    let network_provider = network::provider::NetworkProviderService::new(
        network_epoch_handle.clone(),
        network_topology_handle.clone(),
        conf.indexer_rpc.allowlist.clone(),
    );

    //- The IISA service
    let (iisa_handle, iisa_service) = iisa::service::new();
    tracing::info!("initialized IISA service");

    // Application services
    let context = CtxBuilder::new()
        .with_signer(signer)
        .with_tap_signer(tap_signer)
        .with_agreement_config(conf.dips)
        .with_worker(worker)
        .with_network_provider(network_provider)
        .with_registry(registry)
        .with_indexer_client(indexer_client)
        .with_iisa(iisa_handle.clone())
        .with_admin_allowlist(conf.admin_rpc.allowlist)
        .with_network_allowlist(conf.indexer_rpc.allowlist)
        .with_max_candidates(DEFAULT_MAX_CANDIDATES)
        .build();

    //- The worker service
    let (worker_handle, worker_service) =
        worker::service::new(queue, FromState::from_state(&context));
    tracing::info!("initialized Worker service");

    //- The admin RPC service
    let (admin_rpc_handle, admin_rpc_service) = {
        let config = admin_rpc_server::service::Config {
            listen_addr: conf.admin_rpc.listen_addr,
        };

        admin_rpc_server::service::new(config, FromState::from_state(&context))
    };
    tracing::info!("initialized Admin RPC service");

    //- The indexer RPC service
    let (indexer_rpc_handle, indexer_rpc_service) = {
        let config = indexer_rpc_server::service::Config {
            listen_addr: conf.indexer_rpc.listen_addr,
        };

        indexer_rpc_server::service::new(config, context.clone())
    };
    tracing::info!("initialized Admin RPC service");

    // Construct the task tree
    let mut task_tree: JoinSet<anyhow::Result<()>> = JoinSet::new();

    let network_epoch_task_handle = task_tree.spawn(network_epoch_service);
    tracing::debug!(task_id=%network_epoch_task_handle.id(), "Graph network epoch service started");

    let network_topology_task_handle = task_tree.spawn(network_topology_service);
    tracing::debug!(task_id=%network_topology_task_handle.id(), "Graph network topology service started");

    let iisa_task_handle = task_tree.spawn_blocking(iisa_service);
    tracing::debug!(task_id=%iisa_task_handle.id(), "IISA service started");

    let worker_task_handle = task_tree.spawn(worker_service);
    tracing::debug!(task_id=%worker_task_handle.id(), "Worker service started");

    let indexer_rpc_task_handle = task_tree.spawn(indexer_rpc_service);
    tracing::debug!(task_id=%indexer_rpc_task_handle.id(), "Indexer RPC service started");

    let admin_rpc_task_handle = task_tree.spawn(admin_rpc_service);
    tracing::debug!(task_id=%admin_rpc_task_handle.id(), "Admin RPC service started");

    let signal_handler_task_handle = task_tree.spawn(async move {
        let signal = signal_task().await;
        match signal {
            Ok(AppSignal::Shutdown) => {
                tracing::info!("shutting down");
            }
            Err(err) => {
                tracing::error!(error=?err, "signal handler registration failed. shutting down");
            }
        }

        // Stop all services.
        //
        // Services are stopped in the reverse order of their dependencies. This is to ensure that
        // the services that depend on other services are stopped first
        tracing::trace!("stopping Admin RPC service");
        admin_rpc_handle.stop().await;
        tracing::trace!("stopped Admin RPC service");

        tracing::trace!("stopping Indexer RPC service");
        indexer_rpc_handle.stop().await;
        tracing::trace!("stopped Indexer RPC service");

        tracing::trace!("stopping Worker service");
        worker_handle.stop().await;
        tracing::trace!("stopped Worker service");

        tracing::trace!("stopping IISA service");
        iisa_handle.stop().await;
        tracing::trace!("stopped IISA service");

        tracing::trace!("stopping Graph network service");
        network_epoch_handle.stop().await;
        network_topology_handle.stop().await;
        tracing::trace!("stopped Graph network service");

        tracing::trace!("shutting down DB connection pool");
        db_conn.close().await;
        tracing::trace!("shut down DB connection pool");

        Ok(())
    });
    tracing::debug!(task_id=%signal_handler_task_handle.id(), "signal handler registered");

    // Block on the task tree. Wait for all tasks to complete
    tracing::info!("starting service");
    while let Some(res) = task_tree.join_next_with_id().await {
        match res {
            Ok((id, Ok(()))) => {
                tracing::debug!(task_id=%id, "task completed");
            }
            Ok((id, Err(err))) => {
                tracing::error!(task_id=%id, error=?err, "task failed");
            }
            Err(err) => {
                tracing::error!(task_id=%err.id(), error=?err, "task join error");
            }
        }
    }
    Ok(())
}

/// Signals that the application can receive
enum AppSignal {
    Shutdown,
}

/// Error type for signal handler
#[derive(Debug, thiserror::Error)]
enum SignalHandlerError {
    #[error("signal receiver registration failed: {0}")]
    RegistrationFailed(std::io::Error),
}

/// Signal handler for the application
async fn signal_task() -> Result<AppSignal, SignalHandlerError> {
    let mut signals = Signals::new([Signal::Term, Signal::Int, Signal::Quit, Signal::Abort])
        .map_err(SignalHandlerError::RegistrationFailed)?;

    while let Some(signal) = signals.next().await {
        match signal {
            Ok(signal) => {
                tracing::debug!("received signal '{:?}'", signal);
                return Ok(AppSignal::Shutdown);
            }
            Err(err) => {
                tracing::warn!(error=?err, "unexpected signal received");
            }
        }
    }

    // Fallthrough
    Ok(AppSignal::Shutdown)
}

impl From<config::DipsAgreementConfig>
    for (
        context::IndexingAgreementConfig,
        BTreeMap<ChainId, context::IndexingAgreementChainPrices>,
    )
{
    fn from(value: config::DipsAgreementConfig) -> Self {
        let config = context::IndexingAgreementConfig {
            service: value.service,
            max_initial_amount: value.max_initial_amount,
            max_ongoing_amount_per_epoch: value.max_ongoing_amount_per_epoch,
            max_epochs_per_collection: value.max_epochs_per_collection,
            min_epochs_per_collection: value.min_epochs_per_collection,
            duration_epochs: value.duration_epochs,
        };
        let prices = value
            .pricing_table
            .into_iter()
            .map(|(chain_id, prices)| {
                (
                    chain_id,
                    context::IndexingAgreementChainPrices {
                        base_price_per_epoch: prices.base_price_per_epoch,
                        price_per_entity: prices.price_per_entity,
                    },
                )
            })
            .collect();
        (config, prices)
    }
}
