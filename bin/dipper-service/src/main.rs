use std::{env, path::PathBuf};

use async_signal::{Signal, Signals};
use dipper_core::rpc::eip712_domain;
use dipper_iisa as iisa;
use dipper_pgmq::postgres::PgQueue;
use dipper_registry::postgres::PgRegistry;
use futures_lite::StreamExt;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use thegraph_core::alloy::signers::local::PrivateKeySigner;
use tokio::task::JoinSet;
use tracing_subscriber::EnvFilter;

use self::signer::Eip712Signer;

mod config;
mod indexers;
mod network;
mod rpc_server;
mod signer;
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
        let domain = eip712_domain(conf.signer.chain_id);
        Eip712Signer::new(private_key_signer, private_key_signer_address, domain)
    };

    //- The DB connection pool component
    let db = {
        let mut conn_options: PgConnectOptions =
            conf.db.url.as_str().parse().expect("Invalid DB URL");
        conn_options = conn_options
            .username(&conf.db.username)
            .password(&conf.db.password);

        PgPoolOptions::new()
            .max_connections(conf.db.max_connections.unwrap_or(10))
            .connect_with(conn_options)
            .await
    }?;

    //- The queue component
    let queue = PgQueue::with_max_attempts(db.clone(), 3);

    // The registry component
    let registry = PgRegistry::new(db);

    //- The indexer client component
    let indexer_client = {
        // TODO: Initialize the actual indexer client
        indexers::DummyDipsIndexerClient
    };

    //- The network service
    let (network_handle, network_service) = {
        let network_subgraph_url = conf
            .network
            .gateway_url
            .join(&format!(
                "/api/deployments/id/{}/",
                conf.network.deployment_id
            ))
            .expect("invalid network subgraph URL");

        let network_subgraph_client = network::subgraph::client::Client::new(
            reqwest::Client::new(),
            network_subgraph_url,
            conf.network.api_key.to_string(),
        );
        network::service::new(network_subgraph_client, conf.network.update_interval)
    };

    // network_handle.wait_ready().await; TODO: Wait for the network service to be ready (with timeout)

    //- The network provider component
    let network_provider = network::provider::NetworkProviderService::new(network_handle.clone());

    //- The IISA service
    let (iisa_handle, iisa_service) = {
        let config = iisa::service::Config {
            geoip_auth: conf.iisa.geoip_auth.to_string(),
            bigquery_project_id: conf.iisa.bigquery_project_id.clone(),
            bigquery_region: conf.iisa.bigquery_region.clone(),
        };
        iisa::service::new(config)
    };

    //- The worker service
    let (worker_handle, worker_service) = {
        let context = worker::Context {
            queue: queue.clone(),
            network: network_provider.clone(),
            registry: registry.clone(),
            indexer_client: indexer_client.clone(),
            iisa: iisa_handle.clone(),
        };

        worker::service::new(context)
    };

    //- The admin RPC service
    let (admin_rpc_handle, admin_rpc_service) = {
        let context = rpc_server::CtxBuilder::new()
            .with_worker(queue.clone())
            .with_registry(registry.clone())
            .with_allowlist(conf.admin.allowlist)
            .with_signer(signer)
            .with_max_candidates(3)
            .build();

        let config = rpc_server::service::HttpConfig {
            http_port: conf.admin.listen_addr.port(),
        };

        rpc_server::service::new_admin_rpc_service(config, context)
    };

    // Construct the task tree
    let mut task_tree: JoinSet<anyhow::Result<()>> = JoinSet::new();

    let network_task_handle = task_tree.spawn(network_service);
    tracing::debug!(task_id=%network_task_handle.id(), "network service started");

    let iisa_task_handle = task_tree.spawn_blocking(iisa_service);
    tracing::debug!(task_id=%iisa_task_handle.id(), "IISA service started");

    let worker_task_handle = task_tree.spawn(worker_service);
    tracing::debug!(task_id=%worker_task_handle.id(), "worker service started");

    let admin_rpc_task_handle = task_tree.spawn(admin_rpc_service);
    tracing::debug!(task_id=%admin_rpc_task_handle.id(), "admin RPC service started");

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

        // Stop all services
        admin_rpc_handle.stop();
        // indexers_rpc_handle.stop(); (todo !?)
        worker_handle.stop().await;
        iisa_handle.stop().await;
        network_handle.stop().await;
        // network_provider_handle.stop().await; (todo !?)

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
                tracing::info!("received signal {:?}", signal);
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
