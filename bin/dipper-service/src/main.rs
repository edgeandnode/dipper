use std::{env, path::PathBuf, sync::Arc};

use async_signal::{Signal, Signals};
use dipper_iisa::{self as iisa};
use futures_lite::StreamExt;
use thegraph_core::alloy::signers::local::PrivateKeySigner;
use tokio::task::JoinSet;
use tracing_subscriber::EnvFilter;

use self::{
    config::DEFAULT_MAX_CANDIDATES, registry::RegistryProvider, signing::eip712::Eip712Signer,
    worker::queue::QueueImpl,
};

mod admin_rpc_server;
mod cancel_dispatch;
mod chain_client;
mod config;
mod db;
mod indexer_rpc_client;
mod network;
mod registry;
mod signing;
mod worker;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

/// Extra attempts to fetch the RecurringCollector EIP-712 domain at startup after the first,
/// to ride out a transient RPC blip before dipper refuses to run. Each attempt already fails
/// over across every configured provider with its own backoff.
const RCA_DOMAIN_FETCH_MAX_RETRIES: u32 = 5;

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

    // Reject a config the protocol-managed path can't run with before building
    // anything from it.
    if let Err(err) = conf.dips.validate() {
        anyhow::bail!("invalid dips agreement config: {err}");
    }

    // TODO: Decouple the config file format from the internal representation
    let (agreement_conf, pricing_table) = conf.dips.into();

    // Clones for services that outlive the worker's move of `agreement_conf`;
    // they need the manager address for cancel dispatch.
    let chain_listener_agreement_conf = agreement_conf.clone();
    let liveness_agreement_conf = agreement_conf.clone();
    let escrow_reconciler_agreement_conf = agreement_conf.clone();

    // Canonical chain id and RecurringCollector address, read once and shared by the
    // admin signer, the gRPC proposal signer, and the on-chain chain client so their
    // EIP-712 domains and on-chain calls can't drift to different values.
    let chain_id = conf.signer.chain_id;
    let recurring_collector = agreement_conf.recurring_collector;

    // The chain client is mandatory: dipper signs RCAs under the RecurringCollector's EIP-712
    // domain and settles offers on-chain, so a deployed contract and reachable RPC are required.
    // Fetch the domain (EIP-5267) with retries, then refuse to start rather than sign unverified.
    let rca_domain = {
        let cfg = match &conf.chain_client {
            Some(cfg) if cfg.enabled => cfg,
            _ => anyhow::bail!(
                "chain_client must be enabled with at least one RPC provider and a deployed \
                 recurring_collector: dipper fetches the RecurringCollector EIP-712 domain \
                 on-chain and cannot sign agreements without it"
            ),
        };
        let mut attempt: u32 = 0;
        loop {
            match chain_client::fetch_rca_eip712_domain(cfg, chain_id, recurring_collector).await {
                Ok(domain) => break domain,
                Err(err) if attempt < RCA_DOMAIN_FETCH_MAX_RETRIES => {
                    attempt += 1;
                    let delay = std::time::Duration::from_secs(2u64.pow(attempt.min(5)));
                    tracing::warn!(
                        attempt,
                        delay_secs = delay.as_secs(),
                        error = %err,
                        "RCA EIP-712 domain fetch failed, retrying"
                    );
                    tokio::time::sleep(delay).await;
                }
                Err(err) => anyhow::bail!(
                    "failed to fetch the RecurringCollector EIP-712 domain after {} attempts \
                     across all configured RPC providers: {err}",
                    RCA_DOMAIN_FETCH_MAX_RETRIES + 1
                ),
            }
        }
    };
    let rca_domain = Arc::new(std::sync::RwLock::new(rca_domain));

    // Background refresh so a running dipper follows an in-place contract upgrade
    // without a restart. Refresh failures keep the current domain and only warn.
    if let Some(cfg) = conf.chain_client.as_ref().filter(|cfg| cfg.enabled) {
        let cfg = cfg.clone();
        let rca_domain = Arc::clone(&rca_domain);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(cfg.domain_refresh_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await; // the first tick fires immediately; skip it
            loop {
                ticker.tick().await;
                if let Err(err) = chain_client::refresh_rca_eip712_domain(
                    &cfg,
                    chain_id,
                    recurring_collector,
                    &rca_domain,
                )
                .await
                {
                    tracing::warn!(
                        error = %err,
                        "RCA EIP-712 domain refresh failed; keeping the current domain"
                    );
                }
            }
        });
    }

    // Initialize the different components

    //- The wallet signer. The admin Eip712Signer is verify-only — it recovers
    //  signers from inbound admin-RPC messages. The DIPs indexer client below signs
    //  each outbound proposal, so dipper does still produce outbound signatures.
    let wallet_signer = PrivateKeySigner::from_signing_key(conf.signer.secret_key.as_ref().into());
    tracing::info!(address=%wallet_signer.address(), "Signer wallet imported");

    let signer = {
        let domain = dipper_rpc::admin::eip712_domain();
        Arc::new(Eip712Signer::new(wallet_signer.address(), chain_id, domain))
    };

    //- DB connect and run migrations
    let db_conn = db::connect(&conf.db).await?;
    tracing::info!(db_url=%conf.db.url, "initialized DB connection pool");

    dipper_pgmq::run_db_migrations(&db_conn).await?;
    dipper_pgregistry::run_db_migrations(&db_conn).await?;
    tracing::info!("applied DB migrations");

    //- The message queue component
    let queue = QueueImpl::new(db_conn.clone());

    //- The registry component
    let registry = RegistryProvider::new(db_conn.clone());

    //- The indexer client component (signs each outbound proposal; see DipsIndexerClient).
    let indexer_client = indexer_rpc_client::DipsIndexerClient::with_config(
        conf.indexer_client,
        wallet_signer.clone(),
        rca_domain.clone(),
    );

    //- The network services
    let (network_topology_handle, network_topology_service) = {
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

        // Fetch the initial topology snapshot, retrying with exponential backoff.
        // The gateway may be temporarily unavailable (e.g. during a chain halt).
        let topology_init_snapshot = {
            let mut attempt: u32 = 0;
            loop {
                match network::service::topology::fetch_snapshot(&network_subgraph_client).await {
                    Ok(s) => break s,
                    Err(err) => {
                        attempt += 1;
                        let delay = std::time::Duration::from_secs(2u64.pow(attempt.min(5)));
                        tracing::info!(
                            attempt,
                            delay_secs = delay.as_secs(),
                            error = %err,
                            "initial topology fetch failed, retrying"
                        );
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        };

        network::service::topology::new(
            network_subgraph_client,
            conf.network.update_interval,
            topology_init_snapshot,
        )
    };
    tracing::info!("initialized Graph network service");

    //- The network provider component
    let network_provider =
        network::provider::NetworkProviderService::new(network_topology_handle.clone());

    //- The IISA HTTP client
    // Verify IISA is reachable before accepting traffic (deployment ordering)
    // Retry a few times to handle momentary network issues during startup
    let iisa_config = iisa::HttpClientConfig {
        request_timeout: conf.iisa.request_timeout,
        connect_timeout: conf.iisa.connect_timeout,
        max_retries: conf.iisa.max_retries,
        push_token: conf.iisa.push_token.as_ref().map(|t| t.0.clone()),
    };
    let iisa_client =
        iisa::HttpIisaClient::with_config(conf.iisa.endpoint.to_string(), iisa_config);
    let mut iisa_healthy = false;
    for attempt in 1..=3 {
        if iisa_client.health_check().await.unwrap_or(false) {
            iisa_healthy = true;
            break;
        }
        if attempt < 3 {
            tracing::warn!(
                endpoint=%conf.iisa.endpoint,
                attempt=%attempt,
                "IISA health check failed, retrying in 2s"
            );
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    }
    if !iisa_healthy {
        anyhow::bail!(
            "IISA service is not reachable at {} after 3 attempts",
            conf.iisa.endpoint
        );
    }
    tracing::info!(endpoint=%conf.iisa.endpoint, "IISA service is healthy");

    //- The graph networks registry (maps chain IDs to canonical network names)
    let networks_registry = Arc::new(
        graph_networks_registry::NetworksRegistry::from_latest_version()
            .await
            .expect("Failed to fetch graph networks registry"),
    );
    tracing::info!(
        version=%networks_registry.version,
        networks=%networks_registry.networks.len(),
        "loaded graph networks registry"
    );

    let additional_networks = Arc::new(conf.additional_networks);

    // Application services

    //- The chain client (for on-chain transactions). Required: presence validated above.
    let chain_client: Arc<dyn chain_client::ChainClient + Send + Sync> = {
        let cfg = conf
            .chain_client
            .as_ref()
            .expect("chain_client presence validated at startup");
        // Extract the secret key bytes from the signer config
        let secret_bytes: [u8; 32] = conf.signer.secret_key.as_ref().to_bytes().into();
        let client = chain_client::AlloyChainClient::new(
            cfg,
            chain_id,
            recurring_collector,
            agreement_conf.recurring_agreement_manager(),
            &secret_bytes,
        )
        .expect("Failed to create AlloyChainClient");
        tracing::info!(
            chain_id,
            "initialized AlloyChainClient for on-chain transactions"
        );
        Arc::new(client)
    };

    //- Protocol-managed mode requires dipper's signer to hold AGREEMENT_MANAGER_ROLE on
    //  the manager; without it every offer and cancel reverts on-chain. Fail fast at
    //  startup instead of discovering the missing grant one reverted offer at a time.
    if let Some(cfg) = conf.chain_client.as_ref().filter(|cfg| cfg.enabled) {
        let manager = agreement_conf.recurring_agreement_manager();
        chain_client::verify_signer_has_agreement_manager_role(
            cfg,
            manager,
            wallet_signer.address(),
        )
        .await
        .map_err(|err| anyhow::anyhow!("AGREEMENT_MANAGER_ROLE preflight failed: {err}"))?;
    }

    //- The entity count cache (shared with worker jobs for optimistic fee estimation)
    let entity_count_cache = network::service::entity_count_cache::new_cache();
    let entity_count_handle = match conf.chain_listener {
        Some(ref cl_conf) if cl_conf.enabled => {
            let (handle, fut) = network::service::entity_count_cache::new(
                network::service::entity_count_cache::Ctx {
                    cache: entity_count_cache.clone(),
                    endpoint: cl_conf.subgraph_endpoint.clone(),
                    // Entity counts change slowly (once per collection epoch).
                    // Refresh hourly to balance freshness and query cost.
                    interval: std::time::Duration::from_secs(3600),
                },
            );
            tokio::spawn(fut);
            Some(handle)
        }
        _ => None,
    };

    // Shared notify: worker signals the chain_listener when proposals are
    // dispatched so it switches from 300s idle polling to 5s immediately.
    let chain_listener_notify = Arc::new(tokio::sync::Notify::new());

    //- The worker service
    let (worker_handle, worker_service) = {
        // Each loop can hold up to three pooled connections at once and shares
        // the pool with the registry and background reconcilers. Refuse to start
        // when the loops would crowd it, rather than deadlocking under load.
        const CONNECTIONS_PER_LOOP: usize = 3;
        const SHARED_SERVICES_HEADROOM: usize = 2;
        if conf.worker_concurrency == 0 {
            anyhow::bail!("worker_concurrency must be at least 1");
        }
        let max_conns = conf
            .db
            .max_connections
            .unwrap_or(db::DEFAULT_MAX_CONNECTIONS) as usize;
        // Saturating so an absurd configured value can't overflow and slip past
        // the check below (the very check meant to catch oversized sizing).
        let required_conns = conf
            .worker_concurrency
            .saturating_mul(CONNECTIONS_PER_LOOP)
            .saturating_add(SHARED_SERVICES_HEADROOM);
        if required_conns > max_conns {
            anyhow::bail!(
                "worker_concurrency ({}) needs at least {} db connections (each loop may hold {} \
                 for its listener, open job transaction and a registry query, plus {} reserved for \
                 the registry and background services), but db.max_connections is {}; raise \
                 db.max_connections to at least {} or lower worker_concurrency",
                conf.worker_concurrency,
                required_conns,
                CONNECTIONS_PER_LOOP,
                SHARED_SERVICES_HEADROOM,
                max_conns,
                required_conns,
            );
        }

        // A single global reassess lock, shared across all worker loops.
        let reassess_lock = Arc::new(tokio::sync::Mutex::new(()));
        let unresponsive_breaker = Arc::new(worker::UnresponsiveBreaker::new());
        let dips_accepting_cache = worker::DipsAcceptingCache::new(std::time::Duration::from_secs(
            agreement_conf.dips_accepting_cache_ttl_seconds(),
        ));
        let ctx = worker::Ctx {
            queue,
            signer: signer.clone(),
            agreement_conf,
            rca_domain: rca_domain.clone(),
            pricing_table,
            registry: registry.clone(),
            network: network_provider.clone(),
            client: indexer_client,
            iisa: iisa_client.clone(),
            chain_client: chain_client.clone(),
            networks_registry,
            additional_networks,
            entity_count_cache,
            chain_listener_notify: chain_listener_notify.clone(),
            bypass_chain_clock_defenses: conf
                .chain_listener
                .as_ref()
                .map(|c| c.bypass_chain_clock_defenses)
                .unwrap_or(false),
            chain_listener_chain_id: conf.chain_listener.as_ref().map(|c| c.chain_id),
            reassess_lock,
            unresponsive_breaker,
            dips_accepting_cache,
            concurrency: conf.worker_concurrency,
        };
        worker::service::new(ctx)
    };
    tracing::info!("initialized Worker service");

    //- The reassignment service (optional, enabled by config)
    let reassignment_handle = match conf.reassignment {
        Some(ref reassignment_conf) if reassignment_conf.enabled => {
            let ctx = network::service::reassignment::Ctx {
                registry: registry.clone(),
                worker_queue: worker_handle.queue().clone(),
                config: reassignment_conf.clone(),
            };
            let (handle, service) = network::service::reassignment::new(ctx);
            Some((handle, service))
        }
        _ => None,
    };

    //- The expiration service (optional, enabled by config)
    let expiration_handle = match conf.expiration {
        Some(ref expiration_conf) if expiration_conf.enabled => {
            let ctx = network::service::expiration::Ctx {
                registry: registry.clone(),
                worker_queue: worker_handle.queue().clone(),
                config: expiration_conf.clone(),
                chain_id: conf.chain_listener.as_ref().map(|c| c.chain_id),
            };
            let (handle, service) = network::service::expiration::new(ctx);
            Some((handle, service))
        }
        _ => None,
    };

    //- The chain listener service (optional, enabled by config)
    // Monitors on-chain events for agreement acceptance/cancellation via subgraph
    let chain_listener_handle = match conf.chain_listener {
        Some(ref chain_listener_conf) if chain_listener_conf.enabled => {
            // The subgraph filters agreements by on-chain payer, which is the
            // RecurringAgreementManager contract.
            let listener_payer_address =
                chain_listener_agreement_conf.recurring_agreement_manager();

            // Create the subgraph event source
            let event_source_config = network::service::chain_events::SubgraphEventSourceConfig {
                endpoint: chain_listener_conf.subgraph_endpoint.clone(),
                api_key: chain_listener_conf.subgraph_api_key.clone(),
                payer_address: listener_payer_address,
                request_timeout: chain_listener_conf.request_timeout,
                max_retries: chain_listener_conf.max_retries,
                wall_clock_skew_tolerance_secs: chain_listener_conf.wall_clock_skew_tolerance_secs,
                bypass_chain_clock_defenses: chain_listener_conf.bypass_chain_clock_defenses,
            };
            let event_source =
                network::service::chain_events::SubgraphEventSource::new(event_source_config);

            let ctx = network::service::chain_listener::Ctx {
                registry: registry.clone(),
                worker_queue: worker_handle.queue().clone(),
                event_source,
                chain_client: chain_client.clone(),
                agreement_conf: chain_listener_agreement_conf.clone(),
                config: chain_listener_conf.clone(),
                signer_address: signer.address(),
                chain_listener_notify: chain_listener_notify.clone(),
            };
            let (handle, service) = network::service::chain_listener::new(ctx);
            Some((handle, service))
        }
        _ => None,
    };

    //- The liveness checker service (optional, enabled by config)
    // Detects indexers who silently stop indexing active AcceptedOnChain agreements
    let liveness_checker_handle = match conf.liveness_checker {
        Some(ref lc_conf) if lc_conf.enabled => {
            let ctx = network::service::liveness_checker::Ctx {
                registry: registry.clone(),
                worker_queue: worker_handle.queue().clone(),
                chain_client: chain_client.clone(),
                network: network_provider.clone(),
                agreement_conf: liveness_agreement_conf.clone(),
                config: lc_conf.clone(),
            };
            let (handle, service) = network::service::liveness_checker::new(ctx);
            Some((handle, service))
        }
        _ => None,
    };

    //- The escrow reconciler service (AgreementManager mode only)
    // Reclaims orphaned manager-owned escrow the indexer has no reason to touch.
    let escrow_reconciler_handle = if network::service::escrow_reconciler::should_run(
        conf.escrow_reconciler.as_ref(),
        &escrow_reconciler_agreement_conf,
    ) {
        let er_conf = conf.escrow_reconciler.clone().unwrap_or_default();
        let ctx = network::service::escrow_reconciler::Ctx {
            registry: registry.clone(),
            chain_client: chain_client.clone(),
            config: er_conf,
            collector: escrow_reconciler_agreement_conf.recurring_collector(),
        };
        let (handle, service) = network::service::escrow_reconciler::new(ctx);
        Some((handle, service))
    } else {
        None
    };

    //- The admin RPC service
    let (admin_rpc_handle, admin_rpc_service) = {
        let config = admin_rpc_server::service::Config {
            listen_addr: conf.admin_rpc.listen_addr,
        };

        let ctx = admin_rpc_server::Ctx {
            signer: signer.clone(),
            gateway_operator_allowlist: Arc::new(conf.admin_rpc.gateway_operator_allowlist),
            max_candidates: DEFAULT_MAX_CANDIDATES,
            registry: registry.clone(),
            worker: worker_handle.queue().clone(),
        };

        admin_rpc_server::service::new(config, ctx)
    };
    tracing::info!("initialized Admin RPC service");

    // Construct the task tree
    let mut task_tree = JoinSet::new();

    let network_topology_task_handle = task_tree.spawn(network_topology_service);
    tracing::debug!(task_id=%network_topology_task_handle.id(), "Graph network topology service started");

    let worker_task_handle = task_tree.spawn(worker_service);
    tracing::debug!(task_id=%worker_task_handle.id(), "Worker service started");

    // Spawn the reassignment service if enabled
    let reassignment_stop_handle = if let Some((handle, service)) = reassignment_handle {
        let task_handle = task_tree.spawn(service);
        tracing::debug!(task_id=%task_handle.id(), "Reassignment service started");
        Some(handle)
    } else {
        None
    };

    // Spawn the expiration service if enabled
    let expiration_stop_handle = if let Some((handle, service)) = expiration_handle {
        let task_handle = task_tree.spawn(service);
        tracing::debug!(task_id=%task_handle.id(), "Expiration service started");
        Some(handle)
    } else {
        None
    };

    // Spawn the liveness checker service if enabled
    let liveness_checker_stop_handle = if let Some((handle, service)) = liveness_checker_handle {
        let task_handle = task_tree.spawn(service);
        tracing::debug!(task_id=%task_handle.id(), "Liveness checker service started");
        Some(handle)
    } else {
        None
    };

    // Spawn the chain listener service if enabled
    let chain_listener_stop_handle = if let Some((handle, service)) = chain_listener_handle {
        let task_handle = task_tree.spawn(service);
        tracing::debug!(task_id=%task_handle.id(), "Chain listener service started");
        Some(handle)
    } else {
        None
    };

    // Spawn the escrow reconciler service if enabled
    let escrow_reconciler_stop_handle = if let Some((handle, service)) = escrow_reconciler_handle {
        let task_handle = task_tree.spawn(service);
        tracing::debug!(task_id=%task_handle.id(), "Escrow reconciler service started");
        Some(handle)
    } else {
        None
    };

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

        // Stop all services in reverse dependency order, so a service is stopped before the
        // services it depends on.
        tracing::trace!("stopping Admin RPC service");
        admin_rpc_handle.stop().await;
        tracing::trace!("stopped Admin RPC service");

        // Stop reassignment service before worker (it depends on worker queue)
        if let Some(handle) = reassignment_stop_handle {
            tracing::trace!("stopping Reassignment service");
            handle.stop().await;
            tracing::trace!("stopped Reassignment service");
        }

        // Stop expiration service before worker (it depends on worker queue)
        if let Some(handle) = expiration_stop_handle {
            tracing::trace!("stopping Expiration service");
            handle.stop().await;
            tracing::trace!("stopped Expiration service");
        }

        // Stop liveness checker service before worker (it depends on worker queue)
        if let Some(handle) = liveness_checker_stop_handle {
            tracing::trace!("stopping Liveness checker service");
            handle.stop().await;
            tracing::trace!("stopped Liveness checker service");
        }

        // Stop chain listener service before worker (it depends on worker queue)
        if let Some(handle) = chain_listener_stop_handle {
            tracing::trace!("stopping Chain listener service");
            handle.stop().await;
            tracing::trace!("stopped Chain listener service");
        }

        // Stop escrow reconciler service before the DB pool closes
        if let Some(handle) = escrow_reconciler_stop_handle {
            tracing::trace!("stopping Escrow reconciler service");
            handle.stop().await;
            tracing::trace!("stopped Escrow reconciler service");
        }

        // Stop entity count cache service
        if let Some(handle) = entity_count_handle {
            tracing::trace!("stopping Entity count cache service");
            handle.stop().await;
            tracing::trace!("stopped Entity count cache service");
        }

        tracing::trace!("stopping Worker service");
        worker_handle.stop().await;
        tracing::trace!("stopped Worker service");

        tracing::trace!("stopping Graph network service");
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
