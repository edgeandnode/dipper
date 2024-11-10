use async_signal::{Signal, Signals};
use futures_lite::StreamExt;
use thiserror::Error;
use tracing::level_filters::LevelFilter;

mod config;
mod http_server;
mod indexers;
mod network;
mod signer;
mod worker;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[derive(Clone)]
struct AppState {}

impl AppState {
    fn new() -> Self {
        Self {}
    }
}

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    let opts = config::StartArgs::parse_and_merge()?;

    // Set up logging
    tracing_subscriber::fmt()
        .with_max_level(opts.log_level.unwrap_or(LevelFilter::INFO))
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    tracing::info!("starting dipper-service");

    todo!("start the service");
}

pub enum AppSignal {
    Shutdown,
}

#[derive(Error, Debug)]
pub enum SignalHandlerError {
    #[error("Failed to create signal receiver")]
    SignalReceiverError(std::io::Error),
}

pub async fn signal_task() -> Result<AppSignal, SignalHandlerError> {
    let signal_list = &[Signal::Term, Signal::Int, Signal::Quit, Signal::Abort];
    let mut signals = Signals::new(signal_list).map_err(SignalHandlerError::SignalReceiverError)?;
    while let Some(Ok(signal)) = signals.next().await {
        match signal {
            s if signal_list.contains(&s) => return Ok(AppSignal::Shutdown),
            _ => {}
        }
    }

    // fallthrough
    Ok(AppSignal::Shutdown)
}
