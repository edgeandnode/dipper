use async_signal::{Signal, Signals};
use futures_lite::StreamExt;
use smol::io;
use thiserror::Error;

pub mod config;

pub enum AppSignal {
    Shutdown,
}

#[derive(Error, Debug)]
pub enum SignalHandlerError {
    #[error("Failed to create signal receiver")]
    SignalReceiverError(io::Error),
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
