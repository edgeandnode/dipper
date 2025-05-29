mod agreements;
mod common;
mod requests;
mod init;
mod result;

use self::result::Result;

/// Create and execute the DIPs CLI command line interface
pub async fn run() -> Result<()> {
    let matches = clap::command!()
        .subcommands([init::cmd(), requests::cmd(), agreements::cmd()])
        .infer_long_args(true)
        .infer_subcommands(true)
        .get_matches();

    match matches.subcommand() {
        Some(("init", matches)) => init::run(matches).await,
        Some(("requests", matches)) => requests::run(matches).await,
        Some(("agreements", matches)) => agreements::run(matches).await,
        _ => Err(anyhow::anyhow!("No command specified").into()),
    }
}
