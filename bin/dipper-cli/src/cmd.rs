pub mod agreements;
mod common;
pub mod indexings;
pub mod init;

use clap::{Command, command};
pub use common::load_conf;

/// Create the DIPs CLI command line interface
pub fn cli() -> Command {
    command!()
        .subcommands(&[
            init::init_cmd(),
            indexings::indexings_cmd(),
            agreements::agreements_cmd(),
        ])
        .infer_long_args(true)
        .infer_subcommands(true)
}
