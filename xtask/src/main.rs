use clap::{Parser, Subcommand};

#[derive(Debug, Subcommand)]
enum Command {
    #[clap(name = "todo", about = "todo")]
    Todo,

    #[clap(name = "integration-tests", about = "run all integration tests")]
    IntegrationTests,

    #[clap(name = "integration-test", about = "run a specific integration test")]
    IntegrationTest { test_name: String },
}

#[derive(Debug, Parser)]
struct Arguments {
    #[command(subcommand)]
    command: Command,
}

fn main() -> Result<(), anyhow::Error> {
    let args = Arguments::try_parse()?;
    match args.command {
        Command::Todo => {
            println!("todo");
        }
        Command::IntegrationTests => {
            println!("integration test");

            duct::cmd!(
                "cargo",
                "test",
                "-p",
                "integration-tests",
                "--features",
                "integration-tests",
                "--",
                "--nocapture",
            )
            .run()?;
        }
        Command::IntegrationTest { test_name } => {
            println!("integration test: {}", test_name);

            duct::cmd!(
                "cargo",
                "test",
                "-p",
                "integration-tests",
                "--features",
                "integration-tests",
                "--",
                "--nocapture",
                test_name,
            )
            .run()?;
        }
    }
    Ok(())
}
