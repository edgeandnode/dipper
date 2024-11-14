mod client;
mod cmd;
mod config;
mod signer;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
pub async fn main() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    // Try to load configuration from .env file
    if let Err(err) = dotenvy::dotenv() {
        if err.not_found() {
            tracing::debug!("No .env file found");
        } else {
            tracing::debug!("Failed to load .env file: {}", err);
        }
    } else {
        tracing::debug!("Loaded .env file");
    }

    // Parse command line arguments and construct the configuration
    match cmd::cli().get_matches().subcommand() {
        Some(("init", matches)) => {
            if let Err(err) = cmd::init::run(matches) {
                eprintln!("Failed to initialize configuration: {err}");
                std::process::exit(1);
            }
        }
        Some(("indexings", matches)) => match matches.subcommand() {
            Some(("list", matches)) => {
                let conf = match cmd::load_conf(matches) {
                    Ok(conf) => conf,
                    Err(err) => {
                        eprintln!("Failed to load configuration: {err}");
                        std::process::exit(1);
                    }
                };
                tracing::debug!("Configuration loaded: {:?}", conf);

                if let Err(err) = cmd::indexings::list(conf).await {
                    eprintln!("Failed to list indexings: {}", err);
                    std::process::exit(1);
                }
            }
            Some(("status", matches)) => {
                let conf = match cmd::load_conf(matches) {
                    Ok(conf) => conf,
                    Err(err) => {
                        eprintln!("Failed to load configuration: {err}");
                        std::process::exit(1);
                    }
                };
                tracing::debug!("Configuration loaded: {:?}", conf);

                if let Err(err) = cmd::indexings::status(conf, matches).await {
                    eprintln!("{}", err);
                    std::process::exit(1);
                }
            }
            Some(("register", matches)) => {
                let conf = match cmd::load_conf(matches) {
                    Ok(conf) => conf,
                    Err(err) => {
                        eprintln!("Failed to load configuration: {err}");
                        std::process::exit(1);
                    }
                };
                tracing::debug!("Configuration loaded: {:?}", conf);

                if let Err(err) = cmd::indexings::register(conf, matches).await {
                    eprintln!("{}", err);
                    std::process::exit(1);
                }
            }
            Some(("cancel", matches)) => {
                let conf = match cmd::load_conf(matches) {
                    Ok(conf) => conf,
                    Err(err) => {
                        eprintln!("Failed to load configuration: {err}");
                        std::process::exit(1);
                    }
                };
                tracing::debug!("Configuration loaded: {:?}", conf);

                if let Err(err) = cmd::indexings::cancel(conf, matches).await {
                    eprintln!("{}", err);
                    std::process::exit(1);
                }
            }
            _ => {
                eprintln!("No indexings command specified");
                std::process::exit(1);
            }
        },
        _ => {
            eprintln!("No command specified");
            std::process::exit(1);
        }
    }
}
