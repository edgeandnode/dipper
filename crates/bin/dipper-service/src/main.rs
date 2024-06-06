use dipper_common::{
    db::DbHandle,
    models::{Indexer, Key},
};
use dipper_service::{config::StartArgs, AppSignal};
use log::LevelFilter;
use tide::{convert::json, Request};

#[derive(Clone)]
struct AppState {
    db: DbHandle,
}

impl AppState {
    fn new(db: DbHandle) -> Self {
        Self { db }
    }
}

fn main() -> anyhow::Result<()> {
    let opts = StartArgs::parse_and_merge()?;
    simple_logger::SimpleLogger::new()
        .with_level(opts.log_level.unwrap_or(LevelFilter::Info))
        .init()?;

    log::info!("starting dipper-service");
    smol::block_on(async move {
        let db = DbHandle::load_at(&opts.db_path.unwrap()).await.unwrap();
        let app_state = AppState::new(db);
        let mut app = tide::with_state(app_state);
        app.at("/").get(get_indexers);

        let app_task = app.listen("localhost:9091");
        let signal_task = dipper_service::signal_task();
        async_select::select! {
            _ = app_task => {},
            Ok(AppSignal::Shutdown) = signal_task => {
                log::info!("received shutdown signal");
            }
        }
    });
    Ok(())
}

async fn get_indexers(req: Request<AppState>) -> tide::Result {
    let state = req.state();
    let indexers: Option<Vec<Indexer>> = state.db.get(Key::from_str("indexers"))?;
    Ok(json!(indexers).into())
}
