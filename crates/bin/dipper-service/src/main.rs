use axum::{
    async_trait, body::Body, extract::FromRequest, http::Request, routing::get, Extension, Router,
};
use dipper_common::{
    db::DbHandle,
    models::{Indexer, Key},
};
use dipper_service::config::StartArgs;
use log::LevelFilter;

#[derive(Clone)]
struct AppState {
    db: DbHandle,
}

impl AppState {
    fn new(db: DbHandle) -> Self {
        Self { db }
    }
}

#[async_trait]
impl<S> FromRequest<S> for AppState
where
    S: Send + Sync,
{
    type Rejection = ();

    async fn from_request(req: Request<Body>, _: &S) -> Result<Self, Self::Rejection> {
        let db = req.extensions().get::<AppState>().unwrap().db.clone();
        Ok(AppState::new(db))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = StartArgs::parse_and_merge()?;
    simple_logger::SimpleLogger::new()
        .with_level(opts.log_level.unwrap_or(LevelFilter::Info))
        .init()?;

    log::info!("starting dipper-service");
    let db = DbHandle::load_at(&opts.db_path.unwrap()).await.unwrap();
    let app_state = AppState::new(db);
    let app = Router::new()
        .route(
            "/",
            get(|extension: Extension<AppState>| async {
                log::info!("dipper GET /");
                let Extension(app_state) = extension;
                let indexers: Option<Vec<Indexer>> =
                    app_state.db.get(Key::from("indexers")).expect("db error");
                axum::Json(indexers.unwrap_or_default())
            }),
        )
        .layer(tower::ServiceBuilder::new().layer(Extension(app_state)));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:9091").await.unwrap();

    let signal_task = async {
        let _signal = dipper_service::signal_task().await.unwrap();
    };
    let _app_task = axum::serve(listener, app)
        .with_graceful_shutdown(signal_task)
        .await;
    Ok(())
}
