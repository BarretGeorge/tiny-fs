use std::future::Future;

use tokio::net::TcpListener;

use crate::{api, AppResult, AppState, Config, StorageLayout};

pub fn bootstrap(config: Config) -> AppResult<AppState> {
    let storage = StorageLayout::initialize(config.data_dir.clone())?;
    AppState::new(config, storage)
}

pub async fn serve(listener: TcpListener, state: AppState) -> AppResult<()> {
    serve_until(listener, state, async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await
}

pub async fn serve_until<F>(listener: TcpListener, state: AppState, shutdown: F) -> AppResult<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    axum::serve(listener, api::router(state))
        .with_graceful_shutdown(shutdown)
        .await?;
    Ok(())
}
