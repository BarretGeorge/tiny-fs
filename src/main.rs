use tokio::net::TcpListener;

use tiny_fs::{bootstrap, serve, AppError, Config};

#[tokio::main]
async fn main() -> Result<(), AppError> {
    let config = Config::from_env()?;
    let state = bootstrap(config.clone())?;
    let listener = TcpListener::bind(config.bind_addr()).await?;
    let local_addr = listener.local_addr()?;

    eprintln!(
        "tiny-fs listening on {} with data dir {}",
        local_addr,
        state.storage.root.display()
    );

    serve(listener, state).await
}
