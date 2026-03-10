use tokio::net::TcpListener;

use tiny_fs::{bootstrap, serve, serve_until, AppError, Config};

#[tokio::main]
async fn main() -> Result<(), AppError> {
    let config = Config::from_env()?;
    let state = bootstrap(config.clone())?;
    
    if config.is_cluster_mode() {
        state.start_cluster_services().await?;
        eprintln!(
            "tiny-fs starting in CLUSTER mode: node_id={}, seed_nodes={}",
            config.cluster.node_id,
            config.cluster.seed_nodes_string()
        );
    } else {
        eprintln!("tiny-fs starting in SINGLE NODE mode");
    }
    
    let listener = TcpListener::bind(config.bind_addr()).await?;
    let local_addr = listener.local_addr()?;

    eprintln!(
        "tiny-fs listening on {} with data dir {}",
        local_addr,
        state.storage.root.display()
    );

    serve(listener, state).await
}
