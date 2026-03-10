use std::{
    env,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use tiny_fs::{bootstrap, serve_until, Config};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};

struct TestServer {
    addr: std::net::SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    handle: Option<tokio::task::JoinHandle<()>>,
    data_dir: PathBuf,
}

fn test_config(data_dir: PathBuf) -> Config {
    Config::new("127.0.0.1".parse().expect("valid ip"), 0, data_dir)
}

fn unique_temp_path(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should move forward")
        .as_nanos();
    env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()))
}

impl TestServer {
    async fn start(prefix: &str) -> Self {
        let data_dir = unique_temp_path(prefix);
        let state = bootstrap(test_config(data_dir.clone())).expect("state should bootstrap");

        let listener = TcpListener::bind(state.config.bind_addr())
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("listener should expose addr");
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let handle = tokio::spawn(async move {
            serve_until(listener, state, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("server should run");
        });

        Self {
            addr,
            shutdown: Some(shutdown_tx),
            handle: Some(handle),
            data_dir,
        }
    }

    async fn send(&self, path: &str) -> String {
        let mut stream = TcpStream::connect(self.addr)
            .await
            .expect("client should connect");
        let request = format!(
            "GET {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );

        stream
            .write_all(request.as_bytes())
            .await
            .expect("request should be written");

        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .await
            .expect("response should be readable");
        response
    }

    async fn stop(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(handle) = self.handle.take() {
            handle.await.expect("server task should stop");
        }
        if self.data_dir.exists() {
            std::fs::remove_dir_all(&self.data_dir).expect("test directory should be removable");
        }
    }
}

#[tokio::test]
async fn health_endpoint_returns_ok() {
    let server = TestServer::start("tiny-fs-health-test").await;
    let response = server.send("/health").await;

    assert!(
        response.starts_with("HTTP/1.1 200 OK"),
        "unexpected response: {response}"
    );
    assert!(
        response.contains(r#""status":"ok""#),
        "unexpected response body: {response}"
    );
    assert!(
        response.contains(r#""service":"tiny-fs""#),
        "unexpected response body: {response}"
    );

    server.stop().await;
}

#[tokio::test]
async fn console_assets_are_served() {
    let server = TestServer::start("tiny-fs-console-test").await;

    let index = server.send("/").await;
    let index_lower = index.to_ascii_lowercase();
    assert!(
        index.starts_with("HTTP/1.1 200 OK"),
        "unexpected response: {index}"
    );
    assert!(
        index_lower.contains("content-type: text/html; charset=utf-8"),
        "html content type missing: {index}"
    );
    assert!(
        index.contains("<title>Tiny FS 控制台</title>"),
        "console title missing: {index}"
    );
    assert!(
        index.contains("/ui/app.js"),
        "console script reference missing: {index}"
    );
    assert!(
        index.contains("id=\"previewPanel\""),
        "console preview panel missing: {index}"
    );

    let styles = server.send("/ui/styles.css").await;
    let styles_lower = styles.to_ascii_lowercase();
    assert!(
        styles.starts_with("HTTP/1.1 200 OK"),
        "unexpected response: {styles}"
    );
    assert!(
        styles_lower.contains("content-type: text/css; charset=utf-8"),
        "css content type missing: {styles}"
    );
    assert!(
        styles.contains(".app-shell"),
        "css body missing expected selector: {styles}"
    );

    let script = server.send("/ui/app.js").await;
    let script_lower = script.to_ascii_lowercase();
    assert!(
        script.starts_with("HTTP/1.1 200 OK"),
        "unexpected response: {script}"
    );
    assert!(
        script_lower.contains("content-type: application/javascript; charset=utf-8"),
        "script content type missing: {script}"
    );
    assert!(
        script.contains("loadBuckets"),
        "script body missing expected function: {script}"
    );
    assert!(
        script.contains("loadObjectPreview"),
        "script body missing preview loader: {script}"
    );

    server.stop().await;
}
