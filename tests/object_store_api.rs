use std::{
    collections::BTreeMap,
    env,
    net::SocketAddr,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use tiny_fs::{bootstrap, serve_until, Config};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinHandle,
};

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

struct TestServer {
    addr: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<()>>,
    data_dir: PathBuf,
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

    async fn send(
        &self,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> Response {
        let mut stream = TcpStream::connect(self.addr)
            .await
            .expect("client should connect");
        let mut request = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\n");

        let mut has_content_length = false;
        for (name, value) in headers {
            if name.eq_ignore_ascii_case("content-length") {
                has_content_length = true;
            }
            request.push_str(name);
            request.push_str(": ");
            request.push_str(value);
            request.push_str("\r\n");
        }

        if !has_content_length {
            request.push_str(&format!("Content-Length: {}\r\n", body.len()));
        }
        request.push_str("Connection: close\r\n\r\n");

        stream
            .write_all(request.as_bytes())
            .await
            .expect("request head should write");
        stream
            .write_all(body)
            .await
            .expect("request body should write");

        let mut raw = Vec::new();
        stream
            .read_to_end(&mut raw)
            .await
            .expect("response should be readable");

        parse_response(&raw)
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

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
        if self.data_dir.exists() {
            let _ = std::fs::remove_dir_all(&self.data_dir);
        }
    }
}

#[derive(Debug)]
struct Response {
    status_line: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

impl Response {
    fn body_text(&self) -> String {
        String::from_utf8(self.body.clone()).expect("body should be utf-8 in tests")
    }
}

fn parse_response(raw: &[u8]) -> Response {
    let header_end = raw
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("response should contain headers");
    let header_bytes = &raw[..header_end];
    let body = raw[header_end + 4..].to_vec();
    let header_text = std::str::from_utf8(header_bytes).expect("response headers should be utf-8");
    let mut lines = header_text.lines();
    let status_line = lines
        .next()
        .expect("response should have status line")
        .to_string();
    let mut headers = BTreeMap::new();
    for line in lines {
        let (name, value) = line.split_once(':').expect("header should contain colon");
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
    }

    Response {
        status_line,
        headers,
        body,
    }
}

fn json_string_field(body: &str, field: &str) -> String {
    let marker = format!("\"{field}\":\"");
    let start = body
        .find(&marker)
        .unwrap_or_else(|| panic!("field `{field}` missing in body: {body}"))
        + marker.len();
    let tail = &body[start..];
    let end = tail
        .find('"')
        .unwrap_or_else(|| panic!("field `{field}` is not a string in body: {body}"));
    tail[..end].to_string()
}

#[tokio::test]
async fn bucket_and_object_lifecycle_over_http() {
    let server = TestServer::start("tiny-fs-api-test").await;

    let create_bucket = server.send("PUT", "/buckets/photos", &[], &[]).await;
    assert_eq!(create_bucket.status_line, "HTTP/1.1 201 Created");

    let list_buckets = server.send("GET", "/buckets", &[], &[]).await;
    assert_eq!(list_buckets.status_line, "HTTP/1.1 200 OK");
    assert!(list_buckets.body_text().contains(r#""name":"photos""#));

    let put_object = server
        .send(
            "PUT",
            "/objects/photos/docs/readme.txt",
            &[("Content-Type", "text/plain")],
            b"hello world",
        )
        .await;
    assert_eq!(put_object.status_line, "HTTP/1.1 201 Created");
    assert!(put_object
        .body_text()
        .contains(r#""key":"docs/readme.txt""#));

    let overwrite_object = server
        .send(
            "PUT",
            "/objects/photos/docs/readme.txt",
            &[("Content-Type", "text/plain")],
            b"updated",
        )
        .await;
    assert_eq!(overwrite_object.status_line, "HTTP/1.1 200 OK");

    let head_object = server
        .send("HEAD", "/objects/photos/docs/readme.txt", &[], &[])
        .await;
    assert_eq!(head_object.status_line, "HTTP/1.1 200 OK");
    assert_eq!(
        head_object
            .headers
            .get("content-length")
            .expect("head should include content-length"),
        "7"
    );
    assert_eq!(
        head_object
            .headers
            .get("accept-ranges")
            .expect("head should include accept-ranges"),
        "bytes"
    );
    assert_eq!(head_object.body.len(), 0);

    let get_object = server
        .send("GET", "/objects/photos/docs/readme.txt", &[], &[])
        .await;
    assert_eq!(get_object.status_line, "HTTP/1.1 200 OK");
    assert_eq!(get_object.body, b"updated");
    assert_eq!(
        get_object
            .headers
            .get("content-type")
            .expect("get should include content-type"),
        "text/plain"
    );

    let partial_object = server
        .send(
            "GET",
            "/objects/photos/docs/readme.txt",
            &[("Range", "bytes=0-2")],
            &[],
        )
        .await;
    assert_eq!(partial_object.status_line, "HTTP/1.1 206 Partial Content");
    assert_eq!(partial_object.body, b"upd");
    assert_eq!(
        partial_object
            .headers
            .get("content-range")
            .expect("partial get should include content-range"),
        "bytes 0-2/7"
    );

    let invalid_range = server
        .send(
            "GET",
            "/objects/photos/docs/readme.txt",
            &[("Range", "bytes=100-200")],
            &[],
        )
        .await;
    assert_eq!(
        invalid_range.status_line,
        "HTTP/1.1 416 Range Not Satisfiable"
    );
    assert_eq!(
        invalid_range
            .headers
            .get("content-range")
            .expect("416 should include content-range"),
        "bytes */7"
    );

    let put_nested = server
        .send(
            "PUT",
            "/objects/photos/docs/api/spec.txt",
            &[("Content-Type", "text/plain")],
            b"spec",
        )
        .await;
    assert_eq!(put_nested.status_line, "HTTP/1.1 201 Created");

    let list_objects = server
        .send("GET", "/objects/photos?prefix=docs/&delimiter=/", &[], &[])
        .await;
    assert_eq!(list_objects.status_line, "HTTP/1.1 200 OK");
    let body = list_objects.body_text();
    assert!(body.contains(r#""key":"docs/readme.txt""#));
    assert!(body.contains(r#""common_prefixes":["docs/api/"]"#));

    let delete_bucket_while_non_empty = server.send("DELETE", "/buckets/photos", &[], &[]).await;
    assert_eq!(
        delete_bucket_while_non_empty.status_line,
        "HTTP/1.1 409 Conflict"
    );

    let delete_nested = server
        .send("DELETE", "/objects/photos/docs/api/spec.txt", &[], &[])
        .await;
    assert_eq!(delete_nested.status_line, "HTTP/1.1 204 No Content");

    let delete_object = server
        .send("DELETE", "/objects/photos/docs/readme.txt", &[], &[])
        .await;
    assert_eq!(delete_object.status_line, "HTTP/1.1 204 No Content");

    let delete_bucket = server.send("DELETE", "/buckets/photos", &[], &[]).await;
    assert_eq!(delete_bucket.status_line, "HTTP/1.1 204 No Content");

    server.stop().await;
}

#[tokio::test]
async fn multipart_upload_lifecycle_over_http() {
    let server = TestServer::start("tiny-fs-multipart-test").await;

    let create_bucket = server.send("PUT", "/buckets/media", &[], &[]).await;
    assert_eq!(create_bucket.status_line, "HTTP/1.1 201 Created");

    let start_upload = server
        .send(
            "POST",
            "/objects/media/archive.bin?uploads",
            &[("Content-Type", "application/octet-stream")],
            &[],
        )
        .await;
    assert_eq!(start_upload.status_line, "HTTP/1.1 200 OK");
    let upload_id = json_string_field(&start_upload.body_text(), "upload_id");

    let put_second_part = server
        .send(
            "PUT",
            &format!("/objects/media/archive.bin?uploadId={upload_id}&partNumber=2"),
            &[],
            b"world",
        )
        .await;
    assert_eq!(put_second_part.status_line, "HTTP/1.1 200 OK");
    let second_etag = json_string_field(&put_second_part.body_text(), "etag");

    let put_first_part = server
        .send(
            "PUT",
            &format!("/objects/media/archive.bin?uploadId={upload_id}&partNumber=1"),
            &[],
            b"hello ",
        )
        .await;
    assert_eq!(put_first_part.status_line, "HTTP/1.1 200 OK");
    let first_etag = json_string_field(&put_first_part.body_text(), "etag");

    let overwrite_second_part = server
        .send(
            "PUT",
            &format!("/objects/media/archive.bin?uploadId={upload_id}&partNumber=2"),
            &[],
            b"rust",
        )
        .await;
    assert_eq!(overwrite_second_part.status_line, "HTTP/1.1 200 OK");
    assert!(overwrite_second_part
        .body_text()
        .contains(r#""overwritten":true"#));
    let replaced_second_etag = json_string_field(&overwrite_second_part.body_text(), "etag");

    let list_parts = server
        .send(
            "GET",
            &format!("/objects/media/archive.bin?uploadId={upload_id}"),
            &[],
            &[],
        )
        .await;
    assert_eq!(list_parts.status_line, "HTTP/1.1 200 OK");
    let list_body = list_parts.body_text();
    assert!(list_body.contains(r#""part_number":1"#));
    assert!(list_body.contains(r#""part_number":2"#));
    assert!(list_body.contains(&format!(r#""etag":"{}""#, first_etag)));
    assert!(list_body.contains(&format!(r#""etag":"{}""#, replaced_second_etag)));
    assert!(!list_body.contains(&format!(r#""etag":"{}""#, second_etag)));

    let invalid_complete = server
        .send(
            "POST",
            &format!("/objects/media/archive.bin?uploadId={upload_id}"),
            &[("Content-Type", "application/json")],
            format!(
                "{{\"parts\":[{{\"part_number\":1,\"etag\":\"{}\"}},{{\"part_number\":2,\"etag\":\"{}\"}}]}}",
                first_etag,
                second_etag
            )
            .as_bytes(),
        )
        .await;
    assert_eq!(invalid_complete.status_line, "HTTP/1.1 400 Bad Request");

    let complete_upload = server
        .send(
            "POST",
            &format!("/objects/media/archive.bin?uploadId={upload_id}"),
            &[("Content-Type", "application/json")],
            format!(
                "{{\"parts\":[{{\"part_number\":1,\"etag\":\"{}\"}},{{\"part_number\":2,\"etag\":\"{}\"}}]}}",
                first_etag,
                replaced_second_etag
            )
            .as_bytes(),
        )
        .await;
    assert_eq!(complete_upload.status_line, "HTTP/1.1 201 Created");

    let get_object = server
        .send("GET", "/objects/media/archive.bin", &[], &[])
        .await;
    assert_eq!(get_object.status_line, "HTTP/1.1 200 OK");
    assert_eq!(get_object.body, b"hello rust");
    assert_eq!(
        get_object
            .headers
            .get("content-type")
            .expect("multipart object should preserve content type"),
        "application/octet-stream"
    );

    let start_abort_upload = server
        .send(
            "POST",
            "/objects/media/aborted.bin?uploads",
            &[("Content-Type", "application/octet-stream")],
            &[],
        )
        .await;
    let abort_upload_id = json_string_field(&start_abort_upload.body_text(), "upload_id");

    let upload_abort_part = server
        .send(
            "PUT",
            &format!("/objects/media/aborted.bin?uploadId={abort_upload_id}&partNumber=1"),
            &[],
            b"partial",
        )
        .await;
    assert_eq!(upload_abort_part.status_line, "HTTP/1.1 200 OK");

    let abort_upload = server
        .send(
            "DELETE",
            &format!("/objects/media/aborted.bin?uploadId={abort_upload_id}"),
            &[],
            &[],
        )
        .await;
    assert_eq!(abort_upload.status_line, "HTTP/1.1 204 No Content");

    let complete_aborted = server
        .send(
            "POST",
            &format!("/objects/media/aborted.bin?uploadId={abort_upload_id}"),
            &[],
            &[],
        )
        .await;
    assert_eq!(complete_aborted.status_line, "HTTP/1.1 404 Not Found");

    server.stop().await;
}

#[test]
fn objects_survive_restart() {
    let data_dir = unique_temp_path("tiny-fs-restart-test");

    let first_state = bootstrap(test_config(data_dir.clone())).expect("first bootstrap");
    first_state
        .bucket_service
        .create_bucket("persist")
        .expect("bucket should be created");
    first_state
        .object_service
        .put_object(
            "persist",
            "notes/todo.txt",
            b"persisted",
            Some("text/plain"),
        )
        .expect("object should be stored");
    drop(first_state);

    let second_state = bootstrap(test_config(data_dir.clone())).expect("second bootstrap");
    let object = second_state
        .object_service
        .get_object("persist", "notes/todo.txt")
        .expect("object should survive restart");

    assert_eq!(object.body, b"persisted");
    assert_eq!(object.metadata.content_type.as_deref(), Some("text/plain"));

    std::fs::remove_dir_all(data_dir).expect("test directory should be removable");
}

#[test]
fn bootstrap_cleans_staging_directory() {
    let data_dir = unique_temp_path("tiny-fs-recovery-test");
    let state = bootstrap(test_config(data_dir.clone())).expect("bootstrap should succeed");
    let stale_file = state.storage.staging_dir.join("stale-upload.part");
    std::fs::write(&stale_file, b"partial").expect("stale file should be written");
    drop(state);

    let rebooted =
        bootstrap(test_config(data_dir.clone())).expect("reboot bootstrap should succeed");
    let entries = std::fs::read_dir(&rebooted.storage.staging_dir)
        .expect("staging directory should be readable")
        .count();

    assert_eq!(entries, 0);

    std::fs::remove_dir_all(data_dir).expect("test directory should be removable");
}
