use std::collections::BTreeMap;

use axum::{
    body::{Body, Bytes},
    extract::{Path, State},
    http::{header, HeaderMap, HeaderValue, StatusCode, Uri},
    response::Response,
    routing::{get, put},
    Router,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{
    AppError, AppResult, AppState, Bucket, ByteRangeRequest, ClusterTopology,
    CompleteMultipartPart, ListObjectsResult, MultipartPart, MultipartPartOutcome,
    MultipartUploadHandle, MultipartUploadListing, ObjectMetadata, StoredObject,
};

const CONSOLE_HTML: &str = include_str!("../../static/console.html");
const CONSOLE_STYLES: &str = include_str!("../../static/console.css");
const CONSOLE_SCRIPT: &str = include_str!("../../static/console.js");

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(console_index))
        .route("/ui/styles.css", get(console_styles))
        .route("/ui/app.js", get(console_script))
        .route("/health", get(health))
        .route("/cluster/topology", get(cluster_topology))
        .route("/cluster/nodes", get(list_cluster_nodes))
        .route("/cluster/health", get(cluster_health))
        .route("/buckets", get(list_buckets))
        .route("/buckets/:bucket", put(create_bucket).delete(delete_bucket))
        .route("/objects/:bucket", get(list_objects))
        .route(
            "/objects/:bucket/*key",
            get(get_object)
                .head(head_object)
                .put(put_object)
                .post(post_object)
                .delete(delete_object),
        )
        .with_state(state)
}

async fn console_index() -> Response {
    static_text_response("text/html; charset=utf-8", CONSOLE_HTML)
}

async fn console_styles() -> Response {
    static_text_response("text/css; charset=utf-8", CONSOLE_STYLES)
}

async fn console_script() -> Response {
    static_text_response("application/javascript; charset=utf-8", CONSOLE_SCRIPT)
}

#[derive(Deserialize)]
struct BucketPath {
    bucket: String,
}

#[derive(Deserialize)]
struct ObjectPath {
    bucket: String,
    key: String,
}

#[derive(Deserialize)]
struct CompleteMultipartPayload {
    parts: Vec<CompleteMultipartPayloadPart>,
}

#[derive(Deserialize)]
struct CompleteMultipartPayloadPart {
    #[serde(rename = "part_number", alias = "partNumber")]
    part_number: u16,
    etag: String,
}

async fn health(State(state): State<AppState>) -> AppResult<Response> {
    json_response(
        StatusCode::OK,
        json!({
            "status": "ok",
            "service": "tiny-fs",
            "version": env!("CARGO_PKG_VERSION"),
            "data_dir": state.storage.root.display().to_string(),
            "cluster_mode": state.is_cluster_mode(),
            "node_id": state.config.cluster.node_id,
        }),
    )
}

async fn cluster_topology(State(state): State<AppState>) -> AppResult<Response> {
    if !state.is_cluster_mode() {
        return json_response(
            StatusCode::OK,
            json!({
                "error": "cluster mode not enabled",
                "enabled": false,
            }),
        );
    }

    let topology = state.cluster_manager.as_ref()
        .map(|cm| cm.get_topology())
        .unwrap_or(ClusterTopology {
            nodes: vec![],
            total_capacity: 0,
            replication_factor: 0,
        });

    json_response(
        StatusCode::OK,
        json!({
            "enabled": true,
            "node_id": state.config.cluster.node_id,
            "replication_factor": topology.replication_factor,
            "total_capacity": topology.total_capacity,
            "nodes": topology.nodes,
        }),
    )
}

async fn list_cluster_nodes(State(state): State<AppState>) -> AppResult<Response> {
    if !state.is_cluster_mode() {
        return json_response(
            StatusCode::OK,
            json!({
                "error": "cluster mode not enabled",
                "enabled": false,
                "nodes": [],
            }),
        );
    }

    let topology = state.cluster_manager.as_ref()
        .map(|cm| cm.get_topology())
        .unwrap_or(ClusterTopology {
            nodes: vec![],
            total_capacity: 0,
            replication_factor: 0,
        });

    json_response(StatusCode::OK, json!({ "nodes": topology.nodes }))
}

async fn cluster_health(State(state): State<AppState>) -> AppResult<Response> {
    if !state.is_cluster_mode() {
        return json_response(
            StatusCode::OK,
            json!({
                "enabled": false,
                "healthy": true,
                "message": "single node mode",
            }),
        );
    }

    let is_healthy = state.cluster_manager.as_ref()
        .map(|cm| cm.is_healthy())
        .unwrap_or(false);

    let total_nodes = state.cluster_manager.as_ref()
        .map(|cm| cm.registry().node_count())
        .unwrap_or(0);

    let healthy_nodes = state.cluster_manager.as_ref()
        .map(|cm| cm.registry().healthy_node_count())
        .unwrap_or(0);

    json_response(
        StatusCode::OK,
        json!({
            "enabled": true,
            "healthy": is_healthy,
            "total_nodes": total_nodes,
            "healthy_nodes": healthy_nodes,
            "write_quorum": state.cluster_manager.as_ref().map(|cm| cm.write_quorum()).unwrap_or(1),
            "read_quorum": state.cluster_manager.as_ref().map(|cm| cm.read_quorum()).unwrap_or(1),
        }),
    )
}

async fn list_buckets(State(state): State<AppState>) -> AppResult<Response> {
    let bucket_service = state.bucket_service.clone();
    let buckets = run_blocking(move || bucket_service.list_buckets()).await?;
    json_response(
        StatusCode::OK,
        json!({
            "buckets": buckets.iter().map(bucket_value).collect::<Vec<_>>(),
        }),
    )
}

async fn create_bucket(
    Path(path): Path<BucketPath>,
    State(state): State<AppState>,
) -> AppResult<Response> {
    let bucket_service = state.bucket_service.clone();
    let bucket = run_blocking(move || bucket_service.create_bucket(&path.bucket)).await?;
    json_response(StatusCode::CREATED, bucket_value(&bucket))
}

async fn delete_bucket(
    Path(path): Path<BucketPath>,
    State(state): State<AppState>,
) -> AppResult<Response> {
    let bucket_service = state.bucket_service.clone();
    run_blocking(move || bucket_service.delete_bucket(&path.bucket)).await?;
    Ok(empty_response(StatusCode::NO_CONTENT))
}

async fn list_objects(
    Path(path): Path<BucketPath>,
    State(state): State<AppState>,
    uri: Uri,
) -> AppResult<Response> {
    let query = parse_query(&uri);
    let prefix = query.get("prefix").cloned();
    let delimiter = query.get("delimiter").cloned();
    let object_service = state.object_service.clone();
    let bucket = path.bucket;

    let listing = run_blocking(move || {
        object_service.list_objects(&bucket, prefix.as_deref(), delimiter.as_deref())
    })
    .await?;

    json_response(StatusCode::OK, objects_value(&listing))
}

async fn get_object(
    Path(path): Path<ObjectPath>,
    State(state): State<AppState>,
    uri: Uri,
    headers: HeaderMap,
) -> AppResult<Response> {
    let query = parse_query(&uri);
    let bucket = path.bucket;
    let key = normalize_path_key(path.key);

    if let Some(upload_id) = query.get("uploadId").cloned() {
        let object_service = state.object_service.clone();
        let listing =
            run_blocking(move || object_service.list_multipart_parts(&bucket, &key, &upload_id))
                .await?;
        return json_response(StatusCode::OK, multipart_parts_value(&listing));
    }

    let range = match headers.get(header::RANGE) {
        Some(value) => Some(parse_range_header(value)?),
        None => None,
    };

    let object_service = state.object_service.clone();
    let object =
        run_blocking(move || object_service.get_object_with_range(&bucket, &key, range)).await?;
    object_response(object)
}

async fn head_object(
    Path(path): Path<ObjectPath>,
    State(state): State<AppState>,
    uri: Uri,
) -> AppResult<Response> {
    let query = parse_query(&uri);
    if query.contains_key("uploadId") {
        return Ok(not_found_response());
    }

    let bucket = path.bucket;
    let key = normalize_path_key(path.key);
    let object_service = state.object_service.clone();
    let metadata = run_blocking(move || object_service.head_object(&bucket, &key)).await?;
    head_response(&metadata)
}

async fn put_object(
    Path(path): Path<ObjectPath>,
    State(state): State<AppState>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> AppResult<Response> {
    let query = parse_query(&uri);
    let bucket = path.bucket;
    let key = normalize_path_key(path.key);

    if let Some(upload_id) = query.get("uploadId").cloned() {
        let part_number = parse_part_number(&query)?;
        let object_service = state.object_service.clone();
        let body = body.to_vec();
        let response_upload_id = upload_id.clone();
        let outcome = run_blocking(move || {
            object_service.upload_multipart_part(&bucket, &key, &upload_id, part_number, &body)
        })
        .await?;
        let mut response = json_response(
            StatusCode::OK,
            multipart_part_value(&response_upload_id, &outcome),
        )?;
        response.headers_mut().insert(
            header::ETAG,
            header_value(&format!("\"{}\"", outcome.etag))?,
        );
        return Ok(response);
    }

    let content_type = optional_header(&headers, header::CONTENT_TYPE)?;
    let object_service = state.object_service.clone();
    let body = body.to_vec();
    let outcome = run_blocking(move || {
        object_service.put_object(&bucket, &key, &body, content_type.as_deref())
    })
    .await?;

    let status = if outcome.overwritten {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    json_response(status, object_value(&outcome.metadata))
}

async fn post_object(
    Path(path): Path<ObjectPath>,
    State(state): State<AppState>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> AppResult<Response> {
    let query = parse_query(&uri);
    let bucket = path.bucket;
    let key = normalize_path_key(path.key);

    if query.contains_key("uploads") {
        let content_type = optional_header(&headers, header::CONTENT_TYPE)?;
        let object_service = state.object_service.clone();
        let upload = run_blocking(move || {
            object_service.initiate_multipart_upload(&bucket, &key, content_type.as_deref())
        })
        .await?;
        return json_response(StatusCode::OK, multipart_upload_value(&upload));
    }

    if let Some(upload_id) = query.get("uploadId").cloned() {
        let manifest = parse_complete_manifest(&body)?;
        let object_service = state.object_service.clone();
        let outcome = run_blocking(move || {
            object_service.complete_multipart_upload(&bucket, &key, &upload_id, manifest.as_deref())
        })
        .await?;
        let status = if outcome.overwritten {
            StatusCode::OK
        } else {
            StatusCode::CREATED
        };
        return json_response(status, object_value(&outcome.metadata));
    }

    Ok(not_found_response())
}

async fn delete_object(
    Path(path): Path<ObjectPath>,
    State(state): State<AppState>,
    uri: Uri,
) -> AppResult<Response> {
    let query = parse_query(&uri);
    let bucket = path.bucket;
    let key = normalize_path_key(path.key);
    let object_service = state.object_service.clone();

    if let Some(upload_id) = query.get("uploadId").cloned() {
        run_blocking(move || object_service.abort_multipart_upload(&bucket, &key, &upload_id))
            .await?;
        return Ok(empty_response(StatusCode::NO_CONTENT));
    }

    run_blocking(move || object_service.delete_object(&bucket, &key)).await?;
    Ok(empty_response(StatusCode::NO_CONTENT))
}

async fn run_blocking<F, T>(f: F) -> AppResult<T>
where
    F: FnOnce() -> AppResult<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|_| AppError::Internal)?
}

fn object_response(object: StoredObject) -> AppResult<Response> {
    let content_length = object.body.len();
    let mut response = Response::new(Body::from(object.body));
    *response.status_mut() = if object.range.is_some() {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };

    let content_type = object
        .metadata
        .content_type
        .as_deref()
        .unwrap_or("application/octet-stream");

    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, header_value(content_type)?);
    response.headers_mut().insert(
        header::CONTENT_LENGTH,
        header_value(&content_length.to_string())?,
    );
    response
        .headers_mut()
        .insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    response.headers_mut().insert(
        header::ETAG,
        header_value(&format!("\"{}\"", object.metadata.etag))?,
    );

    if let Some(range) = object.range {
        response.headers_mut().insert(
            header::CONTENT_RANGE,
            header_value(&format!(
                "bytes {}-{}/{}",
                range.start, range.end, range.total_size
            ))?,
        );
    }

    Ok(response)
}

fn head_response(metadata: &ObjectMetadata) -> AppResult<Response> {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::OK;

    response
        .headers_mut()
        .insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    response.headers_mut().insert(
        header::CONTENT_LENGTH,
        header_value(&metadata.size.to_string())?,
    );
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header_value(
            metadata
                .content_type
                .as_deref()
                .unwrap_or("application/octet-stream"),
        )?,
    );
    response.headers_mut().insert(
        header::ETAG,
        header_value(&format!("\"{}\"", metadata.etag))?,
    );

    Ok(response)
}

fn json_response(status: StatusCode, value: Value) -> AppResult<Response> {
    let body = serde_json::to_vec(&value).map_err(|_| AppError::Internal)?;
    let mut response = Response::new(Body::from(body.clone()));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response.headers_mut().insert(
        header::CONTENT_LENGTH,
        header_value(&body.len().to_string())?,
    );
    Ok(response)
}

fn empty_response(status: StatusCode) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(header::CONTENT_LENGTH, HeaderValue::from_static("0"));
    response
}

fn static_text_response(content_type: &'static str, body: &'static str) -> Response {
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = StatusCode::OK;
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    response.headers_mut().insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&body.len().to_string())
            .expect("static asset content length should be valid"),
    );
    response
}

fn not_found_response() -> Response {
    json_response(StatusCode::NOT_FOUND, json!({ "error": "not_found" }))
        .expect("not found response should be serializable")
}

fn parse_query(uri: &Uri) -> BTreeMap<String, String> {
    let mut query = BTreeMap::new();

    if let Some(raw_query) = uri.query() {
        for pair in raw_query.split('&').filter(|pair| !pair.is_empty()) {
            let (key, value) = match pair.split_once('=') {
                Some((key, value)) => (key, value),
                None => (pair, ""),
            };
            query.insert(key.to_string(), value.to_string());
        }
    }

    query
}

fn parse_part_number(query: &BTreeMap<String, String>) -> AppResult<u16> {
    let raw = query.get("partNumber").cloned().ok_or_else(|| {
        AppError::InvalidRequest("missing partNumber query parameter".to_string())
    })?;
    raw.parse::<u16>()
        .map_err(|_| AppError::InvalidPartNumber(raw))
}

fn parse_complete_manifest(body: &[u8]) -> AppResult<Option<Vec<CompleteMultipartPart>>> {
    if body.is_empty() || body.iter().all(u8::is_ascii_whitespace) {
        return Ok(None);
    }

    let payload: CompleteMultipartPayload = serde_json::from_slice(body).map_err(|error| {
        AppError::InvalidRequest(format!("invalid multipart complete body: {error}"))
    })?;

    Ok(Some(
        payload
            .parts
            .into_iter()
            .map(|part| CompleteMultipartPart {
                part_number: part.part_number,
                etag: part.etag,
            })
            .collect(),
    ))
}

fn parse_range_header(value: &HeaderValue) -> AppResult<ByteRangeRequest> {
    let header = value
        .to_str()
        .map_err(|_| AppError::InvalidRange("<non-utf8>".to_string()))?;
    ByteRangeRequest::parse(header)
}

fn optional_header(headers: &HeaderMap, name: header::HeaderName) -> AppResult<Option<String>> {
    headers
        .get(name)
        .map(|value| {
            value.to_str().map(str::to_owned).map_err(|_| {
                AppError::InvalidRequest("request headers must be valid text".to_string())
            })
        })
        .transpose()
}

fn header_value(value: &str) -> AppResult<HeaderValue> {
    HeaderValue::from_str(value)
        .map_err(|_| AppError::StorageInconsistent("invalid response header value".to_string()))
}

fn normalize_path_key(key: String) -> String {
    key.trim_start_matches('/').to_string()
}

fn bucket_value(bucket: &Bucket) -> Value {
    json!({
        "name": bucket.name,
        "created_at": bucket.created_at,
    })
}

fn object_value(object: &ObjectMetadata) -> Value {
    json!({
        "bucket": object.bucket,
        "key": object.key,
        "size": object.size,
        "etag": object.etag,
        "content_type": object.content_type,
        "created_at": object.created_at,
    })
}

fn objects_value(listing: &ListObjectsResult) -> Value {
    json!({
        "objects": listing.objects.iter().map(object_value).collect::<Vec<_>>(),
        "common_prefixes": listing.common_prefixes,
    })
}

fn multipart_upload_value(upload: &MultipartUploadHandle) -> Value {
    json!({
        "upload_id": upload.upload_id,
        "bucket": upload.bucket,
        "key": upload.key,
        "content_type": upload.content_type,
        "created_at": upload.created_at,
    })
}

fn multipart_part_value(upload_id: &str, part: &MultipartPartOutcome) -> Value {
    json!({
        "upload_id": upload_id,
        "part_number": part.part_number,
        "etag": part.etag,
        "size": part.size,
        "overwritten": part.overwritten,
    })
}

fn multipart_parts_value(listing: &MultipartUploadListing) -> Value {
    json!({
        "upload_id": listing.upload.upload_id,
        "bucket": listing.upload.bucket,
        "key": listing.upload.key,
        "content_type": listing.upload.content_type,
        "created_at": listing.upload.created_at,
        "parts": listing.parts.iter().map(multipart_part_info_value).collect::<Vec<_>>(),
    })
}

fn multipart_part_info_value(part: &MultipartPart) -> Value {
    json!({
        "part_number": part.part_number,
        "etag": part.etag,
        "size": part.size,
        "created_at": part.created_at,
    })
}
