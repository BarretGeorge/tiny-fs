use axum::{
    body::Body,
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::json;

use crate::ConfigError;

pub type AppResult<T> = Result<T, AppError>;

#[derive(Debug)]
pub enum AppError {
    Config(ConfigError),
    Io(std::io::Error),
    Metadata(rusqlite::Error),
    BucketAlreadyExists,
    BucketNotFound,
    BucketNotEmpty,
    ObjectNotFound,
    MultipartUploadNotFound,
    InvalidBucketName(String),
    InvalidObjectKey(String),
    InvalidPartNumber(String),
    InvalidRange(String),
    RangeNotSatisfiable(u64),
    InvalidRequest(String),
    StorageInconsistent(String),
    InsufficientNodes { required: usize, available: usize },
    NodeNotAvailable(String),
    ReplicationFailed(String),
    ErasureCodingFailed(String),
    QuorumNotAchieved { required: usize, achieved: usize },
    Internal,
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(error) => write!(f, "{error}"),
            Self::Io(error) => write!(f, "io error: {error}"),
            Self::Metadata(error) => write!(f, "metadata error: {error}"),
            Self::BucketAlreadyExists => write!(f, "bucket already exists"),
            Self::BucketNotFound => write!(f, "bucket not found"),
            Self::BucketNotEmpty => write!(f, "bucket not empty"),
            Self::ObjectNotFound => write!(f, "object not found"),
            Self::MultipartUploadNotFound => write!(f, "multipart upload not found"),
            Self::InvalidBucketName(name) => write!(f, "invalid bucket name `{name}`"),
            Self::InvalidObjectKey(key) => write!(f, "invalid object key `{key}`"),
            Self::InvalidPartNumber(part_number) => {
                write!(f, "invalid multipart part number `{part_number}`")
            }
            Self::InvalidRange(range) => write!(f, "invalid range `{range}`"),
            Self::RangeNotSatisfiable(size) => {
                write!(f, "range not satisfiable for object size {size}")
            }
            Self::InvalidRequest(reason) => write!(f, "invalid request: {reason}"),
            Self::StorageInconsistent(reason) => write!(f, "storage inconsistent: {reason}"),
            Self::InsufficientNodes {
                required,
                available,
            } => {
                write!(
                    f,
                    "insufficient nodes: required {}, available {}",
                    required, available
                )
            }
            Self::NodeNotAvailable(node_id) => write!(f, "node not available: {node_id}"),
            Self::ReplicationFailed(reason) => write!(f, "replication failed: {reason}"),
            Self::ErasureCodingFailed(reason) => write!(f, "erasure coding failed: {reason}"),
            Self::QuorumNotAchieved { required, achieved } => {
                write!(
                    f,
                    "quorum not achieved: required {}, achieved {}",
                    required, achieved
                )
            }
            Self::Internal => write!(f, "internal server error"),
        }
    }
}

impl std::error::Error for AppError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Config(error) => Some(error),
            Self::Io(error) => Some(error),
            Self::Metadata(error) => Some(error),
            Self::BucketAlreadyExists
            | Self::BucketNotFound
            | Self::BucketNotEmpty
            | Self::ObjectNotFound
            | Self::MultipartUploadNotFound
            | Self::InvalidBucketName(_)
            | Self::InvalidObjectKey(_)
            | Self::InvalidPartNumber(_)
            | Self::InvalidRange(_)
            | Self::RangeNotSatisfiable(_)
            | Self::InvalidRequest(_)
            | Self::StorageInconsistent(_)
            | Self::InsufficientNodes { .. }
            | Self::NodeNotAvailable(_)
            | Self::ReplicationFailed(_)
            | Self::ErasureCodingFailed(_)
            | Self::QuorumNotAchieved { .. }
            | Self::Internal => None,
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let error_code = self.error_code();
        let message = self.to_string();
        let range_not_satisfiable = match &self {
            Self::RangeNotSatisfiable(size) => Some(*size),
            _ => None,
        };

        let body = serde_json::to_vec(&json!({
            "error": error_code,
            "message": message,
        }))
        .unwrap_or_else(|_| {
            b"{\"error\":\"internal_error\",\"message\":\"internal server error\"}".to_vec()
        });

        let mut response = Response::new(Body::from(body.clone()));
        *response.status_mut() = status;
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        response.headers_mut().insert(
            header::CONTENT_LENGTH,
            HeaderValue::from_str(&body.len().to_string())
                .expect("content length should always be a valid header"),
        );

        if let Some(size) = range_not_satisfiable {
            response.headers_mut().insert(
                header::CONTENT_RANGE,
                HeaderValue::from_str(&format!("bytes */{size}"))
                    .expect("range not satisfiable header should be valid"),
            );
        }

        response
    }
}

impl From<ConfigError> for AppError {
    fn from(error: ConfigError) -> Self {
        Self::Config(error)
    }
}

impl From<std::io::Error> for AppError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<rusqlite::Error> for AppError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Metadata(error)
    }
}

impl AppError {
    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::BucketAlreadyExists | Self::BucketNotEmpty => StatusCode::CONFLICT,
            Self::BucketNotFound | Self::ObjectNotFound | Self::MultipartUploadNotFound => {
                StatusCode::NOT_FOUND
            }
            Self::InvalidBucketName(_)
            | Self::InvalidObjectKey(_)
            | Self::InvalidPartNumber(_)
            | Self::InvalidRange(_)
            | Self::InvalidRequest(_)
            | Self::InsufficientNodes { .. }
            | Self::NodeNotAvailable(_)
            | Self::ReplicationFailed(_)
            | Self::ErasureCodingFailed(_)
            | Self::QuorumNotAchieved { .. } => StatusCode::BAD_REQUEST,
            Self::RangeNotSatisfiable(_) => StatusCode::RANGE_NOT_SATISFIABLE,
            Self::Config(_)
            | Self::Io(_)
            | Self::Metadata(_)
            | Self::StorageInconsistent(_)
            | Self::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn error_code(&self) -> &'static str {
        match self {
            Self::Config(_) => "configuration_error",
            Self::Io(_) => "io_error",
            Self::Metadata(_) => "metadata_error",
            Self::BucketAlreadyExists => "bucket_already_exists",
            Self::BucketNotFound => "bucket_not_found",
            Self::BucketNotEmpty => "bucket_not_empty",
            Self::ObjectNotFound => "object_not_found",
            Self::MultipartUploadNotFound => "multipart_upload_not_found",
            Self::InvalidBucketName(_) => "invalid_bucket_name",
            Self::InvalidObjectKey(_) => "invalid_object_key",
            Self::InvalidPartNumber(_) => "invalid_part_number",
            Self::InvalidRange(_) => "invalid_range",
            Self::RangeNotSatisfiable(_) => "range_not_satisfiable",
            Self::InvalidRequest(_) => "invalid_request",
            Self::StorageInconsistent(_) => "storage_inconsistent",
            Self::InsufficientNodes { .. } => "insufficient_nodes",
            Self::NodeNotAvailable(_) => "node_not_available",
            Self::ReplicationFailed(_) => "replication_failed",
            Self::ErasureCodingFailed(_) => "erasure_coding_failed",
            Self::QuorumNotAchieved { .. } => "quorum_not_achieved",
            Self::Internal => "internal_error",
        }
    }
}
