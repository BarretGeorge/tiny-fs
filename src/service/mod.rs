mod bucket;
mod object;

pub use bucket::BucketService;
pub use object::{
    ByteRangeRequest, CompleteMultipartPart, ListObjectsResult, MultipartPart,
    MultipartPartOutcome, MultipartUploadHandle, MultipartUploadListing, ObjectService,
    PutObjectOutcome, ResolvedByteRange, StoredObject,
};

use crate::{AppError, AppResult};

pub(crate) fn validate_bucket_name(name: &str) -> AppResult<()> {
    if name.len() < 3 || name.len() > 63 {
        return Err(AppError::InvalidBucketName(name.to_string()));
    }

    let bytes = name.as_bytes();
    if !bytes[0].is_ascii_lowercase() && !bytes[0].is_ascii_digit() {
        return Err(AppError::InvalidBucketName(name.to_string()));
    }
    if !bytes[bytes.len() - 1].is_ascii_lowercase() && !bytes[bytes.len() - 1].is_ascii_digit() {
        return Err(AppError::InvalidBucketName(name.to_string()));
    }

    if name.contains("..")
        || !name
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-' || ch == '.')
    {
        return Err(AppError::InvalidBucketName(name.to_string()));
    }

    Ok(())
}

pub(crate) fn validate_object_key(key: &str) -> AppResult<()> {
    if key.is_empty() || key.len() > 1024 || key.starts_with('/') {
        return Err(AppError::InvalidObjectKey(key.to_string()));
    }

    if key
        .chars()
        .any(|ch| ch == '\0' || ch.is_control() || ch == '\\')
    {
        return Err(AppError::InvalidObjectKey(key.to_string()));
    }

    if key
        .split('/')
        .any(|segment| segment == "." || segment == "..")
    {
        return Err(AppError::InvalidObjectKey(key.to_string()));
    }

    Ok(())
}
