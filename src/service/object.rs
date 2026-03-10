use std::{
    collections::{BTreeMap, BTreeSet},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    meta::{MultipartPartRecord, MultipartUpload, ObjectMetadata, SqliteMetadataStore},
    service::{validate_bucket_name, validate_object_key},
    AppError, AppResult, BlobStore,
};

static MULTIPART_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PutObjectOutcome {
    pub metadata: ObjectMetadata,
    pub overwritten: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MultipartUploadHandle {
    pub upload_id: String,
    pub bucket: String,
    pub key: String,
    pub content_type: Option<String>,
    pub created_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MultipartPartOutcome {
    pub part_number: u16,
    pub etag: String,
    pub size: u64,
    pub overwritten: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MultipartPart {
    pub part_number: u16,
    pub etag: String,
    pub size: u64,
    pub created_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MultipartUploadListing {
    pub upload: MultipartUploadHandle,
    pub parts: Vec<MultipartPart>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompleteMultipartPart {
    pub part_number: u16,
    pub etag: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedByteRange {
    pub start: u64,
    pub end: u64,
    pub total_size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ByteRangeRequest {
    From { start: u64, end: Option<u64> },
    Suffix { len: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredObject {
    pub metadata: ObjectMetadata,
    pub body: Vec<u8>,
    pub range: Option<ResolvedByteRange>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListObjectsResult {
    pub objects: Vec<ObjectMetadata>,
    pub common_prefixes: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct ObjectService {
    metadata: SqliteMetadataStore,
    blob_store: BlobStore,
}

impl ObjectService {
    pub fn new(metadata: SqliteMetadataStore, blob_store: BlobStore) -> Self {
        Self {
            metadata,
            blob_store,
        }
    }

    pub fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: &[u8],
        content_type: Option<&str>,
    ) -> AppResult<PutObjectOutcome> {
        validate_bucket_name(bucket)?;
        validate_object_key(key)?;

        let content_type = normalize_content_type(content_type);
        let blob = self.blob_store.write_bytes(body)?;

        let commit = match self
            .metadata
            .put_object(bucket, key, &blob, content_type.as_deref())
        {
            Ok(commit) => commit,
            Err(error) => {
                let _ = self.blob_store.delete_blob(&blob.relative_path);
                return Err(error);
            }
        };

        if let Some(previous_blob) = &commit.previous_blob {
            let _ = self.blob_store.delete_blob(&previous_blob.file_path);
        }

        Ok(PutObjectOutcome {
            metadata: commit.metadata,
            overwritten: commit.previous_blob.is_some(),
        })
    }

    pub fn get_object(&self, bucket: &str, key: &str) -> AppResult<StoredObject> {
        self.get_object_with_range(bucket, key, None)
    }

    pub fn get_object_with_range(
        &self,
        bucket: &str,
        key: &str,
        range: Option<ByteRangeRequest>,
    ) -> AppResult<StoredObject> {
        validate_bucket_name(bucket)?;
        validate_object_key(key)?;

        let record = self.metadata.get_object(bucket, key)?;
        let resolved_range = range
            .map(|range| range.resolve(record.metadata.size))
            .transpose()?;
        let body = match &resolved_range {
            Some(range) => {
                self.blob_store
                    .read_range(&record.blob.file_path, range.start, range.end)?
            }
            None => self.blob_store.read_bytes(&record.blob.file_path)?,
        };

        Ok(StoredObject {
            metadata: record.metadata,
            body,
            range: resolved_range,
        })
    }

    pub fn head_object(&self, bucket: &str, key: &str) -> AppResult<ObjectMetadata> {
        validate_bucket_name(bucket)?;
        validate_object_key(key)?;
        Ok(self.metadata.get_object(bucket, key)?.metadata)
    }

    pub fn delete_object(&self, bucket: &str, key: &str) -> AppResult<()> {
        validate_bucket_name(bucket)?;
        validate_object_key(key)?;

        let blob = self.metadata.delete_object(bucket, key)?;
        let _ = self.blob_store.delete_blob(&blob.file_path);
        Ok(())
    }

    pub fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
    ) -> AppResult<ListObjectsResult> {
        validate_bucket_name(bucket)?;

        let prefix = prefix.unwrap_or("");
        let delimiter = normalize_delimiter(delimiter);
        let objects = self.metadata.list_objects(bucket)?;

        let mut filtered = Vec::new();
        let mut common_prefixes = BTreeSet::new();

        for object in objects {
            if !object.key.starts_with(prefix) {
                continue;
            }

            if let Some(delimiter) = delimiter {
                let suffix = &object.key[prefix.len()..];
                if let Some(index) = suffix.find(delimiter) {
                    let mut common_prefix = String::from(prefix);
                    common_prefix.push_str(&suffix[..index + delimiter.len()]);
                    common_prefixes.insert(common_prefix);
                    continue;
                }
            }

            filtered.push(object);
        }

        Ok(ListObjectsResult {
            objects: filtered,
            common_prefixes: common_prefixes.into_iter().collect(),
        })
    }

    pub fn initiate_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
    ) -> AppResult<MultipartUploadHandle> {
        validate_bucket_name(bucket)?;
        validate_object_key(key)?;

        let upload_id = unique_upload_id();
        let upload = self.metadata.create_multipart_upload(
            &upload_id,
            bucket,
            key,
            normalize_content_type(content_type).as_deref(),
        )?;

        Ok(multipart_upload_handle(upload))
    }

    pub fn list_multipart_parts(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> AppResult<MultipartUploadListing> {
        validate_bucket_name(bucket)?;
        validate_object_key(key)?;

        let (upload, parts) = self.metadata.list_multipart_parts(upload_id, bucket, key)?;

        Ok(MultipartUploadListing {
            upload: multipart_upload_handle(upload),
            parts: parts.into_iter().map(multipart_part).collect(),
        })
    }

    pub fn upload_multipart_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u16,
        body: &[u8],
    ) -> AppResult<MultipartPartOutcome> {
        validate_bucket_name(bucket)?;
        validate_object_key(key)?;
        validate_part_number(part_number)?;

        let part_blob = self
            .blob_store
            .write_multipart_part(upload_id, part_number, body)?;

        let previous =
            match self
                .metadata
                .put_multipart_part(upload_id, bucket, key, part_number, &part_blob)
            {
                Ok(previous) => previous,
                Err(error) => {
                    let _ = self.blob_store.delete_blob(&part_blob.relative_path);
                    return Err(error);
                }
            };

        if let Some(previous_part) = &previous {
            let _ = self.blob_store.delete_blob(&previous_part.file_path);
        }

        Ok(MultipartPartOutcome {
            part_number,
            etag: part_blob.checksum,
            size: part_blob.size,
            overwritten: previous.is_some(),
        })
    }

    pub fn complete_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        manifest: Option<&[CompleteMultipartPart]>,
    ) -> AppResult<PutObjectOutcome> {
        validate_bucket_name(bucket)?;
        validate_object_key(key)?;

        let (_upload, uploaded_parts) =
            self.metadata.list_multipart_parts(upload_id, bucket, key)?;
        let selected_parts = select_multipart_parts(&uploaded_parts, manifest)?;
        let selected_paths = selected_parts
            .iter()
            .map(|part| part.file_path.clone())
            .collect::<Vec<_>>();

        let blob = self.blob_store.compose_multipart(&selected_paths)?;

        let commit = match self
            .metadata
            .complete_multipart_upload(upload_id, bucket, key, &blob)
        {
            Ok(commit) => commit,
            Err(error) => {
                let _ = self.blob_store.delete_blob(&blob.relative_path);
                return Err(error);
            }
        };

        for part_path in &commit.part_paths {
            let _ = self.blob_store.delete_blob(part_path);
        }
        if let Some(previous_blob) = &commit.put.previous_blob {
            let _ = self.blob_store.delete_blob(&previous_blob.file_path);
        }

        Ok(PutObjectOutcome {
            metadata: commit.put.metadata,
            overwritten: commit.put.previous_blob.is_some(),
        })
    }

    pub fn abort_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> AppResult<()> {
        validate_bucket_name(bucket)?;
        validate_object_key(key)?;

        let part_paths = self
            .metadata
            .abort_multipart_upload(upload_id, bucket, key)?;
        for path in part_paths {
            let _ = self.blob_store.delete_blob(&path);
        }

        Ok(())
    }
}

impl ByteRangeRequest {
    pub fn parse(header: &str) -> AppResult<Self> {
        let raw = header.trim();
        let value = raw
            .strip_prefix("bytes=")
            .ok_or_else(|| AppError::InvalidRange(raw.to_string()))?;

        if value.contains(',') {
            return Err(AppError::InvalidRange(raw.to_string()));
        }

        if let Some(suffix) = value.strip_prefix('-') {
            let len = suffix
                .parse::<u64>()
                .map_err(|_| AppError::InvalidRange(raw.to_string()))?;
            if len == 0 {
                return Err(AppError::InvalidRange(raw.to_string()));
            }
            return Ok(Self::Suffix { len });
        }

        let (start, end) = value
            .split_once('-')
            .ok_or_else(|| AppError::InvalidRange(raw.to_string()))?;
        let start = start
            .parse::<u64>()
            .map_err(|_| AppError::InvalidRange(raw.to_string()))?;
        let end = if end.is_empty() {
            None
        } else {
            Some(
                end.parse::<u64>()
                    .map_err(|_| AppError::InvalidRange(raw.to_string()))?,
            )
        };

        Ok(Self::From { start, end })
    }

    pub fn resolve(&self, total_size: u64) -> AppResult<ResolvedByteRange> {
        if total_size == 0 {
            return Err(AppError::RangeNotSatisfiable(total_size));
        }

        match self {
            Self::From { start, end } => {
                if *start >= total_size {
                    return Err(AppError::RangeNotSatisfiable(total_size));
                }

                let end = end.unwrap_or(total_size - 1).min(total_size - 1);
                if end < *start {
                    return Err(AppError::InvalidRange(format!("{start}-{end}")));
                }

                Ok(ResolvedByteRange {
                    start: *start,
                    end,
                    total_size,
                })
            }
            Self::Suffix { len } => {
                let start = total_size.saturating_sub(*len);
                Ok(ResolvedByteRange {
                    start,
                    end: total_size - 1,
                    total_size,
                })
            }
        }
    }
}

fn select_multipart_parts(
    uploaded_parts: &[MultipartPartRecord],
    manifest: Option<&[CompleteMultipartPart]>,
) -> AppResult<Vec<MultipartPartRecord>> {
    if uploaded_parts.is_empty() {
        return Err(AppError::InvalidRequest(
            "multipart upload has no parts".to_string(),
        ));
    }

    let by_part_number = uploaded_parts
        .iter()
        .cloned()
        .map(|part| (part.part_number, part))
        .collect::<BTreeMap<_, _>>();

    let Some(manifest) = manifest else {
        return Ok(uploaded_parts.to_vec());
    };

    if manifest.is_empty() {
        return Err(AppError::InvalidRequest(
            "multipart complete manifest has no parts".to_string(),
        ));
    }

    let mut selected = Vec::with_capacity(manifest.len());
    let mut previous_part_number = 0_u16;

    for manifest_part in manifest {
        validate_part_number(manifest_part.part_number)?;

        if manifest_part.part_number <= previous_part_number {
            return Err(AppError::InvalidRequest(
                "multipart complete manifest must be strictly ordered by part number".to_string(),
            ));
        }

        let uploaded_part = by_part_number
            .get(&manifest_part.part_number)
            .ok_or_else(|| {
                AppError::InvalidRequest(format!(
                    "multipart complete manifest references missing part {}",
                    manifest_part.part_number
                ))
            })?;

        if normalize_etag(&manifest_part.etag) != uploaded_part.checksum {
            return Err(AppError::InvalidRequest(format!(
                "multipart complete manifest etag mismatch for part {}",
                manifest_part.part_number
            )));
        }

        selected.push(uploaded_part.clone());
        previous_part_number = manifest_part.part_number;
    }

    Ok(selected)
}

fn multipart_upload_handle(upload: MultipartUpload) -> MultipartUploadHandle {
    MultipartUploadHandle {
        upload_id: upload.upload_id,
        bucket: upload.bucket,
        key: upload.key,
        content_type: upload.content_type,
        created_at: upload.created_at,
    }
}

fn multipart_part(record: MultipartPartRecord) -> MultipartPart {
    MultipartPart {
        part_number: record.part_number,
        etag: record.checksum,
        size: record.size,
        created_at: record.created_at,
    }
}

fn normalize_content_type(content_type: Option<&str>) -> Option<String> {
    content_type
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn normalize_delimiter(delimiter: Option<&str>) -> Option<&str> {
    delimiter.filter(|value| !value.is_empty())
}

fn normalize_etag(etag: &str) -> &str {
    etag.trim().trim_matches('"')
}

fn validate_part_number(part_number: u16) -> AppResult<()> {
    if part_number == 0 || part_number > 10_000 {
        return Err(AppError::InvalidPartNumber(part_number.to_string()));
    }

    Ok(())
}

fn unique_upload_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should move forward")
        .as_nanos();
    let counter = MULTIPART_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{nanos:x}-{}-{counter:x}", std::process::id())
}
