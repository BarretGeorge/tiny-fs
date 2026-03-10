#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bucket {
    pub name: String,
    pub created_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobRecord {
    pub id: i64,
    pub file_path: String,
    pub size: u64,
    pub checksum: String,
    pub created_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectMetadata {
    pub bucket: String,
    pub key: String,
    pub size: u64,
    pub etag: String,
    pub content_type: Option<String>,
    pub created_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectRecord {
    pub metadata: ObjectMetadata,
    pub blob: BlobRecord,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PutObjectCommit {
    pub metadata: ObjectMetadata,
    pub previous_blob: Option<BlobRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MultipartUpload {
    pub upload_id: String,
    pub bucket: String,
    pub key: String,
    pub content_type: Option<String>,
    pub created_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MultipartPartRecord {
    pub upload_id: String,
    pub part_number: u16,
    pub file_path: String,
    pub size: u64,
    pub checksum: String,
    pub created_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompleteMultipartCommit {
    pub put: PutObjectCommit,
    pub part_paths: Vec<String>,
}
