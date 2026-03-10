use crate::{meta::SqliteMetadataStore, service::validate_bucket_name, AppResult, Bucket};

#[derive(Clone, Debug)]
pub struct BucketService {
    metadata: SqliteMetadataStore,
}

impl BucketService {
    pub fn new(metadata: SqliteMetadataStore) -> Self {
        Self { metadata }
    }

    pub fn create_bucket(&self, name: &str) -> AppResult<Bucket> {
        validate_bucket_name(name)?;
        self.metadata.create_bucket(name)
    }

    pub fn delete_bucket(&self, name: &str) -> AppResult<()> {
        validate_bucket_name(name)?;
        self.metadata.delete_bucket(name)
    }

    pub fn list_buckets(&self) -> AppResult<Vec<Bucket>> {
        self.metadata.list_buckets()
    }
}
