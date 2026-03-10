use std::sync::Arc;

use crate::{
    meta::SqliteMetadataStore,
    service::{BucketService, ObjectService},
    AppResult, BlobStore, Config, StorageLayout,
};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub storage: Arc<StorageLayout>,
    pub bucket_service: Arc<BucketService>,
    pub object_service: Arc<ObjectService>,
}

impl AppState {
    pub fn new(config: Config, storage: StorageLayout) -> AppResult<Self> {
        let metadata = SqliteMetadataStore::new(storage.meta_db_path.clone());
        metadata.initialize()?;

        let blob_store = BlobStore::new(storage.clone());
        blob_store.cleanup_staging()?;
        metadata.clear_multipart_uploads()?;

        Ok(Self {
            config: Arc::new(config),
            storage: Arc::new(storage),
            bucket_service: Arc::new(BucketService::new(metadata.clone())),
            object_service: Arc::new(ObjectService::new(metadata, blob_store)),
        })
    }
}
