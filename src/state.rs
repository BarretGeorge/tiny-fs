use std::sync::Arc;

use crate::{
    cluster::{ClusterManager, FailureDetector, NodeDiscovery, NodeRole},
    config::Config,
    meta::SqliteMetadataStore,
    replication::ReplicationManager,
    service::{BucketService, ObjectService},
    sharding::ShardMapper,
    AppResult, BlobStore, StorageLayout,
};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub storage: Arc<StorageLayout>,
    pub bucket_service: Arc<BucketService>,
    pub object_service: Arc<ObjectService>,
    pub cluster_manager: Option<Arc<ClusterManager>>,
    pub shard_mapper: Option<Arc<ShardMapper>>,
    pub replication_manager: Option<Arc<ReplicationManager>>,
    pub node_discovery: Option<Arc<NodeDiscovery>>,
}

impl AppState {
    pub fn new(config: Config, storage: StorageLayout) -> AppResult<Self> {
        let metadata = SqliteMetadataStore::new(storage.meta_db_path.clone());
        metadata.initialize()?;

        let blob_store = BlobStore::new(storage.clone());
        blob_store.cleanup_staging()?;
        metadata.clear_multipart_uploads()?;

        let cluster_manager = if config.is_cluster_mode() {
            let cm = ClusterManager::new(
                config.node_id().to_string(),
                config.cluster.replication_factor,
                config.cluster.write_quorum,
                config.cluster.read_quorum,
            );

            cm.add_local_node(
                config.host.to_string(),
                config.port,
                NodeRole::Primary,
                config.cluster.data_weight,
            );

            Some(Arc::new(cm))
        } else {
            None
        };

        let shard_mapper = cluster_manager.as_ref().map(|cm| {
            let mapper = ShardMapper::new(cm.registry().clone(), config.cluster.replication_factor);
            mapper.rebuild_ring();
            Arc::new(mapper)
        });

        let replication_manager = cluster_manager.as_ref().map(|cm| {
            let config = crate::replication::ReplicationConfig {
                mode: crate::replication::ReplicationMode::Sync,
                write_quorum: cm.write_quorum(),
                read_quorum: cm.read_quorum(),
                replication_factor: cm.replication_factor(),
                timeout: std::time::Duration::from_millis(5000),
            };
            Arc::new(ReplicationManager::new(
                config,
                cm.registry()
                    .get_self_node()
                    .map(|n| n.id.id)
                    .unwrap_or_default(),
            ))
        });

        let config_arc = Arc::new(config.clone());
        let node_discovery = cluster_manager.as_ref().map(|cm| {
            Arc::new(NodeDiscovery::new(config_arc.clone(), cm.clone()))
        });

        Ok(Self {
            config: Arc::new(config),
            storage: Arc::new(storage),
            bucket_service: Arc::new(BucketService::new(metadata.clone())),
            object_service: Arc::new(ObjectService::new(metadata, blob_store)),
            cluster_manager,
            shard_mapper,
            replication_manager,
            node_discovery,
        })
    }

    pub async fn start_cluster_services(&self) -> AppResult<()> {
        if let Some(ref discovery) = self.node_discovery {
            discovery.start().await?;
        }
        Ok(())
    }

    pub async fn stop_cluster_services(&self) {
        if let Some(ref discovery) = self.node_discovery {
            discovery.stop().await;
        }
    }

    pub fn is_cluster_mode(&self) -> bool {
        self.cluster_manager.is_some()
    }
}
