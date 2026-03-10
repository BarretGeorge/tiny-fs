use std::{
    collections::HashMap,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::time::interval;

use crate::{
    sharding::ShardMapper,
    AppError, AppResult,
};

#[async_trait]
pub trait DistributedStore: Send + Sync {
    async fn read_object(&self, bucket: &str, key: &str) -> AppResult<Vec<u8>>;
    async fn write_object(&self, bucket: &str, key: &str, data: &[u8]) -> AppResult<WriteResult>;
    async fn delete_object(&self, bucket: &str, key: &str) -> AppResult<()>;
    async fn list_objects(&self, bucket: &str) -> AppResult<ListResult>;
}

#[derive(Clone, Debug)]
pub struct WriteResult {
    pub bucket: String,
    pub key: String,
    pub size: u64,
    pub etag: String,
    pub version: u64,
    pub quorum_achieved: bool,
    pub failed_nodes: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct ListResult {
    pub objects: Vec<ObjectInfo>,
    pub common_prefixes: Vec<String>,
    pub continuation_token: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ObjectInfo {
    pub bucket: String,
    pub key: String,
    pub size: u64,
    pub etag: String,
    pub last_modified: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObjectVersion {
    pub version_id: String,
    pub bucket: String,
    pub key: String,
    pub size: u64,
    pub etag: String,
    pub created_at: i64,
    pub deleted_at: Option<i64>,
}

pub struct DistributedOps {
    local_ops: Arc<LocalObjectOps>,
    shard_mapper: Arc<ShardMapper>,
    replication_config: ReplicationConfig,
    node_clients: Arc<RwLock<HashMap<String, NodeClient>>>,
    write_quorum: usize,
    read_quorum: usize,
}

#[derive(Clone)]
pub struct LocalObjectOps {
    get_object_fn: Arc<dyn Fn(&str, &str) -> Result<Vec<u8>, AppError> + Send + Sync>,
    put_object_fn: Arc<dyn Fn(&str, &str, &[u8]) -> Result<WriteResult, AppError> + Send + Sync>,
    delete_object_fn: Arc<dyn Fn(&str, &str) -> Result<(), AppError> + Send + Sync>,
}

impl LocalObjectOps {
    pub fn new(
        get: impl Fn(&str, &str) -> Result<Vec<u8>, AppError> + Send + Sync + 'static,
        put: impl Fn(&str, &str, &[u8]) -> Result<WriteResult, AppError> + Send + Sync + 'static,
        delete: impl Fn(&str, &str) -> Result<(), AppError> + Send + Sync + 'static,
    ) -> Self {
        Self {
            get_object_fn: Arc::new(get),
            put_object_fn: Arc::new(put),
            delete_object_fn: Arc::new(delete),
        }
    }

    pub fn get(&self, bucket: &str, key: &str) -> Result<Vec<u8>, AppError> {
        (self.get_object_fn)(bucket, key)
    }

    pub fn put(&self, bucket: &str, key: &str, data: &[u8]) -> Result<WriteResult, AppError> {
        (self.put_object_fn)(bucket, key, data)
    }

    pub fn delete(&self, bucket: &str, key: &str) -> Result<(), AppError> {
        (self.delete_object_fn)(bucket, key)
    }
}

#[derive(Debug)]
pub struct ReplicationConfig {
    pub strategy: ReplicationStrategy,
    pub write_quorum: usize,
    pub read_quorum: usize,
    pub replication_factor: usize,
    pub max_retries: usize,
    pub timeout: Duration,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ReplicationStrategy {
    Strict,
    BestEffort,
    Async,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            strategy: ReplicationStrategy::BestEffort,
            write_quorum: 2,
            read_quorum: 2,
            replication_factor: 3,
            max_retries: 3,
            timeout: Duration::from_secs(30),
        }
    }
}

impl Clone for ReplicationConfig {
    fn clone(&self) -> Self {
        Self {
            strategy: self.strategy.clone(),
            write_quorum: self.write_quorum,
            read_quorum: self.read_quorum,
            replication_factor: self.replication_factor,
            max_retries: self.max_retries,
            timeout: self.timeout,
        }
    }
}

impl DistributedOps {
    pub fn new(
        shard_mapper: Arc<ShardMapper>,
        local_ops: LocalObjectOps,
        config: ReplicationConfig,
    ) -> Self {
        Self {
            local_ops: Arc::new(local_ops),
            shard_mapper,
            replication_config: config.clone(),
            node_clients: Arc::new(RwLock::new(HashMap::new())),
            write_quorum: config.write_quorum,
            read_quorum: config.read_quorum,
        }
    }

    pub async fn get_object(&self, bucket: &str, key: &str) -> AppResult<Vec<u8>> {
        let target = self.shard_mapper.get_nodes_for_key(bucket, key);

        let primary_exists = target.primary.as_ref()
            .map(|id| self.shard_mapper.is_local(id))
            .unwrap_or(false);

        if primary_exists {
            return self.local_ops.get(bucket, key).map_err(|e| e);
        }

        for node_id in &target.replicas {
            if self.shard_mapper.is_local(node_id) {
                return self.local_ops.get(bucket, key).map_err(|e| e);
            }
        }

        Err(AppError::ObjectNotFound)
    }

    pub async fn put_object(&self, bucket: &str, key: &str, data: &[u8]) -> AppResult<WriteResult> {
        let target = self.shard_mapper.get_nodes_for_key(bucket, key);

        let primary_exists = target.primary.as_ref()
            .map(|id| self.shard_mapper.is_local(id))
            .unwrap_or(false);

        if !primary_exists {
            return Err(AppError::InvalidRequest("Not the primary node".to_string()));
        }

        let local_result = self.local_ops.put(bucket, key, data).map_err(|e| e)?;

        let replication_tasks: Vec<_> = target.replicas
            .iter()
            .filter(|id| !self.shard_mapper.is_local(id))
            .map(|node_id| {
                let node_id = node_id.clone();
                let bucket = bucket.to_string();
                let key = key.to_string();
                let data = data.to_vec();
                async move {
                    self.replicate_to_node(&node_id, &bucket, &key, &data).await
                }
            })
            .collect();

        let mut failed_nodes = Vec::new();
        let mut success_count = 1;

        for task in replication_tasks {
            match task.await {
                Ok(_) => success_count += 1,
                Err(e) => failed_nodes.push(e.to_string()),
            }
        }

        let quorum_achieved = success_count >= self.write_quorum;

        Ok(WriteResult {
            bucket: bucket.to_string(),
            key: key.to_string(),
            size: data.len() as u64,
            etag: local_result.etag,
            version: local_result.version,
            quorum_achieved,
            failed_nodes,
        })
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> AppResult<()> {
        let target = self.shard_mapper.get_nodes_for_key(bucket, key);

        let primary_exists = target.primary.as_ref()
            .map(|id| self.shard_mapper.is_local(id))
            .unwrap_or(false);

        if !primary_exists {
            return Err(AppError::InvalidRequest("Not the primary node".to_string()));
        }

        self.local_ops.delete(bucket, key).map_err(|e| e)?;

        for node_id in &target.replicas {
            if !self.shard_mapper.is_local(node_id) {
                let _ = self.delete_on_node(node_id, bucket, key).await;
            }
        }

        Ok(())
    }

    async fn replicate_to_node(&self, _node_id: &str, _bucket: &str, _key: &str, _data: &[u8]) -> AppResult<()> {
        Ok(())
    }

    async fn delete_on_node(&self, _node_id: &str, _bucket: &str, _key: &str) -> AppResult<()> {
        Ok(())
    }
}

pub struct NodeClient {
    pub node_id: String,
    pub address: String,
}

impl NodeClient {
    pub fn new(node_id: String, address: String) -> Self {
        Self {
            node_id,
            address,
        }
    }
}

pub struct ReadRepair {
    config: ReplicationConfig,
}

impl ReadRepair {
    pub fn new(config: ReplicationConfig) -> Self {
        Self { config }
    }

    pub async fn repair_read(
        &self,
        _bucket: &str,
        _key: &str,
        available_nodes: &[String],
        data_blocks: usize,
        _parity_blocks: usize,
    ) -> AppResult<Vec<u8>> {
        if available_nodes.len() >= data_blocks {
            Ok(vec![])
        } else {
            Err(AppError::InsufficientNodes {
                required: data_blocks,
                available: available_nodes.len(),
            })
        }
    }
}

pub struct BackgroundRepair {
    interval: Duration,
    running: Arc<RwLock<bool>>,
}

impl BackgroundRepair {
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            running: Arc::new(RwLock::new(false)),
        }
    }

    pub fn start<F>(&self, _check_fn: F)
    where
        F: Fn() -> AppResult<Vec<RepairTask>> + Send + Sync + 'static,
    {
        *self.running.write() = true;
        
        let running = self.running.clone();
        let interval_duration = self.interval;
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval_duration);
            
            loop {
                interval.tick().await;
                
                if !*running.read() {
                    break;
                }
            }
        });
    }

    pub fn stop(&self) {
        *self.running.write() = false;
    }
}

#[derive(Clone, Debug)]
pub struct RepairTask {
    pub bucket: String,
    pub key: String,
    pub missing_nodes: Vec<String>,
    pub priority: RepairPriority,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RepairPriority {
    High,
    Medium,
    Low,
}
