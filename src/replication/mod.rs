use std::{
    collections::HashMap,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::AppResult;

#[async_trait]
pub trait Replicator: Send + Sync {
    async fn replicate_write(
        &self,
        bucket: &str,
        key: &str,
        data: &[u8],
        replication_factor: usize,
    ) -> AppResult<ReplicationResult>;

    async fn replicate_delete(
        &self,
        bucket: &str,
        key: &str,
    ) -> AppResult<()>;
}

#[derive(Clone, Debug)]
pub struct ReplicationResult {
    pub success_count: usize,
    pub failed_nodes: Vec<String>,
    pub quorum_met: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicaStats {
    pub node_id: String,
    pub data_version: u64,
    pub checksum: String,
    pub timestamp: i64,
}

#[derive(Clone, Debug)]
pub enum ReplicationMode {
    Sync,
    Async,
}

pub struct ReplicationConfig {
    pub mode: ReplicationMode,
    pub write_quorum: usize,
    pub read_quorum: usize,
    pub replication_factor: usize,
    pub timeout: Duration,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            mode: ReplicationMode::Sync,
            write_quorum: 2,
            read_quorum: 2,
            replication_factor: 3,
            timeout: Duration::from_secs(5),
        }
    }
}

pub struct ReplicationManager {
    config: ReplicationConfig,
    local_node_id: String,
    replica_states: Arc<RwLock<HashMap<String, ReplicaState>>>,
}

struct ReplicaState {
    node_id: String,
    last_sync: Option<Duration>,
    data_version: u64,
    pending_operations: usize,
}

impl ReplicationManager {
    pub fn new(config: ReplicationConfig, local_node_id: String) -> Self {
        Self {
            config,
            local_node_id,
            replica_states: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn config(&self) -> &ReplicationConfig {
        &self.config
    }

    pub fn register_replica(&self, node_id: String) {
        let mut states = self.replica_states.write();
        states.insert(
            node_id.clone(),
            ReplicaState {
                node_id,
                last_sync: None,
                data_version: 0,
                pending_operations: 0,
            },
        );
    }

    pub fn unregister_replica(&self, node_id: &str) {
        let mut states = self.replica_states.write();
        states.remove(node_id);
    }

    pub fn record_write_success(&self, node_id: &str) {
        let mut states = self.replica_states.write();
        if let Some(state) = states.get_mut(node_id) {
            state.data_version += 1;
            state.pending_operations = state.pending_operations.saturating_sub(1);
            state.last_sync = Some(Duration::from_secs(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            ));
        }
    }

    pub fn record_write_pending(&self, node_id: &str) {
        let mut states = self.replica_states.write();
        if let Some(state) = states.get_mut(node_id) {
            state.pending_operations += 1;
        }
    }

    pub fn get_replica_status(&self) -> Vec<ReplicaStats> {
        let states = self.replica_states.read();
        states
            .values()
            .map(|s| ReplicaStats {
                node_id: s.node_id.clone(),
                data_version: s.data_version,
                checksum: String::new(),
                timestamp: s.last_sync
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0),
            })
            .collect()
    }

    pub fn check_quorum(&self, success_count: usize) -> bool {
        success_count >= self.config.write_quorum
    }

    pub fn should_retry(&self, success_count: usize) -> bool {
        success_count < self.config.write_quorum
            && success_count + self.pending_replicas() >= self.config.write_quorum
    }

    fn pending_replicas(&self) -> usize {
        let states = self.replica_states.read();
        states.values()
            .filter(|s| s.pending_operations > 0)
            .count()
    }

    pub fn is_replication_complete(&self) -> bool {
        let states = self.replica_states.read();
        states.values().all(|s| s.pending_operations == 0)
    }

    pub fn get_lagging_replicas(&self, max_lag: u64) -> Vec<String> {
        let states = self.replica_states.read();
        let local_version = states
            .get(&self.local_node_id)
            .map(|s| s.data_version)
            .unwrap_or(0);

        states
            .values()
            .filter(|s| {
                s.node_id != self.local_node_id
                    && local_version.saturating_sub(s.data_version) > max_lag
            })
            .map(|s| s.node_id.clone())
            .collect()
    }
}

pub struct ConflictResolver {
    strategy: ConflictResolutionStrategy,
}

#[derive(Clone, Debug)]
pub enum ConflictResolutionStrategy {
    LastWriteWins,
    VersionVector,
    Manual,
}

impl ConflictResolver {
    pub fn new(strategy: ConflictResolutionStrategy) -> Self {
        Self { strategy }
    }

    pub fn resolve(&self, versions: &[ReplicaStats]) -> Option<String> {
        match self.strategy {
            ConflictResolutionStrategy::LastWriteWins => {
                versions.iter().max_by_key(|v| v.timestamp).map(|v| v.node_id.clone())
            }
            ConflictResolutionStrategy::VersionVector => {
                let max_version = versions.iter().max_by_key(|v| v.data_version)?;
                Some(max_version.node_id.clone())
            }
            ConflictResolutionStrategy::Manual => None,
        }
    }
}

pub struct WriteAheadLog {
    entries: Arc<RwLock<Vec<WalEntry>>>,
    max_entries: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WalEntry {
    pub id: u64,
    pub operation: WalOperation,
    pub timestamp: i64,
    pub status: WalStatus,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WalOperation {
    Write { bucket: String, key: String, size: u64, checksum: String },
    Delete { bucket: String, key: String },
    MetadataUpdate { bucket: String, key: String },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WalStatus {
    Pending,
    Replicating,
    Completed,
    Failed,
}

impl WriteAheadLog {
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: Arc::new(RwLock::new(Vec::new())),
            max_entries,
        }
    }

    pub fn append(&self, operation: WalOperation) -> u64 {
        let mut entries = self.entries.write();
        let id = entries.len() as u64 + 1;
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        entries.push(WalEntry {
            id,
            operation,
            timestamp,
            status: WalStatus::Pending,
        });

        if entries.len() > self.max_entries {
            entries.remove(0);
        }

        id
    }

    pub fn update_status(&self, id: u64, status: WalStatus) {
        let mut entries = self.entries.write();
        if let Some(entry) = entries.iter_mut().find(|e| e.id == id) {
            entry.status = status;
        }
    }

    pub fn get_pending(&self) -> Vec<WalEntry> {
        let entries = self.entries.read();
        entries
            .iter()
            .filter(|e| e.status == WalStatus::Pending || e.status == WalStatus::Replicating)
            .cloned()
            .collect()
    }

    pub fn clear_completed(&self) {
        let mut entries = self.entries.write();
        entries.retain(|e| e.status != WalStatus::Completed);
    }
}
