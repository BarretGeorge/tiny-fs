use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::cluster::NodeRegistry;

const VIRTUAL_NODES: u32 = 150;

#[derive(Clone, Debug)]
pub struct ShardKey {
    pub bucket: String,
    pub key: String,
}

impl ShardKey {
    pub fn new(bucket: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            key: key.into(),
        }
    }

    pub fn hash(&self) -> u64 {
        let combined = format!("{}:{}", self.bucket, self.key);
        let mut hasher = Sha256::new();
        hasher.update(combined.as_bytes());
        let result = hasher.finalize();
        u64::from_le_bytes(result[..8].try_into().unwrap())
    }
}

#[derive(Clone, Debug)]
pub struct HashRing {
    ring: BTreeMap<u64, RingEntry>,
    virtual_nodes: HashMap<String, Vec<u64>>,
    node_weights: HashMap<String, u32>,
}

#[derive(Clone, Debug)]
pub struct RingEntry {
    pub node_id: String,
    pub virtual_index: u32,
}

impl HashRing {
    pub fn new() -> Self {
        Self {
            ring: BTreeMap::new(),
            virtual_nodes: HashMap::new(),
            node_weights: HashMap::new(),
        }
    }

    pub fn add_node(&mut self, node_id: &str, weight: u32) {
        let normalized_weight = weight.max(1);
        let vnodes = (normalized_weight * VIRTUAL_NODES / 100).max(1) as usize;

        let node_vnodes: Vec<u64> = (0..vnodes)
            .map(|i| self.hash_node(node_id, i as u32))
            .collect();

        for (idx, vnode_hash) in node_vnodes.iter().enumerate() {
            self.ring.insert(
                *vnode_hash,
                RingEntry {
                    node_id: node_id.to_string(),
                    virtual_index: idx as u32,
                },
            );
        }

        self.virtual_nodes.insert(node_id.to_string(), node_vnodes);
        self.node_weights.insert(node_id.to_string(), weight);
    }

    pub fn remove_node(&self, node_id: &str) -> Self {
        let mut new_ring = Self::new();

        for (id, weight) in &self.node_weights {
            if id != node_id {
                new_ring.add_node(id, *weight);
            }
        }

        new_ring
    }

    pub fn get_primary(&self, key: &ShardKey) -> Option<String> {
        let hash = key.hash();
        self.ring
            .range(hash..)
            .next()
            .map(|(_, entry)| entry.node_id.clone())
            .or_else(|| {
                self.ring
                    .first_key_value()
                    .map(|(_, entry)| entry.node_id.clone())
            })
    }

    pub fn get_replicas(&self, key: &ShardKey, count: usize) -> Vec<String> {
        let hash = key.hash();
        let mut replicas = Vec::new();
        let mut seen = HashSet::new();

        let entries: Vec<_> = self.ring.iter().collect();
        if entries.is_empty() {
            return replicas;
        }

        let start = entries
            .binary_search_by(|&(k, _)| {
                if *k <= hash {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Greater
                }
            })
            .unwrap_or_else(|i| i);

        let total = entries.len();
        let mut idx = start;

        while replicas.len() < count && replicas.len() < total {
            let entry = entries[idx % total];
            if seen.insert(entry.1.node_id.clone()) {
                replicas.push(entry.1.node_id.clone());
            }
            idx += 1;
        }

        replicas
    }

    pub fn get_all_nodes(&self) -> Vec<String> {
        self.node_weights.keys().cloned().collect()
    }

    pub fn node_count(&self) -> usize {
        self.node_weights.len()
    }

    fn hash_node(&self, node_id: &str, virtual_index: u32) -> u64 {
        let combined = format!("{}:{}", node_id, virtual_index);
        let mut hasher = Sha256::new();
        hasher.update(combined.as_bytes());
        let result = hasher.finalize();
        u64::from_le_bytes(result[..8].try_into().unwrap())
    }
}

impl Default for HashRing {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct ShardMapper {
    ring: Arc<RwLock<HashRing>>,
    registry: NodeRegistry,
    replication_factor: usize,
}

impl ShardMapper {
    pub fn new(registry: NodeRegistry, replication_factor: usize) -> Self {
        Self {
            ring: Arc::new(RwLock::new(HashRing::new())),
            registry,
            replication_factor,
        }
    }

    pub fn rebuild_ring(&self) {
        let mut ring = self.ring.write();
        *ring = HashRing::new();

        let nodes = self.registry.list_nodes();
        for node in nodes {
            if node.is_alive(std::time::Duration::from_secs(3)) {
                ring.add_node(&node.id.id, node.data_weight);
            }
        }
    }

    pub fn add_node_to_ring(&self, node_id: &str, weight: u32) {
        let mut ring = self.ring.write();
        ring.add_node(node_id, weight);
    }

    pub fn remove_node_from_ring(&self, node_id: &str) {
        let mut ring = self.ring.write();
        *ring = ring.remove_node(node_id);
    }

    pub fn get_nodes_for_key(&self, bucket: &str, key: &str) -> ShardTarget {
        let shard_key = ShardKey::new(bucket, key);
        let ring = self.ring.read();

        let primary = ring.get_primary(&shard_key);
        let replicas = ring.get_replicas(&shard_key, self.replication_factor);

        ShardTarget { primary, replicas }
    }

    pub fn is_local(&self, node_id: &str) -> bool {
        self.registry
            .get_self_node()
            .map(|n| n.id.id == node_id)
            .unwrap_or(false)
    }
}

use parking_lot::RwLock;

#[derive(Clone, Debug)]
pub struct ShardTarget {
    pub primary: Option<String>,
    pub replicas: Vec<String>,
}

impl ShardTarget {
    pub fn all_nodes(&self) -> Vec<String> {
        let mut nodes = Vec::new();
        if let Some(ref primary) = self.primary {
            nodes.push(primary.clone());
        }
        for replica in &self.replicas {
            if !nodes.contains(replica) {
                nodes.push(replica.clone());
            }
        }
        nodes
    }

    pub fn needs_replication(&self) -> bool {
        self.replicas.len() > 1
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShardInfo {
    pub bucket: String,
    pub key: String,
    pub primary_node: String,
    pub replica_nodes: Vec<String>,
    pub is_local: bool,
}

pub struct RoutingTable {
    mapper: ShardMapper,
    self_node_id: String,
}

impl RoutingTable {
    pub fn new(mapper: ShardMapper, self_node_id: String) -> Self {
        Self {
            mapper,
            self_node_id,
        }
    }

    pub fn route(&self, bucket: &str, key: &str) -> RouteDecision {
        let target = self.mapper.get_nodes_for_key(bucket, key);

        let is_primary = target
            .primary
            .as_ref()
            .map(|id| id == &self.self_node_id)
            .unwrap_or(false);

        let is_replica = target.replicas.iter().any(|id| id == &self.self_node_id);

        RouteDecision {
            target,
            is_primary,
            is_replica,
            should_process: is_primary || is_replica,
        }
    }

    pub fn get_shard_info(&self, bucket: &str, key: &str) -> ShardInfo {
        let target = self.mapper.get_nodes_for_key(bucket, key);

        ShardInfo {
            bucket: bucket.to_string(),
            key: key.to_string(),
            primary_node: target.primary.clone().unwrap_or_default(),
            replica_nodes: target.replicas.clone(),
            is_local: self.mapper.is_local(&self.self_node_id),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RouteDecision {
    pub target: ShardTarget,
    pub is_primary: bool,
    pub is_replica: bool,
    pub should_process: bool,
}

impl RouteDecision {
    pub fn can_read(&self) -> bool {
        self.is_primary || self.is_replica
    }

    pub fn can_write(&self) -> bool {
        self.is_primary
    }
}
