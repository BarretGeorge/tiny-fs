mod discovery;
mod gossip;

pub use discovery::{FailureDetector, NodeDiscovery};
pub use gossip::{GossipConfig, GossipMessage, GossipProtocol, GossipState, MemberState, MemberStatus};
pub use crate::config::SeedNode;

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId {
    pub id: String,
    pub host: String,
    pub port: u16,
}

impl NodeId {
    pub fn new(host: String, port: u16) -> Self {
        let id = Uuid::new_v4().to_string();
        Self { id, host, port }
    }

    pub fn socket_addr(&self) -> SocketAddr {
        format!("{}:{}", self.host, self.port).parse().unwrap()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeRole {
    Primary,
    Replica,
    Gateway,
}

#[derive(Clone, Debug)]
pub struct Node {
    pub id: NodeId,
    pub role: NodeRole,
    pub is_healthy: bool,
    pub last_heartbeat: Instant,
    pub data_weight: u32,
}

impl Node {
    pub fn new(id: NodeId, role: NodeRole, data_weight: u32) -> Self {
        Self {
            id,
            role,
            is_healthy: true,
            last_heartbeat: Instant::now(),
            data_weight,
        }
    }

    pub fn is_alive(&self, timeout: Duration) -> bool {
        self.is_healthy && self.last_heartbeat.elapsed() < timeout
    }

    pub fn update_heartbeat(&mut self) {
        self.last_heartbeat = Instant::now();
        self.is_healthy = true;
    }

    pub fn mark_unhealthy(&mut self) {
        self.is_healthy = false;
    }
}

#[derive(Clone, Debug)]
pub struct NodeRegistry {
    nodes: Arc<RwLock<HashMap<String, Node>>>,
    self_node_id: String,
}

impl NodeRegistry {
    pub fn new(self_node_id: String) -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            self_node_id,
        }
    }

    pub fn add_node(&self, node: Node) {
        let mut nodes = self.nodes.write();
        nodes.insert(node.id.id.clone(), node);
    }

    pub fn remove_node(&self, node_id: &str) -> Option<Node> {
        let mut nodes = self.nodes.write();
        nodes.remove(node_id)
    }

    pub fn get_node(&self, node_id: &str) -> Option<Node> {
        let nodes = self.nodes.read();
        nodes.get(node_id).cloned()
    }

    pub fn list_nodes(&self) -> Vec<Node> {
        let nodes = self.nodes.read();
        nodes.values().cloned().collect()
    }

    pub fn list_healthy_nodes(&self) -> Vec<Node> {
        let nodes = self.nodes.read();
        nodes
            .values()
            .filter(|n| n.is_alive(Duration::from_secs(3)))
            .cloned()
            .collect()
    }

    pub fn get_self_node(&self) -> Option<Node> {
        let nodes = self.nodes.read();
        nodes.get(&self.self_node_id).cloned()
    }

    pub fn update_heartbeat(&self, node_id: &str) -> bool {
        let mut nodes = self.nodes.write();
        if let Some(node) = nodes.get_mut(node_id) {
            node.update_heartbeat();
            true
        } else {
            false
        }
    }

    pub fn mark_node_unhealthy(&self, node_id: &str) {
        let mut nodes = self.nodes.write();
        if let Some(node) = nodes.get_mut(node_id) {
            node.mark_unhealthy();
        }
    }

    pub fn node_count(&self) -> usize {
        let nodes = self.nodes.read();
        nodes.len()
    }

    pub fn healthy_node_count(&self) -> usize {
        self.list_healthy_nodes().len()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterTopology {
    pub nodes: Vec<NodeInfo>,
    pub total_capacity: u64,
    pub replication_factor: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: String,
    pub host: String,
    pub port: u16,
    pub role: String,
    pub is_healthy: bool,
    pub data_weight: u32,
}

impl From<Node> for NodeInfo {
    fn from(node: Node) -> Self {
        Self {
            id: node.id.id,
            host: node.id.host,
            port: node.id.port,
            role: format!("{:?}", node.role),
            is_healthy: node.is_healthy,
            data_weight: node.data_weight,
        }
    }
}

pub struct ClusterManager {
    registry: NodeRegistry,
    replication_factor: usize,
    write_quorum: usize,
    read_quorum: usize,
}

impl ClusterManager {
    pub fn new(
        self_node_id: String,
        replication_factor: usize,
        write_quorum: usize,
        read_quorum: usize,
    ) -> Self {
        Self {
            registry: NodeRegistry::new(self_node_id),
            replication_factor,
            write_quorum,
            read_quorum,
        }
    }

    pub fn registry(&self) -> &NodeRegistry {
        &self.registry
    }

    pub fn add_local_node(&self, host: String, port: u16, role: NodeRole, weight: u32) {
        let node_id = NodeId {
            id: self
                .registry
                .get_self_node()
                .map(|n| n.id.id.clone())
                .unwrap_or_else(|| Uuid::new_v4().to_string()),
            host: host.clone(),
            port,
        };
        let node = Node::new(node_id, role, weight);
        self.registry.add_node(node);
    }

    pub fn add_remote_node(&self, host: String, port: u16, role: NodeRole, weight: u32) {
        let node_id = NodeId::new(host, port);
        let node = Node::new(node_id, role, weight);
        self.registry.add_node(node);
    }

    pub fn get_topology(&self) -> ClusterTopology {
        let nodes = self.registry.list_nodes();
        let total_capacity = nodes.iter().map(|n| n.data_weight as u64).sum();

        ClusterTopology {
            nodes: nodes.into_iter().map(NodeInfo::from).collect(),
            total_capacity,
            replication_factor: self.replication_factor,
        }
    }

    pub fn is_healthy(&self) -> bool {
        let healthy = self.registry.healthy_node_count();
        healthy >= self.write_quorum
    }

    pub fn replication_factor(&self) -> usize {
        self.replication_factor
    }

    pub fn write_quorum(&self) -> usize {
        self.write_quorum
    }

    pub fn read_quorum(&self) -> usize {
        self.read_quorum
    }
}
