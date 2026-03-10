use std::{
    collections::HashMap,
    future::Future,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Mutex};

use crate::AppResult;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum GossipMessage {
    Ping,
    Pong,
    Join(JoinMessage),
    Leave(LeaveMessage),
    StateSync(StateSyncMessage),
    RequestState(RequestStateMessage),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JoinMessage {
    pub node_id: String,
    pub host: String,
    pub port: u16,
    pub data_weight: u32,
    pub timestamp: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LeaveMessage {
    pub node_id: String,
    pub timestamp: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StateSyncMessage {
    pub members: Vec<MemberState>,
    pub timestamp: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RequestStateMessage {
    pub from_node: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MemberState {
    pub node_id: String,
    pub host: String,
    pub port: u16,
    pub data_weight: u32,
    pub status: MemberStatus,
    pub incarnation: u64,
    pub last_update: i64,
    pub timestamp: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemberStatus {
    Alive,
    Suspect,
    Dead,
}

pub struct GossipConfig {
    pub port: u16,
    pub gossip_interval: Duration,
    pub probe_interval: Duration,
    pub probe_count: usize,
    pub max_payload_size: usize,
    pub round_timeout: Duration,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            port: 0,
            gossip_interval: Duration::from_secs(1),
            probe_interval: Duration::from_millis(500),
            probe_count: 3,
            max_payload_size: 1024 * 1024,
            round_timeout: Duration::from_secs(5),
        }
    }
}

pub struct GossipState {
    pub members: HashMap<String, MemberState>,
    pub local_node: Option<MemberState>,
    pub incarnation: u64,
    pub gossip_version: u64,
    pub last_gossip_time: Instant,
}

impl Clone for GossipState {
    fn clone(&self) -> Self {
        Self {
            members: self.members.clone(),
            local_node: self.local_node.clone(),
            incarnation: self.incarnation,
            gossip_version: self.gossip_version,
            last_gossip_time: self.last_gossip_time,
        }
    }
}

impl GossipState {
    pub fn new() -> Self {
        Self {
            members: HashMap::new(),
            local_node: None,
            incarnation: 0,
            gossip_version: 0,
            last_gossip_time: Instant::now(),
        }
    }

    pub fn update_incarnation(&mut self) {
        self.incarnation += 1;
    }

    pub fn get_incarnation(&self) -> u64 {
        self.incarnation
    }

    pub fn get_local_node(&self) -> Option<MemberState> {
        self.local_node.clone()
    }

    pub fn set_local_node(&mut self, node: MemberState) {
        self.local_node = Some(node);
    }

    pub fn update_member(&mut self, member: MemberState) -> bool {
        if let Some(existing) = self.members.get(&member.node_id) {
            if member.incarnation < existing.incarnation {
                return false;
            }
            if member.incarnation == existing.incarnation && member.status == MemberStatus::Dead {
                return false;
            }
        }
        self.members.insert(member.node_id.clone(), member);
        true
    }

    pub fn mark_suspect(&mut self, node_id: &str) {
        if let Some(member) = self.members.get_mut(node_id) {
            if member.status == MemberStatus::Alive {
                member.status = MemberStatus::Suspect;
            }
        }
    }

    pub fn mark_dead(&mut self, node_id: &str) {
        if let Some(member) = self.members.get_mut(node_id) {
            member.status = MemberStatus::Dead;
        }
    }

    pub fn get_alive_members(&self) -> Vec<MemberState> {
        self.members
            .values()
            .filter(|m| m.status == MemberStatus::Alive)
            .cloned()
            .collect()
    }

    pub fn get_member(&self, node_id: &str) -> Option<MemberState> {
        self.members.get(node_id).cloned()
    }

    pub fn remove_member(&mut self, node_id: &str) {
        self.members.remove(node_id);
    }

    pub fn get_state_for_sync(&self) -> StateSyncMessage {
        let mut all_members: Vec<MemberState> = self.members.values().cloned().collect();
        if let Some(ref ln) = self.local_node {
            if !all_members.iter().any(|m| m.node_id == ln.node_id) {
                all_members.push(ln.clone());
            }
        }

        StateSyncMessage {
            members: all_members,
            timestamp: current_timestamp(),
        }
    }

    pub fn merge_state(&mut self, incoming: StateSyncMessage) {
        for member in incoming.members {
            if Some(member.node_id.clone()) != self.local_node.as_ref().map(|m| m.node_id.clone()) {
                self.update_member(member);
            }
        }
    }
}

impl Default for GossipState {
    fn default() -> Self {
        Self::new()
    }
}

pub struct GossipProtocol {
    config: GossipConfig,
    state: Arc<RwLock<GossipState>>,
    socket: Arc<Mutex<Option<tokio::net::UdpSocket>>>,
    shutdown_tx: Arc<RwLock<Option<mpsc::Sender<()>>>>,
}

impl GossipProtocol {
    pub fn new(config: GossipConfig, state: GossipState) -> Self {
        Self {
            config,
            state: Arc::new(RwLock::new(state)),
            socket: Arc::new(Mutex::new(None)),
            shutdown_tx: Arc::new(RwLock::new(None)),
        }
    }

    pub async fn start(&self, bind_addr: SocketAddr) -> Result<(), crate::AppError> {
        let socket = tokio::net::UdpSocket::bind(bind_addr).await?;
        socket.set_broadcast(true)?;
        
        *self.socket.lock().await = Some(socket);
        
        Ok(())
    }

    pub fn get_bind_address(&self) -> Option<SocketAddr> {
        None
    }

    pub async fn send_to(&self, addr: SocketAddr, message: &GossipMessage) -> Result<usize, crate::AppError> {
        let mut socket_guard = self.socket.lock().await;
        if let Some(ref mut socket) = *socket_guard {
            let data = serde_json::to_vec(message).map_err(|_e| crate::AppError::Internal)?;
            let sent = socket.send_to(&data, addr).await?;
            Ok(sent)
        } else {
            Err(crate::AppError::Internal)
        }
    }

    pub async fn receive(&self) -> Result<(SocketAddr, GossipMessage), crate::AppError> {
        let mut socket_guard = self.socket.lock().await;
        if let Some(ref mut socket) = *socket_guard {
            let mut buf = vec![0u8; self.config.max_payload_size];
            let (len, addr) = socket.recv_from(&mut buf).await?;
            let message: GossipMessage = serde_json::from_slice(&buf[..len]).map_err(|_e| crate::AppError::Internal)?;
            Ok((addr, message))
        } else {
            Err(crate::AppError::Internal)
        }
    }

    pub fn broadcast(&self, message: GossipMessage) -> impl Future<Output = ()> + '_ {
        let state = self.state.clone();
        async move {
            let members = state.read().get_alive_members();
            for member in members {
                let addr = format!("{}:{}", member.host, member.port)
                    .parse::<SocketAddr>()
                    .ok();
                if let Some(addr) = addr {
                    let _ = GossipProtocol::send_to(&GossipProtocol::new(GossipConfig::default(), GossipState::new()), addr, &message).await;
                }
            }
        }
    }

    pub fn select_targets(&self, count: usize) -> Vec<MemberState> {
        let alive = self.state.read().get_alive_members();
        if alive.len() <= count {
            return alive;
        }

        let mut targets = Vec::new();

        for _ in 0..count {
            if let Some(idx) = alive.iter().position(|m| !targets.iter().any(|t: &MemberState| t.node_id == m.node_id)) {
                targets.push(alive[idx].clone());
            }
        }

        targets
    }

    pub fn merge_state(&self, incoming: StateSyncMessage) {
        self.state.write().merge_state(incoming);
    }

    pub fn is_converged(&self) -> bool {
        let members = self.state.read().get_alive_members();
        if members.len() < 2 {
            return true;
        }

        let versions: Vec<u64> = members.iter().map(|m| m.incarnation).collect();
        let first = versions[0];
        versions.iter().all(|&v| v == first)
    }
}

fn current_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}
