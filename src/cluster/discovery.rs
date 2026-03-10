use std::{
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use parking_lot::RwLock;
use tokio::sync::mpsc;
use tokio::time::interval;

use crate::cluster::gossip::{GossipConfig, GossipMessage, GossipProtocol, GossipState, MemberState, MemberStatus};
use crate::cluster::{ClusterManager, NodeRole};
use crate::config::SeedNode;
use crate::AppResult;

pub struct NodeDiscovery {
    gossip: Arc<GossipProtocol>,
    state: Arc<RwLock<GossipState>>,
    cluster_manager: Arc<ClusterManager>,
    config: Arc<crate::Config>,
    shutdown_tx: Arc<RwLock<Option<mpsc::Sender<()>>>>,
    is_running: Arc<RwLock<bool>>,
}

impl NodeDiscovery {
    pub fn new(
        config: Arc<crate::Config>,
        cluster_manager: Arc<ClusterManager>,
    ) -> Self {
        let gossip_config = GossipConfig {
            port: config.port + 1000,
            gossip_interval: Duration::from_millis(config.cluster.heartbeat_interval_ms),
            probe_interval: Duration::from_millis(config.cluster.heartbeat_interval_ms / 2),
            probe_count: 3,
            ..Default::default()
        };

        let gossip_state = GossipState::new();
        let state = Arc::new(RwLock::new(gossip_state.clone()));

        let gossip = Arc::new(GossipProtocol::new(gossip_config, gossip_state));

        Self {
            gossip,
            state,
            cluster_manager,
            config,
            shutdown_tx: Arc::new(RwLock::new(None)),
            is_running: Arc::new(RwLock::new(false)),
        }
    }

    pub async fn start(&self) -> Result<(), crate::AppError> {
        let bind_addr = SocketAddr::from(([0, 0, 0, 0], self.config.port + 1000));
        self.gossip.start(bind_addr).await?;

        let local_node = MemberState {
            node_id: self.config.cluster.node_id.clone(),
            host: self.config.host.to_string(),
            port: self.config.port,
            data_weight: self.config.cluster.data_weight,
            status: MemberStatus::Alive,
            incarnation: 1,
            last_update: current_timestamp(),
            timestamp: current_timestamp(),
        };

        self.state.write().set_local_node(local_node);
        self.state.write().update_incarnation();

        let (tx, mut rx) = mpsc::channel::<()>(1);
        *self.shutdown_tx.write() = Some(tx);
        let is_running = self.is_running.clone();

        *is_running.write() = true;

        let gossip = self.gossip.clone();
        let state = self.state.clone();
        let cluster_manager = self.cluster_manager.clone();
        let config = self.config.clone();

        tokio::spawn(async move {
            if !config.cluster.seed_nodes.is_empty() {
                Self::connect_to_seed_nodes(&gossip, &config.cluster.seed_nodes, &config.cluster.node_id).await;
            }

            let mut gossip_interval = interval(Duration::from_millis(config.cluster.heartbeat_interval_ms));
            let mut probe_interval = interval(Duration::from_millis(config.cluster.heartbeat_interval_ms / 2));

            loop {
                tokio::select! {
                    _ = rx.recv() => {
                        break;
                    }
                    _ = gossip_interval.tick() => {
                        Self::run_gossip_round(&gossip, &state, &cluster_manager).await;
                    }
                    _ = probe_interval.tick() => {
                        Self::run_probe(&gossip, &state).await;
                    }
                    result = async { gossip.receive().await } => {
                        if let Ok((addr, message)) = result {
                            Self::handle_message(&gossip, &state, &cluster_manager, addr, message).await;
                        }
                    }
                }
            }

            *is_running.write() = false;
        });

        Ok(())
    }

    pub async fn stop(&self) {
        if let Some(tx) = self.shutdown_tx.write().take() {
            let _ = tx.send(()).await;
        }
    }

    pub fn is_running(&self) -> bool {
        *self.is_running.read()
    }

    async fn connect_to_seed_nodes(gossip: &GossipProtocol, seed_nodes: &[SeedNode], local_node_id: &str) {
        for seed in seed_nodes {
            let addr = format!("{}:{}", seed.host, seed.port + 1000);
            if let Ok(addr) = addr.parse::<SocketAddr>() {
                let join_msg = GossipMessage::Join(crate::cluster::gossip::JoinMessage {
                    node_id: local_node_id.to_string(),
                    host: "".to_string(),
                    port: 0,
                    data_weight: 100,
                    timestamp: current_timestamp(),
                });
                let _ = gossip.send_to(addr, &join_msg).await;
            }
        }
    }

    async fn run_gossip_round(gossip: &GossipProtocol, state: &Arc<RwLock<GossipState>>, _cluster_manager: &ClusterManager) {
        let targets = gossip.select_targets(3);
        
        for target in targets {
            let addr = format!("{}:{}", target.host, target.port + 1000);
            if let Ok(addr) = addr.parse::<SocketAddr>() {
                let sync_msg = GossipMessage::StateSync(state.read().get_state_for_sync());
                let _ = gossip.send_to(addr, &sync_msg).await;
            }
        }
    }

    async fn run_probe(gossip: &GossipProtocol, _state: &Arc<RwLock<GossipState>>) {
        let targets = gossip.select_targets(1);
        
        for target in targets {
            let addr = format!("{}:{}", target.host, target.port + 1000);
            if let Ok(addr) = addr.parse::<SocketAddr>() {
                let _ = gossip.send_to(addr, &GossipMessage::Ping).await;
            }
        }
    }

    async fn handle_message(
        gossip: &GossipProtocol,
        state: &Arc<RwLock<GossipState>>,
        _cluster_manager: &ClusterManager,
        _addr: SocketAddr,
        message: GossipMessage,
    ) {
        match message {
            GossipMessage::Ping => {
                let _ = gossip.send_to(_addr, &GossipMessage::Pong).await;
            }
            GossipMessage::Pong => {
            }
            GossipMessage::Join(join_msg) => {
                let member = MemberState {
                    node_id: join_msg.node_id,
                    host: join_msg.host,
                    port: join_msg.port,
                    data_weight: join_msg.data_weight,
                    status: MemberStatus::Alive,
                    incarnation: 1,
                    last_update: join_msg.timestamp,
                    timestamp: join_msg.timestamp,
                };
                state.write().update_member(member);
                
                let sync_msg = GossipMessage::StateSync(state.read().get_state_for_sync());
                let _ = gossip.send_to(_addr, &sync_msg).await;
            }
            GossipMessage::Leave(leave_msg) => {
                state.write().remove_member(&leave_msg.node_id);
            }
            GossipMessage::StateSync(sync_msg) => {
                state.write().merge_state(sync_msg);
            }
            GossipMessage::RequestState(_req_msg) => {
                let sync_msg = GossipMessage::StateSync(state.read().get_state_for_sync());
                let _ = gossip.send_to(_addr, &sync_msg).await;
            }
        }
    }

    pub fn get_cluster_members(&self) -> Vec<MemberState> {
        self.state.read().get_alive_members()
    }

    pub fn sync_to_cluster_manager(&self) {
        let members = self.state.read().get_alive_members();
        
        for member in &members {
            if member.node_id != self.config.cluster.node_id {
                self.cluster_manager.add_remote_node(
                    member.host.clone(),
                    member.port,
                    NodeRole::Replica,
                    member.data_weight,
                );
            }
        }
    }
}

fn current_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

pub struct FailureDetector {
    state: Arc<RwLock<GossipState>>,
    config: Arc<crate::Config>,
    suspicion_timeout: Duration,
}

impl FailureDetector {
    pub fn new(config: Arc<crate::Config>, state: Arc<RwLock<GossipState>>) -> Self {
        let suspicion_timeout = Duration::from_millis(config.cluster.node_timeout_ms);
        
        Self {
            state,
            config,
            suspicion_timeout,
        }
    }

    pub fn start(&self) {
        let state = self.state.clone();
        let timeout = self.suspicion_timeout;
        
        tokio::spawn(async move {
            let mut check_interval = interval(Duration::from_secs(1));
            
            loop {
                check_interval.tick().await;
                
                let members = state.read().get_alive_members();
                
                for member in members {
                    let time_since_update = Duration::from_secs(
                        (current_timestamp() - member.timestamp) as u64
                    );
                    
                    if time_since_update > timeout {
                        state.write().mark_suspect(&member.node_id);
                    }
                    
                    if time_since_update > timeout * 2 {
                        state.write().mark_dead(&member.node_id);
                    }
                }
            }
        });
    }
}
