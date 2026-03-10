use std::{
    env,
    net::{AddrParseError, IpAddr, SocketAddr},
    num::ParseIntError,
    path::PathBuf,
};

const DEFAULT_HOST: &str = "0.0.0.0";
const DEFAULT_PORT: u16 = 20001;
const DEFAULT_DATA_DIR: &str = "data";
const DEFAULT_REPLICATION_FACTOR: usize = 3;
const DEFAULT_WRITE_QUORUM: usize = 2;
const DEFAULT_READ_QUORUM: usize = 2;

#[derive(Clone, Debug)]
pub struct Config {
    pub host: IpAddr,
    pub port: u16,
    pub data_dir: PathBuf,
    pub cluster: ClusterConfig,
}

#[derive(Clone, Debug)]
pub struct ClusterConfig {
    pub enabled: bool,
    pub node_id: String,
    pub seed_nodes: Vec<SeedNode>,
    pub replication_factor: usize,
    pub write_quorum: usize,
    pub read_quorum: usize,
    pub data_weight: u32,
    pub heartbeat_interval_ms: u64,
    pub node_timeout_ms: u64,
}

#[derive(Clone, Debug)]
pub struct SeedNode {
    pub host: String,
    pub port: u16,
}

impl ClusterConfig {
    pub fn default_with_node_id(node_id: String) -> Self {
        Self {
            enabled: false,
            node_id,
            seed_nodes: Vec::new(),
            replication_factor: DEFAULT_REPLICATION_FACTOR,
            write_quorum: DEFAULT_WRITE_QUORUM,
            read_quorum: DEFAULT_READ_QUORUM,
            data_weight: 100,
            heartbeat_interval_ms: 1000,
            node_timeout_ms: 3000,
        }
    }

    pub fn seed_nodes_string(&self) -> String {
        self.seed_nodes
            .iter()
            .map(|n| format!("{}:{}", n.host, n.port))
            .collect::<Vec<_>>()
            .join(",")
    }
}

impl Config {
    pub fn new(host: IpAddr, port: u16, data_dir: impl Into<PathBuf>) -> Self {
        Self {
            host,
            port,
            data_dir: data_dir.into(),
            cluster: ClusterConfig::default_with_node_id(uuid::Uuid::new_v4().to_string()),
        }
    }

    pub fn from_env() -> Result<Self, ConfigError> {
        let host_raw = env::var("TINYFS_HOST").unwrap_or_else(|_| DEFAULT_HOST.to_string());
        let port_raw = env::var("TINYFS_PORT").unwrap_or_else(|_| DEFAULT_PORT.to_string());
        let data_dir = env::var("TINYFS_DATA_DIR").unwrap_or_else(|_| DEFAULT_DATA_DIR.to_string());

        let cluster_enabled = env::var("TINYFS_CLUSTER_MODE")
            .map(|v| v.to_lowercase() == "true")
            .unwrap_or(false);

        let node_id =
            env::var("TINYFS_NODE_ID").unwrap_or_else(|_| uuid::Uuid::new_v4().to_string());

        let seed_nodes_raw = env::var("TINYFS_SEED_NODES").unwrap_or_default();
        let seed_nodes: Vec<SeedNode> = if seed_nodes_raw.is_empty() {
            Vec::new()
        } else {
            seed_nodes_raw
                .split(',')
                .filter(|s| !s.is_empty())
                .map(|s| {
                    let parts: Vec<&str> = s.split(':').collect();
                    SeedNode {
                        host: parts.first().unwrap_or(&"localhost").to_string(),
                        port: parts
                            .get(1)
                            .and_then(|p| p.parse().ok())
                            .unwrap_or(DEFAULT_PORT),
                    }
                })
                .collect()
        };

        let replication_factor = env::var("TINYFS_REPLICATION_FACTOR")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_REPLICATION_FACTOR);

        let write_quorum = env::var("TINYFS_WRITE_QUORUM")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_WRITE_QUORUM);

        let read_quorum = env::var("TINYFS_READ_QUORUM")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_READ_QUORUM);

        let data_weight = env::var("TINYFS_DATA_WEIGHT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(100);

        let heartbeat_interval_ms = env::var("TINYFS_HEARTBEAT_INTERVAL_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1000);

        let node_timeout_ms = env::var("TINYFS_NODE_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(3000);

        let host = host_raw
            .parse()
            .map_err(|source| ConfigError::InvalidHost {
                value: host_raw,
                source,
            })?;
        let port = port_raw
            .parse()
            .map_err(|source| ConfigError::InvalidPort {
                value: port_raw,
                source,
            })?;

        Ok(Self {
            host,
            port,
            data_dir: data_dir.into(),
            cluster: ClusterConfig {
                enabled: cluster_enabled,
                node_id,
                seed_nodes,
                replication_factor,
                write_quorum,
                read_quorum,
                data_weight,
                heartbeat_interval_ms,
                node_timeout_ms,
            },
        })
    }

    pub fn bind_addr(&self) -> SocketAddr {
        SocketAddr::from((self.host, self.port))
    }

    pub fn is_cluster_mode(&self) -> bool {
        self.cluster.enabled
    }

    pub fn node_id(&self) -> &str {
        &self.cluster.node_id
    }
}

#[derive(Debug)]
pub enum ConfigError {
    InvalidHost {
        value: String,
        source: AddrParseError,
    },
    InvalidPort {
        value: String,
        source: ParseIntError,
    },
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidHost { value, .. } => write!(f, "invalid host `{value}`"),
            Self::InvalidPort { value, .. } => write!(f, "invalid port `{value}`"),
        }
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidHost { source, .. } => Some(source),
            Self::InvalidPort { source, .. } => Some(source),
        }
    }
}
