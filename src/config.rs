use std::{
    env,
    net::{AddrParseError, IpAddr, SocketAddr},
    num::ParseIntError,
    path::PathBuf,
};

const DEFAULT_HOST: &str = "0.0.0.0";
const DEFAULT_PORT: u16 = 20001;
const DEFAULT_DATA_DIR: &str = "data";

#[derive(Clone, Debug)]
pub struct Config {
    pub host: IpAddr,
    pub port: u16,
    pub data_dir: PathBuf,
}

impl Config {
    pub fn new(host: IpAddr, port: u16, data_dir: impl Into<PathBuf>) -> Self {
        Self {
            host,
            port,
            data_dir: data_dir.into(),
        }
    }

    pub fn from_env() -> Result<Self, ConfigError> {
        let host_raw = env::var("TINYFS_HOST").unwrap_or_else(|_| DEFAULT_HOST.to_string());
        let port_raw = env::var("TINYFS_PORT").unwrap_or_else(|_| DEFAULT_PORT.to_string());
        let data_dir = env::var("TINYFS_DATA_DIR").unwrap_or_else(|_| DEFAULT_DATA_DIR.to_string());

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

        Ok(Self::new(host, port, data_dir))
    }

    pub fn bind_addr(&self) -> SocketAddr {
        SocketAddr::from((self.host, self.port))
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
