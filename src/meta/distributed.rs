use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::AppResult;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DistributedLock {
    pub key: String,
    pub owner: String,
    pub acquired_at: i64,
    pub expires_at: i64,
}

pub struct LockManager {
    locks: Arc<RwLock<HashMap<String, DistributedLock>>>,
    ttl: Duration,
}

impl LockManager {
    pub fn new(ttl: Duration) -> Self {
        Self {
            locks: Arc::new(RwLock::new(HashMap::new())),
            ttl,
        }
    }

    pub fn acquire(&self, key: &str, owner: &str) -> bool {
        let now = Instant::now();
        let now_ts = now.elapsed().as_secs() as i64;
        let expires_ts = (now + self.ttl).elapsed().as_secs() as i64;

        let mut locks = self.locks.write();

        if let Some(lock) = locks.get(key) {
            if lock.owner == owner {
                let mut updated = lock.clone();
                updated.expires_at = expires_ts;
                locks.insert(key.to_string(), updated);
                return true;
            }
            if lock.expires_at < now_ts {
                locks.remove(key);
            } else {
                return false;
            }
        }

        locks.insert(
            key.to_string(),
            DistributedLock {
                key: key.to_string(),
                owner: owner.to_string(),
                acquired_at: now_ts,
                expires_at: expires_ts,
            },
        );

        true
    }

    pub fn release(&self, key: &str, owner: &str) -> bool {
        let mut locks = self.locks.write();
        if let Some(lock) = locks.get(key) {
            if lock.owner == owner {
                locks.remove(key);
                return true;
            }
        }
        false
    }

    pub fn is_locked(&self, key: &str) -> bool {
        let locks = self.locks.read();
        if let Some(lock) = locks.get(key) {
            let now = Instant::now();
            let now_ts = now.elapsed().as_secs() as i64;
            lock.expires_at >= now_ts
        } else {
            false
        }
    }

    pub fn get_lock_info(&self, key: &str) -> Option<DistributedLock> {
        let locks = self.locks.read();
        locks.get(key).cloned()
    }

    pub fn cleanup_expired(&self) -> usize {
        let now = Instant::now();
        let now_ts = now.elapsed().as_secs() as i64;

        let mut locks = self.locks.write();
        let initial_count = locks.len();

        locks.retain(|_, lock| lock.expires_at >= now_ts);

        initial_count - locks.len()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BucketRouting {
    pub bucket: String,
    pub primary_node: String,
    pub replica_nodes: Vec<String>,
    pub version: u64,
}

pub struct BucketRouter {
    routes: Arc<RwLock<HashMap<String, BucketRouting>>>,
    local_node_id: String,
}

impl BucketRouter {
    pub fn new(local_node_id: String) -> Self {
        Self {
            routes: Arc::new(RwLock::new(HashMap::new())),
            local_node_id,
        }
    }

    pub fn register_bucket(&self, bucket: &str, primary: &str, replicas: Vec<String>) {
        let mut routes = self.routes.write();
        routes.insert(
            bucket.to_string(),
            BucketRouting {
                bucket: bucket.to_string(),
                primary_node: primary.to_string(),
                replica_nodes: replicas,
                version: 1,
            },
        );
    }

    pub fn unregister_bucket(&self, bucket: &str) {
        let mut routes = self.routes.write();
        routes.remove(bucket);
    }

    pub fn get_bucket_node(&self, bucket: &str) -> Option<BucketRouting> {
        let routes = self.routes.read();
        routes.get(bucket).cloned()
    }

    pub fn is_bucket_owner(&self, bucket: &str) -> bool {
        let routes = self.routes.read();
        if let Some(routing) = routes.get(bucket) {
            routing.primary_node == self.local_node_id
                || routing.replica_nodes.contains(&self.local_node_id)
        } else {
            false
        }
    }

    pub fn is_primary(&self, bucket: &str) -> bool {
        let routes = self.routes.read();
        if let Some(routing) = routes.get(bucket) {
            routing.primary_node == self.local_node_id
        } else {
            false
        }
    }

    pub fn list_buckets(&self) -> Vec<String> {
        let routes = self.routes.read();
        routes.keys().cloned().collect()
    }
}

use crate::AppError;

pub struct TransactionCoordinator {
    lock_manager: LockManager,
    bucket_router: BucketRouter,
}

impl TransactionCoordinator {
    pub fn new(local_node_id: String) -> Self {
        Self {
            lock_manager: LockManager::new(Duration::from_secs(30)),
            bucket_router: BucketRouter::new(local_node_id),
        }
    }

    pub fn begin_transaction(&self, bucket: &str, key: &str) -> AppResult<TransactionGuard<'_>> {
        let lock_key = format!("{}:{}", bucket, key);

        if self.lock_manager.is_locked(&lock_key) {
            return Err(AppError::InvalidRequest(
                "Object is locked by another transaction".to_string(),
            ));
        }

        let node_id = self
            .bucket_router
            .get_bucket_node(bucket)
            .map(|r| r.primary_node)
            .unwrap_or_default();

        Ok(TransactionGuard {
            lock_key,
            node_id,
            committed: false,
            coordinator: self,
        })
    }

    pub fn lock_manager(&self) -> &LockManager {
        &self.lock_manager
    }

    pub fn bucket_router(&self) -> &BucketRouter {
        &self.bucket_router
    }
}

pub struct TransactionGuard<'a> {
    lock_key: String,
    node_id: String,
    committed: bool,
    coordinator: &'a TransactionCoordinator,
}

impl<'a> TransactionGuard<'a> {
    pub fn commit(&mut self) {
        self.committed = true;
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }
}

impl<'a> Drop for TransactionGuard<'a> {
    fn drop(&mut self) {
        if !self.committed {
            let _ = self
                .coordinator
                .lock_manager
                .release(&self.lock_key, "transaction");
        }
    }
}

pub struct SequenceGenerator {
    current: Arc<RwLock<u64>>,
    node_id: u16,
}

impl SequenceGenerator {
    pub fn new(node_id: u16) -> Self {
        Self {
            current: Arc::new(RwLock::new(0)),
            node_id,
        }
    }

    pub fn next(&self) -> u64 {
        let mut current = self.current.write();
        *current += 1;
        (self.node_id as u64) << 48 | *current
    }
}
