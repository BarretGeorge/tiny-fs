use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum VersioningStatus {
    #[serde(rename = "Enabled")]
    Enabled,
    #[serde(rename = "Suspended")]
    Suspended,
    #[serde(rename = "Disabled")]
    Disabled,
}

impl Default for VersioningStatus {
    fn default() -> Self {
        VersioningStatus::Disabled
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct VersioningConfig {
    pub status: VersioningStatus,
    pub mfa_delete: Option<String>,
}

pub struct VersionManager {
    configs: Arc<RwLock<HashMap<String, VersioningConfig>>>,
    versions: Arc<RwLock<HashMap<String, Vec<ObjectVersion>>>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObjectVersion {
    pub version_id: String,
    pub bucket: String,
    pub key: String,
    pub size: u64,
    pub etag: String,
    pub is_latest: bool,
    pub created_at: i64,
    pub deleted_at: Option<i64>,
}

impl VersionManager {
    pub fn new() -> Self {
        Self {
            configs: Arc::new(RwLock::new(HashMap::new())),
            versions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn set_versioning(&self, bucket: &str, config: VersioningConfig) {
        let mut configs = self.configs.write();
        configs.insert(bucket.to_string(), config);
    }

    pub fn get_versioning(&self, bucket: &str) -> VersioningConfig {
        let configs = self.configs.read();
        configs.get(bucket).cloned().unwrap_or_default()
    }

    pub fn is_versioning_enabled(&self, bucket: &str) -> bool {
        self.get_versioning(bucket).status == VersioningStatus::Enabled
    }

    fn version_key(bucket: &str, key: &str) -> String {
        format!("{}:{}", bucket, key)
    }

    pub fn add_version(&self, bucket: &str, key: &str, version_id: &str, size: u64, etag: &str) {
        let mut versions = self.versions.write();
        let vk = Self::version_key(bucket, key);

        let bucket_versions = versions.entry(vk).or_default();

        for v in bucket_versions.iter_mut() {
            v.is_latest = false;
        }

        bucket_versions.insert(
            0,
            ObjectVersion {
                version_id: version_id.to_string(),
                bucket: bucket.to_string(),
                key: key.to_string(),
                size,
                etag: etag.to_string(),
                is_latest: true,
                created_at: current_timestamp(),
                deleted_at: None,
            },
        );
    }

    pub fn delete_version(&self, bucket: &str, key: &str, version_id: &str) {
        let mut versions = self.versions.write();
        let vk = Self::version_key(bucket, key);

        if let Some(bucket_versions) = versions.get_mut(&vk) {
            if let Some(pos) = bucket_versions
                .iter()
                .position(|v| v.version_id == version_id)
            {
                let mut v = bucket_versions.remove(pos);
                v.deleted_at = Some(current_timestamp());
                bucket_versions.push(v);

                if let Some(latest) = bucket_versions
                    .iter_mut()
                    .find(|v| v.is_latest && v.deleted_at.is_none())
                {
                    latest.is_latest = true;
                }
            }
        }
    }

    pub fn get_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Option<ObjectVersion> {
        let versions = self.versions.read();
        let vk = Self::version_key(bucket, key);

        let bucket_versions = versions.get(&vk)?;

        if let Some(vid) = version_id {
            bucket_versions
                .iter()
                .find(|v| v.version_id == vid && v.deleted_at.is_none())
                .cloned()
        } else {
            bucket_versions
                .iter()
                .find(|v| v.is_latest && v.deleted_at.is_none())
                .cloned()
        }
    }

    pub fn list_versions(&self, bucket: &str, key_prefix: &str) -> Vec<ObjectVersion> {
        let versions = self.versions.read();
        let vk_prefix = Self::version_key(bucket, key_prefix);

        versions
            .iter()
            .filter(|(k, _)| k.starts_with(&vk_prefix))
            .flat_map(|(_, vs)| vs.iter().filter(|v| v.deleted_at.is_none()).cloned())
            .collect()
    }

    pub fn get_delete_marker(&self, bucket: &str, key: &str) -> Option<ObjectVersion> {
        let versions = self.versions.read();
        let vk = Self::version_key(bucket, key);

        let bucket_versions = versions.get(&vk)?;

        bucket_versions
            .iter()
            .find(|v| v.is_latest && v.deleted_at.is_some())
            .cloned()
    }
}

impl Default for VersionManager {
    fn default() -> Self {
        Self::new()
    }
}

fn current_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_versioning() {
        let vm = VersionManager::new();

        vm.set_versioning(
            "test-bucket",
            VersioningConfig {
                status: VersioningStatus::Enabled,
                mfa_delete: None,
            },
        );

        assert!(vm.is_versioning_enabled("test-bucket"));

        vm.add_version("test-bucket", "test-key", "v1", 1000, "etag1");
        let version = vm.get_version("test-bucket", "test-key", None);
        assert!(version.is_some());
        assert_eq!(version.unwrap().version_id, "v1");
    }
}
