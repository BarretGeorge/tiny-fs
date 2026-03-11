use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BucketPolicy {
    pub version: String,
    pub statements: Vec<PolicyStatement>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PolicyStatement {
    #[serde(rename = "Sid")]
    pub sid: Option<String>,
    #[serde(rename = "Effect")]
    pub effect: PolicyEffect,
    #[serde(rename = "Principal")]
    pub principal: Option<PolicyPrincipal>,
    #[serde(rename = "Action")]
    pub actions: Vec<String>,
    #[serde(rename = "Resource")]
    pub resources: Vec<String>,
    #[serde(rename = "Condition")]
    pub condition: Option<PolicyCondition>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum PolicyEffect {
    #[serde(rename = "Allow")]
    Allow,
    #[serde(rename = "Deny")]
    Deny,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PolicyPrincipal {
    #[serde(rename = "AWS")]
    pub aws: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PolicyCondition {
    #[serde(rename = "StringLike")]
    pub string_like: Option<HashMap<String, Vec<String>>>,
    #[serde(rename = "StringEquals")]
    pub string_equals: Option<HashMap<String, Vec<String>>>,
}

impl Default for BucketPolicy {
    fn default() -> Self {
        Self {
            version: "2012-10-17".to_string(),
            statements: vec![PolicyStatement {
                sid: Some("public-read".to_string()),
                effect: PolicyEffect::Allow,
                principal: Some(PolicyPrincipal {
                    aws: vec!["*".to_string()],
                }),
                actions: vec!["s3:GetObject".to_string()],
                resources: vec!["arn:aws:s3:::${Bucket}/*".to_string()],
                condition: None,
            }],
        }
    }
}

pub struct PolicyManager {
    policies: Arc<RwLock<HashMap<String, BucketPolicy>>>,
}

impl PolicyManager {
    pub fn new() -> Self {
        Self {
            policies: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn set_policy(&self, bucket: &str, policy: BucketPolicy) {
        let mut policies = self.policies.write();
        policies.insert(bucket.to_string(), policy);
    }

    pub fn get_policy(&self, bucket: &str) -> Option<BucketPolicy> {
        let policies = self.policies.read();
        policies.get(bucket).cloned()
    }

    pub fn delete_policy(&self, bucket: &str) {
        let mut policies = self.policies.write();
        policies.remove(bucket);
    }

    pub fn list_buckets_with_policies(&self) -> Vec<String> {
        let policies = self.policies.read();
        policies.keys().cloned().collect()
    }

    pub fn is_action_allowed(&self, bucket: &str, action: &str, _principal: &str) -> bool {
        let policies = self.policies.read();

        let Some(policy) = policies.get(bucket) else {
            return true;
        };

        for statement in &policy.statements {
            if statement.effect != PolicyEffect::Allow {
                continue;
            }

            let resource_matches = statement.resources.iter().any(|r| {
                r == &"*".to_string()
                    || r == &format!("arn:aws:s3:::{}/*", bucket)
                    || r == &format!("arn:aws:s3:::{}/*", bucket)
            });

            let action_matches = statement
                .actions
                .iter()
                .any(|a| a == "*" || a == action || a == &format!("s3:{}", action));

            if resource_matches && action_matches {
                return true;
            }
        }

        true
    }
}

impl Default for PolicyManager {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LifecycleRule {
    pub id: String,
    pub status: LifecycleStatus,
    pub filter: LifecycleFilter,
    pub actions: Vec<LifecycleAction>,
    pub created_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum LifecycleStatus {
    #[serde(rename = "Enabled")]
    Enabled,
    #[serde(rename = "Disabled")]
    Disabled,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LifecycleFilter {
    pub prefix: Option<String>,
    pub tag: Option<HashMap<String, String>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LifecycleAction {
    pub name: String,
    pub expiration_days: Option<i32>,
    pub transition_days: Option<i32>,
    pub transition_storage_class: Option<String>,
}

pub struct LifecycleManager {
    rules: Arc<RwLock<HashMap<String, Vec<LifecycleRule>>>>,
}

impl LifecycleManager {
    pub fn new() -> Self {
        Self {
            rules: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn add_rule(&self, bucket: &str, rule: LifecycleRule) {
        let mut rules = self.rules.write();
        rules.entry(bucket.to_string()).or_default().push(rule);
    }

    pub fn get_rules(&self, bucket: &str) -> Vec<LifecycleRule> {
        let rules = self.rules.read();
        rules.get(bucket).cloned().unwrap_or_default()
    }

    pub fn delete_rule(&self, bucket: &str, rule_id: &str) {
        let mut rules = self.rules.write();
        if let Some(bucket_rules) = rules.get_mut(bucket) {
            bucket_rules.retain(|r| r.id != rule_id);
        }
    }

    pub fn delete_all_rules(&self, bucket: &str) {
        let mut rules = self.rules.write();
        rules.remove(bucket);
    }

    pub fn get_expired_objects(&self, bucket: &str) -> Vec<String> {
        let rules = self.rules.read();
        let bucket_rules = rules.get(bucket);

        match bucket_rules {
            Some(rules) => rules
                .iter()
                .filter(|r| r.status == LifecycleStatus::Enabled)
                .flat_map(|r| {
                    r.actions
                        .iter()
                        .filter(|a| a.expiration_days.is_some())
                        .map(|_| "".to_string())
                })
                .collect(),
            None => vec![],
        }
    }
}

impl Default for LifecycleManager {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BucketQuota {
    pub quota_type: QuotaType,
    pub max_size: Option<u64>,
    pub max_objects: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum QuotaType {
    #[serde(rename = "None")]
    None,
    #[serde(rename = "Bucket")]
    Bucket,
    #[serde(rename = "BucketAndObjects")]
    BucketAndObjects,
}

impl Default for BucketQuota {
    fn default() -> Self {
        Self {
            quota_type: QuotaType::None,
            max_size: None,
            max_objects: None,
        }
    }
}

pub struct QuotaManager {
    quotas: Arc<RwLock<HashMap<String, BucketQuota>>>,
    usage: Arc<RwLock<HashMap<String, QuotaUsage>>>,
}

#[derive(Clone, Debug, Default)]
pub struct QuotaUsage {
    pub total_size: u64,
    pub object_count: u64,
}

impl QuotaManager {
    pub fn new() -> Self {
        Self {
            quotas: Arc::new(RwLock::new(HashMap::new())),
            usage: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn set_quota(&self, bucket: &str, quota: BucketQuota) {
        let mut quotas = self.quotas.write();
        quotas.insert(bucket.to_string(), quota);
    }

    pub fn get_quota(&self, bucket: &str) -> BucketQuota {
        let quotas = self.quotas.read();
        quotas.get(bucket).cloned().unwrap_or_default()
    }

    pub fn update_usage(&self, bucket: &str, size_delta: i64, count_delta: i64) {
        let mut usage = self.usage.write();
        let bucket_usage = usage.entry(bucket.to_string()).or_default();

        if size_delta > 0 {
            bucket_usage.total_size += size_delta as u64;
        } else {
            bucket_usage.total_size = bucket_usage.total_size.saturating_sub((-size_delta) as u64);
        }

        if count_delta > 0 {
            bucket_usage.object_count += count_delta as u64;
        } else {
            bucket_usage.object_count = bucket_usage
                .object_count
                .saturating_sub((-count_delta) as u64);
        }
    }

    pub fn get_usage(&self, bucket: &str) -> QuotaUsage {
        let usage = self.usage.read();
        usage.get(bucket).cloned().unwrap_or_default()
    }

    pub fn check_quota(&self, bucket: &str, size: u64) -> bool {
        let quota = self.get_quota(bucket);
        let usage = self.get_usage(bucket);

        if quota.quota_type == QuotaType::None {
            return true;
        }

        if let Some(max_size) = quota.max_size {
            if usage.total_size + size > max_size {
                return false;
            }
        }

        true
    }
}

impl Default for QuotaManager {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CorsRule {
    pub id: String,
    pub allowed_methods: Vec<String>,
    pub allowed_origins: Vec<String>,
    pub allowed_headers: Vec<String>,
    pub expose_headers: Vec<String>,
    pub max_age_seconds: Option<i32>,
}

pub struct CorsManager {
    rules: Arc<RwLock<HashMap<String, Vec<CorsRule>>>>,
}

impl CorsManager {
    pub fn new() -> Self {
        Self {
            rules: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn set_rules(&self, bucket: &str, cors: Vec<CorsRule>) {
        let mut rules = self.rules.write();
        rules.insert(bucket.to_string(), cors);
    }

    pub fn get_rules(&self, bucket: &str) -> Vec<CorsRule> {
        let rules = self.rules.read();
        rules.get(bucket).cloned().unwrap_or_default()
    }

    pub fn delete_rules(&self, bucket: &str) {
        let mut rules = self.rules.write();
        rules.remove(bucket);
    }

    pub fn check_origin(&self, bucket: &str, origin: &str) -> Option<CorsRule> {
        let rules = self.rules.read();
        let bucket_rules = rules.get(bucket)?;

        for rule in bucket_rules {
            if rule.allowed_origins.iter().any(|o| o == "*" || o == origin) {
                return Some(rule.clone());
            }
        }

        None
    }
}

impl Default for CorsManager {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObjectTags {
    pub tags: HashMap<String, String>,
}

impl ObjectTags {
    pub fn new() -> Self {
        Self {
            tags: HashMap::new(),
        }
    }

    pub fn add(&mut self, key: String, value: String) {
        self.tags.insert(key, value);
    }

    pub fn remove(&mut self, key: &str) {
        self.tags.remove(key);
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.tags.get(key)
    }
}

impl Default for ObjectTags {
    fn default() -> Self {
        Self::new()
    }
}
