# Tiny-FS 多节点分布式架构设计

## 1. 设计目标

将单节点 tiny-fs 扩展为支持多节点分布式对象存储系统，具备：
- **水平扩展**：通过增加节点提升存储容量和吞吐量
- **数据高可用**：多副本复制确保数据不丢失
- **故障自动恢复**：节点故障时自动切换到健康节点

## 2. 架构概览

```
┌─────────────────────────────────────────────────────────────┐
│                        Client                               │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                   Gateway Node (Any Node)                   │
│  - 一致性哈希路由                                            │
│  - 请求转发                                                 │
│  - 集群拓扑管理                                             │
└─────────────────────────┬───────────────────────────────────┘
                          │
         ┌────────────────┼────────────────┐
         ▼                ▼                ▼
    ┌─────────┐     ┌─────────┐     ┌─────────┐
    │ Node A  │     │ Node B  │     │ Node C  │
    │ primary │◄───►│ primary │◄───►│ primary │
    │ replica │     │ replica │     │ replica │
    └─────────┘     └─────────┘     └─────────┘
```

## 3. 核心组件

### 3.1 节点发现与协调 (src/cluster/)

- **NodeRegistry**: 节点注册表，管理集群成员
- **ClusterManager**: 集群协调器，处理节点加入/离开
- **Heartbeat**: 节点心跳检测
- **TopologyManager**: 集群拓扑管理

### 3.2 数据分片 (src/sharding/)

- **ConsistentHashRing**: 一致性哈希环
- **ShardMapper**: 分片映射器
- **ReplicationPlanner**: 复制策略规划

### 3.3 数据复制 (src/replication/)

- **Replicator**: 复制协调器
- **SyncReplicator**: 同步复制
- **AsyncReplicator**: 异步复制
- **ConflictResolver**: 冲突解决

### 3.4 分布式元数据 (src/meta/distributed/)

- **MetadataCoordinator**: 元数据协调
- **BucketLocator**: Bucket 路由
- **TransactionManager**: 分布式事务（简化版）

## 4. 数据分布策略

### 4.1 一致性哈希

```
                    ┌─────────────┐
                    │   Node A    │
                    │  (0-10000) │
           ┌──────►└──────┬──────┘
           │              │
    40000 ─┤              ├─ 20000
           │              │
           ▼              │
    ┌─────────────┐      ┌┴────────────┐
    │   Node D    │◄────►│   Node B    │
    │ (30000-45K) │      │ (10001-30K) │
    └─────────────┘      └─────────────┘
```

### 4.2 复制策略

- **Replication Factor**: 可配置（默认 3）
- **Write Quorum**: 写操作需要成功的副本数
- **Read Quorum**: 读操作需要成功的副本数

| 配置 | 描述 |
|------|------|
| rf=3, w=2, r=2 | 平衡模式（默认） |
| rf=3, w=3, r=1 | 读优先 |
| rf=3, w=1, r=3 | 写优先 |

## 5. 节点类型

### 5.1 节点角色

- **Primary**: 主节点，处理读写请求
- **Replica**: 副本节点，接受复制数据
- **Gateway**: 网关节点，接受客户端请求并路由

### 5.2 节点标识

```rust
struct NodeId {
    pub host: String,
    pub port: u16,
    pub node_id: String,  // 唯一标识
}
```

## 6. 请求流程

### 6.1 写入请求

```
Client -> Gateway Node
  |
  v
 Consistent Hash Ring -> 确定 Primary Node
  |
  v
 并发复制到 N 个 Replica Nodes
  |
  v
 等待 Write Quorum 确认
  |
  v
 返回成功给 Client
```

### 6.2 读取请求

```
Client -> Gateway Node
  |
  v
 Consistent Hash Ring -> 确定 Primary + Replicas
  |
  v
 并发从 N 个节点读取
  |
  v
 等待 Read Quorum 确认
  |
  v
 验证 Checksum
  |
  v
 返回数据给 Client
```

## 7. 故障恢复

### 7.1 节点故障检测

- 每秒发送心跳
- 3 次心跳超时判定节点死亡
- 触发数据重平衡

### 7.2 数据恢复

1. 检测到节点故障
2. 从剩余副本恢复数据到新节点
3. 更新哈希环和路由表

### 7.3 脑裂防护

- 使用租约机制
- 奇数节点确保选举成功

## 8. API 扩展

### 8.1 集群管理 API

```
GET    /cluster/nodes          - 列出所有节点
GET    /cluster/topology       - 获取集群拓扑
GET    /cluster/health         - 集群健康状态
POST   /cluster/join          - 加入集群
POST   /cluster/leave         - 离开集群
```

### 8.2 节点管理 API

```
GET    /node/id                - 获取本节点ID
GET    /node/stats             - 获取节点统计
GET    /node/shards            - 获取节点分片信息
```

## 9. 配置扩展

### 9.1 环境变量

```bash
TINYFS_CLUSTER_MODE=true           # 启用集群模式
TINYFS_NODE_ID=node-001            # 节点唯一ID
TINYFS_SEED_NODES=host1:20001,...  # 种子节点列表
TINYFS_REPLICATION_FACTOR=3        # 复制因子
TINYFS_WRITE_QUORUM=2              # 写 quorum
TINYFS_READ_QUORUM=2               # 读 quorum
```

### 9.2 集群配置

```json
{
  "cluster": {
    "mode": "distributed",
    "node_id": "node-001",
    "seed_nodes": ["192.168.1.1:20001", "192.168.1.2:20001"],
    "replication": {
      "factor": 3,
      "write_quorum": 2,
      "read_quorum": 2
    }
  }
}
```

## 10. 技术选型

### 10.1 通信层

- **gRPC**: 节点间通信（推荐）
- **HTTP**: 兼容现有 API

### 10.2 一致性

- **简化版 Paxos**: 关键元数据
- **最终一致性**: 对象数据

### 10.3 依赖新增

```toml
# 新增依赖
tokio-grpc = "0.1"           # gRPC 支持
raft = "0.9"                 # Raft 共识
consistent-hash = "0.4"     # 一致性哈希
serde = { version = "1", features = ["derive"] }
uuid = { version = "1", features = ["v4"] }
parking_lot = "0.12"         # 锁
```

## 11. 实现计划

### Phase 1: 基础架构
- [x] 集群配置扩展
- [ ] 节点发现机制
- [ ] 一致性哈希环

### Phase 2: 数据复制
- [ ] 同步/异步复制
- [ ] 复制冲突处理
- [ ] 数据校验

### Phase 3: 容错
- [ ] 心跳检测
- [ ] 故障转移
- [ ] 数据重平衡

### Phase 4: 优化
- [ ] 读写缓存
- [ ] 压缩传输
- [ ] 负载均衡

## 12. 限制与权衡

- **CAP 定理**: 选择 AP（可用性 + 分区容忍），保证最终一致性
- **元数据**: 使用简化协调，保证高性能
- **复杂度**: 避免过度工程，保持实现简洁
