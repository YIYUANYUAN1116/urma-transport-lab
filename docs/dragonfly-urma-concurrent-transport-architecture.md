# Dragonfly 适配 URMA：并发 Piece 与 8 节点超节点通信架构

> 日期：2026-08-24  
> 范围：Dragonfly dfdaemon 之间的 Piece 数据传输；URMA RC、SEND/RECV、UDMA provider  
> 目标：在保证 WR/buffer 生命周期、数据完整性和安全 shutdown 的前提下，让一个节点同时作为多个 Piece 的 Parent 与 Child，并扩展到 8 节点超节点。  
> 非目标：本阶段不引入 URMA READ/WRITE、UBS Memory、跨任务零拷贝或改造 Dragonfly Scheduler。

## 1. 证据边界

本文把结论分为三类：

- **源码确认**：来自当前 Dragonfly 候选分支 `rdma-p2p-pr1945`，提交 `1ccc7d10`，或当前 URMA lab 源码。
- **实验确认**：来自 `udmac0d1e2`、RTP + RC、跨节点 `eid_idx=1` 的实测。
- **架构建议**：为 URMA 集成提出，尚需 provider 并发压力测试验证。

特别注意：Dragonfly `main` 当前没有完整 RDMA 实现。本文所说的“现有 Dragonfly RDMA”均指上述候选分支，不能当作已经进入主线或已经过生产验证的实现。

## 2. 结论

URMA 不应按 Piece 创建 Jetty，也不应照搬 libfabric RDM endpoint 的无连接外观。推荐模型是：

```text
每个 dfdaemon
  -> 一个节点级 UrmaTransportManager
  -> 每个 NUMA/NIC 一个 RuntimeShard
  -> 每个远端节点 1 条持久 RC duplex lane（必要时扩到 2 条）
  -> 每条 lane 多路复用多个 PieceSession
  -> 节点级共享 registered-memory budget、shared JFR RX ring 和 completion service
```

其中：

- **Jetty/lane 是节点对之间的长期资源**；
- **PieceSession 是短生命周期的逻辑传输**；
- **WR 是 lane 内短生命周期操作**；
- **registered buffer 是节点级池中的租约**，不能归属于 TCP 连接或 Rust future；
- **RX credit 表示真正已 post 且可被 NIC 写入的容量**，buffer 被 CRC/pwrite 使用期间不能归还 credit；
- **一次 lane 故障使该 lane 上的 Piece 失败并交回 Dragonfly 重试/fallback，不在 transport 内静默重放部分 Piece**。

这与候选 Dragonfly RDMA 的核心思想一致：共享底层 transport、Piece 级会话、bounded window、registered buffer pool、集中 completion progress；区别在于 libfabric `FI_EP_RDM + tag` 要映射成 URMA `RC connection pool + session header`。

## 3. 为什么不能按 Piece 建连接

一个 50 GiB 文件在 Dragonfly 中通常会按最大 64 MiB Piece 划分，约有 800 个 Piece。默认下载并发 Piece 数是 8，Parent server 候选配置允许更多并发 transfer。

如果每个 Piece 都创建和销毁一组 URMA Context/JFC/JFR/Jetty，会产生：

- descriptor exchange、import/bind 和 READY 协议开销；
- provider 对象和 CQ/JFR 深度快速膨胀；
- MR 重复注册、解除注册和页锁定抖动；
- shutdown 与错误恢复时存在大量仍可能 DMA 的对象；
- 8 节点全互联时形成 Piece 数量乘以 peer 数量的资源规模。

正确的资源层次应当是：

```text
Node
`-- UrmaTransportManager
    |-- RuntimeShard (device/EID/NUMA 0)
    |   |-- CompletionService
    |   |-- SharedJfrRxArena
    |   |-- RegisteredMemoryBudget
    |   `-- PeerConnectionPool
    |       |-- Peer B -> Lane 0 -> PieceSession 1, 7, 12 ...
    |       |-- Peer C -> Lane 0 -> PieceSession 2, 9 ...
    |       `-- Peer H -> Lane 0 -> PieceSession ...
    `-- RuntimeShard (可选的第二个 NIC/NUMA)
```

## 4. 8 节点超节点的连接模型

### 4.1 初始拓扑

8 个节点全互联时，每个节点只需要维护到其他 7 个节点的连接状态：

```text
每节点：7 peers × 1 duplex lane = 7 条 lane
全超节点：C(8, 2) = 28 个节点对
```

一条 duplex RC lane 同时承载本节点作为 Parent 的发送和作为 Child 的接收，不要为 upload/download 分别建立连接。

初始实现建议固定每 peer 一条 lane。只有满足以下证据时才扩到两条：

- 单 lane 的 JFS/JFR/CQ 或锁竞争已成为瓶颈；
- provider 明确支持更多 Jetty 且队列资源充足；
- 两 lane 能稳定提升多 Piece 聚合吞吐，而不是只提高 CPU 消耗；
- lane 选择保持稳定，避免 Piece 在多个 lane 间乱序。

不建议把 lane 数直接设置成 Piece 并发数。8 节点、每 peer 8 lane 会让每节点持有 56 条 lane，收益尚未证明，故障和 shutdown 成本却显著增加。

### 4.2 建连与消除双向竞态

两个节点可能同时需要从对方下载 Piece。连接管理必须用确定性规则合并并发建连：

1. 以稳定的 `host_id` 排序，较小的一方作为该 lane 的主动创建方；
2. discovery 返回 device、EID、transport mode、协议版本和 `connection_generation`；
3. 同一 `(local_host, remote_host, shard, lane_index)` 只允许一个 `Connecting` future；
4. 其他 Piece 等待该 future，不能各自创建 Jetty；
5. 建连失败进入带抖动的退避，并让当前 Piece 回退 TCP；
6. 重连递增 generation，旧 generation 的控制帧和迟到数据一律丢弃。

建议状态机：

```text
Absent -> Discovering -> Connecting -> Ready -> Draining -> Closed
                         |          |
                         +-> Failed +-> Failed -> Backoff -> Connecting
```

### 4.3 控制面

近期集成可以保留候选实现的“每 Piece 一条 TCP rendezvous”，因为这最少改变 Dragonfly Piece lifecycle；但 bulk data 应复用已有 URMA lane。

稳定后再把控制面收敛为每 peer 一条持久 framed TCP connection，在其中多路复用：

- `OpenPiece(session_id, task_id, piece_number, metadata)`；
- `AcceptPiece(session_id, length, digest, window)`；
- `CreditGrant(session_id/lane, count)`；
- `PieceDone(session_id)`；
- `AbortPiece(session_id, reason)`；
- keepalive、generation 和 graceful drain。

控制连接断开不等于可以立即释放 DMA buffer。资源释放必须由 URMA completion 或经过验证的 Jetty 销毁屏障决定。

## 5. PieceSession 与 lane 多路复用

### 5.1 数据头

libfabric 候选实现用唯一 tag range 把 completion 路由到 Piece。shared JFR 上的 URMA RECV 不能依赖“这个 RX slot 预先属于某个 Piece”。每个数据消息应携带小型、固定端序的数据头：

```text
protocol_version
connection_generation
session_id
piece_offset
sequence
payload_length
flags (DATA/LAST)
```

Piece digest 和总长度来自可信的 Dragonfly metadata/control plane。Child 仍对实际落入 registered RX window 的数据计算 CRC，并按 piece offset `pwrite`。

`session_id` 必须在一个 connection generation 内唯一；`sequence` 只在 session 内递增。即使 RC 保证发送顺序，也要允许 CQ 批量返回和软件处理造成完成顺序变化，Child 按 sequence/offset 汇合。

### 5.2 公平调度

一个大 Piece 不能把 lane 的全部 WR 和远端 credit 长期占满。每条 lane 维护 active-session queue，用按字节的 deficit round-robin 或等价的 bounded round-robin：

- 一次最多为某个 session post 一个逻辑 window；
- session window 初始使用实测稳定值，例如 `64 × 64 KiB = 4 MiB`；
- lane 仍可连续 post 多个 session 的 linked WR batch；
- control/credit 消息保留独立的小额资源，不能被 DATA 耗尽；
- 对单 Piece 吞吐和多 Piece 公平性分别设置指标。

### 5.3 与 Dragonfly Piece 并发的关系

Dragonfly 的 `concurrent_piece_count` 决定上层同时存在多少 PieceSession；URMA transport 不再创建第二套 Piece scheduler。transport 只负责：

- admission；
- 把 session 映射到 peer lane；
- 在 lane 内公平调度 WR；
- buffer/credit/completion 生命周期；
- 报错，让 Dragonfly 继续执行既有 parent retry 和 TCP fallback。

## 6. Buffer 与注册内存设计

### 6.1 节点级预算，不按连接静态切分

候选 Dragonfly RDMA 的 `Fabric` 使用全局 `max_registered_bytes` semaphore 和 best-fit `PinnedBuf` pool。URMA 应保留这一语义，但底层可采用较大的预注册 Segment 加 size-class/window allocator，减少反复注册。

建议把预算分成：

```text
registered_budget
  = RX posted arena
  + TX leased windows
  + control reserve
  + provider/shutdown quarantine headroom
```

不能把“机器有 1.2 TiB free memory”等同于可以注册 1.2 TiB。实际限制还包括 memlock、provider MR/Segment 数量、IOMMU 映射、NIC cache 和 NUMA locality。

### 6.2 RX arena

UDMA 已实验确认要求 shared JFR，因此推荐每个 RuntimeShard 一个共享 RX arena：

- 预注册若干大 Segment；
- 切成固定 slot，首版继续使用已验证的 64 KiB；
- slot 状态严格为：

```text
Free
 -> PostedRecv
 -> RecvCompleted
 -> AppLease(CRC/pwrite)
 -> RepostPending
 -> PostedRecv
```

- completion poller只产出 `RxLease`，不执行 CRC 或磁盘写入；
- CRC 与 pwrite 可以并行读取同一 lease；
- 两者都完成后才能 recycle；
- **只有 repost 成功后才能向远端返回 credit**。

shared JFR 意味着任意已绑定 Jetty 都可能消耗下一个 receive WR。硬件 RX slot 不应静态归属 peer；公平性通过软件 credit ledger 实现。

### 6.3 RX credit

不要为 7 个 peer 各自静态声明 512 credit，否则会把逻辑承诺放大成 3584 个 slot，且无法反映 app 正持有的 lease。

推荐两级 credit：

1. **全局真实容量**：当前已经 post 到 shared JFR 的 slot 数；
2. **peer/lane 授权额度**：从全局容量中发给各 lane 的未消费 grant。

必须满足：

```text
所有 peer 未消费 credit
+ 已到达但尚未完成的消息
<= shared JFR 中可接收的真实 WR
```

每个 session 另有 `max_inflight_chunks`，用于公平性而非代表独占 buffer。初始 grant 小批量发放，活跃 peer 可以借用公共余量；空闲 peer 不长期占有大块 credit。

连接故障时，未消费 grant 只能在该 generation 被废弃后回收。不能把可能仍在途的数据对应的 slot 立即承诺给新 generation。

### 6.4 TX pool 与 reclaim

TX window 从节点级池动态租用，建议保留 4 MiB 为已验证的基础 size class，并根据 Dragonfly Piece/设备能力增加更大 class。一个活跃发送 session 通常只需要 1–2 个 window：

- window A 由 NIC 发送；
- window B 从 mmap/RangeReader 填充；
- A 的安全 completion 到达后才允许复用。

CQ moderation 后不能按“每个 Piece 收到自己的 completion”回收。每条 JFS/lane 维护单调 `post_seq`：

```text
TxLease -> Prepared -> Posted(post_seq) -> Retired -> Free
```

每隔 N 个 WR以及每个 batch 尾部设置 signaled completion。某个 signaled WR 完成时，只能按 provider 已验证的有序完成语义回收同一 JFS 上 `post_seq <= completed_seq` 的 buffer。不同 lane/JFS 之间绝不能互相推导完成。

为避免 session 取消后提前释放，pending table 中的每个 WR/checkpoint 必须持有 `Arc<TxLease>` 或等价所有权，直到 completion service retire。

### 6.5 一个可用于首轮压测的预算例子

以下只是架构建议，不是 provider 已验证默认值：

```text
chunk size                 64 KiB
per-session logical window 64 chunks = 4 MiB
global RX slots             4096 = 256 MiB
TX active windows budget    256 MiB
control + quarantine reserve 约 128 MiB
node registered budget      从 640 MiB～1 GiB 起测
```

在 8 节点场景中，这些内存由所有 7 个 peer 和所有 PieceSession 共享，而不是乘以 peer 数。应扫描 16/32/64 个 active sessions，观察 MR budget wait、RX lease duration、credit wait 和磁盘队列，而不是仅看单流峰值。

## 7. Queue、completion 与线程模型

### 7.1 推荐所有权

每个 RuntimeShard：

- 一个 provider context/device/EID；
- 一个或少量 shared JFR；
- 每条 peer lane 一个 duplex RC Jetty/JFS；
- send/recv JFC 可共享或按 shard 分组，最终由 provider 压测决定；
- 一个专用 completion progress thread，必要时按 JFC 分成 2 个；
- 独立的 storage/CRC worker pool。

completion poller 的职责只能包括：

- 批量 poll CQE；
- 检查 status/opcode/generation；
- 根据 `user_ctx` 路由到预分配 OpTable；
- retire TX checkpoint；
- 把 RX lease 投递给 session；
- 唤醒等待者。

它不能执行 mmap fault、文件读取、CRC、`pwrite`、连接 discovery 或阻塞日志输出。

### 7.2 `user_ctx` 与 OpTable

避免每个 WR 分配一个 future/oneshot/hash-map 节点。推荐固定容量、带 generation 的 token：

```text
user_ctx = shard_id | op_table_index | op_generation
```

OpTable entry 持有：

- op kind；
- lane generation；
- session id；
- TX/RX buffer lease；
- post sequence；
- 完成回调或轻量 waiter。

index 被复用时必须递增 generation，防止迟到 CQE 命中新操作。

### 7.3 CQ moderation

- SEND：继续采用 sparse signaled completion，并强制 batch tail、idle tail、drain tail 为 signaled；
- RECV：每个接收通常仍产生 CQE，但必须批量 poll 和批量 dispatch；
- moderation interval 不应大于有效 JFS outstanding window，否则可能没有可用于 reclaim 的 completion；
- shutdown 前必须插入最终 checkpoint，不能等待一个永远不会出现的周期 completion。

## 8. Parent 与 Child 数据路径

### 8.1 Parent

```text
Dragonfly Piece metadata/digest
 -> admission + select peer lane
 -> PieceSource(mmap range 或 RangeReader)
 -> lease TX window A/B
 -> fill registered window
 -> lane scheduler post linked SEND batch
 -> sparse SEND completion retire window
```

单个 Piece 内保持双 window；多个 Piece 由上层并发自然提供更高的文件读取 queue depth。不要把整个 50 GiB task 当成一个 transport session，也不要在 transport 内重新实现 Dragonfly Piece 切分。

候选 RDMA 的 mmap 路径仍然会 `copy_from_slice` 到 registered send ring；URMA 首版无需追求“文件 mmap 直接注册给 NIC”。后者会造成大量动态 MR、page pin 和失效处理，且尚无真实收益证据。

### 8.2 Child

```text
shared JFR posted RX slots
 -> CQE + parse session header
 -> session reorder/offset validation
 -> RxLease
 -> CRC worker + positional pwrite
 -> Piece digest/length verification
 -> release lease
 -> repost RX
 -> return credit
```

这与候选 Dragonfly regular Piece 的 registered-window fast path相对应。持久化 Piece namespace 如果仍走通用 `AsyncRead` adapter，可能重新引入 staging copy，应在集成时单独标注，不能把普通 Piece 的 no-bounce 结论推广到全部路径。

## 9. 故障、fallback 与 shutdown

### 9.1 故障边界

- Piece metadata、长度或 CRC 错误：只失败当前 Piece；
- session timeout：abort session，但其在途 WR 的 buffer 继续由 OpTable 持有；
- Jetty/CQE transport error：lane 进入 Failed，该 lane 上全部 session 失败；
- device/context error：RuntimeShard 失败，影响该 shard 的所有 lanes；
- peer 不兼容或连续失败：沿用 Dragonfly parent penalty 和 TCP fallback。

transport 返回明确的可分类错误，不负责在同一 Piece 中从部分 offset 自动续传。Dragonfly 已有 Piece retry、parent 选择和 TCP fallback，重复实现会造成 digest、metadata 状态和重复写入难以推理。

### 9.2 安全 shutdown 顺序

```text
1. stop accepting new PieceSession
2. mark manager Draining and notify control peer
3. stop posting new DATA WR
4. drain/abort active sessions
5. reap all可完成的 SEND/RECV CQE
6. close/destroy Jetty，确认 provider 的 DMA 停止屏障
7. release或隔离仍无终态的 buffer
8. stop completion thread
9. destroy JFR/JFC/Segment/context
```

如果 provider 不能证明 Jetty destroy 后不会再 DMA，无法确认终态的 registered buffer 必须 quarantine/leak 到进程结束，不能为了回收内存破坏生命周期安全。

## 10. 与候选 Dragonfly RDMA 的逐项映射

| Dragonfly 候选 RDMA | 推荐 URMA 实现 | 说明 |
|---|---|---|
| 进程共享 `Fabric/FI_EP_RDM` | 节点级 `UrmaTransportManager` + RuntimeShard | 都不是每 Piece 初始化 transport |
| 一个 RDM endpoint 面向多个 peer | 每 peer 1 条持久 RC duplex lane | RC 是有连接语义，不能伪装成单 endpoint |
| provider address vector | `PeerConnectionPool` + imported Jetty descriptor | discovery 仍走 TCP |
| tag range/transfer | `connection_generation + session_id + sequence` | shared JFR 收包后软件路由 |
| 每 Piece TCP rendezvous | Phase 1 保留；Phase 2 可持久多路复用 | 先降低集成风险 |
| `max_concurrent_transfers` semaphore | 节点 + peer + lane 三级 admission | 防止一个 peer 占满节点 |
| `max_registered_bytes` semaphore | 节点级 registered budget | RX/TX/control/quarantine 要统一核算 |
| best-fit `PinnedBuf` pool | 预注册 Segment + window/slot allocator | 减少 URMA 注册抖动 |
| 每 transfer 1–2 个 receive window | shared JFR RX arena + session leases | UDMA 必须使用 shared JFR |
| per-op `Arc<PinnedBuf>` pending map | generation-safe OpTable 持有 Tx/Rx lease | completion 前绝不释放 |
| 单 progress thread批量 poll CQ | 每 RuntimeShard completion service | 高并发后可按 JFC shard |
| `fi_cancel` + endpoint retire | session abort + Jetty drain/destroy + quarantine | 必须按 UMDK/provider 真实语义实现 |
| Piece 失败回退 TCP | 保留 Dragonfly fallback | URMA 不改变上层容错策略 |
| mmap/RangeReader 填 send ring | mmap range/RangeReader 填 URMA TX window | 两者都不是端到端零拷贝 |
| registered RX window交给 Storage | `RxLease` 直接 CRC + pwrite | lease 完成后才能 repost/return credit |

## 11. 推荐配置层次

配置不要只有一个全局 `window`。建议分为：

```text
transport.urma.device / eid_index / numa_node
transport.urma.max_registered_bytes
transport.urma.rx_slots_per_shard
transport.urma.max_peer_connections
transport.urma.lanes_per_peer            # 初始 1
transport.urma.max_active_sessions
transport.urma.max_active_sessions_per_peer
transport.urma.chunk_size                # 初始采用实测 64 KiB
transport.urma.session_window_chunks     # 初始采用实测 64
transport.urma.send_completion_interval
transport.urma.send_post_list            # 当前实测 16
transport.urma.poll_batch
transport.urma.connect/operation/drain_timeout
transport.urma.fallback_to_tcp
```

Dragonfly 的 `concurrent_piece_count` 继续由 download 层控制，不与 `max_active_sessions` 合并：前者表达业务并发，后者是 transport 资源上限。

## 12. 可观测性与验收

### 12.1 必备指标

节点级：

- active/connecting/failed lanes；
- active/waiting PieceSession；
- registered bytes：RX、TX、idle pool、quarantine；
- JFR posted/free/app-held slots；
- CQ batch、empty ratio、最大 poll gap、CQE error；
- storage read、CRC、pwrite 与 transport 吞吐；
- fallback 次数和失败类别。

peer/lane 级：

- outstanding SEND、available remote credit；
- credit wait time；
- send post-list、completion checkpoint 数；
- bytes、sessions、timeouts、reconnect generation；
- scheduler queue delay和每 session 公平性。

Piece 级：

- metadata length/digest；
- first-byte、transport、storage drain耗时；
- received/written/verified bytes；
- retry/fallback parent。

### 12.2 8 节点验收矩阵

至少覆盖：

1. 1 对 1，单 Piece；
2. 1 对 1，8/16/32 并发 Piece；
3. 1 Parent 对 7 Child；
4. 7 Parent 对 1 Child；
5. 8 节点同时上传和下载；
6. 内存、tmpfs、真实磁盘三种 sink/source；
7. lane 中途失败、peer 重启、控制连接断开；
8. MR budget 耗尽、RX consumer变慢、TCP fallback；
9. 连续运行和 shutdown/restart；
10. 所有场景验证 length、Piece CRC 和最终文件 digest。

聚合吞吐必须同时报告：NIC、源存储、目标存储、CPU/NUMA 和各 peer 公平性。不能把 page-cache 热数据的 transport 峰值当作 8 节点 file-to-file 能力。

## 13. 分阶段落地路线

### 13.1 当前 URMA lab 到目标架构的差距

| 当前 lab | 集成时处理 | 原因 |
|---|---|---|
| 每次 benchmark 创建 Runtime/connection | Runtime 提升为 dfdaemon 节点级长生命周期服务 | 避免每 Piece 重建硬件资源 |
| 一条连接、一个 request | 每 peer lane 上多 PieceSession | 对齐 Dragonfly 并发 Piece |
| 一组静态 TX/RX Segment | 节点级预算下的 RX arena + TX window pool | 并发数不能线性放大注册内存 |
| RX credit 面向唯一远端 | shared JFR 全局容量 + peer/lane grant ledger | 多 peer 不能重复承诺同一批 WR |
| sequence 只描述单传输 | generation + session_id + sequence | 支持重连和 lane 内多路复用 |
| `PipelineTracker` 面向单流 window | lane scheduler + per-session inflight + lane post watermark | 同时保证公平性和安全 reclaim |
| completion polling 与 benchmark 流程耦合 | 独立 RuntimeShard completion service | 不能让任一 Piece 的文件 I/O 阻塞 CQ progress |
| TX sparse completion 已按顺序 retire | 将 watermark 提升到 lane/JFS 级 | 多 session 共享 JFS 后不能按 Piece 单独推导完成 |
| registered RX lease 直接供 CRC/pwrite | 保留并接入 Dragonfly Storage | 这是当前最有价值、已验证的接收路径 |
| linked SEND/RECV post、CRC combine | 保留为 lane 和 sink 内部实现 | 降低 post/CQE/单核处理开销 |
| OOB 只服务一个 benchmark | 接入 discovery、连接缓存、generation 和 Piece control | 支持长生命周期 peer 关系 |
| 错误直接结束进程/benchmark | 错误归类到 Piece、lane、shard，并触发 Dragonfly fallback | 一个 peer 故障不能拖垮整个 dfdaemon |

当前 lab 已经能够作为 `RuntimeShard`、buffer lease、credit 和 completion watermark 的原型，但不能把 `BenchmarkCase`、单请求状态机和固定 Parent/Child 角色原样带入 Dragonfly。

### Phase A：抽取可集成的 URMA runtime

- 从 demo 中抽出 `UrmaTransportManager`、lane、OpTable、TxLease/RxLease；
- 保留 RC duplex、shared JFR 和已验证的 SEND/RECV 路径；
- runtime 生命周期提升为 dfdaemon 进程级；
- 先完成单 peer、多个顺序 PieceSession。

### Phase B：同 peer 并发 Piece

- lane 内 session header 和 demux；
- 公平 scheduler；
- 节点级 TX pool、shared RX arena、两级 credit；
- completion poller与 CRC/pwrite worker 完全分离；
- 验证一个 lane 上 8/16/32 Piece 并发。

### Phase C：8 节点连接管理

- discovery、确定性建连、连接缓存和 generation；
- 每 peer admission/fairness；
- 7 peer 双向并发和节点级 MR budget；
- 失败 lane 的 Piece fallback，不影响其他 peer。

### Phase D：接入 Dragonfly lifecycle

- `RDMADownloader` 优先 URMA，失败保持现有 TCP fallback；
- `RDMAServer` 的 Storage、Piece metadata、bandwidth limiter保持不变；
- regular Piece 使用 registered RX lease 直达 CRC+pwrite；
- persistent/persistent-cache 路径逐一确认是否保留 lease，避免隐式 bounce；
- 保留候选 RDMA 的 capability cache、parent penalty 和 admission语义。

### Phase E：有证据后再优化

- 一 peer 两 lane；
- 多 JFC/completion shard；
- 更大 chunk/window size class；
- NUMA-aware MR pool；
- persistent control multiplexing。

以下内容不应成为初次适配的前置条件：URMA READ/WRITE、直接注册任意文件 mmap、跨节点共享远端 Segment、UBS Memory。

## 14. 最终建议

对 8 节点超节点，最重要的不是让每个 Piece 拥有更多队列，而是让昂贵资源共享、让短生命周期状态隔离：

```text
Context/JFR/JFC/MR budget：节点或 NUMA shard 级
RC Jetty：peer lane 级
credit与completion watermark：lane 级
window/lease：动态池化
digest、offset、retry：PieceSession 级
```

候选 Dragonfly RDMA 已经证明了“共享 Fabric + Piece transfer + pooled registered buffer + progress thread”的方向。URMA 适配应保留这些上层语义，但用 RC 的真实模型实现 peer connection pool，并把 shared JFR、全局 credit 和安全 buffer reclaim 作为首要设计约束。

这条路线既能复用当前 demo 已验证的高性能数据面，又不会把 benchmark 中的单连接/单请求假设带进 Dragonfly 的多 Piece、多 Parent 和双向并发环境。

## 15. 主要源码依据

- Dragonfly 候选 RDMA 总体分析：`/home/yuan/workspace/docs/engineering-lab/dragonfly/11-dragonfly-rdma-p2p-source-analysis.md`
- 共享 Fabric、pending operation、MR pool 和 progress thread：`client/dragonfly-client-storage/src/rdma/fabric.rs`
- Child receive window pipeline：`client/dragonfly-client-storage/src/client/rdma.rs`
- Parent admission、PieceSource 和双 send window：`client/dragonfly-client-storage/src/server/rdma.rs`
- Piece RDMA 优先、capability cache、parent penalty 和 TCP fallback：`client/dragonfly-client/src/resource/piece_downloader.rs`
- Piece 大小与并发：`client/dragonfly-client/src/resource/piece.rs`、`client/dragonfly-client-config/src/dfdaemon.rs`
- 当前 URMA benchmark pipeline：`src/urma_benchmark/native.rs`
- 当前 buffer/lease 状态机：`src/buffer.rs`
- 当前 completion 统计和路由：`src/completion.rs`
