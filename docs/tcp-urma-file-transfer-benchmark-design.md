# TCP vs URMA 文件传输性能验证设计

> 设计日期：2026-08-14  
> `urma-transport-lab` 源码基线：`0e7d4684ad5a3cceac994b5db771e186a811985d`  
> Dragonfly 根仓库基线：`2acbb8b6414939919cfc8474bf0ba4c38ae2c8ba`  
> Dragonfly `client` 子仓库基线：`017575a58d0abad6b3b274142fa470d47d8db327`  
> 范围：仅设计；本轮不实现 benchmark、不修改 Dragonfly 或 URMA demo 代码。

## 0. 目标、证据规则与核心决策

本阶段要回答：在接近 Dragonfly Parent -> Child Piece 数据路径的单连接、单任务场景下，UB/URMA 相比 TCP 是否有吞吐或 CPU 优势；优势出现在哪一层；没有优势时，瓶颈位于 stop-and-wait、消息粒度、copy、CQ polling、TCP 快路径、page cache 还是文件写入。

本文使用以下标记：

- `[源码确认]`：由当前 `urma-transport-lab`、Dragonfly2 或 UMDK 源码直接确认。
- `[文档确认]`：由当前 milestone/status 或工程设计文档确认；不把文档描述提升为源码事实。
- `[架构分析]`：本 benchmark 的设计选择或基于已知事实的推断，尚未实现。
- `[待验证]`：必须通过 capability 探测、真实 UB provider 或正式实验确认。

证据优先级保持为：真实实验 > 当前源码 > 历史设计文档。若三者冲突，报告必须显式记录差异。

核心决策：

1. `[架构分析]` benchmark 继续放在 `urma-transport-lab`，保持 standalone，不依赖 Dragonfly crate，不改 Scheduler/Manager。
2. `[架构分析]` 比较三条路径：TCP userspace、Dragonfly-like TCP（File 场景 Parent 使用 Linux `sendfile`）和 URMA SEND/RECV copy mode。
3. `[架构分析]` 正式比较前先给 URMA 加最小有界 pipeline；不以当前 stop-and-wait 结果代表 URMA 上限。
4. `[架构分析]` pipeline 同时要求多个 TX slot、多个 outstanding SEND、足够的预投递 RX、CQE 批量回收；不能只增加 TX window。
5. `[架构分析]` 第一阶段不做全部参数的笛卡尔积。先校准 chunk/window，再做 size sweep，最后做少量 file cache/durability 诊断。
6. `[架构分析]` 主结果同时报告吞吐和两端 CPU。吞吐相近但 CPU 显著降低仍是有效收益。
7. `[架构分析]` setup-included 与 steady-state 分开计时；文件 buffered-completion 与 durable-completion 分开计时。
8. `[架构分析]` 不做 zero-copy RX lease、URMA READ/WRITE、UBS Memory、多 Peer、多 Piece/请求并发或生产级异步框架。

## 1. 当前基线

### 1.1 M4 Parent 文件读取与发送

`[源码确认]` `src/bin/parent.rs` 当前流程为：

```text
Runtime/JFC/JFR/Jetty/OOB handshake
-> wait Request
-> 第一次打开输入文件，digest_reader() 以 64 KiB 临时 buffer
   顺序读取整个文件，预计算 SHA-256 和 length
-> 第二次打开输入文件
-> SEND Metadata -> drain_completions()
-> 循环 read <= MAX_DATA_PAYLOAD_LEN
   -> 构造 Data(Vec copy)
   -> encode frame
   -> copy frame 到 registered TX slot
   -> post SEND
   -> drain_completions()
-> SEND End -> drain_completions()
-> close/shutdown
```

`[源码确认]` `send_one()` 在每条 Metadata/Data/End 后都调用 `drain_completions()`，所以每条 Data 的 send CQE 到达并回收 TX slot 前不会发送下一条 Data。

`[文档确认]` `docs/m4-build-status.md` 对上述路径的结论也是“Parent 始终只有一个 Data SEND in-flight，没有 pipeline”。

`[架构分析]` M4 的 SHA-256 预扫描不应进入正式 steady-state 传输计时。Dragonfly Parent 的 Piece digest 已存在于 metadata；benchmark 应在计时前准备好期望 CRC32。冷缓存 File 试验要在预计算后再执行缓存驱逐步骤，否则预扫描会把输入文件重新变热。

### 1.2 M4 Child 接收与落盘

`[源码确认]` `src/bin/child.rs` 当前流程为：

```text
handshake
-> SEND Request
-> wait_for_message()
-> CompletionPoller 收到 recv CQE
-> 从 registered RX slot copy 整个 frame 到 Vec<u8>
-> RX slot RecvCompleted -> Free
-> decode Message
-> Metadata/Data 到达后先 recv_ready() 补一个 RX
-> ReceiveState 校验 request_id / sequence / length
-> Data payload write_all() 到 BufWriter<File>
-> 同步更新 SHA-256
-> End 校验 count/length/digest
-> BufWriter::flush()
```

`[源码确认]` Child 不聚合整个文件，但每条 Data 至少经历 registered RX -> owned `Vec<u8>` 的一次 copy；decoder/Data ownership 还可能涉及 frame/payload 的 owned buffer 重组。输出是普通 buffered file write，结束只 `flush()`，没有 `fsync`/`fdatasync`。

`[源码确认]` Metadata 和每条 Data 后只补回一个 RX，OOB handshake 初始也只预投递一个 RX。因此 M4 实际 receive credit 是 1，和 Parent stop-and-wait 相匹配。

### 1.3 TX/RX slot、CQE 与 registered copy

`[源码确认]` `BufferPoolConfig::default()` 是：

```text
slot_size = 64 KiB
tx_slot_count = 8
rx_slot_count = 8
alignment = 4096
```

`[源码确认]` M4 v2 和 M5.1 v3 都使用 24-byte header；当前 64 KiB slot 的最大 Data payload 是 `65536 - 24 = 65512` B，而不是完整 64 KiB。

`[源码确认]` TX 路径：

```text
Message payload
-> encode 到普通 Vec
-> ffi SegmentHandle::write / memcpy 到 registered TX slot
-> SendPosted
-> send CQE 通过 user_ctx 定位 WR/slot
-> SendCompleted
-> Free
```

`[源码确认]` RX 路径：

```text
Free -> Allocated -> PostedRecv
-> recv CQE status/opcode/completion_len 校验
-> SegmentHandle::read / memcpy 到 owned Vec
-> RecvCompleted -> Free
-> application decode/CRC/file write
```

`[源码确认]` `user_ctx` 由 connection/generation/operation/slot 组成；`CompletionPoller` 使用它从 outstanding map 路由 CQE，并在 send CQE 后释放 TX slot、在完成 RX copy 后释放 RX slot。TX slot 在 CQE 前不可复用；交给应用的 RX bytes 不引用 registered slot。

`[源码确认]` CompletionPoller 当前每轮分别 poll send JFC 和 recv JFC，单次 batch 上限配置为 16；统计已有 `send_post/recv_post/send_cqe/recv_cqe/cqe_error`，但没有 empty-poll、当前/峰值 outstanding 或 poll 调用次数。

`[源码确认]` 当前 FFI shim 创建独立 JFR，再以 `URMA_SHARE_JFR` 放入 RC duplex Jetty；真实 benchmark 必须保留这个 shared-JFR 结构。

`[源码确认]` 默认 Jetty `send_depth=64`、`recv_depth=64`，但实际可用窗口还受设备 capability、JFC depth、TX/RX slot 数和已预投递 receive credit 的共同限制。

### 1.4 可直接复用的协议与不可直接复用部分

可直接复用：

- `[源码确认]` M4 v2 的 24-byte envelope、Request/Data/End/Error、request_id、严格 sequence、payload length 和 End count/length 规则。
- `[源码确认]` M5.1 v3 的独立 `IntegrationMessageV3` codec、CRC32/SHA-256 `DigestDescriptor` 和 Dragonfly-compatible `crc32:<decimal-u32>` helper。
- `[源码确认]` v3 与 v2 decoder 相互拒绝；benchmark 不应静默改变 v2 语义。
- `[文档确认]` M4 的 Runtime、OOB descriptor exchange/import/bind、shared JFR、BufferPool、CompletionPoller、shutdown 顺序是应保留的 foundation。

不能原样用于正式 benchmark：

- `[源码确认]` 现有 `UrmaConnection::send()` 能连续 post 多个 SEND，只要有空闲 TX slot；但 Parent CLI 每次显式 drain，形成 stop-and-wait。
- `[源码确认]` `wait_for_message()` 只有在 pending message 非空且 `outstanding_send()==0` 时才返回。这一同步便利语义不适合作为全双工 pipeline 的热路径。
- `[源码确认]` v3 目前只有 codec/helper，尚未接入 `UrmaConnection` 或真实 provider。
- `[源码确认]` 每条 Data/CQE 当前会 `eprintln!`；热路径逐 chunk 日志会严重污染吞吐和 CPU 结果，正式计时必须关闭，只保留聚合计数和错误日志。
- `[文档确认]` M4 真实 provider 16 MiB 单次及连续 10 次在当前 build-status 中仍标记待目标环境补跑；不能把 feature-on 编译当作 M4 文件路径的硬件性能基线。

## 2. Benchmark 架构

### 2.1 共同控制面与三条数据面

```text
                         +-------------------------+
                         | standalone coordinator  |
                         | case id / barrier / JSON|
                         +------------+------------+
                                      |
                 setup、参数、开始/结束 barrier（不承载正式 payload）
                                      |
       +------------------------------+------------------------------+
       |                              |                              |
TCP userspace                  Dragonfly-like TCP                 URMA
read/memory -> write           file -> sendfile                  read/memory
read -> owned chunk            exact-length recv                -> TX registered copy
-> CRC32 -> sink               -> CRC32 -> sink                 -> SEND/RECV/CQE
                                                               -> RX owned copy
                                                               -> CRC32 -> sink
```

`[架构分析]` 三条路径使用相同 case descriptor：scenario、payload bytes、chunk/payload size、integrity algorithm、file policy、timing mode、repeat、data seed。控制面只做参数/同步/结果交换，不混入正式 payload byte 数。

`[架构分析]` benchmark 可以是一个二进制的不同 `--transport/--scenario/--role` 模式，也可以是薄的 parent/child binaries；实现拆分以最小改动为准，不预设生产模块结构。

### 2.2 共同业务语义

```text
Child Request
-> Parent Metadata(offset=0, total_length, CRC32 descriptor)
-> content bytes
-> logical End / exact-length completion
-> Child length + streaming CRC32 verification
```

`[架构分析]` URMA 使用 v3 Metadata/Data/End 语义。TCP 可共享 Request/Metadata 的编码和 CRC32 helper，但不强求把 TCP byte stream伪装成逐 Data frame：userspace TCP 以配置的 chunk 驱动 read/write syscall；Dragonfly-like TCP 在 metadata 后发送 exact-length raw body，以便 `sendfile`。三者比较的是相同 payload、完整性和完成边界，不是相同 wire header 开销。

`[架构分析]` 所有测试数据及 expected CRC32 在计时前生成/计算。Child 在计时区间内做增量 CRC32；计时结束前必须完成 length/CRC32 校验。

### 2.3 两种计时口径

#### Steady-state（主结果）

`[架构分析]` 双端进程已启动，内存/文件、digest、registered pool、JFC/JFR/Jetty、TCP socket/OOB handshake 均准备完成。双方通过 barrier 就绪：

```text
t0 = Child 开始发送 Request
t1(memory) = Child 收到全部 payload，完成 length + CRC32
t1(file, buffered) = Child 写完 payload、完成 CRC32，并 flush 用户态 buffer
t1(file, durable) = 上述完成后 fdatasync 返回
elapsed = t1 - t0
```

Child 的 `t0..t1` 是吞吐主口径；Parent 同时记录从收到 Request 到最后 payload/End 完成的区间，用于定位两端尾差。多轮 steady-state 应复用已经建立的 transport 资源，但仍保持同一时刻只有一个 request。

#### Setup included（单独报告）

`[架构分析]` 使用 fresh process/fresh connection；从双方开始 transport setup 前的协调时间点，到 Child 按相同 sink completion 规则完成。URMA 包含 device/context、JFC、registered Segment、shared JFR、Jetty、OOB exchange/import/bind；TCP 包含 socket create/connect/accept 和 protocol setup。进程装载若无法可靠同步，单列为 launcher wall time，不混进 steady-state。

`[架构分析]` setup-included 用来回答短 Piece/一次性连接成本，不用于否定或证明持续传输的 transport 上限。

## 3. Dragonfly TCP 参考路径

以下均为 `[源码确认]`，基于本文开头记录的 Dragonfly commit：

1. `Storage::upload_piece()` 先等待 Piece finished，再调用 Linux content 的 `read_piece()`，返回读取 task file `(offset, length)` range 的 `RangeReader`。
2. `RangeReader` 持有共享 `Arc<File>`、显式 offset/remaining 和可复用 buffer；`into_parts()` 能恢复未消费 range 的 `(fd, offset, remaining)`。
3. TCP server 读取 Vortex Request，取得 Piece metadata 与 `RangeReader`，先向 Child 写 response header/metadata。
4. Linux `write_stream()` 随后调用 `sendfile_range()`。它在非阻塞 socket writable 后调用 `rustix::fs::sendfile`，显式传入 file offset，处理 `WouldBlock/Interrupted`；单次 count 上限是 Linux `0x7ffff000`。
5. 非 Linux fallback 才使用 `copy_buf(RangeReader, TCP writer)`。
6. TCP client 设置 `TCP_NODELAY`、nonblocking、16 MiB send/recv socket buffer，并在 Linux 可配置 TCP Fast Open；server 同样设置 16 MiB socket buffer并尝试 cubic。
7. Child client 先解析 response metadata，然后把余下 `OwnedReadHalf` 包装成 `ReaderStream` / `PieceContentStream`。
8. 当前默认 storage read/write buffer function 返回 512 KiB。Child `write_range_from_stream()` 边收边 CRC32，把多个 `Bytes` 累积到 `write_buffer_size`，最多 1024 个 iovec 一批，交给 blocking task 中的 `pwritev`；网络接收/CRC 与至多一个 in-flight file write 可重叠。
9. Storage 用 `crc32fast::Hasher` 得到十进制 u32，并与 expected `crc32:<decimal>` 比较；write/digest/metadata commit 完成后才是 Dragonfly Piece success。

`[架构分析]` 因此“Dragonfly 当前 TCP baseline”不能等同于简单 `read -> write`。Parent `sendfile` 避开了 file -> userspace -> socket 的显式 staging，而本阶段 URMA SEND 必须经过 registered TX staging copy；这正是需要实测的竞争关系。

`[架构分析]` standalone benchmark 只模仿数据路径和关键 socket/file 策略，不复制 Vortex、Storage metadata DB、rate limiter、metrics 或 Dragonfly crate 依赖。

## 4. TCP userspace baseline

### 4.1 Memory-to-Memory

```text
Parent pre-generated memory
-> write_all / partial-write loop, 每次最多 chunk bytes
-> TCP
-> Child read loop into owned userspace buffer
-> streaming CRC32
-> preallocated Child memory / counting sink
```

`[架构分析]` Child 必须实际消费并校验每个 byte，不能只读入内核或丢弃未校验数据。预分配/数据生成不计时；避免在计时区间做随机数生成和大块 allocator 扩容。

### 4.2 File-to-File

```text
Parent file
-> userspace read(chunk)
-> TCP write-all
-> Child TCP read(chunk)
-> streaming CRC32
-> 与 Dragonfly-like sink 相同的 batched pwritev
-> output file
```

`[架构分析]` 该路径是与 URMA copy mode 相对公平的 transport baseline：两者 Parent 都从 file/memory 进入 userspace staging，Child 都有 userspace owned bytes 和相同 sink。TCP 不人为设置小 socket buffer；默认按 Dragonfly 的 16 MiB 请求值配置，并记录内核实际 `SO_SNDBUF/SO_RCVBUF`。

`[架构分析]` chunk 参数定义为应用层每次最多处理的 payload bytes。必须正确处理 partial read/write；不能假设一次 `write` 对应一次对端 `read`。

## 5. Dragonfly-like TCP baseline

### 5.1 File-to-File 快路径

```text
Parent metadata (expected length + CRC32)
-> sendfile(fd, explicit offset, exact remaining) -> TCP
-> Child exact-length receive stream
-> streaming CRC32
-> 与 TCP userspace/URMA 相同的 batched pwritev sink
```

`[架构分析]` Parent 实现策略应参考当前 Dragonfly `sendfile_range()`：非阻塞 readiness、显式 offset、partial progress、`EINTR/EAGAIN` 重试、`0x7ffff000` 上限。不要依赖 Dragonfly crate。

`[架构分析]` 该 baseline 的 Parent 没有可比较的应用 read chunk；矩阵中的 `chunk` 对它表示 Child receive stream chunk / sink item 粒度，不限制 `sendfile` 每次只能发送该大小。另采 `sendfile` syscall 次数与实际 bytes，避免错误地宣称逐 chunk sendfile 是 Dragonfly-like。

`[架构分析]` Memory-to-Memory 没有 file descriptor/page-cache `sendfile` 等价路径，所以该场景只比较 TCP userspace 与 URMA；不制造 memfd baseline 混淆 transport 上限。

### 5.2 与真实 Dragonfly 的边界

`[架构分析]` benchmark 对齐的是 Parent sendfile、metadata-before-body、Child stream CRC32 和 batched positional write。它不包含 Vortex 编解码全部细节、Storage metadata/FD cache、Tokio task 调度、rate limiter、metrics、Piece notifier 或 metadata commit。因此结果只能回答数据路径潜力，不能直接声称等于 dfdaemon 端到端性能。

## 6. URMA baseline 与最小 pipeline

### 6.1 保留 foundation

`[架构分析]` 复用当前 Runtime、RC duplex Jetty、shared JFR、JFC、OOB descriptor exchange/import/bind、BufferPool slot state、CompletionPoller CQE validation/`user_ctx` route 和 drain/shutdown。新逻辑只扩展 benchmark 所需的有界 post/poll 调度与统计，不重构 raw FFI/lifecycle。

```text
Parent source
-> 普通 owned chunk / v3 Data encode
-> copy 到 free registered TX slot
-> post SEND（不立即等待该 WR 的 CQE）
-> outstanding < window 时继续填下一个 slot
-> poll send/recv CQE batch
-> CQE 后按 user_ctx 回收准确 slot

Child
-> transfer 前预投递 receive_window 个 RX WR
-> recv CQE batch
-> registered RX slot copy 到 owned payload
-> 立即回收并补投 RX（受 sink/backpressure 上限约束）
-> sequence/length/CRC32
-> memory/file sink
```

### 6.2 为什么必须先消除 stop-and-wait

`[源码确认]` 当前路径是 `SEND -> drain send CQE -> 下一 SEND`，只有一个 Data SEND in-flight。其吞吐上限会被每条消息的 post/CQE 往返和单线程 poll 调度限制，不能代表 RC SEND 队列能力。

`[源码确认]` 当前 pool 已有 8 个 TX 和 8 个 RX slot，Jetty 默认 depth 是 64；CompletionPoller 已能以独立 `user_ctx` 跟踪多个 outstanding WR。UMDK API 接受 WR 并通过 JFC 批量 poll completion，UMDK perftest 也按 post-list/CQE batch 运行。因此“多个 outstanding SEND + CQE 后逐 slot 回收”与现有 API 模型相符。

`[待验证]` 目标 UDMA provider 对具体 message size/window 的稳定行为、实际最大消息、队列深度和 RNR 行为必须由 capability dump 与真实试验确认；源码/API 支持不等于目标硬件已验证。

### 6.3 最小 pipeline 规则

`[架构分析]` 定义：

```text
send_window <= min(tx_slot_count,
                   local max_jfs_depth / configured send_depth,
                   remote posted RX credit)
receive_window <= min(rx_slot_count,
                      local max_jfr_depth / configured recv_depth)
```

实现时使用探测到的精确上限，而不是把上述伪式直接当代码。首轮候选 window 为 `1, 2, 4, 8`；默认 pool 最多支持 8。若 capability 或真实稳定性只允许更小值，记录原因并缩小。

必要不变式：

- `[架构分析]` 每个 outstanding SEND 独占一个 TX slot，直到对应成功/错误 CQE。
- `[架构分析]` Child 在开始 Data 前预投递不少于 send window 的 RX；否则多 TX 会退化为 RNR/credit 测试。
- `[架构分析]` 只在有 free TX slot、send depth 和远端 receive credit 时继续 post。
- `[架构分析]` RC 顺序仍由 Data sequence/End 校验；CQE 的 slot 回收不能假设固定 slot 顺序。
- `[架构分析]` Metadata 在 Data 前，End 只在全部 Data 已 post 后发送；完成结果必须等全部 send CQE 和 Child End/length/CRC 成功。
- `[架构分析]` poll loop 不加入 Tokio/生产级 executor；单 owner 同步循环即可。
- `[架构分析]` benchmark sink 若暂时跟不上，pending owned payload 必须有界；不得用整文件 Vec 隐藏 RX/写盘背压。
- `[架构分析]` CQE/每 chunk 日志在正式计时关闭。

### 6.4 payload/slot sizing

`[源码确认]` 当前 64 KiB slot 只能携带 65,512 B Data。建议矩阵中的 `64 KiB/256 KiB/512 KiB/1 MiB` 指 Data payload，而不是 slot size。

`[架构分析]` 对每个 payload，slot capacity 至少是 `24 + payload`，并按实现要求做安全对齐；启动前验证：

- local/peer `max_msg_size`；
- registered Segment 总大小与分配成功；
- JFS/JFR/JFC depth；
- 8 TX + 8 RX 在该 slot size 下的内存预算；
- 单 SGE 可承载完整 frame。

`[架构分析]` 保留 `65512 B payload + 64 KiB slot` 作为 M4 compatibility datapoint，但不把它误标为 64 KiB payload。若设备不能支持 1 MiB+header，则跳过并记录 capability 证据，不能静默截断。

### 6.5 URMA copy 成本边界

`[源码确认]` 当前 Parent 至少有 encode buffer -> registered TX memcpy；File 场景前面还有 file read。Child 至少有 registered RX -> owned Vec memcpy，之后再做 CRC32/file sink。相比 sendfile，Parent 明确多出 userspace staging。

`[架构分析]` 第一阶段保留这些 copy，因为目标是评估已验证 SEND/RECV foundation 的实际价值。可选 `perf stat`/memory-bandwidth 证据用于解释 copy 瓶颈，但本阶段不以 zero-copy 作为修复。

## 7. Memory/File 两类测试

### 7.1 Memory-to-Memory：隔离 transport 上限

准备阶段：

- `[架构分析]` Parent 用固定 seed 生成 payload 并预触碰全部页；生成时间不计入传输。
- `[架构分析]` 预计算 expected CRC32，不计入传输。
- `[架构分析]` Child 预分配目标/工作 buffer并预触碰；不在热路径扩容。

计时路径：

```text
Parent memory -> TCP userspace 或 URMA registered TX copy/SEND
-> Child userspace/RX owned payload -> CRC32 -> memory sink
```

`[架构分析]` 默认 memory sink 写入预分配目标区域并在末尾校验 CRC32/length，避免 optimizer/实现把接收数据直接丢弃。完整 `memcmp` 可在 `t1` 后作为额外正确性 oracle；正式一致性算法仍为计时内的 CRC32。

### 7.2 File-to-File：模拟 Piece 数据路径

```text
Parent input file/range
-> userspace read + TCP
   或 sendfile + TCP
   或 read + registered TX copy + URMA
-> Child receive/owned bytes
-> streaming CRC32
-> 相同 batched pwritev sink
-> output file/range
```

`[架构分析]` 三条路径使用完全相同的输入文件、offset=0、length、expected CRC32、Child write batch（首选与当前 Dragonfly default 对齐为 512 KiB）、同一 filesystem/mount 和独立输出文件。URMA Data message 边界不是 Piece 边界。

### 7.3 page cache 与完成语义

主口径：`warm + buffered-completion`

- `[架构分析]` Parent 输入在正式 repeat 前顺序预读/预触碰；每个 transport 使用同一 warm procedure。
- `[架构分析]` Child 使用普通 buffered write；`t1` 包含用户态 buffer flush 和所有 write/pwritev 返回，但不包含 `fdatasync`。这更接近 Dragonfly streaming write 热路径，同时会受 page cache 吸收写入影响。
- `[架构分析]` 每个 repeat 使用新输出文件名，计时外删除；记录 filesystem、mount options、介质和可用空间。

耐久口径：`warm + durable-completion`

- `[架构分析]` 只在选定的 1 GiB case 上运行；`t1` 包含 `fdatasync`。它回答落到稳定存储后的端到端差异，不与 buffered-completion 混报。

冷缓存诊断：`cold-input diagnostic`

- `[架构分析]` 也只在选定的 1 GiB case 上运行。优先用对目标 fd/range 的 `posix_fadvise(..., DONTNEED)`，并通过读取行为/系统指标确认效果；它是 hint，不保证真正 cold。
- `[架构分析]` 若必须使用 `sync; echo 3 > /proc/sys/vm/drop_caches`，只在独占测试机、明确授权和每轮统一流程下使用；它是全局破坏性操作，不纳入默认自动化。
- `[架构分析]` expected CRC32 必须先计算，再执行 cache eviction，避免 digest pass 重新加热输入。
- `[待验证]` “cold”是否成立必须结合 major fault、块设备读 bytes/延迟等证据；仅调用一个 hint 不能标记为 experimentally cold。

`[架构分析]` 不在首阶段引入 direct I/O，因为其对齐/缓存/写策略会形成另一套数据路径，并偏离当前 Dragonfly buffered I/O。

## 8. 公平性原则

每个可比较 case 必须满足：

1. `[架构分析]` 相同有效 payload bytes、内容 seed、expected CRC32、offset/length。
2. `[架构分析]` Child 在计时内执行相同的增量 CRC32 和 length 校验；URMA 不用 SHA-256而 TCP 用 CRC32。
3. `[架构分析]` File 三条路径共享同一个 sink、write batch、flush/fdatasync 口径。
4. `[架构分析]` TCP userspace 与 URMA 使用相同应用 payload chunk；Dragonfly-like sendfile 不人为切成该 chunk，明确报告其不同点。
5. `[架构分析]` 使用同一对 Parent/Child 机器、同一 CPU/NUMA 绑定策略、同一链路和 MTU；记录 CPU governor、IRQ/RPS/XPS、NUMA、kernel、UMDK/provider/firmware、TCP congestion control 和 offload 状态。
6. `[架构分析]` 两个 transport 分时交错运行，case 顺序随机或轮转，例如 `TCP-U, URMA, TCP-SF`，避免温度/后台负载/缓存随时间偏置。
7. `[架构分析]` 每个正式 case 至少 1 次未计入 warm-up + 5 次计入 repeat；噪声大时增至 9 次，不挑最好一次。
8. `[架构分析]` 每次记录原始结果；报告 median、min/max 和离散度（建议 CV 或 MAD）。
9. `[架构分析]` 热路径关闭逐 chunk 日志、debug tracing和不对称 metrics；错误仍必须可见。
10. `[架构分析]` TCP 和 URMA 都使用 release build；编译器参数一致并记录 commit/dirty state。
11. `[架构分析]` 不把 OOB TCP bytes计入 URMA payload吞吐；但在 setup-included elapsed 中保留其握手时间。
12. `[架构分析]` 若 TCP 与 UB 不是同一物理端口/链路，记录各自 link speed、MTU、NUMA distance，并用 achieved/link-rate 百分比辅助解释；不能只比较绝对 GB/s 后宣称 transport 优劣。

## 9. 第一阶段测试矩阵

### 9.1 Stage 0：preflight，不计性能结论

- M0-M3 既有测试不回归；M4 provider 16 MiB 单次和连续 10 次先补齐。
- capability dump：`max_msg_size`、JFS/JFR/JFC depth、SGE、设备/port/link/MTU、shared JFR 实际创建成功。
- TCP/UB 连通性、文件系统空间、CPU/NUMA/IRQ 基线。
- correctness smoke：64 MiB，所有实现路径 length/CRC32/byte compare 成功。

所有项目前均为 `[待验证]`。Stage 0 失败时不进入正式性能结论。

### 9.2 Stage 1：URMA pipeline/chunk 校准（Memory，1 GiB）

为避免 4 x 4 全笛卡尔积：

1. 固定 payload `256 KiB`，测试 window `1, 2, 4, 8`。
2. 选择“吞吐已接近平台、CPU/empty poll 可接受且 5 次稳定”的最小 window，记为 `W*`。
3. 固定 `W*`，测试 payload `64 KiB, 256 KiB, 512 KiB, 1 MiB`。
4. 同时运行 TCP userspace 的四个 payload chunk，作为 chunk/syscall 敏感性参照。
5. 额外保留 M4 compatibility 点：payload `65512 B`、window=1；只用于量化 pipeline 前后差异，不参与 64 KiB payload同名比较。

若 256 KiB 或 window=8 超 capability，则从能够运行的中间值开始，并记录缩减原因。Stage 1 的输出是 `W*` 和 `C*`，不是最终 transport 胜负。

### 9.3 Stage 2：主 size sweep

使用 `C*`、URMA `W*`：

| Scenario | Size | TCP userspace | Dragonfly-like TCP | URMA pipeline |
|---|---:|:---:|:---:|:---:|
| Memory-to-Memory | 64 MiB | yes | N/A | yes |
| Memory-to-Memory | 1 GiB | yes | N/A | yes |
| Memory-to-Memory | 4 GiB | yes | N/A | yes |
| File-to-File, warm buffered | 64 MiB | yes | yes | yes |
| File-to-File, warm buffered | 1 GiB | yes | yes | yes |
| File-to-File, warm buffered | 4 GiB | yes | yes | yes |

每个 case：1 次 warm-up + 5 次正式 repeat；steady-state 为必做。setup-included 至少对 64 MiB 和 1 GiB 各做 5 次 fresh connection，4 GiB 可不做以控制时长。

### 9.4 Stage 3：文件瓶颈诊断（仅 1 GiB、`C*`/`W*`）

| Policy | TCP userspace | Dragonfly-like TCP | URMA |
|---|:---:|:---:|:---:|
| warm + buffered | Stage 2 复用 | Stage 2 复用 | Stage 2 复用 |
| warm + fdatasync | yes | yes | yes |
| cold-input diagnostic + buffered | yes | yes | yes |

`[架构分析]` 该 staged matrix 覆盖建议的三种大小和四种 chunk，但避免 size x chunk x window x cache x durability 的不可控全组合。只有结果显示明确拐点时，第二轮才在拐点附近追加参数。

## 10. 指标与采集方式

### 10.1 必选指标

每端输出单行结构化 JSON，至少包含：

```text
case_id, commit, role, transport, scenario, timing_mode,
file_policy, size_bytes, payload_bytes, window,
repeat, elapsed_ns, throughput_Bps,
cpu_user_ns, cpu_system_ns, cpu_total_ns, cpu_cores_avg,
bytes, crc32_expected, crc32_actual, length_ok, digest_ok,
success, error_stage
```

计算：

```text
throughput_Bps = payload bytes / Child steady-state elapsed seconds
cpu_cores_avg = (user CPU + system CPU) / wall elapsed
cpu_cost = (Parent CPU + Child CPU) / GiB
```

CPU 必须分别报告 Parent/Child，并给出总和；不能只看 coordinator 或整机 CPU。

必选采集手段优先级：

1. `[架构分析]` benchmark 在 `t0/t1` 同时快照 monotonic clock 与进程 `getrusage`/等价 process CPU，得到与 steady-state严格对齐的 CPU delta。
2. `[架构分析]` `/usr/bin/time -v` 包住 fresh one-shot，作为 setup-included/最大 RSS/上下文切换的通用外部记录。
3. `[架构分析]` `pidstat -p <pid> 1` 在长 case 采样两端 `%usr/%system/%CPU`；若目标机没有 pidstat，不阻塞主测试。

### 10.2 URMA 必选 transport 指标

```text
send_post, recv_post, send_cqe, recv_cqe, cqe_error,
poll_calls_send, poll_calls_recv,
empty_poll_send, empty_poll_recv,
cqe_batch_total / nonempty_poll（推导 avg batch）, 
current_outstanding_send, max_outstanding_send,
current_posted_recv, min/max_posted_recv,
post_send_error, post_recv_error,
drain_elapsed_ns, shutdown_ok
```

验收时必须满足成功 case 的 post/CQE/slot 账目闭合；`max_outstanding_send > 1` 才能证明 pipeline case 实际不是 stop-and-wait。

### 10.3 TCP 必选/建议指标

必选：应用 read/write/sendfile 调用次数和 bytes、partial/EAGAIN 次数（用低开销内建 counter）。

建议：实际 `SO_SNDBUF/SO_RCVBUF`、TCP retransmit（`ss -ti` 或系统计数器，能可靠关联 case 时）、Child read item 数和 pwritev 次数。若 syscall counter 的内建采集实现成本很低则默认开启；`strace -c` 只做单独诊断，不用于正式 CPU/吞吐，因为 tracing 会扰动结果。

### 10.4 可选深度指标

- `perf stat -p` 或直接包进程：`task-clock, cycles, instructions, cache-misses, context-switches, cpu-migrations, page-faults`。
- 块设备：读写 bytes、IOPS、await/util；用于 cold/durable case。
- 网卡/UB 端口 bytes、packet、error/drop/retry/RNR（仅在 provider 暴露且能清晰解释时）。
- NUMA remote access、memory bandwidth/copy hotspot；用于解释 URMA memcpy 成本。
- flamegraph/perf record 仅针对稳定复现的瓶颈 case，不作为第一轮必选。

`[架构分析]` 不假设目标环境有 `perf` 权限或 PMU；缺少可选工具不得阻止必选结果生成，但报告要写清缺失项。

## 11. 验收标准

### 11.1 Harness/正确性验收

正式数据必须同时满足：

1. 所有计入 repeat 的 Child `bytes == configured size`、length/CRC32 成功；抽样 `cmp` 成功。
2. Parent/Child case_id、参数和结果能一一配对，无超时、provider error、TCP reset、CQE error。
3. URMA 使用 shared JFR；正常结束 outstanding WR=0、slot 全部回收、send/recv 账目闭合、shutdown 成功。
4. URMA pipeline case 实测 `max_outstanding_send > 1`，且 receive credit 不低于声明窗口；否则标为错误配置，不与 TCP 正式比较。
5. 热路径没有 per-chunk/CQE 日志；release build、commit、环境清单完整。
6. 同 case 至少 5 个有效 repeat；若吞吐 CV > 5% 或出现系统干扰，增加到 9 次或标记 inconclusive，不能挑选最好结果。
7. 64 MiB/1 GiB/4 GiB 的主结果都使用同一套 `C*`/`W*`；任何临时参数变化单独成 case。
8. warm/cold、buffered/durable、setup/steady-state 标签不能混合汇总。

### 11.2 性能结论门槛

`[架构分析]` 对每个主 case 报 median 差异和 bootstrap 95% CI（样本不足时至少报告全部原始值/MAD，不伪造精度）。预先使用以下工程判定带：

- **吞吐优势**：URMA 相对目标 TCP baseline median 至少高 10%，且差异方向在重复/CI 中稳定。
- **CPU 优势**：吞吐在 TCP 的 ±5% 内或更高，同时 Parent+Child CPU/GiB 至少低 15%，且没有把 CPU 转移到未统计的 helper 进程。
- **相近**：吞吐差异绝对值 <5%，CPU/GiB 差异绝对值 <10%；视为本轮没有可辨识优势，不等于 UB 永远无价值。
- **退化**：URMA 吞吐低至少 10% 或 CPU/GiB 高至少 15%，且在 pipeline/chunk/poll 排查后仍稳定。
- **不确定**：结果位于判定带之间、方差过大、链路/磁盘未饱和证据不足或环境不等价。

这些阈值是首轮工程筛选，不是统计学/产品 SLA。最终报告必须分别对比 TCP userspace 和 Dragonfly-like TCP；“胜过 userspace TCP”不能替代“胜过 Dragonfly sendfile”。

## 12. 结果解释与诊断顺序

### 12.1 预定义解释

- **Memory URMA 快，File 差不多**：`[架构分析]` transport 有潜力，但 page cache、read、Child CRC/pwritev、writeback 或介质成为共同瓶颈。对比 buffered/durable、两端 CPU 和块设备指标。
- **吞吐相近，URMA CPU 更低**：`[架构分析]` 属于有效收益，尤其在未来多任务并发前可能释放 CPU；需要确认 polling CPU 已完整计入。
- **URMA Memory 更慢**：`[架构分析]` 先查 pipeline 是否真的 >1、payload/slot、post/CQE batch、empty poll、registered copy、日志和 CPU/NUMA；不能先归因于 UB 链路。
- **URMA File 比 userspace TCP 好，但不如 sendfile**：`[架构分析]` 说明网络/CPU路径可能有收益，但 Parent registered staging copy 抵消了优势；这是对当前 Dragonfly 快路径的关键负结果。
- **File warm 快、durable 全部接近**：`[架构分析]` 稳定存储是主瓶颈；transport 优势只存在 buffered completion。
- **64 MiB setup-included 差、1/4 GiB steady-state 好**：`[架构分析]` URMA 初始化/建链存在摊销门槛，应计算 break-even size/connection reuse条件，而不是混成一个平均值。
- **URMA CPU 高且 empty-poll 很高**：`[架构分析]` busy polling 策略是候选瓶颈；先做有限 backoff/poll batch 诊断，不直接引入生产异步框架。
- **chunk 增大后 URMA 上升、TCP 平稳**：`[架构分析]` per-message post/CQE 和 copy 固定成本明显；选择稳定拐点，不机械追求最大消息。
- **合理 window/chunk/poll 调整后 URMA 仍无吞吐或 CPU 优势**：`[架构分析]` 记录为当前 SEND/RECV copy-mode 对 Dragonfly-like 单 Piece 路径没有可证优势，建议暂停 Dragonfly integration，先评估是否有不同业务价值；不在本阶段自动跳到 READ/WRITE/zero-copy。

### 12.2 URMA 更慢时的固定排查顺序

```text
correctness / shared JFR / capability
-> 是否仍 stop-and-wait，max outstanding 是否真实 > 1
-> RX prepost/credit/RNR
-> payload/slot 与 message rate
-> CQ batch、empty poll、poll CPU
-> TX encode + registered memcpy、RX registered memcpy
-> CPU/NUMA/IRQ placement
-> File read、CRC32、pwritev/writeback/fsync
-> 与 TCP sendfile 的 Parent copy 差异
```

`[架构分析]` 每次只改变一个类别并保留原始 case；不能在看到一次负结果后同时改变 window、chunk、CPU pinning、cache和文件系统参数。

## 13. 后续实现拆分

### Phase A：benchmark harness 与共同 oracle

- standalone case descriptor、barrier、结构化 JSON、monotonic/process CPU snapshots。
- 计时外 deterministic data/CRC32 preparation。
- Memory sink、共享 batched pwritev File sink、buffered/fdatasync 完成边界。
- 无 Dragonfly/UMDK 的 unit/integration tests；preserve feature-off build。

### Phase B：两个 TCP baseline

- TCP userspace Memory/File read-write loop、partial I/O、socket sizing/counters。
- Linux Dragonfly-like `sendfile_range` 与非 Linux 明确 unsupported（正式环境为 Linux）。
- exact-length framing、CRC32、syscall/byte counters和 File sink共享。

### Phase C：URMA benchmark adapter

- 复用 v3 CRC32 Metadata/Request/Data/End/Error codec，并增加显式 v3 Connection 边界。
- 复用 Runtime/RC Jetty/shared JFR/BufferPool/CompletionPoller/OOB/shutdown。
- 关闭热路径逐 CQE/Data 日志；补 poll/empty poll/outstanding统计。
- 先实现 window=1 compatibility，再做 `1/2/4/8` 最小 TX pipeline和对应 RX prepost。
- capability-gated slot sizing；不暴露 raw UMDK pointer，不重构 FFI foundation。

### Phase D：自动化 runner 与 preflight

- 环境/commit/capability采集、case轮转、warm-up/repeat、文件空间检查。
- Stage 0 M4 provider基线、correctness/cmp、账目/drain断言。
- page-cache policy作为显式选项；全局 drop_caches不默认自动执行。

### Phase E：真实 UB 分阶段运行与报告

- Stage 1 选 `W*`/`C*`，冻结主参数。
- Stage 2 主 size sweep，Stage 3 file诊断。
- 保留每轮原始 JSON、命令、环境清单和异常；生成 median/离散度/CI、吞吐与 CPU/GiB图表。
- 按第 12 节分类结论，再决定继续 Dragonfly integration、只保留实验分支或暂停。

每个 Phase 仍按仓库规则执行 `cargo fmt --check`、feature-off check/test、feature-on compile/test（环境允许时）和需要的真实 provider验证。mock/unit test 不得标记为 UB 性能验证。

## 14. 明确不在本阶段做的事项

- 不直接依赖或修改 Dragonfly crate、Downloader、Storage、Scheduler、Manager。
- 不做多 Peer、多 connection、多 request、多 Piece并发。
- 不做 registered RX zero-copy lease、sendfile等价的 URMA zero-copy、READ/WRITE、remote Segment、UBS Memory。
- 不做生产级 async runtime、connection pool、reconnect、认证、调度或性能自动调优。
- 不以 `urma_perftest` 的裸 transport 数字替代本 benchmark；它只能作为设备/链路 sanity reference。
- 不用一次测量或单一吞吐指标直接得出“UB 有价值/无价值”。

## 15. 源码与文档依据索引

`urma-transport-lab`：

- `docs/m4-build-status.md`：M4 protocol、Parent/Child flow、slot/CQE lifecycle、真实 provider待验项。
- `docs/m5.1-build-status.md`：v3 CRC32 Metadata、v2/v3隔离、尚未接入 Connection/TX pipeline。
- `src/bin/parent.rs`、`src/bin/child.rs`：当前文件发送/接收、逐 SEND drain、BufWriter flush。
- `src/connection.rs`、`src/completion.rs`、`src/buffer.rs`：post/poll、outstanding、CQE route、registered copy和 slot状态。
- `src/oob.rs`、`src/runtime.rs`、`src/jetty.rs`、`src/ffi/shim.c`：初始 RX、capability、depth和 shared JFR。
- `src/message.rs`、`src/digest.rs`、`src/transfer.rs`：v2/v3 codec、CRC32/SHA-256、sequence/length/digest状态机。

Dragonfly2：

- `client/dragonfly-client-storage/src/lib.rs`：`Storage::upload_piece()` 与下载完成 digest比较。
- `client/dragonfly-client-storage/src/io.rs`：`RangeReader`、CRC32、batched `pwritev`。
- `client/dragonfly-client-storage/src/content_linux.rs`：task file range reader/writer。
- `client/dragonfly-client-storage/src/server/tcp.rs`：metadata response、Linux `sendfile_range()`。
- `client/dragonfly-client-storage/src/client/tcp.rs`、`src/client/mod.rs`：`PieceContentStream`、socket options/buffer。
- `client/dragonfly-client-config/src/dfdaemon.rs`：当前 storage read/write buffer默认值。

UMDK：

- `src/urma/lib/urma/core/include/urma_api.h`、`urma_dp_api.c`：Jetty SEND/RECV post与JFC poll API。
- `src/urma/tools/urma_perftest/`：post list、CQE batch、JFS/JFR depth和credit相关的性能工具实现参考。

工程文档：

- `dragonfly-urma-integration-demo-design.md`：standalone Piece语义、copy-mode生命周期、未来性能问题；其中未实现建议仍保持为设计证据。
- UB/URMA分析文档用于术语与背景；具体 API/provider行为以当前 UMDK源码和真实实验为准。
