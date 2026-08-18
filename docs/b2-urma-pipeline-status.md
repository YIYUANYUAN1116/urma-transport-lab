# B2 Build Status: URMA bounded pipeline

> 更新日期：2026-08-18
> 阶段：B2 correctness；未开始 B3 calibration/性能结论

## 结论

B2 已把 standalone benchmark 的 URMA 数据面从逐消息 stop-and-wait 改为可配置的有界 SEND pipeline，并复用 B0/B1 的 `BenchmarkCase`、source/sink、timer、result、CRC32 和双端进程 CPU 口径。实现仍是单连接、单请求、RC duplex Jetty、shared JFR、SEND/RECV 和 copy mode；没有修改 Linux TCP 数据路径，也没有引入 Dragonfly crate、zero-copy、READ/WRITE 或 UBS Memory。

代码、feature-off 逻辑测试和格式检查已完成。本机是 Windows，不能构建仅支持 Linux target 的 `urma` feature，也没有 UB/UDMA provider；真实 `udmac0d1e2` 验证仍待目标机执行，当前不能声称 hardware validated。

## stop-and-wait 根因（当前源码确认）

根因不在 `UrmaConnection::send()`、Jetty 或 provider：`send()` 在有空闲 TX slot 时可以连续 post 多个 SEND。串行化来自 M4 Parent 的 `send_one()`：Metadata、每条 Data 和 End 都在 `send()` 后立即调用 `drain_completions()`，所以下一条 Data 必须等待上一条 send CQE 和 TX slot 回收。

接收侧也与该行为绑定：OOB handshake 只预投递一个 RX，M4 Child 每收到 Metadata/Data 后只补一个 RX，因此有效 receive credit 为 1。只删除 Parent drain 会让发送窗口与远端 credit 不匹配。

## 最小改动

- 保留 Runtime、JFC、shared JFR、Jetty、Segment、OOB descriptor exchange/import/bind、BufferPool 和 shutdown 所有权树。
- `UrmaConnection` 增加 encoded-frame 发送/接收入口以及 outstanding/credit 只读查询；原 M3/M4 `Message` API 保留为兼容包装。
- `CompletionPoller` 增加 poll/empty-poll/max-outstanding 聚合计数，并关闭逐 CQE 成功日志；错误仍返回结构化错误。
- 新增 `urma_benchmark`：使用现有 v3 `IntegrationMessageV3` 和 CRC32 descriptor；Metadata 在 Start barrier 前完成，Data 使用 bounded pipeline，End 在所有 Data send CQE 回收后发送。
- benchmark CLI 的 TCP 分支未改变；`transport=urma` 增加 `--device`、`--eid-index` 分派。

## window、RX credit 与 chunk 限制

启动数据面前同时校验：

```text
window <= TX slot count
window <= RX slot count
window <= send/recv JFC depth
window <= Jetty send/recv depth
runtime/Jetty config <= device max_jfc/max_jfs/max_jfr depth
slot_size <= provider max_msg_size
```

默认配置是 TX/RX slot 各 8、JFC depth 64、Jetty send/recv depth 64，因此默认 pool 支持的最大 window 是 8。设备 capability 在 Runtime query 后再次校验；任何不合法组合在创建连接/投递 benchmark WR 前返回明确的 `InvalidConfiguration`。

`case.chunk_size` 是每条 Data 的 payload，未被静默调整。B2.1 之后当前协议 header 仍为 24 bytes，合法条件为：

```text
slot_size >= chunk_size + 24
slot_size <= provider_max_msg_size
```

B2 的固定 64 KiB slot 曾把最大 payload 限制为 65512 bytes；B2.1 已解除该 benchmark 限制。v2 M4 codec 仍保留原 65512-byte 上限，只有 benchmark 使用的 v3 codec 接受由 registered slot/provider capability 约束的更大 frame。

### B2.1：registered slot size 参数化

URMA benchmark 不增加独立的必填 CLI 参数，而是从公共 case 自动推导：

```text
required_slot_size = 24 + case.chunk_size
slot_size = align_up(required_slot_size, BufferPoolConfig.alignment)
```

当前 alignment 默认 4096 bytes。对齐只扩大 registered slot，不改变业务 payload；`BenchmarkResult.chunk_size` 和 `transport_stats.effective_payload_size` 都保持用户请求的 Data payload 大小。没有 fallback、clamp 或静默缩小。

Runtime 创建前先执行 header 加法、alignment round-up 和整个 pool footprint 的 checked arithmetic。查询 device capability 后，`slot_size > max_msg_size` 会以明确 `InvalidConfiguration` 失败，并且发生在创建 Jetty/正式 data path 之前。

registered Segment footprint 为：

```text
total_registered_bytes
  = (tx_slot_count + rx_slot_count) * aligned_slot_size
```

slot count 加法、总大小乘法以及每个 slot offset 乘法均使用 checked arithmetic。默认 TX/RX 各 8，因此例如 1 MiB payload 会推导出 1,052,672-byte slot，总 registered memory 为 16,842,752 bytes；最终仍以结果中的实际 `slot_size`/`total_registered_bytes` 为准。

新增 transport stats：

```text
slot_size
effective_payload_size
tx_slot_count
rx_slot_count
total_registered_bytes
configured_window
```

B2.1 没有改变 TX/RX 生命周期、window、receive credit、CQ polling、timer 或 sink 语义。

Child 先用 handshake 的单个 RX 接收 Metadata；得知总长度和 chunk 数后，在发送 Ready 前预投递 `min(2 * window, rx_slot_count, Data 数 + End)` 个 RX。每批 recv CQE 已完成 registered-slot -> owned `Vec` copy 和 slot release；Child 随后先按剩余消息数补 credit，再调用 sink。最后一条 End 使用精确 credit，因此正常完成时 `outstanding_recv=0`，没有为未知未来消息留下 RX WR。

## buffer 生命周期

```text
TX: Free -> Allocated -> registered copy -> SendPosted
    -> send CQE -> SendCompleted -> Free

RX: Free -> Allocated -> PostedRecv -> recv CQE
    -> registered copy to owned frame -> RecvCompleted -> Free
    -> repost replacement credit -> decode/CRC32/FileSink
```

Pipeline 只改变同时处于 `SendPosted` 的 TX slot 数量，不允许 CQE 前复用。FileSink 的 buffered write/flush/sync 不持有 registered RX slot。

## timing 与控制面

Steady-state：source 生成/文件 CRC32 预扫描、Runtime/JFC/JFR/Jetty/Segment、OOB、import/bind、Metadata 和 payload RX prepost 均在 timer 外。Child Ready 后：

```text
Start
-> Data pipeline
-> End
-> sink.finish(buffered | durable)
-> length/CRC32 comparison
-> t1
```

Setup-included 在 Parent bind/Runtime startup 前和 Child Runtime/connect 前启动 timer，因此包含 URMA setup；source 生成和 expected CRC32 仍在 runner 外准备。正式吞吐取 Child elapsed，Parent 从 Start 到 End send CQE 的 elapsed 单独写入 `parent_elapsed_ns`。

CPU 使用与 B1 相同的 `getrusage(RUSAGE_SELF)` 前后 delta，Parent/Child 分别覆盖各自 timer 区间。Child 通过 OOB Done 返回 integrity、elapsed、CPU 和 completion stats；OOB 只承担 barrier/result，不计 payload bytes。

## stats

Canonical Parent JSON 至少包含：

```text
send_post / recv_post
send_cqe / recv_cqe / cqe_error
poll_calls / empty_polls
configured_window / configured_receive_credit
current_outstanding_send / max_outstanding_send
bytes_sent / bytes_received
parent_elapsed_ns
```

`current_outstanding_send` 在成功结果中必须为 0。数据消息数至少为 2 的 W>1 case 若没有观测到 `max_outstanding_send > 1`，runner 明确失败，不能把该结果标记为 pipeline 生效。

## 非 provider 测试

新增硬件无关测试覆盖：

- window 与 slot/JFC/Jetty/provider message/chunk 上限；
- outstanding 永不超过 window，W=4 模拟达到 `max_outstanding_send > 1`；
- pipeline 最终 outstanding 为 0（等价于所有模拟 TX slot 回收）；
- RX credit 初始填充、批量完成后的持续补充和最终 credit 为 0；
- v3 sequence、End length、CRC32 mismatch；
- zero-length Metadata + End。
- 64 KiB、256 KiB、512 KiB、1 MiB payload 的 aligned slot 推导和 v3 1 MiB codec round-trip；
- slot 太小、provider max message size 太小和 registered pool size overflow。

本地执行记录：

```text
cargo fmt --check                  PASS
cargo check --no-default-features PASS（Windows 使用既有 B1 CPU 采集的 cfg portability guard）
cargo test --no-default-features  PASS（66 unit + 2 B0 CLI + 1 runtime integration）
cargo check --features urma       ATTEMPTED/BLOCKED：feature 明确要求 Linux target
cargo test --features urma --no-run
                                    ATTEMPTED/BLOCKED：同上，本机无 Linux UMDK/provider
```

Feature-on 仍须在有 UMDK 的 Linux 构建机执行，不能用 feature-off 结果推断 provider 行为。

## 真实环境 W>1 诊断

真实双节点 UB 环境已经确认以下证据：

- node3 Parent、node4 Child；
- Memory 64 MiB、payload 32768 bytes、W=1 成功；
- W=1 的 `send_post/send_cqe/recv_post/recv_cqe` 均为 2050，`cqe_error=0`，length/CRC32 完整性通过；
- 相同 bytes/chunk 的 W=4 当前失败：Parent 报 `operation URMA pipeline capacity timed out`，Child 报 `operation URMA benchmark receive timed out`。

当前最高概率推断是 W=4 只维持 4 个 RX credit、没有额外 RQ headroom，Parent 收到 send CQE 后可能在 Child 处理 recv CQE 并 repost 前继续发送，使远端 RQ 短暂耗尽并进入 RNR。该判断属于架构推断，尚未被失败现场统计或 UDMA/provider 日志确认，不能记录为已验证根因。CompletionPoller counter、TX slot 回收或 pipeline tracker 是否在真实失败点发生分叉也仍待现场数据确认。

为收集证据，两个目标 timeout 现在各输出一条 `event=urma_benchmark_timeout` 的单行 JSON，不恢复逐 chunk/逐 CQE 成功日志。

Parent `pipeline_capacity` diagnostic 包含：

```text
configured_window
current_outstanding_send
pipeline_tracker_current
max_outstanding_send
send_post / send_cqe
recv_post / recv_cqe
cqe_error
poll_calls / empty_polls
connection_outstanding_send
tx_slots.free / allocated / send_posted / send_completed / other
```

Child `benchmark_receive` diagnostic 包含：

```text
configured_receive_credit
current_receive_credit
benchmark_credit_current
benchmark_credit_remaining_messages
recv_post / recv_cqe
send_post / send_cqe
cqe_error
poll_calls / empty_polls
connection_outstanding_recv
rx_slots.free / allocated / posted_recv / recv_completed / other
```

slot 统计来自 BufferPool 当前状态的只读 snapshot；completion 字段直接复用现有 `CompletionStats`，没有维护第二套 counter。该诊断没有改变 window、RX prepost/repost、poll batch、retry/backoff、timeout、slot 状态转换或 native handle ownership。`send_completed`/`recv_completed` 通常会因为 completion path 随即 release slot 而为 0，但保留它们可以在 timeout 时确认是否存在完成后未释放的 slot。

下一步需在 node3/node4 使用原失败参数重新运行 W=4，同时保存 Parent/Child timeout JSON 和 UDMA/provider 日志。只有现场数据能够区分：RQ/RNR、provider 多 outstanding SEND 路径、completion accounting 分叉或其他原因。

本轮本地验证结果：`cargo fmt --check` 通过；`cargo check --features urma` 和 `cargo test --features urma --no-run` 均因开发机找不到 `urma_api.h` 而在 build script 阶段停止，尚未完成 feature-on 类型检查或测试编译。真实 W=4 provider 回归等待 node3/node4 执行。

为单独验证 RQ headroom 推断，B2 benchmark 当前仅把 receive credit target 从 `window` 调整为：

```text
min(2 * window, rx_slot_count, remaining_messages)
```

默认 RX slot count 为 8，因此 W=4 的 `configured_receive_credit` 从 4 变为 8；W=8 仍受 RX slot count 限制为 8。该实验改动不调整 send window、RX repost 时机、polling、timeout、retry/backoff、Metadata/Data/End 协议或 BufferPool 生命周期。即使 W=4 重跑成功，也只能作为支持 RQ headroom 推断的实验现象，仍需结合 timeout/provider 证据确认根因。

## B2 测试方法

### 1. Linux + UMDK 编译与非 provider 回归

在能找到 UMDK headers、libraries 和 provider 的 Linux 构建机执行：

```bash
cargo fmt --check
cargo check --features urma
cargo test --features urma --no-run
cargo test --no-default-features
cargo build --release --features urma --bin benchmark
```

前三项确认 feature-on 代码能够编译；feature-off 单元测试验证 pipeline/window、RX credit、codec、slot 推导和 completion accounting。它们都不能替代真实 provider 测试。

### 2. 双节点 Memory correctness

下面以 Parent 位于 node3、Child 位于 node4、Parent 数据面地址为 `10.x.x.x:19091`、两端设备名均为 `udmac0d1e2` 为例。先在 node3 启动 Parent：

```bash
./target/release/benchmark \
  --role parent \
  --case-id b2-memory-w4 \
  --repeat 1 \
  --scenario memory \
  --transport urma \
  --bytes 67108864 \
  --chunk-size 65536 \
  --window 4 \
  --timing-mode steady-state \
  --completion-policy buffered \
  --seed 42 \
  --listen 0.0.0.0:19091 \
  --device udmac0d1e2 \
  --eid-index 0
```

再在 node4 启动参数完全匹配的 Child，仅 role、连接地址不同：

```bash
./target/release/benchmark \
  --role child \
  --case-id b2-memory-w4 \
  --repeat 1 \
  --scenario memory \
  --transport urma \
  --bytes 67108864 \
  --chunk-size 65536 \
  --window 4 \
  --timing-mode steady-state \
  --completion-policy buffered \
  --seed 42 \
  --parent 10.x.x.x:19091 \
  --device udmac0d1e2 \
  --eid-index 0
```

分别把 `--window` 和 `--case-id` 改为 W=1、4、8，逐组重新启动 Parent/Child。CLI 的 `--repeat` 是单次样本编号，不会在进程内自动循环；需要多个样本时，应为每个 repeat 重新运行一对进程，并保证两端的 case 参数完全一致。

每个成功 JSON 至少检查：

```text
integrity.ok == true
bytes == 67108864
bytes_sent == bytes_received == 67108864
current_outstanding_send == 0
send_post == send_cqe
recv_post == recv_cqe
cqe_error == 0
configured_window == requested window
configured_receive_credit == min(2 * window, rx_slot_count, remaining_messages)
effective_payload_size == 65536
```

W=4/8 且 Data message 数不少于 2 时还必须满足 `max_outstanding_send > 1`，否则只能证明传输成功，不能证明 bounded pipeline 实际生效。W=1 用作 stop-and-wait correctness 对照。

### 3. 双节点 File correctness

在 node3 准备恰好 64 MiB 的输入文件：

```bash
dd if=/dev/urandom of=/tmp/b2-input.bin bs=1M count=64 status=progress
```

node3 Parent：

```bash
./target/release/benchmark \
  --role parent \
  --case-id b2-file-w4 \
  --repeat 1 \
  --scenario file \
  --transport urma \
  --bytes 67108864 \
  --chunk-size 65536 \
  --window 4 \
  --timing-mode steady-state \
  --completion-policy buffered \
  --seed 42 \
  --input /tmp/b2-input.bin \
  --listen 0.0.0.0:19091 \
  --device udmac0d1e2 \
  --eid-index 0
```

node4 Child：

```bash
./target/release/benchmark \
  --role child \
  --case-id b2-file-w4 \
  --repeat 1 \
  --scenario file \
  --transport urma \
  --bytes 67108864 \
  --chunk-size 65536 \
  --window 4 \
  --timing-mode steady-state \
  --completion-policy buffered \
  --seed 42 \
  --output /tmp/b2-output.bin \
  --parent 10.x.x.x:19091 \
  --device udmac0d1e2 \
  --eid-index 0
```

除检查 Memory case 的 JSON 账目外，还要分别在两端执行 `sha256sum` 并比较 length/digest。若把 Child 输出复制回 node3，再执行字节级比较：

```bash
cmp /tmp/b2-input.bin /tmp/b2-output-from-node4.bin
```

W=1、4、8 均按相同方法执行。`durable` 可作为额外文件落盘检查，但 B2 correctness 的基础矩阵使用 `buffered` 即可；不得把单次成功当作性能或稳定性结论。

## 真实 UB 验证（待执行）

目标环境至少分别运行 Parent/Child；B2 correctness 回归可使用 64 KiB payload，执行：

```text
memory 64 MiB: W=1,4,8
file buffered 64 MiB: W=1,4,8
```

每个 W>1 正式结果必须同时检查：integrity ok、bytes 相等、`current_outstanding_send=0`、`max_outstanding_send>1`、send/recv post 与 CQE 数量一致、`cqe_error=0`。文件 case 还需 `cmp` 输入输出。当前真实 UB 验证状态：`awaiting environment validation`。

## B3 接口（仅保留，未开始）

B3 可直接扫描 `BenchmarkCase.chunk_size` 与 `BenchmarkCase.window`。在 provider `max_msg_size` 和 registered-memory capacity 允许时，当前代码无需修改 pipeline 即可接受 64 KiB、256 KiB、512 KiB、1 MiB Data payload；每个点都会重新推导 slot 并记录 footprint。B2/B2.1 已提供严格 capability validation、原值/effective payload、slot layout、`max_outstanding_send` 和 CQ polling/CPU 统计作为 calibration 观测接口，但尚未运行 B3 matrix、参数推荐或性能结论。
