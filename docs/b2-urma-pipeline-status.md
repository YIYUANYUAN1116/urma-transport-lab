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
current_outstanding_send / current_outstanding_recv / max_outstanding_send
bytes_sent / bytes_received
parent_elapsed_ns
```

`current_outstanding_send` 和 `current_outstanding_recv` 在成功结果中必须为 0。数据消息数至少为 2 的 W>1 case 若没有观测到 `max_outstanding_send > 1`，runner 明确失败，不能把该结果标记为 pipeline 生效。

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

本轮 Linux/当前 UMDK 源码执行记录：

```text
cargo fmt --check                  PASS
cargo check --no-default-features PASS
cargo test --no-default-features  PASS（77 unit + 2 B0 CLI + 1 runtime integration）
cargo check --features urma       PASS（使用当前 UMDK headers/lib）
cargo test --features urma --no-run
                                    PASS（feature-on 全目标测试编译）
cargo build --release --features urma --bin benchmark
                                    PASS
```

上述 feature-on 结果只证明 ABI shim、Rust 类型和链接输入能够构建，不能替代真实 UB/UDMA provider 行为验证。

## 长时间 W>2 卡死定位与修复

实验已确认 W=1、W=2 的 64 MiB case 通过，W=3、W=4 在数百到上千条消息后 timeout，而 W=3 的 196608-byte 小文件通过。失败现场 Parent 的 `send_post - send_cqe` 等于 window，Child 的 `recv_post - recv_cqe` 等于 receive credit；这说明 timeout 快照中的 slot 仍分别处于 `SendPosted`/`PostedRecv`，没有证据表明 CQE 已被 poller 消费后漏掉 slot 回收。

源码定位到的根因是数据阶段错误使用了“总阶段绝对 deadline”：

- Parent `send_source()` 在第一条 Data 前只创建一次 `now + 30s`，随后把同一个 deadline 传给所有 chunk；传输累计超过 30 秒后，窗口下一次满时会在 poll 前直接 timeout。
- Child receive loop 同样只在 Start 后创建一次 deadline，并在每次 poll 前检查；因此持续有 CQE 进展的长传输也会在总耗时达到 30 秒后退出。
- Parent 一旦在 window 满时退出，快照自然留下恰好 W 个 SEND；Child 同时保留当前 receive credit 个 RECV。这与 W=3 时最后 3 SEND、6 RECV 的现场一致，不表示这批 WR 已经发生 slot/token 回收错误。

最小修复保持 `TIMEOUT=30s`、poll batch、yield 策略、window、receive credit、Metadata/Data/End 和 shared-JFR 配置不变，只把判断改为 CQE 无进展 watchdog：每批非空 completion 都刷新 `last_progress`，只有连续 30 秒 poll 不到新 CQE 才报 timeout。Parent capacity wait、最终 pipeline drain 和 Child receive loop 使用同一语义，并先 poll、再在空结果上判断 idle timeout，避免截止点已有 CQE 却未消费。

为继续验证 WR/slot 生命周期，benchmark Data SEND 和 payload RECV 在现有 `user_ctx -> WrHandle` outstanding 表中附带逻辑 sequence；它不改变 pointer-free `WrToken` 编码，也不改变 CQE 路由。timeout 单行 JSON 新增：

```text
Parent:
  last_completed_sequence
  pending_send[].sequence / slot_id / slot_state

Child:
  last_completed_sequence
  pending_recv[].sequence / slot_id / slot_state
```

原有 completion counters 和 slot 聚合 snapshot 保留。Child 的 pending receive sequence 是 RC 消息顺序下预投递 credit 对应的期望 Data/End sequence；最后一次 Data 的 sequence 后一项即 End sequence。诊断只读，不增加第二套完成计数。

当前验证状态：

- 已编译：`cargo check --no-default-features`、使用当前 UMDK headers/lib 的 `cargo check --features urma`。
- 已单元测试：idle watchdog 从最后一次进展计时、RX 预投递 sequence 连续性以及既有 pipeline/credit/codec 测试。
- 待真实 provider 验证：node3/node4 上 W=3/4/8、64 MiB 及更大传输。完成现场回归前，不声称该修复已经 hardware validated。

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
current_outstanding_recv == 0
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

每个 W>1 正式结果必须同时检查：integrity ok、bytes 相等、`current_outstanding_send=0`、`current_outstanding_recv=0`、`max_outstanding_send>1`、send/recv post 与 CQE 数量一致、`cqe_error=0`。文件 case 还需 `cmp` 输入输出。当前真实 UB 验证状态：`awaiting environment validation`。

## B3 接口（仅保留，未开始）

B3 可直接扫描 `BenchmarkCase.chunk_size` 与 `BenchmarkCase.window`。在 provider `max_msg_size` 和 registered-memory capacity 允许时，当前代码无需修改 pipeline 即可接受 64 KiB、256 KiB、512 KiB、1 MiB Data payload；每个点都会重新推导 slot 并记录 footprint。B2/B2.1 已提供严格 capability validation、原值/effective payload、slot layout、`max_outstanding_send` 和 CQ polling/CPU 统计作为 calibration 观测接口，但尚未运行 B3 matrix、参数推荐或性能结论。
