# M4 Build Status: Dragonfly-like Piece File Transfer Demo

## 结论

M4 代码、硬件无关测试和 UMDK 源码基线上的 feature-on 编译已经完成。实现复用了 M3 的 Runtime、双向 RC Jetty、shared JFR、registered BufferPool、CompletionPoller、`user_ctx` 路由和 shutdown 顺序，没有接入 Dragonfly、Vortex、remote Segment 或 READ/WRITE。

真实 UB provider 上的 16 MiB 单次及连续 10 次传输尚未在本开发环境执行，因此进入 Dragonfly integration 的代码前置条件已具备，但最终硬件验收条件仍需在目标机器运行本文命令后确认。

## 修改文件

- `Cargo.toml`、`Cargo.lock`：增加 `sha2`。
- `src/message.rs`：M4 wire header 和 Request/Metadata/Data/End/Error；保留 Ping/Pong。
- `src/transfer.rs`：流式 SHA-256、接收状态机、length/sequence/digest 校验。
- `src/connection.rs`：缓存一次 poll 返回的多个 owned message；不加入文件或 Piece 业务。
- `src/completion.rs`：增加不含 opaque 地址的 send/recv CQE 日志。
- `src/lib.rs`：导出 M4 protocol/transfer API。
- `src/bin/parent.rs`、`src/bin/child.rs`：M4 文件 CLI 与传输闭环；保留显式 M3 模式。
- `tests/m3_real_provider.rs`：迁移到 `--ping-pong` 兼容模式。
- `tests/m4_real_provider.rs`：16 MiB 单次与连续 10 次 ignored provider 测试。
- `docs/m4-build-status.md`：本文。

## Protocol

每次 URMA SEND 对应一个完整 message，不使用 TCP byte-stream framing。所有整数采用 big-endian。

固定 24 字节 header：

| 字段 | 类型 | 说明 |
|---|---:|---|
| magic | u32 | `0x55524d44` |
| version | u16 | `2` |
| message_type | u16 | Ping=1、Pong=2、Request=3、Metadata=4、Data=5、End=6、Error=7 |
| request_id | u64 | 业务 request identity；M4 非零 |
| sequence | u32 | Data 从 0 连续递增；End 等于 Data count |
| payload_len | u32 | 必须等于 CQE owned frame 的剩余长度 |

默认 registered slot 为 64 KiB，因此最大 Data payload 是 `65536 - 24 = 65512` 字节。

Payload：

- Request：`piece_number:u32 + task_id_len:u32 + UTF-8 task_id`。
- Metadata：`offset:u64 + total_length:u64 + SHA-256[32]`。
- Data：非空 payload；空文件不发送 Data。
- End：`total_length:u64 + chunk_count:u32`，header sequence 同 chunk count。
- Error：`code:u32 + 非空 UTF-8 message`。

## Parent / Child 流程

Parent：

```text
handshake -> wait Request -> 流式预计算 input SHA-256/length
-> SEND Metadata + wait send CQE
-> 循环 read <= 65512 bytes -> SEND Data + wait send CQE
-> SEND End + wait send CQE -> JSON -> close
```

Child：

```text
handshake -> SEND Request -> wait Metadata
-> wait Data -> validate request/sequence -> repost RX -> write owned payload
-> wait End -> validate End length/chunk count -> validate streaming SHA-256
-> flush output -> JSON -> close
```

Parent 始终只有一个 Data SEND in-flight，没有 pipeline。Child 不聚合完整文件，只持有当前 owned message、`BufWriter` 和 SHA-256 state。

## Buffer / CQE 生命周期

```text
TX: Free -> Allocated -> registered slot write -> SendPosted
    -> send CQE -> SendCompleted -> Free

RX: Free -> Allocated -> PostedRecv -> recv CQE/status/opcode/length 校验
    -> registered slot copy to Vec<u8> -> RecvCompleted -> Free
    -> Child repost RX -> business state validation -> streaming file write
```

`user_ctx` 继续为 `[connection_id:16][generation:8][operation:8][slot_id:32]`。`request_id` 和 sequence 只位于 wire protocol。CompletionPoller 仍只处理 CQE、slot、owned payload、recycle 和统计；文件 I/O、SHA-256 和 Piece 状态都在 `transfer`/CLI 层。

Metadata、每个 Data 和 End 均消费一个已 post RX。Child 在收到 Metadata/Data 的 owned message 后立即补回一个 RX；End 消费最后一个 RX 后不 repost，保证正常关闭前没有 outstanding receive WR。

## 编译与测试结果

2026-08-13 本地结果：

```text
cargo fmt --check                         PASS
cargo check --no-default-features        PASS
cargo test --no-default-features         PASS (24 unit + 1 integration)
cargo check --features urma              PASS
cargo test --features urma --no-run      PASS
feature-on 非 provider tests              PASS (24 unit + runtime rollback)
```

feature-on 使用的本地 UMDK 源码构建路径：

```bash
export UMDK_INCLUDE_DIR=/home/yuan/workspace/cloud-native/umdk/src/urma/lib/urma/core/include
export UMDK_LIB_DIR=/home/yuan/workspace/cloud-native/umdk/build-urma/lib/urma/core
export LD_LIBRARY_PATH="$UMDK_LIB_DIR:/home/yuan/workspace/cloud-native/umdk/build-urma/common"
```

真实 provider 测试保留为 ignored，本机未宣称硬件通过。

## 真实 UB 验证

目标机器编译：

```bash
export UMDK_INCLUDE_DIR=/usr/include/ub/umdk/urma
export UMDK_LIB_DIR=/usr/lib64
export LIBCLANG_PATH=/usr/lib64
cargo build --features urma
```

手工 16 MiB 传输：

```bash
dd if=/dev/urandom of=/tmp/urma-m4-input.bin bs=1M count=16
./target/debug/parent udmac0d1e2 0.0.0.0:19090 /tmp/urma-m4-input.bin
./target/debug/child udmac0d1e2 10.x.x.x:19090 /tmp/urma-m4-output.bin
sha256sum /tmp/urma-m4-input.bin /tmp/urma-m4-output.bin
cmp /tmp/urma-m4-input.bin /tmp/urma-m4-output.bin
```

自动单次及连续 10 次（测试自行生成 16 MiB 输入并比较 length/SHA-256）：

```bash
export URMA_TEST_DEVICE=udmac0d1e2
cargo test --features urma --test m4_real_provider -- --ignored --nocapture --test-threads=1
```

M3 Ping/Pong 兼容模式：

```bash
./target/debug/parent udmac0d1e2 0.0.0.0:19090 --ping-pong 100
./target/debug/child udmac0d1e2 10.x.x.x:19090 --ping-pong 100
```

## 当前仍未实现

- Dragonfly Downloader、Storage、Scheduler、Vortex 和 `PieceContentStream` adapter。
- multi-peer、multi-request、multi-piece 并发。
- TX pipeline、registered RX zero-copy lease、URMA READ/WRITE、remote Segment、UBS Memory。
- 性能调优、生产级流控、重连、认证和 peer crash recovery。

## 是否满足进入 Dragonfly integration 的条件

协议边界、owned chunk 生命周期、流式落盘、sequence/length/digest 状态机和真实 provider 验收脚本已经具备，且与 Dragonfly 当前按 chunk 消费并校验后落盘的边界兼容。因此代码层面可以开始设计 integration adapter。

在将 M4 标记为“真实 UB 完成”或以它作为 integration 的硬件基线前，仍必须在目标 `udmac0d1e2` 环境确认：16 MiB 单次、连续 10 次、JSON/CQE 日志、digest/length 以及无资源释放异常全部通过。

## 2026-08-19：benchmark registered RX window fast path

后续性能实验已在 `benchmark --transport urma` 路径加入 Dragonfly 候选 RDMA
实现风格的 registered RX window lease。此项不改变上述历史 M4 CLI 协议状态。

当前 benchmark Child 数据路径为：

```text
RECV CQE
-> slot 保持 RecvCompleted
-> 按 receive/wire 顺序聚合为只读 scatter RegisteredRxWindowLease
-> sink worker 原地 CRC32
-> file 场景同时从相同 registered window 执行 positional write
-> worker 返回 lease
-> transport 将 slot 归还并 repost
-> 仅在 repost 成功后返回 remote credit
```

生命周期约束：

- `SegmentHandle`、BufferPool 和 provider 对象不跨线程；跨线程对象只有只读内存视图和 lease 元数据。
- lease 存活期间 slot 状态为 `Leased`，不能重新 post。
- BufferPool 在仍有 active lease 时拒绝注销 Segment；异常析构宁可泄漏注册内存，也不提前释放形成 UAF。
- sink pipeline 在 connection/runtime shutdown 前关闭 channel、等待 worker，并回收成功返回的 lease。
- RX window chunk 数仍按物理 RX slot 数选择稳定批次大小；lease 不再假设 slot 地址连续，不连续 registered span 由 CRC/pwrite 按 wire order 遍历。

本地验证：feature-on 完整单元测试通过；包括 registered window 顺序与 CRC、worker 错误传播、window/RQ 分区，以及 direct positional write 完整性。真实 UB provider 性能与 shutdown 行为仍待目标机器验证，不能标记为硬件通过。

## 2026-08-19：parallel CRC 与 transport-only profile

benchmark fast path 继续加入以下实验性优化：

- verified 模式把 Child RX backing 扩为 32 个 application window，同时保持 provider JFR credit depth 为 512；
- completed window 分派到多个 CRC worker，worker 数按 CPU affinity 自动选择，预留一个 polling CPU，最大为 32，也可通过 `--crc-workers N` 显式设置；
- 每个 window 独立计算 CRC32，完成结果可以乱序返回，但使用 `crc32fast::Hasher::combine` 按 wire order 合并；
- file 模式的 positional write 与该 window 的 CRC 仍并发执行，不提前复用 lease；
- RX free list 改为 FIFO，使新的 backing 先于已回收 backing 使用；正确性不再依赖回收后形成连续物理地址；
- 新增 `--urma-profile transport-only`。该模式为完整 payload 加 End 分配注册 RX backing，传输期间暂停 CRC worker，在收到 End 后先结束 transport timing，再启动完整 CRC/可选 pwrite 校验；
- transport-only 仍输出真实 length/CRC integrity，不是跳过校验，但注册内存约等于 payload 大小，因此只用于硬件数据面诊断。

新增统计包括：

```text
parallel_crc_workers
transport_only
post_transport_verification_ns
registered_rx_window_count
total_registered_bytes
```

本地测试验证并行 CRC combine、乱序 worker 结果的有序合并与 lease retirement、scatter span CRC、direct pwrite、profile RX sizing 和生命周期。

后续真实 provider 已验证 normal verified profile：2 GiB、64 KiB chunk、window 128、8 个 CRC worker 达到 6938.63 MiB/s；transport-only 达到 7042.09 MiB/s；fixed-TX 达到 15224.02 MiB/s。三者 length/CRC32 均通过。详细复盘见 `docs/b3.2-urma-performance-optimization-summary-2026-08-19.md`。

2026-08-20 新增 `fixed-tx-transport-only` 组合 profile，用于同时把 Parent TX payload 构造和 Child CRC 移出 transport sample；READY 交换 profile ID 并拒绝两端配置不一致。该组合 profile 已通过本地 feature-on 单元测试和 release build，尚未经过真实 provider 验证。跨节点 64-byte SEND 同时在 demo 和官方 `urma_perftest` 报 `CR status 2`，因此跨节点 UB 环境仍未验证通过。

同日进一步移除fixed-TX的O(transfer bytes)初始化：Parent使用只保存逻辑长度的虚拟memory source，不再生成完整测试`Vec`；固定payload expected CRC使用二进制CRC combine，由O(transfer bytes)降为O(chunk size + log chunk count)。steady-state数据路径与完整性语义不变。本地14项fast-path单元测试和release build通过，真实provider复测待完成。

随后加入payload阶段独立CQ统计、send/recv JFC空poll计数，以及sender真实remote-credit阻塞的累计/最大纳秒数。payload边界为START之后到End完成，不含初始化、Metadata、CRC收尾和shutdown。DONE控制消息已扩展，控制协议版本升至3，要求两端同步二进制。本地27项URMA benchmark单元测试与release build通过，真实provider诊断结果待验证。

## 2026-08-20：file source registered TX direct-fill

benchmark 的 URMA file scenario 已移除 Parent 普通 heap chunk 到 registered TX slot 的
额外 memcpy。Parent 现在分配一个独占 TX slot，并通过 positional `read_at` 直接填充
其注册内存，再提交 SEND。TX slot 只有在 `Allocated`、尚未 post 时可写；提交后仍由
原有 moderated completion frontier 回收，fill/post 错误路径会在没有硬件引用时回滚并
释放。FFI shim ABI 因新增受边界检查的 segment mutable window 升至 7。

结果 JSON 新增 `direct_file_tx`、`file_pread_calls`、`file_pread_bytes` 和
`file_pread_ns`。本地 feature-on URMA benchmark 28 项单元测试和 ABI 基线测试通过；
真实 provider file scenario 尚未运行，不能据此宣称磁盘或 page-cache 性能收益。

## 2026-08-20：file TX registered batch read

真实 8 GiB 文件和 2 GiB tmpfs 数据确认 Parent 的 64 KiB positional read 占 steady-state
约 98%。file TX 已进一步改为分配一个物理连续 registered TX slot batch，一次读取整个
batch，再按 provider `max_msg_size=65536` 拆成独立 SEND。window=64 时 batch 为 4 MiB。

每个 batch tail 强制 completion，并在复用前完整 drain；fill/partial-post 错误路径区分
尚未提交和已被硬件引用的 slots。新增 `file_tx_batch_count`、
`file_tx_batch_max_bytes`。本地 URMA benchmark 29 项单元测试通过，真实 provider
吞吐和错误/关闭行为待验证。

## 2026-08-21：finished-file mmap 与双 registered TX window

为对齐 Dragonfly2 当前候选 RDMA 上传数据路径，URMA benchmark 的 file source 优先在
START 前建立 `MAP_PRIVATE` 只读映射，然后按 application window 连续复制到 registered
TX ring；mmap 不可用时保留批量 `pread` fallback。memory dynamic source 也复用同一个
window producer。

发送侧新增显式 prepared-batch 生命周期，并在 tx slots 足够时使用 A/B 两个 registered
windows：当前 batch 已 post、尚未 completion-retire 时，只填充另一组 free slots；复用
任一组前必须 drain 到其 batch-tail completion frontier。fill、discard、partial post 和
shutdown 路径都区分尚未被 provider 引用与已提交 WR，避免提前复用。

新增统计为 `file_mmap_tx`、`tx_fill_bytes`、`tx_fill_ns`、
`tx_fill_overlap_batches`、`tx_ring_windows`。这不是文件页直接注册、one-sided I/O 或
零拷贝落盘；仍保留 SEND/RECV、64 KiB WR、RX lease、并行 CRC 和完整 length/digest
校验。当前本地 mmap/fast-path 17 项单元测试、feature-off check 和 feature-on release
build 通过；完整 lib test 仅有 3 项既有 TCP loopback 测试因 sandbox 禁止 bind 而失败。
真实 UB provider 的吞吐与 shutdown 尚待复测。
