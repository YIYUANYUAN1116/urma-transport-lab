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
