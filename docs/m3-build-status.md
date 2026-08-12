# M3 Build Status: SEND/RECV + CQ Polling

## 结论

M0、M1、M2 保持完成。M3 的最小数据面代码已经实现，等待 Linux + 真实 UMDK provider/hardware 验证；当前 Windows 开发机不能证明真实 SEND/RECV 成功。

目标流程为：

```text
Child prepost RX
  -> Child SEND "urma-phase0-ping"
  -> Parent recv CQE + payload validation
  -> Parent SEND "urma-phase0-pong"
  -> Child recv CQE + payload validation
  -> 双方等待 send CQE
  -> 输出 JSON 结果并显式关闭
```

## 已实现

- C shim ABI 更新为 version `4`。
- C shim 内构造 `urma_sge_t`、`urma_jfs_wr_t`、`urma_jfr_wr_t`，Rust/业务代码不初始化含 union/bitfield 的 UMDK WR。
- `urma_post_jetty_send_wr`、`urma_post_jetty_recv_wr`、`urma_poll_jfc` 已加入受限 build allowlist。
- 每个已 post WR 都由 `urma_lab_wr_t` 持有 WR 与 SGE 元数据，直到匹配 CQE 被消费后才调用 `urma_lab_wr_complete`。
- shim 为 Runtime、Jetty、Segment 记录 outstanding WR 数；未 drain 时拒绝 unbind/delete/unregister/JFC delete，避免 timeout 后释放仍可能被 provider 使用的内存。
- BufferPool 增加 `PostedRecv/RecvCompleted/SendPosted/SendCompleted` 状态转换和 registered Segment bounded read/write。
- RX CQE 后先校验 completion length，再从 registered slot copy 为 owned `Vec<u8>`，随后释放 slot；没有 zero-copy lease。
- TX slot 只在 send CQE 后释放。
- `user_ctx` 使用 `[connection_id:16][generation:8][operation:8][slot_id:32]`，不编码裸指针。
- `CompletionPoller` 每次分别非阻塞 poll send/recv JFC，batch 限制为 `1..=16`，完成 CQE route、slot 查找、错误状态处理和统计；业务 Ping/Pong 校验留在 Connection/CLI。
- `Message` 定义独立于 Dragonfly 的 `magic/version/type/payload_len/payload` big-endian wire format。
- `UrmaConnection::send`、`recv_ready`、`poll_once`、`wait_for_message` 和 `drain_completions` 构成最小安全数据面；只有 `Ready` 状态允许 SEND。
- M3 OOB version 更新为 `2`。HELLO 增加 Child Jetty descriptor，使 Parent 也能 import/bind Child，满足 PONG SEND 所需的 target Jetty；双方均在 READY 前 prepost RX。
- Parent/Child 不再依赖 stdin，自动完成 handshake、PING、PONG、completion drain、JSON 输出和退出；第三个位置参数可选择轮数，默认 `1`。

机器可读输出示例：

```json
{"role":"child","rounds":1,"send_post":1,"send_cqe":1,"recv_cqe":1,"payload_ok":true,"elapsed_us":1234}
```

日志不输出 raw pointer、token、opaque descriptor 或 native address。

## API 使用清单

控制面沿用 M0-M2：

- `urma_init` / `urma_uninit`
- device/context/query API
- `urma_create_jfc` / `urma_delete_jfc`
- `urma_register_seg` / `urma_unregister_seg`
- Jetty create/modify/delete、descriptor get/put、import/unimport、bind/unbind

M3 新增真实入口：

- `urma_post_jetty_send_wr`
- `urma_post_jetty_recv_wr`
- `urma_poll_jfc`

没有调用 remote Segment import、READ/WRITE 或 Dragonfly API。

## 数据路径与所有权

```text
TX
Free -> Allocated -> write registered slot -> SendPosted
     -> send CQE -> SendCompleted -> Free
                     |
                     +-- WR/SGE owner released here

RX
Free -> Allocated -> PostedRecv
     -> recv CQE -> validate length -> copy payload
     -> RecvCompleted -> Free / explicit repost
          |
          +-- WR/SGE owner released after CQE

CQE.user_ctx
  -> decode connection/generation/operation/slot
  -> validate JFC direction + Jetty flag + status/opcode
  -> resolve slot without pointer casting
```

`CompletionPoller` 不等待、不解析 Ping/Pong、不访问 Storage。deadline/yield loop 位于 Connection 的同步 prototype API。

## 测试状态

feature-off 单元测试覆盖：

- Ping/Pong encode/decode；
- 单次和连续 100 次 protocol loopback；
- payload length mismatch；
- `user_ctx` round-trip；
- SEND 前没有任何 RX prepost；
- structured CQE error；
- expired timeout；
- M0-M2 既有 descriptor/OOB/BufferPool 测试。

`tests/m3_real_provider.rs` 包含单次及连续 100 次的 ignored 双进程测试，只有真实 provider 环境才执行实际 URMA handshake + Ping/Pong。

## 当前未验证

1. 目标 provider 是否支持对称 RC bind，以及 M3 OOB v2 的两端 descriptor import；
2. shared JFR 的真实 provider 创建、收包和销毁行为；
3. provider 在 post 返回后是否复制 WR/SGE。当前实现按更保守假设一直保留到 CQE；
4. send/recv CR 的 `flag.bs.s_r`、`flag.bs.jetty`、recv opcode 与 `completion_len` 实际值；
5. `urma_poll_jfc` 空 poll、负错误值和单次最多 16 条的 provider 行为；
6. Jetty 转 ERROR 后 outstanding RX/SEND 是否都会产生可 route 的 flush CQE；
7. CQE error、RNR、peer crash 和 drain timeout 时 provider resource 是否按预期保持可安全诊断状态；
8. registered Segment cacheability、DMA 可见性、alignment 和普通内存 allocator 是否满足目标硬件；
9. 真实连续 100 次 ping/pong、资源增长和重复启动/关闭。

## 验证命令

```text
cargo fmt --check
cargo check --no-default-features
cargo test --no-default-features
```

真实 Linux 节点：

```text
export UMDK_INCLUDE_DIR=/usr/include/ub/umdk/urma
export UMDK_LIB_DIR=/usr/lib64
cargo check --features urma
cargo test --features urma
cargo test --features urma --test m3_real_provider -- --ignored --nocapture
```

手工运行：

```text
cargo run --features urma --bin parent -- urma0 0.0.0.0:19090
cargo run --features urma --bin child -- urma0 127.0.0.1:19090
```

连续 100 次时双方最后增加同一个 `100` 参数。

## 明确未实现

- Dragonfly、Piece、Downloader、Storage
- remote Segment、READ/WRITE、zero copy
- async I/O thread/channel façade
- production authentication/reconnect/multi-peer
- CQ event notification、adaptive backoff、performance tuning
