# B1 TCP Baseline 状态

更新时间：2026-08-14

## 1. 修改文件

- `Cargo.toml` / `Cargo.lock`：加入轻量的 `libc` 依赖，用于 Linux `sendfile(2)` 与 `getrusage(2)`。
- `src/tcp_benchmark.rs`：新增 standalone TCP parent/child、控制协议、userspace/sendfile 数据路径、CPU 与 transport stats。
- `src/bin/benchmark.rs`：在保留 B0 `--dry-run` 的基础上增加 `--role parent|child`、地址和文件参数。
- `src/benchmark.rs`：在公共 CPU JSON 中补充 `total_us`，不改变 B0 case/source/sink/timer 语义。
- `src/lib.rs`：公开 B1 runner、TCP source/destination 和 stats 类型，供后续 B2/runner 复用。

B1 未修改 URMA Runtime、shared JFR、Jetty、CompletionPoller、FFI 或 M0-M4 数据面。

## 2. Dragonfly TCP 源码参考

源码确认的 Parent 路径是 `Storage::upload_piece()` 生成 `RangeReader`，HTTP/TCP server 在 Linux 上优先调用带显式 offset 的 `sendfile`，并处理非阻塞 readiness、partial sendfile 和 Linux 单次上限 `0x7ffff000`。Child 把响应体作为 `PieceContentStream` 流式消费，增量计算 CRC32，并通过 storage writer 的 `pwritev` 路径写入文件。

B1 抽取的策略：payload/control 分离、已知范围长度、Parent Linux `sendfile` 显式 offset/remaining、Child 流式接收/CRC32/文件写入。B1 没有复制 Dragonfly crate、HTTP/Vortex、RangeReader、Tokio/nonblocking readiness、Vortex buffer、Storage/pwritev batching、TFO、拥塞算法设置或完整 downloader 生命周期。

Dragonfly 当前源码还配置了 TCP_NODELAY、显式 16 MiB socket buffer、可选 TFO，并在 server 侧选择 cubic。B1 只采用两条 baseline 共用的 TCP_NODELAY；其余优化暂不复现，避免 socket tuning 成为 transport 间的隐藏变量。

## 3. tcp-userspace memory 路径

```text
Parent MemorySource
-> chunk-sized userspace slice
-> counted TCP write loop
-> exact-length TCP receive
-> Child MemorySink
-> incremental CRC32 + length check
```

deterministic payload 与 expected CRC32 在连接和正式计时前生成。发送不复制第二份完整 payload；Child `MemorySink` 流式消费并通过 CRC32 保留可观察结果。write helper 循环处理 partial write，接收端只消费 `expected_bytes`，过早 EOF 会失败。

## 4. tcp-userspace file 路径

```text
Parent FileSource
-> read(case.chunk_size)
-> counted TCP write loop
-> exact-length TCP receive
-> Child FileSink
-> finish(buffered | durable)
```

输入文件长度检查和 expected CRC32 预扫描在正式计时前完成；body 热路径使用单个 chunk buffer，不聚合整个文件。`buffered` 的 `finish()` flush 用户态 writer，`durable` 额外执行 B0 FileSink 的 `sync_data()`。两种 completion policy 都在 t1 前完成。

## 5. tcp-sendfile 路径

仅接受 `scenario=file, transport=tcp-sendfile`，其他组合在 case/runner 边界尽早拒绝。Linux Parent 直接执行：

```text
FileSource fd
-> libc::sendfile(socket fd, explicit offset, remaining)
-> Child exact-length receive
-> FileSink
```

Parent body 热路径不执行 file `read()`，也不建立 userspace payload Vec。实现循环处理 EINTR、WouldBlock 和 partial sendfile，维护显式 `off_t` 与 remaining；单次请求不超过 `0x7ffff000`。Child 路径和 userspace baseline 完全相同，因此 CRC32、length 和 completion policy 口径一致。非 Linux 构建明确返回 unsupported error。

## 6. 控制协议

B1 使用有 magic、version、type、length 的小型 length-prefixed 二进制 frame；单 frame 上限 4096 bytes。消息为：

```text
Child Request(case)
Parent Metadata(case identity, scenario, bytes, expected CRC32)
Child Ready
Parent Start
raw payload bytes
Child Done(integrity, elapsed, child CPU/stats)
```

另有 `Error` frame 用于协议错误。payload 不封装成 control frame，control byte 不计入 payload bytes/stats。双方验证 case identity、传输类型、长度和 digest，错误 magic/version/type、超长 frame、CRC/length 不一致均失败。

## 7. timing t0/t1

`steady-state`：source/payload/expected digest 已准备，TCP connect/accept 已完成，Metadata/Ready barrier 已完成后，Parent 在发送 Start 前启动本端 timer；Child 收到 Start 后启动 timer。Child t1 位于收满 payload、`sink.finish()` 和 integrity comparison 之后。正式吞吐使用 Child 的 end-to-end elapsed，因为它包含接收、sink completion 和 integrity；Parent body elapsed 另存为 `transport_stats.parent_elapsed_ns`。

`setup-included`：Parent 在 bind/socket create 前、Child 在 connect/socket create 前启动 timer；协议 setup、payload、sink.finish 和 integrity 均包含在内。Parent 的 accept 等待时间会进入 Parent 独立 elapsed，正式结果仍以 Child 从 connect 到完成的 elapsed 为准。

两种模式都把 deterministic memory 生成、benchmark 大文件生成、FileSource metadata/expected CRC32 预扫描放在 timer 外。CPU delta 与各端对应的 wall-time 起止点一致；Parent CPU/body timer 在 body 发送完成后结束，等待 Child Done 的空闲时间不计入 Parent CPU。

## 8. socket 配置

- TCP_NODELAY：开启，两条 TCP baseline 相同。
- send/receive buffer：不显式配置，使用 OS 默认值。
- 模式：blocking socket。
- address family：由解析后的 IPv4/IPv6 地址决定，结果记录为 4 或 6。
- connection reuse：关闭；每个 case 建立一条新连接，不做多任务/连接池。
- TFO、cubic 和额外 socket tuning：未开启。

这些设置通过 transport stats 的 `tcp_nodelay`、`socket_buffer_explicit`、`blocking_socket`、`address_family`、`connection_reuse` 显式记录。

## 9. CPU 采集方式

Linux/Unix 使用 `getrusage(RUSAGE_SELF)` 获取进程级 user/system CPU，并在 timer 区间前后取 delta；结果输出 `user_us`、`system_us`、`total_us`。Child 的 CPU/stats 经 Done frame 汇总到 Parent 的 canonical JSON，因此 Parent JSON 同时包含 `parent_cpu` 与 `child_cpu`。Child 自身也输出一行本端 JSON，其中 `parent_cpu=null`。

该指标是进程累计 CPU delta，不是线程级或硬件 counter；B1 不依赖 perf/pidstat。在同进程 unit test 中它会覆盖测试进程其他线程，正式 CLI 则是两个独立进程。

## 10. transport stats

userspace 路径记录 `parent_read_calls`、`parent_write_calls`、`child_read_calls`、`partial_write_count`、`bytes_sent`、`bytes_received`。sendfile 路径额外记录 `sendfile_calls`、`partial_sendfile_count`，且 Parent 的 userspace read/write counter 保持为零。

这些 counter 是 B1 helper 对实际调用尝试/返回值的代码路径计数，适合比较不同 baseline，但不声称等于外部 tracing 或内核内部统计。`partial_write_count`/`partial_sendfile_count` 表示一次成功调用只推进了当前请求的一部分；EINTR/WouldBlock 重试不计作成功 partial。

## 11. 测试结果

2026-08-14 本地执行结果：

- `cargo fmt --check`：通过。
- `cargo check --no-default-features`：通过。
- `cargo test --no-default-features`：通过，58 个 library tests、2 个 B0 CLI tests、1 个 runtime integration test；无失败。
- `cargo clippy --no-default-features --all-targets -- -D warnings`：通过。
- `cargo build --release --no-default-features`：通过。
- 设置现有 UMDK include/lib 路径后，`cargo check --features urma`：通过；仅有既有 generated binding dead-code warnings。
- 同样环境下 `cargo test --features urma --no-run`：通过；B1 未运行真实 UB provider。

新增测试覆盖 control frame round-trip/invalid magic/oversize、partial helpers、CRC mismatch、length mismatch、premature EOF、zero-length、非 chunk 整数倍、steady/setup 两种入口、memory userspace、file userspace buffered/durable、Linux sendfile、stats 及非法 transport/scenario。

release loopback 64 MiB smoke（单次结果只验证功能和字段，不作为性能结论）：

| case | elapsed | throughput | CRC/length | 关键 stats |
| --- | ---: | ---: | --- | --- |
| memory tcp-userspace | 25.063 ms | 2553.57 MiB/s | 通过 | write=256, child read=983 |
| file tcp-userspace buffered | 28.623 ms | 2235.93 MiB/s | 通过，`cmp` 通过 | parent read/write=256/256, child read=257 |
| file tcp-sendfile buffered | 41.633 ms | 1537.26 MiB/s | 通过，`cmp` 通过 | sendfile=1, parent read/write=0/0, child read=256 |

同一 sparse zero-filled 64 MiB source 和相同 buffered policy 用于两条 file smoke。回环、page cache、sparse source 和单次运行使这些吞吐数字不可用于判断 sendfile 优劣。

## 12. 与 Dragonfly 真实路径的差异

B1 是单连接、单 case、blocking raw TCP。Dragonfly 是异步 server/client、HTTP/Vortex body、RangeReader、Linux nonblocking sendfile readiness，Child storage 层包含 PieceContentStream 和 pwritev/batching，并有更完整的错误、取消、超时与 metrics。Dragonfly 还可能使用显式 socket buffer、TFO/cubic 等部署配置。

B1 保留了对比较最关键的差异：userspace read/write 和 Parent sendfile 是两条真实、可辨认的热路径；Child sink、CRC32、长度、completion policy、socket common settings 和 result schema 保持一致。B1 不宣称其绝对吞吐等价于 Dragonfly production。

## 13. B1 明确未实现

- URMA pipeline、multiple outstanding SEND、RX credit 调整或任何 URMA foundation 修改。
- zero-copy、URMA READ/WRITE、UBS Memory。
- 多 peer、多 task/multi-piece 并发、连接复用。
- 自动 matrix runner、统计分析、CSV/report、perf/pidstat 集成。
- Dragonfly crate/integration、完整 HTTP/Vortex/Storage 复现。
- TCP 高级 tuning、超时/取消和生产级异步框架。

## 14. B2 URMA pipeline 对齐接口/语义

B2 应继续复用 `BenchmarkCase`、B0 `MemorySource/FileSource` 与 `MemorySink/FileSink`、`BenchmarkTimer`、`BenchmarkResult`、`IntegrityResult` 和单行 JSON。URMA transport 只负责在 Start barrier 后搬运恰好 `case.bytes`，Child 必须把数据交给相同 sink，并在 `sink.finish()`/CRC/length 后停止 timer。

B2 transport stats 应映射到同一 JSON object，至少提供 bytes sent/received，并加入 send post、send CQE、recv CQE、max outstanding、CQ poll/empty poll。CPU 必须沿用每端相同 measurement region；setup-included 应覆盖 Runtime/connection/protocol setup，steady-state 则在 shared JFR/Jetty/预贴 RX 与双方 Ready 后开始。控制 bytes 不计入 payload，数据生成和 expected digest 预计算仍在正式计时外。

`window` 已存在公共 case，但 B1 TCP 不用它控制 socket；B2 用它表达最小 TX pipeline 的 outstanding 上限。B2 不应为适配 URMA 改写 B1 socket、sink、integrity 或结果语义。
