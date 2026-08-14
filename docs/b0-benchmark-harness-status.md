# B0 Benchmark Harness Build Status

> 完成日期：2026-08-14  
> 基线：`urma-transport-lab` B0 工作树（起点 `0e7d4684ad5a3cceac994b5db771e186a811985d`）

## 1. 结论

B0 已实现 transport-neutral 的 benchmark 基础框架：统一 case/result、显式 timing mode、Dragonfly-compatible CRC32 integrity、deterministic memory source、streaming file source/sink，以及只做参数校验和 case JSON 输出的独立 `benchmark --dry-run` CLI。

本阶段没有实现 TCP 数据传输，也没有修改 URMA Connection、CompletionPoller、BufferPool、FFI、Jetty、shared JFR 或既有 Parent/Child CLI。普通 feature-off 构建不需要 UMDK。

## 2. 修改文件

| 文件 | 内容 |
|---|---|
| `src/benchmark.rs` | 新增 case/result/timer、JSON、memory/file source/sink、integrity 和单元测试。 |
| `src/lib.rs` | 公开导出 B0 API。 |
| `src/bin/benchmark.rs` | 新增独立 dry-run CLI；不含 transport data path。 |
| `tests/b0_benchmark_cli.rs` | dry-run 稳定 JSON 与非法组合集成测试。 |
| `docs/b0-benchmark-harness-status.md` | 本文。 |

没有新增 crate 依赖；JSON 与 CLI 保持当前仓库的小型、零依赖实现方式。

## 3. Benchmark case 模型

`BenchmarkCase` 表达：

```text
case_id
repeat
scenario: memory | file
transport: tcp-userspace | tcp-sendfile | urma
transfer_bytes
chunk_size
window
timing_mode: steady-state | setup-included
completion_policy: buffered | durable
data_seed
```

参数校验集中在 `BenchmarkCase::new()/validate()`：

- `case_id` 必须非空、最大 128 bytes且不能含 control character；
- `repeat/chunk_size/window` 必须非零；
- `chunk_size` 必须能转换为本机 `usize`，且不能超过 Rust buffer size上限；
- `tcp-sendfile` 只允许 file scenario；
- durable completion 只允许 file scenario；
- memory transfer size 必须能由单个 Rust source buffer表示；
- zero-length transfer 合法，chunk count 为 0；
- chunk count 使用 quotient/remainder 计算，避免 `bytes + chunk - 1` 溢出。

`window` 目前只是公共 case 字段。B0 不据此创建多个 SEND、修改 RX credit 或调整任何 URMA native 配置。

## 4. Common result 模型

`BenchmarkResult` 包含：

```text
case_id / repeat
transport / scenario
bytes / chunk_size / window
elapsed_ns / elapsed_us
throughput_mib_s
timing_mode / completion_policy
integrity {
  expected_bytes / actual_bytes
  expected_crc32 / actual_crc32
  length_ok / digest_ok / ok
}
parent_cpu / child_cpu
transport_stats
```

`parent_cpu`、`child_cpu` 当前为可选占位；`transport_stats` 是稳定排序的 `BTreeMap<String, u64>`，供 B1/B2 添加 syscall 或 URMA counter。`to_json_line()` 输出固定字段顺序、经过字符串 escaping、无换行的单行 JSON。吞吐按实际消费 bytes和 elapsed 计算为 MiB/s；zero bytes 或 zero duration 返回 `0.0`，不会输出 JSON 非法的 NaN/Infinity。

`BenchmarkResult::from_sample()` 会检查 timing mode 与 case 一致，也检查 integrity expected bytes 等于 case transfer bytes，避免错误配对不同 case 的结果。

## 5. Timing boundary

`BenchmarkTimer::start(mode)` 和 `TimingSample` 只负责 monotonic elapsed。调用位置由 transport 明确选择：

### steady-state

```text
prepare deterministic data / expected CRC32 / files
-> prepare transport
-> BenchmarkTimer::start(SteadyState)
-> transfer + sink + integrity finish
-> timer.finish()
```

数据生成、expected digest、输入大文件创建默认位于 timer 外。

### setup-included

```text
prepare deterministic data / expected CRC32 / files
-> BenchmarkTimer::start(SetupIncluded)
-> transport setup + transfer + sink + integrity finish
-> timer.finish()
```

B0 不创建 TCP/URMA connection，因此只提供边界模型；B1/B2 必须在正确位置启动相同 timer，不能由 source/sink 隐式启动。

## 6. CRC32 / integrity

B0 直接复用 M5.1 的：

- `Crc32Hasher::new/update/finalize`；
- `crc32_bytes`；
- `crc32_reader`。

没有新增第二套 CRC32 编码。值仍是与 M5.1/Dragonfly standard Piece一致的 CRC32 u32；需要外部字符串时继续使用既有 `format_crc32_digest()`，即 `crc32:<decimal-u32>`。

`IntegrityResult` 统一比较 expected/actual length 和 CRC32。Memory/File sink 都通过同一个结构返回结果。

## 7. Memory source / sink

`MemorySource::generate(length, seed)`：

- 使用固定 SplitMix64 byte stream 生成可复现 payload；
- 相同 size/seed 得到相同 bytes，与生成 chunk 大小无关；
- 使用 fallible reserve，对不可表示/不可分配大小返回配置错误；
- 只持有一份完整 source buffer，不为 harness 强制复制第二份；
- 在构造时计算 expected CRC32，因此调用方可在 timer 前完成准备。

`MemorySink` 不保存第二份完整 payload。每次 `write_chunk()` 真正更新 byte count 和 `crc32fast` state，`finish()` 返回 integrity result；接收内容不会成为可被省略的空操作。

zero-length memory source/sink 合法，CRC32 为 0。

## 8. File source / sink

`FileSource` 保存：

```text
path
length
expected_crc32
```

两种准备方式：

- `from_path()`：流式扫描已有文件得到 length/CRC32；
- `generate()`：用与 MemorySource 相同的 deterministic byte stream 分块创建文件，并在写入时同步计算 CRC32，不把整个文件读入内存。

`open()` 为后续 transport 返回普通 `File`；B0 不决定 B1 使用 read loop或 B2 的 sendfile offset策略。

`FileSink` 使用 `BufWriter<File>` 流式消费 chunk并增量 CRC32：

- buffered：`finish()` 执行 `flush()`；
- durable：在 `flush()` 后执行 `File::sync_data()`；
- 默认 buffer capacity 为 512 KiB，与当前 Dragonfly storage默认 write buffer量级一致；测试/transport可显式指定容量；
- 两种 policy 都执行 expected/actual length和CRC32比较；
- zero-length file 对 buffered/durable 都合法。

B0 sink 是 standalone oracle，不复制 Dragonfly Storage metadata、pwritev batching、offset range或 metadata commit。

## 9. CLI 入口

新增独立 binary：

```bash
cargo run --bin benchmark -- \
  --dry-run \
  --case-id b0-smoke \
  --scenario file \
  --transport tcp-sendfile \
  --bytes 1048576 \
  --chunk-size 262144 \
  --window 1 \
  --timing-mode setup-included \
  --completion-policy durable \
  --seed 42
```

输出：

```json
{"case_id":"b0-smoke","repeat":1,"scenario":"file","transport":"tcp-sendfile","bytes":1048576,"chunk_size":262144,"window":1,"timing_mode":"setup-included","completion_policy":"durable","data_seed":42}
```

不带 `--dry-run` 会明确失败：B0 没有 transport data path。既有 `parent`/`child` 参数和行为未修改。

## 10. 测试结果

2026-08-14 本地执行：

```text
cargo fmt --check                         PASS
cargo check --no-default-features        PASS
cargo test --no-default-features         PASS
  library unit tests                     51 passed
  B0 CLI integration tests                2 passed
  existing runtime integration test       1 passed
cargo check --features urma              PASS
cargo test --features urma --no-run      PASS
```

feature-on 使用：

```text
UMDK_INCLUDE_DIR=/home/yuan/workspace/cloud-native/umdk/src/urma/lib/urma/core/include
UMDK_LIB_DIR=/home/yuan/workspace/cloud-native/umdk/build-urma/lib/urma/core
```

未设置上述环境变量的第一次 feature-on 探测按预期在 build script 报告找不到 `urma_api.h`；设置当前本地 UMDK路径后检查和 test binary link均通过。输出只有 bindgen生成 binding 的既有 dead-code warnings。

新增覆盖包括：case valid/invalid、overflow-safe chunk count、case/result稳定 JSON、吞吐、timer mode、deterministic memory/file一致性、memory CRC32、不可分配大小、streaming file copy、zero-length memory/file、buffered/durable、length/digest mismatch和CLI非法组合。

B0 没有运行真实 UB provider；不宣称硬件行为或性能已验证。

## 11. B0 明确未实现

- TCP userspace data path；
- TCP sendfile；
- URMA v3 Connection接入；
- URMA TX pipeline、multiple outstanding SEND；
- RX prepost/credit调整；
- 完整 Parent/Child CPU采集、pidstat/perf；
- benchmark matrix runner、CSV、统计报告；
- Dragonfly integration；
- zero-copy、READ/WRITE、remote Segment、UBS Memory；
- 多连接、多任务并发。

## 12. B1 TCP baseline 可复用公开接口

B1 不需要改 B0 的结果或 sink语义，只需实现“如何搬运 bytes”：

1. `BenchmarkCase`：解析并校验 transport/scenario/bytes/chunk/timing/completion。
2. `MemorySource::bytes()/chunks()`：Parent userspace TCP 的 source slice。
3. `FileSource::open()/length()/expected_crc32()`：Parent file read loop的 source metadata和 fd。
4. `BenchmarkSink::write_chunk()/finish()`：Child TCP read loop统一投递到 `MemorySink` 或 `FileSink`。
5. `BenchmarkTimer`：steady-state 在 socket ready后启动；setup-included 在 socket setup前启动。
6. `TimingSample + IntegrityResult -> BenchmarkResult::from_sample()`：形成统一结果。
7. `BenchmarkResult::transport_stats`：加入 read/write/partial/EAGAIN/syscall byte counter。
8. `BenchmarkResult::to_json_line()`：只向 stdout写一条正式结果；诊断日志走 stderr。

B1 若需要协议控制面，可以新增 TCP-specific request/metadata framing，但不得把 socket/native选项塞回 `BenchmarkCase`，也不得绕过共同 sink、CRC32、timer和结果格式。
