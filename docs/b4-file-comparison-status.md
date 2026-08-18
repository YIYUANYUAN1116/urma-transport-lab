# B4-code Status: file-to-file comparison framework

> 更新日期：2026-08-16  
> 范围：代码框架与非 provider 测试；没有执行正式 B4 性能实验

## 结论与范围

B4-code 已在 B0/B1/B2 基础上增加统一 file-to-file matrix runner 和聚合报告层。它为同一输入文件展开 `tcp-userspace`、`tcp-sendfile`、`urma` 与 repeat case，保持相同的 `chunk_size`、`window`、timing mode 和 completion policy，并复用一次预扫描得到的 file length/expected CRC32。

本阶段没有修改 transport foundation，没有执行 B3 calibration，没有运行真实 UB file benchmark，也不能从代码或单元测试得出 TCP、sendfile、URMA 的性能优劣。

新增 `file_comparison` 模块提供：

- `B4FileMatrixConfig`：input path、transfer bytes、chunk、window、repeat count、timing、completion policy、transport list；
- `B4FileMatrixRunner::prepare()`：只调用一次 `FileSource::from_path()`；
- 每个 transport/repeat 的显式 file `BenchmarkCase`；
- Parent/Child single-case dispatcher，分别复用 B1 或 B2 runner；
- raw record、per-transport aggregation、stable JSON-lines 和 CSV；
- integrity failure、unsupported、failed、unstable 分类。

执行 callback 必须调用真实 transport runner；框架不会 mock 或制造 URMA 性能样本。单元测试中的 synthetic `BenchmarkResult` 只测试统计公式、账目拒绝和序列化，不是 benchmark 数据。

## 三条 file 数据路径

```text
tcp-userspace:
FileSource -> userspace read(chunk) -> TCP write
-> Child receive -> FileSink -> CRC32 / length

tcp-sendfile:
FileSource fd -> Linux sendfile(explicit offset/remaining)
-> Child receive -> FileSink -> CRC32 / length

urma:
FileSource -> read chunk -> registered TX copy
-> bounded SEND pipeline -> registered RX slot -> owned copy
-> FileSink -> CRC32 / length
```

TCP userspace 保留 read/write/partial counters。TCP sendfile 保留 sendfile counters，并要求 canonical Parent userspace read/write 为 0。URMA 继续使用 RC duplex Jetty、shared JFR、SEND/RECV、copy mode、bounded pipeline、RX credit、B2.1 configurable slot、CompletionPoller 和既有 shutdown/resource ownership。

B4 没有引入 zero-copy、READ/WRITE、UBS Memory、Dragonfly integration、multi-peer 或 multi-piece concurrency。

## 与 B0/B1/B2 的复用

- B0：`BenchmarkCase`、`FileSource`、`FileSink`、CRC32、timer、result、buffered/durable completion。
- B1：TCP control barrier、userspace file loop、Linux sendfile、双端 CPU 和 TCP stats。
- B2/B2.1：v3 CRC32 Metadata/Data/End、bounded SEND、RX credit、registered slot 推导、CQ stats 和 shutdown。

输入文件生成和完整 CRC32 扫描不在 repeat loop 中。`prepare()` 计算一次 metadata，随后所有 transport/repeat 复用；每次正式传输可以重新打开 source fd，但不重新计算完整 CRC32。

## 参数与文件大小

`transfer_bytes`、`chunk_size` 使用 `u64`，框架可描述 64 MiB、1 GiB、4 GiB，以及后续 10/100 GiB 文件；B4-code 没有实际生成或运行这些大文件。

URMA `chunk_size` 和 `window` 必须由 CLI/`BenchmarkCase` 显式传入并原样进入 B2 runner。它们只是等待 B3 real calibration 的 benchmark 参数。B4-code 没有硬编码 C*/W*，也不声明任何 chunk/window 为推荐值或最佳值。

## timing 与 completion

Steady-state：

```text
prepared source / expected CRC32
-> transport setup / Metadata / Ready
-> timer start
-> payload -> sink.finish() -> CRC32 / length validation
-> timer end
```

Setup-included：

```text
prepared source / expected CRC32
-> timer start
-> transport setup / protocol / payload
-> sink.finish() -> CRC32 / length validation
-> timer end
```

Canonical throughput 使用 Child elapsed；Parent measurement 以 `parent_elapsed_ns` 保留。`buffered` 包含 flush，`durable` 进一步包含 `sync_data()`。

## metrics 与成功条件

`BenchmarkResult` 新增 decimal `throughput_gbit_s`，并继续输出 `throughput_mib_s`、elapsed、case parameters、integrity、Parent/Child CPU 和 transport stats。

Runner 检查公共字段、`bytes_sent/bytes_received`、`parent_elapsed_ns` 和各 transport 必需 stats。URMA success 还必须满足：

```text
integrity ok
current_outstanding_send == 0
send_post == send_cqe
recv_post == recv_cqe
cqe_error == 0
configured_window == case.window
effective_payload_size == case.chunk_size
```

W>1 且 Data message count>=2 时，`max_outstanding_send` 必须大于 1，否则 record 标记 failed，不进入成功样本聚合。

## 聚合与输出

每次执行保留 raw canonical JSON；failure/unsupported 使用带 status/detail 的 raw JSON。每个 transport 的 aggregate JSON/CSV 包含 sample count、throughput MiB/s median/min/max、Gbit/s median、CV、integrity failure、unsupported、failed 和 unstable。

成功样本至少 5 个且 throughput CV>5% 时标记 unstable。聚合只准备报告数据，不排名 transport，也不生成“更快/更优”结论。

## 非 provider 测试与状态

测试覆盖三 transport case construction/dispatch、FileSource metadata reuse、buffered/durable、zero-length、非 chunk 整数倍、CRC mismatch、URMA chunk/window/slot 传播、URMA completion accounting、unsupported scenario/provider、median/min/max/CV、unstable 和 JSON/CSV。B1 既有 premature EOF、CRC/length 和 partial I/O 测试继续通过。

```text
cargo fmt --check                  PASS
cargo check --no-default-features PASS
cargo test --no-default-features  PASS（73 library + 2 CLI + 1 runtime integration）
cargo check --features urma       BLOCKED：当前开发机是 Windows target
cargo test --features urma --no-run
                                    BLOCKED：当前开发机是 Windows target
```

Feature-on 编译和真实 provider 行为仍需 Linux + UMDK 环境确认。真实 UB B4 benchmark 尚未执行。

## B4 测试方法

### 1. 先验证 matrix 与报告代码

B4 的 transport-neutral case 展开、结果校验、failure 分类、聚合和 JSON/CSV 可以在没有 URMA provider 的机器上测试：

```bash
cargo fmt --check
cargo check --no-default-features
cargo test --no-default-features file_comparison::tests
```

这组测试使用 synthetic `BenchmarkResult` 验证报告逻辑，不产生真实 TCP/URMA benchmark 样本。Linux + UMDK 构建机还应执行：

```bash
cargo check --features urma
cargo test --features urma --no-run
cargo build --release --features urma --bin benchmark
```

### 2. 准备统一输入与参数

正式 B4 必须先完成 B2 correctness 和 B3 calibration，并使用 B3 得到的 `C*`/`W*`。以下 64 MiB、64 KiB、W=4 只演示命令格式，不表示推荐参数或 B3 结论。

在 Parent 节点生成一次输入文件，三种 transport 和所有 repeat 都复用它：

```bash
dd if=/dev/urandom of=/tmp/b4-input.bin bs=1M count=64 status=progress
sha256sum /tmp/b4-input.bin
```

所有 case 必须保持相同的：

```text
bytes=67108864
chunk_size=C*（示例为 65536）
window=W*（示例为 4；TCP 会记录但不用它控制 socket）
timing_mode
completion_policy
input file
```

### 3. 逐 transport 运行真实 file-to-file case

当前 `benchmark` CLI 一次运行一个 case。每个 case 都先在 Parent/node3 启动监听端，再在 Child/node4 启动连接端。以下先以 `tcp-userspace`、repeat 1 为例。

node3 Parent：

```bash
./target/release/benchmark \
  --role parent \
  --case-id b4-tcp-userspace \
  --repeat 1 \
  --scenario file \
  --transport tcp-userspace \
  --bytes 67108864 \
  --chunk-size 65536 \
  --window 4 \
  --timing-mode steady-state \
  --completion-policy buffered \
  --seed 42 \
  --input /tmp/b4-input.bin \
  --listen 0.0.0.0:19091
```

node4 Child：

```bash
./target/release/benchmark \
  --role child \
  --case-id b4-tcp-userspace \
  --repeat 1 \
  --scenario file \
  --transport tcp-userspace \
  --bytes 67108864 \
  --chunk-size 65536 \
  --window 4 \
  --timing-mode steady-state \
  --completion-policy buffered \
  --seed 42 \
  --output /tmp/b4-tcp-userspace-r1.bin \
  --parent 10.x.x.x:19091
```

测试 `tcp-sendfile` 时，两端同时修改：

```text
--case-id b4-tcp-sendfile
--transport tcp-sendfile
```

并为 Child 使用独立输出文件，例如 `/tmp/b4-tcp-sendfile-r1.bin`。

测试 `urma` 时，两端同时修改为：

```text
--case-id b4-urma
--transport urma
--device udmac0d1e2
--eid-index 0
```

URMA Parent 仍使用 `--listen 0.0.0.0:19091`，Child 仍使用 `--parent 10.x.x.x:19091`，Child 输出可使用 `/tmp/b4-urma-r1.bin`。两端的 `case-id`、repeat、transport、bytes、chunk、window、timing、completion 和 seed 必须完全一致。

### 4. repeat、完整性与结果验收

每种 transport 先做 1 次不计入结果的 warm-up，再分别以 `--repeat 1` 到 `--repeat 5` 重启一对 Parent/Child。`--repeat` 只是样本编号，不会自动循环。每次使用独立 Child 输出路径，并保存两端单行 JSON；正式比较使用 Child elapsed，Parent 的时间保留在 `parent_elapsed_ns`。

每个输出文件均需比较 length/digest；将输出复制回 Parent 后可执行：

```bash
cmp /tmp/b4-input.bin /tmp/b4-tcp-userspace-r1.bin
cmp /tmp/b4-input.bin /tmp/b4-tcp-sendfile-r1.bin
cmp /tmp/b4-input.bin /tmp/b4-urma-r1.bin
```

所有成功 case 都必须满足公共 integrity 和 bytes 账目。另按 transport 检查：

- `tcp-userspace`：存在 userspace read/write counters；
- `tcp-sendfile`：`sendfile_calls > 0`，Parent userspace read/write 为 0；
- `urma`：send/recv post 与 CQE 闭合、`cqe_error=0`、`current_outstanding_send=0`、effective payload/window 与 case 一致，W>1 时 `max_outstanding_send>1`。

需要 `durable` 数据时，三种 transport 全部改为 `--completion-policy durable` 并重新运行完整 repeat，不得把 buffered 与 durable 样本混在同一聚合中。

### 5. 当前自动化边界

`B4FileMatrixRunner` 当前是 library API，`run_with()` 需要调用方提供执行真实 Parent/Child transport 的 callback；仓库尚无独立 B4 matrix CLI。现有 `benchmark` binary 只输出单 case raw JSON，不会自动展开三种 transport/repeat，也不会直接输出 `B4Report` 的 aggregate JSON/CSV。

因此当前可执行验证分为：

```text
file_comparison unit tests
  -> 验证 matrix/report 逻辑

benchmark Parent/Child single cases
  -> 验证真实 transport、文件完整性和 raw metrics
```

在增加真实 matrix driver 之前，正式样本需按上述方法逐 case 调度和留存；不能声称已经端到端验证 `B4FileMatrixRunner` 的真实 transport callback 或自动聚合输出。

## 当前证据边界与后续顺序

B3 尚未执行，因此 C*/W* 未确定。当前不能得出 TCP userspace、Linux sendfile 或 URMA 的性能优劣结论，也不能选择或暗示某组 URMA 参数最优。

B4-code 完成后，正式实验顺序仍然是：

```text
B2 real correctness
-> B3 real calibration
-> 得到 C*/W*
-> B4 real file-to-file comparison
```

不得跳过 B3 实验直接宣称某组 URMA 参数为最优。
