# URMA transport lab 回归基线（2026-08-24）

## 1. 文档用途

本文冻结当前实验环境、命令、正确性条件和性能观测值。证据优先级为真实实验、当前
源码、架构推断。这里的数值用于同环境回归，不等价于 Dragonfly 产品性能承诺。

当前应区分三层结论：

| 层次 | 基线 | 用途 |
| --- | --- | --- |
| 硬件能力 | `urma_perftest send_bw` | 验证 UB/URMA 链路上限 |
| transport | demo memory/fixed-TX | 验证 WR、CQ、credit 和注册内存路径 |
| file application | demo file-to-file | 验证源文件、transport、CRC 和输出文件流水线 |

Dragonfly TCP 与 Dragonfly URMA 必须在未来使用同一 Scheduler、Piece、Storage 和计时
边界另行比较。当前 demo 的 TCP 为单连接、单线程接收/CRC/write；URMA file 使用
registered ring、linked WR、异步 sink 和并行 CRC，因此两者 file 数值不是纯协议 A/B。

## 2. 已验证环境

```text
两节点跨机
device          = udmac0d1e2
transport mode  = RTP + RC
eid index       = 1
payload         = 65536 bytes
Parent address  = 90.91.177.158
completion      = buffered
```

每轮必须满足：

```text
integrity.ok = true
length_ok    = true
digest_ok    = true
cqe_error    = 0（URMA）
output       = fresh inode，成功后可用 --cleanup-output 删除
两端 benchmark 二进制 sha256 一致
```

## 3. 当前真实实验基线

| 场景 | 大小 | 关键参数 | 吞吐 | 稳定性/说明 |
| --- | ---: | --- | ---: | --- |
| `urma_perftest send_bw` | duration 10s | 64 KiB, eid 1 | 71187.55 MB/s，约 569.50 Gbit/s | 硬件参考 |
| URMA memory fixed-TX | 16 GiB | window 64, post-list 16 | 537.90 Gbit/s | perftest 的约 94.5%，CRC 正确 |
| URMA file-to-file | 8 GiB | window 64, post-list 16, CRC workers 4, warmup 64 | 平均 55.10 Gbit/s | 3轮 55.04/55.08/55.17，CV 约 0.12% |
| TCP sendfile file-to-file | 8 GiB | 单连接、单线程 Child | 平均 19.98 Gbit/s | 2轮 19.95/20.01，5 次 sendfile/轮 |
| TCP userspace file-to-file | 8 GiB | 64 KiB、单连接、单线程 Child | 平均 16.08 Gbit/s | 2轮 16.08/16.07 |

URMA file 三轮平均诊断：

```text
tx_fill_ns             = 1224.9 ms
sink_pwrite_ns         = 382.4 ms（worker累计）
sink_crc_ns            = 841.6 ms（worker累计）
sink_drain_ns          = 631.2 us
remote_credit_wait_ns  = 1.87 ms
warmup_elapsed_ns      = 208.7 us
sink_output_fresh      = 1
cqe_error              = 0
```

`tx_fill_ns` 几乎覆盖传输 wall time，是当前 file 路径主要优化目标；worker 累计的 CRC 和
pwrite 时间不能直接相加后与 wall time 比较。极小的 sink drain 和 credit wait 表明 RX
credit/CQ 不是当前 file 瓶颈。

冷启动诊断中，`warmup-messages=0` 与 `64` 各连续运行 5 轮均未再复现 completion 错误。
因此只能确认当前版本稳定，尚不能证明 warmup 与历史首轮失败之间存在因果关系。

## 4. 外部 CRC32 与 page-cache 控制

默认 `FileSource::from_path()` 会在 Parent 打印 listening 前完整扫描输入文件并计算 CRC32。
8 GiB 文件通常带来约 3–4 秒启动时间，并会预热输入 page cache。

已增加：

```text
--expected-crc32 N
```

支持十进制或 `0x` 十六进制。指定后 Parent 只 open/stat 文件，不读取内容；Child 仍对
收到的每个字节计算 CRC32，错误或过期的外部 digest 会导致完整性失败。结果字段
`source_crc32_external=1` 用于确认快速路径生效。

该参数只允许用于：

```text
--role parent --scenario file
```

比较新旧路径时必须记录 page-cache 策略。外部 CRC 消除了 benchmark 自身的预扫描，
但不能自动清除操作系统中已有的缓存。

## 5. 规范测试命令

以下示例使用此前 8 GiB 输入的 CRC32 `1104745215`。更换文件时必须换成该文件的真实
CRC32，否则最终校验应当失败。

### 5.1 URMA file-to-file

Child：

```bash
taskset -c 2-18 ./target/release/benchmark \
  --role child \
  --transport urma \
  --scenario file \
  --case-id regression-urma-file-8g \
  --bytes 8589934592 \
  --chunk-size 65536 \
  --window 64 \
  --completion-policy buffered \
  --device udmac0d1e2 \
  --eid-index 1 \
  --urma-profile normal \
  --crc-workers 4 \
  --warmup-messages 64 \
  --output /data/urma-output-8g.bin \
  --output-mode fresh \
  --cleanup-output \
  --parent 90.91.177.158:19091
```

Parent：

```bash
taskset -c 1 ./target/release/benchmark \
  --role parent \
  --transport urma \
  --scenario file \
  --case-id regression-urma-file-8g \
  --bytes 8589934592 \
  --chunk-size 65536 \
  --window 64 \
  --completion-policy buffered \
  --device udmac0d1e2 \
  --eid-index 1 \
  --urma-profile normal \
  --urma-post-list 16 \
  --warmup-messages 64 \
  --expected-crc32 1104745215 \
  --input /data/input-8g.bin \
  --listen 0.0.0.0:19091
```

### 5.2 TCP sendfile

Child：

```bash
taskset -c 2 ./target/release/benchmark \
  --role child \
  --transport tcp-sendfile \
  --scenario file \
  --case-id regression-tcp-sendfile-8g \
  --bytes 8589934592 \
  --chunk-size 65536 \
  --window 64 \
  --completion-policy buffered \
  --output /data/tcp-sendfile-output-8g.bin \
  --output-mode fresh \
  --cleanup-output \
  --parent 90.91.177.158:19091
```

Parent：

```bash
taskset -c 1 ./target/release/benchmark \
  --role parent \
  --transport tcp-sendfile \
  --scenario file \
  --case-id regression-tcp-sendfile-8g \
  --bytes 8589934592 \
  --chunk-size 65536 \
  --window 64 \
  --completion-policy buffered \
  --expected-crc32 1104745215 \
  --input /data/input-8g.bin \
  --listen 0.0.0.0:19091
```

### 5.3 TCP userspace

Child：

```bash
taskset -c 2 ./target/release/benchmark \
  --role child \
  --transport tcp-userspace \
  --scenario file \
  --case-id regression-tcp-userspace-8g \
  --bytes 8589934592 \
  --chunk-size 65536 \
  --window 64 \
  --completion-policy buffered \
  --output /data/tcp-userspace-output-8g.bin \
  --output-mode fresh \
  --cleanup-output \
  --parent 90.91.177.158:19091
```

Parent：

```bash
taskset -c 1 ./target/release/benchmark \
  --role parent \
  --transport tcp-userspace \
  --scenario file \
  --case-id regression-tcp-userspace-8g \
  --bytes 8589934592 \
  --chunk-size 65536 \
  --window 64 \
  --completion-policy buffered \
  --expected-crc32 1104745215 \
  --input /data/input-8g.bin \
  --listen 0.0.0.0:19091
```

## 6. 回归判定

每个场景至少运行 3 轮。任何 integrity/CQE/shutdown 失败均为功能回归。性能以本机历史
中位数为中心观察；在 CPU、NUMA、page cache、文件系统、provider 和固件均未变化时，
中位吞吐下降超过 10% 需要调查，不能仅凭单轮结果判定。

本 demo 到此不增加 Piece scheduler 或多连接并发。下一阶段在 Dragonfly 内完成单 Piece
URMA adapter，再由 Dragonfly 现有 Piece 调度驱动 TCP/URMA 并发 A/B。
