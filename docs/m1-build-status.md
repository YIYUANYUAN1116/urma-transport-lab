# URMA Transport Lab Current Status

## 总览

| 里程碑 | 状态 | 说明 |
|---|---|---|
| M0 Build & ABI baseline | 已实现 | feature 隔离、bindgen、shim、init/context、ABI DTO |
| M1 Runtime Resource Tree | 已实现，待真实环境验证 | capability、两个 JFC、local registered Segment、BufferPool、显式 rollback/shutdown |
| M2 Jetty/OOB | 未实现 | 不在当前代码 allowlist/调用路径 |
| M3 SEND/RECV/CQ polling | 未实现 | 不在当前代码 allowlist/调用路径 |

“已实现”表示真实 UMDK API 调用入口、所有权和错误路径已经编码；不表示在当前
Windows 工作机或真实 URMA provider 上运行成功。

## M0 已完成

- Cargo `urma` feature：feature-off 不解析 UMDK 路径、不运行 bindgen/C
  compiler、不链接 `liburma`。
- feature-on 仅接受 Linux Cargo target。
- `wrapper.h + bindgen + C shim` 已建立，raw bindings 为 crate-private。
- `urma_init(NULL) -> device -> context` 与 ABI baseline 已建立。
- process-global owner 保持 `!Send + !Sync`，正常关闭不依赖字段 Drop 顺序。

M1 扩展 shim DTO/入口后，`URMA_LAB_SHIM_ABI_VERSION` 已从 1 提升为 2；M0
文档中的 version 1 仍是历史基线。

详细历史状态见 `docs/m0-build-status.md`。

## M1 已实现

### Resource tree

`UrmaRuntime::start()` 的实现顺序：

```text
acquire process guard
  -> urma_init(NULL)
  -> urma_get_device_by_name
  -> urma_create_context
  -> urma_query_device + urma_get_eid_list
  -> copy Rust-owned capability/EID snapshot
  -> create send JFC
  -> create recv JFC
  -> aligned zeroed memory allocation
  -> register local-only Segment
  -> publish UrmaRuntime
```

默认实验参数：send/recv JFC depth 各 64；TX/RX slot 各 8；slot size 64 KiB；
backing allocation alignment 4096。这些只是 prototype 默认值，启动前仍会按
device snapshot 校验 JFC 数量/depth 和 max message size。

`page_size_cap` 的 provider-specific 解释尚未在没有真实设备的情况下确认，代码
保留 `TODO(M1-verify)`，当前不会伪造验证结果。

### Capability snapshot

`UrmaDeviceCapability` 是纯 Rust owned 数据，不保存任何 native pointer：

- device name、transport type、selected EID index；
- 完整 EID byte list 与各自 index；
- max JFC/JFS/JFR/Jetty count；
- max JFC/JFS/JFR depth；
- JFS/JFR SGE、remote SGE、inline limit；
- max message size、transport mode mask、page-size capability。

C shim 使用固定上限 DTO：device name 64 bytes、单 EID storage 32 bytes、最多
256 个 EID。当前 UMDK header 的 name/EID 均能容纳；若未来 header 超过这些
上限，shim 返回 `EOVERFLOW`，不会截断或把越界布局交给 Rust。

### JFC

`src/jfc.rs` 中 `UrmaJfc` 分别拥有 send/recv JFC。实际
`urma_jfc_cfg_t` 只在 C shim 构造：depth 来自已校验配置，其余字段清零，
`jfce == NULL` 表示 polling mode。

目标 provider 是否支持 NULL JFCE polling 仍标记为 `TODO(M1-verify)`。代码没有
创建 JFCE，也没有调用 `urma_poll_jfc`。

### BufferPool 和 registered Segment

`src/buffer.rs` 实现：

- 一个 `UrmaRegisteredSegment`；
- 固定大小 TX/RX slot metadata；
- M1 可执行的状态只有 `Free <-> Allocated`；
- 为 M3 保留但没有启用 `Posted`、`Completed`、`Leased`；
- 安全的 checked-add/checked-multiply 大小计算；
- power-of-two 且至少 pointer-sized 的 alignment 校验。

内存与注册都由 shim owner 管理：

```text
posix_memalign -> memset(0) -> urma_register_seg(LOCAL_ONLY)
urma_unregister_seg -> free
```

如果 unregister 失败，shim 故意保留 registered handle 和 backing allocation，
避免释放仍可能被 provider 引用的内存。没有调用 remote Segment import。

### Rollback 与 shutdown

startup 每一步失败都会显式逆序清理已经创建的资源；cleanup 错误通过
`Error::StartupRollback` 保留，不覆盖原始失败。

正常 `shutdown()` 顺序：

```text
stop new slot allocation
  -> unregister Segment / free memory
  -> delete recv JFC
  -> delete send JFC
  -> delete Context
  -> urma_uninit
  -> release process guard
```

shim 跟踪 JFC/Segment child count；仍有 child 时 `runtime_close` 返回 `EBUSY`。
若任一关闭动作失败，runtime 标记 poisoned 并保持 process guard，防止在 provider
状态不确定时重复 `urma_init`。`Drop` 只是 best-effort 入口，不决定正常顺序。

## 已核实的 M1 public API

以下签名均从 UMDK commit
`aef4007db28ec7e6311343f58b203858156737f7` 的 public header 核对：

```c
urma_status_t urma_query_device(urma_device_t *, urma_device_attr_t *);
urma_eid_info_t *urma_get_eid_list(urma_device_t *, uint32_t *);
void urma_free_eid_list(urma_eid_info_t *);
urma_jfc_t *urma_create_jfc(urma_context_t *, urma_jfc_cfg_t *);
urma_status_t urma_delete_jfc(urma_jfc_t *);
urma_target_seg_t *urma_register_seg(urma_context_t *, urma_seg_cfg_t *);
urma_status_t urma_unregister_seg(urma_target_seg_t *);
```

Rust 不直接初始化 `urma_device_attr_t`、`urma_jfc_cfg_t`、bitfield 或
`urma_seg_cfg_t`；这些 bindgen 类型保持 opaque，稳定 DTO 由 shim 逐字段复制。
EID list 的分配和释放也完全在同一次 shim 调用内完成。

## 测试状态

已增加：

- feature-off runtime/ABI 返回 `FeatureDisabled`；
- feature-off BufferPool config 和 overflow-safe layout 检查；
- feature-on ABI contract 检查；
- feature-on 缺失 device 的 startup failure/process guard rollback 检查；
- ignored real-provider M1 start/shutdown test，通过 `URMA_TEST_DEVICE` 显式启用。

这些入口同时保存在独立的 `tests/runtime_test.rs`，便于目标 Linux 节点只运行
runtime lifecycle 验证。

建议命令：

```text
cargo fmt --all -- --check
cargo check --no-default-features
cargo test --no-default-features
cargo clippy --all-targets --no-default-features -- -D warnings

UMDK_INCLUDE_DIR=/usr/include/ub/umdk/urma \
UMDK_LIB_DIR=/usr/lib64 \
cargo check --features urma

URMA_TEST_DEVICE=urma0 \
cargo test --features urma feature_on_enters_complete_m1_initialization_path -- --ignored
```

## 当前无法验证

当前工作机没有完整可用的 Rust toolchain manifest/rustc、没有 Linux/WSL、没有
可链接的 `liburma.so` 和 provider，因此以下项目必须在真实节点完成：

本轮已按要求实际调用 `cargo fmt --all`、`cargo check --no-default-features` 和
`cargo test --no-default-features`。结果均未进入项目编译：rustfmt 进程因本机
toolchain 文件不完整退出；check/test 在 `rustc -vV` 阶段报告
`Missing manifest in toolchain 'stable-x86_64-pc-windows-msvc'`。独立
`cargo metadata --no-deps` 可以运行并已通过。

1. bindgen 对扩展 allowlist 与 opaque types 的实际生成结果；
2. shim 使用 `posix_memalign` 和目标 C compiler 的编译结果；
3. real device name/EID list 与 capability snapshot 数值；
4. provider 是否支持两个 `jfce == NULL` 的 polling JFC；
5. JFC depth/resource count 的真实约束；
6. `URMA_ACCESS_LOCAL_ONLY` Segment 对 4096 alignment 和默认 pool size 的注册；
7. `page_size_cap` 与 alignment 的准确映射；
8. 每个 startup failure 点的真实 rollback 返回值；
9. start/shutdown 重复 100 次的 provider handle、token-id 与内存泄漏检查。

不得用 mock 的成功结果替代上述验证。当前测试只覆盖无需硬件的配置逻辑和明确
失败路径；real-provider test 默认 ignored。

## 明确未实现

- Jetty/JFS/JFR；
- descriptor exchange、import/bind；
- TCP OOB；
- WR/SGE、SEND/RECV；
- CQ polling、CQE、RX repost；
- remote Segment、READ/WRITE、zero-copy。

这些内容属于 M2/M3。
