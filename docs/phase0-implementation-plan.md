# URMA Transport Lab Phase 0 实现计划

## 1. 目标与验收定义

本仓库用于独立验证以下最小闭环，不接入或修改 Dragonfly：

```text
Parent/Child 静态 OOB
  -> liburma 初始化与 device/context
  -> 双端各自创建 send/recv JFC 与 RC duplex Jetty
  -> 交换并校验 Jetty descriptor
  -> Child import/bind Parent Jetty
  -> 预投 RECV
  -> SEND/RECV
  -> 双端轮询 CQ 并核对 completion
  -> 显式 drain 和逆序释放
```

Phase 0 完成的判定标准是：在两台或同机两个具备 URMA provider 的 Linux
进程之间，Child 发送固定 payload，Parent 收到并回送 ACK；双方均从 JFC
poll 到预期 completion，能够输出资源创建、状态迁移和销毁顺序。OOB TCP
只交换控制信息，不承载测试 payload。

当前提交只建立工程、FFI、初始化生命周期和 Parent/Child 可执行程序骨架，
并未声称已实现 Jetty、SEND/RECV 或 CQ 闭环。

## 2. 范围边界

包含：

- Rust 单 crate 工程与 `parent`、`child` 两个 binary；
- `urma` Cargo feature 隔离 Linux/UMDK 构建依赖；
- `build.rs` 中 header、library、bindgen 与 C shim 编译入口；
- `urma_init -> device -> context` 的最小 owner；
- 后续 JFC、duplex Jetty、descriptor、SEND/RECV、poll 的明确落点；
- 可重复执行的正向、故障注入和资源清理验证。

不包含：

- Dragonfly crate、Downloader、Scheduler、Storage 或 Piece 协议集成；
- one-sided READ/WRITE、远端 Segment import、UBS Memory 或 zero-copy；
- 多 peer、重连、生产级鉴权、TLS、动态服务发现或性能调优；
- 直接复制 UMDK perftest 的 benchmark 管理协议。

## 3. 当前骨架

| 位置 | 职责 | 当前状态 |
|---|---|---|
| `Cargo.toml` | crate、binary 自动发现、`urma` feature | 已建立 |
| `build.rs` | 目标平台检查、include/lib 解析、shim 编译、bindgen、`-lurma` | 已建立；需 Linux UMDK 环境验证 |
| `src/ffi/wrapper.h` | liburma public header 与 lab shim 的 bindgen 入口 | 已建立 |
| `src/ffi/shim.[ch]` | 收敛 C ABI、初始化 device/context、逆序关闭 | 已建立 |
| `src/ffi/mod.rs` | crate-private raw bindings 与窄 unsafe 调用 | 已建立 |
| `src/runtime.rs` | process-global init owner、device/context 生命周期、`!Send + !Sync` | 已建立 |
| `src/bin/{parent,child}.rs` | 两端启动角色骨架 | 已建立；尚无 OOB/Jetty/data path |

默认 feature 关闭时，构建过程不运行 bindgen、不搜索 UMDK、不链接
`liburma`。开启 `urma` 时仅接受 Linux target，路径解析顺序为：

1. `UMDK_INCLUDE_DIR`，否则 `/usr/include/ub/umdk/urma`；
2. `UMDK_LIB_DIR`，否则 `/usr/lib64`；
3. `BINDGEN_EXTRA_CLANG_ARGS` 追加 cross compile 所需 target/sysroot 参数；
4. `UMDK_PROVIDER_DIR` 仅作为后续运行时诊断配置，不写入 rpath。

## 4. FFI 与 unsafe 边界

### 4.1 分层

```text
safe Rust API (runtime / future connection)
  -> crate-private safe-ish ffi functions
  -> generated ffi::sys + lab C shim
  -> liburma public C API
  -> provider
```

规则：

- `ffi::sys` 不得从 crate 导出；业务模块不得保存 `*mut urma_*`；
- raw pointer 的创建、非空检查、唯一所有权和释放只在 `ffi`/runtime owner 中；
- C bitfield、anonymous union、flexible descriptor 不在 Rust 中手工
  `transmute`，由 C shim 构造或提取稳定 DTO；
- native owner 第一阶段不实现 `Send`/`Sync`，后续在 poller OS thread 内创建和销毁；
- `Drop` 仅作兜底，正常路径必须显式 `close`，以便返回清理错误；
- 网络收到的 descriptor 必须先验证 magic、version、role、长度和 capability，
  再交给 import API。

### 4.2 Phase 0 API allowlist

原始 bindings 最终只需覆盖：

- environment/device/context：`urma_init`、`urma_uninit`、
  `urma_get_device_by_name`、`urma_get_eid_list`、`urma_free_eid_list`、
  `urma_query_device`、`urma_create_context`、`urma_delete_context`；
- completion：`urma_create_jfc`、`urma_delete_jfc`、`urma_poll_jfc`；
- Jetty：`urma_create_jetty`、`urma_modify_jetty`、`urma_delete_jetty`、
  `urma_get_rjetty`、`urma_put_rjetty`、`urma_import_jetty`、
  `urma_unimport_jetty`、`urma_bind_jetty`、`urma_unbind_jetty`；
- local memory/data path：`urma_register_seg`、`urma_unregister_seg`、
  `urma_post_jetty_send_wr`、`urma_post_jetty_recv_wr`。

M0 已将 bindgen allowlist 收紧到已核实的 environment/device/context 五个
函数以及 `urma_lab_*` shim ABI；device/context 在 bindings 中保持 opaque。
JFC、Jetty、Segment 和 data-path API 要等对应里程碑核实后才加入 allowlist。

## 5. 分步实施

### M0：构建与 ABI 基线

1. 在目标 Linux 节点记录 UMDK commit/package、架构、compiler、provider、
   header/lib/config 安装位置。
2. 验证 `cargo test` 在无 UMDK 环境下 feature-off 通过。
3. 验证 `cargo test --features urma` 能生成 bindings、编译 shim、链接
   `liburma.so`。
4. 在 shim 增加 `sizeof/alignof/offsetof` probe，只校验 Phase 0 实际穿越
   Rust/C 边界的 DTO。
5. 在真实环境确认当前窄 bindgen allowlist，并固定经目标工具链验证的
   bindgen/clang 组合。

产物：构建日志、ABI fingerprint、环境准备说明。未通过本里程碑前不实现
Jetty data path。

### M1：Runtime 资源树

1. 将 `UrmaRuntime` 创建移动到唯一 I/O OS thread。
2. 查询 device capability 和 EID，并复制为 Rust-owned snapshot。
3. 通过 shim 构造两个 JFC：send JFC 与 recv JFC；poll batch 配置限制为
   `1..=16`。
4. 分配固定 TX/RX backing buffer，注册本地 Segment；Phase 0 不导出或
   import Segment descriptor。
5. 实现显式 creation stack 和逆序 rollback：Segment/JFC 必须先于 Context
   释放，Context 必须先于 `urma_uninit`。

建议初始参数：JFC/JFS/JFR depth 64、poll batch 16、slot 64 KiB、各 8 个
TX/RX slot。它们只是便于调试的起点，必须在 device capability 校验后使用。

### M2：duplex Jetty 与静态 OOB

1. 用 C shim 从无 bitfield DTO 构造 `urma_jetty_cfg_t`：`URMA_TM_RC`、
   non-shared JFR、分别引用 send/recv JFC。
2. Parent 创建并导出本地 `urma_rjetty_t` 的稳定 wire representation；
   Child 同样创建本地 Jetty，但只 import/bind Parent descriptor。
3. 定义独立 OOB frame：固定 magic、u16 version、message kind、payload
   length；所有整数使用显式 network byte order，设置 descriptor 长度上限。
4. 实现最小握手：`HELLO -> HELLO_ACK -> Child import/bind -> READY ->
   READY_ACK`。HELLO 携带 EID/capability，HELLO_ACK 携带 Parent Jetty
   descriptor；不携带 Segment descriptor 或数据 payload。
5. 状态机记录 `Init -> ContextReady -> JettyReady -> Exchanged -> Bound ->
   Ready`，失败沿 creation stack 回滚。

### M3：SEND/RECV 与 CQ polling 闭环

1. READY 前由双方各预投至少一个 RECV WR，并保存 slot/WR 生命周期。
2. Child 用 Jetty SEND 固定消息 `urma-phase0-ping`；Parent recv completion
   后校验 opcode、status、length 和 payload。
3. Parent 回送 `urma-phase0-pong`，Child 做相同校验。
4. 独立轮询 send/recv JFC，每次最多 16 CR；空 poll 做有界 backoff，整体
   操作受 deadline 限制。
5. 每个 completion 通过 `user_ctx` 映射到 owned WR/slot，只有 completion
   后才能复用或释放对应内存。
6. 输出机器可读摘要：post 数、send/recv CR 数、错误 CR、payload hash、
   总耗时；不得打印原始地址、token、完整 descriptor。

### M4：关闭、故障与重复性验证

显式关闭顺序：

```text
stop new posts
  -> Jetty ERROR
  -> bounded CQ drain
  -> unbind / unimport remote Jetty
  -> delete local Jetty
  -> unregister local Segment
  -> delete recv/send JFC
  -> delete Context
  -> urma_uninit
```

验证项：

- 正常 ping/pong 连续运行 100 次，无 double-uninit、悬空 provider handle 或
  资源增长；
- device 不存在、EID 越界、capability 不足时在创建 Connection 前失败；
- OOB bad magic/version/role、descriptor 截断/超长、timeout 均不调用 import；
- import、bind、RECV post、SEND post、CQE error 分别可注入并有界退出；
- READY 前禁止应用 SEND；OOB 字节计数中不出现 ping/pong payload；
- trace 能证明仅使用 SEND/RECV，没有 `get_seg_ctx/import_seg/READ/WRITE`。

## 6. Parent/Child 使用约定（计划态）

目标 CLI：

```text
parent --device urma0 --eid-index 0 --listen 192.0.2.10:19090
child  --device urma0 --eid-index 0 --parent 192.0.2.10:19090
```

当前 skeleton 仅接受第一个位置参数作为 device name，并在 context 创建后
立即退出。M2 再引入完整 CLI；在协议和资源状态未稳定前不增加第三方 async
runtime 或 Dragonfly 依赖。

## 7. 风险与待核实项

- 目标节点的 `liburma.so` 是否仅靠 `-lurma` 即可解析
  `urma_common/dl/rt`，以及 provider/config 的最终部署路径；
- provider 对 RC duplex Jetty、non-shared JFR、无 JFCE polling 的支持矩阵；
- `urma_get_rjetty` descriptor 的跨进程、跨 provider 小版本兼容边界；
- token 的安全 provision 方式，以及 bind 的主动/被动语义和
  `URMA_EEXIST` 重试策略；
- post API 对 WR/SGE 链表的持有期、`bad_wr` 和部分入队行为；
- Jetty ERROR 后 completion flush 保证及 drain timeout 后允许的销毁动作；
- registered memory 的页对齐、access flag、cacheability 约束。

这些问题需要在 M0/M1 用目标硬件、UMDK public header、provider 行为和小型
probe 核实，不能仅凭 perftest 行为推断为稳定 ABI。

## 8. 交付物

Phase 0 最终交付应包含：

1. 本仓库可复现的 feature-off 与 feature-on 构建命令；
2. Parent/Child 源码和静态 OOB wire-format 说明；
3. liburma/shim ABI probe 结果；
4. 成功 ping/pong 的 post/CQ trace 与资源关闭 trace；
5. 故障注入结果矩阵；
6. 一页结论：该组合是否足以进入 Dragonfly 集成设计。

最后一项只给出实验结论，不在本仓库实施 Dragonfly 接入。
