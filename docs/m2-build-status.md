# M2 Build Status: Duplex Jetty + Static OOB

## 结论

M0 与 M1 保持完成状态。M2 已实现到“可在真实 Linux + UMDK provider 环境进行验证”的程度：两端各自创建 RC duplex Jetty，Parent 通过 TCP OOB 导出稳定 descriptor，Child 校验后 import/bind，并以 `READY/READY_ACK` 建立屏障。

本里程碑没有实现任何数据面操作，也不声明当前 Windows 开发机已经成功创建真实 Jetty。

## 已实现

- `build.rs` allowlist 增加经 UMDK public header 核对的 Jetty create/modify/delete、descriptor get/put、import/unimport、bind/unbind API。
- lab shim ABI version 随 M2 native surface 扩展更新为 `3`。
- `src/ffi/shim.[ch]` 在 C 内构造含 bitfield/union 的 `urma_jetty_cfg_t`；Rust 不初始化或读取该结构。
- C shim 先创建独立 JFR，再作为 shared JFR 创建 RC duplex Jetty，并持有 JFR、local/imported/bound native resource。
- `src/jetty.rs` 提供 descriptor DTO、显式 network-byte-order 编解码、长度/version 校验，以及 crate-private `UrmaJetty` owner。
- descriptor wire 不直接发送 `urma_rjetty_t`。provider bytes 作为有 64 KiB 上限的 opaque payload；元数据使用固定宽度字段。
- `src/connection.rs` 实现 `Init/ContextReady/JettyCreated/DescriptorExchanged/Bound/Ready/Failed/Closed` 状态，并且没有 M3 send API。
- `UrmaConnection<'runtime>` 独占借用 runtime，native Jetty handle 不会离开 owner，也不能越过 JFC/context 生命周期。
- `src/oob.rs` 实现固定 12-byte header：`u32 magic/u16 version/u16 type/u32 payload_len`，所有整数使用 big endian。
- OOB 校验 magic、version、message type、128 KiB payload 上限、role、capability transport type、descriptor version/length/EID。
- 握手严格为 `HELLO -> HELLO_ACK(descriptor) -> READY -> READY_ACK`。协议错误会标记 connection `Failed` 并关闭 TCP socket。
- Parent 在 READY 后等待 Child 断开；Child 在 READY 后等待标准输入回车。两者正常路径均显式关闭 OOB、Jetty、runtime。
- Jetty 正常关闭顺序为 unbind、unimport、delete；runtime 随后关闭 JFC、unregister Segment、delete context、`urma_uninit`。`Drop` 只作 best-effort fallback。
- 单元测试覆盖 descriptor round-trip、descriptor version/size、OOB invalid magic/version/oversize。
- `tests/m2_real_provider.rs` 提供 ignored 的双进程真实 provider 验证入口。

## 未实现（M3+）

- SEND/RECV payload
- WR/SGE
- CQ polling、CQE route
- remote Segment import、READ/WRITE、zero copy
- Piece protocol 或 Dragonfly integration

## 源码核对基线与 TODO

本次按 UMDK 源码基线 `aef4007db28ec7e6311343f58b203858156737f7` 的 public `urma_api.h`/`urma_types.h` 核对了所有新增 native 入口，没有凭空定义 API。

需要在真实目标 provider 验证：

1. provider 是否支持 RC duplex Jetty、shared JFR、两个 polling-mode JFC 的组合；
2. shared JFR 与 Jetty 的创建、descriptor 导出及逆序销毁行为；
3. opaque descriptor 在 Parent/Child 使用相同 provider 与兼容 UMDK 小版本时能否成功 import；
4. token `0` 仅是 lab 静态配置，目标 provider 的 token policy/值必须实测；
5. `urma_bind_jetty` 返回 `URMA_EEXIST` 时按 public API 文档作为已绑定处理是否符合目标 provider；
6. 关闭路径的 unbind/unimport/delete 与 JFC/Segment/context 释放能否全部成功；
7. 真实 capability 的 RC mode bit 定义；当前代码在核实之前不凭假设解释 `trans_mode` 位图。

## 验证命令

无 UMDK 的普通开发环境：

```text
cargo fmt --check
cargo check --no-default-features
cargo test --no-default-features
```

安装 UMDK 的 Linux 目标环境：

```text
export UMDK_INCLUDE_DIR=/usr/include/ub/umdk/urma
export UMDK_LIB_DIR=/usr/lib64
cargo check --features urma
cargo test --features urma
cargo test --features urma --test m2_real_provider -- --ignored --nocapture
```

手工双进程：

```text
cargo run --features urma --bin parent -- urma0 0.0.0.0:19090
cargo run --features urma --bin child -- urma0 127.0.0.1:19090
```

## 当前开发机验证限制

当前开发机没有 Linux UMDK provider/hardware。Windows 上的 feature-off `cargo fmt`、`cargo check`、`cargo test` 已执行通过；Cargo 首次解析依赖后生成了 `Cargo.lock`。feature-on 构建被 `build.rs` 正确限制为 Linux target，因此这仍然不是 provider 成功验证。最终应以目标 Linux 节点执行上述 feature-on 命令的日志为准。
