# M0 Build & ABI Baseline 状态

## 结论

M0 代码已完成到“可在目标 Linux + UMDK 开发环境执行验证”的状态：Cargo
feature 隔离、UMDK header/library 定位、窄 bindgen、C shim 编译、安全 Rust
owner 和 ABI fingerprint 均已落盘。当前工作机只有 rustup/cargo launcher 和
一个不完整的 stable toolchain 目录（缺少 toolchain manifest、rustc、rustfmt），
同时没有 Linux/WSL 或可链接的 `liburma.so`，因此不能把 feature-off 编译和
feature-on ABI/链接标记为实测通过。

本轮没有加入 JFC、Jetty、SEND/RECV 或 OOB API。

## 已核实的 UMDK 源码基线

参考仓库：`D:\Deveploment\Workplace\cloud\umdk`

| 项目 | 已核实内容 |
|---|---|
| commit | `aef4007db28ec7e6311343f58b203858156737f7` |
| public API header | `src/urma/lib/urma/core/include/urma_api.h` |
| 安装 include | `/usr/include/ub/umdk/urma` |
| shared target | `urma` / `liburma.so` |
| library version | `0.0.3`，SOVERSION `0` |
| 默认安装 library | `/usr/lib64` |
| link dependencies | UMDK CMake 中为 `urma_common`、`dl`、`rt` |
| status type | `typedef int urma_status_t`，`URMA_SUCCESS == 0` |

M0 直接使用且已从 public header 核实的 API：

```c
urma_status_t urma_init(urma_init_attr_t *conf);
urma_status_t urma_uninit(void);
urma_device_t *urma_get_device_by_name(char *dev_name);
urma_context_t *urma_create_context(urma_device_t *dev, uint32_t eid_index);
urma_status_t urma_delete_context(urma_context_t *ctx);
```

`urma_get_device_by_name` 的参数在当前 public header 中是 `char *`，shim 只在
调用处对 Rust `CString` 做 const cast；不假设 liburma 接管或修改该字符串。

## 当前完成内容

### Cargo 与 feature 隔离

- 默认 feature 为空；`bindgen` 和 `cc` 是 `urma` feature 的 optional
  build dependencies。
- feature-off 时 `build.rs` 在检查任何 UMDK 路径或调用 native tool 之前
  返回，不生成 bindings、不编译 shim、不输出 `-lurma`。
- feature-on 时检查 Cargo target OS，非 Linux target 立即给出明确错误。
- 未添加 Dragonfly、Tokio 或任何数据面依赖。

### UMDK 定位和 native build

`build.rs` 的解析规则：

1. include 优先读取 `UMDK_INCLUDE_DIR`。该变量既可指向直接包含
   `urma_api.h` 的目录，也可指向包含 `ub/umdk/urma/urma_api.h` 的 include
   root。
2. 未配置时搜索 `/usr/include/ub/umdk/urma` 和
   `/usr/local/include/ub/umdk/urma`。
3. library 优先读取 `UMDK_LIB_DIR`，并明确要求存在 linker input
   `liburma.so`，避免只找到目录却在链接阶段产生不透明错误。
4. 未配置时搜索 `/usr/lib64`、`/usr/lib`、`/usr/local/lib64`、
   `/usr/local/lib` 以及 x86_64/aarch64 multiarch 目录。
5. bindgen 接受 `BINDGEN_EXTRA_CLANG_ARGS`；用于 cross target/sysroot，参数
   必须由调用环境正确提供。
6. C shim 使用与 bindgen 相同的 include directory 编译，随后动态链接
   `urma`。
7. 所有已安装 public `.h` 文件均注册为 Cargo rebuild input。

`UMDK_PROVIDER_DIR` 只输出为运行时诊断信息，不自动写入 rpath。provider 和
`urma.conf` 是否可加载属于运行时验证，不以链接成功代替。

### FFI 与 unsafe 边界

- `wrapper.h` 同时包含 UMDK public `urma_api.h` 和 lab-owned `shim.h`。
- bindgen 只生成已确认的五个 M0 liburma 函数和 `urma_lab_*`；
  `urma_device`、`urma_context` 为 opaque。
- C shim 持有 `urma_device_t *` 和 `urma_context_t *`，复杂 UMDK 类型不进入
  runtime/业务代码。
- `src/ffi/mod.rs` 是唯一 raw binding/unsafe 调用位置，对上仅提供安全的
  `NativeRuntime::open/close` 与 `abi_baseline()`。
- `NativeRuntime` 和 public `UrmaRuntime` 都通过 `Rc` marker 保持
  `!Send + !Sync`。

### Runtime owner

当前创建顺序：

```text
process guard
  -> CString device name
  -> urma_init(NULL)
  -> urma_get_device_by_name
  -> urma_create_context(device, eid_index)
```

当前正常关闭顺序：

```text
urma_delete_context
  -> urma_uninit
  -> free shim owner
  -> release Rust process guard
```

失败处理：

- init/device/context 任一步失败时，shim 回滚已创建资源；
- context 删除失败时不调用 `urma_uninit`，避免 provider code 已卸载但
  context 仍通过 `ctx->ops` 引用 provider；
- close 失败后 Rust process guard 保持 active，本进程不允许重新 init；
- 显式 `close(self)` 返回错误，`Drop` 仅作 best-effort 兜底。

该 owner 目前只验证 init/device/context；没有任何 child native resource。

### ABI baseline

C shim 提供稳定的整数 DTO `urma_lab_abi_baseline_t`，报告：

- shim ABI version；
- pointer size；
- `sizeof(urma_status_t)`；
- `sizeof(urma_init_attr_t)`；
- `sizeof(urma_eid_t)`；
- `sizeof(urma_device_t)`；
- `sizeof(urma_context_t)`；
- `URMA_SUCCESS` 数值。

Rust 通过安全的 `urma_transport_lab::abi_baseline()` 读取。该 fingerprint 用于
记录“shim 实际由哪组 header/target ABI 编译”，不承诺 device/context 的私有
字段稳定，也不允许 Rust 读取这些字段。

## 建议验证命令

### A. 无 UMDK 的 feature-off 主机

```text
cargo fmt --check
cargo test --no-default-features
cargo clippy --all-targets --no-default-features -- -D warnings
```

验收：不需要 `UMDK_INCLUDE_DIR`、`UMDK_LIB_DIR`、libclang C header 或
`liburma.so`；测试应返回明确的 `FeatureDisabled`。

### B. 安装 UMDK 的 Linux 主机

```text
export UMDK_INCLUDE_DIR=/usr/include/ub/umdk/urma
export UMDK_LIB_DIR=/usr/lib64
cargo test --features urma
cargo run --features urma --bin parent -- urma0
```

如果使用源码安装/sysroot，应将两个变量改为目标安装产物，不能把 host header
与 target library 混用。cross compile 时还需设置正确的 Cargo target、linker
和 `BINDGEN_EXTRA_CLANG_ARGS`。

期望启动输出先包含 `AbiBaseline`，随后完成一次 device/context open/close。
这不是 Jetty 或数据面闭环。

## 当前环境实际检查结果

| 检查 | 结果 |
|---|---|
| UMDK public header/API 源码核对 | 已完成 |
| UMDK CMake target/install/link 配置核对 | 已完成 |
| `cargo metadata --no-deps` | 已通过：manifest、targets、optional feature 解析正确 |
| 仓库 trailing whitespace 检查 | 已通过 |
| `git diff --check` | 已通过（当前文件尚未纳入 Git index） |
| `cargo fmt/test/clippy` | 已尝试但未进入构建：rustup 报告没有 default toolchain；指定 `+stable` 后报告 `Missing manifest in toolchain` |
| Linux/WSL fallback | 不可用：当前工作机没有安装 WSL distribution |
| feature-on bindgen/C shim compile | 未执行：当前工作机不是 Linux 且无 Rust/libclang |
| `liburma.so` link | 未执行：参考 UMDK `build` 中没有可用 shared artifact |
| provider/device runtime open | 未执行：需要真实 Linux URMA 节点 |

由于 Cargo 未能进入依赖解析/构建，本仓库当前也没有生成 `Cargo.lock`。目标
工具链第一次成功解析依赖后应提交 lockfile，以固定 `bindgen`/`cc` 的完整
传递依赖集合。

## 仍需真实 UMDK 环境验证

以下项目不能由 Windows 上的源码检查替代：

1. 目标 Rust、bindgen、libclang 与 C compiler 组合是否可生成/编译 bindings；
2. ABI baseline 的实际数值，并确认重复 clean build 一致；
3. `-lurma` 是否仅靠 ELF `DT_NEEDED` 正确解析 `liburma_common`、`dl`、`rt`；
4. `liburma.so.0.0.3` 是否安装了供 linker 使用的 `liburma.so` symlink；
5. provider `.so`、`urma.conf`、device 与 EID index 是否可用；
6. `urma_init(NULL)` 后按配置 device name 创建 context，再显式关闭是否成功；
7. start/close 重复 100 次是否无 double-uninit、provider dangling handle 或泄漏；
8. cross compile 时 clang target/sysroot、target libc header 和 UMDK header 是否一致。

## 明确 TODO / 非 M0 内容

- TODO(M0 验证)：在目标节点记录 ABI baseline、完整 build log 和
  `ldd/readelf` 结果。
- TODO(M0 验证)：生成并审核 `Cargo.lock`。
- TODO(M1+)：device capability/EID snapshot、JFC、registered memory。
- TODO(M2+)：Jetty、descriptor、import/bind、OOB。
- TODO(M3+)：SEND/RECV、WR/SGE、CQ polling。

后四类 UMDK API 尚未加入 bindgen allowlist，当前代码不假设其行为或 ABI。
