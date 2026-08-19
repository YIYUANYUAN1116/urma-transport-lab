# M3 Two-node Ping-pong Completion Drain Fix Status

## 结论

真实 UB 双节点连续 ping-pong 已暴露 completion drain 对 CQE 到达顺序的错误假设。本次以最小修改修复：`drain_completions()` 在等待 send completion 时遇到 `RecvCompleted`，不再返回协议错误，而是把事件中已经 owned 的 frame 保存到现有 `pending_frames` FIFO，供后续 `wait_for_message()` 消费。

没有修改 URMA transport 架构、Jetty、JFC、JFR、BufferPool、WR token、CQE 校验或资源关闭路径，也没有引入 pipeline 或新的 transport 抽象。

## 问题现象

在真实 UB 双节点环境中，node3 运行 Parent、node4 运行 Child，Parent 使用 `--ping-pong 100` 时出现：

```text
parent: protocol error: unexpected receive while draining send completions
child: operation wait_for_frame timed out
```

已有真实实验确认 OOB handshake、Jetty descriptor exchange、`Bound -> Ready`、URMA SEND/RECV 数据路径和 CQE 产生均正常。本次本地工作未重复运行真实 provider 测试。

## 根因分析

以下为当前源码确认：

1. `CompletionPoller::poll_once()` 先 poll send JFC，再 poll recv JFC，一次调用可以返回包含 `SendCompleted` 和 `RecvCompleted` 的事件集合。
2. CQE 在 `CompletionPoller::route()` 内完成 `user_ctx`、队列方向、Jetty flag、status 和 receive opcode 校验。
3. `SendCompleted` 产生前，poller 已减少 `outstanding_send`，完成 WR owner，并把 TX slot 从 `SendCompleted` 释放回 `Free`。
4. `RecvCompleted` 产生前，poller 已减少 `outstanding_recv`，完成 WR owner，把 registered RX slot 内容 copy 到 owned `Vec<u8>`，再释放 RX slot。事件携带的是 owned bytes，不再依赖 RX slot 生命周期。
5. Connection 原本已经存在 `pending_frames: VecDeque<Vec<u8>>`。`wait_for_frame()` 会把自己 poll 到的 `RecvCompleted.bytes` 放入该 FIFO，但 `drain_completions()` 把相同事件当作协议错误返回。

因此根因不是 provider、CQE routing 或 BufferPool 生命周期，而是 `drain_completions()` 错误假设等待 send CQE 期间不会收到 recv CQE。双向通信中 send/recv CQE 合法交错，该错误路径既中止 Parent，又使已完成接收的数据无法进入后续消费路径，最终导致对端等待超时。

## 修改文件

- `src/connection.rs`
  - `drain_completions()` 将 `RecvCompleted.bytes` 移入现有 `pending_frames`，继续等待 outstanding send 清零。
  - `wait_for_frame()` 在 deadline 检查和新一轮 polling 前优先检查 pending FIFO。
- `docs/m3-two-node-pingpong-fix-status.md`
  - 记录问题、根因、修改、completion flow 和验证状态。

## Completion flow 变化

修改前：

```text
drain send completions
  -> poll_once()
  -> SendCompleted: outstanding_send--, release TX slot
  -> RecvCompleted: return protocol error
```

修改后：

```text
drain send completions
  -> poll_once()
  -> SendCompleted: outstanding_send--, release TX slot
  -> RecvCompleted:
       registered RX slot -> owned Vec<u8> 已在 poller 内完成
       RX slot 已释放
       move owned Vec<u8> -> pending_frames
  -> continue until outstanding_send == 0

next wait_for_message()
  -> wait_for_frame()
  -> when outstanding_send == 0, pop pending_frames first
  -> decode Message
```

该路径只移动 `Vec<u8>` ownership，不执行第二次 payload copy。`wait_for_frame()` 保留原有“send completion 清零后才交付 receive frame”的同步语义，因此不会提前放宽 TX slot 生命周期约束。

## 本地验证

执行范围严格限于指定命令：

| 命令 | 结果 | 状态说明 |
| --- | --- | --- |
| `cargo fmt` | 通过 | formatting completed |
| `cargo check --features urma` | 未进入 Rust 编译 | build script 找不到 `urma_api.h`；需要安装 UMDK development headers 或设置 `UMDK_INCLUDE_DIR` |
| `cargo test --features urma --no-run` | 未进入 Rust 编译 | 同样因缺少 `urma_api.h` 停止 |

因此当前状态为：

- implemented：是；
- formatted：是；
- feature-on compiled：尚未验证，受本地 UMDK headers 环境阻塞；
- unit tests compiled：尚未验证，受相同环境阻塞；
- real-provider validated：尚未验证；
- real UB two-node regression：由具备目标环境的开发侧执行。
