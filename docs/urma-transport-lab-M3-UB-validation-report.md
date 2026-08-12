# URMA Transport Lab M3 UB 环境验证报告

## 1. 目标

验证 URMA transport foundation demo 在真实 UB 环境中的可运行性，确认：

- URMA runtime 初始化
- UB device 发现
- Context 创建
- Jetty 创建
- 双端 descriptor exchange
- Jetty bind
- SEND/RECV 数据通路
- Completion CQE
- Payload 校验

为后续 Dragonfly 数据面接入 URMA 提供基础验证。

---

## 2. 环境

| 项目 | 信息 |
|---|---|
| OS | openEuler 24.03 LTS SP4 |
| 架构 | aarch64 |
| UMDK | 已安装 |
| liburma | /usr/lib64/liburma.so |
| URMA header | /usr/include/ub/umdk/urma |
| Provider | UDMA |
| 测试设备 | udmac0d1e2 |

---

## 3. 编译验证

环境变量：

```bash
export UMDK_INCLUDE_DIR=/usr/include/ub/umdk/urma
export UMDK_LIB_DIR=/usr/lib64
export LIBCLANG_PATH=/usr/lib64
```

执行：

```bash
cargo build --features urma
```

结果：

```text
Finished `dev` profile
```

编译通过。

---

## 4. 问题定位与修复

### 4.1 runtime_open failed -19

现象：

```text
liburma operation runtime_open failed with status -19
```

原因：

demo 默认设备：

```text
urma0
```

但 UB 环境实际 URMA device 为：

```text
udmac0d1e2
```

修改运行参数：

```bash
./target/debug/parent udmac0d1e2
```

后 Context 创建成功。

---

### 4.2 create_jetty failed -22

现象：

```text
ContextReady
create_jetty failed with status -22
```

原因：

UB transport 对 JFR 模式有特殊要求。

UDMA provider：

```text
transport_type = URMA_TRANSPORT_UB
```

UB 设备要求：

```text
URMA_SHARE_JFR
```

原实现使用：

```text
URMA_NO_SHARE_JFR
```

导致 liburma 参数校验失败。

修改：

- 创建独立 JFR
- Jetty 使用 shared JFR
- 增加对应资源释放逻辑

修改后：

```text
ContextReady -> JettyCreated
```

成功。

---

## 5. M3 完整运行流程

### Parent

```text
ContextReady

ContextReady -> JettyCreated

listening on 127.0.0.1:19090

accepted child

HELLO received

descriptor exchanged

Bound

READY
```

### Child

```text
ContextReady

ContextReady -> JettyCreated

connected to parent

HELLO sent

descriptor received

Bound

READY
```

---

## 6. 数据通路验证

最终输出：

Parent：

```json
{
 "role":"parent",
 "rounds":1,
 "send_post":1,
 "send_cqe":1,
 "recv_cqe":1,
 "payload_ok":true
}
```

Child：

```json
{
 "role":"child",
 "rounds":1,
 "send_post":1,
 "send_cqe":1,
 "recv_cqe":1,
 "payload_ok":true
}
```

说明：

- SEND WR 提交成功
- SEND 完成事件产生
- RECV 完成事件产生
- 数据内容校验成功

---

## 7. 数据路径

```text
Application

    |
    v

URMA Send API
(post_send)

    |
    v

JFS

    |
    v

UB Transport

    |
    v

Remote JFR

    |
    v

RX Buffer

    |
    v

JFC

    |
    v

CQE Poll

    |
    v

Application
```

---

## 8. 当前结论

M3 验证通过。

已确认：

- URMA 可以在真实 UB 环境运行
- UDMA provider 可以创建 Context
- UB shared JFR 约束已验证
- Jetty 建链流程可运行
- SEND/RECV 数据路径可工作

---

## 9. 后续方向

下一阶段：

1. 将 demo message 模型替换为 Dragonfly Piece Request/Data 模型
2. 设计 UrmaDownloader 数据通路
3. 将 TCP/QUIC 数据路径映射到 URMA SEND/RECV
4. 验证 Piece buffer、CQE、缓存生命周期
5. 开展 TCP vs URMA 性能测试
