# 代码结构

``` text
urma-transport-lab
├── src
│   ├── bin
│   │   ├── parent.rs        # parent进程入口
│   │   └── child.rs         # child进程入口
│   │
│   ├── lib.rs               # 对外模块入口
│   │
│   ├── runtime.rs            # URMA运行时管理
│   ├── connection.rs         # 连接抽象（核心）
│   ├── jetty.rs              # Jetty/JFS/JFR封装
│   ├── jfc.rs                # Completion Queue封装
│   ├── buffer.rs             # Buffer池
│   ├── completion.rs         # CQE处理
│   ├── wr.rs                 # user_ctx编码
│   ├── message.rs            # 消息定义
│   ├── oob.rs                # TCP握手
│   ├── error.rs
│   │
│   └── ffi
│       ├── mod.rs            # Rust FFI封装
│       ├── wrapper.h         # bindgen入口
│       ├── shim.h            # C接口声明
│       └── shim.c            # 调liburma
```

# 脑图

``` text
              parent.rs
                  |
              child.rs
                  |
                  v

            runtime.rs
                  |
                  v

              FFI层
        ffi/mod.rs + shim.c
                  |
                  v
              liburma


                  |
                  v

          create_connection()

                  |
        +---------+---------+
        |                   |
      Jetty              BufferPool
        |
   +----+----+
   |         |
  JFS       JFR
   |         |
send_jfc   recv_jfc
   |
   v

CompletionPoller

   |
   v

CompletionEvent

   |
   v

Dragonfly未来接入点
```

# M4

```text
Child                              Parent

post_recv      ← handshake
   |
   |              post_send(Request)
   |=============================>

Parent recv CQE(Request)


Child post_recv ← handshake 已有
                              post_send(Metadata)
      <=============================

recv CQE(Metadata)
copy
release RX
post_recv          ← 为 Data0 准备


                              post_send(Data0)
      <=============================

recv CQE(Data0)
copy
release RX
post_recv          ← 为 Data1 准备


                              post_send(Data1)
      <=============================

recv CQE(Data1)
copy
release RX
post_recv          ← 为 End 准备


                              post_send(End)
      <=============================

recv CQE(End)
copy
release RX
不再 post_recv

```

```text
                 ┌───────────────┐
                 │ AwaitMetadata │
                 └───────┬───────┘
                         │ Metadata
                         v
                 ┌───────────────┐
                 │   Receiving   │
                 └───────┬───────┘
                         │
             ┌───────────┴───────────┐
             │                       │
           Data                     End
             │                       │
             v                       v
 sequence / length check       chunk count
 write file                    length
 SHA256 update                 digest
 received += len               verify
 seq++                         │
             │                 v
             └────────> ┌──────────┐
                        │ Complete │
                        └──────────┘

```
