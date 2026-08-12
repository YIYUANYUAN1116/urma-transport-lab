# 错误1：
``` bash
[root@controller urma-transport-lab]# ./target/debug/parent udmac0d1e2
parent: ContextReady
parent: liburma operation create_jetty failed with status -22
``` 
解决：

## 原因定位

这个报错是真实的 UMDK (liburma) 返回 -22 （即 EINVAL ），发生在 urma\_create\_jetty 的参数校验阶段，与你的 lab 代码里的 Jetty 配置方式直接相关。

### 触发路径

运行的设备 udmac0d1e2 是 UDMA provider 设备。UDMA 提供者把自己的 transport\_type 声明为 URMA\_TRANSPORT\_UB ：

- udma\_u\_ops.c 中 .transport\_type = URMA\_TRANSPORT\_UB
  而 liburma 在 urma\_create\_jetty 里有一道硬校验，UB 设备 必须使用 shared JFR ：

- urma\_cp\_api.c#L1545 ：

```
if (jetty_cfg->flag.bs.share_jfr == URMA_NO_SHARE_JFR && ctx->dev->type == 
URMA_TRANSPORT_UB) {
    URMA_LOG_ERR("UB dev should use share jfr!\n");
    return -1;   // 最终 errno=EINVAL → -22
}
```

 lab shim 在 shim.c#L407 把 Jetty 配成了 share\_jfr = URMA\_NO\_SHARE\_JFR （内嵌非共享 JFR），这正好踩中了上面的校验，导致 urma\_create\_jetty 返回 NULL、errno=EINVAL，shim 再通过 urma\_lab\_pointer\_error(-EIO) 返回 -22 。

### 佐证

官方示例/工具在 UB 设备上全部用 URMA\_SHARE\_JFR ：

- urma\_sample.c#L270 .flag.bs.share\_jfr = 1 ，并先 urma\_create\_jfr 再 shared.jfr 引用
- perftest\_mgmt\_ub.c#L316 share\_jfr = URMA\_SHARE\_JFR
- perftest\_resources.c#L55 明确提示 URMA\_TRANSPORT\_UB only support share\_jfr
  （顺带：URMA 日志里会打印 UB dev should use share jfr! ，可用 dmesg /urma log 核对。）

### 结论

不是环境/硬件问题，而是 lab 的 C shim 用错了 UB 设备不支持的 URMA\_NO\_SHARE\_JFR 模式。 修复方向 ：在 shim.c 的 urma\_lab\_jetty\_create 中改成「先 urma\_create\_jfr 创建 JFR，再用 URMA\_SHARE\_JFR + jetty\_cfg.shared.jfr = jfr 构造 Jetty」，并相应增加 JFR 的释放逻辑。


# 成功结果
``` bash
[root@controller urma-transport-lab]# ./target/debug/parent udmac0d1e2

parent: ContextReady
M2 connection: ContextReady -> JettyCreated
parent: listening on 127.0.0.1:19090
parent: accepted child 127.0.0.1:52450
parent: HELLO received and capability validated
M2 connection: JettyCreated -> DescriptorExchanged
M2 connection: DescriptorExchanged -> Bound
parent: Child descriptor imported, Bound, RX posted
parent: descriptor sent
parent: child reported Bound
parent: READY_ACK sent
M2 connection: Bound -> Ready
{"role":"parent","rounds":1,"send_post":1,"send_cqe":1,"recv_cqe":1,"payload_ok":true,"elapsed_us":47598}
M2 connection: Ready -> Closed

[root@controller urma-transport-lab]# cd /home/y30083740/dragonfly/demo/urma-transport-lab

export UMDK_INCLUDE_DIR=/usr/include/ub/umdk/urma
export UMDK_LIB_DIR=/usr/lib64
export LIBCLANG_PATH=/usr/lib64

./target/debug/child udmac0d1e2

child: ContextReady
M2 connection: ContextReady -> JettyCreated
child: connected to parent 127.0.0.1:19090
M2 connection: JettyCreated -> DescriptorExchanged
child: HELLO sent
child: descriptor received and validated
M2 connection: DescriptorExchanged -> Bound
child: descriptor imported; Jetty Bound; RX posted
child: READY_ACK received
M2 connection: Bound -> Ready
{"role":"child","rounds":1,"send_post":1,"send_cqe":1,"recv_cqe":1,"payload_ok":true,"elapsed_us":48}
M2 connection: Ready -> Closed

```