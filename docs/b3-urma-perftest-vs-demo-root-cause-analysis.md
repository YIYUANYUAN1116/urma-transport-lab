# B3: `urma_perftest send_bw` vs Rust URMA benchmark root-cause analysis

> Analysis date: 2026-08-19  
> `urma-transport-lab`: `feac8e6ea5c3138d68d670879019a95a10c4fe81`  
> UMDK: `3bfff198329a497ed49e53bd5585c34bcb7c9d88`  
> Scope: source analysis only; no benchmark code was changed and no new provider experiment was run.

Evidence labels used below:

- `[源码确认]`: confirmed in the current source trees named above;
- `[实验确认]`: supplied real-provider result or an existing project result;
- `[架构推断]`: mechanism inferred from confirmed code and measurements, but not yet isolated experimentally;
- `[待验证]`: requires a new controlled run.

## 1. Executive Summary

The current evidence does **not** support attributing 1.67 MiB/s to UB, UDMA, SEND/RECV, Rust itself, CRC, or CQ polling alone.

The most important findings are:

1. **The quoted tests do not use the same transport mode.** `[源码确认]` The shown `urma_perftest` command omits `-p`; its default is `URMA_TM_RM`. The Rust shim creates both JFS and JFR as `URMA_TM_RC`. Thus 46,827.7 “MB/s” and 1.67 MiB/s are proof that the device/provider can run SEND/RECV quickly, but they are not yet an apples-to-apples RM/RC comparison. An RC perftest run (`-p 1`) is Experiment 0.
2. **Perftest keeps a much deeper transport pipeline.** `[源码确认]` It fills the JFS to 128 outstanding WRs, preposts 512 RECV WRs, immediately replenishes each completed receive, and never sleeps in the default hot loop. The Rust data phase has only 8 TX slots and 8 RX slots.
3. **`CQ Moderation = 100` is true selective send completion.** `[源码确认]` Perftest sets `wr->flag.bs.complete_enable` only on every 100th SEND (plus the finite-test tail). One successful send CQE advances its completion counter by 100. It is not merely “poll 100 ordinary CQEs at once.” The Rust shim unconditionally sets `complete_enable = 1`, allocates and tracks one WR object per SEND, and requires one CQE before reclaiming each TX slot.
4. **The Rust receiver can transiently empty its entire RQ.** `[源码确认]` Its poller routes up to 16 CQEs before returning; with only 8 RX slots, it can consume/copy/release all 8 posted receives before application code begins reposting them one by one. Repost is correctly done before decode/CRC/sink processing, but the batch boundary still creates a zero-credit interval. Perftest drains at most 16 from a 512-entry RQ, so the same pattern leaves hundreds of receives posted.
5. **The observed rate is latency-shaped, not memcpy-shaped.** `[实验确认]` 1.673647 MiB/s at a 32 KiB payload is about 53.6 messages/s. With `window=8`, that corresponds to about 149 ms per effective window. Normal 32 KiB copies, CRC, codec work, `calloc/free`, a hash lookup, or an FFI call do not individually predict a roughly 150 ms window service time. `[架构推断]` The leading mechanism is a shallow, fully drained receive/send pipeline entering RNR/retry or another RC/provider feedback delay; it must be confirmed with queue-depth and single-window timing experiments.
6. **There are nevertheless severe hot-path overheads in the demo.** `[源码确认]` A Data message performs four payload-sized parent copies, two child copies, two child CRC scans, one C `calloc` per SEND/RECV, one `free` per CQE, HashMap routing, and repeated Rust `Vec` allocation even for empty JFC polls. These can matter once the transport is flowing, but they do not credibly explain a roughly 28,000x gap by themselves.

The most defensible current conclusion is therefore:

> `[架构推断]` The 1.67 MiB/s result is primarily a queue/feedback-path pathology in the current RC demo configuration, most likely exposed by the combination of only eight receive credits, only eight send slots, and one signaled completion per SEND. Application copies and shim bookkeeping are secondary throughput costs. The exact share of RQ starvation/RNR versus an RM/RC provider difference remains `[待验证]`.

The recommended next step is not a broad refactor. First run an RC-matched perftest, then sweep TX/RX depth independently under busy polling while recording one-window timing and provider RNR/retry counters. Only after that should selective completion or a raw Rust mode be implemented.

## 2. Experiment Facts

### 2.1 Supplied real-provider facts

| Fact | Evidence |
|---|---|
| TCP userspace, 256 MiB, 32 KiB, window 8: 1284.76 MiB/s | `[实验确认]` |
| Rust URMA busy poll, same payload/window: best about 1.67 MiB/s | `[实验确认]` |
| Rust accounting closes: `send_post == send_cqe`, `recv_post == recv_cqe`, no CQE error, integrity true, peak outstanding 8 | `[实验确认]` |
| Sleep and JFCE policies reduce CPU but also reduce throughput; busy poll remains slow | `[实验确认]` |
| `urma_perftest send_bw`, UB/UDMA, 64 KiB: about 46,827.7 displayed MB/s and 0.749243 Mpps | `[实验确认]` |

Perftest labels its default binary unit as `MB/sec`, but `PERFTEST_BW_MB` is `2^20` (`perftest_parameters.h:99-101`). `[源码确认]` The displayed 46,827.7 is therefore approximately 46,827.7 MiB/s (45.73 GiB/s), not decimal MB/s. Its message-rate figure independently gives roughly 49.1 GB/s decimal. This unit naming detail does not alter the order-of-magnitude conclusion.

### 2.2 Derived rate facts

For the Rust run:

```text
messages       = 256 MiB / 32 KiB = 8192
message rate   = 8192 / 152.9 s ≈ 53.6 messages/s
window service = 8 / 53.6 ≈ 149 ms
```

For perftest:

```text
message rate ≈ 749,243 messages/s
```

The message-rate ratio is about 14,000x. The byte-rate ratio is about 28,000x because perftest messages are twice as large. `[实验确认 + 算术推导]`

### 2.3 Critical comparison caveat

`perftest_parameters.c:335-336` defaults bandwidth tests to JFS depth 128 and transport mode `URMA_TM_RM`; the CLI help documents `-p 0` as RM and `-p 1` as RC (`perftest_parameters.c:131`). The supplied output also explicitly says `URMA_TM_RM`. The Rust shim sets `jfs_cfg.trans_mode = URMA_TM_RC` and `jfr_cfg.trans_mode = URMA_TM_RC` (`src/ffi/shim.c:529-545`). `[源码确认]`

Consequently:

- `[实验确认]` UDMA and URMA SEND/RECV are capable of high throughput in this environment.
- `[待验证]` The same perftest hot path reaches comparable throughput in RC mode.
- It would be incorrect to use the existing RM result alone to rule out every RC setup/provider issue.

## 3. `urma_perftest send_bw` Hot Path

### 3.1 Call chain

The relevant call chain is:

```text
urma_perftest.c:perftest_parse_args
-> resource/context/JFC/JFR/Jetty setup
-> urma_perftest.c:run_test
-> perftest_run_test.c:run_send_bw
-> run_send_bw_one_size
-> prepare_jfs_wr (client)
-> prepare_jfr_wr (server)
-> run_send_bw_once
-> run_once_bw (client) / run_once_bw_recv (server)
```

`run_send_bw_once()` chooses `run_once_bw()` for the client (`server_ip != NULL`) and `run_once_bw_recv()` for the server (`perftest_run_test.c:3447-3484`). `[源码确认]`

### 3.2 JFS depth 128: actual sliding window

The default `jfs_post_list` is 1, not 128 (`perftest_parameters.c:319`). `prepare_jfs_wr()` therefore preconstructs one reusable WR and its SGE per Jetty, not 128 distinct WR structs (`perftest_run_test.c:1165-1191`). `[源码确认]`

The client loop is nevertheless a real 128-outstanding sliding window:

```text
outstanding = scnt - ccnt
while outstanding + jfs_post_list <= jfs_depth:
    post one WR/list
    scnt += jfs_post_list
poll up to 16 send CQEs
for each CQE:
    ccnt += cq_mod
repeat
```

This is implemented at `perftest_run_test.c:1871-2024`. It fills the queue until `outstanding == 128`, polls, accounts the completed group, and immediately fills the newly available space. It does not post 128 and then drain the queue to zero. `[源码确认]`

The provider consumes the WR/SGE metadata synchronously while encoding the WQE. UDMA walks a linked WR list, writes WQEs, stores `user_ctx`, rings one doorbell for that API call/list, and returns (`udma_u_jfs.c:1019-1057`). With the default post-list of one, perftest still makes one URMA post call and normally one doorbell update per message. `[源码确认]`

### 3.3 CQ Moderation 100: selective completion

The definitive path is:

```text
init_jfs_wr_base()
  wr->flag.bs.complete_enable = ((jfs_wr_index + 1) % cq_mod == 0)

run_once_bw(), jfs_post_list == 1
  toggle complete_enable so SEND #100, #200, ... is signaled
  ensure the finite-test tail is signaled

UDMA udma_set_sqe()
  wqe_ctl->flag = wr->flag.value

poll one successful CQE
  ccnt += cq_mod
```

Source locations are `perftest_run_test.c:790-812`, `1896-1899`, `1954-1958`, `2004-2006`, and `udma_u_jfs.c:938-953`. `[源码确认]`

Therefore `CQ Moderation = 100` means:

- SENDs 1-99 do not request a send CQE;
- SEND 100 requests one;
- the CQE for SEND 100 is used by perftest to retire/account the ordered group of 100;
- receive SENDs still produce receive CQEs one per message;
- this is unrelated to `jfs_post_list`, whose default remains 1;
- poll batching is separate and is 16 CQEs per `urma_poll_jfc()` call.

Perftest itself relies on the signaled completion as the completion frontier for preceding WRs. `[源码确认]` For a future Rust implementation, reclaiming preceding dynamic TX slots on that frontier is `[架构推断]` until success/error ordering and provider behavior are explicitly validated for the chosen RC configuration. A slot must never be reused merely because its WR was unsignaled.

### 3.4 JFR depth 512 and replenishment

`prepare_jfr_wr()` computes `size_per_jetty = jfr_depth / jfr_post_list`, then posts that WR/list `size_per_jetty` times before the test (`perftest_run_test.c:1270-1339`). With one Jetty, default post-list 1, and depth 512, it preposts 512 receive entries. UB forces shared JFR when needed (`perftest_resources.c:53-61`). `[源码确认]`

The server loop:

1. busy-polls up to 16 receive CQEs;
2. counts each receive;
3. immediately reposts the consumed receive WR/list inside the CQE loop;
4. repeats polling until the current poll is empty.

See `perftest_run_test.c:2074-2263`. `[源码确认]`

The optional explicit credit protocol is disabled by default (`perftest_parameters.c:365`). Thus the sender is not gated by an application credit message in this run. The effective receive protection is the deep, continuously replenished 512-entry RQ plus transport RNR behavior. `[源码确认]`

### 3.5 JFC depth 4096 and polling

The bandwidth default is `8 * 512 = 4096` (`perftest_parameters.h:24-38`). Separate send and receive JFCs are created at that depth (`perftest_resources.c:345-399`). The hot path polls at most 16 records per call (`PERFTEST_POLL_BATCH`, `perftest_run_test.c:34`). `[源码确认]`

Default `use_jfce` is false (`perftest_parameters.c:310-312`). Consequently:

- client fills the send queue, then directly polls send JFC;
- server repeatedly polls receive JFC until empty;
- an empty poll causes the outer loop to continue;
- no sleep, `yield`, JFCE wait, or syscall is present in the default data hot loop.

JFC depth 4096 is generous generic capacity, especially for receive CQEs and multi-Jetty variants. `[架构推断]` It is not by itself the explanation for high bandwidth: the Rust JFC depth 64 exceeds its maximum eight outstanding send or receive WRs and shows no CQ overflow/error.

### 3.6 Buffer and payload behavior

Perftest allocates and registers a buffer split into receive and send halves. For 64 KiB with a normal 4 KiB page, `buf_size` is 64 KiB and total registered length is 128 KiB (`perftest_resources.c:851-955`). `[源码确认]`

At default `sge_num=1`, `-s 65536` becomes exactly one SGE with `len=65536` (`perftest_run_test.c:970-1013`). It is the complete URMA SEND payload. Perftest adds no application header. `[源码确认]`

For a message larger than half a page, the address-rotation condition is false. The default single WR therefore repeatedly sends the same registered 64 KiB region. The receive side also repeatedly posts the same registered receive address. Perftest deliberately tolerates those overlapping receive writes because it never consumes or validates the bytes; this is not evidence that an application may reuse a mutable RX buffer before its exact receive CQE. `[源码确认]`

There is no per-message payload memcpy, encode/decode, owned receive copy, digest, or sink operation. Only WQE metadata is rebuilt by the provider from the reusable WR/SGE on each post. `[源码确认]`

## 4. Rust Benchmark Hot Path

### 4.1 Parent path for one 32 KiB Data message

```text
MemorySource chunk slice
-> payload.to_vec()
-> IntegrationMessageV3::data
-> encode_payload(): payload.clone()
-> encode(): copy header + payload into another Vec
-> BufferPool::write_tx
-> C shim segment_write: memcpy into registered TX slot
-> C shim calloc one urma_lab_wr (WR + SGE)
-> complete_enable = 1
-> urma_post_jetty_send_wr
-> provider encodes WQE and rings doorbell
-> HashMap tracks user_ctx -> WrHandle
-> send CQE
-> HashMap remove, C WR free, TX slot release
```

The source path is `src/urma_benchmark/native.rs:456-523`, `src/message.rs:590-653,710-715`, `src/connection.rs:169-217`, `src/buffer.rs:255-275`, and `src/ffi/shim.c:792-860`. `[源码确认]`

At implementation level this is **four payload-sized parent copies** for a memory source:

1. source slice -> Data body's `Vec` (`payload.to_vec()`);
2. Data body's `Vec` -> `encode_payload()` clone;
3. encoded payload -> complete framed `Vec`;
4. framed `Vec` -> registered TX slot (`memcpy`).

The first three collectively implement “source -> encoded Vec”; the fourth is “encoded Vec -> registered TX.” `[源码确认]`

### 4.2 Child path for one Data message

```text
preposted registered RX slot
-> receive CQE
-> C shim segment_read: memcpy registered RX bytes into Rust Vec
-> release RX slot
-> after poll batch returns: repost one receive
-> decode header
-> payload.to_vec() into MessageBody::Data
-> MemorySink CRC update
-> UrmaReceiveState CRC update
-> drop frame and decoded payload Vecs
```

The path is `src/completion.rs:329-405`, `src/buffer.rs:341-360`, `src/ffi/shim.c:492-499`, `src/urma_benchmark/native.rs:372-410`, `src/message.rs:656-691,764-800`, `src/urma_benchmark.rs:321-356`, and `src/benchmark.rs:589-596`. `[源码确认]`

For the memory sink this is **two child payload copies**:

1. registered RX slot -> owned frame `Vec`;
2. decoded Data payload -> a second owned `Vec`.

Decode does copy the Data payload. `MemorySink` does not retain or copy it, but the bytes are scanned twice by CRC32: once in `MemorySink`, once in `UrmaReceiveState`. `[源码确认]`

### 4.3 Actual send-window behavior

The parent posts until `PipelineTracker.current == window`. When full, it calls `poll_once()` repeatedly. A non-empty poll may return up to 16 send CQEs; all returned CQEs are applied, after which the next source iterations refill the freed slots one at a time. It is a sliding window, not an intentional “post 8, drain to zero, post 8” loop (`src/urma_benchmark/native.rs:500-564`). `[源码确认]`

However, only `max_outstanding_send=8` is recorded. The current statistics cannot prove that the time-average stays near eight, nor reveal how often it reaches zero. `[源码确认]`

Add these counters only as a later minimal instrumentation patch:

```text
outstanding_send_time_ns[0..W]   # preferred time-weighted occupancy histogram
window_zero_transitions
window_full_time_ns
posts_per_refill
cqes_per_refill
```

`outstanding_send_sum/sample_count` is easier but can be biased by a high-frequency empty-poll loop. A time-weighted histogram is more reliable. `[架构推断]`

### 4.4 Receive-credit behavior and zero-credit interval

`receive_credit_target()` computes `min(2 * window, rx_slot_count, remaining)`. With window 8 and eight RX slots, the target is exactly 8 (`src/urma_benchmark.rs:191-203`). `[源码确认]`

The initial eight receives are preposted. On receive completion:

1. `CompletionPoller::poll_active()` asks for up to 16 CQEs;
2. `route()` copies and releases every completed slot before returning the event vector;
3. `UrmaConnection::poll_once()` decrements its internal posted counter for every returned receive;
4. only then does the application loop process the events and call `replenish_credit()` after each one;
5. repost occurs before decode/CRC/sink work.

This ordering is source-confirmed at `src/completion.rs:260-285,329-405`, `src/connection.rs:219-238`, and `src/urma_benchmark/native.rs:386-408`. `[源码确认]`

Thus the good property is that codec/CRC does not delay a particular repost. The bad property is that a batch of all eight completions can reduce the hardware RQ from eight to zero before the first repost happens. `[源码确认]`

UDMA/URMA defines `URMA_CR_RNR_RETRY_CNT_EXC_ERR` for an exhausted remote receive queue, JFS `rnr_retry`, and JFR `min_rnr_timer` (`urma_opcode.h:93-95,150`; `urma_types.h:555-570,648-660`). The typical timer value 12 corresponds to 0.64 ms in the API guide. Both perftest and the Rust shim use the typical retry/timer constants. `[源码确认]`

Whether this demo actually enters RNR, how often, and whether another RC retry/ACK timer explains the roughly 149 ms window cadence are `[待验证]`. `cqe_error == 0` does not exclude successful retries; it only excludes retries that ultimately surface as an error CQE.

### 4.5 Per-message shim/provider cost

| Stage | `urma_perftest` | Rust demo | Evidence |
|---|---|---|---|
| WR/SGE preparation | One WR/SGE allocated and initialized before the test; reused | `calloc` a combined C WR/SGE object for every SEND and every RECV | `[源码确认]` |
| Post | Direct `urma_post_jetty_send_wr`; default list length 1 | Rust -> C shim -> same URMA API | `[源码确认]` |
| Provider lock | Default perftest is not lock-free; UDMA takes a queue spinlock | Same default zero flag, also takes queue spinlock | `[源码确认]` |
| Doorbell | One update per default one-WR post | One update per post | `[源码确认]` |
| Kernel syscall | None in UDMA post fast path | None in UDMA post fast path | `[源码确认]` |
| Send completion | One per 100 SENDs | One per SEND | `[源码确认]` |
| Poll result storage | Preallocated C array of 16 | C stack array of 16, then newly allocated Rust `Vec`s | `[源码确认]` |
| WR retirement | Counter arithmetic; reusable WR remains | HashMap remove, slot-state transitions, C `free` | `[源码确认]` |

The provider path copies WR metadata into the userspace SQ, executes memory barriers, and writes a userspace doorbell/MMIO path (`udma_u_jfs.c:981-1057`). It does not reparse a remote descriptor, allocate provider WR memory, take a process mutex, or issue a syscall per message. `[源码确认]`

The C shim does allocate once per posted WR (`src/ffi/shim.c:792-816`) and frees it only at the matching CQE (`889-904`). The Rust poll wrapper also creates a capacity-16 `Vec` on every JFC poll, including empty polls (`src/ffi/mod.rs:364-399`), while `CompletionPoller::poll_active()` creates another event `Vec` (`src/completion.rs:260-285`). With the historical 605 million mostly empty polls, this is a serious CPU-efficiency defect. `[源码确认 + 实验确认]` It does not, however, establish why successful CQEs arrive only about 54 times/s.

## 5. Queue / Credit / CQ Moderation Comparison

| Dimension | `urma_perftest send_bw` | Rust demo | Likely impact |
|---|---|---|---|
| Transport mode | RM by default for supplied command `[源码确认]` | RC, fixed in shim `[源码确认]` | **P0 confounder**; magnitude `[待验证]` |
| Message size | 65,536-byte complete SEND payload, no header `[源码确认]` | 32,768-byte business payload + 24-byte header `[源码确认]` | About 2x byte-rate at equal message rate; not 10,000x |
| JFS depth | 128 `[源码确认]` | Jetty depth 64, but effective window/TX slots 8 `[源码确认]` | High; 16x more in flight in perftest |
| Average outstanding | Refilled to the 128 limit by source logic `[源码确认]` | Peak 8 only `[实验确认]`; average unknown `[待验证]` | High if demo frequently drains |
| JFR depth / physical credit | 512 preposted and continuously replenished `[源码确认]` | 8 `[源码确认]` | **Very high-priority** RQ starvation/RNR candidate |
| Shared JFR | Forced for UB `[源码确认]` | Shared JFR `[源码确认]` | Same foundation; not the gap |
| JFC depth | 4096 `[源码确认]` | 64 send + 64 receive `[源码确认]` | Low while outstanding <= 8 and no overflow |
| Send completion | One requested per 100, plus tail `[源码确认]` | Every SEND requests a CQE `[源码确认]` | High CPU/bookkeeping and retirement effect; not independently proven 100x bandwidth |
| Receive completion | One per received message `[源码确认]` | One per received message `[源码确认]` | Same fundamental requirement |
| Post list | Default 1 `[源码确认]` | 1 `[源码确认]` | Same call/doorbell granularity |
| Poll batch | 16 `[源码确认]` | 16 `[源码确认]` | Same maximum batch |
| Empty-poll behavior | Busy loop, no JFCE/sleep `[源码确认]` | Busy baseline spun/yielded; current tree is JFCE hybrid after 64 hot polls `[源码确认]`; all measured variants slow `[实验确认]` | Secondary; already experimentally bounded |
| RQ replenishment | Repost within CQE loop, with about 496 spare entries after a full poll batch `[源码确认]` | Repost after whole poll batch returns; a batch can consume all 8 `[源码确认]` | High-priority mechanism difference |

## 6. Buffer / Copy / WR Lifecycle Comparison

| Dimension | `urma_perftest` | Rust demo | Likely impact |
|---|---|---|---|
| Registered memory | About 128 KiB for one 64 KiB send and receive half `[源码确认]` | 16 aligned slots; 589,824 bytes for 32 KiB payload because each slot is 36 KiB `[源码确认]` | Registration size is not a steady-state bottleneck |
| TX buffer | Same immutable registered address reused by all outstanding SENDs `[源码确认]` | Eight distinct mutable TX slots `[源码确认]` | Perftest avoids staging and per-slot ownership |
| Parent payload copies | None per message `[源码确认]` | Four payload-sized copies `[源码确认]` | Usually low-single-digit factor; bandwidth/allocator cost after queue fix |
| RX buffer use | Same address can be reposted; bytes are not read `[源码确认]` | Registered bytes copied to frame Vec, then decode copies Data again `[源码确认]` | Usually low-single-digit factor |
| Codec | None `[源码确认]` | Header encode/decode and multiple Vec allocations `[源码确认]` | Secondary |
| CRC | None `[源码确认]` | Two CRC32 scans on child `[源码确认]` | Secondary; TCP baseline shows application path can exceed 1 GiB/s `[实验确认]` |
| WR lifetime | Static WR/SGE reused `[源码确认]` | C `calloc` per post, `free` per CQE `[源码确认]` | Meaningful message-rate cost, not a 150 ms timer |
| TX reclaim | CQE frontier accounts 100 ordered WRs `[源码确认]` | Exact CQE releases exactly one slot `[源码确认]` | Major completion-pressure difference |
| Routing | Integer Jetty id and counters `[源码确认]` | Encoded token + HashMap + slot state machine `[源码确认]` | Secondary correctness overhead |

The two tools measure different things:

- perftest is a transport ceiling: fixed registered bytes, no payload semantics;
- the Rust demo is an application-semantic path: framing, ownership, validation, and sink processing.

That distinction can explain a moderate gap after transport flow is healthy. It does not make 1.67 MiB/s an expected result.

## 7. `max_msg_size = 65536`

### 7.1 What perftest sends

`-s 65536` sets `cfg->size`. With the default single SGE, `init_send_jfs_wr_sg()` sets that SGE length to exactly 65,536. No perftest application header is added, so the provider sees a SEND WR whose total payload is 65,536 bytes. `[源码确认]`

### 7.2 Why a Rust 64 KiB chunk is too large

The Rust v3 header is 24 bytes (`DATA_HEADER_LEN`), so:

```text
business chunk = 65536
SEND payload   = 65536 + 24 = 65560
```

That exceeds the reported 65,536-byte device maximum. The registered slot then rounds up to 69,632 bytes because of 4 KiB alignment, but the provider-visible WR length is 65,560, not the entire slot capacity. `[源码确认]`

The largest theoretical Data business payload under this capability is therefore 65,512 bytes, subject to any additional provider restrictions:

```text
65536 - 24 = 65512
```

### 7.3 Is the limit per SGE or per WR?

The public capability is documented as “max message size supported by the device for transmission,” while `max_jfs_sge` separately limits SGE count (`urma_types.h:269`; URMA User Guide device-capability table). UDMA gathers all SEND SGEs into one WQE/message and computes total completion length as the sum of SGE lengths (`udma_u_jfs.c:1177-1211`). `[源码确认]`

Therefore multiple SGEs are **not a supported way to bypass `max_msg_size`**. They can scatter/gather one legal message but do not turn it into several messages. `[架构推断 strongly supported by source/API]` The current UDMA userspace post path does not visibly compare the sum against `dev_cap.max_msg_size`; enforcement may occur in hardware/firmware or as a completion error. That exact failure point is `[待验证]`.

The capability is read from the device's `max_msg_size` sysfs attribute or command response (`urma_device.c:307`, `urma_cmd_tlv.c:1255`). No per-JFS or per-Jetty configuration in the inspected source raises it. Whether firmware/device configuration can change it is `[待环境/firmware确认]`; the application must treat the queried value as authoritative.

### 7.4 Clear validation issue found

`validate_urma_case()` compares the **aligned slot capacity** to `provider_max_message_size` (`src/urma_benchmark.rs:98-105`). The provider actually receives the encoded frame length passed to `post_send`, not the slot capacity. `[源码确认]`

This is a configuration-validation bug in principle:

- file/function: `src/urma_benchmark.rs`, `validate_urma_case()`;
- issue: compares `slot_size` rather than maximum encoded WR length;
- effect: can reject a legal frame when alignment makes its backing slot larger than a non-aligned provider limit;
- impact on the observed 32 KiB run: none;
- impact on `chunk=65536`: the frame is genuinely too large anyway, so fixing this comparison would not make that case legal.

Per task scope, this is recorded only and not fixed.

## 8. Root-Cause Ranking

### P0: First remove the RM-vs-RC comparison confounder

`[源码确认]` The modes differ. `[待验证]` Run the same native test with RC before assigning any portion of the gap to the Rust implementation. If native RC is also slow, the investigation moves to RC setup/provider/connection parameters before Rust hot-path work.

### P1: Shallow RQ plus batch depletion, causing RNR/retry or RC feedback stalls

Confidence: **highest mechanism candidate, not yet experimentally proven**.

Supporting evidence:

- physical receive depth is only 8 versus 512;
- one poll batch can consume all 8 before any repost;
- no error CQE excludes only terminal failure, not successful RNR retries;
- observed throughput implies roughly 149 ms per eight-message window, which looks like a delayed feedback/retry cadence rather than memory-copy bandwidth.

Disproof condition: RX depth 64/128/512 and RNR counters/timing show no change and no retries.

### P1: Shallow signaled-send pipeline and completion-driven slot ownership

Confidence: **source-confirmed structural bottleneck; magnitude awaiting experiment**.

The demo has at most eight sends and requires eight CQEs to recycle them. Perftest has 128 outstanding and usually one send CQE per 100 WRs. This changes both bandwidth-delay-product coverage and CPU/accounting pressure. But `window=8` alone should not produce only 54 messages/s on a healthy low-latency path, so it is probably part of a combination rather than the sole root cause.

### P2: Per-poll and per-WR allocation/bookkeeping

Confidence: **source-confirmed overhead**.

The shim allocates/frees every WR, the poller uses a HashMap, and empty polling repeatedly allocates Rust Vecs. These explain high CPU and reduce achievable message rate. They should be removed in a later raw mode or preallocated WR experiment, but they do not independently predict the measured 149 ms window cadence.

### P3: Codec, copies, and double CRC

Confidence: **source-confirmed overhead, low probability as primary cause**.

Six payload copies across both endpoints and two CRC scans are unnecessary for a transport ceiling test. Yet the same machines' TCP userspace path, including memory source/sink semantics, exceeds 1.2 GiB/s. Even allowing several-fold overhead, this cannot explain a roughly 28,000x difference from perftest.

### P3: Polling policy

Confidence: **experimentally ruled out as the sole or main root cause**.

Busy polling improved over sleep/JFCE but still achieved only about 1.67 MiB/s. Polling policy changes the symptom and CPU consumption; it does not create the missing four orders of magnitude.

## 9. What Does NOT Explain the Gap

| Factor | Plausible isolated scale | Why it is insufficient |
|---|---:|---|
| 64 KiB vs 32 KiB message | About 2x byte rate at equal pps | Leaves about 14,000x message-rate gap |
| Every SEND signaled | Potentially large CQ/CPU reduction; perftest requests about 100x fewer send CQEs | Receiver still handles one CQE/message at 0.749 Mpps; not evidence for a 54 pps ceiling |
| Window 8 vs 128 | Up to 16x more BDP coverage in perftest | Window 8 still implies only 149 ms/window at measured rate; needs a latency/retry cause |
| Extra memcpy | Commonly 1.x to a few x at these sizes | 32 KiB copies are microsecond/sub-microsecond-scale operations on a machine that runs the TCP path above 1 GiB/s |
| Codec/CRC | Commonly 1.x to a few x | TCP baseline and CPU capabilities rule out four orders of magnitude |
| Rust-to-C FFI call | Small constant overhead | Both paths ultimately call the same userspace provider; no syscall occurs per post |
| Queue spinlock | Usually modest in this single-thread owner design | Both configurations use the default locked provider queues |
| JFC 64 vs 4096 | None while occupancy is <=8 and no CQ overflow | Correctness counters and zero CQE errors show no overflow symptom |
| CQ sleep/JFCE alone | Measured 0.95-1.12 vs 1.67 MiB/s | Busy poll remains catastrophically slow |

No single row above accounts for the full gap. `[架构推断]` A multiplicative combination is credible only when a large latency event—most plausibly RQ starvation/RNR or an RC-mode path difference—is present. Without such an event, multiplying ordinary CPU overhead estimates is not a sound explanation.

## 10. Minimal Experiments

All experiments should use release builds, the same nodes/device/EID, fixed CPU/NUMA placement where possible, and raw result retention. Start with busy polling so JFCE/scheduler behavior is not a variable.

### Experiment 0: Match transport mode before comparing implementations

Priority: **highest**.

Run native perftest in both modes with all other displayed parameters held constant:

```bash
# Existing/default RM
urma_perftest send_bw -d udmac0d1e2 -S 90.91.177.158 -s 65536 -D10 -p 0

# Match Rust RC
urma_perftest send_bw -d udmac0d1e2 -S 90.91.177.158 -s 65536 -D10 -p 1
```

Record full client/server headers and results. If RC is fast, the Rust queue/ownership path becomes the focus. If RC collapses, first compare perftest and shim RC creation/bind/order/retry attributes and provider logs.

Also run native RC with `-s 32768` to remove message-size as a variable.

### Experiment A: Separate TX depth from RX depth

Priority: **highest after Experiment 0**.

Do not change both axes in a single first run. Use a small matrix:

```text
A1: TX/window = 8;   RX = 8, 64, 128, 512
A2: RX = first non-starving value; TX/window = 8, 32, 64 (then 128 if Jetty depth is raised safely)
```

Keep every SEND signaled and retain current codec/copies. This isolates receive starvation before introducing selective-completion lifetime changes.

Record:

```text
throughput and message rate
send/recv post and CQE counts
time-weighted outstanding-send histogram
minimum/current RQ credit and zero-credit transitions
completion batch histogram
provider RNR/retry/ACK-timeout counters or logs
```

Interpretation:

- large gain from RX 8 -> 64 with TX fixed: RQ depletion/RNR confirmed;
- gain only from TX depth: insufficient BDP/completion frontier dominates;
- no gain from either: proceed immediately to raw mode and post/CQE latency tracing.

### Experiment B: One-window latency trace

Priority: **same diagnostic round as A**.

Use 32 KiB, window 8, and at least several windows rather than only one, because the first window may include warm-up. On each endpoint use only its own monotonic clock:

```text
Parent: post #1, post #8, first send CQE, eighth send CQE
Child:  first recv CQE, eighth recv CQE, first repost, eighth repost
Both:   RQ-zero transition and next CQE after refill
```

Do not subtract Parent timestamps from Child timestamps unless clocks have been independently synchronized and bounded. The key metric is local `post -> CQE` and local `poll batch -> repost` duration.

This experiment should explain where the implied roughly 149 ms/window is spent.

### Experiment C: Raw Rust transport mode

Priority: **after queue timing identifies or excludes RNR**.

Add a minimal benchmark mode, not a production abstraction:

```text
preallocated/preconstructed WR and SGE storage
fixed registered payload reused without mutation
no IntegrationMessage encode/decode
no CRC
no owned RX payload copy (count completion length only)
preallocated completion arrays; no Vec allocation on empty poll
simple indexed completion bookkeeping instead of HashMap
```

Keep SEND/RECV, shared JFR, single connection, and safe shutdown. Compare raw Rust RC directly with perftest RC.

- If raw Rust becomes fast, the FFI/provider foundation is healthy and costs can be added back one at a time.
- If it remains in MiB/s, trace shim/post/provider/RC configuration before touching application framing.

### Experiment D: Selective send completion

Priority: **only after queue semantics are measured**.

Test moderation 1, 8, 32, and 100. Do not merely clear `complete_enable` in the existing shim: the current `WrHandle`, HashMap entry, and TX slot are reclaimed only by an exact CQE, so unsignaled WRs would leak/stall.

A safe prototype needs:

1. monotonically ordered send sequence numbers;
2. one signaled frontier WR per group and a signaled tail;
3. retention of all WR metadata and mutable TX slots through that frontier CQE;
4. reclaim of the completed prefix only after the signaled CQE succeeds;
5. explicit error/flush handling that does not assume every unsignaled WR succeeded silently.

For a perftest-like fixed immutable payload, the same registered bytes may be referenced concurrently, but this is not equivalent to safe reuse of dynamic application TX slots.

### Experiment E: Attribute/counter audit

Priority: **parallel read-only support for A/B**.

Capture or query:

```text
actual Jetty/JFS/JFR depth
actual trans_mode and order_type
rnr_retry and min_rnr_timer
err_timeout / retry counters
JFS PI/CI and JFR PI/CI if supported
RQ-empty/RNR and ACK-timeout hardware/provider counters
```

Do not infer “no RNR” from `cqe_error=0`; successful retries do not have to become error CQEs.

## 11. Recommended Next Step

Perform exactly this sequence:

1. run native `urma_perftest send_bw` with `-p 1` at 64 KiB and 32 KiB;
2. run Rust busy-poll with TX/window fixed at 8 and RX depth 64, then 128;
3. capture the one-window timestamps and RNR/retry counters in the same runs;
4. only if the 149 ms cadence remains without RNR, build the raw Rust mode;
5. only after the raw/depth result, prototype selective send completion with grouped lifetime accounting.

This ordering distinguishes four possible outcomes with minimal code churn:

```text
native RC slow
-> RC/provider/setup issue

native RC fast + deeper RX fixes Rust
-> receive starvation/RNR root cause

native RC fast + deeper TX fixes Rust
-> shallow signaled pipeline/BDP root cause

native RC fast + queue depths do not help + raw Rust helps
-> codec/copy/allocation/bookkeeping combination

native RC fast + raw Rust remains slow
-> shim/provider call or RC resource-attribute mismatch
```

At present, the second branch is the leading hypothesis, but it remains `[待验证]`. The source-confirmed result is that perftest's high throughput comes from a deep, continuously replenished RQ, a 128-WR sliding send window, selective send completion, reusable registered bytes/WRs, and allocation-free busy-poll hot loops—none of which is reproduced by the current application-semantic Rust benchmark.
