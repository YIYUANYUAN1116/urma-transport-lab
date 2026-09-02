# RC SEND_IMM correctness validation

Status: implemented and compile-tested; real-provider validation is pending.

The `send_imm_probe` binary validates sender-provided 64-bit immediate data on
the existing RC duplex Jetty and shared-JFR transport foundation. It is a
correctness probe, not a throughput benchmark.

The Parent preposts all receive WRs before releasing the Child through an OOB
barrier. The Child then posts interleaved `SEND_IMM` messages. Every receive CQE
must report `SEND_WITH_IMM`, preserve the full 64-bit value, and bind the
payload to exactly one expected identity. Local RX slots and remote immediate
identities are tracked independently.

The identity set includes:

```text
0x00000001_00000002
0x12345678_9abcdef0
0xfedcba98_76543210
```

and four interleaved transfer-id namespaces. Duplicate, missing, unknown,
ordinary-SEND, payload-mismatch, CQE-error, and repeated-RX-slot evidence all
fail the run.

Build on both nodes:

```bash
UMDK_INCLUDE_DIR=/usr/include/ub/umdk/urma \
UMDK_LIB_DIR=/usr/lib64 \
cargo build --release --features urma --bin send_imm_probe
```

Start Parent first:

```bash
LD_LIBRARY_PATH=/usr/lib64 \
./target/release/send_imm_probe \
  parent udmac0d1e2 1 0.0.0.0:19091 64
```

Then start Child, replacing the address with the Parent management IP:

```bash
LD_LIBRARY_PATH=/usr/lib64 \
./target/release/send_imm_probe \
  child udmac0d1e2 1 90.91.177.158:19091 64
```

Both processes must print a single JSON result with `"state":"passed"` and
exit zero. Parent additionally requires:

```text
transportMode=RC
opcode=SEND_WITH_IMM
messages=distinctImmediate=distinctRxSlots=recvCqe
full64BitIdentity=true
payloadBinding=true
cqeErrors=0
```

Repeat in the reverse node direction. A larger follow-up run may use 256 or
1024 messages if the device's JFS/JFR/JFC depth supports it; the probe rejects
counts above 4096.
