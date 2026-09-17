# RM READ real-provider probe

## Status

The lab now contains an isolated two-process RM/RTP READ probe. It does not
enable READ in Dragonfly production code. The implementation has passed the
feature-off unit suite for the new pure helpers and a feature-on compile check
against the UMDK source headers. It has not yet been run on a real provider.

The probe deliberately does not repeat the 8 KiB policy crossover or the
maximum READ-size boundary. Existing `urma_perftest` evidence already covers
those decisions. Its job is to establish the lifecycle and CQE facts needed by
the Dragonfly owner loop:

- parent exports an RM/RTP Jetty and a pinned, non-cacheable, plain-token,
  READ-only Segment;
- child imports both objects without RC bind and posts one signaled READ per
  chunk;
- `user_ctx` routes each raw send-side CQE to exactly one retained WR owner;
- CQE status, opcode, completion length, local ID, direction, validity flags,
  and event kind are copied before provider storage is reused;
- an unimport attempt while a READ owner is outstanding must return `-EBUSY`;
- child drains all CQEs, retires all owners, verifies the complete SHA-256,
  unimports the Segment and Jetty, and then acknowledges the parent;
- parent unregisters the source Segment only after that acknowledgement, frees
  its explicit token ID while the backing is still alive, and then permits the
  child to finish shutdown.

The READ completion opcode is recorded as evidence but is not interpreted. The
UMDK public definition documents `opcode` and `remote_id` as receive-side facts;
the owner loop therefore routes READ completions by `user_ctx`, direction,
status, and event kind.

## Build on the B7 host

From the `urma-transport-lab` checkout:

```bash
cargo build --release --features urma --bin rm_read_probe
```

If UMDK is installed outside the default include and library locations:

```bash
UMDK_INCLUDE_DIR=/path/to/umdk/include \
UMDK_LIB_DIR=/path/to/umdk/lib \
cargo build --release --features urma --bin rm_read_probe
```

## Single-host run

Use two shells on the same host. The TCP address is only the out-of-band control
channel. Both processes select `udmac0d1e2` EID index 1 and the provider data
path remains RM/RTP.

Shell 1:

```bash
cd /path/to/urma-transport-lab
./target/release/rm_read_probe \
  parent udmac0d1e2 1 127.0.0.1:31912 \
  67108864 1048576 128
```

Shell 2:

```bash
cd /path/to/urma-transport-lab
./target/release/rm_read_probe \
  child udmac0d1e2 1 127.0.0.1:31912 \
  67108864 1048576 128
```

The final child JSON must have:

- `state: "passed"`;
- `transportMode: "RM"` and `tpType: "RTP"`;
- `completions: 64` for this command;
- `busyUnimportStatus: -16` (`EBUSY`);
- `contentVerified`, `ownersRetired`, and `cleanShutdown` all `true`.

Keep the reported `opcodes`, `completionLengths`, `localIds`, `isJettyValues`,
`remoteIdValidValues`, and `immDataValidValues` verbatim in the validation
ledger. They are provider observations, not protocol constants.

The same run is also exposed as an ignored integration test:

```bash
URMA_TEST_DEVICE=udmac0d1e2 \
URMA_TEST_EID_INDEX=1 \
cargo test --release --features urma --test rm_read_real_provider \
  -- --ignored --nocapture
```

## Failure interpretation

- Failure before `IMPORTED` points to RM Jetty or READ Segment export/import.
- `busy unimport probe` succeeding is a wrapper lifecycle defect; do not use
  this path in Dragonfly.
- A completion error is reported with raw status, opcode, and `user_ctx`.
- Unknown or duplicate `user_ctx` is a CQE routing failure.
- Digest mismatch means a successful CQE did not establish correct data.
- Teardown errors after `DRAINED_UNIMPORTED` belong to unregister, token release,
  Jetty/JFC deletion, context deletion, or `urma_uninit` and must remain distinct
  from data-path success.
