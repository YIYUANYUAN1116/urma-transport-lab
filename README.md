# URMA transport lab

This repository is a standalone Phase 0 prototype for validating a minimal
`liburma + Rust FFI` lifecycle. It deliberately has no Dragonfly dependency.

The default build does not inspect UMDK headers or link `liburma`:

```text
cargo test
cargo run --bin parent
```

On a Linux URMA host:

```text
UMDK_INCLUDE_DIR=/usr/include/ub/umdk/urma \
UMDK_LIB_DIR=/usr/lib64 \
cargo run --features urma --bin parent -- urma0
```

The current executables stop after `urma_init -> device -> context`. Jetty,
SEND/RECV and CQ polling are explicit follow-up milestones documented in
`docs/phase0-implementation-plan.md`.

M0 implementation and verification status is recorded in
`docs/m0-build-status.md`.
