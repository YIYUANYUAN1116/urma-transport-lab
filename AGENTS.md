# AGENTS.md

## Repository Purpose

`urma-transport-lab` is a standalone prototype for validating the URMA transport foundation required by future Dragonfly integration.

It is not a production Dragonfly implementation.

Development order:

```text
transport foundation
-> Piece-like transfer
-> Dragonfly integration
-> concurrency/stability
-> performance measurement
-> optimization
```

Do not skip directly to zero-copy, READ/WRITE, UBS Memory, Scheduler integration, or production abstractions.

## Reference Paths

Before non-trivial changes, consult existing documents and source code.

```text
Dragonfly docs:
  /home/yuan/workspace/docs/engineering-lab/dragonfly

UB/URMA docs:
  /home/yuan/workspace/docs/engineering-lab/ub

Dragonfly source:
  /home/yuan/workspace/dev/Dragonfly2

UMDK source:
  /home/yuan/workspace/cloud-native/umdk
```

For URMA API, Jetty/JFC/JFR, WR/CQE, provider behavior, and resource lifecycle, prefer current UMDK source.

For Piece, Downloader, Storage, RangeReader, PieceContentStream, and Parent/Child behavior, prefer current Dragonfly source.

Do not treat design documents as proof of implemented behavior.

## Evidence Rules

Keep these categories distinct:

* source-confirmed;
* experimentally verified;
* architecture/design inference;
* awaiting source confirmation;
* awaiting environment validation.

If sources, documents, and experiments disagree, prefer:

```text
real experiment
> current source
> historical design document
```

State important differences explicitly.

## Current Status

Completed milestones:

```text
M0: init/device/context + ABI baseline
M1: JFC + registered Segment + BufferPool
M2: RC duplex Jetty + OOB + descriptor exchange + import/bind
M3: SEND/RECV + CQ polling + user_ctx routing + Ping/Pong
```

M3 has been validated successfully in a real UB environment.

Validated environment includes:

```text
openEuler 24.03 LTS SP4
aarch64
UDMA provider
device: udmac0d1e2
```

A key experimentally confirmed requirement is:

```text
UB/UDMA uses shared JFR.
```

Do not change the working shared-JFR configuration back to non-shared JFR without a verified reason.

## Current Stage

Current work is M4: Dragonfly-like Piece File Transfer Demo.

Target flow:

```text
Child Request
  -> Parent Metadata
  -> multiple Data messages
  -> End
  -> Child output file
  -> length/digest verification
```

M4 should validate:

```text
file
-> registered TX buffer
-> URMA SEND
-> RX buffer
-> CQE
-> owned buffer
-> output file
```

M4 remains a standalone demo and must not depend on Dragonfly crates.

## Preserve the Working Foundation

M3 is already verified on real hardware.

Prefer incremental changes and reuse the existing:

* Runtime;
* shared JFR;
* JFC/Jetty setup;
* OOB handshake;
* import/bind;
* BufferPool;
* CQ polling;
* `user_ctx` routing;
* drain/shutdown path.

Do not perform broad refactors unless the current implementation demonstrably prevents the milestone from being completed.

## M4 Constraints

M4 should remain:

```text
single connection
single outstanding request
SEND/RECV
copy mode
message-oriented protocol
```

Do not implement during M4:

* Dragonfly Downloader integration;
* Scheduler changes;
* multi-peer;
* multi-request concurrency;
* TX pipelining;
* zero-copy RX leases;
* URMA READ/WRITE;
* remote Segment one-sided access;
* UBS Memory;
* performance tuning.

The current RX ownership model remains:

```text
recv CQE
-> copy payload from registered RX slot
-> release/repost RX slot
-> application owns copied data
```

TX slots must not be reused before send completion.

## Unsafe / FFI Rules

Keep raw UMDK types inside the existing FFI/shim boundary.

Do not expose raw `*mut urma_*` into application code.

Do not manually reinterpret arbitrary network bytes as UMDK structs.

Before introducing a new URMA API, verify its actual signature and semantics against:

```text
/home/yuan/workspace/cloud-native/umdk
```

## Testing

Preserve all existing M0-M3 tests.

For each milestone:

```text
cargo fmt --check
feature-off check/test
feature-on compile/test when environment permits
real-provider validation when required
```

Mock/unit tests do not count as real URMA validation.

Do not claim hardware behavior has been verified unless it was actually run in the UB environment.

## Documentation

Maintain milestone status documents under `docs/`.

For M4:

```text
docs/m4-build-status.md
```

Clearly distinguish:

* implemented;
* compiled;
* unit-tested;
* real-provider validated;
* not yet validated;
* out of scope.

## Core Principle

Prefer:

```text
correctness
-> lifecycle safety
-> reproducibility
-> integration compatibility
-> measurement
-> optimization
```

Do not add complexity merely because URMA provides additional capabilities.
