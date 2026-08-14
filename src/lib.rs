//! Phase 0 URMA transport laboratory.
//!
//! The default build is intentionally hardware independent. Enable `urma` on a
//! Linux host with UMDK installed to compile and link the native boundary.

pub mod benchmark;
pub mod buffer;
pub mod completion;
pub mod connection;
pub mod digest;
mod error;
#[cfg(feature = "urma")]
mod ffi;
pub mod jetty;
#[cfg(feature = "urma")]
mod jfc;
pub mod message;
pub mod oob;
pub mod runtime;
pub mod transfer;
pub mod wr;

pub use benchmark::{
    BenchmarkCase, BenchmarkResult, BenchmarkScenario, BenchmarkSink, BenchmarkTimer,
    BenchmarkTransport, CpuUsage, FileCompletionPolicy, FileSink, FileSource, IntegrityResult,
    MemorySink, MemorySource, TimingMode, TimingSample,
};
pub use buffer::{BufferPoolConfig, SlotId, SlotKind, SlotState};
pub use completion::{CompletionEvent, CompletionStats};
pub use connection::ConnectionState;
#[cfg(feature = "urma")]
pub use connection::UrmaConnection;
pub use digest::{crc32_bytes, crc32_reader, format_crc32_digest, parse_crc32_digest, Crc32Hasher};
pub use error::{Error, Result};
pub use jetty::{JettyConfig, JettyDescriptor, JETTY_DESCRIPTOR_VERSION, MAX_JETTY_DESCRIPTOR_LEN};
pub use message::{
    DigestAlgorithm, DigestDescriptor, IntegrationMessageBodyV3, IntegrationMessageTypeV3,
    IntegrationMessageV3, Message, MessageBody, MessageType,
};
pub use runtime::{
    abi_baseline, AbiBaseline, DeviceEid, RuntimeConfig, UrmaDeviceCapability, UrmaRuntime,
};
pub use transfer::{digest_reader, hex_digest, ReceiveState, TransferSummary};

/// Phase 0 roadmap markers. M2 control-plane variants are now implemented;
/// post/poll variants remain M3 work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrototypeMilestone {
    CreateCompletionQueues,
    CreateDuplexJetty,
    ExchangeJettyDescriptor,
    BindRemoteJetty,
    PostReceive,
    PostSend,
    PollCompletionQueue,
}
