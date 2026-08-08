//! Phase 0 URMA transport laboratory.
//!
//! The default build is intentionally hardware independent. Enable `urma` on a
//! Linux host with UMDK installed to compile and link the native boundary.

mod error;
#[cfg(feature = "urma")]
mod ffi;
pub mod runtime;

pub use error::{Error, Result};
pub use runtime::{abi_baseline, AbiBaseline, RuntimeConfig, UrmaRuntime};

/// Data-plane milestones deliberately left unimplemented in the skeleton.
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
