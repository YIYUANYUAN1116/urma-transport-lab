use std::fmt;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Eq, PartialEq)]
pub enum Error {
    FeatureDisabled,
    AlreadyInitialized,
    InvalidConfiguration(String),
    InvalidDeviceName,
    FfiContract {
        operation: &'static str,
        detail: &'static str,
    },
    StartupRollback {
        primary: Box<Error>,
        cleanup_failures: Vec<String>,
    },
    Shutdown {
        failures: Vec<String>,
    },
    NullHandle { operation: &'static str },
    Native { operation: &'static str, status: i32 },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FeatureDisabled => write!(f, "this build does not enable the `urma` feature"),
            Self::AlreadyInitialized => {
                write!(f, "an URMA runtime already owns process-global liburma")
            }
            Self::InvalidConfiguration(detail) => write!(f, "invalid configuration: {detail}"),
            Self::InvalidDeviceName => write!(f, "device name contains an interior NUL byte"),
            Self::FfiContract { operation, detail } => {
                write!(f, "FFI contract violation during {operation}: {detail}")
            }
            Self::StartupRollback {
                primary,
                cleanup_failures,
            } => write!(
                f,
                "startup failed: {primary}; rollback failures: {}",
                cleanup_failures.join("; ")
            ),
            Self::Shutdown { failures } => {
                write!(f, "shutdown failures: {}", failures.join("; "))
            }
            Self::NullHandle { operation } => {
                write!(
                    f,
                    "native operation {operation} succeeded without returning a handle"
                )
            }
            Self::Native { operation, status } => {
                write!(f, "liburma operation {operation} failed with status {status}")
            }
        }
    }
}

impl std::error::Error for Error {}
