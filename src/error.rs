use std::fmt;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Eq, PartialEq)]
pub enum Error {
    FeatureDisabled,
    AlreadyInitialized,
    InvalidConfiguration(String),
    Protocol(String),
    Timeout {
        operation: &'static str,
    },
    Completion {
        status: i32,
        opcode: u32,
        user_ctx: u64,
        sequence: Option<u64>,
        post_call: Option<u64>,
        post_index: Option<u32>,
        post_count: Option<u32>,
    },
    Io {
        operation: &'static str,
        message: String,
    },
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
    NullHandle {
        operation: &'static str,
    },
    Native {
        operation: &'static str,
        status: i32,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FeatureDisabled => write!(f, "this build does not enable the `urma` feature"),
            Self::AlreadyInitialized => {
                write!(f, "an URMA runtime already owns process-global liburma")
            }
            Self::InvalidConfiguration(detail) => write!(f, "invalid configuration: {detail}"),
            Self::Protocol(detail) => write!(f, "protocol error: {detail}"),
            Self::Timeout { operation } => write!(f, "operation {operation} timed out"),
            Self::Completion {
                status,
                opcode,
                user_ctx,
                sequence,
                post_call,
                post_index,
                post_count,
            } => {
                let status_name = match *status {
                    0 => "SUCCESS",
                    1 => "UNSUPPORTED_OPCODE_ERR",
                    2 => "LOC_LEN_ERR",
                    3 => "LOC_OPERATION_ERR",
                    4 => "LOC_ACCESS_ERR",
                    5 => "REM_RESP_LEN_ERR",
                    6 => "REM_UNSUPPORTED_REQ_ERR",
                    7 => "REM_OPERATION_ERR",
                    8 => "REM_ACCESS_ABORT_ERR",
                    9 => "ACK_TIMEOUT_ERR",
                    10 => "RNR_RETRY_CNT_EXC_ERR",
                    11 => "WR_FLUSH_ERR",
                    12 => "WR_SUSPEND_DONE",
                    13 => "WR_FLUSH_ERR_DONE",
                    14 => "WR_UNHANDLED",
                    15 => "LOC_DATA_POISON",
                    16 => "REM_DATA_POISON",
                    _ => "UNKNOWN",
                };
                write!(
                    f,
                    "completion failed: status={status}({status_name}), opcode={opcode}, user_ctx={user_ctx}, sequence={sequence:?}, post_call={post_call:?}, post_index={post_index:?}, post_count={post_count:?}"
                )
            }
            Self::Io { operation, message } => {
                write!(f, "I/O operation {operation} failed: {message}")
            }
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
                write!(
                    f,
                    "liburma operation {operation} failed with status {status}"
                )
            }
        }
    }
}

impl std::error::Error for Error {}
