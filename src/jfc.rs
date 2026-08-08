use crate::{ffi, Error, Result};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum JfcKind {
    Send,
    Receive,
}

/// Safe owner of one native JFC. The raw handle remains inside `ffi`.
pub(crate) struct UrmaJfc {
    kind: JfcKind,
    depth: u32,
    handle: ffi::JfcHandle,
}

impl UrmaJfc {
    pub(crate) fn create(
        runtime: &mut ffi::NativeRuntime,
        kind: JfcKind,
        depth: u32,
    ) -> Result<Self> {
        let operation = match kind {
            JfcKind::Send => "create_send_jfc",
            JfcKind::Receive => "create_recv_jfc",
        };
        let handle = ffi::JfcHandle::create(runtime, depth)
            .map_err(|error| map_ffi_error(operation, error))?;
        Ok(Self {
            kind,
            depth,
            handle,
        })
    }

    pub(crate) fn kind(&self) -> JfcKind {
        self.kind
    }

    pub(crate) fn depth(&self) -> u32 {
        self.depth
    }

    pub(crate) fn handle(&self) -> &ffi::JfcHandle {
        &self.handle
    }

    pub(crate) fn close(&mut self) -> Result<()> {
        let operation = match self.kind {
            JfcKind::Send => "delete_send_jfc",
            JfcKind::Receive => "delete_recv_jfc",
        };
        self.handle
            .close()
            .map_err(|error| map_ffi_error(operation, error))
    }
}

fn map_ffi_error(operation: &'static str, error: ffi::FfiError) -> Error {
    match error {
        ffi::FfiError::Contract(detail) => Error::FfiContract { operation, detail },
        ffi::FfiError::NullHandle => Error::NullHandle { operation },
        ffi::FfiError::Status(status) => Error::Native { operation, status },
    }
}
