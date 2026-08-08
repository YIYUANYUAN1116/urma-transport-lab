#[cfg(feature = "urma")]
use crate::{Error, Result};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionState {
    Init,
    ContextReady,
    JettyCreated,
    DescriptorExchanged,
    Bound,
    Ready,
    Failed,
    Closed,
}

#[cfg(feature = "urma")]
mod native {
    use super::*;
    use crate::{jetty::UrmaJetty, JettyDescriptor, UrmaDeviceCapability};
    use std::marker::PhantomData;

    /// M2 control-plane owner. It intentionally exposes no data-plane API.
    pub struct UrmaConnection<'runtime> {
        state: ConnectionState,
        capability: UrmaDeviceCapability,
        jetty: UrmaJetty,
        _runtime: PhantomData<&'runtime mut crate::UrmaRuntime>,
    }

    impl<'runtime> UrmaConnection<'runtime> {
        pub(crate) fn new(capability: UrmaDeviceCapability, jetty: UrmaJetty) -> Self {
            let mut connection = Self {
                state: ConnectionState::ContextReady,
                capability,
                jetty,
                _runtime: PhantomData,
            };
            connection.transition(ConnectionState::JettyCreated);
            connection
        }

        pub fn state(&self) -> ConnectionState {
            self.state
        }

        pub fn capability(&self) -> &UrmaDeviceCapability {
            &self.capability
        }

        pub(crate) fn export_descriptor(&mut self) -> Result<JettyDescriptor> {
            self.require(ConnectionState::JettyCreated)?;
            let descriptor = self.jetty.export_descriptor()?;
            self.transition(ConnectionState::DescriptorExchanged);
            Ok(descriptor)
        }

        pub(crate) fn import_and_bind(&mut self, descriptor: &JettyDescriptor) -> Result<()> {
            self.require(ConnectionState::JettyCreated)?;
            let local_transport = u32::try_from(self.capability.transport_type)
                .map_err(|_| Error::Protocol("local transport type is negative".into()))?;
            if descriptor.transport_type != local_transport {
                return Err(Error::Protocol(format!(
                    "remote transport type {} does not match local {}",
                    descriptor.transport_type, local_transport
                )));
            }
            self.transition(ConnectionState::DescriptorExchanged);
            self.jetty.import(descriptor)?;
            self.jetty.bind()?;
            self.transition(ConnectionState::Bound);
            Ok(())
        }

        pub(crate) fn peer_bound(&mut self) -> Result<()> {
            self.require(ConnectionState::DescriptorExchanged)?;
            self.transition(ConnectionState::Bound);
            Ok(())
        }

        pub(crate) fn mark_ready(&mut self) -> Result<()> {
            self.require(ConnectionState::Bound)?;
            self.transition(ConnectionState::Ready);
            Ok(())
        }

        pub(crate) fn fail(&mut self) {
            if self.state != ConnectionState::Closed {
                self.transition(ConnectionState::Failed);
            }
        }

        pub fn close(mut self) -> Result<()> {
            self.close_inner()
        }

        fn close_inner(&mut self) -> Result<()> {
            if self.state == ConnectionState::Closed {
                return Ok(());
            }
            let result = self.jetty.close();
            if result.is_ok() {
                self.transition(ConnectionState::Closed);
            } else {
                self.transition(ConnectionState::Failed);
            }
            result
        }

        fn require(&self, expected: ConnectionState) -> Result<()> {
            if self.state == expected {
                Ok(())
            } else {
                Err(Error::Protocol(format!(
                    "connection state {:?}, expected {:?}",
                    self.state, expected
                )))
            }
        }

        fn transition(&mut self, state: ConnectionState) {
            eprintln!("M2 connection: {:?} -> {state:?}", self.state);
            self.state = state;
        }
    }

    impl Drop for UrmaConnection<'_> {
        fn drop(&mut self) {
            let _ = self.close_inner();
        }
    }
}

#[cfg(feature = "urma")]
pub use native::UrmaConnection;
