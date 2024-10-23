//! Utilities for testing inter-worker communication

use crate::{
    runtime::CommunicationBackend,
    types::{OperatorId, WorkerId},
};
use thiserror::Error;

/// A CommunicationBackend which will always return an error when trying to create a connection
/// This is only really useful for unit tests where you know the operator will not attempt
/// to make a connection or want to assert it does not
#[derive(Debug, Default)]
pub struct NoCommunication;
impl CommunicationBackend for NoCommunication {
    fn new_connection(
        &mut self,
        _to_worker: WorkerId,
        _to_operator: OperatorId,
        _from_operator: OperatorId,
    ) -> Result<
        Box<dyn crate::runtime::communication::Transport>,
        crate::runtime::communication::CommunicationBackendError,
    > {
        Err(
            crate::runtime::communication::CommunicationBackendError::ClientBuildError(Box::new(
                NoCommunicationError::CannotCreateClientError,
            )),
        )
    }
}
#[derive(Error, Debug)]
pub enum NoCommunicationError {
    #[error("NoCommunication backend cannot create clients")]
    CannotCreateClientError,
}
