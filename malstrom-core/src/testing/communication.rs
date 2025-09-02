//! Utilities for testing inter-worker communication

use crate::{
    runtime::OperatorOperatorComm,
    types::{OperatorId, WorkerId},
};
use thiserror::Error;

/// A CommunicationBackend which will always return an error when trying to create a connection
/// This is only really useful for unit tests where you know the operator will not attempt
/// to make a connection or want to assert it does not
#[derive(Debug, Default)]
pub struct NoCommunication;
impl OperatorOperatorComm for NoCommunication {
    fn operator_to_operator(
        &self,
        _to_worker: WorkerId,
        _operator: OperatorId,
    ) -> Result<
        Box<dyn crate::runtime::communication::BiStreamTransport>,
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
