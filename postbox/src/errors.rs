use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConnectionError {
    #[error("Address could not be serialized")]
    EncodingError(#[from] bincode::error::EncodeError),
}
#[derive(Error, Debug)]
pub enum ClientCreationError {
    #[error("Error connection the client")]
    ConnectionError(#[from] ConnectionError),

    #[error("Address {0} could not be resolved to an endpoint")]
    AddressResolutionError(String),
}

#[derive(Error, Debug)]
pub enum BuildError {
    #[error("Error creating Tokio runtime")]
    ConnectionError(#[from] std::io::Error),
}

#[derive(Error, Debug)]
pub enum SendError {
    #[error("Message could not be serialized")]
    EncodingError(#[from] bincode::error::EncodeError),
    #[error("GRPC client is dead, possibly due to loosing connection")]
    DeadClientError,
}
