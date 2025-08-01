//! Traits for implementing inter-worker and worker-coordinator communication in different runtimes
use std::marker::PhantomData;

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;
use tracing::debug;

use crate::{
    errorhandling::MalstromFatal,
    types::{OperatorId, WorkerId},
};

/// A type which can be sent (distributed) between workers
pub trait Distributable: Serialize + DeserializeOwned {}
impl<T> Distributable for T where T: Serialize + DeserializeOwned {}

/// A backend facilitating inter-worker communication in malstrom
pub trait OperatorOperatorComm {
    /// Establish a new two-way communciation transport between the same operators
    /// on two workers.
    /// Implementors can expect this method to be called at most once per
    /// unique combination of arguments
    /// The returned future should complete when the other side has accepted the connection
    /// The implementation must not wait for the other side to accept the stream, i.e.
    /// implementations must be able to buffer outgoing messages
    fn operator_to_operator(
        &self,
        to_worker: WorkerId,
        operator: OperatorId,
    ) -> Result<Box<dyn BiStreamTransport>, CommunicationBackendError>;
}

/// An communication implementation for sending messages from the coordinator to a worker
pub trait CoordinatorWorkerComm {
    /// Establish a connection from the coordinator to a given worker.
    /// The returned Future should complete once the worker has accepted the Connection vie
    /// its [WorkerCoordinatorBackend]
    /// The implementation must not wait for the other side to accept the stream, i.e.
    /// implementations must be able to buffer outgoing messages
    fn coordinator_to_worker(
        &self,
        worker_id: WorkerId,
    ) -> Result<Box<dyn BiStreamTransport>, CommunicationBackendError>;
}

/// An communication implementation for sending messages from a worker to the coordinator.
pub trait WorkerCoordinatorComm {
    /// Establish a connection to the coordinator.
    /// The returned future should complete once the coordinator has accepted the connection via
    /// its [CoordinatorWorkerBackend]
    /// The implementation must not wait for the other side to accept the stream, i.e.
    /// implementations must be able to buffer outgoing messages
    fn worker_to_coordinator(
        &self,
    ) -> Result<Box<dyn BiStreamTransport>, CommunicationBackendError>;
}

/// Bi-directional streaming transport where each end can send many messages to the other without
/// requiring a response
#[async_trait]
pub trait BiStreamTransport: Send + Sync {
    /// Send a single message to the Operator on the other end of the transport.
    ///
    /// Fallible transports must implement applicable retry logic internally,
    /// an error should only be returned on **unrecoverable conditions**.
    fn send(&self, msg: Vec<u8>) -> Result<(), TransportError>;

    /// Receive a single message for the operator on this end of the transport.
    ///
    /// If no message is available at this moment, `Ok(None)` shall be returned.
    /// Fallible transports must implement applicable retry logic internally,
    /// an error should only be returned on **unrecoverable conditions**.
    fn recv(&self) -> Result<Option<Vec<u8>>, TransportError>;

    /// Wait until a message becomes available
    async fn recv_async(&self) -> Result<Vec<u8>, TransportError>;

    /// Receive all currently available messages for this operator.
    /// Some transport implementations may handle message reception more efficiently
    /// if messages are received in bulk. For these cases they may implement this
    /// method.
    /// The default implementation simply calls [`Transport::recv()`] repeatedly.
    fn recv_all<'a>(&'a self) -> Box<dyn Iterator<Item = Result<Vec<u8>, TransportError>> + 'a> {
        Box::new(std::iter::from_fn(|| self.recv().transpose()))
    }
}

/// A Client for point to point communication between operators
/// on different workers and possibly different machines
pub struct CommunicationClient<TSend, TRecv> {
    transport: Box<dyn BiStreamTransport>,
    message_type: PhantomData<(TSend, TRecv)>,
}
/// A communication client which sends and receives the same type of message
pub type BiCommunicationClient<T> = CommunicationClient<T, T>;

/// Operator-to-Operator impl
impl<T> CommunicationClient<T, T>
where
    T: Distributable,
{
    /// Create a new client for communication with the given worker and operator
    pub(crate) fn new(
        to_worker: WorkerId,
        operator: OperatorId,
        backend: &dyn OperatorOperatorComm,
    ) -> Result<Self, CommunicationBackendError> {
        debug!(
            message = "Creating operator-operator communication client",
            ?to_worker,
            ?operator
        );
        let transport = backend.operator_to_operator(to_worker, operator)?;
        Ok(Self {
            transport,
            message_type: PhantomData,
        })
    }
}

impl<TSend, TRecv> CommunicationClient<TSend, TRecv> {
    /// Establish a connection from the coordinator to a given worker
    pub(crate) fn coordinator_to_worker(
        worker_id: WorkerId,
        backend: &dyn CoordinatorWorkerComm,
    ) -> Result<Self, CommunicationBackendError> {
        let transport = backend.coordinator_to_worker(worker_id)?;
        Ok(Self {
            transport,
            message_type: PhantomData,
        })
    }

    /// Establish a connection to the coordinator
    pub(crate) fn worker_to_coordinator(
        backend: &dyn WorkerCoordinatorComm,
    ) -> Result<Self, CommunicationBackendError> {
        let transport = backend.worker_to_coordinator()?;
        Ok(Self {
            transport,
            message_type: PhantomData,
        })
    }
}

impl<TSend, TRecv> CommunicationClient<TSend, TRecv>
where
    TSend: Distributable,
{
    /// Send a message using this client. This does not wait for the other worker
    /// to receive the message, but merely queues the message for delivery.
    pub fn send(&self, msg: TSend) {
        self.transport.send(Self::encode(msg)).malstrom_fatal()
    }

    pub(crate) fn encode(msg: TSend) -> Vec<u8> {
        rmp_serde::encode::to_vec(&msg).malstrom_fatal()
    }
}

impl<TSend, TRecv> CommunicationClient<TSend, TRecv>
where
    TRecv: Distributable,
{
    /// Try receiving a message in a non-blocking manner. This function returns immediatly either
    /// with a message if one is available or with `None` if no message is available.
    pub fn recv(&self) -> Option<TRecv> {
        let encoded = self.transport.recv().malstrom_fatal()?;
        Some(Self::decode(&encoded))
    }

    /// Asycnhronously receive a message on this client. The returned future completes,
    /// once a message is available
    pub async fn recv_async(&self) -> TRecv {
        let encoded = self.transport.recv_async().await.malstrom_fatal();
        Self::decode(&encoded)
    }

    pub(crate) fn decode(msg: &[u8]) -> TRecv {
        rmp_serde::decode::from_slice(msg)
            .map_err(|e| DecodeError::Serde(e, std::any::type_name::<TRecv>()))
            .malstrom_fatal()
    }
}

#[derive(Debug, Error)]
enum DecodeError {
    #[error("Expected to decode to type {1}")]
    Serde(#[source] rmp_serde::decode::Error, &'static str),
}

/// A convinience method to broadcast a message to all available clients
pub fn broadcast<'a, TSend: Distributable + Clone + 'a, TRecv: 'a>(
    clients: impl Iterator<Item = &'a CommunicationClient<TSend, TRecv>>,
    msg: TSend,
) {
    for c in clients {
        c.send(msg.clone());
    }
}

/// An error from the inter-worker communication backend
#[derive(thiserror::Error, Debug)]
pub enum CommunicationBackendError {
    /// Error to be returned if a communication client for a specific connection
    /// could not be built
    #[error("Error building Client: {0:?}")]
    ClientBuildError(Box<dyn std::error::Error + Send + Sync>),
}

/// An error from the inter-worker communication transport
#[derive(thiserror::Error, Debug)]
pub enum TransportError {
    /// Error returned if the communication transport failed to receive a message
    /// for example due to a decoding error
    #[error("Error sending message: {0}")]
    SendError(#[from] Box<dyn std::error::Error + Send + Sync>),

    /// Error returned if the communication transport failed to receive a message
    /// for example due to a decoding error
    ///
    /// **NOTE:** No new message being available should **NOT** return an error.
    #[error("Error receiving message: {0}")]
    RecvError(Box<dyn std::error::Error + Send + Sync>),
}
