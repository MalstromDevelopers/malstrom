use std::marker::PhantomData;

use postbox::Postbox;
use serde::{de::DeserializeOwned, Deserialize};

use crate::{keyed::distributed::Distributable, OperatorId, WorkerId};

/// A backend facilitating inter-worker communication in jetstream
pub trait CommunicationBackend {
    /// Establish a new two-way communciation transport between operators
    /// Implementors can expect this method to be called at most once per
    /// unique combination of arguments
    fn new_connection(
        &mut self,
        to_worker: WorkerId,
        to_operator: OperatorId,
        from_operator: OperatorId,
    ) -> Result<Box<dyn Transport>, CommunicationBackendError>;
}

pub trait Transport {
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

    /// Receive all currently available messages for this operator.
    /// Some transport implementations may handle message reception more efficiently
    /// if messages are received in bulk. For these cases they may implement this
    /// method.
    /// The default implementation simply calls [`Transport::recv()`] repeatedly.
    fn recv_all<'a>(&'a self) -> Box<dyn Iterator<Item = Result<Vec<u8>, TransportError>> + 'a> {
        Box::new(std::iter::from_fn(|| self.recv().transpose()))
    }
}

const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

/// A Client for point to point communication between operators
/// on different workers and possibly different machines
pub struct CommunicationClient<T> {
    transport: Box<dyn Transport>,
    message_type: PhantomData<T>,
}
impl<T> CommunicationClient<T>
where
    T: Distributable,
{
    pub fn new(
        to_worker: WorkerId,
        to_operator: OperatorId,
        from: OperatorId,
        backend: &mut dyn CommunicationBackend,
    ) -> Result<Self, CommunicationBackendError> {
        let transport = backend.new_connection(to_worker, to_operator, from)?;
        Ok(Self {
            transport,
            message_type: PhantomData,
        })
    }

    pub fn send(&self, msg: T) -> () {
        let encoded =
            bincode::serde::encode_to_vec(msg, BINCODE_CONFIG).expect("Serialization Error");
        self.transport.send(encoded).unwrap()
    }

    pub fn recv(&self) -> Option<T> {
        let encoded = self.transport.recv();
        if let Some(e) = encoded.unwrap() {
            bincode::serde::decode_from_slice(&e, BINCODE_CONFIG)
                .unwrap()
                .0
        } else {
            None
        }
    }

    pub fn recv_all<'a>(&'a self) -> RecvAllIterator<'a, T> {
        RecvAllIterator::new(self.transport.recv_all())
    }
}

/// The Iterator returned by CommunicationClient::recv_all
pub struct RecvAllIterator<'a, T> {
    inner: Box<dyn Iterator<Item = Result<Vec<u8>, TransportError>> + 'a>,
    item_type: PhantomData<T>,
}
impl<'a, T> RecvAllIterator<'a, T> {
    fn new(inner: Box<dyn Iterator<Item = Result<Vec<u8>, TransportError>> + 'a>) -> Self {
        Self {
            inner,
            item_type: PhantomData,
        }
    }
}
impl<'a, T> Iterator for RecvAllIterator<'a, T>
where
    T: DeserializeOwned,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.inner.next()?.unwrap();
        Some(
            bincode::serde::decode_from_slice(&item, BINCODE_CONFIG)
                .unwrap()
                .0,
        )
    }
}

/// A convinience method to broadcast a message to all available clients
pub fn broadcast<'a, T: Distributable + Clone + 'a>(
    clients: impl Iterator<Item = &'a CommunicationClient<T>>,
    msg: T,
) -> () {
    for c in clients {
        c.send(msg.clone());
    }
    ()
}

#[derive(thiserror::Error, Debug)]
pub enum CommunicationBackendError {
    /// Error to be returned if a communication client for a specific connection
    /// could not be built
    #[error("Error building Client: {0}")]
    ClientBuildError(Box<dyn std::error::Error>),
}

#[derive(thiserror::Error, Debug)]
pub enum TransportError {
    /// Error returned if the communication transport failed to receive a message
    /// for example due to a decoding error
    #[error("Error sending message: {0}")]
    SendError(Box<dyn std::error::Error>),

    /// Error returned if the communication transport failed to receive a message
    /// for example due to a decoding error
    ///
    /// **NOTE:** No new message being available should **NOT** return an error.
    #[error("Error receiving message: {0}")]
    RecvError(Box<dyn std::error::Error>),
}
