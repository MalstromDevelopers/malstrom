use std::marker::PhantomData;

use postbox::Postbox;
use serde::{de::DeserializeOwned, Deserialize};

use crate::{keyed::distributed::Distributable, OperatorId, WorkerId};

/// A backend facilitating inter-worker communication in jetstream
pub trait CommunicationBackend {

    fn new_connection(
        &mut self,
        to_worker: WorkerId, to_operator: OperatorId,
        from: OperatorId,
    ) -> Result<Box<dyn Transport>, ClientBuildError>;
}


pub trait Transport {
    fn send(&self, msg: Vec<u8>) -> Result<(), SendError>;

    fn recv(&self) -> Result<Option<Vec<u8>>, RecvError>;

    fn recv_all(&self) -> &mut dyn Iterator<Item = Result<Vec<u8>, RecvError>>;
}

const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

/// A Client for point to point communication between operators
/// on different workers and possibly different machines
pub struct CommunicationClient<T>{
    transport: Box<dyn Transport>,
    message_type: PhantomData<T>
}
impl <T> CommunicationClient<T> where T: Distributable {

    pub fn new(to_worker: WorkerId, to_operator: OperatorId, from: OperatorId, backend: &mut dyn CommunicationBackend) -> Result<Self, ClientBuildError> {
        let transport = backend.new_connection(to_worker, to_operator, from)?;
        Ok(Self { transport, message_type: PhantomData })
    }

    pub fn send(&self, msg: T) -> () {
        let encoded = bincode::serde::encode_to_vec(msg, BINCODE_CONFIG).expect("Serialization Error");
        self.transport.send(encoded).unwrap()
    }

    pub fn recv(&self) -> Option<T> {
        let encoded = self.transport.recv();
        if let Some(e) = encoded.unwrap() {
            bincode::serde::decode_from_slice(&e, BINCODE_CONFIG).unwrap().0
        } else {
            None
        }
    }

    pub fn recv_all<'a>(&'a self) -> RecvAllIterator<'a, T> {
        RecvAllIterator::new(self.transport.recv_all())
    }
}

/// The Iterator returned by CommunicationClient::recv_all
pub struct RecvAllIterator<'a, T>{
    inner: &'a mut dyn Iterator<Item = Result<Vec<u8>, RecvError>>,
    item_type: PhantomData<T>
}
impl <'a, T> RecvAllIterator<'a, T> {
    fn new(inner: &'a mut dyn Iterator<Item = Result<Vec<u8>, RecvError>>) -> Self {
        Self { inner, item_type: PhantomData }
    }
}
impl <'a, T> Iterator for RecvAllIterator<'a, T> where T: DeserializeOwned {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.inner.next()?.unwrap();
        Some(bincode::serde::decode_from_slice(&item, BINCODE_CONFIG).unwrap().0)
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

/// Error to be returned if a communication client for a specific connection
/// could not be built
#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub struct ClientBuildError(#[from] Box<dyn std::error::Error>);

/// Error returned if the communication backend failed to send a message
#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub struct SendError(#[from] Box<dyn std::error::Error>);

/// Error returned if the communication backend failed to receive a message
/// for example due to a decoding error
#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub struct RecvError(#[from] Box<dyn std::error::Error>);



// TODO: REMOVE THE STUFF BELOW THIS
impl<A> CommunicationBackend for Postbox<A>{
    fn new_connection(
        &mut self,
        to_worker: WorkerId, to_operator: OperatorId,
        from: OperatorId,
    ) -> Result<Box<dyn Transport>, ClientBuildError> {
        todo!()
    }
}