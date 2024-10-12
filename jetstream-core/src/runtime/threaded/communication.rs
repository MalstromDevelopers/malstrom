use crate::{
    runtime::{
        communication::{CommunicationBackendError, Transport, TransportError},
        CommunicationBackend,
    },
    types::{OperatorId, WorkerId,}
};
use std::sync::{
    mpsc::{self, Receiver, Sender},
    Arc, Mutex,
};

use indexmap::{map::Entry, IndexMap, IndexSet};
use thiserror::Error;
use tracing::debug;

/// uniquely identifies a connection
#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub(crate) struct ConnectionKey {
    worker_low: WorkerId,
    operator_low: OperatorId,
    worker_high: WorkerId,
    operator_high: OperatorId,
}
impl ConnectionKey {
    /// generates the same key no matter in which direction the connection is supplied
    fn new(
        worker_a: WorkerId,
        operator_a: OperatorId,
        worker_b: WorkerId,
        operator_b: OperatorId,
    ) -> Self {
        Self {
            worker_low: worker_a.min(worker_b),
            operator_low: operator_a.min(operator_b),
            worker_high: worker_a.max(worker_b),
            operator_high: operator_a.max(operator_b),
        }
    }
}

/// Stores mapping from key to Transport
/// When a worker attempts to create a connection and does not
/// find it in this map, it generates a pair of 2 `ChannelTransport`s
/// keeps one and places the other one in the map
type AddressMap = IndexMap<ConnectionKey, ChannelTransport>;
/// AddressMap shared across multiple threads
pub(crate) type Shared = Arc<Mutex<AddressMap>>;

/// Provides simple inter-thread communication via channels
pub struct InterThreadCommunication {
    shared: Shared,
    this_worker: WorkerId,
    // this set contains the keys this worker has already taken
    // the purpose is to prevent the worker from obtaining the same
    // channel twice (which should not happen), as then they would have both
    // ends, and the other worker would simply create a new Connection pair
    burnt_keys: IndexSet<ConnectionKey>
}
impl InterThreadCommunication {
    pub(crate) fn new(shared: Shared, this_worker: WorkerId) -> Self {
        Self {
            shared,
            this_worker,
            burnt_keys: IndexSet::new()
        }
    }
}
impl CommunicationBackend for InterThreadCommunication {
    fn new_connection(
        &mut self,
        to_worker: WorkerId,
        to_operator: OperatorId,
        from_operator: OperatorId,
    ) -> Result<Box<dyn Transport>, CommunicationBackendError> {
        let wid = self.this_worker;
        let mut shared = self.shared.lock().unwrap();
        let key = ConnectionKey::new(to_worker, to_operator, self.this_worker, from_operator);
        
        if self.burnt_keys.contains(&key) {
            return Err(CommunicationBackendError::ClientBuildError(Box::new(
                InterThreadCommunicationError::TransportAlreadyEstablished,
            )))
        }
        
        let transport = match shared.entry(key) {
            Entry::Occupied(o) => o.swap_remove(),
            Entry::Vacant(v) => {
                let (a, b) = new_transport_pair();
                v.insert(a);
                b
            }
        };
        self.burnt_keys.insert(key);
        Ok(Box::new(transport))
    }
}

/// MPSC Channel based transport
pub(crate) struct ChannelTransport {
    sender: Sender<Vec<u8>>,
    receiver: Receiver<Vec<u8>>,
}

impl Transport for ChannelTransport {
    fn send(&self, msg: Vec<u8>) -> Result<(), TransportError> {
        self.sender
            .send(msg)
            .map_err(|e| TransportError::SendError(Box::new(e)))
    }

    fn recv(&self) -> Result<Option<Vec<u8>>, TransportError> {
        match self.receiver.try_recv() {
            Ok(x) => Ok(Some(x)),
            Err(mpsc::TryRecvError::Empty) => Ok(None),
            // TODO: Currently we don't really have a way to know if the channel
            // is closed because the other thread paniced or if it was closed in
            // an orderly fashion. In the panic case we probably should emit
            // an error instead....
            Err(mpsc::TryRecvError::Disconnected) => Ok(None)
        }
    }
}

/// Generate a new pair of bi-directional transport clients.
/// Each client can send messages to and receive messages from the
/// other client
fn new_transport_pair() -> (ChannelTransport, ChannelTransport) {
    let (tx1, rx1) = mpsc::channel();
    let (tx2, rx2) = mpsc::channel();
    let a = ChannelTransport {
        sender: tx1,
        receiver: rx2,
    };
    let b = ChannelTransport {
        sender: tx2,
        receiver: rx1,
    };
    (a, b)
}

#[derive(Error, Debug)]
pub enum InterThreadCommunicationError {
    #[error("Transport for this connection was already established. Listener already taken")]
    TransportAlreadyEstablished,
}

#[cfg(test)]
mod test {
    use super::*;

    /// check we can send and recv a single message on two transports
    #[test]
    fn send_recv_message() {
        let shared = Shared::default();
        let mut worker0 = InterThreadCommunication::new(shared.clone(), 0);
        let mut worker1 = InterThreadCommunication::new(shared.clone(), 1);

        let operator_0_42 = worker0.new_connection(1, 1337, 42).unwrap();
        let operator_1_1337 = worker1.new_connection(0, 42, 1337).unwrap();

        let val = vec![1, 8, 8, 7];
        operator_0_42.send(val.clone()).unwrap();
        assert_eq!(operator_1_1337.recv().unwrap().unwrap(), val);

        // and now the other way around!!
        let val = vec![7, 8, 8, 1];
        operator_1_1337.send(val.clone()).unwrap();
        assert_eq!(operator_0_42.recv().unwrap().unwrap(), val);
    }

    /// If a receiver attempts to subscribe twice, we should error
    #[test]
    fn error_on_double_listen() {
        let shared = Shared::default();
        let mut worker0 = InterThreadCommunication::new(shared.clone(), 0);

        worker0.new_connection(1, 1, 0).unwrap();
        // this should work: it is for the same listener, but the to_operator arg is different
        worker0.new_connection(1, 0, 0).unwrap();
        // this should also work
        worker0.new_connection(0, 1, 0).unwrap();

        // but this should err since we already made the connection
        let err = worker0.new_connection(1, 1, 0);
        match err {
            Err(CommunicationBackendError::ClientBuildError(e)) => {
                let concrete = *e.downcast::<InterThreadCommunicationError>().unwrap();
                assert!(matches!(
                    concrete,
                    InterThreadCommunicationError::TransportAlreadyEstablished
                ));
            }
            Ok(_) => panic!("OK IS NOT OK")
        };
    }

    /// Check we don't give out an error on recv if the other side disconnected in
    /// an orderly way (e.g because it is done)
    #[test]
    fn no_error_orderly_disconnect() {
        let shared = Shared::default();
        let mut worker0 = InterThreadCommunication::new(shared.clone(), 0);
        let mut worker1 = InterThreadCommunication::new(shared.clone(), 1);
        let operator_0_42 = worker0.new_connection(1, 42, 42).unwrap();
        let operator_1_42 = worker1.new_connection(0, 42, 42).unwrap();

        // now drop one of them
        drop(operator_0_42);
        // this should not be an error

        assert!(operator_1_42.recv().is_ok());
        assert!(operator_1_42.recv_all().all(|x| x.is_ok()));
    }


}
