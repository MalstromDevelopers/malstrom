use crate::{
    runtime::{
        communication::{
            BiStreamTransport, CommunicationBackendError, CoordinatorWorkerComm, TransportError,
            WorkerCoordinatorComm,
        },
        OperatorOperatorComm,
    },
    types::{OperatorId, WorkerId},
};
use async_trait::async_trait;
use flume::{Receiver, Sender};
use std::sync::{Arc, Mutex};

use indexmap::IndexMap;
use tracing::debug;

/// uniquely identifies a connection
#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub struct ConnectionKey {
    worker_low: WorkerId,
    worker_high: WorkerId,
    operator: OperatorId,
}
impl ConnectionKey {
    /// generates the same key no matter in which direction the connection is supplied
    fn new(worker_a: WorkerId, worker_b: WorkerId, operator: OperatorId) -> Self {
        Self {
            worker_low: worker_a.min(worker_b),
            worker_high: worker_a.max(worker_b),
            operator,
        }
    }
}

/// Stores mapping from key to Transport
/// When a worker attempts to create a connection and does not
/// find it in this map, it generates a pair of 2 `ChannelTransport`s
/// keeps one and places the other one in the map
type AddressMap = IndexMap<ConnectionKey, ChannelTransportContainer>;
/// AddressMap shared across multiple threads
pub(crate) type Shared = Arc<Mutex<AddressMap>>;

/// Provides simple inter-thread communication via channels
pub struct InterThreadCommunication {
    shared: Shared,
    this_worker: WorkerId,
}
impl InterThreadCommunication {
    pub(crate) fn new(shared: Shared, this_worker: WorkerId) -> Self {
        debug!(
            "Creating communication backend for worker {:?}",
            this_worker
        );
        Self {
            shared,
            this_worker,
        }
    }

    fn inner_operator_to_operator(
        &self,
        to_worker: WorkerId,
        from_worker: WorkerId,
        operator: OperatorId,
    ) -> Result<Box<dyn BiStreamTransport>, CommunicationBackendError> {
        let key = ConnectionKey::new(to_worker, from_worker, operator);

        #[allow(clippy::unwrap_used)]
        let mut shared = self.shared.lock().unwrap();
        let transport_container = shared.entry(key).or_insert_with(new_transport_pair);
        // We return a clone of the instantiated transport instead of the value to
        // allow reconnecting
        let transport = if to_worker < from_worker {
            transport_container.to_low()
        } else {
            transport_container.to_high()
        };
        Ok(Box::new(transport))
    }
}

impl OperatorOperatorComm for InterThreadCommunication {
    fn operator_to_operator(
        &self,
        to_worker: WorkerId,
        operator: OperatorId,
    ) -> Result<Box<dyn BiStreamTransport>, CommunicationBackendError> {
        self.inner_operator_to_operator(to_worker, self.this_worker, operator)
    }
}

impl WorkerCoordinatorComm for InterThreadCommunication {
    fn worker_to_coordinator(
        &self,
    ) -> Result<Box<dyn BiStreamTransport>, CommunicationBackendError> {
        // HACK but works
        self.operator_to_operator(WorkerId::MAX, 0)
    }
}
impl CoordinatorWorkerComm for InterThreadCommunication {
    fn coordinator_to_worker(
        &self,
        worker_id: WorkerId,
    ) -> Result<Box<dyn BiStreamTransport>, CommunicationBackendError> {
        // HACK but works
        self.inner_operator_to_operator(worker_id, WorkerId::MAX, 0)
    }
}

#[derive(Debug)]
pub(crate) struct ChannelTransportContainer {
    /// Transport TO the lower ID worker
    to_low: ChannelTransport,
    // Transport TO the higher ID worker
    to_high: ChannelTransport,
}

impl ChannelTransportContainer {
    // Return a Clone of the inner transport
    fn to_low(&self) -> ChannelTransport {
        self.to_low.clone().clone()
    }

    // Return a Clone of the inner transport
    fn to_high(&self) -> ChannelTransport {
        self.to_high.clone().clone()
    }
}

/// MPSC Channel based transport
#[derive(Debug, Clone)]
pub(crate) struct ChannelTransport {
    sender: Sender<Vec<u8>>,
    receiver: Receiver<Vec<u8>>,
}

#[async_trait]
impl BiStreamTransport for ChannelTransport {
    fn send(&self, msg: Vec<u8>) -> Result<(), TransportError> {
        self.sender
            .send(msg)
            .map_err(|e| TransportError::SendError(Box::new(e)))
    }

    async fn recv_async(&self) -> Result<Vec<u8>, TransportError> {
        self.receiver
            .recv_async()
            .await
            .map_err(|x| TransportError::RecvError(Box::new(x)))
    }

    fn recv(&self) -> Result<Option<Vec<u8>>, TransportError> {
        match self.receiver.try_recv() {
            Ok(x) => Ok(Some(x)),
            Err(flume::TryRecvError::Empty) => Ok(None),
            // TODO: Currently we don't really have a way to know if the channel
            // is closed because the other thread paniced or if it was closed in
            // an orderly fashion. In the panic case we probably should emit
            // an error instead....
            Err(flume::TryRecvError::Disconnected) => Ok(None),
        }
    }
}

/// Generate a new pair of bi-directional transport clients.
/// Each client can send messages to and receive messages from the
/// other client
fn new_transport_pair() -> ChannelTransportContainer {
    let (tx1, rx1) = flume::unbounded();
    let (tx2, rx2) = flume::unbounded();
    let to_low = ChannelTransport {
        sender: tx1,
        receiver: rx2,
    };
    let to_high = ChannelTransport {
        sender: tx2,
        receiver: rx1,
    };
    ChannelTransportContainer { to_low, to_high }
}

mod test {
    use crate::runtime::{threaded::{InterThreadCommunication, Shared}, OperatorOperatorComm as _};

    /// check we can send and recv a single message on two transports
    #[test]
    fn send_recv_message() {
        let shared = Shared::default();
        let worker0 = InterThreadCommunication::new(shared.clone(), 0);
        let worker1 = InterThreadCommunication::new(shared.clone(), 1);

        let operator_0_42 = worker0.operator_to_operator(1, 1337).unwrap();
        let operator_1_1337 = worker1.operator_to_operator(0, 1337).unwrap();

        let val = vec![1, 8, 8, 7];
        operator_0_42.send(val.clone()).unwrap();
        assert_eq!(operator_1_1337.recv().unwrap().unwrap(), val);

        // and now the other way around!!
        let val = vec![7, 8, 8, 1];
        operator_1_1337.send(val.clone()).unwrap();
        assert_eq!(operator_0_42.recv().unwrap().unwrap(), val);
    }

    /// Check we don't give out an error on recv if the other side disconnected in
    /// an orderly way (e.g because it is done)
    #[test]
    fn no_error_orderly_disconnect() {
        let shared = Shared::default();
        let worker0 = InterThreadCommunication::new(shared.clone(), 0);
        let worker1 = InterThreadCommunication::new(shared.clone(), 1);
        let operator_0_42 = worker0.operator_to_operator(1, 42).unwrap();
        let operator_1_42 = worker1.operator_to_operator(0, 42).unwrap();

        // now drop one of them
        drop(operator_0_42);
        // this should not be an error

        assert!(operator_1_42.recv().is_ok());
        assert!(operator_1_42.recv_all().all(|x| x.is_ok()));
    }

    /// Check we can reconnect channels
    #[test]
    fn reconnect() {
        let shared = Shared::default();
        let worker0 = InterThreadCommunication::new(shared.clone(), 0);
        let worker1 = InterThreadCommunication::new(shared.clone(), 1);

        let operator_0_42 = worker0.operator_to_operator(1, 1337).unwrap();
        let operator_1_1337 = worker1.operator_to_operator(0, 1337).unwrap();

        let val = vec![1, 8, 8, 7];
        operator_0_42.send(val.clone()).unwrap();
        assert_eq!(operator_1_1337.recv().unwrap().unwrap(), val);

        drop(operator_0_42);
        let operator_0_42 = worker0.operator_to_operator(1, 1337).unwrap();
        let val = vec![22];
        operator_0_42.send(val.clone()).unwrap();
        assert_eq!(operator_1_1337.recv().unwrap().unwrap(), val);

        // other way around
        drop(operator_1_1337);
        let operator_1_1337 = worker1.operator_to_operator(0, 1337).unwrap();
        let val = vec![100];
        operator_1_1337.send(val.clone()).unwrap();
        assert_eq!(operator_0_42.recv().unwrap().unwrap(), val);
    }
}
