use std::marker::PhantomData;

use crate::{
    runtime::communication::ReqResResponder,
    types::{WorkerId, distributable::Distributable},
};

/// A communication implementation for sending messages from a worker to the coordinator.
///
/// This trait defines the methods required to establish communication channels between
/// workers and the coordinator.
pub trait WorkerCoordinatorComm {
    /// Establishes a connection from a worker to the coordinator.
    ///
    /// # Returns
    /// A boxed dynamic trait object implementing `ReqResReceiver`.
    /// The future completes once the coordinator has accepted the connection.
    async fn worker_to_coordinator(
        &self,
    ) -> Result<impl super::ReqResReceiver, Box<dyn std::error::Error>>;

    /// Establishes a connection from the coordinator to a specific worker.
    ///
    /// # Arguments
    /// * `to_worker` - The ID of the worker to connect to.
    async fn coordinator_to_worker(
        &self,
        to_worker: WorkerId,
    ) -> Result<impl super::ReqResSender, Box<dyn std::error::Error>>;
}

/// Client used by the Coordinator to communicate with workers.
///
/// This struct encapsulates the sender side of the communication channel,
/// allowing the coordinator to send messages to workers and await responses.
pub(crate) struct CoordinatorClient {
    sender: Box<dyn super::ReqResSender>,
}

impl CoordinatorClient {
    /// Creates a new `CoordinatorClient` for communicating with a specific worker.
    ///
    /// # Arguments
    /// * `to_worker` - The ID of the worker to communicate with.
    /// * `backend` - The backend implementing the `WorkerCoordinatorComm` trait.
    pub(crate) async fn new<Backend: WorkerCoordinatorComm>(
        to_worker: WorkerId,
        backend: &Backend,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let sender = backend.coordinator_to_worker(to_worker).await?;
        let sender = Box::new(sender);
        Ok(Self { sender })
    }

    /// Sends a message to the worker and waits for the response.
    ///
    /// # Arguments
    /// * `msg` - The message to send.
    ///
    /// # Returns
    /// The decoded response from the worker.
    pub(crate) async fn send<TSend, TRecv>(&self, msg: TSend) -> TRecv
    where
        TSend: Distributable,
        TRecv: Distributable,
    {
        let encoded = TSend::encode(msg);
        let response = self.sender.send(encoded).await.expect("Backend send error");
        TRecv::decode(&response)
    }
}

/// Client used by the Worker to communicate with the coordinator.
///
/// This struct encapsulates the receiver side of the communication channel,
/// allowing the worker to receive messages from the coordinator and respond to them.
pub(crate) struct WorkerClient {
    receiver: Box<dyn super::ReqResReceiver>,
}

impl WorkerClient {
    /// Creates a new `WorkerClient` for communicating with the coordinator.
    ///
    /// # Arguments
    /// * `backend` - The backend implementing the `WorkerCoordinatorComm` trait.
    ///
    /// # Returns
    /// A `Result` containing the `WorkerClient` or an error from the backend.
    pub(crate) async fn new<Backend: WorkerCoordinatorComm>(
        backend: Backend,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let receiver = backend.worker_to_coordinator().await?;
        let receiver = Box::new(receiver);
        Ok(Self { receiver })
    }

    /// Waits for a message from the coordinator and prepares a responder.
    ///
    /// # Returns
    /// A tuple containing the decoded message and a `WorkerResponder` for sending a response.
    pub(crate) async fn recv<TRecv, TResp>(&self) -> (TRecv, WorkerResponder<TResp>)
    where
        TRecv: Distributable,
        TResp: Distributable,
    {
        let (msg, responder) = self.receiver.recv().await.expect("Backend receive error");
        let decoded = TRecv::decode(&msg);
        (
            decoded,
            WorkerResponder {
                responder,
                msg: PhantomData,
            },
        )
    }
}

/// A responder for sending a response back to the coordinator.
///
/// This struct is used by the worker to send a response after receiving a message
/// from the coordinator.
pub(crate) struct WorkerResponder<T> {
    responder: Box<dyn ReqResResponder>,
    msg: PhantomData<T>,
}

impl<T> WorkerResponder<T>
where
    T: Distributable,
{
    /// Sends a response back to the coordinator.
    ///
    /// # Arguments
    /// * `msg` - The response message to send.
    pub(crate) async fn respond(mut self, msg: T) {
        let encoded = T::encode(msg);
        self.responder
            .respond(encoded)
            .await
            .expect("Backend respond error")
    }
}
