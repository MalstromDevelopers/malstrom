use std::{marker::PhantomData, pin::Pin, task::{Context, Poll}};

use async_trait::async_trait;
use futures::Stream;
use pin_project::pin_project;
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;
use tracing::debug;

use crate::types::{OperatorId, WorkerId, distributable::Distributable};

/// A backend facilitating inter-worker communication in malstrom.
/// This trait defines the methods required to establish communication channels
/// between operators running on different workers.
#[async_trait]
pub trait OperatorOperatorComm {
    /// Creates a new sender for sending messages to a specific operator on a specific worker.
    ///
    /// # Arguments
    /// * `to_worker` - The ID of the worker hosting the target operator.
    /// * `to_operator` - The ID of the target operator.
    async fn new_sender(
        &self,
        to_worker: WorkerId,
        to_operator: OperatorId,
    ) -> Result<Box<dyn super::StreamSender>, Box<dyn std::error::Error>>;

    /// Creates a new receiver for receiving messages from a specific operator on a specific worker.
    ///
    /// # Arguments
    /// * `from_worker` - The ID of the worker hosting the source operator.
    /// * `from_operator` - The ID of the source operator.
    async fn new_receiver(
        &self,
        from_worker: WorkerId,
        from_operator: OperatorId,
    ) -> Result<Box<dyn super::StreamReceiver>, Box<dyn std::error::Error>>;
}

/// A sender for sending messages to an operator on another worker.
/// This struct encapsulates the sender side of the communication channel.
pub(crate) struct OperatorCommSender<T> {
    sender: Box<dyn super::StreamSender>,
    msg_type: PhantomData<T>,
}

impl<T> OperatorCommSender<T>
where
    T: super::Distributable,
{
    /// Creates a new [`OperatorCommSender`] for sending messages to a specific operator.
    ///
    /// # Arguments
    /// * `to_worker` - The ID of the worker hosting the target operator.
    /// * `to_operator` - The ID of the target operator.
    /// * `backend` - The backend implementing the [`OperatorOperatorComm`] trait.
    pub(crate) async fn new<Backend: OperatorOperatorComm + ?Sized>(
        to_worker: WorkerId,
        to_operator: OperatorId,
        backend: &Backend,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let sender = backend.new_sender(to_worker, to_operator).await?;
        Ok(Self {
            sender,
            msg_type: PhantomData,
        })
    }

    /// Sends a message to the target operator.
    ///
    /// # Arguments
    /// * `msg` - The message to send.
    pub(crate) async fn send(&self, msg: T) {
        let encoded = T::encode(msg);
        self.sender.send(encoded).await.expect("Backend send error")
    }
}

/// A receiver for receiving messages from an operator on another worker.
/// This struct encapsulates the receiver side of the communication channel.
pub(crate) struct OperatorCommReceiver<T> {
    receiver: Box<dyn super::StreamReceiver>,
    msg_type: PhantomData<T>,
}

impl<T> OperatorCommReceiver<T>
where
    T: Distributable,
{
    /// Creates a new [`OperatorCommReceiver`] for receiving messages from a specific operator.
    ///
    /// # Arguments
    /// * `from_worker` - The ID of the worker hosting the source operator.
    /// * `from_operator` - The ID of the source operator.
    /// * `backend` - The backend implementing the [`OperatorOperatorComm`] trait.
    pub(crate) async fn new<Backend: OperatorOperatorComm + ?Sized>(
        from_worker: WorkerId,
        from_operator: OperatorId,
        backend: &Backend,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let receiver = backend.new_receiver(from_worker, from_operator).await?;
        Ok(Self {
            receiver,
            msg_type: PhantomData,
        })
    }

    /// Receives a message from the source operator.
    /// TODO: Remove because we have [crate::channels::recv_trait::Receiver::recv]
    pub(crate) async fn receive(&self) -> T {
        let encoded = self.receiver.recv().await.expect("Backend receive error");
        T::decode(&encoded)
    }
}

impl<T> crate::channels::recv_trait::Receiver for OperatorCommReceiver<T>
where
    T: Distributable,
 {
    type Output = T;
    
    /// Receives a message from the source operator.
    async fn recv(&mut self) -> Self::Output {
        let encoded = self.receiver.recv().await.expect("Backend receive error");
        T::decode(&encoded)
    }
}