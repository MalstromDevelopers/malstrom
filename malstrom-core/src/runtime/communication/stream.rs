use std::marker::PhantomData;

use async_trait::async_trait;

/// Bi-directional streaming transport where each end can send many messages to the other without
/// requiring a response
#[async_trait]
pub trait StreamSender: 'static {
    /// Send a single message to the Operator on the other end of the transport.
    ///
    /// Fallible transports must implement applicable retry logic internally,
    /// an error should only be returned on **unrecoverable conditions**.
    async fn send(&self, msg: Vec<u8>) -> Result<(), Box<dyn std::error::Error>>;
}

/// Bi-directional streaming transport where each end can send many messages to the other without
/// requiring a response
#[async_trait]
pub trait StreamReceiver: 'static {
    /// Wait until a message becomes available
    /// Fallible transports must implement applicable retry logic internally,
    /// an error should only be returned on **unrecoverable conditions**.
    async fn recv(&self) -> Result<Vec<u8>, Box<dyn std::error::Error>>;
}

/// A typed client for point to point communication between operators
/// on different workers and possibly different machines
pub struct StreamSendClient<T> {
    transport: Box<dyn StreamSender>,
    message_type: PhantomData<T>,
}

/// A typed client for point to point communication between operators
/// on different workers and possibly different machines
pub struct StreamRecvClient<T> {
    transport: Box<dyn StreamReceiver>,
    message_type: PhantomData<T>,
}
