use async_trait::async_trait;

/// A trait for sending request-response messages over a transport.
/// Implementations of this trait are responsible for sending a message and awaiting a response.
#[async_trait]
pub trait ReqResSender: Send + Sync + 'static {
    /// Sends a single message to the other end of the transport and awaits a response.
    ///
    /// # Arguments
    /// * `msg` - The message to send, encoded as a byte vector.
    ///
    /// # Returns
    /// A `Result` containing the response as a byte vector, or an error if the operation fails.
    ///
    /// # Notes
    /// Fallible transports must implement applicable retry logic internally.
    /// An error should only be returned on **unrecoverable conditions**.
    async fn send(&self, msg: Vec<u8>) -> Result<Vec<u8>, Box<dyn std::error::Error>>;
}

/// A trait for receiving request-response messages over a transport.
/// Implementations of this trait are responsible for waiting for incoming messages and providing a responder.
#[async_trait]
pub trait ReqResReceiver: Send + Sync + 'static {
    /// Waits until a message becomes available and returns it along with a responder.
    ///
    /// # Returns
    /// A `Result` containing a tuple of the received message (as a byte vector) and a `Responder` for sending a response.
    /// An error is returned if the operation fails.
    ///
    /// # Notes
    /// Fallible transports must implement applicable retry logic internally.
    /// An error should only be returned on **unrecoverable conditions**.
    async fn recv(&self)
    -> Result<(Vec<u8>, Box<dyn ReqResResponder>), Box<dyn std::error::Error>>;
}

/// A trait for responding to a received message.
/// Implementations of this trait are responsible for sending a response back to the sender.
#[async_trait]
pub trait ReqResResponder: Send + Sync + 'static {
    /// Sends a response back to the sender of the original message.
    ///
    /// # Arguments
    /// * `msg` - The response message to send, encoded as a byte vector.
    ///
    /// # Returns
    /// An error if the operation fails.
    ///
    /// # Notes
    /// Fallible transports must implement applicable retry logic internally.
    /// An error should only be returned on **unrecoverable conditions**.
    ///
    /// # Implementation Note:
    /// For [dyn compatibility](https://doc.rust-lang.org/reference/items/traits.html#dyn-compatibility)
    /// this methods takes `self` by reference, however Implementations are free to ignore any calls
    /// to this method after the first call.
    async fn respond(&mut self, msg: Vec<u8>) -> Result<(), Box<dyn std::error::Error>>;
}
