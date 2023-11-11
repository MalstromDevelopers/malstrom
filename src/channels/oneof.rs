/// One-of channels
/// One-of Channels are MPMC channels, where each value is delivered
/// to the first Consumer which receives it and **only** that consumer.
use crossbeam::channel as cb;

#[derive(Debug, Clone)]
pub struct Receiver<T>(cb::Receiver<T>);
#[derive(Debug, Clone)]
pub struct Sender<T>(cb::Sender<T>);

pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = cb::unbounded();
    (Sender(tx), Receiver(rx))
}

impl<T> super::Receiver<T> for Receiver<T> {
    /// Try to receive a value. If no value is available,
    /// return None
    fn recv(&self) -> Option<T> {
        self.0.try_recv().ok()
    }
}

impl<T> super::Sender<T> for Sender<T> {
    /// send a value into the channel
    fn send(&mut self, msg: T) -> Result<(), super::CommunicationError> {
        self.0.send(msg).map_err(|_| super::CommunicationError::ChannelClosed)
    }
}
