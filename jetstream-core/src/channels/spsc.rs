/// This just wraps some crossbeam channels to turn them into SPSC
use crossbeam::channel as cb;

/// SPSC Receiver, does not implement clone
#[derive(Debug)]
pub struct Receiver<T>(cb::Receiver<T>);
/// SPSC Sender, does not implement clone
#[derive(Debug)]
pub struct Sender<T>(cb::Sender<T>);

pub fn unbounded<T>() -> (Sender<T>,Receiver<T>) {
    let (tx, rx) = cb::unbounded();
    (
        Sender(tx),
        Receiver(rx)
    )
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