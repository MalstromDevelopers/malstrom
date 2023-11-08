/// These "useless" Receiver and Sender types
/// exists, so you can build operators which require
/// the Sender or Receiver trait, but can not have any meaningful
/// Input/Output

/// Does nothing
pub struct Sender;
pub struct Receiver;

impl <T> super::Sender<T> for Sender {
    fn send(&mut self, _: T) -> Result<(), super::CommunicationError> {
        Ok(())
    }
}

impl <T> super::Receiver<T> for Receiver {
    fn recv(&self) -> Option<T> {
        None
    }
}