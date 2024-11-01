//! A dead simple non-threaded unbounded channel
//! Inspiration taken from https://docs.rs/local-channel

use std::{cell::RefCell, collections::LinkedList, rc::Rc};

type Shared<T> = Rc<RefCell<SharedInner<T>>>;

#[derive(Debug)]
struct SharedInner<T> {
    buffer: LinkedList<T>,
    has_receiver: bool
}
impl <T> SharedInner<T> {
    /// push a value into the shared buffer
    fn push(&mut self, value: T) -> () {
        if self.has_receiver {
            self.buffer.push_back(value);
        }
    }

    /// Pop the last value from the buffer, None if the buffer
    /// is empty
    fn pop(&mut self) -> Option<T> {
        self.buffer.pop_front()
    }

    /// Check whether the buffer is empty
    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
}
impl <T> Default for SharedInner<T> {
    fn default() -> Self {
        Self { buffer: Default::default(), has_receiver: Default::default() }
    }
}


/// A sender for sending messages into the channel
#[derive(Debug)]
pub struct Sender<T> {shared: Shared<T>}
impl <T> Sender<T> {
    /// Send a message into the channel. Note that sending
    /// to a channel without any receiver drops the message
    pub fn send(&self, msg: T) -> () {
        self.shared.borrow_mut().push(msg);
    }
}
impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self { shared: self.shared.clone() }
    }
}

/// A receiver for receiving messages from the channel
#[derive(Debug)]
pub struct Receiver<T> {shared: Shared<T>}
impl <T> Receiver<T> {

    fn new(shared: Shared<T>) -> Self {
        shared.borrow_mut().has_receiver = true;
        Self { shared: shared }
    }

    /// Receive a message from the channel, returns None if the channel
    /// contains no messages
    pub fn recv(&self) -> Option<T> {
        self.shared.borrow_mut().pop()
    }

    /// Check whether this channel is empty
    pub fn is_empty(&self) -> bool {
        self.shared.borrow().is_empty()
    }
}
impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.shared.borrow_mut().has_receiver = false
    }
}

pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    let shared = Rc::new(RefCell::new(SharedInner::default()));
    let sender = Sender{shared: shared.clone()};
    let receiver = Receiver::new(shared);
    (sender, receiver)
}


#[cfg(test)]
mod tests {
    use super::*;
    
    /// If sending without a receiver the message should be dropped
    #[test]
    fn sending_without_receiver() {
        let foo = Rc::new(42);
        let (tx, _) = unbounded();
        tx.send(foo.clone());
        // we should be able to unwrap since the other reference
        // was dropped due to no receiver
        assert!(Rc::try_unwrap(foo).is_ok())
    }

    /// Send a message and receive it
    #[test]
    fn send_and_receive() {
        let (tx, rx) = unbounded();
        tx.send("HelloWorld");
        assert_eq!(rx.recv().unwrap(), "HelloWorld")
    }

    /// Check the is_empty method on the receiver
    #[test]
    fn is_empty_receiver() {
        let (tx, rx) = unbounded();
        assert!(rx.is_empty());
        tx.send("Foo");
        assert!(!rx.is_empty());
        let _ = rx.recv();
        assert!(rx.is_empty());
    }
}