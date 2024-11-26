//! A dead simple non-threaded unbounded channel
//! Inspiration taken from https://docs.rs/local-channel

use std::{
    cell::RefCell,
    collections::LinkedList,
    rc::Rc,
};

type Shared<T> = Rc<RefCell<SharedInner<T>>>;

#[derive(Debug)]
struct SharedInner<T> {
    buffer: LinkedList<T>,
    has_receiver: bool,
}
impl<T> SharedInner<T> {
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

    /// Get a reference to the last value if any
    /// without removing it
    fn peek(&self) -> Option<&T> {
        self.buffer.front()
    }
}
impl<T> Default for SharedInner<T> {
    fn default() -> Self {
        Self {
            buffer: Default::default(),
            has_receiver: Default::default(),
        }
    }
}

/// A sender for sending messages into the channel
#[derive(Debug)]
pub struct Sender<T> {
    shared: Shared<T>,
}
impl<T> Sender<T> {
    /// Send a message into the channel. Note that sending
    /// to a channel without any receiver drops the message
    pub fn send(&self, msg: T) -> () {
        self.shared.borrow_mut().push(msg);
    }
    pub(crate) fn strong_count(&self) -> usize {
        Rc::strong_count(&self.shared)
    }
}

/// A receiver for receiving messages from the channel
#[derive(Debug)]
pub struct Receiver<T> {
    shared: Shared<T>,
}
impl<T> Receiver<T> {
    fn new(shared: Shared<T>) -> Self {
        shared.borrow_mut().has_receiver = true;
        Self { shared }
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

    /// Apply a function to a reference of the next receivable
    /// element if any.
    /// Returns None if there currently is no next element
    pub fn peek_apply<U, F: FnOnce(&T) -> U>(&self, func: F) -> Option<U> {
        self.shared.borrow().peek().map(func)
    }
}
impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.shared.borrow_mut().has_receiver = false
    }
}

pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    let shared = Rc::new(RefCell::new(SharedInner::default()));
    let sender = Sender {
        shared: shared.clone(),
    };
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

    /// Sends and receives messages in the correct
    /// order
    #[test]
    fn send_and_receive_order() {
        let (tx, rx) = unbounded();
        tx.send("HelloWorld");
        tx.send("FooBar");
        assert_eq!(rx.recv().unwrap(), "HelloWorld");
        assert_eq!(rx.recv().unwrap(), "FooBar");
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

    #[test]
    fn peek_does_not_remove() {
        let (tx, rx) = unbounded();
        tx.send(42);
        tx.send(13);
        assert_eq!(rx.peek_apply(|x| *x).unwrap(), 42);
        assert_eq!(rx.recv(), Some(42));
        assert_eq!(rx.peek_apply(|x| *x).unwrap(), 13);
    }

    /// copied from https://users.rust-lang.org/t/a-macro-to-assert-that-a-type-does-not-implement-trait-bounds/31179
    macro_rules! assert_not_impl {
        ($x:ty, $($t:path),+ $(,)*) => {
            const _: fn() -> () = || {
                struct Check<T: ?Sized>(T);
                trait AmbiguousIfImpl<A> { fn some_item() { } }

                impl<T: ?Sized> AmbiguousIfImpl<()> for Check<T> { }
                impl<T: ?Sized $(+ $t)*> AmbiguousIfImpl<u8> for Check<T> { }

                <Check::<$x> as AmbiguousIfImpl<_>>::some_item()
            };
        };
    }

    /// Check neither the sender nor receiver implement Clone or Copy, making them
    /// SPSC
    #[test]
    fn is_spsc() {
        assert_not_impl!(Sender<()>, Copy);
        assert_not_impl!(Sender<()>, Clone);
        assert_not_impl!(Receiver<()>, Copy);
        assert_not_impl!(Receiver<()>, Clone);
    }
}
