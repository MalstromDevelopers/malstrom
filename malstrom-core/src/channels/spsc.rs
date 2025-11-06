//! A dead simple non-threaded unbounded channel
//! Inspiration taken from https://docs.rs/local-channel

use std::{
    cell::RefCell,
    collections::VecDeque,
    rc::Rc,
    task::{Poll, Waker},
};

type Shared<T> = Rc<RefCell<SharedInner<T>>>;

// TODO: Make this configurable
static CAPACITY: usize = 1024;

#[derive(Debug)]
struct SharedInner<T> {
    queue: VecDeque<T>,
    capacity: usize,
    has_receiver: bool,
    recv_waker: Option<Waker>,
    send_waker: Option<Waker>,
}
impl<T> SharedInner<T> {
    // /// push a value into the shared buffer
    // fn push(&mut self, value: T) {
    //     if self.has_receiver {
    //         self.queue.push_back(value);
    //     }
    // }

    // /// Pop the last value from the buffer, None if the buffer
    // /// is empty
    // fn pop(&mut self) -> Option<T> {
    //     self.queue.pop_front()
    // }

    // /// Check whether the buffer is empty
    // fn is_empty(&self) -> bool {
    //     self.queue.is_empty()
    // }

    /// Get a reference to the last value if any
    /// without removing it
    fn peek(&self) -> Option<&T> {
        self.queue.front()
    }
}
impl<T> Default for SharedInner<T> {
    fn default() -> Self {
        Self {
            queue: Default::default(),
            has_receiver: Default::default(),
            capacity: CAPACITY,
            recv_waker: None,
            send_waker: None,
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
    pub fn send(&self, msg: T) -> Send<'_, T> {
        Send {
            sender: &self,
            value: RefCell::new(Some(msg)),
        }
    }

    /// Send a message without respecting the capacity
    pub(crate) fn force_send(&self, msg: T) {
        let mut shared = self.shared.borrow_mut();
        shared.queue.push_back(msg);
        if let Some(waker) = shared.recv_waker.take() {
            waker.wake();
        }
    }
}

pub struct Send<'a, T> {
    sender: &'a Sender<T>,
    value: RefCell<Option<T>>,
}

impl<'a, T> Future for Send<'a, T> {
    type Output = ();

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut shared = self.sender.shared.borrow_mut();
        if shared.capacity > shared.queue.len() {
            if let Some(v) = self.value.take() {
                shared.queue.push_back(v);
            }
            // wake up receiver
            shared.recv_waker.take().map(Waker::wake);
            Poll::Ready(())
        } else {
            // wake any receiver to free up the queue
            shared.recv_waker.take().map(Waker::wake);
            // set a waker so we can try again when the receiver frees up the queue
            shared.send_waker = Some(cx.waker().clone());
            Poll::Pending
        }
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
    pub fn recv(&self) -> Receive<'_, T> {
        Receive(self)
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

pub struct Receive<'a, T>(&'a Receiver<T>);

impl<'a, T> Future for Receive<'a, T> {
    type Output = T;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let mut shared = self.0.shared.borrow_mut();
        match shared.queue.pop_front() {
            Some(x) => {
                // tell any waiting sender there is space in queue
                shared.send_waker.take().map(Waker::wake);
                Poll::Ready(x)
            }
            None => {
                // let sender know we are waiting for a message
                shared.send_waker = Some(cx.waker().clone());
                debug_assert!({
                    // 2: One sender, one receiver
                    // <2: Only receiver (this one) left
                    drop(shared);
                    Rc::strong_count(&self.0.shared) <= 2
                });
                Poll::Pending
            }
        }
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
    #[tokio::test]
    async fn send_and_receive() {
        let (tx, rx) = unbounded();
        tx.send("HelloWorld").await;
        assert_eq!(rx.recv().await, "HelloWorld")
    }

    /// Sends and receives messages in the correct
    /// order
    #[tokio::test]
    async fn send_and_receive_order() {
        let (tx, rx) = unbounded();
        tx.send("HelloWorld").await;
        tx.send("FooBar").await;
        assert_eq!(rx.recv().await, "HelloWorld");
        assert_eq!(rx.recv().await, "FooBar");
    }

    #[tokio::test]
    async fn peek_does_not_remove() {
        let (tx, rx) = unbounded();
        tx.send(42);
        tx.send(13);
        assert_eq!(rx.peek_apply(|x| *x).unwrap(), 42);
        assert_eq!(rx.recv().await, 42);
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
