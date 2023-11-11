use std::{rc::Rc, sync::RwLock};

/// A vastly simplified version of tokios watch channel
#[derive(Debug)]
struct Shared<T> {
    /// The most recent value.
    value: RwLock<T>,
}

#[derive(Debug)]
pub struct Receiver<T> {
    shared: Rc<Shared<T>>,
}

#[derive(Debug, Clone)]
pub struct Sender<T> {
    shared: Rc<Shared<T>>,
}

pub fn channel<T>(init: T) -> (Sender<T>, Receiver<T>) {
    let shared = Rc::new(Shared {
        value: RwLock::new(init),
    });

    let tx = Sender {
        shared: shared.clone(),
    };
    let rx = Receiver {
        shared: shared.clone(),
    };
    (tx, rx)
}

impl<T> Receiver<T>
where
    T: Clone,
{
    pub fn read(&self) -> T {
        self.shared.value.read().unwrap().clone()
    }
}

impl<T> Receiver<T>
where
    T: Clone,
{
    pub fn recv(&self) -> Option<T> {
        Some(self.read())
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T> Sender<T>
where
    T: Clone,
{
    pub fn send(&mut self, new: T) {
        self.write(new)
    }
}

impl<T> Sender<T>
where
    T: Clone,
{
    pub fn write(&self, new: T) {
        *self.shared.value.write().unwrap() = new;
    }
    pub fn subscribe(&self) -> Receiver<T> {
        Receiver {
            shared: self.shared.clone(),
        }
    }

    pub fn read(&self) -> T {
        self.shared.value.read().unwrap().clone()
    }
}

#[cfg(test)]
mod tests {

    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_send_recv() {
        let (tx, rx) = channel(0);
        assert_eq!(rx.read(), 0);
        tx.write(1);
        assert_eq!(rx.read(), 1);
    }

    #[test]
    fn test_send_recv_multiple() {
        let (tx, rx) = channel(0);
        let rx_2 = rx.clone();

        assert_eq!(rx.read(), 0);
        assert_eq!(rx_2.read(), 0);

        tx.write(1);

        assert_eq!(rx.read(), 1);
        assert_eq!(rx_2.read(), 1);
    }

    fn test_subscribe() {
        let (tx, _) = channel(0);
        let rx = tx.subscribe();
        assert_eq!(rx.read(), 0);
        tx.write(1);
        assert_eq!(rx.read(), 1);
    }
}
