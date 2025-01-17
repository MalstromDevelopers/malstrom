use crate::{
    operators::SinkFullImpl,
    sinks::StatelessSinkImpl,
    types::{Data, DataMessage, MaybeData, MaybeKey, MaybeTime, Message},
};
use std::{ops::RangeBounds, sync::Arc, sync::Mutex};

/// A Helper to write values into a shared vector and take them out
/// again.
/// This is mainly useful to extract values from a stream in unit tests.
/// This struct uses an Arc<Mutex<Vec<T>> internally, so it can be freely
/// cloned
#[derive(Clone)]
pub struct VecSink<T> {
    inner: Arc<Mutex<Vec<T>>>,
}
impl<T> Default for VecSink<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> VecSink<T> {
    pub fn new() -> Self {
        VecSink {
            inner: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Put a value into this sink
    pub fn give(&self, value: T) {
        self.inner.lock().unwrap().push(value)
    }

    /// Take the given range out of this sink
    pub fn drain_vec<R: RangeBounds<usize>>(&self, range: R) -> Vec<T> {
        self.inner.lock().unwrap().drain(range).collect()
    }
    /// Returns the len of the contained vec
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().is_empty()
    }
}

impl<T> IntoIterator for VecSink<T> {
    type Item = T;

    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.drain_vec(..).into_iter()
    }
}

impl<K, V, T> StatelessSinkImpl<K, V, T> for VecSink<DataMessage<K, V, T>>
where
    K: MaybeKey,
    V: Data,
    T: MaybeTime,
{
    fn sink(&mut self, msg: DataMessage<K, V, T>) {
        self.give(msg);
    }
}

impl<K, V, T> SinkFullImpl<K, V, T> for VecSink<Message<K, V, T>>
where
    K: MaybeKey,
    V: MaybeData,
    T: MaybeTime,
{
    fn sink_full(&mut self, msg: Message<K, V, T>) {
        self.give(msg);
    }
}
