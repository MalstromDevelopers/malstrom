use std::{ops::RangeBounds, rc::Rc, sync::Mutex};

/// Test utilities for JetStream
use itertools::Itertools;

use crate::{
    config::Config, snapshot::NoPersistence, stream::jetstream::JetStreamBuilder, time::NoTime,
    NoData, NoKey, Worker,
};

/// A Helper to write values into a shared vector and take them out
/// again.
/// This is mainly useful to extract values from a stream in unit tests.
/// This struct uses an Rc<Mutex<Vec<T>> internally, so it can be freely
/// cloned
#[derive(Clone)]
pub struct VecCollector<T> {
    inner: Rc<Mutex<Vec<T>>>,
}
impl<T> VecCollector<T> {
    pub fn new() -> Self {
        VecCollector {
            inner: Rc::new(Mutex::new(Vec::new())),
        }
    }

    /// Put a value into this collector
    pub fn give(&self, value: T) -> () {
        self.inner.lock().unwrap().push(value)
    }

    /// Take the given range out of this collector
    pub fn drain_vec<R: RangeBounds<usize>>(&self, range: R) -> Vec<T> {
        self.inner.lock().unwrap().drain(range).collect()
    }

    /// Returns the len of the contained vec
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }
}

/// Creates a JetStream worker with no persistence and
/// a JetStream stream, which does not produce any messages
pub fn get_test_stream() -> (
    Worker<NoPersistence>,
    JetStreamBuilder<NoKey, NoData, NoTime, NoPersistence>,
) {
    let mut worker = Worker::new(|| false);
    let stream = worker.new_stream();
    (worker, stream)
}

/// Creates configs to run N workers locally
/// The workers will use port 29091 + n with n being
/// in range 0..N
pub fn get_test_configs<const N: usize>() -> [Config; N] {
    let ports = (0..N).into_iter().map(|i| 29091 + i).collect_vec();
    let addresses: Vec<tonic::transport::Uri> = ports
        .iter()
        .map(|x| format!("http://localhost:{x}").parse().unwrap())
        .collect_vec();

    (0..N)
        .into_iter()
        .map(|i| Config {
            worker_id: i,
            port: 29091 + u16::try_from(i).unwrap(),
            cluster_addresses: addresses.iter().map(|x| x.clone()).collect(),
        })
        .collect_vec()
        .try_into()
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_test_configs() {
        let [a, b, c] = get_test_configs();

        assert_eq!(a.worker_id, 0);
        assert_eq!(b.worker_id, 1);
        assert_eq!(c.worker_id, 2);

        assert_eq!(a.port, 29091);
        assert_eq!(b.port, 29092);
        assert_eq!(c.port, 29093);

        assert_eq!(
            a.get_peer_uris(),
            vec![
                (1, "http://localhost:29092".parse().unwrap()),
                (2, "http://localhost:29093".parse().unwrap())
            ]
        );
        assert_eq!(
            b.get_peer_uris(),
            vec![
                (0, "http://localhost:29091".parse().unwrap()),
                (2, "http://localhost:29093".parse().unwrap())
            ]
        );
        assert_eq!(
            c.get_peer_uris(),
            vec![
                (0, "http://localhost:29091".parse().unwrap()),
                (1, "http://localhost:29092".parse().unwrap())
            ]
        );
    }

    #[test]
    fn test_vec_collector() {
        let col = VecCollector::new();
        let col_a = col.clone();

        for i in 0..5 {
            col.give(i)
        }

        // the cloned one should return these values
        let collected = col_a.drain_vec(..);
        assert_eq!(collected, (0..5).collect_vec())
    }
}
