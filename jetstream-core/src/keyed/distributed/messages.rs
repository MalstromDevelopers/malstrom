use std::{rc::Rc, sync::Mutex};

use derive_new::new;
use indexmap::{IndexMap, IndexSet};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{snapshot::Barrier, time::Epoch, DataMessage, Key, OperatorId, ShutdownMarker, WorkerId};

use super::Version;

/// These are all message types which may come into this operator
/// Either locally or remotely
pub(super) enum IncomingMessage<K, V, T, P> {
    /// Optionally with a version if it comes from remote
    Data(DataMessage<K, V, T>, Option<Version>),
    Epoch(Epoch<T>),
    AbsBarrier(Barrier<P>),
    ScaleRemoveWorker(IndexSet<WorkerId>),
    ScaleAddWorker(IndexSet<WorkerId>),
    ShutdownMarker(ShutdownMarker),
    Done(DoneMessage),
    Acquire(NetworkAcquire<K>)

    // Interrogate, Collect, DropKey and the "normal" Acquire
    // can not come into here, since this is the start of the
    // persistence region
}

/// Messages which may go out of this operator locally
pub(super) enum LocalOutgoingMessage<K, V, T, P> {
    Data(DataMessage<K, V, T>),
    Epoch(Epoch<T>),
    AbsBarrier(Barrier<P>),
    ScaleRemoveWorker(IndexSet<WorkerId>),
    ScaleAddWorker(IndexSet<WorkerId>),
    ShutdownMarker(ShutdownMarker),
    Interrogate(Interrogate<K>),
    Collect(Collect<K>),
    Acquire(Acquire<K>),
    DropKey(K),
}

#[derive(Debug)]
pub(super) enum RescaleMessage {
    ScaleRemoveWorker(IndexSet<WorkerId>),
    ScaleAddWorker(IndexSet<WorkerId>),
}

#[derive(Serialize, Deserialize, new)]
pub(super) struct TargetedMessage<K, V, T> {
    pub(super) target: WorkerId,
    pub(super) version: Version,
    pub(super) message: DataMessage<K, V, T>
}

/// Message which may go out of this operator remotely
/// Or come in from a remote worker
#[derive(Serialize, Deserialize)]
pub(super) enum RemoteMessage<K, V, T> {
    Data(TargetedMessage<K, V, T>),
    Done(DoneMessage),
    Acquire(NetworkAcquire<K>)
}

/// Messages which may flow out of this operator
pub(super) enum OutgoingMessage<K, V, T, P> {
    Local(LocalOutgoingMessage<K, V, T, P>),
    Remote(RemoteMessage<K, V, T>)
}

#[derive(Debug, Clone)]
pub struct Interrogate<K> {
    shared: Rc<Mutex<IndexSet<K>>>,
}
impl<K> Interrogate<K>
where
    K: Key,
{
    pub fn add_keys(&mut self, keys: &[K]) {
        let mut guard = self.shared.lock().unwrap();
        for key in keys.iter().cloned() {
            guard.insert(key);
        }
    }

    pub(super) fn ref_count(&self) -> usize {
        Rc::strong_count(&self.shared)
    }
}

#[derive(Debug, Clone)]
pub struct Collect<K> {
    pub key: K,
    collection: Rc<Mutex<IndexMap<OperatorId, Vec<u8>>>>,
}
impl<K> Collect<K>
where
    K: Key,
{
    fn new(key: K) -> Self {
        Self {
            key,
            collection: Rc::new(Mutex::new(IndexMap::new())),
        }
    }
    pub fn add_state<S: Serialize>(&mut self, operator_id: OperatorId, state: &S) {
        let encoded = bincode::serde::encode_to_vec(state, bincode::config::standard())
            .expect("State serialization error");
        self.collection.lock().unwrap().insert(operator_id, encoded);
    }

    fn ref_count(&self) -> usize {
        Rc::strong_count(&self.collection)
    }
}

#[derive(Debug, Clone)]
pub struct Acquire<K> {
    key: K,
    collection: Rc<Mutex<IndexMap<OperatorId, Vec<u8>>>>,
}
impl<K> Acquire<K>
where
    K: Key,
{
    pub fn take_state<S: DeserializeOwned>(&self, operator_id: &OperatorId) -> Option<(K, S)> {
        self.collection
            .lock()
            .unwrap()
            .swap_remove(operator_id)
            .map(|x| {
                bincode::serde::decode_from_slice(&x, bincode::config::standard())
                    .expect("State deserialization error")
                    .0
            })
            .map(|s| (self.key.clone(), s))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct NetworkAcquire<K> {
    key: K,
    collection: IndexMap<OperatorId, Vec<u8>>,
}

// impl<K> From<Acquire<K>> for NetworkAcquire<K> {
//     fn from(value: Acquire<K>) -> Self {
//         Self {
//             key: value.key,
//             collection: value.collection.into_inner().unwrap(),
//         }
//     }
// }
impl<K> From<NetworkAcquire<K>> for Acquire<K> {
    fn from(value: NetworkAcquire<K>) -> Self {
        Self {
            key: value.key,
            collection: Rc::new(Mutex::new(value.collection)),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub(super) struct DoneMessage {
    worker_id: WorkerId,
    version: Version,
}
