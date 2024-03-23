use std::{fmt::Debug, rc::Rc, sync::Mutex};

use derive_new::new;
use indexmap::{IndexMap, IndexSet};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    snapshot::Barrier, time::Epoch, DataMessage, Key, OperatorId, ShutdownMarker, WorkerId,
};

use super::Version;

/// These are all message types which must be handled by the
/// distributor, coming from either locally or remotely
/// Either locally or remotely
// pub(super) enum IncomingMessage<K, V, T> {
//     /// Optionally with a version if it comes from remote
//     Data(DataMessage<K, V, T>, Option<Version>),
//     Done(DoneMessage),
//     //these we can handle without the distributor
//     // Epoch(Epoch<T>),
//     // Acquire(NetworkAcquire<K>)
//     // AbsBarrier(Barrier<P>),
//     // ShutdownMarker(ShutdownMarker),
//     // Rescale(RescaleMessage),

//     // Interrogate, Collect, DropKey and the "normal" Acquire
//     // can not come into here, since this is the start of the
//     // key region
// }

/// Messages which may go out of a distributor and flow downstream
#[derive(Clone)]
pub(super) enum LocalOutgoingMessage<K, V, T> {
    Data(DataMessage<K, V, T>),
    Interrogate(Interrogate<K>),
    Collect(Collect<K>),
    DropKey(K),
}

#[derive(Debug)]
pub(super) enum RescaleMessage {
    ScaleRemoveWorker(IndexSet<WorkerId>),
    ScaleAddWorker(IndexSet<WorkerId>),
}

#[derive(Serialize, Deserialize, new, Clone)]
pub(super) struct TargetedMessage<K, V, T> {
    pub(super) target: WorkerId,
    pub(super) version: Version,
    pub(super) message: DataMessage<K, V, T>,
}

/// Message which may go out of this operator remotely
/// Or come in from a remote worker
#[derive(Serialize, Deserialize, Clone)]
pub(super) enum RemoteMessage<K, V, T> {
    Data(TargetedMessage<K, V, T>),
    Done(DoneMessage),
    Acquire(NetworkAcquire<K>),
}

/// Messages which may flow out of this operator
#[derive(Clone)]
pub(super) enum OutgoingMessage<K, V, T> {
    Local(LocalOutgoingMessage<K, V, T>),
    Remote(RemoteMessage<K, V, T>),
    None,
}

#[derive(Clone)]
pub struct Interrogate<K> {
    shared: Rc<Mutex<IndexSet<K>>>,
    tester: Rc<dyn Fn(&K) -> bool>,
}
impl<K> Interrogate<K>
where
    K: Key,
{
    pub(super) fn new(
        shared: Rc<Mutex<IndexSet<K>>>,
        tester: impl Fn(&K) -> bool + 'static,
    ) -> Self {
        Self {
            shared,
            tester: Rc::new(tester),
        }
    }

    pub fn add_keys(&mut self, keys: &[K]) {
        let mut guard = self.shared.lock().unwrap();
        for key in keys.iter().cloned() {
            if (self.tester)(&key) {
                guard.insert(key);
            }
        }
    }

    // pub(super) fn ref_count(&self) -> usize {
    //     Rc::strong_count(&self.shared)
    // }

    /// Try unwrapping the inner Rc and Mutex. This succeeds if there
    /// is exactly one strong count to this Interrogate.
    ///
    /// PANICS: If the Mutex is poisened
    pub(super) fn try_unwrap(self) -> Result<IndexSet<K>, Self> {
        Rc::try_unwrap(self.shared)
            .map(|x| Mutex::into_inner(x).unwrap())
            .map_err(|x| Self {
                shared: x,
                tester: self.tester,
            })
    }
}

#[derive(Clone, Debug)]
pub struct Collect<K> {
    pub key: K,
    collection: Rc<Mutex<IndexMap<OperatorId, Vec<u8>>>>,
}
impl<K> Collect<K>
where
    K: Key,
{
    pub(super) fn new(key: K) -> Self {
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

    /// Try unwrapping the inner Rc and Mutex. This succeeds if there
    /// is exactly one strong count to this Collect.
    ///
    /// PANICS: If the Mutex is poisened
    pub(super) fn try_unwrap(self) -> Result<(K, IndexMap<OperatorId, Vec<u8>>), Self> {
        match Rc::try_unwrap(self.collection).map(|mutex| mutex.into_inner().unwrap()) {
            Ok(collection) => Ok((self.key, collection)),
            Err(collection) => Err(Self{key: self.key, collection})
        }
    }
}

impl<K> Debug for Interrogate<K>
where
    K: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Interrogate")
            .field("shared", &self.shared)
            .field("tester", &"Fn(&K) -> bool")
            .finish()
    }
}

#[derive(Debug, Clone, new)]
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

#[derive(Debug, Clone, Serialize, Deserialize, new)]
pub(super) struct NetworkAcquire<K> {
    pub(super) key: K,
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

#[derive(Serialize, Deserialize, new, Clone)]
pub(super) struct DoneMessage {
    pub(super) worker_id: WorkerId,
    pub(super) version: Version,
}
