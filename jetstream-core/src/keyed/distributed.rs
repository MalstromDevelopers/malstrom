use std::{fmt::Debug, rc::Rc, sync::Mutex};

use derive_new::new;
use indexmap::{IndexMap, IndexSet};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{time::MaybeTime, Key, MaybeData, OperatorId, WorkerId};

mod collect_dist;
mod icadd_operator;
mod interrogate_dist;
mod normal_dist;
mod data_exchange;
mod versioner;
mod epoch_align;

pub(super) use epoch_align::epoch_aligner;
pub(super) use versioner::versioner;
pub(super) use icadd_operator::icadd;
pub(super) use data_exchange::{upstream_exchanger, downstream_exchanger};

/// Marker trait for distributable key
pub trait DistKey: Key + Serialize + DeserializeOwned + 'static {}
impl<T: Key + Serialize + DeserializeOwned + 'static> DistKey for T {}
/// Marker trait for distributable value
pub trait DistData: MaybeData + Serialize + DeserializeOwned {}
impl<T: MaybeData + Serialize + DeserializeOwned> DistData for T {}

pub trait DistTimestamp: MaybeTime + Serialize + DeserializeOwned {}
impl<T: MaybeTime + Serialize + DeserializeOwned> DistTimestamp for T {}

type Version = u64;

#[derive(Clone)]
pub struct Interrogate<K> {
    shared: Rc<Mutex<IndexSet<K>>>,
    tester: Rc<dyn Fn(&K) -> bool>,
}
impl<K> Interrogate<K>
where
    K: Key,
{
    pub(crate) fn new(keys: IndexSet<K>, tester: Rc<dyn Fn(&K) -> bool>) -> Self {
        let shared = Rc::new(Mutex::new(keys));
        Self { shared, tester }
    }

    pub fn add_keys(&mut self, keys: &[K]) {
        let mut guard = self.shared.lock().unwrap();
        for key in keys.iter().cloned() {
            if (self.tester)(&key) {
                guard.insert(key);
            }
        }
    }

    pub(crate) fn print_ref_count_and_die(&self) {
        let rc = Rc::strong_count(&self.shared);
        println!("Interrogate refcount is {rc}");
        panic!("OOF")
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
    pub(crate) fn new(key: K) -> Self {
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
    /// PANICS: If the Mutex is poisoned
    pub(super) fn try_unwrap(self) -> Result<(K, IndexMap<OperatorId, Vec<u8>>), Self> {
        match Rc::try_unwrap(self.collection).map(|mutex| mutex.into_inner().unwrap()) {
            Ok(collection) => Ok((self.key, collection)),
            Err(collection) => Err(Self {
                key: self.key,
                collection,
            }),
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
