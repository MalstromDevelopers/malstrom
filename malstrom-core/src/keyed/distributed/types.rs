//! Types and type aliases commonly used throughout Malstrom
use std::fmt::Debug;
use std::{
    ops::{Deref, DerefMut},
    rc::Rc,
    sync::Mutex,
};

use indexmap::{IndexMap, IndexSet};
use serde::{Deserialize, Serialize};

use crate::runtime::BiCommunicationClient;
use crate::{runtime::communication::Distributable, types::*};
// Marker trait for distributable keys, values or time
// pub trait Distributable: Serialize + DeserializeOwned + 'static{}
// impl<T: Serialize + DeserializeOwned + 'static> Distributable for T {}

/// Marker trait for distributable key
pub trait DistKey: Key + Distributable {}
impl<T: Key + Distributable> DistKey for T {}
/// Marker trait for distributable value
pub trait DistData: MaybeData + Distributable {}
impl<T: MaybeData + Distributable> DistData for T {}
/// A timestamp which can be sent to other workers
pub trait DistTimestamp: MaybeTime + Distributable {}
impl<T: MaybeTime + Distributable> DistTimestamp for T {}

pub(super) type Version = u64;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(super) enum NetworkMessage<K, V, T> {
    Data(NetworkDataMessage<K, V, T>),
    Epoch(T),
    BarrierMarker,
    SuspendMarker,
    Acquire(NetworkAcquire<K>),
    Upgrade(Version),
    AckUpgrade(Version),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(super) struct NetworkDataMessage<K, V, T> {
    pub content: DataMessage<K, V, T>,
    pub version: Version,
}

impl<K, V, T> NetworkDataMessage<K, V, T> {
    pub(super) fn new(content: DataMessage<K, V, T>, version: Version) -> Self {
        Self { content, version }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct NetworkAcquire<K> {
    pub(super) key: K,
    collection: IndexMap<OperatorId, Vec<u8>>,
}

impl<K> NetworkAcquire<K> {
    pub(super) fn new(key: K, collection: IndexMap<OperatorId, Vec<u8>>) -> Self {
        Self { key, collection }
    }
}

impl<K> From<NetworkAcquire<K>> for Acquire<K> {
    fn from(value: NetworkAcquire<K>) -> Self {
        Self {
            key: value.key,
            collection: Rc::new(Mutex::new(value.collection)),
        }
    }
}

impl<K> TryFrom<Collect<K>> for NetworkAcquire<K> {
    type Error = Collect<K>;

    fn try_from(value: Collect<K>) -> Result<Self, Self::Error> {
        value
            .try_unwrap()
            .map(|(key, collection)| Self::new(key, collection))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct RemoteState<T> {
    pub is_barred: bool,
    pub frontier: Option<T>,
    /// Last config version of this remote
    pub last_version: Option<Version>,
    /// Last config this remote acknowledged to use, essentially
    /// what the remote thinks what version we are at
    pub last_ack_version: Option<Version>,

    /// This is not persisted, as a worker which was shut down will by
    /// definition be back up and running if we restart the cluster
    /// and reload from state
    #[serde(skip)]
    pub sent_suspend: bool,
}
impl<T> Default for RemoteState<T> {
    fn default() -> Self {
        Self {
            is_barred: Default::default(),
            frontier: Default::default(),
            last_ack_version: Default::default(),
            last_version: None,
            sent_suspend: false,
        }
    }
}

/// Wraps a value for interior mutability.
/// In Rust it is not possible to replace the value in a struct field
/// without having full ownership of the struct.
/// The usual solution is to wrap the field type in `Option`, but this does
/// not quite capture the semantics.
/// Instead you can wrap it in `Container` and use [Container::apply] to
/// replace the value in-place.
///
/// For convenience, `Container<T>` derefs to `T`.
pub(super) struct Container<T> {
    inner: Option<T>,
}
impl<T> Container<T> {
    pub(super) fn new(value: T) -> Self {
        Self { inner: Some(value) }
    }

    pub(super) fn apply(&mut self, func: impl FnOnce(T) -> T) {
        let new_value = func(self.inner.take().expect("Always Some"));
        self.inner = Some(new_value);
    }
}
impl<T> Deref for Container<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        self.inner.as_ref().expect("Always Some")
    }
}
impl<T> DerefMut for Container<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.as_mut().expect("Always Some")
    }
}

/// A pratitioning function for selecting which worker a keyed message will go to
pub type WorkerPartitioner<K> = fn(&K, &IndexSet<WorkerId>) -> WorkerId;

use crate::types::OperatorId;

/// An interrogation message for inquiring which keys are associated with state on a worker
#[derive(Clone)]
pub struct Interrogate<K> {
    shared: Rc<Mutex<IndexSet<K>>>,
    tester: Rc<dyn Fn(&K) -> bool>,
}
impl<K> Interrogate<K>
where
    K: Key,
{
    /// Creates a new interrogator.
    /// Tester is a function which checks if the supplied keys should be added to
    /// the interrogation set, i.e. if these are keys, that need to be relocated
    pub(crate) fn new(tester: Rc<dyn Fn(&K) -> bool>) -> Self {
        let shared = Rc::new(Mutex::new(IndexSet::new()));
        Self { shared, tester }
    }

    /// Add all keys for which the caller holds state to this interrogator
    pub fn add_keys<'a>(&mut self, keys: impl IntoIterator<Item = &'a K>) {
        #[allow(clippy::unwrap_used)]
        let mut guard = self.shared.lock().unwrap();
        for key in keys {
            if (self.tester)(key) {
                guard.insert(key.clone());
            }
        }
    }

    /// Try unwrapping the inner Rc and Mutex. This succeeds if there
    /// is exactly one strong count to this Interrogate.
    ///
    /// PANICS: If the Mutex is poisened
    pub(crate) fn try_unwrap(self) -> Result<IndexSet<K>, Self> {
        #[allow(clippy::unwrap_used)]
        Rc::try_unwrap(self.shared)
            .map(|x| Mutex::into_inner(x).unwrap())
            .map_err(|x| Self {
                shared: x,
                tester: self.tester,
            })
    }
}

/// A collect message collecting key-state for redistribution
#[derive(Clone, Debug)]
pub struct Collect<K> {
    /// The key for which state is being collected
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

    /// Add the given state to the collection, allowing it to be transferred to another worker
    pub fn add_state<S: Distributable>(&mut self, operator_id: OperatorId, state: S) {
        #[allow(clippy::unwrap_used)]
        self.collection
            .lock()
            .unwrap()
            .insert(operator_id, BiCommunicationClient::encode(state));
    }
}
impl<K> Collect<K> {
    /// Try unwrapping the inner Rc and Mutex. This succeeds if there
    /// is exactly one strong count to this Collect.
    ///
    /// PANICS: If the Mutex is poisoned
    pub(crate) fn try_unwrap(self) -> Result<(K, IndexMap<OperatorId, Vec<u8>>), Self> {
        #[allow(clippy::unwrap_used)]
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

/// Acquire message of the ICA rescale process which is sent to operators for them to take the
/// state they will manage under the new configuration
#[derive(Debug, Clone)]
pub struct Acquire<K> {
    key: K,
    collection: Rc<Mutex<IndexMap<OperatorId, Vec<u8>>>>,
}
impl<K> Acquire<K> {
    #[cfg(test)]
    pub(crate) fn new(key: K, collection: IndexMap<OperatorId, Vec<u8>>) -> Self {
        Self {
            key,
            collection: Rc::new(Mutex::new(collection)),
        }
    }
}

impl<K> Acquire<K>
where
    K: Key,
{
    /// Try taking state for this operator from this Acquire message, returning the state and
    /// its key, if the message holds any for the given operator
    pub fn take_state<S: Distributable>(&self, operator_id: &OperatorId) -> Option<(K, S)> {
        #[allow(clippy::unwrap_used)]
        self.collection
            .lock()
            .unwrap()
            .swap_remove(operator_id)
            .map(|x| BiCommunicationClient::decode(&x))
            .map(|s| (self.key.clone(), s))
    }
}
