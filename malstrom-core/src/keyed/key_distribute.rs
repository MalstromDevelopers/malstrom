use std::marker::PhantomData;

use serde::{Serialize, de::DeserializeOwned};

use crate::{
    stream::{BuildContext, LogicBuilder, Malstrom, Operator, StreamBuilder},
    types::{DataMessage, Key, Kvt, MaybeKey},
};

use super::{
    KeyLocal,
    distributed::{
        Distributor,
        types::{DistData, DistKey, DistTimestamp, WorkerPartitioner},
    },
};

/// Key a stream and distribute message to workers according to their key
pub trait KeyDistribute<In: Kvt, Key: DistKey> {
    /// Turn a stream into a keyed stream and distribute
    /// messages across workers via the partitioning function.
    /// The keyed stream returned by this method is capable
    /// of redistributing state on cluster size changes
    /// with no downtime.
    fn key_distribute(
        self,
        name: &str,
        key_func: impl Fn(&DataMessage<In>) -> Key + 'static,
        partitioner: WorkerPartitioner<Key>,
    ) -> StreamBuilder<(Key, In::Value, In::Timestamp)>;
}

impl<In, Key> KeyDistribute<In, Key> for StreamBuilder<In>
where
    In: Kvt,
    In::Value: Serialize + DeserializeOwned,
    In::Timestamp: Serialize + DeserializeOwned,
    Key: DistKey,
{
    fn key_distribute(
        self,
        name: &str,
        key_func: impl Fn(&DataMessage<In>) -> Key + 'static,
        partitioner: WorkerPartitioner<Key>,
    ) -> StreamBuilder<(Key, In::Value, In::Timestamp)> {
        self.key_local(format!("{name}-key"), key_func)
            .distribute(format!("{name}-distribute"), partitioner)
    }
}

pub(crate) trait Distribute<K: Key, M: Kvt> {
    /// Turn a stream into a keyed stream and distribute
    /// messages across workers via the partitioning function.
    /// The keyed stream returned by this method is capable
    /// of redistributing state on cluster size changes
    /// with no downtime.
    fn distribute(
        self,
        name: impl Into<String>,
        partitioner: WorkerPartitioner<K>,
    ) -> StreamBuilder<M>;
}

impl<K, M, X> Distribute<K, M> for X
where
    X: Malstrom<M>,
    K: DistKey,
    M: Kvt<Key = K>,
    M::Value: Serialize + DeserializeOwned,
    M::Timestamp: Serialize + DeserializeOwned,
{
    fn distribute(
        self,
        name: impl Into<String>,
        partitioner: WorkerPartitioner<K>,
    ) -> StreamBuilder<M> {
        self.then(Operator::built_by(
            name.into(),
            DistributorBuilder {
                partitioner,
                _message_type: PhantomData::<M>,
            },
        ))
    }
}

struct DistributorBuilder<K, M> {
    partitioner: WorkerPartitioner<K>,
    _message_type: PhantomData<M>,
}
impl<M, K> LogicBuilder<M, M> for DistributorBuilder<K, M>
where
    M: Kvt<Key = K>,
    M::Value: Serialize + DeserializeOwned,
    M::Timestamp: Serialize + DeserializeOwned,
    K: Key + Serialize + DeserializeOwned,
{
    type Logic = Distributor<M>;

    async fn build(self, ctx: &mut BuildContext<'_>) -> Self::Logic {
        Distributor::<M>::new(self.partitioner, ctx).await
    }
}
