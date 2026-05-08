use std::{hash::Hash, marker::PhantomData};

use serde::{Serialize, de::DeserializeOwned};

use crate::{
    keyed::{WorkerPartitioner, distributed::distributor::DistributorBuilder},
    stream::{BuildContext, Logic, LogicBuilder, Malstrom, Operator, StreamBuilder},
    types::{DataMessage, Key, Kvt, MaybeKey, distributable::Distributable},
};

use super::KeyLocal;

/// Key a stream and distribute message to workers according to their key
pub trait KeyDistribute<In: Kvt, K: Key> {
    /// Turn a stream into a keyed stream and distribute
    /// messages across workers via the partitioning function.
    /// The keyed stream returned by this method is capable
    /// of redistributing state on cluster size changes
    /// with no downtime.
    fn key_distribute(
        self,
        name: &str,
        key_func: impl Fn(&DataMessage<In>) -> K + 'static,
        partitioner: WorkerPartitioner<K>,
    ) -> StreamBuilder<(K, In::Value, In::Timestamp)>;
}

impl<In, K> KeyDistribute<In, K> for StreamBuilder<In>
where
    In: Kvt,
    In::Value: Serialize + DeserializeOwned,
    In::Timestamp: Serialize + DeserializeOwned,
    K: Key + Distributable,
{
    fn key_distribute(
        self,
        name: &str,
        key_func: impl Fn(&DataMessage<In>) -> K + 'static,
        partitioner: WorkerPartitioner<K>,
    ) -> StreamBuilder<(K, In::Value, In::Timestamp)> {
        self.key_local(format!("{name}-key"), key_func)
            .distribute(format!("{name}-distribute"), partitioner)
    }
}

pub(crate) trait Distribute<M: Kvt> {
    fn distribute(self, name: String, partitioner: WorkerPartitioner<M::Key>) -> StreamBuilder<M>;
}

impl<M> Distribute<M> for StreamBuilder<M>
where
    M: Kvt + Distributable,
    M::Key: Key + Distributable,
    M::Value: Distributable,
    M::Timestamp: Distributable,
{
    fn distribute(self, name: String, partitioner: WorkerPartitioner<M::Key>) -> StreamBuilder<M> {
        let op = Operator::built_by(name, DistributorBuilder::new(partitioner));
        self.then(op)
    }
}
