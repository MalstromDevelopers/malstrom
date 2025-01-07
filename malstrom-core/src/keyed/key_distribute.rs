use crate::{
    stream::{JetStreamBuilder, OperatorBuilder},
    types::{DataMessage, Key, MaybeKey},
};

use super::{
    distributed::{
        types::{DistData, DistKey, DistTimestamp, WorkerPartitioner},
        Distributor,
    },
    KeyLocal,
};

pub trait KeyDistribute<X, K: Key, V, T> {
    /// Turn a stream into a keyed stream and distribute
    /// messages across workers via the partitioning function.
    /// The keyed stream returned by this method is capable
    /// of redistributing state on cluster size changes
    /// with no downtime.
    fn key_distribute(
        self,
        name: &str,
        key_func: impl Fn(&DataMessage<X, V, T>) -> K + 'static,
        partitioner: WorkerPartitioner<K>,
    ) -> JetStreamBuilder<K, V, T>;
}

impl<X, K, V, T> KeyDistribute<X, K, V, T> for JetStreamBuilder<X, V, T>
where
    X: MaybeKey,
    K: DistKey,
    V: DistData,
    T: DistTimestamp,
{
    fn key_distribute(
        self,
        name: &str,
        key_func: impl Fn(&DataMessage<X, V, T>) -> K + 'static,
        partitioner: WorkerPartitioner<K>,
    ) -> JetStreamBuilder<K, V, T> {
        self.key_local(&format!("{name}-key"), key_func)
            .distribute(&format!("{name}-distribute"), partitioner)
    }
}

pub(crate) trait Distribute<K: Key, V, T> {
    /// Turn a stream into a keyed stream and distribute
    /// messages across workers via the partitioning function.
    /// The keyed stream returned by this method is capable
    /// of redistributing state on cluster size changes
    /// with no downtime.
    fn distribute(self, name: &str, partitioner: WorkerPartitioner<K>)
        -> JetStreamBuilder<K, V, T>;
}

impl<K, V, T> Distribute<K, V, T> for JetStreamBuilder<K, V, T>
where
    K: DistKey,
    V: DistData,
    T: DistTimestamp,
{
    fn distribute(
        self,
        name: &str,
        partitioner: WorkerPartitioner<K>,
    ) -> JetStreamBuilder<K, V, T> {
        self.then(OperatorBuilder::built_by(name, move |ctx| {
            let mut dist = Distributor::new(partitioner, ctx);
            move |input, output, op_ctx| dist.run(input, output, op_ctx)
        }))
    }
}
