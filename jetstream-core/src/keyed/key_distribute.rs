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
        key_func: impl Fn(&DataMessage<X, V, T>) -> K + 'static,
        partitioner: WorkerPartitioner<K>,
    ) -> JetStreamBuilder<K, V, T> {
        // TODO: The communication between upstream and downstream exchanger effectively
        // creates a loop in th dataflow graph. This is not good, because potentially the
        // cluster could deadlock in (most likely rare) edge cases
        let keyed = self.key_local(key_func);
        keyed.then(OperatorBuilder::built_by(move |ctx| {
            let mut dist = Distributor::new(partitioner, ctx);
            move |input, output, op_ctx| dist.run(input, output, op_ctx)
        }))
    }
}
