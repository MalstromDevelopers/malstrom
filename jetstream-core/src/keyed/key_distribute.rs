use std::rc::Rc;

use crate::{stream::{JetStreamBuilder, OperatorBuilder}, types::{DataMessage, Key, MaybeKey}};

use super::{distributed::{downstream_exchanger, epoch_aligner, icadd, upstream_exchanger, versioner}, types::{DistData, DistKey, DistTimestamp, WorkerPartitioner}, KeyLocal};


pub trait KeyDistribute<X, K: Key, V, T> {
    /// Turn a stream into a keyed stream and distribute
    /// messages across workers via the partitioning function.
    /// The keyed stream returned by this method is capable
    /// of redistributing state on cluster size changes
    /// with no downtime.
    fn key_distribute(
        self,
        key_func: impl Fn(&DataMessage<X, V, T>) -> K + 'static,
        partitioner: impl WorkerPartitioner<K>,
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
        partitioner: impl WorkerPartitioner<K>,
    ) -> JetStreamBuilder<K, V, T> {
        // let mut distributor = Distributor::new(partitioner);
        let keyed = self
            .key_local(key_func)
            .label("jetstream::key_distribute::key_local");
        keyed
            .then(OperatorBuilder::built_by(versioner))
            .label("jetstream::key_distribute::versioner")
            .then(OperatorBuilder::built_by(epoch_aligner))
            .label("jetstream::key_distribute::epoch_aligner")
            .then(OperatorBuilder::built_by(|ctx| upstream_exchanger(2, ctx)))
            .label("jetstream::key_distribute::upstream_exchanger")
            .then(OperatorBuilder::built_by(move |ctx| {
                icadd(Rc::new(partitioner), ctx)
            }))
            .label("jetstream::key_distribute::icadd")
            .then(OperatorBuilder::built_by(|ctx| {
                downstream_exchanger(2, ctx)
            }))
            .label("jetstream::key_distribute::downstream_exchanger")
    }
}
