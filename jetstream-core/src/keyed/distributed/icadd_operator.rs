use std::rc::Rc;

use derive_new::new;
use indexmap::IndexSet;
use serde::{Deserialize, Serialize};

use crate::{keyed::types::{DistKey, DistTimestamp, WorkerPartitioner}, stream::{BuildContext, Logic}, types::{MaybeData, WorkerId}};

use super::{
    collect_dist::CollectDistributor, interrogate_dist::InterrogateDistributor,
    normal_dist::NormalDistributor, versioner::VersionedMessage,
    NetworkAcquire,
};
use crate::stream::OperatorContext;

/// Control messages which the ICADD controllers exchange
/// directly between each other
#[derive(Serialize, Deserialize, Clone)]
pub(super) enum DirectlyExchangedMessage<K> {
    Done,
    Acquire(NetworkAcquire<K>),
}

#[derive(Clone, Debug, new)]
pub(crate) struct TargetedMessage<V> {
    pub(super) inner: VersionedMessage<V>,
    pub(super) target: Option<WorkerId>,
}

pub enum DistributorKind<K, V, T> {
    Normal(NormalDistributor<K, V, T>),
    Interrogate(InterrogateDistributor<K, V, T>),
    Collect(CollectDistributor<K, V, T>),
}

pub(crate) fn icadd<K: DistKey, V: MaybeData, T: DistTimestamp>(
    partitioner: Rc<dyn WorkerPartitioner<K>>,
    ctx: &BuildContext,
) -> impl Logic<K, VersionedMessage<V>, T, K, TargetedMessage<V>, T> {
    let mut worker_set: IndexSet<WorkerId> = ctx.get_worker_ids().collect();
    worker_set.insert(ctx.worker_id);
    let normal_dist: NormalDistributor<K, V, T> = ctx
        .load_state()
        .unwrap_or_else(|| NormalDistributor::new(ctx.worker_id, worker_set));
    let mut dist = Some(DistributorKind::Normal(normal_dist));

    move |input, output, op_ctx| {
        let d = dist.take().unwrap();
        let new = match d {
            DistributorKind::Normal(x) => x.run(input, output, op_ctx, partitioner.clone()),
            DistributorKind::Interrogate(x) => x.run(input, output, op_ctx, partitioner.clone()),
            DistributorKind::Collect(x) => x.run(input, output, op_ctx, partitioner.clone()),
        };
        dist.replace(new);
    }
}
