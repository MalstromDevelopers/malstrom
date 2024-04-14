use crate::keyed::WorkerPartitioner;

use super::{
    collect::CollectDistributor, interrogate::InterrogateDistributor, normal::NormalDistributor,
};

/// The trait each distributor must implement

#[derive(Debug)]
pub(super) enum DistributorKind<K, V, T> {
    Normal(NormalDistributor),
    Interrogate(InterrogateDistributor<K>),
    Collect(CollectDistributor<K, V, T>),
}

// pub(super) trait Distributor<K, V, T> {
//     /// Gets called for every message
//     fn handle_msg(
//         self,
//         msg: IncomingMessage<K, V, T>,
//         partitioner: impl WorkerPartitioner<K>,
//     ) -> (DistributorKind<K, V, T>, OutgoingMessage<K, V, T>);

//     /// Gets called on every schedule
//     fn run(
//         self,
//         partitioner: impl WorkerPartitioner<K>,
//     ) -> (DistributorKind<K, V, T>, OutgoingMessage<K, V, T>);
// }
