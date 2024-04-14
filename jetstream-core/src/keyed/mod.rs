use std::rc::Rc;

use indexmap::IndexSet;

use crate::channels::selective_broadcast::{Receiver, Sender};
use crate::snapshot::PersistenceBackend;
use crate::stream::jetstream::JetStreamBuilder;
use crate::stream::operator::OperatorBuilder;
use crate::time::{MaybeTime, Timestamp};
use crate::{Data, DataMessage, Key, MaybeKey, Message, WorkerId};

use self::distributed::{downstream_exchanger, epoch_aligner, upstream_exchanger, versioner};
use self::distributed::{icadd, DistData, DistKey, DistTimestamp};

pub mod distributed;
// mod exchange;

/// Marker trait for functions which determine inter-worker routing
/// TODO: Maybe this should just be a function pointer
pub trait WorkerPartitioner<K>:
    for<'a> Fn(&K, &'a IndexSet<WorkerId>) -> &'a WorkerId + 'static
{
}
impl<K, U: for<'a> Fn(&K, &'a IndexSet<WorkerId>) -> &'a WorkerId + 'static> WorkerPartitioner<K>
    for U
{
}

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
pub trait KeyLocal<X, K: Key, V, T> {
    /// Turn a stream into a keyed stream and **do not** distribute
    /// messages across workers.
    /// # ⚠️ Warning:
    /// The keyed stream created by this function **does not**
    /// redistribute state when the local worker is shut down.
    /// If the worker gets de-scheduled all state is potentially lost.
    /// To have the state moved to a different worker in this case, use
    /// `key_distribute`.
    fn key_local(
        self,
        key_func: impl Fn(&DataMessage<X, V, T>) -> K + 'static,
    ) -> JetStreamBuilder<K, V, T>;
}

impl<X, K, V, T> KeyLocal<X, K, V, T> for JetStreamBuilder<X, V, T>
where
    X: MaybeKey,
    K: Key,
    V: Data,
    T: MaybeTime,
{
    fn key_local(
        self,
        key_func: impl Fn(&DataMessage<X, V, T>) -> K + 'static,
    ) -> JetStreamBuilder<K, V, T> {
        let op = OperatorBuilder::direct(
            move |input: &mut Receiver<X, V, T>, output: &mut Sender<K, V, T>, _ctx| {
                match input.recv() {
                    Some(Message::Data(d)) => {
                        let new_key = key_func(&d);
                        let new_msg = DataMessage {
                            timestamp: d.timestamp,
                            key: new_key,
                            value: d.value,
                        };
                        output.send(Message::Data(new_msg))
                    }
                    // key messages may not cross key region boundaries
                    Some(Message::Interrogate(_)) => (),
                    Some(Message::Collect(_)) => (),
                    Some(Message::Acquire(_)) => (),
                    Some(Message::DropKey(_)) => (),
                    // necessary to convince Rust it is a different generic type now
                    Some(Message::AbsBarrier(b)) => output.send(Message::AbsBarrier(b)),
                    // Some(Message::Load(l)) => output.send(Message::Load(l)),
                    Some(Message::Rescale(x)) => output.send(Message::Rescale(x)),
                    Some(Message::ShutdownMarker(x)) => output.send(Message::ShutdownMarker(x)),
                    Some(Message::Epoch(x)) => output.send(Message::Epoch(x)),
                    None => (),
                }
            },
        );
        self.then(op)
    }
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
        let keyed = self.key_local(key_func);
        keyed
            .then(OperatorBuilder::built_by(versioner))
            .then(OperatorBuilder::built_by(epoch_aligner))
            .then(OperatorBuilder::built_by(upstream_exchanger))
            .then(OperatorBuilder::built_by(move |ctx| {
                icadd(Rc::new(partitioner), ctx)
            }))
            .then(OperatorBuilder::built_by(|ctx| downstream_exchanger(2, ctx)))

    }
}
