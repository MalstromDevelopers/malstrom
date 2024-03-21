use indexmap::IndexSet;

use crate::channels::selective_broadcast::{Receiver, Sender};
use crate::snapshot::PersistenceBackend;
use crate::stream::jetstream::JetStreamBuilder;
use crate::stream::operator::OperatorBuilder;
use crate::time::Timestamp;
use crate::{Data, DataMessage, Key, MaybeKey, Message, WorkerId};

use self::distributed::{DistData, DistKey, DistTimestamp};

pub mod distributed;
/// Marker trait for functions which determine inter-worker routing
pub trait WorkerPartitioner<K>:
    for<'a> Fn(&K, &'a IndexSet<WorkerId>) -> &'a WorkerId + 'static
{
}
impl<K, U: for<'a> Fn(&K, &'a IndexSet<WorkerId>) -> &'a WorkerId + 'static> WorkerPartitioner<K>
    for U
{
}

pub trait KeyDistribute<X, K: Key, V, T, P: PersistenceBackend> {
    /// Turn a stream into a keyed stream and distribute
    /// messages across workers via the partitioning function.
    /// The keyed stream returned by this method is capable
    /// of redistributing state on cluster size changes
    /// with no downtime.
    fn key_distribute(
        self,
        key_func: impl Fn(&DataMessage<X, V, T>) -> K + 'static,
        partitioner: impl WorkerPartitioner<K>,
    ) -> JetStreamBuilder<K, V, T, P>;
}
pub trait KeyLocal<X, K: Key, V, T, P> {
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
    ) -> JetStreamBuilder<K, V, T, P>;
}

impl<X, K, V, T, P> KeyLocal<X, K, V, T, P> for JetStreamBuilder<X, V, T, P>
where
    X: MaybeKey,
    K: Key,
    V: Data,
    T: Timestamp,
    P: PersistenceBackend,
{
    fn key_local(
        self,
        key_func: impl Fn(&DataMessage<X, V, T>) -> K + 'static,
    ) -> JetStreamBuilder<K, V, T, P> {
        let op = OperatorBuilder::direct(
            move |input: &mut Receiver<X, V, T, P>, output: &mut Sender<K, V, T, P>, _ctx| {
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
                    Some(Message::ScaleAddWorker(x)) => output.send(Message::ScaleAddWorker(x)),
                    Some(Message::ScaleRemoveWorker(x)) => {
                        output.send(Message::ScaleRemoveWorker(x))
                    }
                    Some(Message::ShutdownMarker(x)) => output.send(Message::ShutdownMarker(x)),
                    Some(Message::Epoch(x)) => output.send(Message::Epoch(x)),
                    None => (),
                }
            },
        );
        self.then(op)
    }
}

impl<X, K, V, T, P> KeyDistribute<X, K, V, T, P> for JetStreamBuilder<X, V, T, P>
where
    X: MaybeKey,
    K: DistKey,
    V: DistData,
    T: DistTimestamp,
    P: PersistenceBackend,
{
    fn key_distribute(
        self,
        key_func: impl Fn(&DataMessage<X, V, T>) -> K + 'static,
        partitioner: impl WorkerPartitioner<K>,
    ) -> JetStreamBuilder<K, V, T, P> {
        // let mut distributor = Distributor::new(partitioner);
        let keyed = self.key_local(key_func);
        keyed.then(OperatorBuilder::direct(move |input, output, ctx| {
            todo!()
            // distributor.run(input, output, ctx)
        }))
    }
}
