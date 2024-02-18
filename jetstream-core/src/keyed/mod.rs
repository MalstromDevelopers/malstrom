use indexmap::IndexSet;

use crate::frontier::Timestamp;
use crate::snapshot::PersistenceBackend;
use crate::stream::jetstream::JetStreamBuilder;
use crate::stream::operator::StandardOperator;
use crate::{Data, DataMessage, Key, Message, OperatorId, Scale, WorkerId};

pub mod distributed;
mod normal_dist;

/// Marker trait for functions which determine inter-worker routing
trait WorkerPartitioner<K>: for<'a> Fn(&K, &'a IndexSet<WorkerId>) -> &'a WorkerId + 'static {}
impl<K, U: for<'a> Fn(&K, &'a IndexSet<WorkerId>) -> &'a WorkerId + 'static> WorkerPartitioner<K>
    for U
{
}

pub trait KeyDistribute<K: Key, T, P: PersistenceBackend> {
    /// Turn a stream into a keyed stream and distribute
    /// messages across workers via the partitioning function.
    /// The keyed stream returned by this method is capable
    /// of redistributing state on cluster size changes
    /// with no downtime.
    fn key_distribute(
        self,
        key_func: impl Fn(&T) -> K,
        partitioner: impl WorkerPartitioner<K>,
    ) -> JetStreamBuilder<K, T, P>;
}
pub trait KeyLocal<K: Key, T, P> {
    /// Turn a stream into a keyed stream and **do not** distribute
    /// messages across workers.
    /// **NOTE:** The keyed stream created by this function **does not**
    /// redistribute state when the local worker is shut down.
    fn key_local(self, key_func: impl Fn(&T) -> K) -> JetStreamBuilder<K, T, P>;
}

impl<K, T, P> KeyLocal<K, T, P> for JetStreamBuilder<K, T, P>
where
    K: Key,
    T: Data,
    P: PersistenceBackend
{
    fn key_local(self, key_func: impl Fn(&T) -> K) -> JetStreamBuilder<K, T, P> {
        let op = StandardOperator::new(move |input, output, ctx| {
            ctx.frontier.advance_to(Timestamp::MAX);
            match input.recv() {
                Some(Message::Data(DataMessage { time, key, value })) => {
                    let new_key = key_func(&value);
                    let new_msg = DataMessage {
                        time,
                        key: new_key,
                        value,
                    };
                    output.send(Message::Data(new_msg))
                }
                // key messages may not cross key region boundaries
                Some(Message::Interrogate(_)) => (),
                Some(Message::Collect(_)) => (),
                Some(Message::Acquire(_)) => (),
                Some(Message::DropKey(_)) => (),
                Some(x) => {
                    output.send(x);
                }
                None => (),
            }
        });
        self.then(op)
    }
}

impl<K, T, P> KeyDistribute<K, T, P> for JetStreamBuilder<K, T, P>
where
    K: Key,
    P: PersistenceBackend,
{
    fn key_distribute(
        self,
        key_func: impl Fn(&T) -> K,
        partitioner: impl WorkerPartitioner<K>,
    ) -> JetStreamBuilder<K, T, P> {
        let keyed = self.key_local(key_func);
    }
}
