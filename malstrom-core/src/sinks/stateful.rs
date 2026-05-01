//! This module provides a simplified interface for defining stateful
//! paritioned sources that support dynamic rescaling

use std::{cell::RefCell, hash::Hash, marker::PhantomData, rc::Rc};

use indexmap::IndexMap;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
    channels::operator_io::{Input, Output},
    keyed::{
        KeyDistribute,
        distributed::{Acquire, Collect, Interrogate},
        rendezvous_select,
    },
    operators::StreamSink,
    snapshot::SnapshotBarrier,
    stream::{
        BuildContext, Logic, Malstrom, Operator, OperatorContext, SafeLogic, SafeLogicWrapper,
        StreamBuilder,
    },
    types::{
        Data, DataMessage, Key, Kvt, MaybeKey, MaybeTime, Message, NoData, NoKey, NoTime,
        RescaleMessage, SuspendMarker, distributable::Distributable,
    },
};
/// Implementation of a stateful sink
pub trait StatefulSinkImpl<M: Kvt>: 'static {
    /// A `Part` of a partition is a key by which any partition of the source is
    /// uniquely identified. It is perfectly valid for a source to only have a single part and in
    /// turn only a single partition, though this may not be very useful.
    type Part: Key + Distributable;
    /// State for a partition of this sink. The state is persisted across job restarts
    /// and moved with the partition to a different worker when the jobs worker set changes.
    type PartitionState: Distributable;
    /// A partition of this sink.
    /// Partitions may be moved to different workers, when the jobs worker set changes.
    type SinkPartition: StatefulSinkPartition<M, PartitionState = Self::PartitionState>;

    /// Assign a message to a specific sink partition, if the partition does not yet
    /// exist it will be created using the "build_part" function.
    /// **This function MUST BE stable and deterministic**.
    fn assign_part(&self, msg: &DataMessage<M>) -> Self::Part;

    /// Build the partition for the given part
    fn build_part(
        &mut self,
        part: &Self::Part,
        part_state: Option<Self::PartitionState>,
    ) -> Self::SinkPartition;
}

/// A sink which emits records and holds some persistent state.
pub struct StatefulSink<M: Kvt, S: StatefulSinkImpl<M>>(S, PhantomData<M>);

impl<M: Kvt, S: StatefulSinkImpl<M>> StatefulSink<M, S> {
    /// Create a new stateful sink by wrapping an implementation
    pub fn new(source: S) -> Self {
        Self(source, PhantomData)
    }
}

/// A source which emits records and holds some persistent state.
pub trait StatefulSinkPartition<M: Kvt> {
    /// State for a partition of this sink. The state is persisted across job restarts
    /// and moved with the partition to a different worker when the jobs worker set changes.
    type PartitionState;

    /// Poll this partition, possibly returning a record
    fn sink(&mut self, msg: DataMessage<M>);

    /// snapshot the current state of this partition
    fn snapshot(&self) -> Self::PartitionState;

    /// collect and shutdown this partition
    /// this gets called when the partition is moved to another worker
    fn collect(self) -> Self::PartitionState;
}

impl<M, S> StreamSink<M> for StatefulSink<M, S>
where
    M: Kvt,
    M::Key: Serialize + DeserializeOwned,
    M::Value: Serialize + DeserializeOwned,
    M::Timestamp: Serialize + DeserializeOwned,
    S: StatefulSinkImpl<M>,
{
    fn consume_stream(self, name: &str, builder: StreamBuilder<M>) {
        // HACK: Bit ugly, but RefCell works because the scheduler will only schedule
        // one operator at a time.
        let builder_ref = Rc::new(RefCell::new(self.0));
        let assigner = Rc::clone(&builder_ref);
        let part_assigner: Operator<_, _, (S::Part, (M::Key, M::Value), M::Timestamp)> =
            Operator::direct(format!("{name}-assign-parts"), PartAssigner { assigner });

        let stream = builder.then(part_assigner);
        let stream = stream.key_distribute(
            &format!("{name}-distribute-partitions"),
            |msg| msg.key.clone(),
            rendezvous_select,
        );
        stream.then(Operator::direct(
            format!("{name}-partition"),
            StatefulSinkPartitionOp::<M, S>::new(builder_ref).into_logic(),
        ));
    }
}

struct PartAssigner<S> {
    assigner: Rc<RefCell<S>>,
}

impl<M, S> Logic<M, (S::Part, (M::Key, M::Value), M::Timestamp)> for PartAssigner<S>
where
    M: Kvt,
    S: StatefulSinkImpl<M>,
{
    async fn apply(
        &mut self,
        input: &mut Input<M>,
        output: &mut Output<(S::Part, (M::Key, M::Value), M::Timestamp)>,
        _ctx: &mut OperatorContext,
    ) {
        match input.recv().await {
            Message::Data(d) => {
                let part = self.assigner.borrow().assign_part(&d);
                output
                    .send(Message::Data(DataMessage::new(
                        part,
                        (d.key, d.value),
                        d.timestamp,
                    )))
                    .await
            }
            Message::Epoch(e) => output.send(Message::Epoch(e)).await,
            Message::AbsBarrier(barrier) => output.send(Message::AbsBarrier(barrier)).await,
            Message::Rescale(rescale_message) => {
                output.send(Message::Rescale(rescale_message)).await
            }
            Message::SuspendMarker(suspend_marker) => {
                output.send(Message::SuspendMarker(suspend_marker)).await
            }
            // these don't matter since we have a key_distribute next anyway
            Message::Interrogate(_) => (),
            Message::Collect(_) => (),
            Message::Acquire(_) => (),
        }
    }
}

/// Marker we send to broadcast, that a partition has finished.
/// We need this to avoid an edge case where all local partitions finish and we send the MAX time,
/// but then get assigned a new unfinished partition due to a rescale.
/// So we broadcast partition info to only emit MAX time when all partitions globally are finished
#[derive(Serialize, Deserialize, Hash, PartialEq, Eq, Clone)]
enum PartOrData<V> {
    Part,
    Data(V),
}

struct StatefulSinkPartitionOp<M: Kvt, Builder: StatefulSinkImpl<M>> {
    partitions: IndexMap<Builder::Part, Builder::SinkPartition>,
    part_builder: Rc<RefCell<Builder>>,
}

impl<M, Builder> StatefulSinkPartitionOp<M, Builder>
where
    M: Kvt,
    Builder: StatefulSinkImpl<M>,
    Builder::Part: Hash + Eq,
{
    fn new(part_builder: Rc<RefCell<Builder>>) -> Self {
        Self {
            partitions: IndexMap::new(),
            part_builder,
        }
    }

    fn add_partition(&mut self, part: Builder::Part, part_state: Option<Builder::PartitionState>) {
        let partition = self.part_builder.borrow_mut().build_part(&part, part_state);
        self.partitions.insert(part, partition);
    }
}

impl<M, Builder>
    SafeLogic<(Builder::Part, (M::Key, M::Value), M::Timestamp), (Builder::Part, (), M::Timestamp)>
    for StatefulSinkPartitionOp<M, Builder>
where
    M: Kvt,
    Builder: StatefulSinkImpl<M>,
{
    async fn on_data(
        &mut self,
        data_message: DataMessage<(Builder::Part, (M::Key, M::Value), M::Timestamp)>,
        _output: &mut Output<(Builder::Part, (), M::Timestamp)>,
        _ctx: &mut OperatorContext,
    ) {
        let partition = self
            .partitions
            .entry(data_message.key)
            .or_insert_with_key(|k| self.part_builder.borrow_mut().build_part(k, None));
        let msg = DataMessage::new(
            data_message.value.0,
            data_message.value.1,
            data_message.timestamp,
        );
        partition.sink(msg);
    }

    async fn on_barrier(
        &mut self,
        barrier: &mut SnapshotBarrier,
        _output: &mut Output<(Builder::Part, (), M::Timestamp)>,
        ctx: &mut OperatorContext,
    ) {
        let state: Vec<_> = self
            .partitions
            .iter()
            .map(|(k, v)| (k.clone(), v.snapshot()))
            .collect();
        barrier.persist(&state, &ctx.operator_id);
    }

    async fn on_interrogate(
        &mut self,
        interrogate: &mut Interrogate<Builder::Part>,
        _output: &mut Output<(Builder::Part, (), M::Timestamp)>,
        _ctx: &mut OperatorContext,
    ) {
        let keys = self.partitions.keys();
        interrogate.add_keys(keys);
    }

    async fn on_collect(
        &mut self,
        collect: &mut Collect<Builder::Part>,
        _output: &mut Output<(Builder::Part, (), M::Timestamp)>,
        ctx: &mut OperatorContext,
    ) {
        let key_state = self.partitions.swap_remove(collect.get_key());
        if let Some(partition) = key_state {
            collect.add_state(ctx.operator_id, partition.collect());
        }
    }

    async fn on_acquire(
        &mut self,
        acquire: &mut Acquire<Builder::Part>,
        _output: &mut Output<(Builder::Part, (), M::Timestamp)>,
        ctx: &mut OperatorContext,
    ) {
        let partition_state = acquire.take_state(&ctx.operator_id);
        if let Some((part, part_state)) = partition_state {
            self.add_partition(part, Some(part_state));
        }
    }
}
