//! This module provides a simplified interface for defining stateful
//! paritioned sources that support dynamic rescaling

use std::{cell::RefCell, hash::Hash, marker::PhantomData, rc::Rc};

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::{
    channels::operator_io::{Input, Output},
    keyed::{
        distributed::{Acquire, Collect, DistData, DistKey, DistTimestamp, Interrogate},
        partitioners::rendezvous_select, KeyDistribute,
    },
    operators::{Map, StreamSink},
    runtime::communication::Distributable,
    snapshot::Barrier,
    stream::{BuildContext, JetStreamBuilder, LogicWrapper, OperatorBuilder, OperatorContext},
    types::{
        Data, DataMessage, MaybeKey, MaybeTime, Message, NoData, NoTime, RescaleMessage, SuspendMarker
    },
};

pub trait StatefulSinkImpl<K, V, T>: 'static {
    type Part: Distributable + MaybeKey + Hash + Eq;
    type PartitionState: Distributable;
    type SinkPartition: StatefulSinkPartition<K, V, T, PartitionState = Self::PartitionState>;

    /// Assign a message to a specific sink partition, if the partition does not yet
    /// exist it will be created using the "build_part" function.
    /// **This function MUST BE stable and deterministic**.
    fn assign_part(&self, msg: &DataMessage<K, V, T>) -> Self::Part;

    fn build_part(
        &mut self,
        part: &Self::Part,
        part_state: Option<Self::PartitionState>,
    ) -> Self::SinkPartition;
}
pub struct StatefulSink<K, V, T, S: StatefulSinkImpl<K, V, T>>(S, PhantomData<(K, V, T)>);

impl<K, V, T, S: StatefulSinkImpl<K, V, T>> StatefulSink<K, V, T, S> {
    pub fn new(source: S) -> Self {
        Self(source, PhantomData)
    }
}

pub trait StatefulSinkPartition<K, V, T> {
    type PartitionState;

    /// Poll this partition, possibly returning a record
    fn sink(&mut self, msg: DataMessage<K, V, T>) -> ();

    /// snapshot the current state of this partition
    fn snapshot(&self) -> Self::PartitionState;

    /// collect and shutdown this partition
    /// this gets called when the partition is moved to another worker
    fn collect(self) -> Self::PartitionState;

    /// Gets called when execution gets suspended, possibly resuming later.
    fn suspend(&mut self) -> () {}
}

impl<K, V, T, S> StreamSink<K, V, T> for StatefulSink<K, V, T, S>
where
    S: StatefulSinkImpl<K, V, T>,
    K: DistKey,
    V: DistData,
    T: DistTimestamp,
{
    fn consume_stream(self, name: &str, builder: JetStreamBuilder<K, V, T>) -> () {
        // HACK: Bit ugly, but RefCell works because the scheduler will only schedule
        // one operator at a time.
        let builder_ref = Rc::new(RefCell::new(self.0));
        let assigner = Rc::clone(&builder_ref);
        let part_assigner = OperatorBuilder::direct(
            &format!("{name}-assign-parts"),
            move |input: &mut Input<K, V, T>, output: &mut Output<S::Part, (K, V), T>, _ctx| {
                if let Some(msg) = input.recv() {
                    match msg {
                        Message::Data(d) => {
                            let part = assigner.borrow().assign_part(&d);
                            output.send(Message::Data(DataMessage::new(
                                part,
                                (d.key, d.value),
                                d.timestamp,
                            )))
                        }
                        Message::Epoch(e) => output.send(Message::Epoch(e)),
                        Message::AbsBarrier(barrier) => output.send(Message::AbsBarrier(barrier)),
                        Message::Rescale(rescale_message) => {
                            output.send(Message::Rescale(rescale_message))
                        }
                        Message::SuspendMarker(suspend_marker) => {
                            output.send(Message::SuspendMarker(suspend_marker))
                        }
                        // these don't matter since we have a key_distribute next anyway
                        Message::Interrogate(_) => (),
                        Message::Collect(_) => (),
                        Message::Acquire(_) => (),
                    }
                }
            },
        );

        builder
            .then(part_assigner)
            .key_distribute(
                &format!("{name}-distribute-partitions"),
                |msg| msg.key.clone(),
                rendezvous_select,
            )
            .then(OperatorBuilder::built_by(
                &format!("{name}-partition"),
                |ctx| {
                    let partition_op = StatefulSinkPartitionOp::<K, V, T, S>::new(ctx, builder_ref);
                    partition_op.into_logic()
                },
            ))
            .finish()
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

struct StatefulSinkPartitionOp<K, V, T, Builder: StatefulSinkImpl<K, V, T>> {
    partitions: IndexMap<Builder::Part, Builder::SinkPartition>,
    part_builder: Rc<RefCell<Builder>>,
    _phantom: PhantomData<(Builder::PartitionState, V)>,
}

impl<K, V, T, Builder> StatefulSinkPartitionOp<K, V, T, Builder>
where
    Builder: StatefulSinkImpl<K, V, T>,
    Builder::Part: Hash + Eq,
{
    fn new(_ctx: &mut BuildContext, part_builder: Rc<RefCell<Builder>>) -> Self {
        Self {
            partitions: IndexMap::new(),
            part_builder,
            _phantom: PhantomData,
        }
    }

    fn add_partition(
        &mut self,
        part: Builder::Part,
        part_state: Option<Builder::PartitionState>,
    ) -> () {
        let partition = self.part_builder.borrow_mut().build_part(&part, part_state);
        self.partitions.insert(part, partition);
    }
}

impl<K, V, T, Builder> LogicWrapper<Builder::Part, (K, V), T, NoData, NoTime>
    for StatefulSinkPartitionOp<K, V, T, Builder>
where
    Builder: StatefulSinkImpl<K, V, T>,
    K: MaybeKey,
    V: Data,
    T: MaybeTime,
{
    fn on_schedule(
        &mut self,
        _output: &mut Output<Builder::Part, NoData, NoTime>,
        _ctx: &mut OperatorContext,
    ) -> () {
    }

    fn on_data(
        &mut self,
        data_message: DataMessage<Builder::Part, (K, V), T>,
        _output: &mut Output<Builder::Part, NoData, NoTime>,
        _ctx: &mut OperatorContext,
    ) -> () {
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

    fn on_barrier(
        &mut self,
        barrier: &mut Barrier,
        _output: &mut Output<Builder::Part, NoData, NoTime>,
        ctx: &mut OperatorContext,
    ) -> () {
        let state: Vec<_> = self
            .partitions
            .iter()
            .map(|(k, v)| (k.clone(), v.snapshot()))
            .collect();
        barrier.persist(&state, &ctx.operator_id);
    }

    fn on_rescale(
        &mut self,
        _rescale_message: &mut RescaleMessage,
        _output: &mut Output<Builder::Part, NoData, NoTime>,
        _ctx: &mut OperatorContext,
    ) -> () {
    }

    fn on_suspend(
        &mut self,
        _suspend_marker: &mut SuspendMarker,
        _output: &mut Output<Builder::Part, NoData, NoTime>,
        _ctx: &mut OperatorContext,
    ) -> () {
        for partition in self.partitions.values_mut() {
            partition.suspend();
        }
    }

    fn on_interrogate(
        &mut self,
        interrogate: &mut Interrogate<Builder::Part>,
        _output: &mut Output<Builder::Part, NoData, NoTime>,
        _ctx: &mut OperatorContext,
    ) -> () {
        let keys = self.partitions.keys();
        interrogate.add_keys(keys);
    }

    fn on_collect(
        &mut self,
        collect: &mut Collect<Builder::Part>,
        _output: &mut Output<Builder::Part, NoData, NoTime>,
        ctx: &mut OperatorContext,
    ) -> () {
        let key_state = self.partitions.swap_remove(&collect.key);
        if let Some(partition) = key_state {
            collect.add_state(ctx.operator_id, partition.collect());
        }
    }

    fn on_acquire(
        &mut self,
        acquire: &mut Acquire<Builder::Part>,
        _output: &mut Output<Builder::Part, NoData, NoTime>,
        ctx: &mut OperatorContext,
    ) -> () {
        let partition_state = acquire.take_state(&ctx.operator_id);
        if let Some((part, part_state)) = partition_state {
            self.add_partition(part, Some(part_state));
        }
    }

    fn on_epoch(
        &mut self,
        _epoch: T,
        _output: &mut Output<Builder::Part, NoData, NoTime>,
        ctx: &mut OperatorContext,
    ) -> () {
    }
}
