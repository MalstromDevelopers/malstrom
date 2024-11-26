//! This module provides a simplified interface for defining stateful
//! paritioned sources that support dynamic rescaling

use std::{hash::Hash, marker::PhantomData};

use indexmap::IndexMap;

use crate::{
    channels::selective_broadcast::Sender,
    keyed::{
        distributed::{Acquire, Collect, DistKey, Interrogate},
        partitioners::rendezvous_select,
        KeyDistribute,
    },
    operators::{Map, Source, StreamSource},
    runtime::communication::Distributable,
    snapshot::Barrier,
    stream::{JetStreamBuilder, LogicWrapper, OperatorBuilder, OperatorContext},
    types::{
        Data, DataMessage, NoData, NoKey, NoTime,
        RescaleMessage, SuspendMarker, Timestamp,
    },
};

use super::SingleIteratorSource;

pub trait StatefulSourceImpl<V, T>: 'static {
    type Part: DistKey;
    type PartitionState: Distributable;
    type SourcePartition: StatefulSourcePartition<V, T, PartitionState = Self::PartitionState>;

    /// List all initial partitions for this source
    fn list_parts(&self) -> impl IntoIterator<Item = Self::Part> + 'static;

    fn build_part(
        &self,
        part: &Self::Part,
        part_state: Option<Self::PartitionState>,
    ) -> Self::SourcePartition;
}
pub struct StatefulSource<V, T, S: StatefulSourceImpl<V, T>>(S, PhantomData<(V, T)>);
impl<V, T, S> StatefulSource<V, T, S>
where
    S: StatefulSourceImpl<V, T>,
{
    pub fn new(source: S) -> Self {
        Self(source, PhantomData)
    }
}

pub trait StatefulSourcePartition<V, T> {
    type PartitionState;

    /// Poll this partition, return anywhere from 0 to N new records
    fn poll(&mut self) -> Option<(V, T)>;

    /// snapshot the current state of this partition
    fn snapshot(&self) -> Self::PartitionState;

    /// collect and shutdown this partition
    /// this gets called when the partition is moved to another worker
    fn collect(self) -> Self::PartitionState;

    /// Suspend this partition.
    /// Suspend means the execution will be halted, but could continue later
    /// from a snapshot.
    /// Use this method to clean up any recources like external connections or
    /// file handles
    fn suspend(&mut self) -> () {}
}

impl<V, T, S> StreamSource<S::Part, V, T> for StatefulSource<V, T, S>
where
    S: StatefulSourceImpl<V, T>,
    V: Data,
    T: Timestamp,
{
    fn into_stream(
        self,
        builder: JetStreamBuilder<NoKey, NoData, NoTime>,
    ) -> JetStreamBuilder<S::Part, V, T> {
        let part_lister = SingleIteratorSource::new(self.0.list_parts());
        let partition_op = StatefulSourcePartitionOp::new(self.0);
        let keyed_stream = builder
            .source(part_lister)
            .key_distribute(|msg| msg.value.clone(), rendezvous_select);
        keyed_stream.then(OperatorBuilder::direct(partition_op.into_logic()))
    }
}

struct StatefulSourcePartitionOp<V, T, Builder: StatefulSourceImpl<V, T>> {
    partitions: IndexMap<Builder::Part, Builder::SourcePartition>,
    part_builder: Builder,
    _out_data: PhantomData<(Builder::PartitionState, V, T)>,
}

impl<V, T, Builder> StatefulSourcePartitionOp<V, T, Builder>
where
    Builder: StatefulSourceImpl<V, T>,
    Builder::Part: Hash + Eq,
{
    fn new(part_builder: Builder) -> Self {
        Self {
            partitions: IndexMap::new(),
            part_builder,
            _out_data: PhantomData,
        }
    }

    fn add_partition(
        &mut self,
        part: Builder::Part,
        part_state: Option<Builder::PartitionState>,
    ) -> () {
        let partition = self.part_builder.build_part(&part, part_state);
        self.partitions.insert(part, partition);
    }
}

impl<VO, TO, Builder> LogicWrapper<Builder::Part, Builder::Part, usize, VO, TO>
    for StatefulSourcePartitionOp<VO, TO, Builder>
where
    Builder: StatefulSourceImpl<VO, TO>,
    VO: Data,
    TO: Timestamp,
{
    fn on_schedule(
        &mut self,
        output: &mut Sender<Builder::Part, VO, TO>,
        _ctx: &mut OperatorContext,
    ) -> () {
        for (part, partition) in self.partitions.iter_mut() {
            if let Some((data, time)) = partition.poll() {
                let out_msg = DataMessage::new(part.clone(), data, time);
                output.send(out_msg.into());
            }
        }
    }

    fn on_data(
        &mut self,
        data_message: DataMessage<Builder::Part, Builder::Part, usize>,
        _output: &mut Sender<Builder::Part, VO, TO>,
        _ctx: &mut OperatorContext,
    ) -> () {
        let part = data_message.key;
        let partition = self.part_builder.build_part(&part, None);
        self.partitions.insert(part, partition);
    }

    fn on_epoch(
        &mut self,
        _epoch: usize,
        _output: &mut Sender<Builder::Part, VO, TO>,
        _ctx: &mut OperatorContext,
    ) -> () {
    }

    fn on_barrier(
        &mut self,
        barrier: &mut Barrier,
        _output: &mut Sender<Builder::Part, VO, TO>,
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
        _output: &mut Sender<Builder::Part, VO, TO>,
        _ctx: &mut OperatorContext,
    ) -> () {
    }

    fn on_suspend(
        &mut self,
        _suspend_marker: &mut SuspendMarker,
        _output: &mut Sender<Builder::Part, VO, TO>,
        _ctx: &mut OperatorContext,
    ) -> () {
        for partition in self.partitions.values_mut() {
            partition.suspend();
        }
    }

    fn on_interrogate(
        &mut self,
        interrogate: &mut Interrogate<Builder::Part>,
        _output: &mut Sender<Builder::Part, VO, TO>,
        _ctx: &mut OperatorContext,
    ) -> () {
        let keys = self.partitions.keys();
        interrogate.add_keys(keys);
    }

    fn on_collect(
        &mut self,
        collect: &mut Collect<Builder::Part>,
        _output: &mut Sender<Builder::Part, VO, TO>,
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
        _output: &mut Sender<Builder::Part, VO, TO>,
        ctx: &mut OperatorContext,
    ) -> () {
        let partition_state = acquire.take_state(&ctx.operator_id);
        if let Some((part, part_state)) = partition_state {
            self.add_partition(part, Some(part_state));
        }
    }
}
