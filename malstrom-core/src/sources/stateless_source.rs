use std::hash::Hash;
use std::marker::PhantomData;

use crate::{
    keyed::distributed::DistKey,
    operators::{Source, StreamSource},
    runtime::communication::Distributable,
    stream::JetStreamBuilder,
    types::{Data, MaybeKey, MaybeTime, NoData, NoKey, NoTime, Timestamp},
};

use super::{StatefulSource, StatefulSourceImpl, StatefulSourcePartition};

pub struct StatelessSource<V, T, S: StatelessSourceImpl<V, T>>(SourceWrapper<V, T, S>);
impl<V, T, S> StatelessSource<V, T, S>
where
    S: StatelessSourceImpl<V, T>,
{
    pub fn new(source: S) -> Self {
        Self(SourceWrapper(source, PhantomData))
    }
}

pub trait StatelessSourceImpl<V, T>: 'static {
    type Part: Distributable + MaybeKey + Hash + Eq;
    type SourcePartition: StatelessSourcePartition<V, T>;

    /// List all initial partitions for this source
    fn list_parts(&self) -> Vec<Self::Part>;

    fn build_part(&mut self, part: &Self::Part) -> Self::SourcePartition;
}

pub trait StatelessSourcePartition<V, T> {
    /// Poll this partition, return anywhere from 0 to N new records
    fn poll(&mut self) -> Option<(V, T)>;

    /// Suspend this partition.
    /// Suspend means the execution will be halted, but could continue later.
    /// Use this method to clean up any recources like external connections or
    /// file handles
    fn suspend(&mut self) -> () {}

    /// Return true if this parition is finished and can be removed
    fn is_finished(&mut self) -> bool;
}

/// NewType on which we can implement StatefulSourceImpl
struct SourceWrapper<V, T, S: StatelessSourceImpl<V, T>>(S, PhantomData<(V, T)>);
impl<S, V, T> StatefulSourceImpl<V, T> for SourceWrapper<V, T, S>
where
    V: Data,
    T: MaybeTime,
    S: StatelessSourceImpl<V, T>,
{
    type Part = S::Part;
    type PartitionState = ();
    type SourcePartition = PartitionWrapper<S::SourcePartition>;

    fn list_parts(&self) -> Vec<Self::Part> {
        self.0.list_parts()
    }

    fn build_part(
        &mut self,
        part: &Self::Part,
        _part_state: Option<Self::PartitionState>,
    ) -> Self::SourcePartition {
        PartitionWrapper(self.0.build_part(part))
    }
}

struct PartitionWrapper<S>(S);

impl<S, V, T> StatefulSourcePartition<V, T> for PartitionWrapper<S>
where
    S: StatelessSourcePartition<V, T>,
{
    type PartitionState = ();

    fn poll(&mut self) -> Option<(V, T)> {
        self.0.poll()
    }

    fn snapshot(&self) -> Self::PartitionState {}

    fn collect(mut self) -> Self::PartitionState {
        self.0.suspend();
    }

    fn suspend(&mut self) -> () {
        self.0.suspend();
    }

    fn is_finished(&mut self) -> bool {
        self.0.is_finished()
    }
}

impl<V, T, S> StreamSource<S::Part, V, T> for StatelessSource<V, T, S>
where
    V: Data,
    T: Timestamp,
    S: StatelessSourceImpl<V, T>,
{
    fn into_stream(
        self,
        name: &str,
        builder: JetStreamBuilder<NoKey, NoData, NoTime>,
    ) -> JetStreamBuilder<S::Part, V, T> {
        builder.source(name, StatefulSource::new(self.0))
    }
}
