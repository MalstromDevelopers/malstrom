use std::hash::Hash;
use std::marker::PhantomData;

use crate::{
    operators::{Source, StreamSource},
    runtime::communication::Distributable,
    stream::StreamBuilder,
    types::{Data, MaybeKey, MaybeTime, NoData, NoKey, NoTime, Timestamp},
};

use super::{StatefulSource, StatefulSourceImpl, StatefulSourcePartition};

/// A source which provides records for processing and does not hold any state
/// (or only ephemeral state)
pub struct StatelessSource<V, T, S: StatelessSourceImpl<V, T>>(SourceWrapper<V, T, S>);
impl<V, T, S> StatelessSource<V, T, S>
where
    S: StatelessSourceImpl<V, T>,
{
    /// Create a new stateless source from the given source implementation.
    pub fn new(source: S) -> Self {
        Self(SourceWrapper(source, PhantomData))
    }
}

/// Implementation of a stateless stream source
pub trait StatelessSourceImpl<V, T>: 'static {
    /// A `Part` of a partition is a key by which any partition of the source is
    /// uniquely identified. It is perfectly valid for a source to only have a single part and in
    /// turn only a single partition, though this may not be very useful.
    type Part: Distributable + MaybeKey + Hash + Eq;
    /// A partition of this source. Each partition must be able to read unique values.
    /// Partitions may be moved to different workers, when the jobs worker set changes. Usually
    /// partitions will directly relate to some partitioning used by the external system providing
    /// the data.
    type SourcePartition: StatelessSourcePartition<V, T>;

    /// List all initial partitions for this source
    fn list_parts(&self) -> Vec<Self::Part>;

    /// Build the partition for the given part
    fn build_part(&mut self, part: &Self::Part) -> Self::SourcePartition;
}

/// A single partition of a stateless source. A partition is the smallest unit of a source and may
/// be moved to a different worker when the job's worker set changes.
pub trait StatelessSourcePartition<V, T> {
    /// Poll this partition, return anywhere from 0 to N new records
    fn poll(&mut self) -> Option<(V, T)>;

    /// Suspend this partition.
    /// Suspend means the execution will be halted, but could continue later.
    /// Use this method to clean up any recources like external connections or
    /// file handles
    fn suspend(&mut self) {}

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

    fn suspend(&mut self) {
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
        builder: StreamBuilder<NoKey, NoData, NoTime>,
    ) -> StreamBuilder<S::Part, V, T> {
        builder.source(name, StatefulSource::new(self.0))
    }
}
