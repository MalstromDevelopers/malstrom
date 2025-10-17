use std::hash::Hash;
use std::marker::PhantomData;

use crate::{
    operators::{Source, StreamSource},
    runtime::communication::Distributable,
    stream::{InitialStreamBuilder, Malstrom, StreamBuilder},
    types::{Data, Key, Kvt, MaybeKey, MaybeTime, NoData, NoKey, NoTime, Timestamp},
};

use super::{StatefulSource, StatefulSourceImpl, StatefulSourcePartition};

/// A source which provides records for processing and does not hold any state
/// (or only ephemeral state)
pub struct StatelessSource<M: Kvt, S: StatelessSourceImpl<M>>(SourceWrapper<M, S>);
impl<M, S> StatelessSource<M, S>
where
    M: Kvt,
    S: StatelessSourceImpl<M>,
{
    /// Create a new stateless source from the given source implementation.
    pub fn new(source: S) -> Self {
        Self(SourceWrapper(source, PhantomData))
    }
}

/// Implementation of a stateless stream source
pub trait StatelessSourceImpl<M: Kvt>: 'static {
    /// A `Part` of a partition is a key by which any partition of the source is
    /// uniquely identified. It is perfectly valid for a source to only have a single part and in
    /// turn only a single partition, though this may not be very useful.
    type Part: Distributable + MaybeKey + Hash + Eq;
    /// A partition of this source. Each partition must be able to read unique values.
    /// Partitions may be moved to different workers, when the jobs worker set changes. Usually
    /// partitions will directly relate to some partitioning used by the external system providing
    /// the data.
    type SourcePartition: StatelessSourcePartition<M>;

    /// List all initial partitions for this source
    fn list_parts(&self) -> Vec<Self::Part>;

    /// Build the partition for the given part
    fn build_part(&mut self, part: &Self::Part) -> Self::SourcePartition;
}

/// A single partition of a stateless source. A partition is the smallest unit of a source and may
/// be moved to a different worker when the job's worker set changes.
pub trait StatelessSourcePartition<M: Kvt> {
    /// Poll this partition, return anywhere from 0 to N new records
    fn poll(&mut self) -> Option<(<M as Kvt>::Value, <M as Kvt>::Timestamp)>;

    /// Suspend this partition.
    /// Suspend means the execution will be halted, but could continue later.
    /// Use this method to clean up any recources like external connections or
    /// file handles
    fn suspend(&mut self) {}

    /// Return true if this parition is finished and can be removed
    fn is_finished(&mut self) -> bool;
}

/// NewType on which we can implement StatefulSourceImpl
struct SourceWrapper<M: Kvt, S: StatelessSourceImpl<M>>(S, PhantomData<M>);

impl<Out, S> StatefulSourceImpl<Out> for SourceWrapper<Out, S>
where
    Out: Kvt<Key = S::Part>,
    S: StatelessSourceImpl<Out>,
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

impl<S, M> StatefulSourcePartition<M> for PartitionWrapper<S>
where
    M: Kvt,
    S: StatelessSourcePartition<M>,
{
    type PartitionState = ();

    fn poll(&mut self) -> Option<(<M as Kvt>::Value, <M as Kvt>::Timestamp)> {
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

impl<S, M> StreamSource<M> for StatelessSource<M, S>
where
    M: Kvt<Key = S::Part>,
    M::Timestamp: Timestamp,
    S: StatelessSourceImpl<M>,
{
    fn into_stream(
        self,
        name: &str,
        builder: InitialStreamBuilder,
    ) -> StreamBuilder<(M::Key, M::Value, M::Timestamp)> {
        builder.source(name, StatefulSource::new(self.0))
    }
}
