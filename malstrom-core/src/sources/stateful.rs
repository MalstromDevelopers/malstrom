use std::marker::PhantomData;

use futures::channel::oneshot::Cancellation;

use crate::{
    channels::operator_io::{Input, Output}, keyed::{Distribute as _, rendezvous_select}, operators::{CommUtility, StreamSource}, stream::{InitialStreamBuilder, Logic, LogicBuilder, Malstrom as _, Operator, OperatorContext, StreamBuilder}, types::{
        Data, DataMessage, Key, Kvt, Message, NoData, Timestamp, WorkerId, distributable::Distributable
    }
};

/// Implementation of a stateful source.
pub trait StatefulSourceImpl<V, T>: 'static {
    /// A `Part` of a partition is a key by which any partition of the source is
    /// uniquely identified. It is perfectly valid for a source to only have a single part and in
    /// turn only a single partition, though this may not be very useful.
    type Part: Distributable + Key;
    /// State for a partition of this source. The state is persisted across job restarts
    /// and moved with the partition to a different worker when the jobs worker set changes.
    type PartitionState: Distributable;
    /// A partition of this source. Each partition must be able to read unique values.
    /// Partitions may be moved to different workers, when the jobs worker set changes. Usually
    /// partitions will directly relate to some partitioning used by the external system providing
    /// the data.
    type SourcePartition: StatefulSourcePartition<V, T, PartitionState = Self::PartitionState>;

    /// List all partitions for this source
    async fn list_parts(&self) -> Vec<Self::Part>;

    /// Build the partition for the given part
    async fn build_part(
        &mut self,
        part: &Self::Part,
        part_state: Option<Self::PartitionState>,
    ) -> Self::SourcePartition;
}

/// A source which provides records for processing and holds some persistent state.
pub struct StatefulSource<
    Out: Kvt,
    SrcImpl: StatefulSourceImpl<Out::Value, Out::Timestamp, Part = Out::Key>,
>(SrcImpl, PhantomData<Out>);

impl<V, T, SrcImpl> StatefulSource<(SrcImpl::Part, V, T), SrcImpl>
where
    V: Data,
    T: Timestamp,
    SrcImpl: StatefulSourceImpl<V, T>,
{
    /// Create a new stateful source from the given source implementation.
    pub fn new(source: SrcImpl) -> Self {
        Self(source, PhantomData)
    }
}
impl<Out, SrcImpl> StreamSource<Out> for StatefulSource<Out, SrcImpl>
where
    SrcImpl: StatefulSourceImpl<Out::Value, Out::Timestamp, Part = Out::Key>,
    Out: Kvt,
    Out::Key: Distributable + Key,
    Out::Value: Data,
    Out::Timestamp: Timestamp,
{
    fn into_stream(
        self,
        name: &str,
        builder: InitialStreamBuilder,
    ) -> StreamBuilder<(Out::Key, Out::Value, Out::Timestamp)> {
        let part_lister = PartLister::new(self.0);

        builder
            // this thing also emits the max epoch once all partitions on the worker are finished,
            // the distribute then makes sure the MAX epoch is only emitted downstream once it is
            // aligned across workers
            .then(Operator::built_by(format!("{name}-list-partitions"), part_lister))
            .distribute(format!("{name}-distribute-partitions"), rendezvous_select)
            .then(Operator::built_by(
                format!("{name}-partition"),
                partition_builder,
            ))
    }
}

/// A single partition of a statefull source. A partition is the smallest unit of a source and may
/// be moved to a different worker when the job's worker set changes.
pub trait StatefulSourcePartition<V, T> {
    /// Persistent state of this partition. This state will be retained across job restarts and
    /// moved along with the partition if the jobs worker set changes
    type PartitionState;

    /// Poll this partition, return None if no further records
    /// will be returned by this partition
    async fn poll(&mut self) -> Option<(V, T)>;

    /// Return true if this parition is finished and can be removed
    // fn is_finished(&mut self) -> bool;

    /// snapshot the current state of this partition
    async fn snapshot(&self) -> Self::PartitionState;

    /// collect and shutdown this partition
    /// this gets called when the partition is moved to another worker
    async fn collect(self) -> Self::PartitionState;
}

struct PartitionsFinished;

struct PartLister<Out: Kvt, S> {
    parts: Vec<Out::Key>,
    source_impl: S,
    /// Communication to other Workers
    comm: CommUtility<WorkerId>
}

enum PartListerCom<Part> {
    /// Inform main PartLister that a partition has been finished
    PartFinished(Part),
    /// Inform the other workers, that all Partitions have been finished
    AllPartsFinished
}

impl<Out, S> PartLister<Out, S> where Out: Kvt {
    fn new(source_impl: S) -> Self {
        Self{parts: Vec::new(), source_impl}
    }
}

impl<Out, S> LogicBuilder<(), (Out::Key, NoData, Out::Timestamp)> for PartLister<Out, S>
where
    Out: Kvt,
    Out::Key: Key,
    Out::Timestamp: Timestamp,
    S: StatefulSourceImpl<Out::Value, Out::Timestamp, Part = Out::Key>,
{
    type Logic = Self;

    async fn build(mut self, ctx: &mut crate::stream::BuildContext) -> Self::Logic {
        if ctx.worker_id == 0 {
            self.parts = (self.source_impl).list_parts().await;
        }
        self
    }
}
impl<Out, S> Logic<(), (Out::Key, NoData, Out::Timestamp)> for PartLister<Out, S>
where
    Out: Kvt,
    Out::Key: Key,
    Out::Timestamp: Timestamp,
    S: StatefulSourceImpl<Out::Value, Out::Timestamp, Part = Out::Key>,
{
    async fn apply(
        &mut self,
        input: &mut Input<()>,
        output: &mut Output<(Out::Key, NoData, Out::Timestamp)>,
        ctx: &mut OperatorContext,
    ) {
        for part in self.parts.drain(..) {
            let msg = DataMessage::new(part, NoData, Out::Timestamp::MIN);
            output.send(Message::Data(msg)).await;
        }

        tokio::select! {
            msg = input.recv() => {
                match msg {
                Message::Data(_) => (),
                Message::Epoch(_) => (),
                Message::AbsBarrier(x) => output.send(Message::AbsBarrier(x)).await,
                Message::Rescale(x) => output.send(Message::Rescale(x)).await,
                Message::ReconfigComplete(x) => output.send(Message::ReconfigComplete(x)).await,
                Message::Interrogate(x) => (),
                Message::Collect(x) => (),
                Message::Acquire(x) => (),
                }
            }
        }
    }
}
