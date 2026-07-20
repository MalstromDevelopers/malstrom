use std::{cell::RefCell, marker::PhantomData, rc::Rc};

use futures::{StreamExt, channel::oneshot::Cancellation, stream::FuturesUnordered};
use indexmap::{IndexMap, IndexSet};
use serde::{Deserialize, Serialize};

use crate::{
    channels::operator_io::{Input, Output},
    keyed::{
        Distribute as _,
        distributed::{Acquire, Collect, Interrogate},
        rendezvous_select,
    },
    operators::{CommUtility, StreamSource},
    stream::{
        BuildContext, InitialStreamBuilder, Logic, LogicBuilder, Malstrom as _, Operator,
        OperatorContext, SafeLogic, SafeLogicWrapper, StreamBuilder,
    },
    types::{
        Barrier, Data, DataMessage, Key, Kvt, Message, NoData, OnceTime, Timestamp, WorkerId,
        distributable::Distributable,
    },
};

/// Implementation of a stateful source.
pub trait StatefulSourceImpl: 'static {
    /// A `Part` of a partition is a key by which any partition of the source is
    /// uniquely identified. It is perfectly valid for a source to only have a single part and in
    /// turn only a single partition, though this may not be very useful.
    type Part: Distributable + Key;
    /// Values emitted by this source
    type Value: Distributable + Data;
    /// Timestamps emitted by this source
    type Timestamp: Distributable + Timestamp;
    /// State for a partition of this source. The state is persisted across job restarts
    /// and moved with the partition to a different worker when the jobs worker set changes.
    type PartitionState: Distributable;
    /// A partition of this source. Each partition must be able to read unique values.
    /// Partitions may be moved to different workers, when the jobs worker set changes. Usually
    /// partitions will directly relate to some partitioning used by the external system providing
    /// the data.
    type SourcePartition: StatefulSourcePartition<
            Value = Self::Value,
            Timestamp = Self::Timestamp,
            PartitionState = Self::PartitionState,
        >;

    /// List all partitions for this source
    async fn list_parts(&mut self) -> Vec<Self::Part>;

    /// Build the partition for the given part
    async fn build_part(
        &mut self,
        part: &Self::Part,
        part_state: Option<Self::PartitionState>,
    ) -> Self::SourcePartition;
}

/// A source which provides records for processing and holds some persistent state.
pub struct StatefulSource<SrcImpl: StatefulSourceImpl>(SrcImpl);

impl<SrcImpl> StatefulSource<SrcImpl>
where
    SrcImpl: StatefulSourceImpl,
{
    /// Create a new stateful source from the given source implementation.
    pub fn new(source: SrcImpl) -> Self {
        Self(source)
    }
}
impl<SrcImpl> StreamSource<(SrcImpl::Part, SrcImpl::Value, SrcImpl::Timestamp)>
    for StatefulSource<SrcImpl>
where
    SrcImpl: StatefulSourceImpl,
{
    fn into_stream(
        self,
        name: &str,
        builder: InitialStreamBuilder,
    ) -> StreamBuilder<(SrcImpl::Part, SrcImpl::Value, SrcImpl::Timestamp)> {
        let src_impl = Rc::new(RefCell::new(self.0));
        let part_lister = PartListerBuilder::new(Rc::clone(&src_impl));
        let partition_op = StatefulSourcePartitionOpBuilder::new(src_impl);
        builder
            // this thing also emits the max epoch once all partitions on the worker are finished,
            // the distribute then makes sure the MAX epoch is only emitted downstream once it is
            // aligned across workers
            .then(Operator::built_by(
                format!("{name}-list-partitions"),
                part_lister,
            ))
            .distribute(format!("{name}-distribute-partitions"), rendezvous_select)
            .then(Operator::built_by(
                format!("{name}-partition"),
                partition_op,
            ))
    }
}

/// A single partition of a statefull source. A partition is the smallest unit of a source and may
/// be moved to a different worker when the job's worker set changes.
pub trait StatefulSourcePartition {
    /// Persistent state of this partition. This state will be retained across job restarts and
    /// moved along with the partition if the jobs worker set changes
    type PartitionState;
    /// Values emitted by this partition
    type Value: Distributable + Data;
    /// Timestamps emitted by this partition
    type Timestamp: Distributable + Timestamp;

    /// Poll this partition, return None if no further records
    /// will be returned by this partition
    /// NOTE: This operation MUST BE cancel safe
    async fn poll(&mut self) -> Option<(Self::Value, Self::Timestamp)>;

    /// Return true if this parition is finished and can be removed
    // fn is_finished(&mut self) -> bool;

    /// snapshot the current state of this partition
    async fn snapshot(&self) -> Self::PartitionState;

    /// collect and shutdown this partition
    /// this gets called when the partition is moved to another worker
    async fn collect(self) -> Self::PartitionState;
}

struct PartitionsFinished;

struct PartListerBuilder<SrcImpl: StatefulSourceImpl> {
    source_impl: Rc<RefCell<SrcImpl>>,
}

impl<SrcImpl> PartListerBuilder<SrcImpl>
where
    SrcImpl: StatefulSourceImpl,
{
    fn new(source_impl: Rc<RefCell<SrcImpl>>) -> Self {
        Self { source_impl }
    }
}

impl<SrcImpl> LogicBuilder<(), (SrcImpl::Part, NoData, SrcImpl::Timestamp)>
    for PartListerBuilder<SrcImpl>
where
    SrcImpl: StatefulSourceImpl,
{
    type Logic = PartLister<SrcImpl>;

    async fn build(self, ctx: &mut BuildContext) -> Self::Logic {
        let com_utility = CommUtility::new(ctx).await;
        PartLister {
            parts: IndexSet::new(),
            source_impl: self.source_impl,
            comm: com_utility,
        }
    }
}

struct PartLister<SrcImpl: StatefulSourceImpl> {
    parts: IndexSet<SrcImpl::Part>,
    source_impl: Rc<RefCell<SrcImpl>>,
    /// Communication to other Workers
    comm: CommUtility<PartitionFinished<SrcImpl::Part>>,
}

// NOTE: Bit hacky using the SrcImpl timestamp type here instead of OnceTime or similar,
// but makes implementation a lot simpler because we can use SafeLogic for the PartitionOp
impl<SrcImpl> LogicBuilder<(), (SrcImpl::Part, NoData, SrcImpl::Timestamp)> for PartLister<SrcImpl>
where
    SrcImpl: StatefulSourceImpl,
{
    type Logic = Self;

    async fn build(mut self, ctx: &mut crate::stream::BuildContext) -> Self::Logic {
        if ctx.worker_id == 0 {
            let parts = (self.source_impl).borrow_mut().list_parts().await;
            self.parts = parts.into_iter().collect();
        }
        self
    }
}
impl<SrcImpl> Logic<(), (SrcImpl::Part, NoData, SrcImpl::Timestamp)> for PartLister<SrcImpl>
where
    SrcImpl: StatefulSourceImpl,
{
    async fn apply(
        &mut self,
        input: &mut Input<()>,
        output: &mut Output<(SrcImpl::Part, NoData, SrcImpl::Timestamp)>,
        ctx: &mut OperatorContext,
    ) {
        // only happens on worker 0 because the builder only populates parts there
        for part in self.parts.iter() {
            debug_assert!(ctx.worker_id == 0);
            let msg = DataMessage::new(part.clone(), NoData, SrcImpl::Timestamp::MIN);
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
            part_finished = self.comm.recv() => {
                // branch only active on worker 0 because messages are sent to there
                debug_assert!(ctx.worker_id == 0);
                let part = part_finished.0;
                self.parts.swap_remove(&part);
                /// If all partitions are finished send MAX epoch to indicate computation finish
                if self.parts.is_empty() {
                    output.send(Message::Epoch(SrcImpl::Timestamp::MAX)).await;
                }
            }
        }
    }
}

/// Marker we send to advise, that a partition has finished.
/// We need this to avoid an edge case where all local partitions finish and we send the MAX time,
/// but then get assigned a new unfinished partition due to a rescale.
/// So we broadcast partition info to only emit MAX time when all partitions globally are finished
#[derive(Serialize, Deserialize, Hash, PartialEq, Eq, Clone)]
struct PartitionFinished<Part>(Part);

struct StatefulSourcePartitionOp<SrcImpl: StatefulSourceImpl> {
    partitions: IndexMap<SrcImpl::Part, SrcImpl::SourcePartition>,
    part_builder: Rc<RefCell<SrcImpl>>,
    /// com to part lister
    com_utility: CommUtility<PartitionFinished<SrcImpl::Part>>,
}

/// Builds [StatefulSourcePartitionOp]
struct StatefulSourcePartitionOpBuilder<SrcImpl: StatefulSourceImpl> {
    src_impl: Rc<RefCell<SrcImpl>>,
}
impl<SrcImpl>
    LogicBuilder<
        (SrcImpl::Part, NoData, SrcImpl::Timestamp),
        (SrcImpl::Part, SrcImpl::Value, SrcImpl::Timestamp),
    > for StatefulSourcePartitionOpBuilder<SrcImpl>
where
    SrcImpl: StatefulSourceImpl,
{
    type Logic = SafeLogicWrapper<StatefulSourcePartitionOp<SrcImpl>>;

    async fn build(self, ctx: &mut BuildContext) -> Self::Logic {
        StatefulSourcePartitionOp::new(ctx, self.src_impl)
            .await
            .into_logic()
    }
}

impl<SrcImpl> StatefulSourcePartitionOpBuilder<SrcImpl>
where
    SrcImpl: StatefulSourceImpl,
{
    fn new(src_impl: Rc<RefCell<SrcImpl>>) -> Self {
        Self { src_impl }
    }
}

impl<SrcImpl> StatefulSourcePartitionOp<SrcImpl>
where
    SrcImpl: StatefulSourceImpl,
{
    async fn new(ctx: &mut BuildContext, part_builder: Rc<RefCell<SrcImpl>>) -> Self {
        let partitions = IndexMap::default();
        let com_utility = CommUtility::new(ctx).await;
        let mut this = StatefulSourcePartitionOp {
            partitions,
            part_builder,
            com_utility,
        };

        if let Some(state) = ctx
            .load_state::<IndexMap<SrcImpl::Part, SrcImpl::PartitionState>>()
            .await
        {
            for (k, v) in state.into_iter() {
                this.add_partition(k, Some(v)).await;
            }
        }
        this
    }

    async fn add_partition(
        &mut self,
        part: SrcImpl::Part,
        part_state: Option<SrcImpl::PartitionState>,
    ) {
        if !self.partitions.contains_key(&part) {
            let partition = self
                .part_builder
                .borrow_mut()
                .build_part(&part, part_state)
                .await;
            self.partitions.insert(part, partition);
        }
    }
}

impl<SrcImpl>
    SafeLogic<
        (SrcImpl::Part, NoData, SrcImpl::Timestamp),
        (SrcImpl::Part, SrcImpl::Value, SrcImpl::Timestamp),
    > for StatefulSourcePartitionOp<SrcImpl>
where
    SrcImpl: StatefulSourceImpl,
{
    async fn on_schedule(
        &mut self,
        output: &mut Output<(SrcImpl::Part, SrcImpl::Value, SrcImpl::Timestamp)>,
        ctx: &mut OperatorContext,
    ) {
        let mut polls: FuturesUnordered<_> = self
            .partitions
            .iter_mut()
            .map(|(k, v)| async move { (k, v.poll().await) })
            .collect();
        let next_data = polls.next().await;
        drop(polls); // drop so we can modify self.partitions
        match next_data {
            // fetched data
            Some((part, Some((data, timestamp)))) => {
                let msg = DataMessage::new(part.clone(), data, timestamp);
                output.send(Message::Data(msg)).await
            }
            // partition finished
            Some((part, None)) => {
                // need to clone because part is borrowed from self.partitions which
                // we can not mutate while it is borrowed
                let part = part.clone();
                self.partitions.swap_remove(&part);
                self.com_utility
                    .send(0, PartitionFinished(part.clone()))
                    .await;
            }
            // no partitions
            None => (),
        }
    }

    async fn on_data(
        &mut self,
        data_message: DataMessage<(SrcImpl::Part, NoData, SrcImpl::Timestamp)>,
        output: &mut Output<(SrcImpl::Part, SrcImpl::Value, SrcImpl::Timestamp)>,
        ctx: &mut OperatorContext,
    ) {
        let partition_key = data_message.key;
        self.add_partition(partition_key, None).await;
    }

    async fn on_acquire(
        &mut self,
        acquire: &mut Acquire<SrcImpl::Part>,
        output: &mut Output<(SrcImpl::Part, SrcImpl::Value, SrcImpl::Timestamp)>,
        ctx: &mut OperatorContext,
    ) {
        if let Some((part, part_state)) = acquire.take_state(&ctx.operator_id) {
            self.add_partition(part, part_state).await;
        }
    }

    async fn on_barrier(
        &mut self,
        barrier: &mut Barrier,
        output: &mut Output<(SrcImpl::Part, SrcImpl::Value, SrcImpl::Timestamp)>,
        ctx: &mut OperatorContext,
    ) {
        let mut snapshot: IndexMap<SrcImpl::Part, SrcImpl::PartitionState> = IndexMap::with_capacity(self.partitions.len());
        for (k, v) in self.partitions.iter() {
            let state = v.snapshot().await;
            snapshot.insert(k.clone(), state);
        }
        barrier.persist(&snapshot, &ctx.operator_id);
    }

    async fn on_collect(
        &mut self,
        collect: &mut Collect<SrcImpl::Part>,
        output: &mut Output<(SrcImpl::Part, SrcImpl::Value, SrcImpl::Timestamp)>,
        ctx: &mut OperatorContext,
    ) {
        let part = collect.get_key();
        if let Some(partition) = self.partitions.swap_remove(part) {
            let part_state = partition.snapshot().await;
            collect.add_state(ctx.operator_id, &part_state);
        }
    }

    async fn on_interrogate(
        &mut self,
        interrogate: &mut Interrogate<SrcImpl::Part>,
        output: &mut Output<(SrcImpl::Part, SrcImpl::Value, SrcImpl::Timestamp)>,
        ctx: &mut OperatorContext,
    ) {
        interrogate.add_keys(self.partitions.keys().cloned());
    }
}
