//! This module provides a simplified interface for defining stateful
//! paritioned sources that support dynamic rescaling

use std::{hash::Hash, marker::PhantomData};

use futures::{FutureExt, StreamExt, stream::FuturesUnordered};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::{
    channels::operator_io::{Input, Output},
    keyed::{
        Distribute,
        distributed::{Acquire, Collect, Interrogate},
        partitioners::rendezvous_select,
    },
    operators::StreamSource,
    runtime::{
        BiCommunicationClient,
        communication::{Distributable, broadcast},
    },
    snapshot::Barrier,
    stream::{
        BuildContext, InitialStreamBuilder, Logic, LogicBuilder, Malstrom as _, Operator,
        OperatorContext, SafeLogic, SafeLogicWrapper, StreamBuilder,
    },
    types::{
        Data, DataMessage, Key, Kvt, MaybeKey, Message, NoData, NoKey, NoTime, OnceTime, RescaleMessage, SuspendMarker, Timestamp, WorkerId
    },
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
    fn list_parts(&self) -> Vec<Self::Part>;

    /// Build the partition for the given part
    fn build_part(
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
    fn snapshot(&self) -> Self::PartitionState;

    /// collect and shutdown this partition
    /// this gets called when the partition is moved to another worker
    fn collect(self) -> Self::PartitionState;

    /// Gets called when execution gets suspended, possibly resuming later.
    fn suspend(&mut self) {}
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
        let parts = self.0.list_parts();
        let all_partitions: IndexMap<SrcImpl::Part, bool> =
            parts.iter().map(|x| (x.clone(), false)).collect();

        let part_lister =
            Operator::built_by(format!("{name}-list-parts"), PartListerBuilder { parts });

        builder
            .then(part_lister)
            .distribute(&format!("{name}-distribute-partitions"), rendezvous_select)
            .then(Operator::built_by(
                format!("{name}-partition"),
                StatefulSourcePartitionOpBuilder {
                    src_impl: self.0,
                    all_partitions,
                    _out_type: PhantomData::<Out>,
                },
            ))
    }
}

struct PartListerBuilder<Part> {
    parts: Vec<Part>,
}

impl<Part> LogicBuilder<(), (Part, NoData, OnceTime)> for PartListerBuilder<Part>
where
    Part: Key,
{
    type Logic = PartLister<Part>;

    async fn build(self, ctx: &mut BuildContext) -> Self::Logic {
        let parts = if ctx.worker_id == 0 {
            Box::new(self.parts.into_iter())
        } else {
            // do not emit on non-0 worker
            Box::new(std::iter::empty::<Part>()) as Box<dyn Iterator<Item = Part>>
        };
        PartLister { parts, max_ts: Some(()) }
    }
}

struct PartLister<Part> {
    parts: Box<dyn Iterator<Item = Part>>,
    /// take this option to send the MAX timestamp indicating
    /// the iterator has finished
    max_ts: Option<()>
}

impl<Part> Logic<(), (Part, NoData, OnceTime)> for PartLister<Part>
where
    Part: Key,
{
    async fn apply(
        &mut self,
        input: &mut Input<()>,
        output: &mut Output<(Part, NoData, OnceTime)>,
        _ctx: &mut OperatorContext,
    ) {
        for part in self.parts.by_ref() {
            output
                .send(Message::Data(DataMessage::new(part, NoData, OnceTime::MIN)))
                .await;
        }
        if let Some(_) = self.max_ts.take() {
            output.send(Message::Epoch(OnceTime::MAX)).await;
        }
        
        match input.recv().await {
            Message::Data(_) => (),
            Message::Epoch(_) => (),
            Message::AbsBarrier(x) => output.send(Message::AbsBarrier(x)).await,
            Message::Rescale(x) => output.send(Message::Rescale(x)).await,
            Message::SuspendMarker(x) => output.send(Message::SuspendMarker(x)).await,
            Message::Interrogate(_) => unreachable!(),
            Message::Collect(_) => unreachable!(),
            Message::Acquire(_) => unreachable!(),
        }
    }
}

/// Marker we send to broadcast, that a partition has finished.
/// We need this to avoid an edge case where all local partitions finish and we send the MAX time,
/// but then get assigned a new unfinished partition due to a rescale.
/// So we broadcast partition info to only emit MAX time when all partitions globally are finished
#[derive(Serialize, Deserialize, Hash, PartialEq, Eq, Clone)]
struct PartitionFinished<Part>(Part);

/// Java-esque name, maybe we should name it Factory instead of Builder?
struct StatefulSourcePartitionOpBuilder<
    Out: Kvt,
    SrcImpl: StatefulSourceImpl<Out::Value, Out::Timestamp, Part = Out::Key>,
> {
    src_impl: SrcImpl,
    all_partitions: IndexMap<SrcImpl::Part, bool>,
    _out_type: PhantomData<Out>,
}
impl<In, Out, SrcImpl> LogicBuilder<In, (Out::Key, Out::Value, Out::Timestamp)>
    for StatefulSourcePartitionOpBuilder<Out, SrcImpl>
where
    SrcImpl: StatefulSourceImpl<Out::Value, Out::Timestamp, Part = Out::Key>,
    In: Kvt<Key = SrcImpl::Part, Value = NoData, Timestamp = OnceTime>,
    Out: Kvt,
    Out::Key: Key + Distributable,
    Out::Value: Data,
    Out::Timestamp: Timestamp,
{
    type Logic = StatefulSourcePartitionOp<Out, SrcImpl>;

    async fn build(self, ctx: &mut BuildContext) -> Self::Logic {
        StatefulSourcePartitionOp::new(ctx, self.src_impl, self.all_partitions).await
    }
}

struct StatefulSourcePartitionOp<
    Out: Kvt,
    SrcImpl: StatefulSourceImpl<Out::Value, Out::Timestamp, Part = Out::Key>,
> {
    partitions: IndexMap<SrcImpl::Part, SrcImpl::SourcePartition>,
    part_builder: SrcImpl,
    all_partitions: IndexMap<SrcImpl::Part, bool>, // true if partition is finished
    comm_clients: IndexMap<WorkerId, BiCommunicationClient<PartitionFinished<SrcImpl::Part>>>,
    // final marker, we keep it in an option to only send it once
    max_t: Option<Out::Timestamp>,
    _phantom: PhantomData<(SrcImpl::PartitionState, Out::Value)>,
}

impl<Out, SrcImpl> StatefulSourcePartitionOp<Out, SrcImpl>
where
    SrcImpl: StatefulSourceImpl<Out::Value, Out::Timestamp, Part = Out::Key>,
    SrcImpl::Part: Key,
    Out: Kvt,
    Out::Key: Key + Distributable,
    Out::Timestamp: Timestamp,
{
    async fn new(
        ctx: &mut BuildContext,
        part_builder: SrcImpl,
        all_partitions: IndexMap<SrcImpl::Part, bool>,
    ) -> Self {
        let comm_clients =
            ctx.create_all_communication_clients::<PartitionFinished<SrcImpl::Part>>();
        let mut this = Self {
            partitions: IndexMap::new(),
            part_builder,
            all_partitions,
            comm_clients,
            // This is technically state which gets lost on restarts, but sending T::MAX multiple
            // times should not be an issue
            max_t: Some(Out::Timestamp::MAX),
            _phantom: PhantomData,
        };

        if let Some(state) = ctx
            .load_state::<IndexMap<SrcImpl::Part, SrcImpl::PartitionState>>()
            .await
        {
            for (k, v) in state.into_iter() {
                this.add_partition(k, Some(v));
            }
        }
        this
    }

    fn add_partition(&mut self, part: SrcImpl::Part, part_state: Option<SrcImpl::PartitionState>) {
        let partition = self.part_builder.build_part(&part, part_state);
        self.partitions.insert(part, partition);
    }
}

impl<In, Out, SrcImpl> Logic<In, (Out::Key, Out::Value, Out::Timestamp)>
    for StatefulSourcePartitionOp<Out, SrcImpl>
where
    SrcImpl: StatefulSourceImpl<Out::Value, Out::Timestamp, Part = Out::Key>,
    In: Kvt<Key = SrcImpl::Part, Value = NoData, Timestamp = OnceTime>,
    Out: Kvt,
    Out::Key: Key + Distributable,
    Out::Value: Data,
    Out::Timestamp: Timestamp,
{
    async fn apply(
        &mut self,
        input: &mut Input<In>,
        output: &mut Output<(Out::Key, Out::Value, Out::Timestamp)>,
        ctx: &mut OperatorContext,
    ) {
        // TODO: All these iterations may be kinda inefficient
        // try to emit an epoch
        if let Some(t) = self
            .max_t
            .take_if(|_| self.all_partitions.values().all(|x| *x))
        {
            output.send(Message::Epoch(t)).await;
            return;
        }
        /// new partition assigned
        let new_partition = input.recv();
        /// new data from local partition
        let mut data_polls: FuturesUnordered<_> = self
            .partitions
            .iter_mut()
            .map(async |(part, partition)| (part, partition.poll().await))
            .collect();
        /// information about from remote partition
        let mut remote_msgs: FuturesUnordered<_> = self
            .comm_clients
            .values()
            .map(async |x| x.recv_async().await)
            .collect();

        // the Some() is needed because an empty iterator returns immediately with None
        // drop() calls are needed so we can borrwo self mutably again
        tokio::select! {
            Some((part, value)) = data_polls.next() => {
                    match value {
                        Some((dt, ts)) => {
                            let msg = DataMessage::new(part.clone(), dt, ts);
                            output.send(Message::Data(msg)).await;
                        },
                        None => {
                            let part_state = self
                                .all_partitions
                                .get_mut(part)
                                .expect("Expected partition state to exist");
                            *part_state = true;
                            broadcast(self.comm_clients.values(), PartitionFinished(part.clone()));
                        },
                    }
            },
            Some(msg) = remote_msgs.next() => {
                *self
                    .all_partitions
                    .get_mut(&msg.0)
                    .expect("Expected partition state to exist") = true;
            }
            msg = new_partition => {
                // need to drop these so we can mut self
                drop(data_polls);
                drop(remote_msgs);
                match msg {
                    Message::Data(data_message) => {
                        let part = data_message.key;
                        if !self.partitions.contains_key(&part) {
                            let partition = self.part_builder.build_part(&part, None);
                            self.partitions.insert(part, partition);
                        }
                    }
                    Message::Epoch(_) => {}
                    Message::AbsBarrier(mut barrier) => {
                        let state: IndexMap<SrcImpl::Part, SrcImpl::PartitionState> = self
                            .partitions
                            .iter()
                            .map(|(k, v)| (k.clone(), v.snapshot()))
                            .collect();
                        barrier.persist(&state, &ctx.operator_id);
                        output.send(Message::AbsBarrier(barrier));
                    }
                    Message::Rescale(rescale_message) => {
                        let new_workers = rescale_message.get_new_workers();
                        self.comm_clients.retain(|wid, _| new_workers.contains(wid));
                        for wid in new_workers.iter() {
                            if !self.comm_clients.contains_key(wid) && !wid == ctx.worker_id {
                                let client = ctx.create_communication_client(*wid);
                                self.comm_clients.insert(*wid, client);
                            }
                        }
                        output.send(Message::Rescale(rescale_message));
                    }
                    Message::SuspendMarker(suspend_marker) => {
                        for partition in self.partitions.values_mut() {
                            partition.suspend();
                        }
                        output.send(Message::SuspendMarker(suspend_marker));
                    }
                    Message::Interrogate(mut interrogate) => {
                        let keys = self.partitions.keys();
                        interrogate.add_keys(keys);
                        output.send(Message::Interrogate(interrogate));
                    }
                    Message::Collect(mut collect) => {
                        let key_state = self.partitions.swap_remove(&collect.key);
                        if let Some(partition) = key_state {
                            collect.add_state(ctx.operator_id, partition.collect());
                        }
                        output.send(Message::Collect(collect));
                    }
                    Message::Acquire(acquire) => {
                        let partition_state = acquire.take_state(&ctx.operator_id);
                        if let Some((part, part_state)) = partition_state {
                            self.add_partition(part, Some(part_state));
                        }
                        output.send(Message::Acquire(acquire));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Mutex, time::Duration};

    use crate::{
        operators::*,
        runtime::SingleThreadRuntime,
        sinks::{StatelessSink, VecSink},
        sources::{StatefulSource, StatefulSourceImpl, StatefulSourcePartition},
        testing::CapturingPersistenceBackend,
        worker::StreamProvider,
    };

    struct MockSource(i32);
    struct MockSourcePartition {
        max: i32,
        next: i32,
        was_snapshotted: Mutex<bool>,
    }

    impl StatefulSourceImpl<i32, i32> for MockSource {
        type Part = ();

        type PartitionState = i32;

        type SourcePartition = MockSourcePartition;

        fn list_parts(&self) -> Vec<Self::Part> {
            vec![()]
        }

        fn build_part(
            &mut self,
            _part: &Self::Part,
            part_state: Option<Self::PartitionState>,
        ) -> Self::SourcePartition {
            MockSourcePartition {
                max: self.0,
                next: part_state.unwrap_or_default(),
                was_snapshotted: Mutex::new(false),
            }
        }
    }

    impl StatefulSourcePartition<i32, i32> for MockSourcePartition {
        type PartitionState = i32;

        async fn poll(&mut self) -> Option<(i32, i32)> {
            if self.next > self.max {
                None
            } else {
                let out = (self.next, self.next);
                self.next += 1;
                Some(out)
            }
        }

        // fn is_finished(&mut self) -> bool {
        //     // only terminate after we have made a snapshot
        //     self.next > self.max && *self.was_snapshotted.lock().unwrap()
        // }

        fn snapshot(&self) -> Self::PartitionState {
            *self.was_snapshotted.lock().unwrap() = true;
            self.next
        }

        fn collect(self) -> Self::PartitionState {
            self.next
        }
    }

    /// Check that state gets loaded from persistence backend
    /// on initial start
    #[test]
    fn test_state_is_loaded_from_persistence() {
        let persistence = CapturingPersistenceBackend::default();

        let first_sink = VecSink::new();
        let first_collected = first_sink.clone();

        // execute once, this will finish as soon as a snapshot was taken
        let rt = SingleThreadRuntime::builder()
            .snapshots(Duration::from_millis(50))
            .persistence(persistence.clone())
            .build(move |provider: &mut dyn StreamProvider| {
                provider
                    .new_stream()
                    .source("mock-source", StatefulSource::new(MockSource(10)))
                    .sink("vec-sink", StatelessSink::new(first_sink));
            });
        rt.execute().unwrap();
        let result: Vec<_> = first_collected
            .drain_vec(..)
            .iter()
            .map(|x| x.value)
            .collect();
        let expected: Vec<_> = (0..=10).collect();
        assert_eq!(result, expected);

        // execute again, only numbers 11-15 should have been counted since we started from the
        // state which had already counted to 10
        let second_sink = VecSink::new();
        let second_collected = second_sink.clone();

        // execute again
        let rt = SingleThreadRuntime::builder()
            .snapshots(Duration::from_millis(50))
            .persistence(persistence)
            .build(move |provider: &mut dyn StreamProvider| {
                provider
                    .new_stream()
                    .source("mock-source", StatefulSource::new(MockSource(15)))
                    .sink("vec-sink", StatelessSink::new(second_sink));
            });
        rt.execute().unwrap();
        let result: Vec<_> = second_collected
            .drain_vec(..)
            .iter()
            .map(|x| x.value)
            .collect();
        let expected: Vec<_> = (11..=15).collect();
        assert_eq!(result, expected);
    }
}
