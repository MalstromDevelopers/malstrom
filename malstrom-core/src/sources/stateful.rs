//! This module provides a simplified interface for defining stateful
//! paritioned sources that support dynamic rescaling

use std::{hash::Hash, marker::PhantomData};

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::{
    channels::operator_io::{Input, Output},
    keyed::{
        distributed::{Acquire, Collect, Interrogate},
        partitioners::rendezvous_select,
        Distribute,
    },
    operators::StreamSource,
    runtime::{
        communication::{broadcast, Distributable},
        BiCommunicationClient,
    },
    snapshot::Barrier,
    stream::{BuildContext, LogicWrapper, OperatorBuilder, OperatorContext, StreamBuilder},
    types::{
        Data, DataMessage, MaybeKey, Message, NoData, NoKey, NoTime, RescaleMessage, SuspendMarker,
        Timestamp, WorkerId,
    },
};

/// Implementation of a stateful source.
pub trait StatefulSourceImpl<V, T>: 'static {
    /// A `Part` of a partition is a key by which any partition of the source is
    /// uniquely identified. It is perfectly valid for a source to only have a single part and in
    /// turn only a single partition, though this may not be very useful.
    type Part: Distributable + MaybeKey + Hash + Eq;
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
pub struct StatefulSource<V, T, S: StatefulSourceImpl<V, T>>(S, PhantomData<(V, T)>);
impl<V, T, S> StatefulSource<V, T, S>
where
    S: StatefulSourceImpl<V, T>,
{
    /// Create a new stateful source from the given source implementation.
    pub fn new(source: S) -> Self {
        Self(source, PhantomData)
    }
}

/// A single partition of a statefull source. A partition is the smallest unit of a source and may
/// be moved to a different worker when the job's worker set changes.
pub trait StatefulSourcePartition<V, T> {
    /// Persistent state of this partition. This state will be retained across job restarts and
    /// moved along with the partition if the jobs worker set changes
    type PartitionState;

    /// Poll this partition, possibly returning a record
    fn poll(&mut self) -> Option<(V, T)>;

    /// Return true if this parition is finished and can be removed
    fn is_finished(&mut self) -> bool;

    /// snapshot the current state of this partition
    fn snapshot(&self) -> Self::PartitionState;

    /// collect and shutdown this partition
    /// this gets called when the partition is moved to another worker
    fn collect(self) -> Self::PartitionState;

    /// Gets called when execution gets suspended, possibly resuming later.
    fn suspend(&mut self) {}
}

impl<V, T, S> StreamSource<S::Part, V, T> for StatefulSource<V, T, S>
where
    S: StatefulSourceImpl<V, T>,
    V: Data,
    T: Timestamp,
{
    fn into_stream(
        self,
        name: &str,
        builder: StreamBuilder<NoKey, NoData, NoTime>,
    ) -> StreamBuilder<S::Part, V, T> {
        let parts = self.0.list_parts();
        let all_partitions: IndexMap<S::Part, bool> =
            parts.iter().map(|x| (x.clone(), false)).collect();

        let parts = parts.into_iter();
        let part_lister =
            OperatorBuilder::built_by(&format!("{name}-list-parts"), move |build_context| {
                let mut inner = if build_context.worker_id == 0 {
                    Box::new(parts)
                } else {
                    // do not emit on non-0 worker
                    Box::new(std::iter::empty::<S::Part>()) as Box<dyn Iterator<Item = S::Part>>
                };
                move |input: &mut Input<NoKey, NoData, NoTime>,
                      output: &mut Output<S::Part, (), NoTime>,
                      _ctx| {
                    for part in inner.by_ref() {
                        output.send(Message::Data(DataMessage::new(part, (), NoTime)));
                    }
                    if let Some(msg) = input.recv() {
                        match msg {
                            Message::Data(_) => (),
                            Message::Epoch(_) => (),
                            Message::AbsBarrier(x) => output.send(Message::AbsBarrier(x)),
                            Message::Rescale(x) => output.send(Message::Rescale(x)),
                            Message::SuspendMarker(x) => output.send(Message::SuspendMarker(x)),
                            Message::Interrogate(_) => unreachable!(),
                            Message::Collect(_) => unreachable!(),
                            Message::Acquire(_) => unreachable!(),
                        }
                    }
                }
            });

        builder
            .then(part_lister)
            .distribute(&format!("{name}-distribute-partitions"), rendezvous_select)
            .then(OperatorBuilder::built_by(
                &format!("{name}-partition"),
                |ctx| {
                    let partition_op =
                        StatefulSourcePartitionOp::<V, T, S>::new(ctx, self.0, all_partitions);
                    partition_op.into_logic()
                },
            ))
    }
}

/// Marker we send to broadcast, that a partition has finished.
/// We need this to avoid an edge case where all local partitions finish and we send the MAX time,
/// but then get assigned a new unfinished partition due to a rescale.
/// So we broadcast partition info to only emit MAX time when all partitions globally are finished
#[derive(Serialize, Deserialize, Hash, PartialEq, Eq, Clone)]
struct PartitionFinished<Part>(Part);

struct StatefulSourcePartitionOp<V, T, Builder: StatefulSourceImpl<V, T>> {
    partitions: IndexMap<Builder::Part, Builder::SourcePartition>,
    part_builder: Builder,
    all_partitions: IndexMap<Builder::Part, bool>, // true if partition is finished
    comm_clients: IndexMap<WorkerId, BiCommunicationClient<PartitionFinished<Builder::Part>>>,
    // final marker, we keep it in an option to only send it once
    max_t: Option<T>,
    _phantom: PhantomData<(Builder::PartitionState, V)>,
}

impl<V, T, Builder> StatefulSourcePartitionOp<V, T, Builder>
where
    Builder: StatefulSourceImpl<V, T>,
    Builder::Part: Hash + Eq,
    T: Timestamp,
{
    fn new(
        ctx: &mut BuildContext,
        part_builder: Builder,
        all_partitions: IndexMap<Builder::Part, bool>,
    ) -> Self {
        let comm_clients =
            ctx.create_all_communication_clients::<PartitionFinished<Builder::Part>>();
        let mut this = Self {
            partitions: IndexMap::new(),
            part_builder,
            all_partitions,
            comm_clients,
            // This is technically state which gets lost on restarts, but sending T::MAX multiple
            // times should not be an issue
            max_t: Some(T::MAX),
            _phantom: PhantomData,
        };

        if let Some(state) = ctx.load_state::<IndexMap<Builder::Part, Builder::PartitionState>>() {
            for (k, v) in state.into_iter() {
                this.add_partition(k, Some(v));
            }
        }
        this
    }

    fn add_partition(&mut self, part: Builder::Part, part_state: Option<Builder::PartitionState>) {
        let partition = self.part_builder.build_part(&part, part_state);
        self.partitions.insert(part, partition);
    }
}

impl<VO, TO, Builder> LogicWrapper<Builder::Part, (), NoTime, VO, TO>
    for StatefulSourcePartitionOp<VO, TO, Builder>
where
    Builder: StatefulSourceImpl<VO, TO>,
    VO: Data,
    TO: Timestamp,
{
    fn on_schedule(
        &mut self,
        output: &mut Output<Builder::Part, VO, TO>,
        _ctx: &mut OperatorContext,
    ) {
        // TODO: All these iterations may be kinda inefficient
        for (part, partition) in self.partitions.iter_mut() {
            if let Some((data, time)) = partition.poll() {
                let out_msg = DataMessage::new(part.clone(), data, time);
                output.send(out_msg.into());
            }
            let part_state = self
                .all_partitions
                .get_mut(part)
                .expect("Expected partition state to exist");
            // Partition finished but not yet marked as finished
            if partition.is_finished() && !*part_state {
                *part_state = true;
                broadcast(self.comm_clients.values(), PartitionFinished(part.clone()));
            }
        }
        for msg in self.comm_clients.values().flat_map(|x| x.recv()) {
            *self
                .all_partitions
                .get_mut(&msg.0)
                .expect("Expected partition state to exist") = true;
        }
        if let Some(t) = self
            .max_t
            .take_if(|_| self.all_partitions.values().all(|x| *x))
        {
            output.send(Message::Epoch(t));
        }
    }

    fn on_data(
        &mut self,
        data_message: DataMessage<Builder::Part, (), NoTime>,
        _output: &mut Output<Builder::Part, VO, TO>,
        _ctx: &mut OperatorContext,
    ) {
        let part = data_message.key;
        if !self.partitions.contains_key(&part) {
            let partition = self.part_builder.build_part(&part, None);
            self.partitions.insert(part, partition);
        }
    }

    fn on_epoch(
        &mut self,
        _epoch: NoTime,
        _output: &mut Output<Builder::Part, VO, TO>,
        _ctx: &mut OperatorContext,
    ) {
    }

    fn on_barrier(
        &mut self,
        barrier: &mut Barrier,
        _output: &mut Output<Builder::Part, VO, TO>,
        ctx: &mut OperatorContext,
    ) {
        let state: IndexMap<Builder::Part, Builder::PartitionState> = self
            .partitions
            .iter()
            .map(|(k, v)| (k.clone(), v.snapshot()))
            .collect();
        barrier.persist(&state, &ctx.operator_id);
    }

    fn on_rescale(
        &mut self,
        rescale_message: &mut RescaleMessage,
        _output: &mut Output<Builder::Part, VO, TO>,
        ctx: &mut OperatorContext,
    ) {
        let new_workers = rescale_message.get_new_workers();
        self.comm_clients.retain(|wid, _| new_workers.contains(wid));
        for wid in new_workers.iter() {
            if !self.comm_clients.contains_key(wid) && !wid == ctx.worker_id {
                let client = ctx.create_communication_client(*wid);
                self.comm_clients.insert(*wid, client);
            }
        }
    }

    fn on_suspend(
        &mut self,
        _suspend_marker: &mut SuspendMarker,
        _output: &mut Output<Builder::Part, VO, TO>,
        _ctx: &mut OperatorContext,
    ) {
        for partition in self.partitions.values_mut() {
            partition.suspend();
        }
    }

    fn on_interrogate(
        &mut self,
        interrogate: &mut Interrogate<Builder::Part>,
        _output: &mut Output<Builder::Part, VO, TO>,
        _ctx: &mut OperatorContext,
    ) {
        let keys = self.partitions.keys();
        interrogate.add_keys(keys);
    }

    fn on_collect(
        &mut self,
        collect: &mut Collect<Builder::Part>,
        _output: &mut Output<Builder::Part, VO, TO>,
        ctx: &mut OperatorContext,
    ) {
        let key_state = self.partitions.swap_remove(&collect.key);
        if let Some(partition) = key_state {
            collect.add_state(ctx.operator_id, partition.collect());
        }
    }

    fn on_acquire(
        &mut self,
        acquire: &mut Acquire<Builder::Part>,
        _output: &mut Output<Builder::Part, VO, TO>,
        ctx: &mut OperatorContext,
    ) {
        let partition_state = acquire.take_state(&ctx.operator_id);
        if let Some((part, part_state)) = partition_state {
            self.add_partition(part, Some(part_state));
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

        fn poll(&mut self) -> Option<(i32, i32)> {
            if self.next > self.max {
                None
            } else {
                let out = (self.next, self.next);
                self.next += 1;
                Some(out)
            }
        }

        fn is_finished(&mut self) -> bool {
            // only terminate after we have made a snapshot
            self.next > self.max && *self.was_snapshotted.lock().unwrap()
        }

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
