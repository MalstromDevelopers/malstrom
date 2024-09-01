use std::time::{Duration, Instant};
use std::{collections::HashMap, ops::RangeBounds, rc::Rc, sync::Mutex};

use crate::keyed::distributed::{Acquire, Collect, Interrogate};

use crate::runtime::communication::Distributable;
use crate::runtime::threaded::{InterThreadCommunication, Shared};
use crate::snapshot::Barrier;
use crate::stream::BuildableOperator;
use crate::runtime::{CommunicationBackend, CommunicationClient, RuntimeBuilder};
use crate::runtime::threaded::SingleThreadRuntime;
use crate::types::MaybeTime;
use crate::{
    channels::selective_broadcast::{full_broadcast, link, Receiver, Sender},
    snapshot::{NoPersistence, PersistenceClient, PersistenceBackend},
    stream::{
        JetStreamBuilder,
        {AppendableOperator, BuildContext, Logic, OperatorBuilder, RunnableOperator},
    },
    types::{NoTime, MaybeData, MaybeKey, Message, NoData, NoKey, OperatorId, WorkerId,}
};
use crate::types::{Key, RescaleMessage, ShutdownMarker};
use indexmap::{IndexMap, IndexSet};

use thiserror::Error;
mod communication;
mod vec_sink;
mod iterator_source;

pub use vec_sink::VecSink;
pub use iterator_source::SingleIteratorSource;
pub use communication::{NoCommunication, NoCommunicationError};

/// Creates a JetStream worker with no persistence and
/// a JetStream stream, which does not produce any messages
pub fn get_test_stream() -> (RuntimeBuilder<SingleThreadRuntime, NoPersistence>, JetStreamBuilder<NoKey, NoData, NoTime>) {
    let mut worker = RuntimeBuilder::new(SingleThreadRuntime);
    let stream = worker.new_stream();
    (worker, stream)
}

#[derive(Default, Clone, Debug)]
/// A backend which simply captures any state it is given into a shared
/// HashMap.
/// If you have a clone of this backend you can retrieve the state using
/// the corresponding operator_id
pub struct CapturingPersistenceBackend {
    capture: Rc<Mutex<HashMap<OperatorId, Vec<u8>>>>,
}
impl PersistenceBackend for CapturingPersistenceBackend {
    fn latest(&self, _worker_id: WorkerId) -> Box<dyn PersistenceClient> {
        Box::new(self.clone())
    }

    fn for_version(
        &self,
        _worker_id: WorkerId,
        _snapshot_epoch: &crate::snapshot::SnapshotVersion,
    ) -> Box<dyn PersistenceClient> {
        Box::new(self.clone())
    }
}

impl PersistenceClient for CapturingPersistenceBackend {
    fn get_version(&self) -> crate::snapshot::SnapshotVersion {
        0
    }

    fn load(&self, operator_id: &OperatorId) -> Option<Vec<u8>> {
        self.capture.lock().unwrap().remove(operator_id)
    }

    fn persist(&mut self, state: &[u8], operator_id: &OperatorId) {
        self.capture
            .lock()
            .unwrap()
            .insert(*operator_id, state.into());
    }
}

/// Test helper to simulate a two Worker cluster with this local
/// worker being index 0 and the other being index 1
pub struct OperatorTester<KI, VI, TI, KO, VO, TO, R> {
    op_id: usize,
    op: RunnableOperator,
    // Use this to send messages into the distributor
    local_in: Sender<KI, VI, TI>,

    /// Use this to get local messages out of the distributor
    local_out: Receiver<KO, VO, TO>,

    local_comm: InterThreadCommunication,
    // used to emulate another worker sending messages
    _remote_comm: InterThreadCommunication,
    remote_comm_client: CommunicationClient<R>,
}

impl<KI, VI, TI, KO, VO, TO, R> OperatorTester<KI, VI, TI, KO, VO, TO, R>
where
    KI: MaybeKey,
    VI: MaybeData,
    TI: MaybeTime,
    KO: MaybeKey,
    VO: MaybeData,
    TO: MaybeTime,
    R: Distributable,
{
    pub fn new_built_by<M: Logic<KI, VI, TI, KO, VO, TO>>(
        logic_builder: impl FnOnce(&mut BuildContext) -> M + 'static,
    ) -> Self {
        let op_id: usize = 42;

        
        let shared = Shared::default();
        let mut comm_0 = InterThreadCommunication::new(shared.clone(), 0);
        let mut comm_1 = InterThreadCommunication::new(shared, 1);

        let persistence = CapturingPersistenceBackend::default();
        let mut buildcontext = BuildContext::new(
            0,
            op_id,
            "NO_LABEL".to_owned(),
            persistence.latest(0),
            &mut comm_0,
            0..2,
        );
        let mut op = OperatorBuilder::built_by(logic_builder);
        let mut local_in = Sender::new_unlinked(full_broadcast);
        let mut local_out = Receiver::new_unlinked();
        link(&mut local_in, op.get_input_mut());
        link(op.get_output_mut(), &mut local_out);
        let op = Box::new(op).into_runnable(&mut buildcontext);

        let remote_recv = CommunicationClient::new(0, op_id, op_id, &mut comm_1).unwrap();
        Self {
            op_id,
            op,
            local_in,
            local_out,
            local_comm: comm_0,
            _remote_comm: comm_1,
            remote_comm_client: remote_recv,
        }
    }

    pub fn new_direct<M: Logic<KI, VI, TI, KO, VO, TO>>(logic: M) -> Self {
        Self::new_built_by(|_| Box::new(logic))
    }

    /// Returns the operators id
    pub fn operator_id(&self) -> OperatorId {
        self.op_id
    }
}

impl<KI, VI, TI, KO, VO, TO, R> OperatorTester<KI, VI, TI, KO, VO, TO, R>
where
    KI: MaybeKey,
    VI: MaybeData,
    TI: MaybeTime,
    KO: MaybeKey,
    VO: MaybeData,
    TO: MaybeTime,
    R: Distributable,
{
    /// Schedule the operator
    pub fn step(&mut self) {
        self.op.step(&mut self.local_comm)
    }

    /// Give a message to the distributor as if it where coming from a local upstream
    pub fn send_from_local(&mut self, message: Message<KI, VI, TI>) {
        self.local_in.send(message)
    }

    /// Receive a message from the distributor local downstream
    pub fn receive_on_local(&mut self) -> Option<Message<KO, VO, TO>> {
        self.local_out.recv()
    }

    pub fn receive_on_local_timeout(&mut self, timeout: Option<Duration>) -> R {
        let timeout = timeout.unwrap_or(Duration::from_secs(3));
        let start = Instant::now();
        loop {
            if let Some(msg) = self.remote_comm_client.recv() {
                return msg;
            }
            if Instant::now().duration_since(start) > timeout {
                panic!("Timeout receiving message on remote operator")
            }
        }
    }

    /// places a message into the operatorcontext postbox, as if it was sent by a remote
    pub fn send_from_remote(&mut self, message: R) {
        self.remote_comm_client.send(message);
    }

    /// receive all the messages for the remote worker
    pub fn receive_on_remote(&mut self, timeout: Option<Duration>) -> R {
        let timeout = timeout.unwrap_or(Duration::from_secs(3));
        let start = Instant::now();
        loop {
            if let Some(msg) = self.remote_comm_client.recv() {
                return msg;
            }
            if Instant::now().duration_since(start) > timeout {
                panic!("Timeout receiving message on remote operator")
            }
        }
    }
}

/// A test which panics if the given operator does not forward a system message from local upstream
pub fn test_forward_system_messages<
    KI: Key + Default,
    VI: MaybeData,
    TI: MaybeTime,
    KO: MaybeKey,
    VO: MaybeData,
    TO: MaybeTime,
    R: Distributable,
>(
    tester: &mut OperatorTester<KI, VI, TI, KO, VO, TO, R>,
) {
    let msg = Message::AbsBarrier(Barrier::new(Box::<NoPersistence>::default()));
    tester.send_from_local(msg);
    tester.step();
    assert!(matches!(
        tester.receive_on_local().unwrap(),
        Message::AbsBarrier(_)
    ));

    let msg = Message::Acquire(Acquire::new(
        KI::default(),
        Rc::new(Mutex::new(IndexMap::new())),
    ));
    tester.send_from_local(msg);
    tester.step();
    assert!(matches!(
        tester.receive_on_local().unwrap(),
        Message::Acquire(_)
    ));

    let msg = Message::Collect(Collect::new(KI::default()));
    tester.send_from_local(msg);
    tester.step();
    assert!(matches!(
        tester.receive_on_local().unwrap(),
        Message::Collect(_)
    ));

    let msg = Message::DropKey(KI::default());
    tester.send_from_local(msg);
    tester.step();
    assert!(matches!(
        tester.receive_on_local().unwrap(),
        Message::DropKey(_)
    ));

    let msg = Message::Interrogate(Interrogate::new(Rc::new(|_| false)));
    tester.send_from_local(msg);
    tester.step();
    assert!(matches!(
        tester.receive_on_local().unwrap(),
        Message::Interrogate(_)
    ));

    let msg = Message::Rescale(RescaleMessage::ScaleAddWorker(IndexSet::new()));
    tester.send_from_local(msg);
    tester.step();
    assert!(matches!(
        tester.receive_on_local().unwrap(),
        Message::Rescale(RescaleMessage::ScaleAddWorker(_))
    ));

    let msg = Message::Rescale(RescaleMessage::ScaleRemoveWorker(IndexSet::new()));
    tester.send_from_local(msg);
    tester.step();
    assert!(matches!(
        tester.receive_on_local().unwrap(),
        Message::Rescale(RescaleMessage::ScaleRemoveWorker(_))
    ));

    let msg = Message::ShutdownMarker(ShutdownMarker::default());
    tester.send_from_local(msg);
    tester.step();
    assert!(matches!(
        tester.receive_on_local().unwrap(),
        Message::ShutdownMarker(_)
    ));
}

#[cfg(test)]
mod tests {
    
    use itertools::Itertools;

    use crate::{
        snapshot::{deserialize_state, serialize_state}, stream::
        OperatorContext, types::DataMessage
    };

    use super::*;

    #[test]
    fn test_vec_collector() {
        let col = VecSink::new();
        let col_a = col.clone();

        for i in 0..5 {
            col.give(i)
        }

        // the cloned one should return these values
        let collected = col_a.drain_vec(..);
        assert_eq!(collected, (0..5).collect_vec())
    }

    #[test]
    fn capturing_persistence_backend() {
        let backend = CapturingPersistenceBackend::default();
        let a = backend.latest(0);
        let mut b = backend.latest(0);

        let val = "hello world".to_string();
        let ser = serialize_state(&val);
        b.persist(&ser, &42);

        let deser: String = a.load(&42).map(deserialize_state).unwrap();
        assert_eq!(deser, val);
    }

    #[test]
    fn test_operator_tester() {
        panic!("TODO: This test hangs forever");
        let mut tester = OperatorTester::new_built_by(|build_ctx| {
            let client = build_ctx.create_communication_client::<i32>(1, 0);
            move |input: &mut Receiver<NoKey, i32, NoTime>,
                  output: &mut Sender<NoKey, i32, NoTime>,
                  _ctx: &mut OperatorContext| {
                if let Some(Message::Data(d)) = input.recv() {
                    let v = d.value * 2;
                    output.send(Message::Data(DataMessage::new(d.key, v, d.timestamp)));
                };

                for value in client.recv_all() {
                    output.send(Message::Data(DataMessage::new(NoKey, value, NoTime)))
                }
            }
        });

        tester.send_from_local(Message::Data(DataMessage::new(NoKey, 42, NoTime)));
        tester.step();
        let out = tester.receive_on_local().unwrap();
        assert!(matches!(
            out,
            Message::Data(DataMessage {
                key: _,
                value: 84,
                timestamp: _
            })
        ));
        tester.send_from_remote(555i32);
        loop {
            tester.step();
            if let Some(x) = tester.receive_on_local() {
                assert!(matches!(
                    x,
                    Message::Data(DataMessage {
                        key: _,
                        value: 555,
                        timestamp: _
                    })
                ));
                break;
            }
        }
    }
}
