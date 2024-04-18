use std::{any::Any, collections::HashMap, ops::RangeBounds, rc::Rc, sync::Mutex};

/// Test utilities for JetStream
use itertools::Itertools;
use postbox::{CommunicationBackend, Postbox, RecvIterator};
use serde::{de::DeserializeOwned, Serialize};

use crate::stream::operator::BuildableOperator;
use crate::{
    channels::selective_broadcast::{full_broadcast, link, Receiver, Sender},
    config::Config,
    snapshot::{NoPersistence, PersistenceBackend, PersistenceBackendBuilder},
    stream::{
        jetstream::JetStreamBuilder,
        operator::{
            AppendableOperator, BuildContext, Logic, OperatorBuilder, OperatorContext,
            RunnableOperator, StandardOperator,
        },
    },
    time::{MaybeTime, NoTime},
    MaybeData, MaybeKey, Message, NoData, NoKey, OperatorId, Worker, WorkerId,
};

/// A Helper to write values into a shared vector and take them out
/// again.
/// This is mainly useful to extract values from a stream in unit tests.
/// This struct uses an Rc<Mutex<Vec<T>> internally, so it can be freely
/// cloned
#[derive(Clone)]
pub struct VecCollector<T> {
    inner: Rc<Mutex<Vec<T>>>,
}
impl<T> Default for VecCollector<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> VecCollector<T> {
    pub fn new() -> Self {
        VecCollector {
            inner: Rc::new(Mutex::new(Vec::new())),
        }
    }

    /// Put a value into this collector
    pub fn give(&self, value: T) {
        self.inner.lock().unwrap().push(value)
    }

    /// Take the given range out of this collector
    pub fn drain_vec<R: RangeBounds<usize>>(&self, range: R) -> Vec<T> {
        self.inner.lock().unwrap().drain(range).collect()
    }
    /// Returns the len of the contained vec
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }
}

impl<T> IntoIterator for VecCollector<T> {
    type Item = T;

    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.drain_vec(..).into_iter()
    }
}

/// Creates a JetStream worker with no persistence and
/// a JetStream stream, which does not produce any messages
pub fn get_test_stream() -> (Worker, JetStreamBuilder<NoKey, NoData, NoTime>) {
    let mut worker = Worker::new(NoPersistence::default(), || false);
    let stream = worker.new_stream();
    (worker, stream)
}

/// Creates configs to run N workers locally
/// The workers will use port 29091 + n with n being
/// in range 0..N
pub fn get_test_configs<const N: usize>() -> [Config; N] {
    let ports = (0..N).map(|i| 29091 + i).collect_vec();
    let addresses: Vec<tonic::transport::Uri> = ports
        .iter()
        .map(|x| format!("http://localhost:{x}").parse().unwrap())
        .collect_vec();

    (0..N)
        .map(|i| Config {
            worker_id: i,
            port: 29091 + u16::try_from(i).unwrap(),
            cluster_addresses: addresses.to_vec(),
        })
        .collect_vec()
        .try_into()
        .unwrap()
}

#[derive(Default, Clone, Debug)]
/// A backend which simply captures any state it is given into a shared
/// HashMap.
/// If you have a clone of this backend you can retrieve the state using
/// the corresponding operator_id
pub struct CapturingPersistenceBackend {
    capture: Rc<Mutex<HashMap<OperatorId, Vec<u8>>>>,
}
impl PersistenceBackendBuilder for CapturingPersistenceBackend {
    fn latest(&self, worker_id: crate::WorkerId) -> Box<dyn PersistenceBackend> {
        Box::new(self.clone())
    }

    fn for_version(
        &self,
        worker_id: crate::WorkerId,
        snapshot_epoch: &crate::snapshot::SnapshotVersion,
    ) -> Box<dyn PersistenceBackend> {
        Box::new(self.clone())
    }
}

impl PersistenceBackend for CapturingPersistenceBackend {
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
            .insert(operator_id.clone(), state.into());
    }
}

/// Test helper to simulate a two Worker cluster with this local
/// worker being index 0 and the other being index 1
pub struct OperatorTester<KI, VI, TI, KO, VO, TO> {
    op: RunnableOperator,
    // Use this to send messages into the distributor
    local_in: Sender<KI, VI, TI>,

    /// Use this to get local messages out of the distributor
    local_out: Receiver<KO, VO, TO>,

    local_postbox: Postbox<WorkerId, OperatorId>,
    // used to emulate another worker sending messages
    remote_postbox: Postbox<WorkerId, OperatorId>,
    // need to keep them around so they don't get dropped
    backends: (CommunicationBackend, CommunicationBackend),
    persistence_backend: Box<dyn PersistenceBackend>,
}

impl<KI, VI, TI, KO, VO, TO> OperatorTester<KI, VI, TI, KO, VO, TO>
where
    KI: MaybeKey,
    VI: MaybeData,
    TI: MaybeTime,
    KO: MaybeKey,
    VO: MaybeData,
    TO: MaybeTime,
{
    pub fn new_built_by<M: Logic<KI, VI, TI, KO, VO, TO>>(
        logic_builder: impl FnOnce(&BuildContext) -> M + 'static,
    ) -> Self {
        let op_id: usize = 42;
        let postbox_local_builder = postbox::BackendBuilder::new(
            0,
            "127.0.0.1:29091".parse().unwrap(),
            vec![(1, "http://127.0.0.1:29092".parse().unwrap())],
            vec![op_id],
            4096,
        );
        let postbox_local = postbox_local_builder.for_operator(op_id).unwrap();
        let persistence = CapturingPersistenceBackend::default();
        let buildcontext = BuildContext::new(0, op_id, "NO_LABEL".to_owned(), persistence.latest(0), postbox_local);
        let mut op = OperatorBuilder::built_by(logic_builder);
        let mut local_in = Sender::new_unlinked(full_broadcast);
        let mut local_out = Receiver::new_unlinked();
        link(&mut local_in, op.get_input_mut());
        link(op.get_output_mut(), &mut local_out);
        let op = Box::new(op).into_runnable(buildcontext);

        let postbox_remote_builder = postbox::BackendBuilder::new(
            1,
            "127.0.0.1:29092".parse().unwrap(),
            vec![(0, "http://127.0.0.1:29091".parse().unwrap())],
            vec![op_id],
            4096,
        );
        let local_postbox = postbox_local_builder.for_operator(op_id).unwrap();
        let postbox_remote = postbox_remote_builder.for_operator(op_id).unwrap();
        let backend_local_build =
            std::thread::spawn(move || postbox_local_builder.connect().unwrap());
        let backend_remote_build =
            std::thread::spawn(move || postbox_remote_builder.connect().unwrap());

        let backend_local = backend_local_build.join().unwrap();
        let backend_remote = backend_remote_build.join().unwrap();

        Self {
            op,
            local_in,
            local_out,
            local_postbox,
            remote_postbox: postbox_remote,
            backends: (backend_local, backend_remote),
            persistence_backend: persistence.latest(0),
        }
    }

    pub fn new_direct<M: Logic<KI, VI, TI, KO, VO, TO>>(logic: M) -> Self {
        Self::new_built_by(|_| Box::new(logic))
    }
}

impl<KI, VI, TI, KO, VO, TO> OperatorTester<KI, VI, TI, KO, VO, TO>
where
    KI: MaybeKey,
    VI: MaybeData,
    TI: MaybeTime,
    KO: MaybeKey,
    VO: MaybeData,
    TO: MaybeTime,
{
    /// Schedule the operator
    pub fn step(&mut self) -> () {
        self.op.step()
    }

    /// Give a message to the distributor as if it where coming from a local upstream
    pub fn send_from_local(&mut self, message: Message<KI, VI, TI>) {
        self.local_in.send(message)
    }

    /// Receive a message from the distributor local downstream
    pub fn receive_on_local(&mut self) -> Option<Message<KO, VO, TO>> {
        self.local_out.recv()
    }

    /// places a message into the operatorcontext postbox, as if it was sent by a remote
    pub fn send_from_remote<T: Serialize + DeserializeOwned>(&mut self, message: T) {
        self.remote_postbox.send_same(&0, message).unwrap();
    }

    /// receive all the messages for the remote worker
    pub fn receive_on_remote<T: Serialize + DeserializeOwned>(
        &mut self,
    ) -> RecvIterator<WorkerId, OperatorId, T> {
        self.remote_postbox.recv_all()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        snapshot::{deserialize_state, serialize_state},
        DataMessage,
    };

    use super::*;

    #[test]
    fn test_get_test_configs() {
        let [a, b, c] = get_test_configs();

        assert_eq!(a.worker_id, 0);
        assert_eq!(b.worker_id, 1);
        assert_eq!(c.worker_id, 2);

        assert_eq!(a.port, 29091);
        assert_eq!(b.port, 29092);
        assert_eq!(c.port, 29093);

        assert_eq!(
            a.get_peer_uris(),
            vec![
                (1, "http://localhost:29092".parse().unwrap()),
                (2, "http://localhost:29093".parse().unwrap())
            ]
        );
        assert_eq!(
            b.get_peer_uris(),
            vec![
                (0, "http://localhost:29091".parse().unwrap()),
                (2, "http://localhost:29093".parse().unwrap())
            ]
        );
        assert_eq!(
            c.get_peer_uris(),
            vec![
                (0, "http://localhost:29091".parse().unwrap()),
                (1, "http://localhost:29092".parse().unwrap())
            ]
        );
    }

    #[test]
    fn test_vec_collector() {
        let col = VecCollector::new();
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
        let mut tester = OperatorTester::new_direct(
            |input: &mut Receiver<NoKey, i32, NoTime>,
             output: &mut Sender<NoKey, i32, NoTime>,
             ctx: &mut OperatorContext| {
                if let Some(Message::Data(d)) = input.recv() {
                    let v = d.value * 2;
                    output.send(Message::Data(DataMessage::new(d.key, v, d.timestamp)));
                };
                for x in ctx.communication.recv_all() {
                    let value = x.data;
                    output.send(Message::Data(DataMessage::new(NoKey, value, NoTime)))
                }
            },
        );

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
