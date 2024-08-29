use std::net::{SocketAddr, SocketAddrV4, TcpListener};
use std::time::{Duration, Instant};
use std::{collections::HashMap, ops::RangeBounds, rc::Rc, sync::Mutex};

use crate::keyed::distributed::{Acquire, Collect, Distributable, Interrogate};
use crate::operators::{timely::InspectFrontier};
use crate::runtime::threaded::{InterThreadCommunication, Shared};
use crate::snapshot::Barrier;
use crate::stream::operator::BuildableOperator;
use crate::runtime::{CommunicationBackend, CommunicationClient, RuntimeBuilder};
use crate::runtime::threaded::SingleThreadRuntime;
use crate::{
    channels::selective_broadcast::{full_broadcast, link, Receiver, Sender},
    config::Config,
    snapshot::{NoPersistence, PersistenceClient, PersistenceBackend},
    stream::{
        jetstream::JetStreamBuilder,
        operator::{AppendableOperator, BuildContext, Logic, OperatorBuilder, RunnableOperator},
    },
    time::{NoTime, Timestamp},
    Data, MaybeData, MaybeKey, Message, NoData, NoKey, OperatorId, WorkerId,
};
use crate::{Key, RescaleMessage, ShutdownMarker};
use indexmap::{IndexMap, IndexSet};
/// Test utilities for JetStream
use itertools::Itertools;
use postbox::{Client, Postbox, PostboxBuilder};

use thiserror::Error;
use tonic::transport::Uri;

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

    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().is_empty()
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
pub fn get_test_stream() -> (RuntimeBuilder<SingleThreadRuntime, NoPersistence>, JetStreamBuilder<NoKey, NoData, NoTime>) {
    let mut worker = RuntimeBuilder::new(SingleThreadRuntime::default());
    let stream = worker.new_stream();
    (worker, stream)
}

fn get_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

/// Creates configs to run N workers locally
/// The workers will use port 29091 + n with n being
/// in range 0..N
pub fn get_test_configs<const N: usize>() -> [Config; N] {
    let ports = (0..N).map(|_| get_port()).collect_vec();
    let addresses: Vec<tonic::transport::Uri> = ports
        .iter()
        .map(|x| format!("http://localhost:{x}").parse().unwrap())
        .collect_vec();

    ports
        .iter()
        .enumerate()
        .map(|(i, p)| Config {
            worker_id: i,
            port: *p,
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
impl PersistenceBackend for CapturingPersistenceBackend {
    fn latest(&self, _worker_id: crate::WorkerId) -> Box<dyn PersistenceClient> {
        Box::new(self.clone())
    }

    fn for_version(
        &self,
        _worker_id: crate::WorkerId,
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

fn get_socket() -> SocketAddr {
    // find a free port
    let port = {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        port
    };
    SocketAddr::V4(SocketAddrV4::new("127.0.0.1".parse().unwrap(), port))
}

/// returns a pair of postboxes, that always connect to each other
fn pair_postbox() -> (
    Postbox<(WorkerId, OperatorId)>,
    Postbox<(WorkerId, OperatorId)>,
) {
    let sock_a = get_socket();
    let sock_b = get_socket();

    let uri_a = Uri::builder()
        .scheme("http")
        .authority(sock_a.to_string())
        .path_and_query("")
        .build()
        .unwrap();
    let uri_b = Uri::builder()
        .scheme("http")
        .authority(sock_b.to_string())
        .path_and_query("")
        .build()
        .unwrap();

    let pb_a = PostboxBuilder::new()
        .build(sock_a, move |_| Some(uri_b.clone()))
        .unwrap();
    let pb_b = PostboxBuilder::new()
        .build(sock_b, move |_| Some(uri_a.clone()))
        .unwrap();
    (pb_a, pb_b)
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
    remote_comm: InterThreadCommunication,
    remote_comm_client: CommunicationClient<R>,
    persistence_backend: Box<dyn PersistenceClient>,
}

impl<KI, VI, TI, KO, VO, TO, R> OperatorTester<KI, VI, TI, KO, VO, TO, R>
where
    KI: MaybeKey,
    VI: MaybeData,
    TI: Timestamp,
    KO: MaybeKey,
    VO: MaybeData,
    TO: Timestamp,
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
            remote_comm: comm_1,
            remote_comm_client: remote_recv,
            persistence_backend: persistence.latest(0),
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
    TI: Timestamp,
    KO: MaybeKey,
    VO: MaybeData,
    TO: Timestamp,
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
    TI: Timestamp,
    KO: MaybeKey,
    VO: MaybeData,
    TO: Timestamp,
    R: Distributable,
>(
    tester: &mut OperatorTester<KI, VI, TI, KO, VO, TO, R>,
) -> () {
    let msg = Message::AbsBarrier(Barrier::new(Box::new(NoPersistence::default())));
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

/// Runs a stream until the MAX timestamp is reached and returns all emitted messages
pub fn collect_stream_messages<K: MaybeKey, V: MaybeData, T: Timestamp>(
    stream: JetStreamBuilder<K, V, T>,
) -> Vec<Message<K, V, T>> {
    let worker = RuntimeBuilder::new(SingleThreadRuntime::default());

    let collector = VecCollector::new();
    let collector_cloned = collector.clone();
    let (stream, frontier) = stream
        .then(OperatorBuilder::direct(move |input, output, _| {
            if let Some(msg) = input.recv() {
                collector.give(msg.clone());
                output.send(msg)
            }
        }))
        .inspect_frontier();

    stream.finish();

    let [config] = get_test_configs();
    let mut runtime = worker.build().unwrap();

    while frontier.get_time().map_or(true, |x| x != T::MAX) {
        runtime.step()
    }
    collector_cloned.into_iter().collect()
}

/// Runs a stream until the MAX timestamp is reached and returns all emitted values
pub fn collect_stream_values<K: MaybeKey, V: Data, T: Timestamp>(
    stream: JetStreamBuilder<K, V, T>,
) -> Vec<V> {
    collect_stream_messages(stream)
        .into_iter()
        .filter_map(|msg| {
            if let Message::Data(d) = msg {
                Some(d.value)
            } else {
                None
            }
        })
        .collect()
}


/// A CommunicationBackend which will always return an error when trying to create a connection
/// This is only really useful for unit tests where you know the operator will not attempt
/// to make a connection or want to assert it does not
#[derive(Debug, Default)]
pub struct NoCommunication;
impl CommunicationBackend for NoCommunication {
    fn new_connection(
        &mut self,
        to_worker: WorkerId,
        to_operator: OperatorId,
        from_operator: OperatorId,
    ) -> Result<Box<dyn crate::runtime::communication::Transport>, crate::runtime::communication::CommunicationBackendError> {
        Err(crate::runtime::communication::CommunicationBackendError::ClientBuildError(Box::new(NoCommunicationError::CannotCreateClientError)))
    }
}
#[derive(Error, Debug)]
pub enum NoCommunicationError {
    #[error("NoCommunication backend cannot create clients")]
    CannotCreateClientError,
}

#[cfg(test)]
mod tests {
    use std::iter::once;

    use indexmap::IndexMap;

    use crate::{
        operators::{
            source::Source,
            timely::{GenerateEpochs, TimelyStream},
        }, snapshot::{deserialize_state, serialize_state}, sources::SingleIteratorSource, stream::operator::OperatorContext, DataMessage
    };

    use super::*;

    #[test]
    fn test_get_test_configs() {
        let [a, b, c] = get_test_configs();

        assert_eq!(a.worker_id, 0);
        assert_eq!(b.worker_id, 1);
        assert_eq!(c.worker_id, 2);

        let expected_uris = IndexMap::<usize, Uri>::from([
            (0, format!("http://localhost:{}", a.port).parse().unwrap()),
            (1, format!("http://localhost:{}", b.port).parse().unwrap()),
            (2, format!("http://localhost:{}", c.port).parse().unwrap()),
        ]);
        assert_eq!(a.get_cluster_uris(), expected_uris);
        assert_eq!(b.get_cluster_uris(), expected_uris);
        assert_eq!(c.get_cluster_uris(), expected_uris);
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

    #[test]
    fn test_get_stream_values() {
        let stream = JetStreamBuilder::new_test()
            .source(SingleIteratorSource::new(vec![1, 2, 3]))
            .assign_timestamps(|_| 0)
            .generate_epochs(|x, _| if x.value == 3 { Some(i32::MAX) } else { None })
            .0;
        let expected = vec![1, 2, 3];
        assert_eq!(collect_stream_values(stream), expected);
    }

    #[test]
    fn test_collect_stream_messages() {
        let stream = JetStreamBuilder::new_test()
            .source(SingleIteratorSource::new(0..3))
            .assign_timestamps(|x| x.value)
            .generate_epochs(|x, _| {
                if x.value == 2 {
                    Some(usize::MAX)
                } else {
                    Some(x.timestamp)
                }
            })
            .0;

        let expected = (0..3)
            .enumerate()
            .flat_map(|(i, x)| {
                [
                    Message::Data(DataMessage::new(NoKey, x, i)),
                    Message::Epoch(i),
                ]
            })
            .chain(once(Message::Epoch(usize::MAX)));
        for (s, e) in collect_stream_messages(stream).into_iter().zip_eq(expected) {
            match (s, e) {
                (Message::Data(a), Message::Data(b)) => {
                    assert_eq!(a.value, b.value);
                    assert_eq!(a.timestamp, b.timestamp);
                }
                (Message::Epoch(a), Message::Epoch(b)) => {
                    assert_eq!(a, b)
                }
                x => panic!("{x:?}"),
            }
        }
    }
}
