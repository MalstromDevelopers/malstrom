use std::{
    collections::{HashMap, VecDeque},
    ops::Range,
    rc::Rc,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;

use crate::types::*;
use crate::{
    channels::operator_io::{full_broadcast, link, Input, Output},
    runtime::{
        communication::{
            BiStreamTransport, CommunicationBackendError, Distributable, TransportError,
        },
        BiCommunicationClient, OperatorOperatorComm,
    },
    snapshot::NoPersistence,
    stream::{BuildContext, Logic, OperatorContext},
};

pub struct OperatorTester<KI, VI, TI, KO, VO, TO, R> {
    logic: Box<dyn Logic<KI, VI, TI, KO, VO, TO>>,
    input: Input<KI, VI, TI>,
    input_handle: Output<KI, VI, TI>,

    output: Output<KO, VO, TO>,
    output_handle: Input<KO, VO, TO>,
    comm_shim: FakeCommunication<R>,

    worker_id: WorkerId,
    operator_id: OperatorId,
}

impl<KI, VI, TI, KO, VO, TO, R> OperatorTester<KI, VI, TI, KO, VO, TO, R>
where
    KI: MaybeKey,
    VI: MaybeData,
    TI: MaybeTime,
    KO: MaybeKey,
    VO: MaybeData,
    TO: MaybeTime,
    R: Distributable + Send + Sync + 'static,
{
    /// Build this Test from an operator builder function
    pub fn built_by<M: Logic<KI, VI, TI, KO, VO, TO>>(
        logic_builder: impl FnOnce(&mut BuildContext) -> M + 'static,
        worker_id: WorkerId,
        operator_id: OperatorId,
        worker_ids: Range<u64>,
    ) -> Self {
        assert!(worker_ids.contains(&worker_id));
        let mut input_handle = Output::new_unlinked(full_broadcast);
        let mut input = Input::new_unlinked();
        link(&mut input_handle, &mut input);

        let mut output = Output::new_unlinked(full_broadcast);
        let mut output_handle = Input::new_unlinked();
        link(&mut output, &mut output_handle);

        let mut comm_shim = FakeCommunication::default();
        let mut build_ctx = BuildContext::new(
            worker_id,
            operator_id,
            "test".to_owned(),
            Rc::new(NoPersistence),
            &mut comm_shim,
            worker_ids.collect(),
        );
        let logic = Box::new(logic_builder(&mut build_ctx));

        Self {
            logic,
            input,
            input_handle,
            output,
            output_handle,
            comm_shim,
            worker_id,
            operator_id,
        }
    }

    /// Send a message to the operators local input
    pub fn send_local(&mut self, msg: Message<KI, VI, TI>) {
        self.input_handle.send(msg);
    }

    /// Receive a message from this operators local output
    pub fn recv_local(&mut self) -> Option<Message<KO, VO, TO>> {
        self.output_handle.recv()
    }

    /// Get a fake commounication backend to emulate remote communication
    /// on this operator
    pub fn remote(&self) -> &FakeCommunication<R> {
        &self.comm_shim
    }

    /// Perform one execution step on the operator
    pub fn step(&mut self) {
        let mut op_ctx =
            OperatorContext::new(self.worker_id, self.operator_id, &mut self.comm_shim);
        (self.logic)(&mut self.input, &mut self.output, &mut op_ctx);
    }
}
/// This is a Fake communication backend we can use in unit tests to emulate cross-worker
/// Communication
pub struct FakeCommunication<R> {
    // these are the messages the operator under test sent
    sent_by_operator: Arc<Mutex<VecDeque<SentMessage<R>>>>,
    // these are the messages the operator under test is yet to receive
    sent_to_operator: Arc<Mutex<HashMap<ImpersonatedSender, VecDeque<R>>>>,
}

impl<R> Default for FakeCommunication<R> {
    fn default() -> Self {
        Self {
            sent_by_operator: Default::default(),
            sent_to_operator: Default::default(),
        }
    }
}

impl<R> FakeCommunication<R> {
    /// Send a message to the operator under test, pretending to be a given worker and operator
    ///
    /// # Example
    /// ```
    /// use malstrom::testing::FakeCommunication;
    /// let comm = FakeCommunication::<String>::default();
    /// // pretend operator `4` on worker `15` sends the message `"Hello World"`
    /// comm.send_to_operator("Hello World".to_owned(), 15, 4);
    /// ```
    pub fn send_to_operator(&self, msg: R, from_worker: WorkerId, from_operator: OperatorId) {
        let key = ImpersonatedSender {
            worker_id: from_worker,
            operator_id: from_operator,
        };
        let mut guard = self.sent_to_operator.lock().unwrap();
        guard.entry(key).or_default().push_back(msg);
    }

    /// Receive a message sent from the operator under test.
    /// None if there are no non-received messages from the operator
    pub fn recv_from_operator(&self) -> Option<SentMessage<R>> {
        self.sent_by_operator.lock().unwrap().pop_front()
    }
}

/// A Message the operator under test has sent
#[derive(Debug)]
pub struct SentMessage<R> {
    /// WorkerId this message was intended for
    pub to_worker: WorkerId,
    /// OperatorId this message was intended for
    pub to_operator: OperatorId,
    /// Message content
    pub msg: R,
}

/// This is the sender we impersonate when we send a message to the operator under test
#[derive(Debug, Hash, PartialEq, Eq)]
struct ImpersonatedSender {
    worker_id: WorkerId,
    operator_id: OperatorId,
}

impl<R> OperatorOperatorComm for FakeCommunication<R>
where
    R: Distributable + Send + 'static,
{
    fn operator_to_operator(
        &self,
        to_worker: WorkerId,
        operator: OperatorId,
    ) -> Result<Box<dyn BiStreamTransport>, CommunicationBackendError> {
        let transport = FakeCommunicationTransport {
            sent_by_operator: Arc::clone(&self.sent_by_operator),
            sent_to_operator: Arc::clone(&self.sent_to_operator),
            impersonate: ImpersonatedSender {
                worker_id: to_worker,
                operator_id: operator,
            },
        };
        Ok(Box::new(transport))
    }
}

struct FakeCommunicationTransport<R> {
    // these are the messages the operator under test sent
    sent_by_operator: Arc<Mutex<VecDeque<SentMessage<R>>>>,
    // these are the messages the operator under test is yet to receive
    sent_to_operator: Arc<Mutex<HashMap<ImpersonatedSender, VecDeque<R>>>>,
    impersonate: ImpersonatedSender,
}
#[async_trait]
impl<R> BiStreamTransport for FakeCommunicationTransport<R>
where
    R: Distributable + Send,
{
    fn send(&self, msg: Vec<u8>) -> Result<(), TransportError> {
        let decoded: R = BiCommunicationClient::decode(&msg);

        // since communication clients are bidirectional, the intended reciptient and the sender we are impersonating
        // are the same
        let wrapped = SentMessage {
            to_worker: self.impersonate.worker_id,
            to_operator: self.impersonate.operator_id,
            msg: decoded,
        };
        self.sent_by_operator.lock().unwrap().push_back(wrapped);
        Ok(())
    }

    fn recv(&self) -> Result<Option<Vec<u8>>, TransportError> {
        let mut guard = self.sent_to_operator.lock().unwrap();
        let queue = guard.get_mut(&self.impersonate);
        match queue {
            Some(q) => Ok(q.pop_front().map(BiCommunicationClient::encode)),
            None => Ok(None),
        }
    }

    async fn recv_async(&self) -> Result<Vec<u8>, TransportError> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::{rc::Rc, sync::Mutex};

    use crate::{
        runtime::{BiCommunicationClient, OperatorOperatorComm},
        testing::operator_tester::SentMessage,
        types::Message,
    };

    use super::{DataMessage, FakeCommunication, NoKey, OperatorTester};

    /// We should be able to send messages to our operator under test
    /// using our fake communication
    #[test]
    fn test_fake_comm_send_to_operator() {
        let fake_comm = FakeCommunication::<i32>::default();
        // this is the client the operator would have
        let client = fake_comm.operator_to_operator(1, 0).unwrap();

        // impersonate worker 1 and operator 0
        fake_comm.send_to_operator(42, 1, 0);
        let raw = client.recv().unwrap().unwrap();
        let msg: i32 = BiCommunicationClient::decode(&raw);
        assert_eq!(msg, 42);
        assert!(client.recv().unwrap().is_none())
    }

    /// We should be able to receive messages from our operator under test
    #[test]
    fn test_fake_comm_receive() {
        let fake_comm = FakeCommunication::<i32>::default();
        // this is the client the operator would have
        let client = fake_comm.operator_to_operator(1, 0).unwrap();
        client.send(BiCommunicationClient::encode(42)).unwrap();

        let msg = fake_comm.recv_from_operator().unwrap();
        assert!(matches!(
            msg,
            SentMessage {
                to_worker: 1,
                to_operator: 0,
                msg: 42
            }
        ));
        assert!(fake_comm.recv_from_operator().is_none())
    }

    /// We should be able to send a message to the operators local input
    #[test]
    fn test_operator_test_send_local() {
        let capture = Rc::new(Mutex::new(Option::None));
        let capture_moved = Rc::clone(&capture);

        let mut tester: OperatorTester<NoKey, i32, i32, NoKey, i32, i32, ()> =
            OperatorTester::built_by(
                move |_| {
                    move |input, _output, _ctx| {
                        if let Some(x) = input.recv() {
                            let _ = capture_moved.lock().unwrap().insert(x);
                        }
                    }
                },
                0,
                0,
                0..1,
            );
        tester.send_local(Message::Data(DataMessage::new(NoKey, 42, 111)));
        tester.step();
        let received = capture.lock().unwrap().take().unwrap();
        assert!(matches!(
            received,
            Message::Data(DataMessage {
                key: NoKey,
                value: 42,
                timestamp: 111
            })
        ))
    }

    /// We should be able to receive a message from the operators local output
    #[test]
    fn test_operator_tester_receive_local() {
        let mut tester: OperatorTester<NoKey, i32, i32, NoKey, i32, i32, ()> =
            OperatorTester::built_by(
                move |_| {
                    move |_input, output, _ctx| {
                        output.send(Message::Data(DataMessage::new(NoKey, 12345, 0)));
                    }
                },
                0,
                0,
                0..1,
            );
        tester.step();
        let received = tester.recv_local().unwrap();
        assert!(matches!(
            received,
            Message::Data(DataMessage {
                key: NoKey,
                value: 12345,
                timestamp: 0
            })
        ));
    }
}
