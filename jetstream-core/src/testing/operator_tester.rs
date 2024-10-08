use std::{collections::{HashMap, VecDeque}, marker::PhantomData, ops::Range, rc::Rc, sync::Mutex};

use crate::{channels::selective_broadcast::{full_broadcast, link, Receiver, Sender}, runtime::{communication::{CommunicationBackendError, Distributable, Transport, TransportError}, threaded::InterThreadCommunication, CommunicationBackend, CommunicationClient}, snapshot::NoPersistence, stream::{BuildContext, Logic, OperatorContext}};
use crate::types::*;

pub struct OperatorTester<KI, VI, TI, KO, VO, TO, R>{
    logic: Box<dyn Logic<KI, VI, TI, KO, VO, TO>>,
    input: Receiver<KI, VI, TI>,
    input_handle: Sender<KI, VI, TI>,

    output: Sender<KO, VO, TO>,
    output_handle: Receiver<KO, VO, TO>,
    comm_shim: FakeCommunication<R>,

    worker_id: WorkerId,
    operator_id: OperatorId
}


impl<KI, VI, TI, KO, VO, TO, R> OperatorTester<KI, VI, TI, KO, VO, TO, R> where 
KI: MaybeKey,
VI: MaybeData,
TI: MaybeTime,
KO: MaybeKey,
VO: MaybeData,
TO: MaybeTime,
R: Distributable + 'static
{
    /// Build this Test from an operator builder function
    pub fn built_by<M: Logic<KI, VI, TI, KO, VO, TO>>(
        logic_builder: impl FnOnce(&mut BuildContext) -> M + 'static,
        worker_id: WorkerId,
        operator_id: OperatorId,
        worker_ids: Range<usize>
    ) -> Self {
        assert!(worker_ids.contains(&worker_id));
        let mut input_handle = Sender::new_unlinked(full_broadcast);
        let mut input = Receiver::new_unlinked();
        link(&mut input_handle, &mut input);

        let mut output = Sender::new_unlinked(full_broadcast);
        let mut output_handle = Receiver::new_unlinked();
        link(&mut output, &mut output_handle);

        let mut comm_shim = FakeCommunication::default();
        let mut build_ctx = BuildContext::new(worker_id, operator_id, "test".to_owned(), Box::new(NoPersistence::default()), &mut comm_shim, worker_ids);
        let logic = Box::new(logic_builder(&mut build_ctx));

        Self { logic, input, input_handle, output, output_handle, comm_shim, worker_id, operator_id }
    }

    /// Send a message to the operators local input
    pub fn send_local(&mut self, msg: Message<KI, VI, TI>) -> () {
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
    pub fn step(&mut self) -> () {
        let mut op_ctx = OperatorContext::new(self.worker_id, self.operator_id, &mut self.comm_shim);
        (self.logic)(&mut self.input, &mut self.output, &mut op_ctx);
    }
}
/// This is a Fake communication backend we can use in unit tests to emulate cross-worker
/// Communication
pub struct FakeCommunication<R> {
    // these are the messages the operator under test sent
    sent_by_operator: Rc<Mutex<VecDeque<SentMessage<R>>>>,
    // these are the messages the operator under test is yet to receive
    sent_to_operator: Rc<Mutex<HashMap<ImpersonatedSender, VecDeque<R>>>>
}

impl <R> Default for FakeCommunication<R> {
    fn default() -> Self {
        Self { sent_by_operator: Default::default(), sent_to_operator: Default::default() }
    }
}

impl <R> FakeCommunication<R> {

    /// Send a message to the operator under test, pretending to be a given worker and operator
    /// 
    /// # Example
    /// ```
    /// let comm = FakeCommunication::<String>::default();
    /// // pretend operator `4` on worker `15` sends the message `"Hello World"`
    /// comm.send_to_operator("Hello World".to_owned(), 15, 4);
    /// ```
    pub fn send_to_operator(&self, msg: R, from_worker: WorkerId, from_operator: OperatorId) {
        let key = ImpersonatedSender {
            worker_id: from_worker,
            operator_id: from_operator
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
    pub worker_id: WorkerId,
    /// OperatorId this message was intended for
    pub operator_id: OperatorId,
    /// Message content
    pub msg: R
}

/// This is the sender we impersonate when we send a message to the operator under test
#[derive(Debug, Hash, PartialEq, Eq)]
struct ImpersonatedSender {
    worker_id: WorkerId,
    operator_id: OperatorId,
}

impl <R> CommunicationBackend for FakeCommunication<R> where R: Distributable + 'static {
    fn new_connection(
        &mut self,
        to_worker: WorkerId,
        to_operator: OperatorId,
        _from_operator: OperatorId,
    ) -> Result<Box<dyn Transport>, CommunicationBackendError> {
        let transport = FakeCommunicationTransport {
            sent_by_operator: Rc::clone(&self.sent_by_operator),
            sent_to_operator: Rc::clone(&self.sent_to_operator),
            impersonate: ImpersonatedSender { worker_id: to_worker, operator_id: to_operator }
        };
        Ok(Box::new(transport))
    }
}
struct FakeCommunicationTransport<R> {
    // these are the messages the operator under test sent
    sent_by_operator: Rc<Mutex<VecDeque<SentMessage<R>>>>,
    // these are the messages the operator under test is yet to receive
    sent_to_operator: Rc<Mutex<HashMap<ImpersonatedSender, VecDeque<R>>>>,
    impersonate: ImpersonatedSender
}
impl<R> Transport for FakeCommunicationTransport<R> where R: Distributable {
    fn send(&self, msg: Vec<u8>) -> Result<(), TransportError> {
        let decoded: R = CommunicationClient::decode(&msg);

        // since communication clients are bidirectional, the intended reciptient and the sender we are impersonating
        // are the same
        let wrapped = SentMessage { worker_id: self.impersonate.worker_id, operator_id: self.impersonate.operator_id, msg: decoded };
        self.sent_by_operator.lock().unwrap().push_back(wrapped);
        Ok(())
    }

    fn recv(&self) -> Result<Option<Vec<u8>>, TransportError> {
        let mut guard = self.sent_to_operator.lock().unwrap();
        let queue = guard.get_mut(&self.impersonate);
        match queue {
            Some(q) => Ok(q.pop_front().map(CommunicationClient::encode)),
            None => Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{rc::Rc, sync::Mutex};

    use crate::{runtime::{CommunicationBackend, CommunicationClient}, testing::operator_tester::SentMessage, types::Message};

    use super::{DataMessage, FakeCommunication, NoKey, OperatorTester};


    /// We should be able to send messages to our operator under test
    /// using our fake communication
    #[test]
    fn test_fake_comm_send_to_operator() {
        let mut fake_comm = FakeCommunication::<i32>::default();
        // this is the client the operator would have
        let client = fake_comm.new_connection(1, 0, 0).unwrap();
        
        // impersonate worker 1 and operator 0
        fake_comm.send_to_operator(42, 1, 0);
        let raw = client.recv().unwrap().unwrap();
        let msg: i32 = CommunicationClient::decode(&raw);
        assert_eq!(msg, 42);
        assert!(client.recv().unwrap().is_none())
    }



    /// We should be able to receive messages from our operator under test
    #[test]
    fn test_fake_comm_receive() {
        let mut fake_comm = FakeCommunication::<i32>::default();
        // this is the client the operator would have
        let client = fake_comm.new_connection(1, 0, 0).unwrap();
        client.send(CommunicationClient::encode(42)).unwrap();

        let msg = fake_comm.recv_from_operator().unwrap();
        assert!(matches!(msg, SentMessage{worker_id: 1, operator_id: 0, msg: 42}));
        assert!(fake_comm.recv_from_operator().is_none())
    }

    /// We should be able to send a message to the operators local input
    #[test]
    fn test_operator_test_send_local() {
        let capture = Rc::new(Mutex::new(Option::None));
        let capture_moved = Rc::clone(&capture);

        let mut tester: OperatorTester<NoKey, i32, i32, NoKey, i32, i32, ()> = OperatorTester::built_by(move |_| {
            move |input, _output, _ctx| {
                if let Some(x) = input.recv() {
                    let _ = capture_moved.lock().unwrap().insert(x);
                }
            }
        },
    0, 0, 0..1
    );
        tester.send_local(Message::Data(DataMessage::new(NoKey, 42, 111)));
        tester.step();
        let received = capture.lock().unwrap().take().unwrap();
        assert!(matches!(received, Message::Data(DataMessage{key: NoKey, value: 42, timestamp: 111})))
    }

    /// We should be able to receive a message from the operators local output
    #[test]
    fn test_operator_tester_receive_local() {
        let mut tester: OperatorTester<NoKey, i32, i32, NoKey, i32, i32, ()> = OperatorTester::built_by(move |_| {
            move |_input, output, _ctx| {
                output.send(Message::Data(DataMessage::new(NoKey, 12345, 0)));
            }
        }, 0, 0, 0..1);
        tester.step();
        let received = tester.recv_local().unwrap();
        assert!(matches!(received, Message::Data(DataMessage{key: NoKey, value: 12345, timestamp: 0})));
    }
}