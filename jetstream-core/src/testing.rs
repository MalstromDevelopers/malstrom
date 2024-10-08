use std::time::{Duration, Instant};
use std::{collections::HashMap, rc::Rc, sync::Mutex};

use crate::keyed::distributed::{Acquire, Collect, Interrogate};

use crate::runtime::communication::Distributable;
use crate::runtime::threaded::SingleThreadRuntime;
use crate::runtime::threaded::{InterThreadCommunication, Shared};
use crate::runtime::{CommunicationClient, RuntimeBuilder};
use crate::snapshot::Barrier;
use crate::stream::BuildableOperator;
use crate::types::MaybeTime;
use crate::types::{Key, RescaleMessage, ShutdownMarker};
use crate::{
    channels::selective_broadcast::{full_broadcast, link, Receiver, Sender},
    snapshot::{NoPersistence, PersistenceBackend, PersistenceClient},
    stream::{
        JetStreamBuilder,
        {AppendableOperator, BuildContext, Logic, OperatorBuilder, RunnableOperator},
    },
    types::{MaybeData, MaybeKey, Message, NoData, NoKey, NoTime, OperatorId, WorkerId},
};
use indexmap::{IndexMap, IndexSet};

mod communication;
mod vec_sink;
mod operator_tester;
// mod iterator_source;

pub use vec_sink::VecSink;
// pub use iterator_source::SingleIteratorSource;
pub use communication::{NoCommunication, NoCommunicationError};
pub use operator_tester::{OperatorTester, SentMessage};

/// Creates a JetStream worker with no persistence and
/// a JetStream stream, which does not produce any messages
pub fn get_test_stream() -> (
    RuntimeBuilder<SingleThreadRuntime, NoPersistence>,
    JetStreamBuilder<NoKey, NoData, NoTime>,
) {
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
        println!("CPB loading state for {operator_id}");
        self.capture.lock().unwrap().remove(operator_id)
    }

    fn persist(&mut self, state: &[u8], operator_id: &OperatorId) {
        self.capture
            .lock()
            .unwrap()
            .insert(*operator_id, state.into());
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
    R: Distributable + 'static
>(
    tester: &mut OperatorTester<KI, VI, TI, KO, VO, TO, R>
) {
    let msg = Message::AbsBarrier(Barrier::new(Box::<NoPersistence>::default()));
    tester.send_local(msg);
    tester.step();
    assert!(matches!(
        tester.recv_local().unwrap(),
        Message::AbsBarrier(_)
    ));

    let msg = Message::Acquire(Acquire::new(
        KI::default(),
        IndexMap::new(),
    ));
    tester.send_local(msg);
    tester.step();
    assert!(matches!(
        tester.recv_local().unwrap(),
        Message::Acquire(_)
    ));

    let msg = Message::Collect(Collect::new(KI::default()));
    tester.send_local(msg);
    tester.step();
    assert!(matches!(
        tester.recv_local().unwrap(),
        Message::Collect(_)
    ));

    let msg = Message::DropKey(KI::default());
    tester.send_local(msg);
    tester.step();
    assert!(matches!(
        tester.recv_local().unwrap(),
        Message::DropKey(_)
    ));

    let msg = Message::Interrogate(Interrogate::new(Rc::new(|_| false)));
    tester.send_local(msg);
    tester.step();
    assert!(matches!(
        tester.recv_local().unwrap(),
        Message::Interrogate(_)
    ));

    let msg = Message::Rescale(RescaleMessage::ScaleAddWorker(IndexSet::new()));
    tester.send_local(msg);
    tester.step();
    assert!(matches!(
        tester.recv_local().unwrap(),
        Message::Rescale(RescaleMessage::ScaleAddWorker(_))
    ));

    let msg = Message::Rescale(RescaleMessage::ScaleRemoveWorker(IndexSet::new()));
    tester.send_local(msg);
    tester.step();
    assert!(matches!(
        tester.recv_local().unwrap(),
        Message::Rescale(RescaleMessage::ScaleRemoveWorker(_))
    ));

    let msg = Message::ShutdownMarker(ShutdownMarker::default());
    tester.send_local(msg);
    tester.step();
    assert!(matches!(
        tester.recv_local().unwrap(),
        Message::ShutdownMarker(_)
    ));
}

#[cfg(test)]
mod tests {

    use itertools::Itertools;

    use crate::{
        snapshot::{deserialize_state, serialize_state},
        stream::OperatorContext,
        types::DataMessage,
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

}
