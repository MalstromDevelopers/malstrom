use std::{collections::HashMap, rc::Rc, sync::Mutex};

use crate::keyed::distributed::{Acquire, Collect, Interrogate};

use crate::runtime::communication::Distributable;
use crate::runtime::threaded::SingleThreadRuntimeFlavor;
use crate::runtime::{SingleThreadRuntime, StreamProvider, WorkerBuilder};
use crate::snapshot::Barrier;
use crate::types::{Key, SuspendMarker};
use crate::types::{MaybeTime, RescaleMessage};
use crate::{
    snapshot::{NoPersistence, PersistenceBackend, PersistenceClient},
    stream::JetStreamBuilder,
    types::{MaybeData, MaybeKey, Message, NoData, NoKey, NoTime, OperatorId, WorkerId},
};
use indexmap::{IndexMap, IndexSet};

mod communication;
mod operator_tester;
mod vec_sink;
// mod iterator_source;

pub use vec_sink::VecSink;
// pub use iterator_source::SingleIteratorSource;
pub use communication::{NoCommunication, NoCommunicationError};
pub use operator_tester::{FakeCommunication, OperatorTester, SentMessage};

/// Creates a JetStream worker with no persistence and
/// a JetStream stream, which does not produce any messages
pub fn get_test_rt<F>(stream: F) -> SingleThreadRuntime<NoPersistence, F>
where
    F: FnMut(&mut dyn StreamProvider) -> (),
{
    SingleThreadRuntime::builder()
        .persistence(NoPersistence::default())
        .build(stream)
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
    type Client = CapturingPersistenceBackend;

    fn last_commited(&self, _worker_id: WorkerId) -> Self::Client {
        self.clone()
    }

    fn for_version(
        &self,
        _worker_id: WorkerId,
        _snapshot_epoch: &crate::snapshot::SnapshotVersion,
    ) -> Self::Client {
        self.clone()
    }

    fn commit_version(&self, _snapshot_version: &crate::snapshot::SnapshotVersion) {
        // TODO
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

/// A test which panics if the given operator does not forward a system message from local upstream
pub fn test_forward_system_messages<
    KI: Key + Default,
    VI: MaybeData,
    TI: MaybeTime,
    KO: MaybeKey,
    VO: MaybeData,
    TO: MaybeTime,
    R: Distributable + Send + Sync + 'static,
>(
    tester: &mut OperatorTester<KI, VI, TI, KO, VO, TO, R>,
) {
    let msg = Message::AbsBarrier(Barrier::new(Box::<NoPersistence>::default()));
    tester.send_local(msg);
    tester.step();
    assert!(matches!(
        tester.recv_local().unwrap(),
        Message::AbsBarrier(_)
    ));

    let msg = Message::Acquire(Acquire::new(KI::default(), IndexMap::new()));
    tester.send_local(msg);
    tester.step();
    assert!(matches!(tester.recv_local().unwrap(), Message::Acquire(_)));

    let msg = Message::Collect(Collect::new(KI::default()));
    tester.send_local(msg);
    tester.step();
    assert!(matches!(tester.recv_local().unwrap(), Message::Collect(_)));

    let msg = Message::Interrogate(Interrogate::new(Rc::new(|_| false)));
    tester.send_local(msg);
    tester.step();
    assert!(matches!(
        tester.recv_local().unwrap(),
        Message::Interrogate(_)
    ));

    let msg = Message::Rescale(RescaleMessage::new(IndexSet::new()));
    tester.send_local(msg);
    tester.step();
    assert!(matches!(tester.recv_local().unwrap(), Message::Rescale(_)));

    let msg = Message::SuspendMarker(SuspendMarker::default());
    tester.send_local(msg);
    tester.step();
    assert!(matches!(
        tester.recv_local().unwrap(),
        Message::SuspendMarker(_)
    ));
}

#[cfg(test)]
mod tests {

    use itertools::Itertools;

    use crate::snapshot::{deserialize_state, serialize_state};

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
        let a = backend.last_commited(0);
        let mut b = backend.last_commited(0);

        let val = "hello world".to_string();
        let ser = serialize_state(&val);
        b.persist(&ser, &42);

        let deser: String = a.load(&42).map(deserialize_state).unwrap();
        assert_eq!(deser, val);
    }
}
