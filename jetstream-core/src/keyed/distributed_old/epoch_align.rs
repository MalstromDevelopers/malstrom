use indexmap::IndexMap;
use itertools::Itertools;

use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::keyed::types::DistTimestamp;
use crate::runtime::communication::broadcast;
use crate::runtime::CommunicationClient;
use crate::stream::Logic;
use crate::types::{ShutdownMarker, Timestamp};
use crate::{
    stream::BuildContext, types::{MaybeData, MaybeKey, Message, WorkerId,}
};

/// Builds an operator that aligns epochs with its remote counterparts
/// Aligning here means, that all downstream operators only get epochs
/// which are <= the smallest epoch of all aligner operators across all
/// workers
pub(crate) fn epoch_aligner<K: MaybeKey, V: MaybeData, T: DistTimestamp>(
    ctx: &mut BuildContext,
) -> impl Logic<K, V, T, K, V, T> {
    let mut epoch_states: IndexMap<WorkerId, Option<T>> = ctx
        .load_state()
        .unwrap_or_else(|| IndexMap::from_iter(ctx.get_worker_ids().map(|k| (k, None))));

    let mut local_shutdown: Option<ShutdownMarker> = None;

    let mut clients: IndexMap<WorkerId, CommunicationClient<ExchangedMessage<T>>> = ctx
        .get_worker_ids()
        .filter_map(|i| {
            if i != ctx.worker_id {
                Some((i, ctx.create_communication_client(i, ctx.operator_id)))
            } else {
                None
            }
        })
        .collect();

    move |input, output, ctx| {
        if let Some(msg) = input.recv() {
            match msg {
                Message::Epoch(e) => {
                    debug!(
                        "{}-{} Broadcasting epoch {:?} to {:?}",
                        ctx.worker_id,
                        ctx.operator_id,
                        e,
                        clients.keys().collect_vec()
                    );

                    broadcast(clients.values(), ExchangedMessage::Epoch(e.clone()));
                    // PANIC: We can unwrap, since our own workerid is guaranteed to exist
                    epoch_states.get_mut(&ctx.worker_id).unwrap().replace(e);
                    println!("{epoch_states:?}");

                    if let Some(m) = merge_timestamps(epoch_states.values()) {
                        output.send(Message::Epoch(m))
                    }
                }
                Message::ShutdownMarker(s) => {
                    broadcast(clients.values(), ExchangedMessage::<T>::Shutdown);
                    local_shutdown = Some(s)
                }
                Message::AbsBarrier(mut b) => {
                    b.persist(&epoch_states, &ctx.operator_id);
                    output.send(Message::AbsBarrier(b))
                }
                x => output.send(x),
            };
        }

        // Same pattern as in data_exchange
        // Check if we can somhow reconcile this
        let mut to_remove = Vec::new();
        for (sender, client) in clients.iter() {
            for network_msg in client.recv_all() {
                match network_msg {
                    ExchangedMessage::Epoch(e) => {
                        debug!(
                            "{}-{} Got epoch {:?} from {:?}",
                            ctx.worker_id, ctx.operator_id, e, sender
                        );
                        epoch_states.get_mut(sender).unwrap().replace(e);

                        if let Some(m) = merge_timestamps(epoch_states.values()) {
                            output.send(Message::Epoch(m))
                        }
                    }
                    ExchangedMessage::Shutdown => {
                        to_remove.push(*sender);
                        // if this remote was holding back the epoch, we want to advance it
                        let old_merged = merge_timestamps(epoch_states.values());
                        // epoch_states.swap_remove(&sender);
                        let new_merged = merge_timestamps(epoch_states.values());
                        if new_merged != old_merged {
                            if let Some(n) = new_merged {
                                output.send(Message::Epoch(n))
                            }
                        }
                    }
                }
            }
        }

        for k in to_remove.into_iter() {
            epoch_states.swap_remove(&k);
            clients.swap_remove(&k);
        }

        if local_shutdown.is_some() && clients.is_empty() {
            let wid = ctx.worker_id;
            debug!("epoch_align emitting shutdown on {wid}");
            output.send(Message::ShutdownMarker(local_shutdown.take().unwrap()));
        }
    }
}

/// Small reducer hack, as we can't use iter::reduce because of ownership
fn merge_timestamps<'a, T: Timestamp>(
    mut timestamps: impl Iterator<Item = &'a Option<T>>,
) -> Option<T> {
    let mut merged = timestamps.next()?.clone();
    for x in timestamps {
        if let Some(y) = x {
            merged = merged.map(|a| a.merge(y));
        } else {
            return None;
        }
    }
    merged
}

#[derive(Debug, Serialize, Deserialize, Clone)]
enum ExchangedMessage<T> {
    Epoch(T),
    Shutdown,
}

#[cfg(test)]
mod test {
    use std::{rc::Rc, sync::Mutex, time::Duration};

    use super::*;
    use crate::{keyed::distributed::{Acquire, Collect, Interrogate}, snapshot::{Barrier, NoPersistence, PersistenceClient}, testing::{CapturingPersistenceBackend, OperatorTester, SentMessage}, types::*};
    use indexmap::IndexSet;
    use serde::{de::DeserializeOwned, Serialize};

    /// An epoch coming in from local upstream should be broadcasted
    #[test]
    fn broadcast_epoch() {
        let mut tester = OperatorTester::built_by(epoch_aligner::<i32, String, u64>, 0, 0, 0..2);
        tester.send_local(Message::Epoch(42u64));
        tester.step();

        let remote_result = tester.remote().recv_from_operator().unwrap();
        assert!(matches!(remote_result, SentMessage { worker_id: 1, operator_id: 0, msg: ExchangedMessage::Epoch(42u64) }))
    }

    /// A shutdown marker coming in from local upstream should be broadcasted and sent downstream
    #[test]
    fn broadcast_shutdown() {
        let mut tester = OperatorTester::built_by(epoch_aligner::<i32, String, u64>, 0, 0, 0..2);
        tester.send_local(Message::ShutdownMarker(ShutdownMarker::default()));
        tester.step();

        // should not forward yet, since there is no consens from remote
        assert!(tester.recv_local().is_none());
        tester.remote().send_to_operator(ExchangedMessage::Shutdown, 1, 0);
        tester.step();

        // now it should be allowed to forward

        let local_result = tester.recv_local().unwrap();
        assert!(matches!(local_result, Message::ShutdownMarker(_)));
        let remote_result: SentMessage<ExchangedMessage<()>> = tester.remote().recv_from_operator().unwrap();
        assert!(matches!(remote_result, SentMessage { worker_id: 1, operator_id: 0, msg: ExchangedMessage::Shutdown }), "{remote_result:?}")
    }

    /// An epoch received from a local upstream should not trigger any output, when there is no state on the remote epoch
    #[test]
    fn align_epoch_from_local_none() {
        let mut tester: OperatorTester<i32, String, u64, i32, String, u64, ()> = OperatorTester::built_by(epoch_aligner::<i32, String, u64>, 0, 0, 0..1);
        tester.send_local(Message::Epoch(42));
        tester.step();
        assert!(tester.recv_local().is_none());
    }

    /// An epoch received from a remote should not trigger any output, when there is no state on the local epoch
    #[test]
    fn align_epoch_from_remote_none() {
        let mut tester = OperatorTester::built_by(epoch_aligner::<i32, String, u64>, 0, 0, 0..1);
        tester.remote().send_to_operator(ExchangedMessage::Epoch(42), 1, 0);
        tester.step();
        assert!(tester.recv_local().is_none());
    }

    /// An epoch received from a local upstream should trigger a merged epoch output when there is state
    /// for the remote
    #[test]
    fn align_epoch_from_local() {
        let mut tester = OperatorTester::built_by(epoch_aligner::<i32, String, u64>, 0, 0, 0..1);
        tester.remote().send_to_operator(ExchangedMessage::Epoch(7u64), 1, 0);
        tester.send_local(Message::Epoch(42u64));
        tester.step();
        let local_result = tester.recv_local().unwrap();
        assert!(
            matches!(local_result, Message::Epoch(7u64)),
            "{local_result:?}"
        );
    }

    /// An epoch received from a remote should trigger a merged epoch output when there is state
    /// for the local epoch
    #[test]
    fn align_epoch_from_remote() {
        let mut tester = OperatorTester::built_by(epoch_aligner::<i32, String, u64>, 0, 0, 0..1);
        tester.send_local(Message::Epoch(15u64));
        tester.remote().send_to_operator(ExchangedMessage::Epoch(177u64), 1, 0);
        tester.step();
        let local_result = tester.recv_local().unwrap();
        assert!(
            matches!(local_result, Message::Epoch(15u64)),
            "{local_result:?}"
        );
    }

    /// If we receive a shutdownmarker from a remote and that remote was previously holding back the
    /// advancement of the epoch, the epoch should advance after the remote has shut down
    #[test]
    fn advance_epoch_after_remote_shutdown() {
        let mut tester = OperatorTester::built_by(epoch_aligner::<i32, String, u64>, 0, 0, 0..1);
        tester.send_local(Message::Epoch(15u64));
        tester.remote().send_to_operator(ExchangedMessage::Epoch(10u64), 1, 0);
        tester.step();
        let low = tester.recv_local().unwrap();

        assert!(matches!(low, Message::Epoch(10)));
        tester.remote().send_to_operator(ExchangedMessage::<u64>::Shutdown, 1, 0);
        tester.step();

        let advanced = tester.recv_local().unwrap();

        assert!(matches!(advanced, Message::Epoch(15)));
    }

    /// If we receive a shutdownmarker from a remote and that remote was NOT previously holding back the
    /// advancement of the epoch nothing should happen
    #[test]
    fn no_epoch_after_remote_shutdown() {
        let mut tester = OperatorTester::built_by(epoch_aligner::<i32, String, u64>, 0, 0, 0..1);
        tester.send_local(Message::Epoch(10u64));
        tester.remote().send_to_operator(ExchangedMessage::Epoch(15u64), 1, 0);
        tester.step();
        let low = tester.recv_local().unwrap();

        assert!(matches!(low, Message::Epoch(10)));
        tester.remote().send_to_operator(ExchangedMessage::<u64>::Shutdown, 1, 0);
        tester.step();


        assert!(tester.recv_local().is_none())
    }

    /// These message types should simply pass the operator
    #[test]
    fn pass_throug_operator() {
        let messages = vec![
            Message::Data(DataMessage::new(1, "Hello".to_string(), 2)),
            Message::AbsBarrier(Barrier::new(Box::new(NoPersistence::default()))),
            Message::Rescale(RescaleMessage::ScaleAddWorker(IndexSet::new())),
            Message::Rescale(RescaleMessage::ScaleRemoveWorker(IndexSet::new())),
            Message::Interrogate(Interrogate::new(Rc::new(|_| false))),
            Message::Collect(Collect::new(1)),
            Message::Acquire(Acquire::new(1, Rc::new(Mutex::new(IndexMap::new())))),
            Message::DropKey(1),
        ];
        let mut tester: OperatorTester<i32, String, u64, i32, String, u64, ()> = OperatorTester::built_by(epoch_aligner::<i32, String, u64>, 0, 0, 0..1);
        for m in messages.into_iter() {
            tester.send_local(m.clone());
            tester.step();
            assert!(tester.recv_local().is_some());
        }
    }

    /// State should be snapshotted
    #[test]
    fn snapshots_state() {
        let persistence = CapturingPersistenceBackend::default();
        let mut tester: OperatorTester<i32, String, u64, i32, String, u64, ()> = OperatorTester::built_by(epoch_aligner::<i32, String, u64>, 0, 42, 0..1);
        tester.send_local(Message::AbsBarrier(Barrier::new(Box::new(persistence.clone()))));
        tester.step();
        persistence.load(&42).unwrap();
    }

    /// Operator should resume from state if present
    #[test]
    fn resumes_from_snapshot() {
        let persistence = CapturingPersistenceBackend::default();
        let mut barrier = Barrier::new(Box::new(persistence.clone()));
        // persisiting worker 1 was last observed at epoch 55
        barrier.persist(&IndexMap::<WorkerId, Option<u64>>::from([(1, Some(555))]), &42);
        let mut tester: OperatorTester<i32, String, u64, i32, String, u64, ()> = OperatorTester::built_by(epoch_aligner::<i32, String, u64>, 0, 42, 0..2);
        
        // this should be merged down to 555
        tester.send_local(Message::Epoch(1_000));
        tester.step();
        let out = tester.recv_local().unwrap();
        assert!(matches!(out, Message::Epoch(555)), "{out:?}")
    }
}
