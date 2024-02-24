
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::{Data, Key, Message, WorkerId};
use crate::{frontier::Timestamp};

use crate::{
    channels::selective_broadcast::{Receiver},
    stream::{jetstream::JetStreamBuilder, operator::StandardOperator},
};

use super::SnapshotVersion;
use super::{PersistenceBackend};

#[derive(Serialize, Deserialize)]
pub enum ComsMessage {
    StartSnapshot(SnapshotVersion),
    LoadSnapshot(SnapshotVersion),
    CommitSnapshot(WorkerId, SnapshotVersion),
}

#[derive(Clone)]
pub struct RegionHandle<P> {
    sender: crossbeam::channel::Sender<P>,
}

#[derive(Default)]
struct ControllerState {
    commited_snapshots: IndexMap<WorkerId, SnapshotVersion>,
}

pub fn start_snapshot_region<K: Key, T: Data, P: PersistenceBackend>(
    mut timer: impl FnMut() -> bool + 'static,
) -> (StandardOperator<K, T, K, T, P>, RegionHandle<P>) {
    let _bincode_conf = bincode::config::standard();
    // channel from leafs of region to root
    let (backchannel_tx, backchannel_rx) = crossbeam::channel::unbounded::<P>();

    let mut state: Option<ControllerState> = None;
    let mut snapshot_in_progress = false;

    let op = StandardOperator::new(move |input: &mut Receiver<K, T, P>, output, ctx| {
        let _peers = ctx.communication.get_peers();
        ctx.frontier.advance_to(Timestamp::MAX);
        match input.recv() {
            Some(Message::AbsBarrier(_)) => {
                unimplemented!("Barriers must not cross persistance regions!")
            }
            Some(Message::Load(_)) => {
                unimplemented!("Loads must not cross persistance regions!")
            }
            Some(x) => output.send(x),
            None => ()
        };

        if state.is_none() && ctx.worker_id == 0 {
            println!("Loading latest state");
            // initial startup, send load command to all if leader
            let backend = P::new_latest();
            let latest: ControllerState = backend
                .load(ctx.operator_id)
                .map(|x| x.1)
                .unwrap_or_default();
            let last_commited = latest.commited_snapshots.values().min().unwrap_or(&0);
            // load last committed state
            let backend = P::new_for_epoch(last_commited);
            state = backend
                .load(ctx.operator_id)
                .map(|x| x.1)
                .unwrap_or_default();
            output.send(Message::Load(backend));

            // instruct other workers to load the state
            ctx.communication.broadcast(ComsMessage::LoadSnapshot(*last_commited));
        }

        // PANIC: Can unwrap here because we loaded the state before
        let s = state.as_mut().unwrap();
        if !snapshot_in_progress && ctx.worker_id == 0 && timer() {
            let snapshot_epoch = s.commited_snapshots.values().min().unwrap_or(&0) + 1;
            println!("Starting new snapshot at epoch {snapshot_epoch}");
            let mut backend = P::new_for_epoch(&snapshot_epoch);
            backend.persist(ctx.frontier.get_actual(), &s, ctx.operator_id);

            // instruct other workers to start snapshotting
            ctx.communication.broadcast(ComsMessage::StartSnapshot(snapshot_epoch));
            output.send(Message::AbsBarrier(backend));
            snapshot_in_progress = true;
        }

        if snapshot_in_progress {
            // since the channel will only yield the barrier once it has
            // received it on all inputs, we now here that we are done with
            // the snapshot
            snapshot_in_progress = match backchannel_rx.try_recv().ok() {
                Some(b) => {
                    state
                        .as_mut()
                        .unwrap()
                        .commited_snapshots
                        .insert(ctx.worker_id, b.get_epoch());
                    let epoch = b.get_epoch();
                    println!("Completed snapshot at {epoch}");
                    if ctx.worker_id != 0 {
                        ctx.communication
                            .send(&0, ComsMessage::CommitSnapshot(ctx.worker_id, epoch))
                            .unwrap();
                    }
                    false
                }
                _ => true,
            }
        }

        for msg in ctx.communication.recv_all() {
            match msg
            {
                Some(ComsMessage::StartSnapshot(i)) => {
                    let mut backend = P::new_for_epoch(&i);
                    if let Some(s) = state.as_ref() {
                        backend.persist(ctx.frontier.get_actual(), s, ctx.operator_id);
                    }
                    output.send(Message::AbsBarrier(backend));
                }
                Some(ComsMessage::LoadSnapshot(i)) => {
                    let backend = P::new_for_epoch(&i);
                    state = backend
                        .load(ctx.operator_id)
                        .map(|x| x.1)
                        .unwrap_or_default();
                }
                Some(ComsMessage::CommitSnapshot(name, epoch)) => {
                    // TODO handle this more elegantly than unwrap
                    state
                        .as_mut()
                        .unwrap()
                        .commited_snapshots
                        .insert(name, epoch);
                }
                None => (),
            }
        }

    });

    (
        op,
        RegionHandle {
            sender: backchannel_tx,
        },
    )
}

pub fn end_snapshot_region<K: Key, T: Data, P: PersistenceBackend>(
    stream: JetStreamBuilder<K, T, P>,
    region_handle: RegionHandle<P>,
) -> JetStreamBuilder<K, T, P> {
    let op = StandardOperator::new(move |input: &mut Receiver<K, T, P>, output, ctx| {
        ctx.frontier.advance_to(Timestamp::MAX);
        match input.recv() {
            Some(Message::AbsBarrier(b)) => region_handle.sender.send(b).unwrap(),
            Some(x) => output.send(x),
            None => (),
        };
    });
    stream.then(op)
}
