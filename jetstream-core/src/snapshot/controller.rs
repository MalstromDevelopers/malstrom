/// A snapshot controller that uses K8S configmaps as a synchronization mechanism
/// to time snapshots
use bincode::{Decode, Encode};
use indexmap::IndexMap;

use crate::Message;
use crate::{frontier::Timestamp, NoData, NoKey};

use crate::{
    channels::selective_broadcast::{self, Receiver, Sender},
    stream::{jetstream::JetStreamBuilder, operator::StandardOperator},
};

use super::{barrier::BarrierData, PersistenceBackend};

#[derive(Encode, Decode)]
pub enum ComsMessage {
    StartSnapshot(u64),
    LoadSnapshot(u64),
    CommitSnapshot(u32, u64),
}

#[derive(Clone)]
pub struct RegionHandle<P> {
    sender: crossbeam::channel::Sender<P>,
}

#[derive(Default)]
struct ControllerState {
    commited_snapshots: IndexMap<u32, u64>,
}

pub fn start_snapshot_region<K, T, P: PersistenceBackend>(
    mut timer: impl FnMut() -> bool + 'static,
) -> (StandardOperator<K, T, K, T, P>, RegionHandle<P>) {
    let bincode_conf = bincode::config::standard();
    // channel from leafs of region to root
    let (backchannel_tx, backchannel_rx) = crossbeam::channel::unbounded();

    let mut state: Option<ControllerState> = None;
    let mut snapshot_in_progress = false;

    let op = StandardOperator::new(move |input: &mut Receiver<K, T, P>, output, ctx| {
        let peers = ctx.communication.get_peers();
        ctx.frontier.advance_to(Timestamp::MAX);
        match input.recv() {
            Some(Message::AbsBarrier(_)) => {
                unimplemented!("Barriers must not cross persistance regions!")
            }
            Some(Message::Load(_)) => {
                unimplemented!("Loads must not cross persistance regions!")
            }
            x => output.send(x),
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
            output.send(BarrierData::Load(backend));

            // instruct other workers to load the state
            let msg =
                bincode::encode_to_vec(ComsMessage::LoadSnapshot(*last_commited), bincode_conf)
                    .unwrap();
            for (p, m) in peers.iter().zip(itertools::repeat_n(msg, peers.len())) {
                ctx.communication
                    .send(p, Message::new(ctx.operator_id, m))
                    .unwrap();
            }
        }

        // PANIC: Can unwrap here because we loaded the state before
        let s = state.as_mut().unwrap();
        if !snapshot_in_progress && ctx.worker_id == 0 && timer() {
            let snapshot_epoch = s.commited_snapshots.values().min().unwrap_or(&0) + 1;
            println!("Starting new snapshot at epoch {snapshot_epoch}");
            let mut backend = P::new_for_epoch(&snapshot_epoch);
            backend.persist(ctx.frontier.get_actual(), &s, ctx.operator_id);

            // instruct other workers to start snapshotting
            let msg =
                bincode::encode_to_vec(ComsMessage::StartSnapshot(snapshot_epoch), bincode_conf)
                    .unwrap();
            for (p, m) in peers.iter().zip(itertools::repeat_n(msg, peers.len())) {
                ctx.communication
                    .send(p, Message::new(ctx.operator_id, m))
                    .unwrap();
            }

            output.send(BarrierData::Barrier(backend));
            snapshot_in_progress = true;
        }

        if snapshot_in_progress {
            // since the channel will only yield the barrier once it has
            // received it on all inputs, we now here that we are done with
            // the snapshot
            snapshot_in_progress = match backchannel_rx.recv() {
                Some(b) => {
                    state
                        .as_mut()
                        .unwrap()
                        .commited_snapshots
                        .insert(ctx.worker_id, b.get_epoch());
                    let epoch = b.get_epoch();
                    println!("Completed snapshot at {epoch}");
                    if ctx.worker_id != 0 {
                        let msg = bincode::encode_to_vec(
                            ComsMessage::CommitSnapshot(ctx.worker_id, epoch),
                            bincode_conf,
                        )
                        .unwrap();
                        ctx.communication
                            .send(&0, Message::new(ctx.operator_id, msg))
                            .unwrap();
                    }
                    false
                }
                _ => true,
            }
        }

        match ctx
            .communication
            .recv()
            .ok()
            .flatten()
            .map(|x| bincode::decode_from_slice(&x.data, bincode_conf).unwrap().0)
        {
            Some(ComsMessage::StartSnapshot(i)) => {
                let mut backend = P::new_for_epoch(&i);
                if let Some(s) = state.as_ref() {
                    backend.persist(ctx.frontier.get_actual(), s, ctx.operator_id);
                }
                output.send(BarrierData::Barrier(backend));
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
    });

    (
        op,
        RegionHandle {
            sender: backchannel_tx,
        },
    )
}

pub fn end_snapshot_region<K, T, P: PersistenceBackend>(
    stream: JetStreamBuilder<K, T, P>,
    mut region_handle: RegionHandle<P>,
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
