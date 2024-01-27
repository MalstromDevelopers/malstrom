/// A snapshot controller that uses K8S configmaps as a synchronization mechanism
/// to time snapshots
use bincode::{Decode, Encode};
use indexmap::IndexMap;

use crate::frontier::Timestamp;
use postbox::Message;
use serde::{Deserialize, Serialize};

use crate::{
    channels::selective_broadcast::{self, Receiver, Sender},
    stream::{
        jetstream::{Data, JetStreamBuilder},
        operator::StandardOperator,
    },
};

use super::{barrier::BarrierData, PersistenceBackend};

use std::path::Path;

#[derive(Serialize, Deserialize)]
struct ConfigMap {
    snapshot_svc_port: u16,
    statefulset_name: String,
    replica_count: u32,
    snapshot_leader_uri: String,
}

fn get_configmap() -> anyhow::Result<ConfigMap> {
    let path_raw = std::env::var("JETSTREAM_SNAPSHOT_CONFIG")
        .unwrap_or("/etc/jetstream/snapshot/config.yaml".into());
    let config_path = Path::new(&path_raw);
    let content = std::fs::read_to_string(config_path)?;
    let config: ConfigMap = serde_yaml::from_str(&content)?;
    Ok(config)
}

// TODO: Move this configuration to some central place
fn get_worker_idx() -> anyhow::Result<u32> {
    // see: https://kubernetes.io/docs/reference/labels-annotations-taints/#apps-kubernetes.io-pod-index
    let idx: u32 = std::env::var("JETSTREAM_SNAPSHOT_POD_INDEX")?.parse()?;
    Ok(idx)
}

#[derive(Encode, Decode)]
pub enum ComsMessage {
    StartSnapshot(u64),
    LoadSnapshot(u64),
    CommitSnapshot(u32, u64),
}

#[derive(Clone)]
pub struct RegionHandle<P> {
    sender: Sender<(), P>,
}

#[derive(Default)]
struct ControllerState {
    commited_snapshots: IndexMap<u32, u64>,
}

pub fn start_snapshot_region<O: Data, P: PersistenceBackend>(
    mut timer: impl FnMut() -> bool + 'static,
) -> (StandardOperator<O, O, P>, RegionHandle<P>) {
    let config = get_configmap().expect("Failed to get snapshot config");
    let bincode_conf = bincode::config::standard();

    // figure out addresses of remote workers
    let worker_idx = get_worker_idx().unwrap();
    let sts_name = config.statefulset_name;
    let sts_cnt = config.replica_count;
    let peers: Vec<u32> = (0..sts_cnt).filter(|x| *x != worker_idx).collect();

    // channel from leafs of region to root
    let mut backchannel_tx = Sender::<(), P>::new_unlinked(selective_broadcast::full_broadcast);
    let mut backchannel_rx = Receiver::new_unlinked();
    selective_broadcast::link(&mut backchannel_tx, &mut backchannel_rx);

    let mut state: Option<ControllerState> = None;
    let mut snapshot_in_progress = false;

    let op = StandardOperator::new(move |input: &mut Receiver<O, P>, output, ctx| {
        ctx.frontier.advance_to(Timestamp::MAX);
        match input.recv() {
            Some(BarrierData::Barrier(_)) => {
                unimplemented!("Barriers must not cross persistance regions!")
            }
            Some(BarrierData::Load(_)) => {
                unimplemented!("Loads must not cross persistance regions!")
            }
            Some(BarrierData::Data(d)) => output.send(BarrierData::Data(d)),
            None => (),
        };

        if state.is_none() && worker_idx == 0 {
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
                ctx.communication.send(p, Message::new(ctx.operator_id, m));
            }
        }

        // PANIC: Can unwrap here because we loaded the state before
        let s = state.as_mut().unwrap();
        if !snapshot_in_progress && worker_idx == 0 && timer() {
            let snapshot_epoch = s.commited_snapshots.values().min().unwrap_or(&0) + 1;
            println!("Starting new snapshot at epoch {snapshot_epoch}");
            let mut backend = P::new_for_epoch(&snapshot_epoch);
            backend.persist(ctx.frontier.get_actual(), &s, ctx.operator_id);

            // instruct other workers to start snapshotting
            let msg =
                bincode::encode_to_vec(ComsMessage::StartSnapshot(snapshot_epoch), bincode_conf)
                    .unwrap();
            for (p, m) in peers.iter().zip(itertools::repeat_n(msg, peers.len())) {
                ctx.communication.send(p, Message::new(ctx.operator_id, m));
            }

            output.send(BarrierData::Barrier(backend));
            snapshot_in_progress = true;
        }

        if snapshot_in_progress {
            // since the channel will only yield the barrier once it has
            // received it on all inputs, we now here that we are done with
            // the snapshot
            snapshot_in_progress = match backchannel_rx.recv() {
                Some(BarrierData::Barrier(b)) => {
                    state
                        .as_mut()
                        .unwrap()
                        .commited_snapshots
                        .insert(worker_idx, b.get_epoch());
                    let epoch = b.get_epoch();
                    println!("Completed snapshot at {epoch}");
                    if worker_idx != 0 {
                        let msg = bincode::encode_to_vec(
                            ComsMessage::CommitSnapshot(worker_idx, epoch),
                            bincode_conf,
                        )
                        .unwrap();
                        ctx.communication
                            .send(&0, Message::new(ctx.operator_id, msg));
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

pub fn end_snapshot_region<O: Data, P: PersistenceBackend>(
    stream: JetStreamBuilder<O, P>,
    mut region_handle: RegionHandle<P>,
) -> JetStreamBuilder<O, P> {
    let op = StandardOperator::new(move |input: &mut Receiver<O, P>, output, ctx| {
        ctx.frontier.advance_to(Timestamp::MAX);
        match input.recv() {
            Some(BarrierData::Barrier(b)) => region_handle.sender.send(BarrierData::Barrier(b)),
            Some(BarrierData::Data(d)) => output.send(BarrierData::Data(d)),
            _ => (),
        };
    });
    stream.then(op)
}
