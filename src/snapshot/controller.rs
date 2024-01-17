
use std::net::{Ipv4Addr, SocketAddr};

/// A snapshot controller that uses K8S configmaps as a synchronization mechanism
/// to time snapshots
use std::{
    net::IpAddr,
};

use anyhow::Context;
use indexmap::IndexMap;

use serde::{Serialize, Deserialize};

use tokio_stream::StreamExt;
use tonic::transport::Uri;



use crate::frontier::{FrontierHandle, Timestamp};

use crate::{
    channels::selective_broadcast::{self, Receiver, Sender},
    stream::{
        jetstream::{Data, JetStreamBuilder},
        operator::{StandardOperator},
    },
};

use super::{barrier::BarrierData, server::SnapshotServer, PersistenceBackend};
use super::server::SnapshotClient;

use std::path::Path;

#[derive(Serialize, Deserialize)]
struct ConfigMap {
    listen_port: u16,
    statefulset_name: String,
    replica_count: u32
}

fn get_configmap() -> anyhow::Result<ConfigMap> {
    let path_raw = std::env::var("JETSTREAM_SNAPSHOT_CONFIG").unwrap_or("/etc/jetstream/snapshot/config.yaml".into());
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

pub enum ComsMessage {
    StartSnapshot(u64),
    LoadSnapshot(u64),
    CommitSnapshot(u32, u64),
}

#[derive(Clone)]
pub struct RegionHandle<P> {
    sender: Sender<(), P>
}

#[derive(Default)]
struct ControllerState {
    commited_snapshots: IndexMap<u32, u64>,
}

pub fn start_snapshot_region<O: Data, P: PersistenceBackend>(
    mut timer: impl FnMut() -> bool + 'static,
) -> (StandardOperator<O, O, P>, RegionHandle<P>) {
    let config = get_configmap().expect("Failed to get snapshot config");

    // start the server handling incoming requests
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Error creating tokio runtime");
    let (server_tx, server_rx) = flume::unbounded();

    let port = config.listen_port;
    let server = SnapshotServer::new(server_tx);
    let uri = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), port);
    let _server_task = runtime.spawn(async move {
        tonic::transport::Server::builder()
            .add_service(super::server::api::snapshot_server::SnapshotServer::new(
                server,
            ))
            .serve(uri)
            .await
            .context("GRPC Server terminated")
            .unwrap();
    });

    // figure out addresses of remote workers
    let worker_idx = get_worker_idx().unwrap();
    let sts_name = config.statefulset_name;
    let sts_cnt = config.replica_count;
    let remotes: Vec<Uri> = (0..sts_cnt)
        .filter(|i| *i != worker_idx)
        .map(|i| {
            Uri::builder()
                .scheme("http")
                .authority(format!("{sts_name}-{i}:{port}"))
                .build()
                .unwrap()
        })
        .collect();
    
    let grpc_client = SnapshotClient::new(runtime.handle().clone());
    // channel from leafs of region to root
    let mut backchannel_tx = Sender::<(), P>::new_unlinked(selective_broadcast::full_broadcast);
    let mut backchannel_rx = Receiver::new_unlinked();
    selective_broadcast::link(&mut backchannel_tx, &mut backchannel_rx);

    let mut state: Option<ControllerState> = None;
    let mut snapshot_in_progress = false;


    let op = StandardOperator::new(
        move |input: &mut Receiver<O, P>, output, frontier: &mut FrontierHandle, operator_id: usize| {
            frontier.advance_to(Timestamp::MAX);
            match input.recv() {
                Some(BarrierData::Barrier(_)) => unimplemented!("Barriers must not cross persistance regions!"),
                Some(BarrierData::Load(_)) => unimplemented!("Loads must not cross persistance regions!"),
                Some(BarrierData::Data(d)) => output.send(BarrierData::Data(d)),
                None => ()
            };
            // we need to reference these here, so they don't get dropped
            assert!(!_server_task.is_finished());
            runtime.enter();
            
            if state.is_none() && worker_idx == 0 {
                println!("Loading latest state");
                // initial startup, send load command to all if leader
                let backend = P::new_latest();
                let latest = backend.load(operator_id).map(|x| x.1).unwrap_or_else(ControllerState::default);
                let last_commited = latest.commited_snapshots.values().min().unwrap_or(&0);
                // load last committed state
                let backend = P::new_for_epoch(last_commited);
                state.insert(backend.load(operator_id).map(|x| x.1).unwrap_or_else(ControllerState::default));
                output.send(BarrierData::Load(backend));
                grpc_client.load_snapshot(*last_commited, &remotes).unwrap();
            }
            
            // PANIC: Can unwrap here because we loaded the state before
            let s = state.as_mut().unwrap();
            if !snapshot_in_progress && worker_idx == 0 && timer() {
                let snapshot_epoch = s.commited_snapshots.values().min().unwrap_or(&0) + 1;
                println!("Starting new snapshot at epoch {snapshot_epoch}");
                let mut backend = P::new_for_epoch(&snapshot_epoch);
                backend.persist(frontier.get_actual(), &s, operator_id);

                grpc_client.start_snapshot(snapshot_epoch, &remotes).unwrap();
                output.send(BarrierData::Barrier(backend));
                snapshot_in_progress = true;
            }

            if snapshot_in_progress {
                // since the channel will only yield the barrier once it has
                // received it on all inputs, we now here that we are done with
                // the snapshot
                snapshot_in_progress = match backchannel_rx.recv() {
                    Some(BarrierData::Barrier(b)) => {
                        state.as_mut().unwrap().commited_snapshots.insert(worker_idx, b.get_epoch());
                        let epoch = b.get_epoch();
                        println!("Completed snapshot at {epoch}");
                        false
                    },
                    _ => true
                }
            }

            match server_rx.try_recv().ok() {
                Some(ComsMessage::StartSnapshot(i)) => {
                    let mut backend = P::new_for_epoch(&i);
                    if let Some(s) = state.as_ref() {
                        backend.persist(frontier.get_actual(), s, operator_id);
                    }
                    output.send(BarrierData::Barrier(backend));
                }
                Some(ComsMessage::LoadSnapshot(i)) => {
                    let backend = P::new_for_epoch(&i);
                    state.insert(backend.load(operator_id).map(|x| x.1).unwrap_or_else(ControllerState::default));
                }
                Some(ComsMessage::CommitSnapshot(name, epoch)) => {
                    // TODO handle this more elegantly than unwrap
                    state.as_mut().unwrap().commited_snapshots.insert(name, epoch);
                }
                None => (),
            }
        });

    (op, RegionHandle{sender: backchannel_tx})
}

pub fn end_snapshot_region<O: Data, P: PersistenceBackend>(
    stream: JetStreamBuilder<O, P>,
    mut region_handle: RegionHandle<P>
) -> JetStreamBuilder<O, P> {
    let op = StandardOperator::new(
        move |input: &mut Receiver<O, P>, output, frontier: &mut FrontierHandle, _operator_id: usize| {
            frontier.advance_to(Timestamp::MAX);
            match input.recv() {
                Some(BarrierData::Barrier(b)) => region_handle.sender.send(BarrierData::Barrier(b)),
                Some(BarrierData::Data(d)) => output.send(BarrierData::Data(d)),
                _ => ()
            };
        }
    );
    stream.then(op)
}

