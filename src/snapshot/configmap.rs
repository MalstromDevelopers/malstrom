/// A snapshot controller that uses K8S configmaps as a synchronization mechanism
/// to time snapshots
use std::{any::Any, collections::{HashMap, BTreeMap}, error::Error};

use k8s_openapi::api::core::v1::{ConfigMap, Pod};
use kube::{
    api::PatchParams,
    runtime::{watcher, WatchStreamExt},
    Api, Client,
};
use tokio::runtime::Handle;
use tokio_stream::StreamExt;
use tracing::error;

use crate::{
    channels::selective_broadcast::{self, full_broadcast, Receiver, Sender},
    stream::{
        jetstream::{Data, JetStreamBuilder, NoData},
        operator::{pass_through_operator, StandardOperator},
    },
};

use super::{barrier::BarrierData, PersistenceBackend, SnapshotController};
struct ConfigMapController<P> {
    backend: P,
    // bit of a dance with functions to get around
    // heterogenous senders and receivers
    roots: Vec<Box<dyn SenderWrapper<P>>>,
    leafs: Vec<Box<dyn ReceiverWrapper<P>>>,
}

impl<P> ConfigMapController<P>
where
    P: PersistenceBackend,
{
    pub fn region_start<O: Data>(
        &mut self,
        stream: JetStreamBuilder<O, P>,
    ) -> JetStreamBuilder<O, P> {
        let mut root_sender: Sender<O, P> =
            Sender::new_unlinked(selective_broadcast::full_broadcast);
        // this operator will be our gate to inject loads and barriers
        let mut op = pass_through_operator();
        selective_broadcast::link(&mut root_sender, op.get_input_mut());
        self.roots.push(Box::new(root_sender));
        stream.then(op)
    }

    pub fn region_end<O: Data>(
        &mut self,
        mut stream: JetStreamBuilder<O, P>,
    ) -> JetStreamBuilder<O, P> {
        let mut leaf_receiver: Receiver<O, P> = Receiver::new_unlinked();
        selective_broadcast::link(stream.get_output_mut(), &mut leaf_receiver);
        self.leafs.push(Box::new(leaf_receiver));
        stream
    }
}

trait SenderWrapper<P> {
    fn send_load(&mut self, backend: P) -> ();
    fn send_barrier(&mut self, backend: P) -> ();
}

impl<T, P> SenderWrapper<P> for Sender<T, P>
where
    T: Clone,
    P: PersistenceBackend,
{
    fn send_load(&mut self, backend: P) -> () {
        self.send(BarrierData::Load(backend))
    }

    fn send_barrier(&mut self, backend: P) -> () {
        self.send(BarrierData::Barrier(backend))
    }
}

trait ReceiverWrapper<P> {
    fn receive_barrier(&mut self) -> Option<P>;
}

impl<T, P> ReceiverWrapper<P> for Receiver<T, P>
where
    T: Clone,
    P: PersistenceBackend,
{
    fn receive_barrier(&mut self) -> Option<P> {
        match self.recv() {
            Some(BarrierData::Barrier(p)) => Some(p),
            _ => None,
        }
    }
}

use crate::channels::watch;
use indexmap::{IndexMap, map::Values};
struct SnapshotClusterState {
    target_tx: watch::Sender<u64>,
    own_commit_tx: watch::Sender<u64>,
    commited: watch::Receiver<BTreeMap<String, u64>>,
    _rt: Handle,
}
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SnapshotError {
    #[error("Outstanding commits for in progress snapshot")]
    CommitMismatch,
    #[error("Cluster snapshot commit map is empty")]
    ClusterMapEmpty,
    #[error("Cluster snapshot commit map is corrupted: Expeted u64 found `{0}`")]
    ClusterMapCorrupt(String),
}

impl SnapshotClusterState {
    /// Try to start a new cluster-wide snapshot
    fn start_new_snapshot(&self, epoch: u64) -> Result<(), SnapshotError> {
        let committed = self.commited.read();
        let min_commit = committed
            .values()
            .min()
            .ok_or(SnapshotError::ClusterMapEmpty)?;
        if !committed.values().all(|x| x == min_commit) {
            // can not start new snapshot as not all workers
            // have comitted the previous one
            return Err(SnapshotError::CommitMismatch);
        }
        self.target_tx.send(epoch);
        Ok(())
    }

    // Get the snapshot epoch the cluster is currently targeting
    fn get_cluster_target(&self) -> u64 {
        *self
            .commited
            .read()
            .get("target")
            .expect("Snapshot configmap corrupted: Missing key `target`")
    }

    fn get_own_committed(&self) -> u64 {
        self.own_commit_tx.read()
    }

    fn commit(&self, epoch: u64) -> () {
        self.own_commit_tx.send(epoch)
    }
}


fn parse_commit_map(configmap: BTreeMap<String, String>) -> Result<IndexMap<String, u64>, SnapshotError> {
    let mut new = IndexMap::with_capacity(configmap.len());
    for (k, v) in configmap.into_iter() {
        let parsed: u64 = v.parse().map_err(|_| SnapshotError::ClusterMapCorrupt(v))?;
        new.insert(k, parsed);
    };
    Ok(new)
}

async fn watch_cfm(
    own_name: String,
    committed_send: watch::Sender<Result<IndexMap<String, u64>, SnapshotError>>,
) -> Result<(), watcher::Error> {
    let namespace = "default";
    let cm_name = "snapshot-sync-cm";
    let client = Client::try_default().await.unwrap();
    let configmaps: Api<ConfigMap> = Api::namespaced(client, "default");
    // configmaps.patch("bump-commit", PatchParams::default(), patch)

    for update in watcher(
        configmaps,
        watcher::Config::default().labels("jetstream.role=snapshot-sync-cm"),
    )
    .applied_objects().n
     {
        committed_send.send(parse_commit_map(update))
    }

    todo!()
}
