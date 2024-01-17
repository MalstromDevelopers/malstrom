



use tokio::runtime::Handle;
use tonic::transport::Uri;
use tonic::{Request, Response, Status};


use tracing::info;

use crate::snapshot::controller::ComsMessage;

use self::api::snapshot_client::SnapshotClient as GrpcClient;
use self::api::snapshot_server::Snapshot;
use self::api::*;

/// Receiving end of snapshot communication

pub mod api {
    tonic::include_proto!("jetstream.snapshot");
}

pub struct SnapshotServer {
    output_tx: flume::Sender<ComsMessage>,
}

impl SnapshotServer {
    pub fn new(output_tx: flume::Sender<ComsMessage>) -> Self {
        Self { output_tx }
    }
}

#[tonic::async_trait]
impl Snapshot for SnapshotServer {
    async fn load_snapshot(
        &self,
        request: Request<LoadSnapshotRequest>,
    ) -> Result<Response<LoadSnapshotResponse>, Status> {
        let epoch = request.into_inner().snapshot_epoch;
        info!("Got request to load snapshot {epoch}");
        self.output_tx
            .send_async(ComsMessage::LoadSnapshot(epoch))
            .await
            .map_err(|_| Status::internal("Internal communication error"))?;
        Ok(Response::new(LoadSnapshotResponse {}))
    }
    async fn start_snapshot(
        &self,
        request: Request<StartSnapshotRequest>,
    ) -> Result<Response<StartSnapshotResponse>, Status> {
        let epoch = request.into_inner().snapshot_epoch;
        info!("Got request to load snapshot {epoch}");
        self.output_tx
            .send_async(ComsMessage::StartSnapshot(epoch))
            .await
            .map_err(|_| Status::internal("Internal communication error"))?;
        Ok(Response::new(StartSnapshotResponse {}))
    }
    async fn commit_snapshot(
        &self,
        request: Request<CommitSnapshotRequest>,
    ) -> Result<Response<CommitSnapshotResponse>, Status> {
        let req = request.into_inner();
        self.output_tx
            .send_async(ComsMessage::CommitSnapshot(
                req.from_name,
                req.snapshot_epoch,
            ))
            .await
            .map_err(|_| Status::internal("Internal communication error"))?;
        Ok(Response::new(CommitSnapshotResponse {}))
    }
}

use thiserror::Error;

#[derive(Error, Debug)]
pub enum SendError {
    #[error("Connection Error")]
    TransportError(#[from] tonic::transport::Error),
    #[error("Request returned non-ok status")]
    StatusError(#[from] tonic::Status),
    // You can add more variants as needed
}

pub struct SnapshotClient {
    _rt: Handle,
}

impl SnapshotClient {
    pub fn new(rt: Handle) -> Self {
        Self { _rt: rt }
    }

    /// Instruct all members of the cluster to load this epoch
    pub fn load_snapshot(&self, snapshot_epoch: u64, remotes: &[Uri]) -> Result<(), SendError> {
        for r in remotes.iter() {
            let result: Result<(), SendError> = self._rt.block_on(async {
                let mut client = GrpcClient::connect(r.clone())
                    .await
                    .map_err(SendError::from)?;
                client
                    .load_snapshot(LoadSnapshotRequest { snapshot_epoch })
                    .await
                    .map_err(SendError::from)?;
                Ok(())
            });
            result?
        }
        Ok(())
    }

    /// Instruct all members of the cluster to start a snapshot
    pub fn start_snapshot(&self, snapshot_epoch: u64, remotes: &[Uri]) -> Result<(), SendError> {
        for r in remotes.iter() {
            let result: Result<(), SendError> = self._rt.block_on(async {
                let mut client = GrpcClient::connect(r.clone())
                    .await
                    .map_err(SendError::from)?;
                client
                    .start_snapshot(StartSnapshotRequest { snapshot_epoch })
                    .await
                    .map_err(SendError::from)?;
                Ok(())
            });
            result?
        }
        Ok(())
    }

    /// Commit a snapshot to the leader
    pub fn commit_snapshot(
        &self,
        worker_idx: u32,
        snapshot_epoch: u64,
        leader_uri: &Uri,
    ) -> Result<(), SendError> {
        self._rt.block_on(async {
            let mut client = GrpcClient::connect(leader_uri.clone())
                .await
                .map_err(SendError::from)?;
            client
                .commit_snapshot(CommitSnapshotRequest {
                    from_name: worker_idx,
                    snapshot_epoch,
                })
                .await
                .map_err(SendError::from)?;
            Ok(())
        })
    }
}
