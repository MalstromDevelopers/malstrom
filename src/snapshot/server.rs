use std::sync::Arc;

use nng::options::protocol::reqrep;
use tokio::runtime::Handle;
use tonic::transport::Uri;
use tonic::{Request, Response, Status};

use tokio::sync::{watch, RwLock};
use tracing::info;

use self::api::snapshot_server::Snapshot;
use self::api::snapshot_client::SnapshotClient;
use self::api::*;

/// Receiving end of snapshot communication

mod api {
    tonic::include_proto!("jetstream.snapshot");
}

enum ComsMessage {
    StartSnapshot(u64),
    LoadSnapshot(u64),
    CommitSnapshot(String, u64),
}

struct SnapshotServer {
    output_tx: flume::Sender<ComsMessage>,
}

impl 

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
