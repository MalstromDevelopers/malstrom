use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::{collections::HashMap, sync::Arc};

use crate::CONFIG;

use super::exchange::worker_service_server::{WorkerService, WorkerServiceServer};
use super::{
    client::GrpcTransport,
    exchange::{
        CoordinatorWorkerRequest, CoordinatorWorkerResponse, OperatorOperatorRequest,
        OperatorOperatorResponse,
    },
    util::{decode_id, new_channel},
};
use flume::{Receiver, Sender};
use futures::StreamExt;
use malstrom::{
    runtime::{
        communication::{BiStreamTransport, WorkerCoordinatorComm},
        OperatorOperatorComm,
    },
    types::{OperatorId, WorkerId},
};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tonic::transport::Server;
use tonic::{metadata::MetadataMap, Request, Response, Status, Streaming};

type InboundChannels =
    Arc<Mutex<HashMap<(WorkerId, OperatorId), (Sender<Vec<u8>>, Receiver<Vec<u8>>)>>>;
type OutboundChannels =
    Arc<Mutex<HashMap<(WorkerId, OperatorId), (Sender<Vec<u8>>, Receiver<Vec<u8>>)>>>;

pub struct WorkerGrpcBackend {
    pub(super) rt: tokio::runtime::Runtime,
    server_task: JoinHandle<Result<(), tonic::transport::Error>>,
    /// this worker
    pub(super) this_worker: WorkerId,
    /// channels on which we transport messages to our local operators
    pub(super) inbound_channels: InboundChannels,
    /// channels on which we send messages to other workers
    pub(super) outbound_channels: OutboundChannels,

    pub(super) coordinator_inbound: Arc<Mutex<(Sender<Vec<u8>>, Receiver<Vec<u8>>)>>,
    pub(super) coordinator_outbound: Arc<Mutex<(Sender<Vec<u8>>, Receiver<Vec<u8>>)>>,
}
pub(crate) struct WorkerGrpcServer {
    /// channels on which we transport messages to our local operators
    pub(super) inbound_channels: InboundChannels,
    pub(super) coordinator_inbound: Arc<Mutex<(Sender<Vec<u8>>, Receiver<Vec<u8>>)>>,
}

impl WorkerGrpcBackend {
    /// Create a new backend by creating a new tokio runtime and starting
    /// the GRPC Worker server
    pub(crate) fn new() -> Result<Self, std::io::Error> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let inbound_channels = Arc::new(Mutex::new(HashMap::new()));
        let coordinator_inbound = Arc::new(Mutex::new(new_channel()));

        let server = WorkerGrpcServer {
            inbound_channels: Arc::clone(&inbound_channels),
            coordinator_inbound: Arc::clone(&coordinator_inbound),
        };
        let server_future = Server::builder()
            .add_service(WorkerServiceServer::new(server))
            .serve(SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::UNSPECIFIED,
                CONFIG.network.port,
            )));
        let server_task = rt.spawn(server_future);
        Ok(WorkerGrpcBackend {
            rt,
            server_task,
            this_worker: CONFIG.get_worker_id(),
            inbound_channels,
            outbound_channels: Arc::new(Mutex::new(HashMap::new())),
            coordinator_inbound,
            coordinator_outbound: Arc::new(Mutex::new(new_channel())),
        })
    }
}

impl OperatorOperatorComm for WorkerGrpcBackend {
    fn operator_to_operator(
        &self,
        to_worker: WorkerId,
        operator: OperatorId,
    ) -> Result<
        Box<dyn BiStreamTransport>,
        malstrom::runtime::communication::CommunicationBackendError,
    > {
        Ok(Box::new(GrpcTransport::operator_to_operator(
            self, to_worker, operator,
        )))
    }
}

impl WorkerCoordinatorComm for WorkerGrpcBackend {
    fn worker_to_coordinator(
        &self,
    ) -> Result<
        Box<dyn BiStreamTransport>,
        malstrom::runtime::communication::CommunicationBackendError,
    > {
        Ok(Box::new(GrpcTransport::worker_coordinator(self)))
    }
}

#[tonic::async_trait]
impl WorkerService for WorkerGrpcServer {
    async fn operator_operator(
        &self,
        request: Request<Streaming<OperatorOperatorRequest>>,
    ) -> Result<Response<OperatorOperatorResponse>, Status> {
        let target = extract_metadata(request.metadata())?;

        let inbound = {
            self.inbound_channels
                .lock()
                .await
                .entry(target)
                .or_insert_with(new_channel)
                .0
                .clone()
        };
        let mut stream = request.into_inner();
        tokio::spawn(async move {
            while let Some(msg) = stream.next().await {
                let msg = msg.unwrap();
                inbound.send_async(msg.data).await;
            }
        });
        Ok(Response::new(OperatorOperatorResponse {}))
    }

    async fn coordinator_worker(
        &self,
        request: Request<Streaming<CoordinatorWorkerRequest>>,
    ) -> Result<Response<CoordinatorWorkerResponse>, Status> {
        let inbound = { self.coordinator_inbound.lock().await.0.clone() };
        let mut stream = request.into_inner();
        tokio::spawn(async move {
            while let Some(msg) = stream.next().await {
                let msg = msg.unwrap();
                inbound.send_async(msg.data).await;
            }
        });
        Ok(Response::new(CoordinatorWorkerResponse {}))
    }
}

/// Extract from_worker and operator from Metadata. Returning the appropriate status
/// if they are not present or invalid
fn extract_metadata(metadata: &MetadataMap) -> Result<(WorkerId, OperatorId), Status> {
    let operator = metadata
        .get_bin("operator_id")
        .ok_or(Status::invalid_argument("Missing operator_id metadata"))
        .map(|x| {
            x.to_bytes().map_err(|e| {
                Status::invalid_argument(format!("Invalid operator_id metadata: {e:?}"))
            })
        })?
        .map(decode_id)??;

    let remote_worker = metadata
        .get_bin("from_worker_id")
        .ok_or(Status::invalid_argument("Missing worker_id metadata"))
        .map(|x| {
            x.to_bytes()
                .map_err(|e| Status::invalid_argument(format!("Invalid worker_id metadata: {e:?}")))
        })?
        .map(decode_id)??;

    Ok((remote_worker, operator))
}
