use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::Duration;
use std::{collections::HashMap, sync::Arc};

use crate::CONFIG;

use super::discovery::{lookup_coordinator_addr, lookup_worker_addr};
use super::exchange::worker_service_server::{WorkerService, WorkerServiceServer};
use super::{
    exchange::{
        CoordinatorWorkerRequest, CoordinatorWorkerResponse, OperatorOperatorRequest,
        OperatorOperatorResponse,
    },
    transport::GrpcTransport,
    util::{decode_id, new_channel},
};
use flume::{Receiver, Sender};
use futures::{Stream, StreamExt};
use malstrom::{
    runtime::{
        communication::{BiStreamTransport, WorkerCoordinatorComm},
        OperatorOperatorComm,
    },
    types::{OperatorId, WorkerId},
};
use std::sync::Mutex;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::server::Connected;
use tonic::transport::Server;
use tonic::{metadata::MetadataMap, Request, Response, Status, Streaming};
use tracing::{debug, info};

type InboundChannels =
    Arc<Mutex<HashMap<(WorkerId, OperatorId), (Sender<Vec<u8>>, Receiver<Vec<u8>>)>>>;
type OutboundChannels =
    Arc<Mutex<HashMap<(WorkerId, OperatorId), (Sender<Vec<u8>>, Receiver<Vec<u8>>)>>>;

type CoordinatorInbound = Arc<Mutex<(Sender<Vec<u8>>, Receiver<Vec<u8>>)>>;
type CoordinatorOutbound = Arc<Mutex<(Sender<Vec<u8>>, Receiver<Vec<u8>>)>>;

pub struct WorkerGrpcBackend {
    pub(super) rt: tokio::runtime::Handle,
    _server_task: JoinHandle<Result<(), tonic::transport::Error>>,
    /// this worker
    pub(super) this_worker: WorkerId,
    /// channels on which we transport messages to our local operators
    pub(super) inbound_channels: InboundChannels,
    /// channels on which we send messages to other workers
    pub(super) outbound_channels: OutboundChannels,

    pub(super) coordinator_inbound: CoordinatorInbound,
    pub(super) coordinator_outbound: CoordinatorOutbound,
}
pub(crate) struct WorkerGrpcServer {
    /// channels on which we transport messages to our local operators
    pub(super) inbound_channels: InboundChannels,
    pub(super) coordinator_inbound: CoordinatorInbound,
}

impl WorkerGrpcBackend {
    /// Create a new backend by creating a new tokio runtime and starting
    /// the GRPC Worker server
    pub(crate) fn new(rt: tokio::runtime::Handle) -> Result<Self, std::io::Error> {
        let socket = SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::UNSPECIFIED,
            CONFIG.network.port,
        ));
        let incoming = rt.block_on(TcpListener::bind(socket))?;
        let incoming = TcpListenerStream::new(incoming);
        Self::new_with_incoming(rt, incoming)
    }

    pub(crate) fn new_with_incoming<I, IO, IE>(
        rt: Handle,
        incoming: I,
    ) -> Result<Self, std::io::Error>
    where
        // these are tonic's bounds, not mine
        I: Stream<Item = Result<IO, IE>> + std::marker::Send + 'static,
        IO: AsyncRead + AsyncWrite + Connected + Unpin + Send + 'static,
        IO::ConnectInfo: Clone + Send + Sync + 'static,
        IE: Into<Box<dyn std::error::Error + Send + Sync>> + std::marker::Send + 'static,
    {
        let inbound_channels = Arc::new(Mutex::new(HashMap::new()));
        let coordinator_inbound = Arc::new(Mutex::new(new_channel()));

        let server = WorkerGrpcServer {
            inbound_channels: Arc::clone(&inbound_channels),
            coordinator_inbound: Arc::clone(&coordinator_inbound),
        };
        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        let server_future = Server::builder()
            .add_service(WorkerServiceServer::new(server))
            .add_service(health_service)
            .serve_with_incoming(incoming);
        let server_task = rt.spawn(server_future);
        rt.spawn(async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            health_reporter
                .set_serving::<WorkerServiceServer<WorkerGrpcServer>>()
                .await;
        });
        Ok(WorkerGrpcBackend {
            rt,
            _server_task: server_task,
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
        let endpoint = lookup_worker_addr(to_worker);
        Ok(Box::new(GrpcTransport::operator_to_operator(
            self, to_worker, endpoint, operator,
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
        let endpoint = lookup_coordinator_addr();
        debug!("Found coordinator at {:?}", endpoint.uri());
        Ok(Box::new(GrpcTransport::worker_coordinator(self, endpoint)))
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
                .unwrap()
                .entry(target)
                .or_insert_with(new_channel)
                .0
                .clone()
        };
        let mut stream = request.into_inner();
        tokio::spawn(async move {
            while let Some(msg) = stream.next().await {
                let msg = msg.unwrap();
                inbound.send_async(msg.data).await.unwrap();
            }
        });
        Ok(Response::new(OperatorOperatorResponse {}))
    }

    async fn coordinator_worker(
        &self,
        request: Request<Streaming<CoordinatorWorkerRequest>>,
    ) -> Result<Response<CoordinatorWorkerResponse>, Status> {
        let inbound = { self.coordinator_inbound.lock().unwrap().0.clone() };
        info!("Accepted connection from coordinator");

        let mut stream = request.into_inner();
        tokio::spawn(async move {
            while let Some(msg) = stream.next().await {
                let msg = msg.unwrap();
                inbound.send_async(msg.data).await.unwrap();
            }
        });
        Ok(Response::new(CoordinatorWorkerResponse {}))
    }
}

/// Extract from_worker and operator from Metadata. Returning the appropriate status
/// if they are not present or invalid
fn extract_metadata(metadata: &MetadataMap) -> Result<(WorkerId, OperatorId), Status> {
    let operator = metadata
        .get("operator-id")
        .ok_or(Status::invalid_argument("Missing operator_id metadata"))
        .map(|x| {
            x.to_str().map_err(|e| {
                Status::invalid_argument(format!("Invalid operator_id metadata: {e:?}"))
            })
        })?
        .map(decode_id)??;

    let remote_worker = metadata
        .get("from-worker-id")
        .ok_or(Status::invalid_argument("Missing worker_id metadata"))
        .map(|x| {
            x.to_str()
                .map_err(|e| Status::invalid_argument(format!("Invalid worker_id metadata: {e:?}")))
        })?
        .map(decode_id)??;

    Ok((remote_worker, operator))
}
