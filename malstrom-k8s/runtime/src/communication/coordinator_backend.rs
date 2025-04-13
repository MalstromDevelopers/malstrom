use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::Duration;
use std::{collections::HashMap, sync::Arc};

use crate::CONFIG;

use super::discovery::lookup_worker_addr;
use super::exchange::coordinator_service_server::CoordinatorService;
use super::k8s_operator::coordinator_operator_service_server::{
    CoordinatorOperatorService, CoordinatorOperatorServiceServer,
};

use super::k8s_operator::{RescaleRequest, RescaleResponse};
use super::{
    exchange::{
        coordinator_service_server::CoordinatorServiceServer, WorkerCoordinatorRequest,
        WorkerCoordinatorResponse,
    },
    transport::GrpcTransport,
    util::{decode_id, new_channel},
};
use super::{APICommand, RescaleCommand};
use flume::{Receiver, Sender};
use futures::{Stream, StreamExt};
use malstrom::{
    runtime::communication::{BiStreamTransport, CoordinatorWorkerComm},
    types::WorkerId,
};
use std::sync::Mutex;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::server::Connected;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info};

type InboundChannels = Arc<Mutex<HashMap<WorkerId, (Sender<Vec<u8>>, Receiver<Vec<u8>>)>>>;
type OutboundChannels = Arc<Mutex<HashMap<WorkerId, (Sender<Vec<u8>>, Receiver<Vec<u8>>)>>>;

/// GRPC backend of th coordinator handling its incoming and outgoing connections.
/// For the coordinator connections may come from the workers as well as from the K8S operator
pub(crate) struct CoordinatorGrpcBackend {
    pub(super) rt: tokio::runtime::Handle,
    pub(super) _server_task: JoinHandle<Result<(), tonic::transport::Error>>,
    /// channels on which we receive messages from worker
    pub(super) inbound_channels: InboundChannels,
    /// channels on which we send messages to workers
    pub(super) _outbound_channels: OutboundChannels,
}
struct CoordinatorGrpcServer {
    /// channels on which we receive messages from worker
    pub(super) inbound_channels: InboundChannels,

    /// channel to send commands to coordinator
    command_tx: Sender<APICommand>,
}

impl CoordinatorGrpcBackend {
    /// Create a new backend by creating a new Tokio runtime and starting
    /// the communication server
    pub(crate) fn new(
        command_tx: flume::Sender<APICommand>,
        rt: tokio::runtime::Handle,
    ) -> Result<Self, std::io::Error> {
        let socket = SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::UNSPECIFIED,
            CONFIG.network.port,
        ));
        let incoming = rt.block_on(TcpListener::bind(socket))?;
        let incoming = TcpListenerStream::new(incoming);
        Self::new_with_incoming(rt, command_tx, incoming)
    }

    pub(crate) fn new_with_incoming<I, IO, IE>(
        rt: Handle,
        command_tx: flume::Sender<APICommand>,
        incoming: I,
    ) -> Result<Self, std::io::Error>
    where
        // these are tonic's bounds, not mine
        I: Stream<Item = Result<IO, IE>> + std::marker::Send + 'static,
        IO: AsyncRead + AsyncWrite + Connected + Unpin + Send + 'static,
        IO::ConnectInfo: Clone + Send + Sync + 'static,
        IE: Into<Box<dyn std::error::Error + Send + Sync>> + std::marker::Send + 'static,
    {
        let inbound = Arc::new(Mutex::new(HashMap::new()));
        let outbound = Arc::new(Mutex::new(HashMap::new()));

        let server = Arc::new(CoordinatorGrpcServer {
            inbound_channels: Arc::clone(&inbound),
            command_tx,
        });
        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        let server_future = Server::builder()
            .add_service(CoordinatorServiceServer::from_arc(Arc::clone(&server)))
            .add_service(CoordinatorOperatorServiceServer::from_arc(server))
            .add_service(health_service)
            .serve_with_incoming(incoming);

        let server_task = rt.spawn(server_future);
        rt.spawn(async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            health_reporter
                .set_serving::<CoordinatorServiceServer<CoordinatorGrpcServer>>()
                .await;
            health_reporter
                .set_serving::<CoordinatorOperatorServiceServer<CoordinatorGrpcServer>>()
                .await;
        });
        Ok(CoordinatorGrpcBackend {
            rt,
            _server_task: server_task,
            inbound_channels: inbound,
            _outbound_channels: outbound,
        })
    }
}

impl CoordinatorWorkerComm for CoordinatorGrpcBackend {
    fn coordinator_to_worker(
        &self,
        worker_id: WorkerId,
    ) -> Result<
        Box<dyn BiStreamTransport>,
        malstrom::runtime::communication::CommunicationBackendError,
    > {
        let endpoint = lookup_worker_addr(worker_id);
        Ok(Box::new(GrpcTransport::coordinator_worker(
            self, worker_id, endpoint,
        )))
    }
}

#[tonic::async_trait]
impl CoordinatorService for CoordinatorGrpcServer {
    async fn worker_coordinator(
        &self,
        request: Request<Streaming<WorkerCoordinatorRequest>>,
    ) -> Result<Response<WorkerCoordinatorResponse>, Status> {
        debug!("Incoming connection");
        let remote_worker = request
            .metadata()
            .get("from-worker-id")
            .ok_or(Status::invalid_argument("Missing worker_id metadata"))
            .map(|x| {
                x.to_str().map_err(|e| {
                    Status::invalid_argument(format!("Invalid worker_id metadata: {e:?}"))
                })
            })?
            .map(decode_id)??;

        info!("Accepted connection from worker {remote_worker}");
        let inbound = {
            self.inbound_channels
                .lock()
                .unwrap()
                .entry(remote_worker)
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
        Ok(Response::new(WorkerCoordinatorResponse {}))
    }
}

#[tonic::async_trait]
impl CoordinatorOperatorService for CoordinatorGrpcServer {
    async fn rescale(
        &self,
        request: Request<RescaleRequest>,
    ) -> Result<Response<RescaleResponse>, Status> {
        let desired = request.into_inner().desired_scale;
        let (promise_tx, promise_rx) = tokio::sync::oneshot::channel();
        let command = APICommand::Rescale(RescaleCommand {
            desired,
            on_finish: promise_tx,
        });
        self.command_tx
            .send_async(command)
            .await
            .map_err(|_| Status::internal("Coordinator is dead"))?;
        match promise_rx.await {
            Ok(_) => Ok(Response::new(RescaleResponse {})),
            Err(e) => Err(Status::unknown(format!(
                "Unknown error while resacling: {e:?}"
            ))),
        }
    }
}
