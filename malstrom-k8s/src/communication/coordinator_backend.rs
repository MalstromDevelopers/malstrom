use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::{collections::HashMap, sync::Arc};

use crate::CONFIG;

use super::exchange::coordinator_service_server::CoordinatorService;
use super::k8s_operator::coordinator_operator_service_server::{CoordinatorOperatorService, CoordinatorOperatorServiceServer};

use super::k8s_operator::{GetStatusRequest, GetStatusResponse, RescaleRequest, RescaleResponse, RescaleResult};
use super::{
    client::GrpcTransport,
    exchange::{
        coordinator_service_server::CoordinatorServiceServer, WorkerCoordinatorRequest,
        WorkerCoordinatorResponse,
    },
    util::{decode_id, new_channel},
};
use flume::{Receiver, Sender};
use futures::channel::oneshot;
use futures::StreamExt;
use malstrom::{
    runtime::communication::{BiStreamTransport, CoordinatorWorkerComm},
    types::WorkerId,
};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

type InboundChannels = Arc<Mutex<HashMap<WorkerId, (Sender<Vec<u8>>, Receiver<Vec<u8>>)>>>;
type OutboundChannels = Arc<Mutex<HashMap<WorkerId, (Sender<Vec<u8>>, Receiver<Vec<u8>>)>>>;

pub(crate) struct CoordinatorGrpcBackend {
    pub(super) rt: tokio::runtime::Runtime,
    pub(super) server_task: JoinHandle<Result<(), tonic::transport::Error>>,
    /// channels on which we receive messages from worker
    pub(super) inbound_channels: InboundChannels,
    /// channels on which we send messages to workers
    pub(super) outbound_channels: OutboundChannels,
}
struct CoordinatorGrpcServer {
    /// channels on which we receive messages from worker
    pub(super) inbound_channels: InboundChannels,

    /// channel to send commands to coordinator
    command_tx: Sender<APICommand>
}

enum APICommand {
    Rescale(RescaleCommand)
}
struct RescaleCommand {
    desired: u64,
    on_finish: tokio::sync::oneshot::Sender<()>
}

impl CoordinatorGrpcBackend {
    /// Create a new backend by creating a new Tokio runtime and starting
    /// the communication server
    pub(crate) fn new(command_tx: flume::Sender<APICommand>) -> Result<Self, std::io::Error> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let inbound = Arc::new(Mutex::new(HashMap::new()));
        let outbound = Arc::new(Mutex::new(HashMap::new()));

        let server = Arc::new(CoordinatorGrpcServer {
            inbound_channels: Arc::clone(&inbound),
            command_tx
        });
        let server_future = Server::builder()
            .add_service(CoordinatorServiceServer::from_arc(Arc::clone(&server)))
            .add_service(CoordinatorOperatorServiceServer::from_arc(server))
            .serve(SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::UNSPECIFIED,
                CONFIG.network.port,
            )));
        let server_task = rt.spawn(server_future);
        Ok(CoordinatorGrpcBackend {
            rt,
            server_task,
            inbound_channels: inbound,
            outbound_channels: outbound,
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
        Ok(Box::new(GrpcTransport::coordinator_worker(self, worker_id)))
    }
}

#[tonic::async_trait]
impl CoordinatorService for CoordinatorGrpcServer {
    async fn worker_coordinator(
        &self,
        request: Request<Streaming<WorkerCoordinatorRequest>>,
    ) -> Result<Response<WorkerCoordinatorResponse>, Status> {
        let remote_worker = request
            .metadata()
            .get_bin("from_worker_id")
            .ok_or(Status::invalid_argument("Missing worker_id metadata"))
            .map(|x| {
                x.to_bytes().map_err(|e| {
                    Status::invalid_argument(format!("Invalid worker_id metadata: {e:?}"))
                })
            })?
            .map(decode_id)??;

        let inbound = {
            self.inbound_channels
                .lock()
                .await
                .entry(remote_worker)
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
        Ok(Response::new(WorkerCoordinatorResponse {}))
    }
}

#[tonic::async_trait]
impl CoordinatorOperatorService for CoordinatorGrpcServer {

    async fn rescale(&self, request: Request<RescaleRequest>) -> Result<Response<RescaleResponse>, Status> {
        let desired = request.into_inner().desired_scale;
        let (promise_tx, promise_rx) = tokio::sync::oneshot::channel();
        let command = APICommand::Rescale(RescaleCommand { desired, on_finish: promise_tx });
        self.command_tx.send_async(command).await.map_err(|_| Status::internal("Coordinator is dead"))?;
        match promise_rx.await {
            Ok(_) => Ok(Response::new(RescaleResponse {})),
            Err(e) => Err(Status::unknown(format!("Unknown error while resacling: {e:?}")))
        }
    }

    async fn get_status(&self, request: Request<GetStatusRequest>) -> Result<Response<GetStatusResponse>, Status> {
        todo!()
    }
}