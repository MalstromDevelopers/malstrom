use bytes::{Buf, BufMut, Bytes, BytesMut};
use flume::{Receiver, Sender, TryRecvError};
use futures::{Stream, StreamExt};
use malstrom::runtime::communication::{BiStreamTransport, CommunicationBackendError, CoordinatorWorkerComm, WorkerCoordinatorComm};
use malstrom::runtime::OperatorOperatorComm;
use malstrom::types::{OperatorId, WorkerId};
use log::debug;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

use super::channel_transport::{ChannelMap, ChannelTransport, ChannelTransportContainer};
use super::discovery::{lookup_coordinator_addr, lookup_worker_addr};
use super::grpc_transport::GrpcTransport;
use super::proto::worker_worker_service_server::{WorkerWorkerService, WorkerWorkerServiceServer};
use super::proto::{OperatorOperatorRequest, OperatorOperatorResponse, };
use tokio::sync::Mutex;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use thiserror::Error;
use crate::config::CONFIG;
/// Communication based on peer-to-peer GRPC channels
pub struct GrpcBackend {
    this_worker: WorkerId,
    rt: Runtime,
    _server_thread: JoinHandle<()>,
    outgoing: ChannelMap,
}

#[derive(Debug, Error)]
pub(crate) enum BackendCreationError {
    #[error("Error spawning async runtime")]
    Runtime(#[from] std::io::Error)
}

impl GrpcBackend {
    /// Create a new backend, this launches a Tokio runtime
    /// and the server task where other workers can listen for messages
    pub(crate) fn new(this_worker: WorkerId) -> Result<Self, BackendCreationError> {
        debug!("Spawning async runtime");
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;

        // PANIC: We know this is safe, since it is hardcoded
        let addr = CONFIG.network.get_bind_addr();
        let outgoing = ChannelMap::default();
        let backend = WorkerServer::new(Arc::clone(&outgoing));
        let svc = WorkerWorkerServiceServer::new(backend);

        debug!("Starting server thread");
        let server_thread = rt.spawn(async move {
            Server::builder()
                .add_service(svc)
                .serve(addr)
                .await
                .unwrap();
        });
        Ok(Self {
            this_worker,
            rt,
            _server_thread: server_thread,
            outgoing,
        })
    }

    fn get_or_create_outgoing_send(
        &self,
        worker_id: WorkerId,
        operator_id: OperatorId,
    ) -> ChannelTransportContainer {
        self.outgoing
            .blocking_lock()
            .entry((worker_id, operator_id))
            .or_insert_with(ChannelTransportContainer::new)
            .clone()
    }
}

impl OperatorOperatorComm for GrpcBackend {
    fn operator_to_operator(
        &self,
        to_worker: WorkerId,
        operator: OperatorId,
    ) -> Result<Box<dyn BiStreamTransport>, CommunicationBackendError>
    {
        let transport = self.get_or_create_outgoing_send(to_worker, operator)
        .from_to(self.this_worker, to_worker).ok_or(CommunicationBackendError::ClientBuildError(
            Box::new(GrpcBackendError::SameId(to_worker))
        ))?;
        Ok(Box::new(transport))
    }
}

impl WorkerCoordinatorComm for GrpcBackend {
    fn worker_to_coordinator(
        &self,
    ) -> Result<Box<dyn BiStreamTransport>, malstrom::runtime::communication::CommunicationBackendError> {
        todo!()
    }
}


#[derive(Debug)]
struct WorkerServer {
    /// ID of this worker
    worker_id: WorkerId,
    // keyed by (remote worker, operator)
    operator_channels: ChannelMap,
    /// channel for communication with coordinator
    coordinator_channel: Arc<ChannelTransportContainer>
}

impl WorkerServer {
    fn new(operator_channels: ChannelMap) -> Self {
        Self { operator_channels }
    }

    async fn get_or_create_transport(
        &self,
        other_worker_id: WorkerId,
        operator_id: OperatorId,
    ) -> ChannelTransportContainer {
        let mut channel_map = self.operator_channels.lock().await;
        channel_map.entry((other_worker_id, operator_id)).or_insert_with(ChannelTransportContainer::new).clone()
    }
}

type PinBoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

#[tonic::async_trait]
impl WorkerWorkerService for WorkerServer {
    // type UniStreamStream = ReceiverStream<Result<StreamResponse, Status>>;
    type OperatorOperatorStream = PinBoxStream<OperatorOperatorResponse>;

    async fn operator_operator(
        &self,
        request: Request<Streaming<OperatorOperatorRequest>>,
    ) -> Result<Response<Self::OperatorOperatorStream>, Status> {
        debug!("Receiving request from {:?}", request.remote_addr());
        let operator = request
        .metadata()
        .get_bin("operator_id")
        .ok_or(Status::invalid_argument("Missing operator_id metadata"))
        .map(|x| x.to_bytes().map_err(|e| Status::invalid_argument(format!("Invalid operator_id metadata: {e:?}"))))?
        .map(decode_id)??;

        let remote_worker = request
        .metadata()
        .get_bin("from_worker_id")
        .ok_or(Status::invalid_argument("Missing worker_id metadata"))
        .map(|x| x.to_bytes().map_err(|e| Status::invalid_argument(format!("Invalid worker_id metadata: {e:?}"))))?
        .map(decode_id)??;

        let transport = self
            .get_or_create_transport(remote_worker, operator)
            .await;

        let (to_local, to_remote) = if remote_worker > self.worker_id {
            (transport.to_low(), transport.to_high())
        } else if remote_worker < self.worker_id {
            (transport.to_high() , transport.to_low())
        } else {
            return Err(Status::invalid_argument("Requestee WorkerId == server WorkerId"));
        };
        
        // send all messages of the incoming stream to our local operator
        tokio::spawn(async move {
            let mut stream = request.into_inner();
            while let Some(msg) = stream.message().await.unwrap() {
                to_local.send_async(msg.data).await.unwrap();
        }});

        let out_stream = Box::pin(to_remote.into_receiver().into_stream().map(
            |data| Result::<OperatorOperatorResponse, Status>::Ok(OperatorOperatorResponse{data})
        ));
        Ok(Response::new(out_stream))
    }

}
