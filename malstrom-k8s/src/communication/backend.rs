use flume::{Receiver, Sender, TryRecvError};
use malstrom::runtime::communication::Transport;
use malstrom::runtime::CommunicationBackend;
use malstrom::types::{OperatorId, WorkerId};
use log::debug;
use std::collections::HashMap;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

use super::discovery::lookup_worker_addr;
use super::grpc_transport::GrpcTransport;
use super::proto::exchange_server::{Exchange, ExchangeServer};
use super::proto::{StreamRequest, StreamResponse};
use tokio::sync::Mutex;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
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
        let backend = BackendServer::new(Arc::clone(&outgoing));
        let svc = ExchangeServer::new(backend);

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
    ) -> Sender<Vec<u8>> {
        self.outgoing
            .blocking_lock()
            .entry((worker_id, operator_id))
            .or_insert_with(|| flume::bounded(CONFIG.network.outbound_buf_cap))
            .0
            .clone()
    }
}

impl CommunicationBackend for GrpcBackend {
    fn new_connection(
        &mut self,
        to_worker: WorkerId,
        operator: OperatorId,
    ) -> Result<Box<dyn Transport>, malstrom::runtime::communication::CommunicationBackendError>
    {
        let remote_addr = lookup_worker_addr(to_worker);
        let outgoing = self.get_or_create_outgoing_send(to_worker, operator);
        let transport = GrpcTransport::new(
            self.rt.handle(),
            self.this_worker,
            remote_addr,
            operator,
            outgoing,
        );
        Ok(Box::new(transport))
    }
}

type ChannelPair = (Sender<Vec<u8>>, Receiver<Vec<u8>>);
type FullId = (WorkerId, OperatorId);
type ChannelMap = Arc<Mutex<HashMap<FullId, ChannelPair>>>;
#[derive(Debug)]
struct BackendServer {
    // keyed by worker, operator where messages sent into channel will be sent to
    outgoing_channels: ChannelMap,
}

impl BackendServer {
    fn new(outgoing_channels: ChannelMap) -> Self {
        Self { outgoing_channels }
    }

    async fn get_or_create_outgoing_recv(
        &self,
        worker_id: WorkerId,
        operator_id: OperatorId,
    ) -> Receiver<Vec<u8>> {
        self.outgoing_channels
            .lock()
            .await
            .entry((worker_id, operator_id))
            .or_insert_with(|| flume::bounded(CONFIG.network.outbound_buf_cap))
            .1
            .clone()
    }
}

#[tonic::async_trait]
impl Exchange for BackendServer {
    // type UniStreamStream = ReceiverStream<Result<StreamResponse, Status>>;
    type UniStreamStream = ResponseStream;

    async fn uni_stream(
        &self,
        request: Request<StreamRequest>,
    ) -> Result<Response<Self::UniStreamStream>, Status> {
        debug!("Receiving request from {:?}", request.remote_addr());
        let req = request.into_inner();
        let recv = self
            .get_or_create_outgoing_recv(req.worker_id, req.operator_id)
            .await;
        Ok(Response::new(ResponseStream(recv)))
    }
}

struct ResponseStream(Receiver<Vec<u8>>);

impl futures_core::Stream for ResponseStream {
    type Item = Result<StreamResponse, Status>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.0.try_recv() {
            Ok(data) => {
                Poll::Ready(Some(Ok(StreamResponse { data })))
            }
            Err(TryRecvError::Empty) => {
                // we don't have any good way of knowing when a value
                // will be awailable and checking is just as expensive
                // as calling recv
                let waker = cx.waker();
                waker.wake_by_ref();
                Poll::Pending
            },
            // internal sender disconnected
            Err(TryRecvError::Disconnected) => Poll::Ready(None),
        }
    }
}
