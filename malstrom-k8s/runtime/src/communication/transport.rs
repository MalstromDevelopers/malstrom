use std::time::Duration;

use super::{
    coordinator_backend::CoordinatorGrpcBackend,
    exchange::{
        coordinator_service_client::CoordinatorServiceClient, CoordinatorWorkerRequest,
        OperatorOperatorRequest, WorkerCoordinatorRequest,
    },
    util::new_channel,
    worker_backend::WorkerGrpcBackend,
};
use crate::CONFIG;
use async_trait::async_trait;
use backon::{ConstantBuilder, Retryable};
use flume::{Receiver, RecvError, SendTimeoutError, Sender, TryRecvError};
use malstrom::{
    runtime::communication::{BiStreamTransport, TransportError},
    types::{OperatorId, WorkerId},
};
use thiserror::Error;
use tokio::task::JoinHandle;
use tonic::{metadata::MetadataValue, transport::Endpoint, Request};
use tracing::{debug, error};

/// A Malstrom transport which sends message over the Network via unidirectional
/// GRPC streams
pub(crate) struct GrpcTransport {
    /// sends to other worker
    outbound_sender: Sender<Vec<u8>>,
    /// Receives from other worker
    inbound_receiver: Receiver<Vec<u8>>,
    /// task handling outbound messages
    _outbound_task: JoinHandle<()>,
}

impl GrpcTransport {
    /// Connection between two worker operators
    pub(crate) fn operator_to_operator(
        backend: &WorkerGrpcBackend,
        to_worker_id: WorkerId,
        to_worker_endpoint: Endpoint,
        operator: OperatorId,
    ) -> Self {
        let target = (to_worker_id, operator);
        let inbound_receiver = {
            let mut guard = backend.inbound_channels.lock().unwrap();
            guard.entry(target).or_insert_with(new_channel).1.clone()
        };
        let (outbound_sender, outbound_recv) = {
            let mut guard = backend.outbound_channels.lock().unwrap();
            guard.entry(target).or_insert_with(new_channel).clone()
        };

        let this_worker = backend.this_worker;
        let outbound_task =
            operator_operator_outbound(this_worker, operator, to_worker_endpoint, outbound_recv);
        let outbound_task = backend.rt.spawn(outbound_task);

        GrpcTransport {
            outbound_sender,
            inbound_receiver,
            _outbound_task: outbound_task,
        }
    }

    /// Connection from worker to coordinator
    pub(crate) fn worker_coordinator(backend: &WorkerGrpcBackend, coordinator: Endpoint) -> Self {
        let (outbound_sender, outbound_recv) =
            { backend.coordinator_outbound.lock().unwrap().clone() };
        let inbound_receiver = { backend.coordinator_inbound.lock().unwrap().1.clone() };

        let this_worker = backend.this_worker;
        let outbound_task = worker_coordinator_outbound(this_worker, coordinator, outbound_recv);
        let outbound_task = backend.rt.spawn(outbound_task);

        GrpcTransport {
            outbound_sender,
            inbound_receiver,
            _outbound_task: outbound_task,
        }
    }

    /// Connection from coordinator to worker
    pub(crate) fn coordinator_worker(
        backend: &CoordinatorGrpcBackend,
        to_worker_id: WorkerId,
        to_worker_endpoint: Endpoint,
    ) -> Self {
        let (outbound_sender, outbound_recv) = flume::bounded(CONFIG.network.buffer_capacity);
        let inbound_receiver = {
            backend
                .inbound_channels
                .lock()
                .unwrap()
                .entry(to_worker_id)
                .or_insert_with(new_channel)
                .1
                .clone()
        };
        let outbound_task = backend.rt.spawn(coordinator_worker_outbound(
            outbound_recv,
            to_worker_endpoint,
        ));
        GrpcTransport {
            outbound_sender,
            inbound_receiver,
            _outbound_task: outbound_task,
        }
    }
}

#[async_trait]
impl BiStreamTransport for GrpcTransport {
    fn send(&self, msg: Vec<u8>) -> Result<(), TransportError> {
        match self
            .outbound_sender
            .send_timeout(msg, CONFIG.network.enqeueue_timeout())
        {
            Ok(_) => Ok(()),
            Err(SendTimeoutError::Timeout(_)) => {
                Err(GrpcTransportSendError::QueueFullTimeout.wrapped())
            }
            Err(SendTimeoutError::Disconnected(_)) => {
                Err(GrpcTransportSendError::SenderDead.wrapped())
            }
        }
    }

    fn recv(&self) -> Result<Option<Vec<u8>>, TransportError> {
        match self.inbound_receiver.try_recv() {
            Ok(msg) => Ok(Some(msg)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(GrpcTransportRecvError::ReceiverDead.wrapped()),
        }
    }

    async fn recv_async(&self) -> Result<Vec<u8>, TransportError> {
        match self.inbound_receiver.recv_async().await {
            Ok(msg) => Ok(msg),
            Err(RecvError::Disconnected) => Err(GrpcTransportRecvError::ReceiverDead.wrapped()),
        }
    }
}

#[derive(Debug, Error)]
enum GrpcTransportSendError {
    #[error("Queue Full: Timeout trying to enqueue message into outbound queue")]
    QueueFullTimeout,
    #[error("Sender Thread Dead: Sender thread is disconnected. Check for errors above")]
    SenderDead,
}

impl GrpcTransportSendError {
    fn wrapped(self) -> TransportError {
        TransportError::SendError(Box::new(self))
    }
}

#[derive(Debug, Error)]
enum GrpcTransportRecvError {
    #[error("Receiver Dead: This indicates the internal server died. Check for errors above.")]
    ReceiverDead,
}

impl GrpcTransportRecvError {
    fn wrapped(self) -> TransportError {
        TransportError::RecvError(Box::new(self))
    }
}

use super::exchange::worker_service_client::WorkerServiceClient;

async fn operator_operator_outbound(
    this_worker: WorkerId,
    operator: OperatorId,
    to_worker: Endpoint,
    outbound: Receiver<Vec<u8>>,
) {
    let connect_fn = || to_worker.connect();
    let channel = connect_fn
        .retry(CONFIG.network.initial_conn_retry())
        .sleep(tokio::time::sleep)
        .notify(|e, _| error!("Error connecting to worker: {e:?}. Retrying..."))
        .await
        .map_err(OutboundConnectionError::GrpcConnection)
        .inspect_err(|e| tracing::error!("{e}"))
        .unwrap();

    let mut client = WorkerServiceClient::with_interceptor(channel, move |mut req: Request<()>| {
        req.metadata_mut().insert(
            "operator-id",
            MetadataValue::try_from(operator.to_string()).unwrap(),
        );
        req.metadata_mut().insert(
            "from-worker-id",
            MetadataValue::try_from(this_worker.to_string()).unwrap(),
        );
        Ok(req)
    });
    let stream = async_stream::stream! {
        while let Ok(data) = outbound.recv_async().await {
            yield OperatorOperatorRequest{data};
        }
    };
    client.operator_operator(stream).await.unwrap();
}

/// Worker side
async fn worker_coordinator_outbound(
    this_worker: WorkerId,
    coordinator: Endpoint,
    outbound: Receiver<Vec<u8>>,
) {
    let max_retries = CONFIG.network.initial_conn_timeout_sec as usize;
    let connect_fn = || coordinator.connect();
    let channel = connect_fn
        .retry(
            ConstantBuilder::default()
                .with_delay(Duration::from_secs(1))
                .with_max_times(max_retries),
        )
        .sleep(tokio::time::sleep)
        .notify(|e, _| error!("Error connecting to coordinator: {e:?}. Retrying..."))
        .await
        .map_err(OutboundConnectionError::GrpcConnection)
        .inspect_err(|e| tracing::error!("{e}"))
        .unwrap();

    let mut client =
        CoordinatorServiceClient::with_interceptor(channel, move |mut req: Request<()>| {
            req.metadata_mut().insert(
                "from-worker-id",
                MetadataValue::try_from(this_worker.to_string()).expect("WorkerId should be ASCII"),
            );
            Ok(req)
        });
    let stream = async_stream::stream! {
        while let Ok(data) = outbound.recv_async().await {
            yield WorkerCoordinatorRequest{data};
        }
    };
    debug!("Starting outbound stream to coordinator");
    client.worker_coordinator(stream).await.unwrap();
}

/// Coordinator side
async fn coordinator_worker_outbound(outbound: Receiver<Vec<u8>>, to_worker: Endpoint) {
    let uri = to_worker.uri().clone();
    let max_retries = CONFIG.network.initial_conn_timeout_sec as usize;
    let connect_fn = || to_worker.connect();
    let channel = connect_fn
        .retry(
            ConstantBuilder::default()
                .with_delay(Duration::from_secs(1))
                .with_max_times(max_retries),
        )
        .sleep(tokio::time::sleep)
        .notify(|e, _| error!("Error connecting to worker at {uri:?}: {e:?}. Retrying..."))
        .await
        .map_err(OutboundConnectionError::GrpcConnection)
        .inspect_err(|e| tracing::error!("{e}"))
        .unwrap();

    let mut client = WorkerServiceClient::new(channel);
    let stream = async_stream::stream! {
        while let Ok(data) = outbound.recv_async().await {
            yield CoordinatorWorkerRequest{data};
        }
    };
    debug!("Starting outbound stream to worker");
    client.coordinator_worker(stream).await.unwrap();
}

#[derive(Debug, Error)]
enum OutboundConnectionError {
    #[error("GRPC Error on connection: {0}")]
    GrpcConnection(#[from] tonic::transport::Error),
    #[error(transparent)]
    GrpcStatus(#[from] tonic::Status),
}
