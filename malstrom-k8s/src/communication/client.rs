use super::{
    coordinator_backend::CoordinatorGrpcBackend,
    discovery::{lookup_coordinator_addr, lookup_worker_addr},
    exchange::{
        coordinator_service_client::CoordinatorServiceClient, CoordinatorWorkerRequest,
        OperatorOperatorRequest, WorkerCoordinatorRequest,
    },
    util::{encode_id, new_channel},
    worker_backend::WorkerGrpcBackend,
};
use crate::CONFIG;
use async_trait::async_trait;
use flume::{Receiver, RecvError, SendTimeoutError, Sender, TryRecvError};
use malstrom::{
    runtime::communication::{BiStreamTransport, TransportError},
    types::{OperatorId, WorkerId},
};
use thiserror::Error;
use tokio::task::JoinHandle;
use tonic::{metadata::MetadataValue, Request};

pub(super) struct GrpcTransport {
    /// sends to other worker
    outbound_sender: Sender<Vec<u8>>,
    /// Receives from other worker
    inbound_receiver: Receiver<Vec<u8>>,
    /// task handling outbound messages
    outbound_task: JoinHandle<Result<(), OutboundConnectionError>>,
}

impl GrpcTransport {
    pub(super) fn operator_to_operator(
        backend: &WorkerGrpcBackend,
        to_worker: WorkerId,
        operator: OperatorId,
    ) -> Self {
        let target = (to_worker, operator);
        let inbound_receiver = {
            let mut guard = backend.inbound_channels.blocking_lock();
            guard.entry(target).or_insert_with(new_channel).1.clone()
        };
        let (outbound_sender, outbound_recv) = {
            let mut guard = backend.outbound_channels.blocking_lock();
            guard.entry(target).or_insert_with(new_channel).clone()
        };

        let this_worker = backend.this_worker;
        let outbound_task =
            operator_operator_outbound(this_worker, to_worker, operator, outbound_recv);
        let outbound_task = backend.rt.spawn(outbound_task);

        GrpcTransport {
            outbound_sender,
            inbound_receiver,
            outbound_task,
        }
    }

    /// worker side
    pub(super) fn worker_coordinator(backend: &WorkerGrpcBackend) -> Self {
        let (outbound_sender, outbound_recv) =
            { backend.coordinator_outbound.blocking_lock().clone() };
        let inbound_receiver = { backend.coordinator_inbound.blocking_lock().1.clone() };

        let this_worker = backend.this_worker;
        let outbound_task = worker_coordinator_outbound(this_worker, outbound_recv);
        let outbound_task = backend.rt.spawn(outbound_task);

        GrpcTransport {
            outbound_sender,
            inbound_receiver,
            outbound_task,
        }
    }

    /// coordinator side
    pub(super) fn coordinator_worker(
        backend: &CoordinatorGrpcBackend,
        to_worker: WorkerId,
    ) -> Self {
        let (outbound_sender, outbound_recv) = {
            backend
                .outbound_channels
                .blocking_lock()
                .entry(to_worker)
                .or_insert_with(new_channel)
                .clone()
        };
        let inbound_receiver = {
            backend
                .outbound_channels
                .blocking_lock()
                .entry(to_worker)
                .or_insert_with(new_channel)
                .1
                .clone()
        };
        let outbound_task = coordinator_worker_outbound(outbound_recv, to_worker);
        let outbound_task = backend.rt.spawn(outbound_task);

        GrpcTransport {
            outbound_sender,
            inbound_receiver,
            outbound_task,
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
    to_worker: WorkerId,
    operator: OperatorId,
    outbound: Receiver<Vec<u8>>,
) -> Result<(), OutboundConnectionError> {
    let endpoint = lookup_worker_addr(to_worker);
    let channel = endpoint
        .connect_timeout(CONFIG.network.initial_conn_timeout())
        .connect()
        .await?;

    let mut client = WorkerServiceClient::with_interceptor(channel, move |mut req: Request<()>| {
        req.metadata_mut().insert_bin(
            "operator_id",
            MetadataValue::from_bytes(&encode_id(operator)),
        );
        req.metadata_mut().insert_bin(
            "from_worker_id",
            MetadataValue::from_bytes(&encode_id(this_worker)),
        );
        Ok(req)
    });
    let stream = async_stream::stream! {
        while let Ok(data) = outbound.recv_async().await {
            yield OperatorOperatorRequest{data};
        }
    };
    client.operator_operator(stream).await?;
    Ok(())
}

/// Worker side
async fn worker_coordinator_outbound(
    this_worker: WorkerId,
    outbound: Receiver<Vec<u8>>,
) -> Result<(), OutboundConnectionError> {
    let endpoint = lookup_coordinator_addr();
    let channel = endpoint
        .connect_timeout(CONFIG.network.initial_conn_timeout())
        .connect()
        .await?;

    let mut client =
        CoordinatorServiceClient::with_interceptor(channel, move |mut req: Request<()>| {
            req.metadata_mut().insert_bin(
                "from_worker_id",
                MetadataValue::from_bytes(&encode_id(this_worker)),
            );
            Ok(req)
        });
    let stream = async_stream::stream! {
        while let Ok(data) = outbound.recv_async().await {
            yield WorkerCoordinatorRequest{data};
        }
    };
    client.worker_coordinator(stream).await?;
    Ok(())
}

/// Coordinator side
async fn coordinator_worker_outbound(
    outbound: Receiver<Vec<u8>>,
    to_worker: WorkerId,
) -> Result<(), OutboundConnectionError> {
    let endpoint = lookup_worker_addr(to_worker);
    let channel = endpoint
        .connect_timeout(CONFIG.network.initial_conn_timeout())
        .connect()
        .await?;

    let mut client =
        WorkerServiceClient::with_interceptor(channel, move |req: Request<()>| Ok(req));
    let stream = async_stream::stream! {
        while let Ok(data) = outbound.recv_async().await {
            yield CoordinatorWorkerRequest{data};
        }
    };
    client.coordinator_worker(stream).await?;
    Ok(())
}

#[derive(Debug, Error)]
enum OutboundConnectionError {
    #[error("Initial connection to remote worker timed out")]
    InitialConnectionTimeout,
    #[error("GRPC Error on connection")]
    GrpcConnection(#[from] tonic::transport::Error),
    #[error(transparent)]
    GrpcStatus(#[from] tonic::Status),
}
