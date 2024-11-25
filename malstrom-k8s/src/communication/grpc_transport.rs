use std::time::Duration;

use crate::config::CONFIG;
use flume::{Receiver, Sender};
use jetstream::runtime::communication::{Transport, TransportError};
use jetstream::types::{OperatorId, WorkerId};
use log::{debug, warn};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use tonic::transport::Endpoint;

use super::proto::exchange_client::ExchangeClient;
use super::proto::StreamRequest;
use backon::Retryable;

pub(crate) struct GrpcTransport {
    inbound_rx: Receiver<Vec<u8>>,
    outgoing: Sender<Vec<u8>>,
    _inbound_thread: JoinHandle<()>,
}

impl GrpcTransport {
    /// Create a new connection to the remote operator and worker
    /// This will eagerly initialize the connection
    pub(crate) fn new(
        rt: &Handle,
        this_worker: WorkerId,
        remote_worker_addr: Endpoint,
        operator_id: OperatorId,
        outgoing: Sender<Vec<u8>>,
    ) -> Self {
        let (inbound_tx, inbound_rx) = flume::bounded(CONFIG.network.inbound_buf_cap);

        let inbound_thread = rt.spawn(async move {
            receive_task(this_worker, operator_id, inbound_tx, remote_worker_addr).await
        });
        Self {
            inbound_rx,
            outgoing,
            _inbound_thread: inbound_thread,
        }
    }
}

impl Transport for GrpcTransport {
    fn send(&self, msg: Vec<u8>) -> Result<(), jetstream::runtime::communication::TransportError> {
        self.outgoing.send(msg).map_err(TransportError::send_error)
    }

    fn recv(&self) -> Result<Option<Vec<u8>>, jetstream::runtime::communication::TransportError> {
        match self.inbound_rx.try_recv() {
            Ok(x) => Ok(Some(x)),
            // Err is returned if no data or the sender was dropped
            // The sender will only be dropped if we shut down, so this is
            // not an issue
            Err(_) => Ok(None),
        }
    }
}

/// Background task to receive messages from another worker
async fn receive_task(
    this_worker: WorkerId,
    operator_id: OperatorId,
    channel: Sender<Vec<u8>>,
    remote_worker_addr: Endpoint,
) -> () {
    debug!("Connecting to endpoint {:?}", remote_worker_addr.uri());
    let mut client = (|| ExchangeClient::connect(remote_worker_addr.clone()))
        .retry(CONFIG.network.initial_conn_retry())
        .sleep(tokio::time::sleep)
        .when(|_| true)
        .notify(|e, dur: Duration| warn!("Failed to connect client: {e:?}, retry in {dur:#?}"))
        .await
        .unwrap();
    debug!("Client connected, sending stream request");
    let request = tonic::Request::new(StreamRequest {
        worker_id: this_worker,
        operator_id,
    });
    let mut msg_stream = client
        .uni_stream(request)
        .await
        .expect("Get stream")
        .into_inner();

    loop {
        let msg = msg_stream.message().await;
        match msg {
            Ok(Some(x)) => {
                channel.send_async(x.data).await.unwrap();
            }
            Err(e) => Err(e).unwrap(),
            Ok(None) => break,
        }
    }
}
