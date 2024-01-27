mod grpc {
    tonic::include_proto!("postbox");
}
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_stream::stream;
use flume::{Receiver, Sender, TryRecvError};
use indexmap::IndexMap;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tonic::transport::{Server, Uri};
use tonic::{Request, Response, Status};

use grpc::generic_communication_client::GenericCommunicationClient;
use grpc::generic_communication_server::GenericCommunication;
use grpc::generic_communication_server::GenericCommunicationServer;
use grpc::{ExchangeMessage, ExchangeResponse};

use thiserror::Error;

type OperatorId = u32;
type WorkerId = u32;

#[derive(Debug)]
pub struct Message {
    pub operator: u32,
    pub data: Vec<u8>,
}
impl Message {
    pub fn new(operator: u32, data: Vec<u8>) -> Self {
        Self { operator, data }
    }
}

impl From<ExchangeMessage> for Message {
    fn from(value: ExchangeMessage) -> Self {
        Message {
            operator: value.operator_id,
            data: value.data,
        }
    }
}

impl Into<ExchangeMessage> for Message {
    fn into(self) -> ExchangeMessage {
        ExchangeMessage {
            operator_id: self.operator,
            data: self.data,
        }
    }
}

#[derive(Error, Debug)]
pub enum SendError {
    #[error("Recipient not found")]
    RecipientNotFound,
    #[error("Channel receiving end at the GRPC sender has been dropped")]
    GrpcSenderDropped,
}
#[derive(Error, Debug)]
pub enum RecvError {
    #[error("Receiver task paniced")]
    ReceiverLost,
}

pub struct Postbox {
    incoming: flume::Receiver<ExchangeMessage>,
    outgoing: IndexMap<WorkerId, flume::Sender<ExchangeMessage>>,
}

impl Postbox {
    pub fn recv(&self) -> Result<Option<Message>, RecvError> {
        match self.incoming.try_recv() {
            Ok(x) => Ok(Some(x.into())),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(RecvError::ReceiverLost),
        }
    }

    pub fn get_peers(&self) -> Vec<&WorkerId> {
        self.outgoing.keys().collect()
    }

    pub fn send(&self, recipient: &WorkerId, message: Message) -> Result<(), SendError> {
        self.outgoing
            .get(recipient)
            .ok_or(SendError::RecipientNotFound)?
            .send(message.into())
            .map_err(|_| SendError::GrpcSenderDropped)
    }
}

pub struct BackendBuilder {
    connection_timeout: Duration,
    retry_interval: Duration,
    retry_count: usize,
    listen_addr: SocketAddr,
    outgoing: IndexMap<WorkerId, flume::Sender<ExchangeMessage>>,
    outgoing_rx: IndexMap<(WorkerId, Uri), flume::Receiver<ExchangeMessage>>,
    incoming: IndexMap<OperatorId, flume::Receiver<ExchangeMessage>>,
    incoming_tx: Vec<(OperatorId, flume::Sender<ExchangeMessage>)>,
}

impl BackendBuilder {
    pub fn new(
        listen_addr: SocketAddr,
        peers: Vec<(WorkerId, Uri)>,
        operators: Vec<OperatorId>,
        queue_size: usize,
    ) -> Self {
        let mut incoming = IndexMap::with_capacity(operators.len());
        let mut incoming_tx = Vec::with_capacity(operators.len());
        for op in operators.iter() {
            let (tx, rx) = flume::bounded(queue_size);
            incoming_tx.push((op.clone(), tx));
            incoming.insert(op.clone(), rx);
        }
        let mut outgoing = IndexMap::with_capacity(peers.len());
        let mut outgoing_rx = IndexMap::with_capacity(peers.len());
        for (w, uri) in peers.into_iter() {
            let (tx, rx) = flume::bounded(queue_size);
            outgoing.insert(w, tx);
            outgoing_rx.insert((w, uri), rx);
        }
        Self {
            connection_timeout: Duration::from_secs(5),
            retry_interval: Duration::from_secs(5),
            retry_count: 12,
            listen_addr,
            outgoing,
            outgoing_rx,
            incoming,
            incoming_tx,
        }
    }

    pub fn with_connection_timeout(mut self, connection_timeout: Duration) -> Self {
        self.connection_timeout = connection_timeout;
        self
    }

    pub fn with_retry_interval(mut self, retry_interval: Duration) -> Self {
        self.retry_interval = retry_interval;
        self
    }

    pub fn with_retry_count(mut self, retry_count: usize) -> Self {
        self.retry_count = retry_count;
        self
    }

    pub fn for_operator(&self, operator_id: &OperatorId) -> Option<Postbox> {
        let incoming = self.incoming.get(operator_id)?.clone();
        Some(Postbox {
            incoming,
            outgoing: self.outgoing.clone(),
        })
    }

    pub fn connect(self) -> Result<CommunicationBackend, BuildError> {
        CommunicationBackend::new_connect(
            self.connection_timeout,
            self.retry_interval,
            self.retry_count,
            self.listen_addr,
            self.outgoing_rx,
            self.incoming_tx,
        )
    }
}

#[derive(Error, Debug)]
pub enum BuildError {
    #[error("Error creating tokio runtime")]
    RuntimeCreationFailed,
}

pub struct CommunicationBackend {
    clients: Vec<GrpcSender>,
    _server_task: JoinHandle<()>,
    _rt: Runtime,
}

impl CommunicationBackend {
    fn new_connect(
        connection_timeout: Duration,
        retry_interval: Duration,
        retry_count: usize,
        listen_addr: SocketAddr,
        outgoing_rx: IndexMap<(WorkerId, Uri), flume::Receiver<ExchangeMessage>>,
        incoming_tx: Vec<(OperatorId, flume::Sender<ExchangeMessage>)>,
    ) -> Result<Self, BuildError> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|_| BuildError::RuntimeCreationFailed)?;

        let server = GrpcReceiver::new(incoming_tx);
        let _guard = rt.enter();
        let server_task = rt.spawn(async move {
            Server::builder()
                .add_service(GenericCommunicationServer::new(server))
                .serve(listen_addr)
                .await
                .unwrap();
        });
        std::thread::sleep(Duration::from_secs(1));

        let mut clients = Vec::with_capacity(outgoing_rx.len());
        for (k, v) in outgoing_rx.into_iter() {
            let sender = GrpcSender::new_connect(
                &rt,
                k.1,
                v,
                connection_timeout,
                retry_interval,
                retry_count,
            );
            clients.push(sender);
        }

        while !clients.iter().all(|x| x.is_connected()) {
            std::thread::sleep(Duration::from_millis(1))
        }

        Ok(Self {
            clients,
            _server_task: server_task,
            _rt: rt,
        })
    }
}

struct GrpcSender {
    _task: JoinHandle<()>,
    is_connected: Arc<RwLock<bool>>,
}

impl GrpcSender {
    pub fn new_connect(
        rt: &Runtime,
        addr: Uri,
        recv: Receiver<ExchangeMessage>,
        connection_timeout: Duration,
        retry_interval: Duration,
        retry_count: usize,
    ) -> Self {
        let is_connected = Arc::new(RwLock::new(false));
        let ready = is_connected.clone();

        let _guard = rt.enter();
        let _task = rt.spawn(async move {
            let mut retries_left = retry_count;
            let stream = stream! {
                for await value in recv.into_stream() {
                    yield value;
                }
            };
            let request = Request::new(stream);

            let connection = loop {
                match tonic::transport::Endpoint::new(addr.clone())
                    .unwrap()
                    .connect_timeout(connection_timeout)
                    .connect()
                    .await
                {
                    Ok(x) => {
                        break x;
                    }
                    e => {
                        retries_left -= 1;
                        if retries_left == 0 {
                            e.expect("Error connecting to remote");
                        } else {
                            tokio::time::sleep(retry_interval).await;
                            continue;
                        }
                    }
                }
            };
            {
                *ready.write().unwrap() = true;
            }
            let mut client = GenericCommunicationClient::new(connection);
            match client.generic_exchange(request).await {
                Ok(_) => (),
                e => {
                    e.expect("Streaming error");
                }
            }
        });
        Self {
            _task,
            is_connected,
        }
    }

    pub fn is_connected(&self) -> bool {
        *self.is_connected.read().unwrap()
    }
}

struct GrpcReceiver {
    channels: IndexMap<OperatorId, Sender<ExchangeMessage>>,
}
impl GrpcReceiver {
    fn new(senders: Vec<(OperatorId, Sender<ExchangeMessage>)>) -> Self {
        let inner = IndexMap::from_iter(senders);
        Self { channels: inner }
    }
}

#[tonic::async_trait]
impl GenericCommunication for GrpcReceiver {
    async fn generic_exchange(
        &self,
        request: Request<tonic::Streaming<ExchangeMessage>>,
    ) -> Result<Response<ExchangeResponse>, Status> {
        let channels = self.channels.clone();
        let mut stream = request.into_inner();

        while let Some(msg) = stream.next().await {
            let msg = msg?;
            let sender = channels
                .get(&msg.operator_id)
                .ok_or(Status::unauthenticated("Operator ID unknown"))?;
            sender
                .send_async(msg)
                .await
                .map_err(|_| Status::internal("Receivers dropped"))
                .expect("All receivers have been dropped, no one is listening.");
        }
        Ok(Response::new(ExchangeResponse {}))
    }
}
