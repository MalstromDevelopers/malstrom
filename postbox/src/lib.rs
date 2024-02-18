mod grpc {
    tonic::include_proto!("postbox");
}
use std::hash::Hash;
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

use bincode::serde::{decode_from_slice, encode_to_vec};
use grpc::generic_communication_client::GenericCommunicationClient;
use grpc::generic_communication_server::GenericCommunication;
use grpc::generic_communication_server::GenericCommunicationServer;
use grpc::{ExchangeMessage, ExchangeResponse};
use itertools;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use thiserror::Error;

const CONFIG: bincode::config::Configuration = bincode::config::standard();

pub trait WorkerId: Clone + Hash + Eq {}
impl<T: Clone + Hash + Eq> WorkerId for T {}
pub trait OperatorId:
    Clone + Serialize + DeserializeOwned + Hash + Eq + Sync + Send + 'static
{
}
impl<T: Clone + Serialize + DeserializeOwned + Hash + Eq + Sync + Send + 'static> OperatorId for T {}
pub trait Data: Serialize + DeserializeOwned {}
impl<T: Serialize + DeserializeOwned> Data for T {}

#[derive(Error, Debug)]
pub enum SendError {
    #[error("Recipient not found")]
    RecipientNotFound,
    #[error("Channel receiving end at the GRPC sender has been dropped")]
    GrpcSenderDropped,
    #[error("Message could not be serialized")]
    EncodingError(#[from] bincode::error::EncodeError),
}
#[derive(Error, Debug)]
pub enum RecvError {
    #[error("Receiver task paniced")]
    ReceiverLost,
    #[error("Message could not be deserialized")]
    DecodingError(#[from] bincode::error::DecodeError),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message<O, D> {
    pub operator: O,
    pub data: D,
}
impl<O, D> Message<O, D> {
    pub fn new(operator: O, data: D) -> Self {
        Self { operator, data }
    }
}

struct IntermediateMessage<O> {
    operator_id: O,
    data: Vec<u8>,
}

impl<O> TryFrom<ExchangeMessage> for IntermediateMessage<O>
where
    O: DeserializeOwned,
{
    type Error = bincode::error::DecodeError;
    fn try_from(value: ExchangeMessage) -> Result<Self, Self::Error> {
        decode_from_slice(&value.operator_id, CONFIG)
            .map(|x| x.0)
            .map(|x| Self {
                operator_id: x,
                data: value.data,
            })
    }
}

impl<O, D> TryFrom<IntermediateMessage<O>> for Message<O, D>
where
    D: DeserializeOwned,
{
    type Error = bincode::error::DecodeError;
    fn try_from(value: IntermediateMessage<O>) -> Result<Self, Self::Error> {
        decode_from_slice(&value.data, CONFIG)
            .map(|x| x.0)
            .map(|x| Self {
                operator: value.operator_id,
                data: x,
            })
    }
}

impl<O, D> TryInto<ExchangeMessage> for Message<O, D>
where
    O: Serialize,
    D: Serialize,
{
    type Error = bincode::error::EncodeError;
    fn try_into(self) -> Result<ExchangeMessage, Self::Error> {
        let operator_id = encode_to_vec(self.operator, CONFIG)?;
        let data = encode_to_vec(self.data, CONFIG)?;
        Ok(ExchangeMessage { operator_id, data })
    }
}

pub struct Postbox<W, O> {
    incoming: flume::Receiver<IntermediateMessage<O>>,
    outgoing: IndexMap<W, flume::Sender<ExchangeMessage>>,
}

impl<W, O> Postbox<W, O>
where
    W: WorkerId,
    O: OperatorId,
{
    pub fn recv<D: Data>(&self) -> Result<Option<Message<O, D>>, RecvError> {
        match self.incoming.try_recv() {
            Ok(x) => Ok(Some(x.try_into()?)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(RecvError::ReceiverLost),
        }
    }

    pub fn get_peers(&self) -> Vec<&W> {
        self.outgoing.keys().collect()
    }

    pub fn send<D: Data>(&self, recipient: &W, message: Message<O, D>) -> Result<(), SendError> {
        self.send_encoded(recipient, message.try_into()?)
    }

    fn send_encoded(&self, recipient: &W, message: ExchangeMessage) -> Result<(), SendError> {
        self.outgoing
            .get(recipient)
            .ok_or(SendError::RecipientNotFound)?
            .send(message)
            .map_err(|_| SendError::GrpcSenderDropped)
    }

    pub fn broadcast<D: Data>(&self, message: Message<O, D>) -> Result<(), SendError> {
        let targets = self.get_peers();
        let encoded = message.try_into()?;
        let messages = itertools::repeat_n(encoded, targets.len());
        let res: Result<Vec<()>, SendError> = itertools::zip_eq(targets, messages)
            .map(|(w, m)| self.send_encoded(w, m))
            .collect();
        res.map(|_| ())
    }
}

pub struct BackendBuilder<O, W> {
    connection_timeout: Duration,
    retry_interval: Duration,
    retry_count: usize,
    listen_addr: SocketAddr,
    outgoing: IndexMap<W, flume::Sender<ExchangeMessage>>,
    outgoing_rx: IndexMap<(W, Uri), flume::Receiver<ExchangeMessage>>,
    incoming: IndexMap<O, flume::Receiver<IntermediateMessage<O>>>,
    incoming_tx: Vec<(O, flume::Sender<IntermediateMessage<O>>)>,
}

impl<O, W> BackendBuilder<O, W>
where
    O: OperatorId,
    W: WorkerId,
{
    pub fn new(
        listen_addr: SocketAddr,
        peers: Vec<(W, Uri)>,
        operators: Vec<O>,
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
            outgoing.insert(w.clone(), tx);
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

    pub fn for_operator(&self, operator_id: &O) -> Option<Postbox<W, O>> {
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
    _clients: Vec<GrpcSender>,
    _server_task: JoinHandle<()>,
    _rt: Runtime,
}

impl CommunicationBackend {
    fn new_connect<O: OperatorId, W>(
        connection_timeout: Duration,
        retry_interval: Duration,
        retry_count: usize,
        listen_addr: SocketAddr,
        outgoing_rx: IndexMap<(W, Uri), flume::Receiver<ExchangeMessage>>,
        incoming_tx: Vec<(O, flume::Sender<IntermediateMessage<O>>)>,
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
            _clients: clients,
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

struct GrpcReceiver<O> {
    channels: IndexMap<O, Sender<IntermediateMessage<O>>>,
}
impl<O> GrpcReceiver<O>
where
    O: OperatorId,
{
    fn new(senders: Vec<(O, Sender<IntermediateMessage<O>>)>) -> Self {
        let inner = IndexMap::from_iter(senders);
        Self { channels: inner }
    }
}

#[tonic::async_trait]
impl<O> GenericCommunication for GrpcReceiver<O>
where
    O: OperatorId,
{
    async fn generic_exchange(
        &self,
        request: Request<tonic::Streaming<ExchangeMessage>>,
    ) -> Result<Response<ExchangeResponse>, Status> {
        let channels = self.channels.clone();
        let mut stream = request.into_inner();

        while let Some(msg) = stream.next().await {
            let msg: IntermediateMessage<O> = msg?.try_into().expect("Error decoding operator id");
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
