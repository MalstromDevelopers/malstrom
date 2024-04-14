mod grpc {
    tonic::include_proto!("postbox");
}
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
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

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use thiserror::Error;

const CONFIG: bincode::config::Configuration = bincode::config::standard();

pub trait WorkerId:
    Clone + Serialize + DeserializeOwned + Hash + Eq + Sync + Send + Debug + 'static
{
}
impl<T: Clone + Serialize + DeserializeOwned + Hash + Eq + Sync + Send + Debug + 'static> WorkerId
    for T
{
}
pub trait OperatorId:
    Clone + Serialize + DeserializeOwned + Hash + Eq + Sync + Send + Debug + 'static
{
}
impl<T: Clone + Serialize + DeserializeOwned + Hash + Eq + Sync + Send + Debug + 'static> OperatorId
    for T
{
}
pub trait Data: Serialize + DeserializeOwned {}
impl<T: Serialize + DeserializeOwned> Data for T {}

type Binary = Vec<u8>;
type EncodedOperator = Vec<u8>;

#[derive(Error, Debug)]
pub enum SendError {
    #[error("Recipient not found {0}")]
    RecipientNotFound(String),
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
#[derive(Error, Debug)]
pub enum BuildError {
    #[error("Recipient not found {0}")]
    OperatorNotFound(String),
    #[error("WorkerId could not be serialized")]
    WorkerEncodingError,
    #[error("OperatorId could not be serialized")]
    OperatorEncodingError,
    #[error("Error creating tokio runtime")]
    RuntimeCreationFailed,
}

#[derive(Clone)]
struct MessageWrapper {
    recv_operator: Binary,
    message: Binary,
}

pub struct Postbox<W, O> {
    this_worker: W,
    this_operator: O,
    // serialized representation of O
    this_operator_enc: EncodedOperator,
    incoming: flume::Receiver<Binary>,
    outgoing: IndexMap<W, flume::Sender<MessageWrapper>>,
}

pub struct RecvIterator<'a, W, O, D>(&'a Postbox<W, O>, PhantomData<D>);

#[derive(Serialize, Deserialize)]
pub struct NetworkMessage<W, O, D> {
    pub sender_worker: W,
    pub sender_operator: O,
    pub data: D,
    // private field to prevent public construction
    private: PhantomData<()>,
}

impl<'a, W, O, D> Iterator for RecvIterator<'a, W, O, D>
where
    W: WorkerId,
    O: OperatorId,
    D: Data,
{
    type Item = NetworkMessage<W, O, D>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.recv::<D>().expect("Postbox error")
    }
}

impl<W, O> Postbox<W, O>
where
    W: WorkerId,
    O: OperatorId,
{
    fn recv<D: Data>(&self) -> Result<Option<NetworkMessage<W, O, D>>, RecvError> {
        match self.incoming.try_recv() {
            Ok(x) => Ok(Some(decode_from_slice(&x, CONFIG)?.0)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(RecvError::ReceiverLost),
        }
    }

    pub fn recv_all<D: Data>(&self) -> RecvIterator<'_, W, O, D> {
        RecvIterator(self, PhantomData)
    }

    // pub fn recv_with_sender<D: Data>(&self) -> Result<Option<(W, D)>, RecvError> {
    //     match self.incoming.try_recv() {
    //         Ok(x) => {
    //             let sender = x.0;
    //             let data = decode_from_slice(&x.1, CONFIG)?.0;
    //             Ok(Some((sender, data)))
    //         }
    //         Err(TryRecvError::Empty) => Ok(None),
    //         Err(TryRecvError::Disconnected) => Err(RecvError::ReceiverLost),
    //     }
    // }

    pub fn get_peers(&self) -> Vec<&W> {
        self.outgoing.keys().collect()
    }

    pub fn send_same<D: Data>(&self, recipient: &W, message: D) -> Result<(), SendError> {
        let net_msg = NetworkMessage {
            sender_worker: self.this_worker.clone(),
            sender_operator: self.this_operator.clone(),
            data: message,
            private: PhantomData,
        };
        let encoded = encode_to_vec(net_msg, CONFIG)?;
        self.send_encoded(
            recipient,
            MessageWrapper {
                recv_operator: self.this_operator_enc.clone(),
                message: encoded,
            },
        )
    }

    pub fn send_other_operator<D: Data>(
        &self,
        recipient: &W,
        recipient_op: &O,
        message: D,
    ) -> Result<(), SendError> {
        let net_msg = NetworkMessage {
            sender_worker: self.this_worker.clone(),
            sender_operator: self.this_operator.clone(),
            data: message,
            private: PhantomData,
        };
        let encoded = encode_to_vec(net_msg, CONFIG)?;
        self.send_encoded(
            recipient,
            MessageWrapper {
                recv_operator: bincode::serde::encode_to_vec(recipient_op, CONFIG)?,
                message: encoded,
            },
        )
    }

    fn send_encoded(&self, recipient: &W, message: MessageWrapper) -> Result<(), SendError> {
        self.outgoing
            .get(recipient)
            .ok_or(SendError::RecipientNotFound(format!("{recipient:?}")))?
            .send(message)
            .map_err(|_| SendError::GrpcSenderDropped)
    }

    pub fn broadcast<D: Data>(&self, message: D) -> Result<(), SendError> {
        let targets = self.get_peers();
        let net_msg = NetworkMessage {
            sender_worker: self.this_worker.clone(),
            sender_operator: self.this_operator.clone(),
            data: message,
            private: PhantomData,
        };
        let encoded = MessageWrapper {
            recv_operator: self.this_operator_enc.clone(),
            message: encode_to_vec(net_msg, CONFIG)?,
        };
        let messages = itertools::repeat_n(encoded, targets.len());
        let res: Result<Vec<()>, SendError> = itertools::zip_eq(targets, messages)
            .map(|(w, m)| self.send_encoded(w, m))
            .collect();
        res.map(|_| ())
    }
}

pub struct BackendBuilder<W, O> {
    this_worker: W,
    connection_timeout: Duration,
    retry_interval: Duration,
    retry_count: usize,
    listen_addr: SocketAddr,
    outgoing: IndexMap<W, flume::Sender<MessageWrapper>>,
    outgoing_rx: IndexMap<(W, Uri), flume::Receiver<MessageWrapper>>,
    incoming: IndexMap<O, flume::Receiver<Binary>>,
    incoming_tx: Vec<(O, flume::Sender<Binary>)>,
    operator_id: PhantomData<O>,
}

impl<W, O> BackendBuilder<W, O>
where
    W: WorkerId,
    O: OperatorId,
{
    pub fn new(
        this_worker: W,
        listen_addr: SocketAddr,
        peers: Vec<(W, Uri)>,
        operators: Vec<O>,
        queue_size: usize,
    ) -> Self {
        let mut incoming = IndexMap::with_capacity(operators.len());
        let mut incoming_tx = Vec::with_capacity(operators.len());
        for op in operators.into_iter() {
            let (tx, rx) = flume::bounded(queue_size);
            incoming_tx.push((op.clone(), tx));
            incoming.insert(op, rx);
        }
        let mut outgoing = IndexMap::with_capacity(peers.len());
        let mut outgoing_rx = IndexMap::with_capacity(peers.len());
        for (w, uri) in peers.into_iter() {
            let (tx, rx) = flume::bounded(queue_size);
            outgoing.insert(w.clone(), tx);
            outgoing_rx.insert((w, uri), rx);
        }
        Self {
            this_worker,
            connection_timeout: Duration::from_secs(5),
            retry_interval: Duration::from_secs(5),
            retry_count: 12,
            listen_addr,
            outgoing,
            outgoing_rx,
            incoming,
            incoming_tx,
            operator_id: PhantomData,
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

    pub fn for_operator(&self, operator_id: O) -> Result<Postbox<W, O>, BuildError> {
        let incoming = self
            .incoming
            .get(&operator_id)
            .ok_or(BuildError::OperatorNotFound(format!("{operator_id:?}")))?
            .clone();
        let op_enc =
            encode_to_vec(&operator_id, CONFIG).map_err(|_| BuildError::OperatorEncodingError)?;
        Ok(Postbox {
            this_worker: self.this_worker.clone(),
            this_operator: operator_id,
            // this_operator: operator_id,
            this_operator_enc: op_enc,
            incoming,
            outgoing: self.outgoing.clone(),
        })
    }

    pub fn connect(self) -> Result<CommunicationBackend, BuildError> {
        let this_worker =
            encode_to_vec(self.this_worker, CONFIG).map_err(|_| BuildError::WorkerEncodingError)?;
        CommunicationBackend::new_connect(
            this_worker,
            self.connection_timeout,
            self.retry_interval,
            self.retry_count,
            self.listen_addr,
            self.outgoing_rx,
            self.incoming_tx,
        )
    }
}

pub struct CommunicationBackend {
    _clients: Vec<GrpcSender>,
    _server_task: JoinHandle<()>,
    _rt: Runtime,
}

impl CommunicationBackend {
    fn new_connect<W: WorkerId, O: OperatorId>(
        this_worker: Binary,
        connection_timeout: Duration,
        retry_interval: Duration,
        retry_count: usize,
        listen_addr: SocketAddr,
        outgoing_rx: IndexMap<(W, Uri), flume::Receiver<MessageWrapper>>,
        incoming_tx: Vec<(O, flume::Sender<Binary>)>,
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
                this_worker.clone(),
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
        this_worker: Binary,
        addr: Uri,
        recv: Receiver<MessageWrapper>,
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
                    yield ExchangeMessage {
                        data: value.message,
                        recipient_operator: value.recv_operator
                    };
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

impl Drop for GrpcSender {
    fn drop(&mut self) {
        // we need to wait a bit to wait for the stream to end
        // so the async task finishes and we don't blurp out a giant
        // tokio stacktrace on drop
        for _ in 0..200 {
            // this is hacky
            if self._task.is_finished() {
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

struct GrpcReceiver<O> {
    channels: IndexMap<O, Sender<Binary>>,
}
impl<O> GrpcReceiver<O>
where
    O: OperatorId,
{
    fn new(senders: Vec<(O, Sender<Binary>)>) -> Self {
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
            let msg = msg?;
            let operator_id: O = decode_from_slice(&msg.recipient_operator, CONFIG)
                .map_err(|_| Status::invalid_argument("Error decoding operator id"))?
                .0;
            let sender = channels
                .get(&operator_id)
                .ok_or(Status::out_of_range("Operator ID unknown"))?;
            sender
                .send_async(msg.data)
                .await
                .map_err(|_| Status::internal("Receivers dropped"))
                .expect("All receivers have been dropped, no one is listening.");
        }
        Ok(Response::new(ExchangeResponse {}))
    }
}
