mod grpc {
    tonic::include_proto!("postbox");
}
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

use async_stream::stream;
use base64::Engine;
use flume::{Receiver, Sender, TryRecvError};
use indexmap::IndexMap;
use prost::bytes::Bytes;
use tokio::runtime::{Handle, Runtime};
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

pub trait Address:
    Clone + Serialize + DeserializeOwned + Hash + Eq + Sync + Send + Debug + 'static
{
}
impl<T: Clone + Serialize + DeserializeOwned + Hash + Eq + Sync + Send + Debug + 'static> Address
    for T
{
}
pub trait Data: Serialize + DeserializeOwned {}
impl<T: Serialize + DeserializeOwned> Data for T {}

type Binary = Vec<u8>;

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
    #[error("Address not connected")]
    AddressNotConnected,
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
    _clients: Vec<PostBoxClient>,
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

        let server = PostBoxServer::new(incoming_tx);
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
            let sender = PostBoxClient::new_connect(
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


fn make_connection<T>(
    addr: Uri,
    target: Binary,
    retry_count: &usize,
    retry_interval: &Duration,
    connection_timeout: &Duration,
    rt: Handle,
    channel_size: usize,
) -> Result<(Sender<Binary>, JoinHandle<()>), BuildError> {
    let (tx, recv) = flume::bounded::<T>(channel_size);

    let _guard = rt.enter();
    let task = rt.spawn(async move {
        let mut retries_left = retry_count;
        let stream = stream! {
            for await value in recv.into_stream() {
                yield ExchangeMessage {
                    data: value.message,
                };
            }
        };
        let request = Request::new(stream);
        request.metadata_mut().insert(
            "postbox-target",
            base64::prelude::BASE64_URL_SAFE.encode(target),
        );
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
        let mut client = GenericCommunicationClient::new(connection);
        match client.generic_exchange(request).await {
            Ok(_) => (),
            e => {
                e.expect("Streaming error");
            }
        }
    });
}

struct PostBoxClient<A> {
    this_address: A,
    received: AdressedReceivers<A>,
    outgoing: IndexMap<A, (Sender<Binary>, JoinHandle<()>)>,
    rt: Handle,
    retry_count: usize,
    retry_interval: Duration,
    connection_timeout: Duration,
    channel_size: usize,
    addr_resolver: Rc<dyn Fn(&A) -> Uri>,
}

impl<A> PostBoxClient<A>
where
    A: Address
{

    fn new(address: A, incoming: Rc<RwLock<IndexMap<A, Receiver<Binary>>>>,rt: Handle,
        retry_count: usize,
        retry_interval: Duration,
        connection_timeout: Duration,
        channel_size: usize,
        addr_resolver: Rc<dyn Fn(&A) -> Uri>) -> Self {
            Self { this_address: address, incoming, outgoing: IndexMap::new(), rt, retry_count, retry_interval, connection_timeout, channel_size, addr_resolver }
        }

    fn send_to(&mut self, address: &A, msg: Binary) {
        if !self.outgoing.contains_key(&address) {
            self.connect_to(address.clone())
        }
        self.outgoing.get(&address).unwrap().0.send(msg)
    }

    fn connect_to(&mut self, address: A) -> BuildError {
        let conn = make_connection(
            self.addr_resolver(&address),
            &self.retry_count,
            &self.retry_interval,
            &self.connection_timeout,
            self.rt.clone(),
            self.channel_size,
        )?;
        self.outgoing.insert(address, conn)
    }

    fn disconnect(&mut self, address: &A) -> () {
        if let Some((channel, task)) = self.outgoing.swap_remove(address) {
            drop(channel);
            // having the sender dropped should evntually finish the task
            // once all queued messages are sent
            while !task.1.is_finished() {
                thread::sleep(Duration::from_millis(50));
            }
        }
    }

    fn recv_from<T: DeserializeOwned>(&self, address: &A) -> Option<Result<T, RecvError>> {
        let raw = self.incoming.read().unwrap().get(address).ok_or_else(|| RecvError::AddressNotConnected).try_recv().ok()?;
        Some(bincode::serde::decode_from_slice(&raw, CONFIG)?)

    }

    fn recv_any<T: DeserializeOwned>(&self) -> Option<(A, Result<T, RecvError>)> {
        let incoming = self.incoming.read();
        for (a, r) in incoming.items() {
            if let Some(x) = r.try_recv().ok() {
                let decoded = bincode::serde::decode_from_slice(&x, CONFIG).map_err(|e| Some((a, e)))?;
                return Some((a, Ok(decoded)));
            }
        }
        None
    }
}

impl<A> Drop for PostBoxClient<A> {
    fn drop(&mut self) {
        let keys: Vec<A> = self.outgoing.keys().cloned().collect();
        for a in keys.iter() {
            self.disconnect(a)
        }
    }
}


type AdressedSenders<A> = Rc<RwLock<IndexMap<A, Sender<Binary>>>>;
type AdressedReceivers<A> = Rc<RwLock<IndexMap<A, Receiver<Binary>>>>;

struct PostBoxServer<A> {
    // key: client addr, value: keyed by sender
    clients: IndexMap<A, AdressedSenders<A>>,
    runtime: Runtime,
    retry_count: usize,
    retry_interval: Duration,
    connection_timeout: Duration,
    channel_size: usize,
}
impl<A> PostBoxServer<A>
where
    A: Address
{
    fn new() -> Result<Self, BuildError> {
        let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|_| BuildError::RuntimeCreationFailed)?;
        Ok(Self { 
            clients: IndexMap::new(),
            runtime: rt,
            retry_count: 12,
            retry_interval: Duration::from_secs(5),
            connection_timeout: Duration::from_secs(10),
            channel_size: 4096,
         })
    }

    fn get_client(&self, client_address: A) -> PostBoxClient<A> {
        self.clients.entry(client_address).or_insert_with(|| {
            
            let (tx, rx) = flume::bounded(self.channel_size);

        })

        PostBoxClient::new(A, self.clients., rt, retry_count, retry_interval, connection_timeout, channel_size, addr_resolver)
    }

}

#[tonic::async_trait]
impl<O> GenericCommunication for PostBoxServer<O>
where
    O: OperatorId,
{
    async fn generic_exchange(
        &self,
        request: Request<tonic::Streaming<ExchangeMessage>>,
    ) -> Result<Response<ExchangeResponse>, Status> {
        let channels = self.channels.clone();
        let target_b64 = request
            .metadata()
            .get("postbox-target")
            .ok_or(Status::invalid_argument("Missing 'postbox-target' header"))?;
        let target = base64::prelude::BASE64_URL_SAFE.decode(target_b64.as_bytes()).map_err(|_| Status::invalid_argument("Invalid 'postbox-target' header"))?;
        let sender = channels
            .get(&target)
            .ok_or(Status::out_of_range(format!("Target '{target}' unknown")))?;

        let mut stream = request.into_inner();
        while let Some(msg) = stream.next().await {
            let msg = msg?;
            sender
                .send_async(msg.data)
                .await
                .map_err(|_| Status::internal("Receivers dropped"))
                .expect("All receivers have been dropped, no one is listening.");
        }
        Ok(Response::new(ExchangeResponse {}))
    }
}
