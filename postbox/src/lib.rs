use async_stream::stream;
use errors::{BuildError, ClientCreationError, ConnectionError, RecvError, SendError};
use grpc::generic_communication_client::GenericCommunicationClient;
use grpc::generic_communication_server::{GenericCommunication, GenericCommunicationServer};
use grpc::{ExchangeMessage, ExchangeResponse};
use indexmap::IndexMap;
use serde::{de::DeserializeOwned, Serialize};
use tokio_stream::StreamExt;
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::runtime::{Handle, Runtime};
use tokio::task::JoinHandle;
use tonic::metadata::{Binary, BinaryMetadataKey, MetadataValue};
use tonic::transport::{Server, Uri};
use tonic::{Request, Response, Status};
use tracing::log::warn;

mod grpc {
    tonic::include_proto!("postbox");
}
pub mod errors;

const BC_CONFIG: bincode::config::Configuration = bincode::config::standard();

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

type Raw = Vec<u8>;

#[derive(Clone)]
struct PostboxConfig {
    connection_timeout: Duration,
    retry_interval: Duration,
    retry_count: usize,
    queue_size: usize,
}
impl Default for PostboxConfig {
    fn default() -> Self {
        Self {
            connection_timeout: Duration::from_secs(5),
            retry_interval: Duration::from_secs(5),
            retry_count: 12,
            queue_size: 4096,
        }
    }
}

pub struct PostboxBuilder<A> {
    config: PostboxConfig,
    address_type: PhantomData<A>
}
impl<A> Default for PostboxBuilder<A> {
    fn default() -> Self {
        Self { config: PostboxConfig::default(), address_type: PhantomData }
    }
}

impl<A> PostboxBuilder<A> where A: Address {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_connection_timeout(mut self, timeout: Duration) -> Self {
        self.config.connection_timeout = timeout;
        self
    }
    pub fn with_retry_interval(mut self, interval: Duration) -> Self {
        self.config.retry_interval = interval;
        self
    }
    pub fn with_retry_count(mut self, count: usize) -> Self {
        self.config.retry_count = count;
        self
    }
    pub fn with_queue_size(mut self, size: usize) -> Self {
        self.config.queue_size = size;
        self
    }

    /// Build this postbox with the current configuration and starts the
    /// Postbox server
    pub fn build(self, socket: SocketAddr, address_resolver: impl FnMut(&A) -> Option<Uri> + 'static) -> Result<Postbox<A>, BuildError> {
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
        let remotes = AddressMap::default();
        let server = PostBoxServer::new(remotes.clone(), self.config.clone());

        let server_task = rt.spawn(async move {
            Server::builder()
                .add_service(GenericCommunicationServer::new(server))
                .serve(socket)
                .await
                .unwrap();
        });
        Ok(Postbox{
            rt,
            server_task,
            remotes,
            config: self.config,
            address_resolver: Box::new(address_resolver)
        })
    }
}

type AddressMap<A> = Arc<Mutex<IndexMap<A, (flume::Sender<Raw>, flume::Receiver<Raw>)>>>;

/// The postbox backend
pub struct Postbox<A> {
    rt: Runtime,
    server_task: JoinHandle<()>,

    // Any messages from these addresses are
    // placed into the sender. The map can be accepted by either creating a client
    // for an address (key) locally, or the client connecting to the server.
    // We allow both ends (client & server) to add to this map, to avoid deadlocks
    // on the cluster level
    // Clients can clone the Receiver to receive messages from a remote
    remotes: Arc<Mutex<IndexMap<A, (flume::Sender<Raw>, flume::Receiver<Raw>)>>>,

    config: PostboxConfig,
    address_resolver: Box<dyn FnMut(&A) -> Option<Uri>>,
}

impl<A> Postbox<A>
where
    A: Address,
{
    /// Creates a client and immediatly connects it to the given address
    pub fn new_client<T: Data>(
        &mut self,
        connect_to: A,
        this_client: A,
    ) -> Result<Client<T>, errors::ClientCreationError> {
        let endpoint = (self.address_resolver)(&connect_to).ok_or(
            ClientCreationError::AddressResolutionError(format!("{:?}", connect_to)),
        )?;
        let incoming = {
            self.remotes
                .lock()
                .unwrap()
                .entry(this_client.clone())
                .or_insert_with(|| flume::bounded(self.config.queue_size))
                .1
                .clone()
        };
        Ok(Client::new_connect(
            connect_to,
            endpoint,
            incoming,
            self.rt.handle().clone(),
            self.config.clone(),
        )?)
    }
}

/// A client for bi-directional communication with a given address
pub struct Client<T> {
    outgoing: flume::Sender<Raw>,
    incoming: flume::Receiver<Raw>,

    // keep a handle to keep the runtime alive
    rt: Handle,
    grpc_sender: JoinHandle<()>,

    msg_type: PhantomData<T>,
}

/// BASE64 encode a Postbox address
fn encode_address<A: Address>(
    address: A,
) -> Result<MetadataValue<Binary>, bincode::error::EncodeError> {
    let address = bincode::serde::encode_to_vec(address, BC_CONFIG)?;
    Ok(MetadataValue::from_bytes(&address))
}

impl<T> Client<T>
where
    T: Data,
{
    /// Create a client and connect it immediately
    fn new_connect<A: Address>(
        address: A,
        endpoint: Uri,
        incoming: flume::Receiver<Raw>,
        rt: Handle,
        config: PostboxConfig,
    ) -> Result<Self, ConnectionError> {
        let (out_tx, out_rx) = flume::bounded::<Raw>(config.queue_size);

        let address = encode_address(address)?;
        let _guard = rt.enter();
        let task = rt.spawn(async move {
            let mut retries_left = config.retry_count;
            let stream = stream! {
                loop {
                    match out_rx.recv_async().await {
                        Ok(value) => {yield ExchangeMessage { data: value, }},
                        Err(_) => break // channel ended
                    }
                }
            };
            let mut request = Request::new(stream);
            request.metadata_mut().insert_bin(BinaryMetadataKey::from_static("postbox-target-bin"), address);
            let connection = loop {
                match tonic::transport::Endpoint::new(endpoint.clone())
                    .unwrap()
                    .connect_timeout(config.connection_timeout)
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
                            warn!("Timeout connecting to {:?}. Trying {} more times...", endpoint, retries_left);
                            tokio::time::sleep(config.retry_interval).await;
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

        Ok(Self {
            outgoing: out_tx,
            incoming,
            rt,
            grpc_sender: task,
            msg_type: PhantomData,
        })
    }

    /// Queue up a message to be sent by this client
    pub fn send(&self, msg: T) -> Result<(), SendError> {
        let encoded = bincode::serde::encode_to_vec(msg, BC_CONFIG)?;
        self.outgoing.send(encoded).map_err(|_| SendError::DeadClientError)?;
        Ok(())
    }

    /// Receive the next buffered message if any
    pub fn recv(&self) -> Option<Result<T, RecvError>> {
        let encoded = self.incoming.try_recv().ok()?;
        let decoded = bincode::serde::decode_from_slice(&encoded, BC_CONFIG).map_err(|e| RecvError::EncodingError(e)).map(|x| x.0);
        Some(decoded)
    }
}

impl<T> Drop for Client<T>  {
    fn drop(&mut self) {
        // drain the channel
        while !self.outgoing.is_empty() {
            std::thread::sleep(Duration::from_millis(10));
        }

        // dropping the sender should cause the stream
        // consumed by the GRPC client to end, which will lead to
        // the client finishing
        // a bit hacky
        let (mut tx, _) = flume::bounded(0);
        std::mem::swap(&mut tx, &mut self.outgoing);
        drop(tx);
        // also a bit hacky, since we can not take ownership of the JoinHandle
        while !self.grpc_sender.is_finished() {
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

struct PostBoxServer<A> {
    // key: client addr, value: keyed by sender
    channels: AddressMap<A>,
    config: PostboxConfig
}

impl<A> PostBoxServer<A> {
    fn new(channels: AddressMap<A>, config: PostboxConfig) -> Self {
        Self { channels, config }
    }
}

#[tonic::async_trait]
impl<A> GenericCommunication for PostBoxServer<A>
where
    A: Address,
{
    async fn generic_exchange(
        &self,
        request: Request<tonic::Streaming<ExchangeMessage>>,
    ) -> Result<Response<ExchangeResponse>, Status> {
        let target = request
            .metadata()
            .get_bin("postbox-target-bin")
            .ok_or(Status::invalid_argument("Missing 'postbox-target-bin' header"))?
            .to_bytes()
            .map_err(|_| Status::invalid_argument("Invalid 'postbox-target-bin' header"))?;
        
        let target = bincode::serde::decode_from_slice(&target, BC_CONFIG)
            .map_err(|_| Status::invalid_argument("Error decoding 'postbox-target-bin' header"))?.0;

        let sender = {
            self.channels.lock().unwrap().entry(target).or_insert_with(|| flume::bounded(self.config.queue_size)).0.clone()
        };

        let mut stream = request.into_inner();
        while let Some(msg) = stream.next().await {
            let msg = msg?;
            sender
                .send_async(msg.data)
                .await
                // should never happen unless the postbox is dropped
                .map_err(|_| Status::internal("Receivers dropped"))
                .expect("All receivers have been dropped, no one is listening.");
        }
        Ok(Response::new(ExchangeResponse {}))
    }
}

#[cfg(test)]
mod test {
    use std::net::{SocketAddr, SocketAddrV4, TcpListener};

    use tonic::{client, transport::Uri};

    use crate::{Address, Postbox, PostboxBuilder};

    fn get_socket() -> SocketAddr {
        // find a free port
        let port = {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let port = listener.local_addr().unwrap().port();
            drop(listener);
            port
        };
        SocketAddr::V4(SocketAddrV4::new("127.0.0.1".parse().unwrap(), port))
    }

    /// Returns a very ego-centric postbox (it resolves all addresses to itself)
    fn self_postbox<A: Address>() -> Postbox<A> {
        let sock = get_socket();
        let here = Uri::builder().scheme("http").authority(sock.to_string()).path_and_query("").build().unwrap();
        PostboxBuilder::<A>::new().build(sock, move |_| Some(here.clone())).unwrap()
    }

    /// returns a pair of postboxes, that always connect to each other
    fn pair_postbox<A: Address>() -> (Postbox<A>, Postbox<A>) {
        let sock_a = get_socket();
        let sock_b = get_socket();

        let uri_a = Uri::builder().scheme("http").authority(sock_a.to_string()).path_and_query("").build().unwrap();
        let uri_b = Uri::builder().scheme("http").authority(sock_b.to_string()).path_and_query("").build().unwrap();

        let pb_a = PostboxBuilder::<A>::new().build(sock_a, move |_| Some(uri_b.clone())).unwrap();
        let pb_b = PostboxBuilder::<A>::new().build(sock_b, move |_| Some(uri_a.clone())).unwrap();
        (pb_a, pb_b)
    }

    /// Check we do not sputter a giant tokio stack trace all over the console
    /// but instead end the client with grace and elegance
    #[test]
    fn drop_client_gracefully() {
        let mut pb: Postbox<String> = self_postbox();
        let client = pb.new_client::<()>("foobar".into(), "baz".into()).unwrap();
        drop(client)
    }

    /// Check we can pass a message around
    #[test]
    fn send_to_remote() {
        let (mut pb_a, mut pb_b) = pair_postbox::<String>();
        let client_a = pb_a.new_client::<i32>("foo".into(), "bar".into()).unwrap();
        client_a.send(42).unwrap();

        let client_b = pb_b.new_client::<i32>("bar".into(), "foo".into()).unwrap();
        loop {
            if let Some(msg) = client_b.recv().map(|x| x.unwrap()) {
                assert_eq!(msg, 42);
                break;
            }
        }
    }
}