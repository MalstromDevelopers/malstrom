mod api {
    tonic::include_proto!("jetstream.network_exchange");
}

use anyhow::Context;
use api::network_exchange_server::NetworkExchange;
use api::{SubscribeRequest, WrapperResponse};
use flume::{Receiver, Sender};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use tokio_stream::{Stream, StreamExt};
use tonic::{transport::Server, Request, Response, Status};
use tracing::{debug, info, instrument, warn};

// based off https://github.com/hyperium/tonic/blob/master/examples/src/streaming/server.rs

pub struct ExchangeServer {
    server_task: JoinHandle<()>,
    send_queues: Vec<Sender<Vec<u8>>>,
    shutdown_tx: Sender<()>,
}

impl ExchangeServer {
    /// Create a new Exchange server with N connections.
    /// Returns server and N senders, which may be used to queue messages
    /// for remote receivers
    ///
    /// The parameter `queue_size` controls how many messages **each** queue
    /// can hold.
    /// If a queue is full, trying to send via the sender becomes a blocking
    /// operation.
    pub fn new_run(
        rt: &Handle,
        addr: SocketAddr,
        queue_size: usize,
        remote_names: &[String],
    ) -> Self {
        let mut send_queues_rx = HashMap::with_capacity(remote_names.len());
        let mut send_queues = Vec::with_capacity(remote_names.len());

        for name in remote_names.iter() {
            let (tx, rx) = flume::bounded::<Vec<u8>>(queue_size);
            send_queues_rx.insert(name.clone(), rx);
            send_queues.push(tx);
        }
        let inner_server = InnerExchangeServer {
            send_queues: send_queues_rx,
        };

        // create a channel which signals shutdown to our grpc server
        let (shutdown_tx, shutdown_rx) = flume::bounded::<()>(0);

        let server_task = rt.spawn(async move {
            println!("Starting Server future");
            Server::builder()
                .add_service(api::network_exchange_server::NetworkExchangeServer::new(
                    inner_server,
                ))
                .serve_with_shutdown(addr, async { shutdown_rx.recv_async().await.unwrap() })
                .await
                .context("GRPC Server terminated")
                .unwrap();
        });

        Self {
            server_task,
            send_queues,
            shutdown_tx,
        }
    }

    /// Queue a message for sending on the given endpoint index
    /// If queue is full, will block
    ///
    /// # Returns
    /// None if index is out of range
    ///
    /// # Panics
    /// If the internal server has crashed
    pub fn send(&self, msg: Vec<u8>, idx: usize) -> Option<()> {
        assert!(!self.server_task.is_finished());

        // TODO: panics if self.send_queues.len() == 0
        // SAFETY: We can unwrap, because we wrapped the index
        self.send_queues
            .get(idx)?
            .send(msg)
            .expect("GRPC Server terminated");
        Some(())
    }
}

impl Drop for ExchangeServer {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(());
    }
}

pub struct InnerExchangeServer<T> {
    send_queues: HashMap<String, Receiver<T>>,
}

#[tonic::async_trait]
impl NetworkExchange for InnerExchangeServer<Vec<u8>> {
    type SimpleExchangeStream = Pin<Box<dyn Stream<Item = Result<WrapperResponse, Status>> + Send>>;

    async fn simple_exchange(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SimpleExchangeStream>, Status> {
        let req = request.into_inner();

        let borrowed_recv = self
            .send_queues
            .get(&req.client_id)
            .ok_or(Status::out_of_range("Subscriber id out of range"))?;

        // only allow one connection per channel
        if borrowed_recv.receiver_count() > 1 {
            return Err(Status::already_exists("SubscriberId already subscribed"));
        }
        let recv = borrowed_recv.to_owned();

        let (tx, rx_sync) = flume::bounded::<Result<WrapperResponse, Status>>(1);
        let rx = rx_sync.into_stream();
        tokio::spawn(async move {
            // let mut application_stream = recv.re();
            while let Ok(item) = recv.recv_async().await {
                let msg = Ok(WrapperResponse { encoded: item });

                match tx.send_async(msg).await {
                    Ok(_) => {
                        // item (server response) was queued to be send to client
                    }
                    Err(_) => {
                        // output_stream was build from rx and both are dropped
                        break;
                    }
                }
            }

            warn!("Application disconnected")
        });
        Ok(Response::new(Box::pin(rx) as Self::SimpleExchangeStream))
    }
}

use api::network_exchange_client::NetworkExchangeClient;
use tonic::transport::Uri;

pub struct ExchangeClient {
    recv_queue_rx: Receiver<Vec<u8>>,

    client_task: tokio::task::JoinHandle<()>,
}

impl ExchangeClient {
    #[instrument(skip_all)]
    pub fn start_new(
        rt: &Handle,
        remote_addr: Uri,
        retries: usize,
        retry_interval: Duration,
        queue_size: usize,
        client_id: String,
    ) -> Self {
        let (recv_queue_tx, recv_queue_rx) = flume::bounded::<Vec<u8>>(queue_size);

        // try connecting to a remote endpoint
        let mut maybe_client = None;
        for _ in 0..retries {
            info!("Attempting connection to {remote_addr}");
            let _guard = rt.enter();
            let connection = rt.block_on(tokio::time::timeout(
                retry_interval,
                NetworkExchangeClient::connect(remote_addr.clone()),
            ));
            debug!("Connecton result {connection:?}");

            match connection {
                // timeout
                Err(_) => continue,
                // connection error
                Ok(Err(_)) => continue,
                Ok(Ok(c)) => {
                    maybe_client = Some(c);
                    break;
                }
            }
        }
        let mut client = maybe_client.expect("Could not establish connection");
        // start receiving updates
        let client_task = rt.spawn(async move {
            // TODO: Reconnection logic
            let mut stream = client
                .simple_exchange(SubscribeRequest {
                    client_id: client_id,
                })
                .await
                .context(format!("Failed to subscribe to {remote_addr:?}"))
                .unwrap()
                .into_inner();
            while let Some(msg) = stream.next().await {
                match msg {
                    Ok(m) => recv_queue_tx.send_async(m.encoded).await.unwrap(),
                    Err(e) => panic!("Unimplemented client excepion: {}", e),
                }
            }
        });

        ExchangeClient {
            recv_queue_rx,
            client_task,
        }
    }

    /// Retrieve all messages received since last calling this method
    pub fn recv_all(&self) -> impl Iterator<Item = Vec<u8>> + '_ {
        self.recv_queue_rx.drain()
    }
}
