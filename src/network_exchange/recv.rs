/// Receiving end of the network exchange
use std::time::Duration;
use crate::{
    channels::selective_broadcast::{Receiver, Sender},
    frontier::{FrontierHandle, Timestamp},
    stream::jetstream::NoData,
    snapshot::{PersistenceBackend, barrier::BarrierData},
};

use super::{grpc::ExchangeClient, Remote, ExchangeData, CONFIG};
use super::api::exchange_message::ExchangeContent;
use tokio::runtime::Handle;

const RETRIES: usize = 5;
const RETRY_INTERVAL: Duration = Duration::from_secs(1);
const QUEUE_SIZE: usize = 128;

pub struct ExchangeReceiver<P> {
    _rt: Handle,
    connection: ExchangeClient,
    local_barrier: Option<P>,
    remote_barrier: Option<()>,
    loaded: bool,
}

impl<P> ExchangeReceiver<P> where P: PersistenceBackend {

    pub fn new(client_id: String, remote: Remote, rt: Handle) -> Self {
        let connection = ExchangeClient::start_new(&rt, remote.uri, RETRIES, RETRY_INTERVAL, QUEUE_SIZE, client_id);
        Self { _rt: rt, connection, local_barrier: None, remote_barrier: None, loaded: false }
    }

    pub fn schedule<O: ExchangeData>(&mut self, input: &mut Receiver<NoData, P>, output: &mut Sender<O, P>, frontier: &mut FrontierHandle, operator_id: usize) -> () {
        if self.local_barrier.is_none() {
            self.local_barrier = match input.recv() {
                Some(BarrierData::Barrier(b)) => Some(b),
                Some(BarrierData::Load(p)) => {
                    // Prevent accepting remote data before the initial load
                    self.loaded = true;
                    frontier.advance_to(p.load::<()>(operator_id).0);
                    output.send(BarrierData::Load(p)); None},
                _ => None
            }
        }
        if !self.loaded {
            return;
        }
        if self.remote_barrier.is_none() {
            for msg in self.connection.recv_all() {
                match msg {
                    ExchangeContent::Barrier(_) => {self.remote_barrier = Some(()); break;}
                    ExchangeContent::Frontier(x) => {let _ = frontier.advance_to(Timestamp::new(x));},
                    ExchangeContent::Data(d) => {
                        let decoded: O = bincode::decode_from_slice(&d, CONFIG).expect("Decoding error").0;
                        output.send(BarrierData::Data(decoded));

                    }
                }
            }
        }
        (self.local_barrier, self.remote_barrier) = match (self.local_barrier.take(), self.remote_barrier.take()) {
            (Some(mut b), Some(_)) => {
                b.persist(frontier.get_actual(), &(), operator_id);
                output.send(BarrierData::Barrier(b));
                (None, None)
            },
            (x, y) => (x, y)
        }
    }
}
