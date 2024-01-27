use crate::{
    channels::selective_broadcast::{Receiver, Sender},
    frontier::{FrontierHandle, Timestamp},
    snapshot::{barrier::BarrierData, PersistenceBackend},
};
/// Sending end (server) of the Network Exchange
use std::{
    marker::PhantomData,
    net::SocketAddr,
    time::{Duration, Instant},
};

use super::{
    api::exchange_message::ExchangeContent, grpc::ExchangeServer, ExchangeData, Remote, CONFIG,
};
use bincode::encode_to_vec;
use tokio::runtime::Handle;

const QUEUE_SIZE: usize = 128;
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

pub struct ExchangeSender<F, O> {
    _rt: Handle,
    server: ExchangeServer,
    partitioner: F,
    remote_count: usize,
    last_heartbeat: Instant,
    _data_type: PhantomData<O>,
}

impl<F, O> ExchangeSender<F, O>
where
    F: FnMut(&O, usize) -> usize,
    O: ExchangeData,
{
    pub fn new(
        rt: Handle,
        local_addr: SocketAddr,
        remote_names: &[Remote],
        partitioner: F,
    ) -> Self {
        let server = ExchangeServer::new_run(&rt, local_addr, QUEUE_SIZE, remote_names);
        Self {
            _rt: rt,
            server,
            partitioner,
            remote_count: remote_names.len(),
            last_heartbeat: Instant::now(),
            _data_type: PhantomData,
        }
    }

    pub fn schedule<P: PersistenceBackend>(
        &mut self,
        input: &mut Receiver<O, P>,
        output: &mut Sender<O, P>,
        frontier: &mut FrontierHandle,
        operator_id: usize,
    ) {
        frontier.advance_to(Timestamp::MAX);
        let now = Instant::now();
        if now.duration_since(self.last_heartbeat) > HEARTBEAT_INTERVAL {
            // send progress update
            for i in 0..self.remote_count {
                self.server
                    .send(ExchangeContent::Frontier(frontier.get_actual().into()), i);
            }
            self.last_heartbeat = now;
        }
        match input.recv() {
            Some(BarrierData::Load(p)) => {
                frontier.advance_to(p.load::<()>(operator_id).unwrap_or_default().0);
                output.send(BarrierData::Load(p))
            }
            Some(BarrierData::Barrier(mut b)) => {
                // persist frontier
                b.persist(frontier.get_actual(), &(), operator_id);
                for i in 0..self.remote_count {
                    self.server
                        .send(ExchangeContent::Barrier(super::api::Barrier {}), i);
                }
                output.send(BarrierData::Barrier(b))
            }
            Some(BarrierData::Data(d)) => {
                // send data and progress
                let idx = (self.partitioner)(&d, self.remote_count + 1);
                if idx == 0 {
                    output.send(BarrierData::Data(d));
                } else {
                    let encoded = encode_to_vec(d, CONFIG).expect("Encoding error");
                    self.server
                        .send(ExchangeContent::Data(encoded), idx - 1)
                        .expect("Out of range partition");
                }
            }
            None => (),
        }
    }
}
