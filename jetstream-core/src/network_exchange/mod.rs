/// Network Exchange Operator
/// This operator can be used to send data over the network to other processing nodes
/// executing the same dataflow graph
mod grpc;
mod recv;
mod send;
mod api {
    tonic::include_proto!("jetstream.network_exchange");
}

use crate::{
    channels::selective_broadcast::{Receiver, Sender}, snapshot::PersistenceBackend, stream::jetstream::{Data, JetStreamBuilder}, stream::operator::StandardOperator, worker::Worker, WorkerId
};
use bincode::{config::Configuration, Decode, Encode};
use derive_new::new;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    net::SocketAddr,
    ops::Range,
};
use tonic::transport::Uri;

/// Data which can be exchanged via network
pub trait ExchangeData: Data + Encode + Decode {}
impl<T: Data + Encode + Decode> ExchangeData for T {}
const CONFIG: Configuration = bincode::config::standard();

/// Select an index from a given range using a rendezvous hash.
/// Rendezvous hashing minimizes changes in the returned indices
/// when the requested range changes.
///
/// One disadvantage of rendezvous hashing is, that N hash operations
/// have to be performed for every value, with N being the size of the
/// given range.
/// Users with very large ranges might consider caching results.
///
/// See: https://en.wikipedia.org/wiki/Rendezvous_hashing
pub fn rendezvous_hash<T: Hash>(value: &T, range: Range<usize>) -> Option<usize> {
    range
        // calculate hash for combination of index value
        .map(|i| {
            let mut hasher = DefaultHasher::new();
            value.hash(&mut hasher);
            i.hash(&mut hasher);
            (hasher.finish(), i)
        })
        // max by hash
        .max_by_key(|x| x.0)
        .map(|x| x.1)
}

#[derive(new, Clone)]
pub struct Remote {
    name: String,
    uri: Uri,
}

pub trait NetworkExchange<O, P> {
    /// Network exchange operator, sends data via network to remote nodes
    fn network_exchange(
        self,
        partitioner: impl for<'a> FnMut(&O, &'a[WorkerId]) -> &'a[WorkerId] + 'static,
    ) -> Self;
}

impl<O, P> NetworkExchange<O, P> for JetStreamBuilder<O, P>
where
    O: ExchangeData,
    P: PersistenceBackend,
{
    fn network_exchange(
        self,
        partitioner: impl for<'a> FnMut(&O, &'a[WorkerId]) -> &'a[WorkerId] + 'static,
    ) -> Self {
        let send_op = StandardOperator::new(
            move |input: &mut Receiver<O, P>, output, ctx| {
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
            },
        );
        streams.push(self.then(send_op));
        // create all receivers (clients)
        for r in remotes.iter() {
            let mut recv_client = recv::ExchangeReceiver::<P>::new(
                local_name.clone(),
                r.clone(),
                runtime.handle().clone(),
            );
            streams.push(JetStreamBuilder::from_operator(StandardOperator::new(
                move |input, output: &mut Sender<O, P>, ctx| {
                    recv_client.schedule(input, output, ctx)
                },
            )))
        }

        worker.union(streams)
    }
}
