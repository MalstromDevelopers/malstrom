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
    channels::selective_broadcast::{Receiver, Sender},
    snapshot::{PersistenceBackend, SnapshotController},
    stream::jetstream::{Data, JetStreamBuilder},
    stream::operator::StandardOperator,
    worker::Worker,
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
        // return index
        .and_then(|x| Some(x.1))
}

#[derive(new, Clone)]
pub struct Remote {
    name: String,
    uri: Uri,
}

pub trait NetworkExchange<O, P, S> {
    /// Network exchange operator, sends data via network to remote nodes
    fn network_exchange(
        self,
        local_name: String,
        local_addr: SocketAddr,
        remotes: Vec<Remote>,
        worker: &mut Worker<P, S>,
        partitioner: impl FnMut(&O, usize) -> usize + 'static,
    ) -> Self;
}

impl<O, P, S> NetworkExchange<O, P, S> for JetStreamBuilder<O, P>
where
    O: ExchangeData,
    P: PersistenceBackend,
    S: SnapshotController<P> + 'static,
{
    fn network_exchange(
        self,
        local_name: String,
        local_addr: SocketAddr,
        remotes: Vec<Remote>,
        worker: &mut Worker<P, S>,
        partitioner: impl FnMut(&O, usize) -> usize + 'static,
    ) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Error creating tokio runtime");
        let mut streams = Vec::with_capacity(remotes.len() + 1);
        // create sender (server)
        let mut sender =
            send::ExchangeSender::new(runtime.handle().clone(), local_addr, &remotes, partitioner);
        let send_op = StandardOperator::new(
            move |input: &mut Receiver<O, P>, output, frontier, operator_id| {
                sender.schedule(input, output, frontier, operator_id)
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
                move |input, output: &mut Sender<O, P>, frontier, operator_id| {
                    recv_client.schedule(input, output, frontier, operator_id)
                },
            )))
        }

        worker.union(streams)
    }
}
