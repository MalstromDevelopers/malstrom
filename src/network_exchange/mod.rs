mod grpc;

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    net::SocketAddr,
    ops::Range,
    time::{Duration, Instant},
};

use self::grpc::{ExchangeClient, ExchangeServer};
use crate::{
    channels::selective_broadcast,
    frontier::{Frontier, FrontierError, FrontierHandle, Timestamp},
    stream::jetstream::{Data, JetStreamBuilder},
    stream::operator::StandardOperator,
};
use bincode::{Decode, Encode};
use derive_new::new;
use tonic::transport::Uri;
use tracing::{event, instrument, Level};

pub trait ExchangeData: Data + Encode + Decode {}
impl<T: Data + Encode + Decode> ExchangeData for T {}

fn hash<T>(obj: T) -> u64
where
    T: Hash,
{
    let mut hasher = DefaultHasher::new();
    obj.hash(&mut hasher);
    hasher.finish()
}

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

#[derive(new)]
pub struct Remote {
    name: String,
    uri: Uri,
}

pub trait NetworkExchange<T> {
    /// Create a network exchange given the addresses of the remote exchanges
    /// and a partitioning function.
    /// This operator will take care of establishing and maintaining channels with
    /// the remotes.
    ///
    /// The partitioning function will be given a reference to every message `T` and the
    /// number of remotes `N` and should return all indices which shall receive
    /// `T`. If multiple receivers are specified, `T` will be cloned.
    /// Index `0` always refers to the local stream, i.e. `ìndex == N` refers to the last remote.
    /// Out of range indices are wrapped to avoid panics.
    /// Note that this will duplicate messages, if the same index is given multiple times
    ///
    /// This function returns an error if communication with the remotes could not be established.
    ///
    /// # Frontier
    /// The Frontier of the network exchange is the minimum of the local frontier
    /// and the frontiers it has received from remotes.
    /// If there are remotes, but they have not transmitted any messages, their frontier
    /// will be 0.
    ///
    /// # Panics
    /// This function will panic on
    /// - encoding errors
    /// - networking errors
    fn network_exchange(
        self,
        local_name: String,
        local_addr: SocketAddr,
        remotes: &[Remote],
        // implementation note: initially i had the partition return an enum Local/Remote(idx)
        // but that does not really work well if you have no remotes at all
        // with this impl if we have no remotes, the function gets passed a `0` and can
        // just always return zero(s), which seems more elgant
        partitioner: impl FnMut(&T, usize) -> Vec<usize> + 'static,
    ) -> Result<JetStreamBuilder<T>, Box<dyn std::error::Error>>;
}

impl<T> NetworkExchange<T> for JetStreamBuilder<T>
where
    T: ExchangeData,
{
    #[instrument(skip_all)]
    fn network_exchange(
        self,
        client_id: String,
        local_addr: SocketAddr,
        remotes: &[Remote],
        mut partitioner: impl FnMut(&T, usize) -> Vec<usize> + 'static,
    ) -> Result<JetStreamBuilder<T>, Box<dyn std::error::Error>> {
        // TODO: Make these configurable
        let queue_size = 128;
        let retries = 5;
        let retry_interval = Duration::from_secs(1);
        // if we only attach frontier updates to data, remotes may never know
        // about a finished stream, so we will occasionally send frontier
        // info
        // TODD: probably will want to make this independent for diffferent
        // remotes
        let frontier_heartbeat = Duration::from_secs(1);
        let mut last_heartbeat = Instant::now();

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Error creating tokio runtime");

        // Start the local server, which will serve records for remotes to fetch
        // TODO add mechanism to allow checking for server panic.
        // Maybe a lock we can check for poison?
        let remote_names: Vec<String> = remotes.iter().map(|x| x.name.clone()).collect();
        let server =
            ExchangeServer::new_run(runtime.handle(), local_addr, queue_size, &remote_names);

        // Construct clients to fetch messages from remotes
        let mut remotes_clients = Vec::with_capacity(remotes.len());
        for r in remotes {
            let connection = ExchangeClient::start_new(
                runtime.handle(),
                r.uri.clone(),
                retries,
                retry_interval,
                queue_size,
                client_id.clone(),
            );
            remotes_clients.push((connection, Frontier::default()));
        }

        let config = bincode::config::standard();

        // Operator logic
        let logic = move |input: &mut selective_broadcast::Receiver<T>,
                          output: &mut selective_broadcast::Sender<T>,
                          frontier: &mut FrontierHandle| {
            // HACK: we must reference the runtime here to keep it alive,
            // as otherwise server and client will get dropped
            let _ = runtime.enter();

            // send messages from upstream
            if let Some(msg) = input.recv() {
                let indices = partitioner(&msg, remotes_clients.len());
                let indices_cnt = indices.len();

                for (target, msg) in indices
                    .into_iter()
                    .zip(itertools::repeat_n(msg, indices_cnt))
                {
                    if target == 0 || remotes_clients.len() == 0 {
                        output.send(msg);
                        continue;
                    } else {
                        // idx - 1 since 0 encodes for 'local'
                        // PANIC: The mod will not panic with zero div as we checked
                        // for zero len in the if
                        let wrapped_idx = (target - 1) % remotes_clients.len();
                        // encode the data and attach current upstream frontier
                        let frontier = frontier.get_upstream_actual();
                        let encoded =
                            bincode::encode_to_vec(msg, config).expect("Error encoding value");
                        server.send((frontier, Some(encoded)), wrapped_idx);
                    }
                }
            }

            // send some blind progress updates
            if Instant::now().duration_since(last_heartbeat) > frontier_heartbeat {
                let frontier = frontier.get_upstream_actual();
                for i in 0..remotes_clients.len() {
                    server.send((frontier, None), i);
                }
                last_heartbeat = Instant::now();
            }

            // handle incoming
            // prev_frontier is the biggest frontier we have seen so far from this remote
            for (client, prev_frontier) in remotes_clients.iter_mut() {
                for (frontier, msg) in client.recv_all() {
                    let decoded = msg.map(|m| {
                        bincode::decode_from_slice::<T, _>(m.as_slice(), config).map(|x| x.0)
                    });
                    match decoded {
                        Some(Ok(data)) => match prev_frontier.advance_to(frontier) {
                            Ok(_) => output.send(data),
                            Err(FrontierError::DesiredLessThanActual) => {
                                event!(Level::WARN, "Received outdated message. Discarding...")
                            }
                        },
                        Some(Err(e)) => {
                            event!(Level::ERROR, "Error decoding remote message: {}", e)
                        }

                        // pure frontier update
                        None => {
                            let _ = prev_frontier.advance_to(frontier);
                        }
                    }
                }
            }
            // advance frontier to smallest one observed, or MAX if no remotes
            let this_new_frontier = remotes_clients
                .iter()
                .map(|x| x.1.get_actual())
                .min()
                .unwrap_or(Timestamp::MAX);
            frontier.advance_to(this_new_frontier).unwrap();
        };

        let operator = StandardOperator::new(logic);

        Ok(self.then(operator))
    }
}
