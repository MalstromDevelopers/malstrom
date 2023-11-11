use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    ops::Range,
};

use url::Url;

use crate::{
    channels::selective_broadcast,
    frontier::FrontierHandle,
    stream::jetstream::{Data, JetStreamBuilder},
    stream::operator::StandardOperator,
};
use bincode::{Decode, Encode};
use thiserror::Error;
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

use nng;
#[derive(Error, Debug)]
pub enum ExchangeError {
    #[error("Error opening socket: {0}")]
    OpenSocket(nng::Error),

    #[error("Error binding socket: {0}")]
    BindSocket(nng::Error),

    #[error("Error connecting to remote: {0}")]
    ConnectRemote(nng::Error),

    #[error("Error sending to remote: {1}")]
    SendRemote(nng::Message, nng::Error),

    #[error("Error receiving message: {0}")]
    ReceiveMessage(nng::Error),
}

struct Remote(nng::Socket);
impl Remote {
    fn connect(remote_addr: &str) -> Result<Self, ExchangeError> {
        let client =
            nng::Socket::new(nng::Protocol::Req0).map_err(|e| ExchangeError::OpenSocket(e))?;
        client
            .dial(remote_addr)
            .map_err(|e| ExchangeError::ConnectRemote(e))?;
        Ok(Self(client))
    }

    fn send(&self, value: &[u8]) -> Result<(), ExchangeError> {
        self.0
            .send(value)
            .map_err(|(msg, e)| ExchangeError::SendRemote(msg, e))?;
        Ok(())
    }
}

struct Listener(nng::Socket);
impl Listener {
    fn new(bind_addr: &str) -> Result<Self, ExchangeError> {
        let server =
            nng::Socket::new(nng::Protocol::Rep0).map_err(|e| ExchangeError::OpenSocket(e))?;
        server
            .listen(bind_addr)
            .map_err(|e| ExchangeError::BindSocket(e))?;
        Ok(Self(server))
    }

    fn recv(&self) -> Result<Option<nng::Message>, ExchangeError> {
        match self.0.try_recv() {
            Ok(msg) => Ok(Some(msg)),
            Err(nng::Error::TryAgain) => Ok(None),
            Err(e) => Err(ExchangeError::ReceiveMessage(e)),
        }
    }
}

pub trait NetworkExchange<T> {
    /// Create a network exchange given the addresses of the remote exchanges
    /// and a partitioning function.
    /// This operator will take care of establishing and maintaining channels with
    /// the remotes.
    ///
    /// The partitioning function will be given a reference to every message `T` and the
    /// number of remotes + 1 (for local) and should return all indices which shall receiver
    /// `T`. If multiple receivers are specified, `T` will be cloned.
    /// Index `0` always refers to the local stream.
    /// Out of range indices are wrapped to avoid panics.
    /// Note that this will duplicate messages, if the same index is given multiple times
    ///
    /// This function returns an error if communication with the remotes could not be established.
    ///
    /// Panic:
    /// This function will panic on
    /// - encoding errors
    /// - networking errors
    fn network_exchange(
        self,
        local_sock: Url,
        remotes: &[Url],
        partitioner: impl FnMut(&T, usize) -> Vec<usize> + 'static,
    ) -> Result<JetStreamBuilder<T>, ExchangeError>;
}

impl<T> NetworkExchange<T> for JetStreamBuilder<T>
where
    T: ExchangeData,
{
    #[instrument(skip_all)]
    fn network_exchange(
        self,
        local_addr: Url,
        remote_addresses: &[Url],
        mut partitioner: impl FnMut(&T, usize) -> Vec<usize> + 'static,
    ) -> Result<JetStreamBuilder<T>, ExchangeError> {
        // TODO: Make connection timeout configurable

        let listener = Listener::new(local_addr.as_str())?;
        let mut remotes = Vec::with_capacity(remote_addresses.len());
        for r in remote_addresses {
            let connection = Remote::connect(r.as_str())?;
            remotes.push(connection);
        }

        let config = bincode::config::standard();

        // Operator logic
        let logic = move |input: &mut selective_broadcast::Receiver<T>,
                          output: &mut selective_broadcast::Sender<T>,
                          frontier: &mut FrontierHandle| {
            // send messages from upstream
            if let Some(msg) = input.recv() {
                let indices = partitioner(&msg, remotes.len() + 1);
                let indices_cnt = indices.len();

                for (idx, msg) in indices
                    .into_iter()
                    .zip(itertools::repeat_n(msg, indices_cnt))
                {
                    if (idx == 0usize) || remotes.is_empty() {
                        output.send(msg);
                        continue;
                    }
                    // deduct 1 because idx 0 is the local
                    let wrapped_idx = (idx - 1) % remotes.len();
                    // SAFETY: We can unwrap, because we wrapped the index
                    // and asserted that remotes is not empty in the if
                    let target = remotes.get(wrapped_idx).unwrap();
                    let encoded = bincode::encode_to_vec((msg, frontier.get_actual()), config)
                        .expect("Error encoding value");
                    target.send(&encoded).expect("Error sending value");
                }
            }

            // handle incoming
            if let Some(msg) = listener.recv().expect("Error on exchange listener") {
                let decoded = bincode::decode_from_slice::<(T, u64), _>(msg.as_slice(), config);
                match decoded {
                    Ok(val) => {
                        let (data, msg_frontier) = val.0;
                        if msg_frontier <= frontier.get_actual() {
                            // TODO: this is probably not desirable
                            event!(Level::WARN, "Received outdated message. Discarding...");
                        } else {
                            let _ = frontier.advance_to(msg_frontier);
                        }
                        output.send(data);
                    }
                    Err(e) => event!(Level::ERROR, "Error decoding remote message: {}", e),
                }
            }
        };

        let operator = StandardOperator::new(logic);

        Ok(self.then(operator))
    }
}
