/// Network Exchange Operator
/// This operator can be used to send data over the network to other processing nodes
/// executing the same dataflow graph
use crate::{
    channels::selective_broadcast::Receiver,
    snapshot::{barrier::BarrierData, PersistenceBackend},
    stream::jetstream::{Data, JetStreamBuilder},
    stream::operator::StandardOperator,
    WorkerId,
};
use bincode::{config::Configuration, Decode, Encode};
use postbox::Message;
use std::{
    collections::{hash_map::DefaultHasher, HashSet},
    hash::{Hash, Hasher},
    ops::Range,
};

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

#[derive(Encode, Decode)]
pub enum ExchangeMessage<T> {
    BarrierAlign(WorkerId),
    LoadAlign(WorkerId),
    Data(T),
}

pub trait NetworkExchange<O, P> {
    /// Network exchange operator, sends data via network to remote nodes
    type ExMsg;
    fn network_exchange(self, partitioner: impl FnMut(&O, usize) -> usize + 'static) -> Self;
}

impl<O, P> NetworkExchange<O, P> for JetStreamBuilder<O, P>
where
    O: ExchangeData,
    P: PersistenceBackend,
{
    type ExMsg = ExchangeMessage<O>;

    fn network_exchange(self, mut partitioner: impl FnMut(&O, usize) -> usize + 'static) -> Self {
        // we need to align these before forwarding
        let mut received_barriers: (Option<P>, HashSet<WorkerId>) = (None, HashSet::new());
        let mut received_loads: (Option<P>, HashSet<WorkerId>) = (None, HashSet::new());

        let op = StandardOperator::new(move |input: &mut Receiver<O, P>, output, ctx| {
            match input.recv() {
                Some(BarrierData::Load(p)) => {
                    ctx.frontier
                        .advance_to(p.load::<()>(ctx.operator_id).unwrap_or_default().0);
                    let _ = received_loads.0.insert(p);
                    for w in ctx.communication.get_peers() {
                        let msg =
                            bincode::encode_to_vec(Self::ExMsg::LoadAlign(ctx.worker_id), CONFIG)
                                .expect("Encoding error");
                        ctx.communication
                            .send(w, Message::new(ctx.operator_id, msg))
                            .expect("Communication error");
                    }
                }
                Some(BarrierData::Barrier(mut b)) => {
                    // persist frontier
                    b.persist(ctx.frontier.get_actual(), &(), ctx.operator_id);
                    for w in ctx.communication.get_peers() {
                        let msg = bincode::encode_to_vec(
                            Self::ExMsg::BarrierAlign(ctx.worker_id),
                            CONFIG,
                        )
                        .expect("Encoding error");
                        ctx.communication
                            .send(w, Message::new(ctx.operator_id, msg))
                            .expect("Communication error");
                    }
                    let _ = received_barriers.0.insert(b);
                }
                Some(BarrierData::Data(d)) => {
                    // send data and progress
                    // + 1 for self
                    let idx: u32 = partitioner(&d, ctx.communication.get_peers().len() + 1)
                        .try_into()
                        .unwrap();
                    if idx == ctx.worker_id {
                        output.send(BarrierData::Data(d));
                    } else {
                        let msg = bincode::encode_to_vec(Self::ExMsg::Data(d), CONFIG)
                            .expect("Encoding error");
                        ctx.communication
                            .send(&idx, Message::new(ctx.operator_id, msg))
                            .unwrap()
                    }
                }
                None => (),
            }
            match ctx.communication.recv().unwrap() {
                Some(x) => {
                    let decode: Self::ExMsg = bincode::decode_from_slice(&x.data, CONFIG)
                        .expect("Decoding error")
                        .0;
                    match decode {
                        ExchangeMessage::Data(d) => output.send(BarrierData::Data(d)),
                        ExchangeMessage::BarrierAlign(w) => {
                            received_barriers.1.insert(w);
                        }
                        ExchangeMessage::LoadAlign(w) => {
                            received_loads.1.insert(w);
                        }
                    }
                }
                None => (),
            };
            // synchronize barriers and loads
            if received_barriers.1.len() >= ctx.communication.get_peers().len() {
                if let Some(p) = received_barriers.0.take() {
                    output.send(BarrierData::Barrier(p));
                    received_barriers.1.drain();
                }
            }
            if received_loads.1.len() >= ctx.communication.get_peers().len() {
                if let Some(p) = received_loads.0.take() {
                    output.send(BarrierData::Load(p));
                    received_loads.1.drain();
                }
            }
        });

        self.then(op)
    }
}
