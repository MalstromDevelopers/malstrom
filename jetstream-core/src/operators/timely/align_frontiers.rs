use indexmap::IndexMap;

use crate::channels::selective_broadcast::{Receiver, Sender};
use crate::keyed::distributed::DistTimestamp;
use crate::operators::stateless_op::StatelessOp;
use crate::runtime::communication::broadcast;
use crate::runtime::CommunicationClient;
use crate::stream::jetstream::JetStreamBuilder;

use crate::stream::operator::{
    BuildContext, Logic, OperatorBuilder, OperatorContext, StandardOperator,
};
use crate::time::{NoTime, Timestamp};
use crate::{Data, DataMessage, MaybeData, MaybeKey, Message, WorkerId};

pub trait AlignFrontiers<K, V, T> {
    fn align_frontiers(self, max_diff: T) -> JetStreamBuilder<K, V, T>;
}

impl<K, V, T> AlignFrontiers<K, V, T> for JetStreamBuilder<K, V, T>
where
    K: MaybeKey,
    V: MaybeData,
    T: DistTimestamp + Timestamp + std::ops::Sub<Output = T>,
{
    fn align_frontiers(self, max_diff: T) -> JetStreamBuilder<K, V, T> {
        let op = OperatorBuilder::built_by(|ctx| build_aligner(ctx, max_diff));
        self.then(op)
    }
}

fn build_aligner<K, V, T>(build_ctx: &mut BuildContext, max_diff: T) -> impl Logic<K, V, T, K, V, T>
where
    K: MaybeKey,
    V: MaybeData,
    T: DistTimestamp + Timestamp + std::ops::Sub<Output = T>,
{
    let mut this_frontier: Option<T> = None;
    let mut other_frontiers: IndexMap<WorkerId, Option<T>> = build_ctx
        .get_worker_ids()
        .filter(|wid| *wid != build_ctx.worker_id)
        .map(|wid| (wid, None))
        .collect();

    let this_id = build_ctx.worker_id;
    let clients: IndexMap<WorkerId, CommunicationClient<T>> = build_ctx
        .get_worker_ids()
        .filter(|wid| *wid != this_id)
        .map(|wid| {
            (
                wid,
                build_ctx.create_communication_client(wid, build_ctx.operator_id),
            )
        })
        .collect();

    move |input, output, _| {
        // receive the epochs from other workers
        for (wid, client) in clients.iter() {
            // last msg will always be highest epoch
            if let Some(x) = client.recv_all().last() {
                other_frontiers.get_mut(wid).unwrap().replace(x);
            }
        }

        this_frontier = match this_frontier.take() {
            Some(this) => {
                let others_min = other_frontiers.values().filter_map(|x| x.as_ref()).min();
                let must_wait = match others_min {
                    Some(x) => *x < this.clone().sub(max_diff.clone()),
                    None => false
                };
                if must_wait {
                    // simpy do not advance the input
                    Some(this)
                } else {
                    advance_input(input, output, &clients)
                }
            }
            None => {
                input.recv().map(|x| output.send(x));
                None
            }
        }
    }
}

fn advance_input<K: MaybeKey, V: MaybeData, T: DistTimestamp + Timestamp>(
    input: &mut Receiver<K, V, T>,
    output: &mut Sender<K, V, T>,
    clients: &IndexMap<WorkerId, CommunicationClient<T>>,
) -> Option<T> {
    let msg = input.recv()?;
    match msg {
        Message::Epoch(e) => {
            broadcast(clients.values(), e.clone());
            output.send(Message::Epoch(e.clone()));
            Some(e)
        },
        x => {output.send(x); None}

    }
}
