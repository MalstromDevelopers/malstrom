use std::{hash::Hash, marker::PhantomData};

use indexmap::IndexSet;
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    channels::operator_io::{Input, Output},
    keyed::{Distribute as _, WorkerPartitioner, distributed::distributor::DistributorBuilder},
    stream::{BuildContext, Logic, LogicBuilder, Malstrom, Operator, StreamBuilder},
    types::{DataMessage, Key, Kvt, MaybeKey, Message, WorkerId, distributable::Distributable},
};

use super::KeyLocal;

pub trait WorkerBroadcast<M: Kvt> {
    fn worker_broadcast(self, name: &str) -> StreamBuilder<(WorkerId, (M::Key, M::Value), M::Timestamp)>;
}

impl<M> WorkerBroadcast<M> for StreamBuilder<M>
where
    M: Kvt,
    M::Key: Serialize + DeserializeOwned,
    M::Value: Serialize + DeserializeOwned,
    M::Timestamp: Serialize + DeserializeOwned,
{
    fn worker_broadcast(self, name: &str) -> StreamBuilder<(WorkerId, (M::Key, M::Value), M::Timestamp)> {
        self.then(Operator::built_by(
            format!("{name}-broadcast"),
            KeyByWid::default(),
        ))
        .distribute(format!("{name}-distribute"), wid_select)

    }
}

#[derive(Default)]
struct KeyByWid {
    workers: IndexSet<WorkerId>,
}

impl<M, N> LogicBuilder<M, N> for KeyByWid
where
    M: Kvt,
    N: Kvt<Key = WorkerId, Value = (M::Key, M::Value), Timestamp = M::Timestamp>,
{
    type Logic = Self;

    async fn build(self, ctx: &mut BuildContext) -> Self::Logic {
        let workers = ctx.get_worker_ids().clone();
        Self { workers }
    }
}

impl<M, N> Logic<M, N> for KeyByWid
where
    M: Kvt,
    N: Kvt<Key = WorkerId, Value = (M::Key, M::Value), Timestamp = M::Timestamp>,
{
    async fn apply(
        &mut self,
        input: &mut Input<M>,
        output: &mut Output<N>,
        ctx: &mut crate::stream::OperatorContext,
    ) {
        match input.recv().await {
            Message::Data(d) => {
                for wid in self.workers.clone() {
                    let value = (d.key.clone(), d.value.clone());
                    let new_msg = DataMessage::new(wid, value, d.timestamp.clone());
                    output.send(Message::Data(new_msg)).await
                }
            }
            // key messages may not cross key region boundaries
            Message::Interrogate(_) => (),
            Message::Collect(_) => (),
            Message::Acquire(_) => (),
            // necessary because it is a different generic type now
            Message::AbsBarrier(b) => output.send(Message::AbsBarrier(b)).await,
            Message::Rescale(x) => {
                self.workers = self
                    .workers
                    .intersection(x.get_all_workers())
                    .map(|x| *x)
                    .collect();
                output.send(Message::Rescale(x)).await
            }
            Message::Epoch(x) => output.send(Message::Epoch(x)).await,
            Message::ReconfigComplete(x) => {
                self.workers = x.get_new_worker_set().clone();
                output.send(Message::ReconfigComplete(x)).await
            }
        }
    }
}

struct KeyByWidUnwrapper;

impl<M, N> Logic<M, N> for KeyByWidUnwrapper
where
    M: Kvt<Value = (N::Key, N::Value), Timestamp = N::Timestamp>,
    N: Kvt,
{
    async fn apply(
        &mut self,
        input: &mut Input<M>,
        output: &mut Output<N>,
        ctx: &mut crate::stream::OperatorContext,
    ) {
        match input.recv().await {
            Message::Data(d) => {
                let new_msg = DataMessage::new(d.value.0, d.value.1, d.timestamp);
                output.send(Message::Data(new_msg)).await
            }
            // key messages may not cross key region boundaries
            Message::Interrogate(_) => (),
            Message::Collect(_) => (),
            Message::Acquire(_) => (),
            // necessary because it is a different generic type now
            Message::AbsBarrier(b) => output.send(Message::AbsBarrier(b)).await,
            Message::Rescale(x) => output.send(Message::Rescale(x)).await,
            Message::Epoch(x) => output.send(Message::Epoch(x)).await,
            Message::ReconfigComplete(x) => output.send(Message::ReconfigComplete(x)).await,
        }
    }
}

fn wid_select(wid: &WorkerId, _workers: &IndexSet<WorkerId>) -> WorkerId {
    *wid
}
