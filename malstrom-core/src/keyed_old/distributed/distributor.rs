use std::{marker::PhantomData, rc::Rc};

use futures::{FutureExt, stream::FuturesUnordered};
use indexmap::{IndexMap, IndexSet};

use crate::{
    channels::{alignment::AlignmentGroup, operator_io::{Input, Output}, spsc},
    keyed::distributed::{Acquire, Version, WorkerPartitioner, routers::MessageRouter, wire_message::{VersionedMessage, WireMessage}},
    runtime::communication::{Distributable, OperatorCommReceiver},
    snapshot::{PersistenceBackend, SnapshotBarrier},
    stream::Logic,
    types::{Barrier, DataMessage, Kvt, Message, NoKey, SuspendMarker, WorkerId},
};

/// Represents the different phases of the ICA (Interrogate-Collect-Acquire) reconfiguration process
enum ICAPhase {
    Normal,
    Interrogating,
    Collecting,
    Finished,
}

struct CollectBuffer<M>;

impl<M> CollectBuffer<M> {
    fn append(&mut self, msg: M) {
        todo!()
    }
}

struct Distributor<M> where
// TODO: simplify these trait bounds
    M: Kvt + Distributable,
    M::Key: Distributable,
    M::Value: Distributable,
    M::Timestamp: Distributable {

    /// Remote receivers
    remote_recvs: AlignmentGroup<OperatorCommReceiver<WireMessage<M>>, fn(WireMessage<M>) -> bool>,
    /// a local barrier waiting for alignment
    local_barrier: Option<Barrier>,
    own_version: Version,
    router: Box<dyn MessageRouter<M::Key>>,
    /// currently collected key + buffer if any
    current_collect: Option<(M::Key, CollectBuffer)>

}

impl<M> Logic<M, M> for Distributor<M> where     M: Kvt + Distributable,
    M::Key: Distributable,
    M::Value: Distributable,
    M::Timestamp: Distributable {
        async fn apply(
        &mut self,
        input: &mut Input<M>,
        output: &mut Output<M>,
        ctx: &mut crate::stream::OperatorContext,
    ) {
        /// select over
        /// - local input message
        /// - remote input message
        /// - ICA reconfig progress
        
        let local_recv = if self.local_barrier.is_none() {
            input.recv().boxed_local()
        } else {
            std::future::pending().boxed_local()
        };
        
    }
}

impl<M> Distributor<M> where M: Kvt + Distributable,
    M::Key: Distributable,
    M::Value: Distributable,
    M::Timestamp: Distributable {

    fn handle_local_msg(&mut self, msg: Message<M>) {
        match msg {
            Message::Data(data_message) => {
                /// buffer message if key is currently getting collected
                if let Some((key, buffer)) = self.current_collect.as_mut() {
                    if key == data_message.key {
                        buffer.append(msg);
                        return;
                    }
                };
                let target = self.router.route_message(msg.key, self.worker_id);
            },
            Message::Epoch(_) => todo!(),
            Message::AbsBarrier(barrier) => todo!(),
            Message::Rescale(rescale_message) => todo!(),
            Message::ReconfigComplete(_) => todo!(),
            Message::Interrogate(_) => todo!(),
            Message::Collect(_) => todo!(),
            Message::Acquire(_) => todo!(),
        }
    }

}