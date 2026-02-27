use std::{marker::PhantomData, rc::Rc};

use futures::{FutureExt, stream::FuturesUnordered};
use indexmap::{IndexMap, IndexSet};

use crate::{
    channels::{alignment::AlignmentGroup, operator_io::{Input, Output}, spsc},
    keyed::distributed::{Acquire, Version, WorkerPartitioner, wire_message::{VersionedMessage, WireMessage}},
    runtime::communication::{Distributable, OperatorCommReceiver},
    snapshot::{PersistenceBackend, SnapshotBarrier},
    stream::Logic,
    types::{Barrier, DataMessage, Kvt, Message, NoKey, SuspendMarker, WorkerId},
};


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
    ica_phase: ICAPhase,
    own_version: Version

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

        tokio::select! {
            local_msg = local_recv => {
                match local_msg {
                    Message::Data(data_message) => todo!(),
                    Message::Epoch(_) => todo!(),
                    Message::AbsBarrier(barrier) => self.local_barrier = Some(barrier),
                    Message::Rescale(rescale_message) => todo!(),
                    Message::ReconfigComplete(_) => todo!(),
                    Message::Interrogate(_) => todo!(),
                    Message::Collect(_) => todo!(),
                    Message::Acquire(_) => todo!(),
                }
            }
            remote_msg = self.remote_recvs.recv() => {

            }
        }
        
    }
}