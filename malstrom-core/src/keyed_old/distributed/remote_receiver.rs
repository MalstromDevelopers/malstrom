use std::rc::Rc;

use futures::{FutureExt, Stream, stream::FuturesUnordered};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::{
    channels::{operator_io::{Input, Output}, spsc},
    keyed::distributed::{Acquire, Version, wire_message::WireMessage},
    runtime::communication::{Distributable, OperatorCommReceiver},
    snapshot::{PersistenceBackend, SnapshotBarrier},
    stream::Logic,
    types::{DataMessage, Kvt, Message, NoKey, SuspendMarker, WorkerId},
};

/// Receives messages from other workers
pub(super) struct RemoteReceiver<M> where
    M: Kvt + Distributable,
    M::Key: Distributable,
    M::Value: Distributable,
    M::Timestamp: Distributable {
    /// connection to other worker
    receiver: OperatorCommReceiver<WireMessage<M>>,
    /// ID of connected worker
    connected_worker: WorkerId
}

impl<M> Stream for RemoteReceiver<M> where
    M: Kvt + Distributable,
    M::Key: Distributable,
    M::Value: Distributable,
    M::Timestamp: Distributable {
        type Item = WireMessage<M>;
    
        fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        
        self.receiver.receive().poll_unpin(cx)
    }
}


impl<M> SingleRemoteReceiver<M>
where
    M: Kvt + Distributable,
    M::Key: Distributable,
    M::Value: Distributable,
    M::Timestamp: Distributable,
{
    /// Start receiving messages from the remote worker
    async fn start(mut self) -> tokio::task::JoinHandle<()> {
        loop {
            let msg: WireMessage<M> = self.client.receive().await;
            match msg {
                WireMessage::Data(m) => {
                    let (data_message, version) = (m.content, m.version);
                    let versioned_msg = ICAMessage::Data((data_message, version));
                    self.local_output.send(versioned_msg).await
                }
                WireMessage::Epoch(e) => self.local_output.send(ICAMessage::Epoch(e)).await,
                WireMessage::SnapshotBarrier => {
                    // wait for local alignment
                    self.barrier_recv.recv().await;
                    self.local_output.send(ICAMessage::Barrier).await
                },
                WireMessage::SuspendMarker => {
                    self.suspend_recv.recv().await;
                    self.local_output.send(ICAMessage::Suspend).await
                },
                WireMessage::Acquire(wire_acquire) => {
                    let acquire = Acquire::new(wire_acquire.key, wire_acquire.collection);
                },
                WireMessage::Upgrade(version) => {
                    let msg = ICAMessage::Upgrade((self.connected_worker, version));
                    self.local_output.send(msg).await;
                },
                WireMessage::AckUpgrade(version) => {
                    let msg = ICAMessage::AckUpgrade((self.connected_worker, version));
                    self.local_output.send(msg).await;
                },
            }
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
enum ICAMessage<M: Kvt>{
    /// Normal data with version attached
    Data((DataMessage<M>, Version)),
    Epoch(M::Timestamp),
    // Another worker is telling us it has upgraded
    Upgrade((WorkerId, Version)),
    /// Another worker has acknowledged our upgrade
    AckUpgrade((WorkerId, Version)),
    Barrier,
    Suspend,
}