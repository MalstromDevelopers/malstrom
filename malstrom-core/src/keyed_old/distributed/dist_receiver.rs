use crate::channels::operator_io::Output;
use crate::channels::recv_trait::Receiver as _;
use crate::keyed::distributed::wire_message::VersionedDataMessage;
use crate::stream::Logic;
use crate::types::Key;
use crate::{
    channels::{alignment::AlignmentGroup, operator_io::Input},
    keyed::distributed::wire_message::{Version, VersionedMessage, WireMessage},
    runtime::communication::{Distributable, OperatorCommReceiver},
    types::{Barrier, Kvt, Message, WorkerId},
};

type Remotes<M> = AlignmentGroup<OperatorCommReceiver<WireMessage<M>>, fn(WireMessage<M>) -> bool>;

struct DistReceiver<M>
where
    // TODO: simplify these trait bounds
    M: Kvt + Distributable,
    M::Key: Distributable,
    M::Value: Distributable,
    M::Timestamp: Distributable,
{
    /// Remote receivers
    remote_recvs: Remotes<M>,
    barrier: BarrierAligner,
    /// our own config version
    version: Version,
    /// our own workerId
    workerid: WorkerId
}


impl<M> Logic<M, VersionedMessage<M>> for DistReceiver<M> where 
    M: Kvt + Distributable,
    M::Key: Distributable,
    M::Value: Distributable,
    M::Timestamp: Distributable {
        async fn apply(
        &mut self,
        input: &mut Input<M>,
        output: &mut Output<VersionedMessage<M>>,
        ctx: &mut crate::stream::OperatorContext,
    ) {
        match self.barrier.status() {
            AlignStatus::NotWaiting => todo!(),
            AlignStatus::WaitingForLocal => {
                let msg = DistReceiver::recv_local(input, self.version, self.workerid).await;
                match msg {
                    Message::AbsBarrier(b) => {
                        if let Some(barrier) = self.barrier.give_local(b) {
                            output.send(Message::AbsBarrier(barrier)).await
                        }
                    }
                    Message::ReconfigComplete() => todo!(),
                    Message::Rescale() => todo!(),
                    x => output.send(x).await
                }
            },
            AlignStatus::WatingForRemote => {
                let msg = DistReceiver::recv_remote(&mut self.remote_recvs).await;
                
            },
        };        
    }
}


impl<M> DistReceiver<M>
where
    // TODO: simplify these trait bounds
    M: Kvt + Distributable,
    M::Key: Distributable,
    M::Value: Distributable,
    M::Timestamp: Distributable,
{
    
    async fn handle_local_msg(&mut self, msg: VersionedMessage<M>, output: &mut Output<M>) {
        match msg {
            Message::AbsBarrier(b) => {
                if let Some(barrier) = self.barrier.give_local(b) {
                    output.send(Message::AbsBarrier(barrier)).await
                }
            }
            Message::ReconfigComplete() => todo!(),
            Message::Rescale() => todo!(),
            x => output.send(x).await
        }
    }
    
    async fn handle_remote_msg(&mut self, msg: RemoteRecv<M>, output: &mut Output<M>) {
        match msg {
            RemoteRecv::Message(message) => output.send(message).await,
            RemoteRecv::Barrier => {
                if let Some(barrier) = self.barrier.give_remote() {
                   output.send(Message::AbsBarrier(barrier)).await
                }
            },
        }
    }
    
    async fn recv_local(input: &mut Input<M>, version: Version, worker: WorkerId) -> VersionedMessage<M> {
        let local_msg = input.recv().await;
        to_versioned_message(local_msg, version, worker)
    }

    async fn recv_remote(remotes: &mut Remotes<M>) -> RemoteRecv<M> {
        let msg: WireMessage<M> = remotes.recv().await;
        match msg {
            WireMessage::Data(msg) => RemoteRecv::Message(VersionedMessage::Data(msg)),
            WireMessage::Epoch(e) => RemoteRecv::Message(VersionedMessage::Epoch(e)),
            WireMessage::SnapshotBarrier => RemoteRecv::Barrier,
            WireMessage::Acquire(wire_acquire) => RemoteRecv::Message(VersionedMessage::Acquire(todo!())),
        }
    }
}

enum BarrierAlignment {
    /// We have a local barrier and are waiting for the remote ones
    Local(Barrier),
    /// We have the remote barriers, but still need the local one
    Remote,
}

enum RemoteRecv<M: Kvt> {
    Message(VersionedMessage<M>),
    Barrier
}


fn to_versioned_message<M: Kvt>(msg: Message<M>, version: Version, sender: WorkerId) -> VersionedMessage<M> {
    match msg {
        Message::Data(msg) => {
            let data_msg = VersionedDataMessage::new(msg.key, (msg.value, version, sender), msg.timestamp);
            VersionedMessage::Data(data_msg)
        },
        Message::Epoch(e) => VersionedMessage::Epoch(e),
        Message::AbsBarrier(x) => VersionedMessage::AbsBarrier(x),
        Message::Rescale(x) => VersionedMessage::Rescale(x),
        Message::ReconfigComplete(x) => VersionedMessage::ReconfigComplete(x),
        Message::Interrogate(x) => VersionedMessage::Interrogate(x),
        Message::Collect(x) => VersionedMessage::Collect(x),
        Message::Acquire(x) => VersionedMessage::Acquire(x),
    }
}

struct BarrierAligner {
    /// local barrier if we have received it yet
    local_barrier: Option<Barrier>,
    /// whether or not we have received the remote barrier yet
    got_remote: bool
}

impl BarrierAligner {
    fn give_local(&mut self, barrier: Barrier) -> Option<Barrier> {
        let prev_local = self.local_barrier.insert(barrier);
        debug_assert!(prev_local.is_none(), "Received multiple local barriers in succession");
        
        self.try_take()
    }
    
    fn give_remote(&mut self) -> Option<Barrier> {
        debug_assert!(!self.got_remote, "Got multiple remote barriers in succession");
        self.got_remote = true;
        self.try_take()
    }
    
    fn try_take(&mut self) -> Option<Barrier> {
        match self.local_barrier.take_if(|| self.got_remote) {
            Some(x) => {self.got_remote = false; Some(x)},
            None => None,
        }
    }
    
    fn status(&self) -> AlignStatus {
        match (self.local_barrier.is_some(), self.got_remote) {
            (false, false) => AlignStatus::NotWaiting,
            (true, false) => AlignStatus::WatingForRemote,
            (false, true) => AlignStatus::WaitingForLocal,
            (true, true) => unreachable!("Barrier should have been emitted"),
        }
    }
}

enum AlignStatus {
    /// Neither waiting for a remote nor a local barrier
    NotWaiting,
    /// Waiting for a local barrier to arrive, already got remote
    WaitingForLocal,
    /// Waiting for a remote barrier to arrive, already got local
    WatingForRemote
}