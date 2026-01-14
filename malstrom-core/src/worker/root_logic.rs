use tokio::sync::mpsc;

use crate::{
    channels::operator_io::{Input, Output},
    snapshot::{Barrier, PersistenceClient},
    stream::Logic,
    types::*,
    worker::sys_message::SysMessage,
};

pub(super) struct RootLogic<P>(mpsc::Receiver<SysMessage<P>>);
impl<P> RootLogic<P> {
    pub fn new(receiver: mpsc::Receiver<SysMessage<P>>) -> Self {
        Self(receiver)
    }
}

impl<P: PersistenceClient> Logic<(), ()> for RootLogic<P> {
    async fn apply(
        &mut self,
        input: &mut Input<()>,
        output: &mut Output<()>,
        ctx: &mut crate::stream::OperatorContext,
    ) {
        while let Some(sys_msg) = self.0.recv().await {
            match sys_msg {
                SysMessage::Snapshot { client, callback } => {
                    let barrier = Barrier::new(Box::new(client), callback);
                    output.send(Message::AbsBarrier(barrier)).await;
                }
                SysMessage::Reconfigure {
                    new_set,
                    new_version,
                    callback,
                } => {
                    let reconfig = RescaleMessage::new(new_set, new_version, callback);
                    output.send(Message::Rescale(reconfig)).await;
                }
                SysMessage::Suspend { callback } => {
                    let suspend = SuspendMarker::new(callback);
                    output.send(Message::SuspendMarker(suspend)).await;
                }
            }
        }
    }
}
