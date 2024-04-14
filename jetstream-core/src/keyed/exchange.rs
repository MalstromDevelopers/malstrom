//! Exchange operators for key-distribute

use indexmap::IndexMap;
use postbox::NetworkMessage;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::channels::selective_broadcast::Sender;
use crate::time::NoTime;
use crate::{NoData, NoKey, OperatorId, ShutdownMarker, WorkerId};
use crate::{
    channels::selective_broadcast::Receiver, stream::operator::OperatorContext, DataMessage,
    Message,
};

use super::distributed::{DistData, DistKey, DistTimestamp};

#[derive(Serialize, Deserialize)]
enum UpstreamExchangeMessage<K, V, T> {
    Data(DataMessage<K, V, T>),
    Epoch(T),
    /// Barrier used for asynchronous snapshotting
    AbsBarrier(usize),
    /// Information that this worker plans on shutting down
    /// See struct docstring for more information
    ShutdownMarker,
}

impl<K, V, T> UpstreamExchangeMessage<K, V, T> {
    fn to_local(self) -> Message<K, V, T> {
        match self {
            UpstreamExchangeMessage::Data(d) => Message::Data(d),
            UpstreamExchangeMessage::Epoch(t) => Message::Epoch(t),
            UpstreamExchangeMessage::AbsBarrier(b) => todo!(),
            UpstreamExchangeMessage::ShutdownMarker => Message::ShutdownMarker(ShutdownMarker::),
        }
    }
}

pub(super) fn upstream_exchange<K, V, T>(
    input: &mut Receiver<NoKey, NoData, NoTime>,
    output: &mut Sender<K, V, T>,
    ctx: &mut OperatorContext,
    // Messages from remotes are written into these
    mut remote_outputs: IndexMap<WorkerId, Sender<K, V, T>>
) where K: DistKey, V: DistData, T: DistTimestamp {
    if let Some(msg) = input.recv() {
        match msg {
            Message::Data(_) => todo!(),
            Message::Epoch(_) => todo!(),
            Message::AbsBarrier(b) => ctx.communication.broadcast(UpstreamExchangeMessage::<K, V, T>::AbsBarrier(b.get_version())).unwrap(),
            Message::Rescale(_) => todo!(),
            Message::ShutdownMarker(_) => todo!(),
            Message::Interrogate(_) => todo!(),
            Message::Collect(_) => todo!(),
            Message::Acquire(_) => todo!(),
            Message::DropKey(_) => todo!(),
        }
    }

    for rmsg in ctx.communication.recv_all::<UpstreamExchangeMessage<K, V, T>>() {
        let out = remote_outputs.entry(rmsg.sender_worker).or_insert_with(|| output.clone());
        out.send(rmsg.data.into());
    }
}

#[cfg(test)]
mod test {
    use postbox::{NetworkMessage, RecvIterator};

    use super::*;
    use crate::{
        snapshot::{Barrier, NoPersistence},
        test::OperatorTester,
        time::MaybeTime,
        MaybeData, MaybeKey, OperatorId, WorkerId,
    };

    fn loop_till_recv<KI, VI, TI, KO, VO, TO, U>(
        tester: &mut OperatorTester<KI, VI, TI, KO, VO, TO>,
    ) -> NetworkMessage<WorkerId, OperatorId, U>
    where
        KI: MaybeKey,
        VI: MaybeData,
        TI: MaybeTime,
        KO: MaybeKey,
        VO: MaybeData,
        TO: MaybeTime,
        U: Serialize + DeserializeOwned,
    {
        loop {
            tester.step();
            if let Some(x) = tester.receive_on_remote().next() {
                return x;
            }
        }
    }
    fn loop_till_recv_local<KI, VI, TI, KO, VO, TO>(
        tester: &mut OperatorTester<KI, VI, TI, KO, VO, TO>,
    ) -> Message<KO, VO, TO>
    where
        KI: MaybeKey,
        VI: MaybeData,
        TI: MaybeTime,
        KO: MaybeKey,
        VO: MaybeData,
        TO: MaybeTime,
    {
        loop {
            tester.step();
            if let Some(x) = tester.receive_on_local() {
                return x;
            }
        }
    }
    /// These messages should be broadcasted
    #[test]
    fn broadcast_messages() {
        let mut tester = OperatorTester::new_direct(upstream_exchange::<i32, String, u64>);
        tester.send_from_local(crate::Message::AbsBarrier(Barrier::new(Box::new(
            NoPersistence::default(),
        ))));
        let remote_result: NetworkMessage<WorkerId, OperatorId, UpstreamExchangeMessage<i32, String, u64>> = loop_till_recv(&mut tester);
        assert!(matches!(remote_result.data, UpstreamExchangeMessage::AbsBarrier(0)))
    }

    /// Messages received from remote should be forwarded locally downstream
    #[test]
    fn forward_from_remote() {
        let mut tester = OperatorTester::new_direct(upstream_exchange::<i32, String, NoTime>);
        let val = "Hello".to_string();
        tester.send_from_remote(UpstreamExchangeMessage::Data(DataMessage::new(42, val.to_string(), NoTime)));
        let result = loop_till_recv_local(&mut tester);
        assert!(matches!(result, Message::Data(DataMessage{key: 42, value: _, timestamp: NoTime})));
    }

}
