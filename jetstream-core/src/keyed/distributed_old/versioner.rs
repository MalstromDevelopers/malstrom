use derive_new::new;
use serde::{Deserialize, Serialize};

use crate::{
    keyed::types::{DistTimestamp, Version},
    stream::{BuildContext, Logic},
    types::{DataMessage, MaybeData, MaybeKey, Message, WorkerId},
};

#[derive(Debug, Clone, Serialize, Deserialize, new)]
pub(crate) struct VersionedMessage<V> {
    pub(super) inner: V,
    pub(super) version: Version,
    pub(super) sender: WorkerId,
}

/// Builds an operator that creates versioned messages. Note that this is
/// just a type converter, which always issues messages with version 0
pub(crate) fn versioner<K: MaybeKey, V: MaybeData, T: DistTimestamp>(
    ctx: &mut BuildContext,
) -> impl Logic<K, V, T, K, VersionedMessage<V>, T> {
    let wid = ctx.worker_id;
    move |input, output, _ctx| {
        if let Some(msg) = input.recv() {
            match msg {
                Message::Data(d) => {
                    let vmsg = VersionedMessage {
                        inner: d.value,
                        version: 0,
                        sender: wid,
                    };
                    output.send(Message::Data(DataMessage::new(d.key, vmsg, d.timestamp)))
                }
                Message::Epoch(e) => output.send(Message::Epoch(e)),
                Message::AbsBarrier(b) => output.send(Message::AbsBarrier(b)),
                Message::Rescale(r) => output.send(Message::Rescale(r)),
                Message::ShutdownMarker(s) => output.send(Message::ShutdownMarker(s)),
                Message::Interrogate(i) => output.send(Message::Interrogate(i)),
                Message::Collect(c) => output.send(Message::Collect(c)),
                Message::Acquire(a) => output.send(Message::Acquire(a)),
                Message::DropKey(d) => output.send(Message::DropKey(d)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::test_forward_system_messages;
    use crate::types::*;
    use crate::{testing::OperatorTester, types::NoData};
    /// Should attache version 0 to all data messages
    #[test]
    fn attaches_version_0() {
        let mut tester: OperatorTester<i32, NoData, i32, i32, VersionedMessage<NoData>, i32, ()> =
            OperatorTester::built_by(versioner, 0, 0, 0..1);
        tester.send_local(Message::Data(DataMessage::new(1, NoData, 2)));
        tester.step();
        let out = tester.recv_local().unwrap();
        assert!(
            matches!(
                out,
                Message::Data(DataMessage {
                    key: 1,
                    value: VersionedMessage {
                        inner: NoData,
                        version: 0,
                        sender: 0
                    },
                    timestamp: 2
                })
            ),
            "{out:?}"
        )
    }

    /// All system messages should be forwared as-is
    #[test]
    fn forwards_sys_messages() {
        let mut tester: OperatorTester<i32, NoData, i32, i32, VersionedMessage<NoData>, i32, ()> =
            OperatorTester::built_by(versioner, 0, 0, 0..1);
        test_forward_system_messages(&mut tester);
    }
}
