//! A basic example which runs a no-op dataflow
use indexmap::IndexMap;
use malstrom::channels::operator_io::Output;
use malstrom::operators::*;
use malstrom::runtime::SingleThreadRuntime;
use malstrom::sinks::{StatelessSink, StdOutSink};
use malstrom::snapshot::NoPersistence;
use malstrom::sources::{SingleIteratorSource, StatelessSource};
use malstrom::types::{Data, DataMessage, Key, Kvt, Message, Timestamp};
use malstrom::worker::StreamProvider;

// #region custom_impl
struct CustomBatching(usize);
// #region impl_head
impl<Msg> StatefulLogic<Msg, Vec<Msg::Value>, Vec<Msg::Value>> for CustomBatching
where
    Msg: Kvt,
    Msg::Timestamp: Timestamp,
{
    fn on_data(
        &mut self,
        msg: DataMessage<Msg>,
        mut key_state: Vec<Msg::Value>,
        output: &mut Output<(Msg::Key, Vec<Msg::Value>, Msg::Timestamp)>,
    ) -> Option<Vec<Msg::Value>> {
        key_state.push(msg.value);
        if key_state.len() == self.0 {
            output.send(Message::Data(DataMessage::new(
                msg.key,
                key_state,
                msg.timestamp,
            )));
            None
        } else {
            Some(key_state)
        }
    }
    // #endregion custom_impl

    // #region on_epoch
    fn on_epoch(
        &mut self,
        epoch: &Msg::Timestamp,
        state: &mut IndexMap<Msg::Key, Vec<Msg::Value>>,
        output: &mut Output<(Msg::Key, Vec<Msg::Value>, Msg::Timestamp)>,
    ) {
        if *epoch == Msg::Timestamp::MAX {
            // emit all states
            for (k, v) in state.drain(..) {
                output.send(Message::Data(DataMessage::new(k, v, Msg::Timestamp::MAX)));
            }
        }
    }
    // #endregion on_epoch
}

// #region usage
fn main() {
    SingleThreadRuntime::builder()
        .persistence(NoPersistence)
        .build(build_dataflow)
        .execute()
        .unwrap()
}

fn build_dataflow(provider: &mut dyn StreamProvider) -> () {
    let data = 0..=100;
    provider
        .new_stream()
        .source(
            "iter-source",
            StatelessSource::new(SingleIteratorSource::new(data)),
        )
        .key_local("key-one", |_| ()) // only keyed streams can use state
        .stateful_op("batches", CustomBatching(5))
        .sink("stdout", StatelessSink::new(StdOutSink));
}
// #endregion usage
