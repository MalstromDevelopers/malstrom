//! A basic example which runs a no-op dataflow
use indexmap::IndexMap;
use malstrom::channels::operator_io::Output;
use malstrom::operators::*;
use malstrom::runtime::SingleThreadRuntime;
use malstrom::sinks::{StatelessSink, StdOutSink};
use malstrom::snapshot::NoPersistence;
use malstrom::sources::{SingleIteratorSource, StatelessSource};
use malstrom::types::{Data, DataMessage, Key, Message, Timestamp};
use malstrom::worker::StreamProvider;

// #region custom_impl
struct CustomBatching(usize);
// #region impl_head
impl<K, V, T> StatefulLogic<K, V, T, Vec<V>, Vec<V>> for CustomBatching
where
    K: Key,
    T: Timestamp,
    V: Data, // #endregion impl_head
{
    fn on_data(
        &mut self,
        msg: DataMessage<K, V, T>,
        mut key_state: Vec<V>,
        output: &mut Output<K, Vec<V>, T>,
    ) -> Option<Vec<V>> {
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
        epoch: &T,
        state: &mut IndexMap<K, Vec<V>>,
        output: &mut Output<K, Vec<V>, T>,
    ) {
        if *epoch == T::MAX {
            // emit all states
            for (k, v) in state.drain(..) {
                output.send(Message::Data(DataMessage::new(k, v, T::MAX)));
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
        .stateful_op("batches", CustomBatching(5))
        .sink("stdout", StatelessSink::new(StdOutSink));
}
// #endregion usage
