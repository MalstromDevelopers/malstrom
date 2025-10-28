//! A basic example which runs a no-op dataflow
use malstrom::channels::operator_io::Output;
use malstrom::operators::*;
use malstrom::runtime::SingleThreadRuntime;
use malstrom::sinks::{StatelessSink, StdOutSink};
use malstrom::snapshot::NoPersistence;
use malstrom::sources::{SingleIteratorSource, StatelessSource};
use malstrom::types::Kvt;

use malstrom::types::{Data, DataMessage, MaybeKey, Message, Timestamp};
use malstrom::worker::StreamProvider;

// #region custom_impl
struct CustomFlatten;
// #region impl_head
impl<In, T> StatelessLogic<In, T> for CustomFlatten
where
    In: Kvt,
    T: Data,
    <In as Kvt>::Value: IntoIterator<Item = T>,
{
    async fn on_data(
        &mut self,
        msg: DataMessage<In>,
        output: &mut Output<(In::Key, T, In::Timestamp)>,
    ) {
        for x in msg.value {
            output.send(Message::Data(DataMessage::new(
                msg.key.clone(),
                x,
                msg.timestamp.clone(),
            )))
        }
    }
}
// #endregion custom_impl

// #region usage
fn main() {
    SingleThreadRuntime::builder()
        .persistence(NoPersistence)
        .build(build_dataflow)
        .execute()
        .unwrap()
}

fn build_dataflow(provider: &mut dyn StreamProvider) -> () {
    let data = [vec![1, 2, 3, 4], vec![5, 6, 7], vec![8, 9, 10]];
    provider
        .new_stream()
        .source(
            "iter-source",
            StatelessSource::new(SingleIteratorSource::new(data)),
        )
        .stateless_op("flatten", CustomFlatten)
        .sink("stdout", StatelessSink::new(StdOutSink));
}
// #endregion usage
