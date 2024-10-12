use jetstream::keyed::partitioners::partition_index;
use jetstream::operators::*;
use jetstream::runtime::threaded::MultiThreadRuntime;
use jetstream::runtime::{RuntimeFlavor, WorkerBuilder};
use jetstream::snapshot::NoPersistence;
use jetstream::sources::SingleIteratorSource;

fn main() {
    let rt = MultiThreadRuntime::new(2, build_dataflow);
    rt.execute();
}

fn build_dataflow<R: RuntimeFlavor>(flavor: R) -> WorkerBuilder<R, NoPersistence> {
    let mut worker = WorkerBuilder::new(flavor);

    let stream = worker.new_stream();

    let (ontime, _late) = stream
        .source(SingleIteratorSource::new(0..100))
        .key_distribute(|msg| msg.value % 2, partition_index)
        .generate_epochs(|msg, _| (msg.timestamp % 10 == 0).then_some(msg.timestamp));

    ontime
        .tumbling_window(10, |_| Some(vec![]), |msg, state| state.push(msg.value))
        .inspect(|msg, ctx| println!("{msg:?} @ {}", ctx.worker_id))
        .finish();

    worker
}
