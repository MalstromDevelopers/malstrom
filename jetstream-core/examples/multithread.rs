//! This is a very simple example of a stream running on multiple threads
//! via keying and with a stateful computation happening in these threads.
//! Every message will be routed to the correct thread based on its key

use jetstream::keyed::partitioners::index_select;
use jetstream::operators::*;
use jetstream::runtime::threaded::MultiThreadRuntime;
use jetstream::runtime::{no_snapshots, RuntimeFlavor, WorkerBuilder};
use jetstream::snapshot::NoPersistence;
use jetstream::sources::SingleIteratorSource;

fn main() {
    // Create a runtime for multithreaded execution
    let rt = MultiThreadRuntime::new(8, build_dataflow);
    // execute on all threads and return an error if any of them paniced
    rt.execute().unwrap();
}

/// A Builder function for the dataflow logic executed in each runtime thread.
///
/// The actual runtime implementation is specified as a generic, this allows us
/// to reuse the same builder function with a different runtime. For example
/// we can use [MultiThreadRuntime] locally and
/// [jetstream::runtime::kubernetes::KubernetesRuntime](KubernetesRuntime) in
/// production.
fn build_dataflow<R: RuntimeFlavor>(flavor: R) -> WorkerBuilder<R, NoPersistence> {
    let mut worker = WorkerBuilder::new(flavor, no_snapshots, NoPersistence::default());

    let stream = worker.new_stream();

    // we can discard the late message stream here, since our source is strictly ordered
    let (ontime, _late) = stream
        .source(SingleIteratorSource::new(0..100))
        // key_distribute creates a keyed stream and distributes messages by key
        // to the different workers
        .key_distribute(|msg| msg.value % 10, index_select)
        // we generate an epoch every 10th message to progress time
        .generate_epochs(|msg, _| (msg.timestamp % 5 == 0).then_some(msg.timestamp));

    ontime
        // each workers has its own window(s) but the `key_distribute` operator makes sure
        // all messages find their way to the correct window state.
        // this message just aggregates time periods of length `10` into a vec
        .tumbling_window(10, |_| Some(vec![]), |msg, state| state.push(msg.value))
        // print the aggregated vec and where it was produced
        .inspect(|msg, ctx| println!("{msg:?} @ {}", ctx.worker_id))
        .finish();

    worker
}
