use jetstream::{keyed::{partitioners::rendezvous_select, KeyDistribute}, prelude::{Inspect, Source}, runtime::{RuntimeFlavor, WorkerBuilder}, snapshot::NoPersistence, sources::SingleIteratorSource};
use malstrom_k8s::KubernetesRuntime;

fn main() {
    env_logger::init();
    let rt = KubernetesRuntime::new(build_dataflow);
    rt.execute();
}

fn build_dataflow<F: RuntimeFlavor>(flavor: F) -> WorkerBuilder<F, NoPersistence> {
    let mut worker = WorkerBuilder::new(flavor);

    worker.new_stream()
    .source(SingleIteratorSource::new(0..1_000_000))
    .key_distribute(|x| x.value, rendezvous_select)
    .inspect(|msg, _| if msg.value > 999_000 {println!("{msg:?}")})
    .finish();

    worker
}
