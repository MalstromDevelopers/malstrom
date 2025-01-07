/// A basic example which runs a no-op dataflow
use malstrom::runtime::{SingleThreadRuntime, SingleThreadRuntimeFlavor, WorkerBuilder};
use malstrom::snapshot::{NoPersistence, NoSnapshots};

fn main() {
    SingleThreadRuntime::new(build_dataflow).execute().unwrap()
}

fn build_dataflow(
    flavor: SingleThreadRuntimeFlavor,
) -> WorkerBuilder<SingleThreadRuntimeFlavor, NoPersistence> {
    let mut worker = WorkerBuilder::new(flavor, NoSnapshots, NoPersistence::default());
    worker.new_stream().finish();
    worker
}
