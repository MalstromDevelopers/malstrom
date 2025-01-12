use std::time::Duration;

use malstrom::keyed::partitioners::rendezvous_select;
use malstrom::operators::*;
/// A multithreaded program
use malstrom::runtime::{MultiThreadRuntime, RuntimeFlavor, WorkerBuilder};
use malstrom::snapshot::{NoPersistence, NoSnapshots};
use malstrom::sources::{SingleIteratorSource, StatelessSource};

fn main() {
    env_logger::init();
    let (rt, api) = MultiThreadRuntime::new(1, build_dataflow).with_api();
    let tokio_rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    tokio_rt.spawn(async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
            api.scale(4).await.unwrap();
            // this does not work :(
            tokio::time::sleep(Duration::from_secs(5)).await;
            api.scale(-1).await.unwrap();

    });
    rt.execute().unwrap();
}

fn build_dataflow<F: RuntimeFlavor>(flavor: F) -> WorkerBuilder<F, NoPersistence> {
    let mut worker = WorkerBuilder::new(flavor, NoSnapshots, NoPersistence::default());
    worker
        .new_stream()
        .source(
            "iter-source",
            StatelessSource::new(SingleIteratorSource::new((0..=100).cycle())),
        )
        .key_distribute("key-by-value", |x| x.value, rendezvous_select)
        .map("double", |x| x * 2)
        .inspect("print", |x, ctx| {
            std::thread::sleep(Duration::from_millis(100)); // throttle stream
            println!("{x:?} @ Worker {}", ctx.worker_id)
        })
        .finish();
    worker
}
