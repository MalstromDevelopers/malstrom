use std::time::Duration;

use malstrom::keyed::partitioners::rendezvous_select;
use malstrom::operators::*;
/// A multithreaded program
use malstrom::runtime::{MultiThreadRuntime, RuntimeFlavor, StreamProvider, WorkerBuilder};
use malstrom::snapshot::{NoPersistence, NoSnapshots};
use malstrom::sources::{SingleIteratorSource, StatelessSource};

fn main() {
    // tracing_subscriber::fmt::init();
    let rt = MultiThreadRuntime::builder()
        .parrallelism(2)
        .persistence(NoPersistence::default())
        .build(build_dataflow);
    let api_handle = rt.api_handle();

    std::thread::spawn(move || {
        let tokio_rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        loop {
            std::thread::sleep(Duration::from_secs(2));
            println!("Rescaling to 2 workers");
            tokio_rt.block_on(api_handle.rescale(2)).unwrap();
            println!("Rescale complete!");
            std::thread::sleep(Duration::from_secs(2));
            println!("Rescaling to 1 workers");
            tokio_rt.block_on(api_handle.rescale(1)).unwrap();
            println!("Rescale complete!");
        }
    });

    rt.execute().unwrap();
}

fn build_dataflow(provider: &mut dyn StreamProvider) -> () {
    provider
        .new_stream()
        .source(
            "iter-source",
            StatelessSource::new(SingleIteratorSource::new((0..=100).cycle())),
        )
        .key_distribute("key-odd-even", |x| x.value & 1 == 0, rendezvous_select)
        .stateful_map("keyed-sum", |_, num, mut sum: i32| {
            sum += num;
            (sum, Some(sum))
        })
        .inspect("print", |x, ctx| {
            println!("{} @ Worker {}", x.value, ctx.worker_id);
            std::thread::sleep(Duration::from_millis(300)); // slowing things down a bit
        });
}
