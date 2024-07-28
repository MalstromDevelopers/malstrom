use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Instant;

use jetstream::config::Config;
use jetstream::operators::map::Map;
use jetstream::operators::source::Source;
use jetstream::worker::RuntimeBuilder;
use jetstream::{snapshot::NoPersistence, test::get_test_configs};
use nexmark::config::NexmarkConfig;
use nexmark::event::Event;
use nexmark::EventGenerator;

fn main() {
    q1::<8>(100_000_000);
}

/// Runs the q1 nexmark benchmark:
/// Convert each bid value from dollars to euros. Illustrates a simple transformation.
fn q1<const THREAD_CNT: usize>(message_count: usize) -> () {
    let configs = get_test_configs::<THREAD_CNT>();
    let cnt_per_thread = message_count / THREAD_CNT;
    let threads = configs
        .map(|c| (c, cnt_per_thread.clone()))
        .map(|(c, x)| std::thread::spawn(move || run_stream(c, x)));
    let start = Instant::now();
    for t in threads {
        t.join().unwrap();
    }
    let elapsed = Instant::now().duration_since(start);
    println!("Took {elapsed:?} for {message_count} records on {THREAD_CNT} threads");
}

fn run_stream(config: Config, msg_count: usize) -> () {
    let worker = RuntimeBuilder::new(NoPersistence::default(), || false);

    let nex = NexmarkConfig::default();
    let gen = EventGenerator::new(nex).take(msg_count);
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_moved = counter.clone();

    let stream = worker.new_stream().source(gen).map(move |x| {
        counter_moved.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        match x {
            Event::Bid(b) => Some(b.price as f64 * 0.908),
            _ => None,
        }
    });

    stream.finish();
    let mut runtime = worker.build(config).unwrap();
    loop {
        let cnt = counter.load(std::sync::atomic::Ordering::Relaxed);
        if cnt >= msg_count {
            return;
        }
        runtime.step()
    }
}
