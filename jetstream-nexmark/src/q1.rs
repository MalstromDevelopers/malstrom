use std::time::Instant;
use std::usize;

use jetstream::config::Config;
use jetstream::operators::*;
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

    let (stream, frontier) = worker.new_stream()
    .source(gen.enumerate())
    .assign_timestamps(|x| x.value.0)
    // emit a MAX epoch to show we reached end of input
    .generate_epochs(move |x, _| (x.timestamp == msg_count - 1).then_some(usize::MAX)).0
    .map(|x| x.1)
    .map(move |x| {
        match x {
            Event::Bid(b) => Some(b.price as f64 * 0.908),
            _ => None,
        }
    })
    .inspect_frontier();

    stream.finish();
    let mut runtime = worker.build(config).unwrap();
    while frontier.get_time().map_or(true, |x| x != usize::MAX) {
        runtime.step();
    }
}
