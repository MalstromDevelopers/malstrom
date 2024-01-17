use std::time::{Instant, Duration};

use jetstream::frontier::Timestamp;
use jetstream::stateful_map::StatefulMap;
use jetstream::{worker::Worker, snapshot::NoPersistenceBackend};
use jetstream::source::PollSource;



fn main() {

    let mut clock = Instant::now();
    let snapshot_timer = move || {
        let now = Instant::now();
        if now.duration_since(clock) > Duration::from_secs(5) {
            clock = now;
            true
        } else {
            false
        }
    };
    let mut worker = Worker::<NoPersistenceBackend>::new(snapshot_timer);
    let stream = worker.new_stream().poll_source(|f| {
        f.advance_to(f.get_actual() + Timestamp::from(1));
        Some(rand::random::<i64>())
    }).stateful_map(|input, f, state: &mut u64| {
        f.advance_to(f.get_actual() + Timestamp::from(1));
        *state += 1;
        if *state % 1_000_000 == 0 {
            println!("Working....")
        }
        input
    });
    worker.add_stream(stream);
    let mut runtime = worker.build();
    loop {
        runtime.step()
    }
}