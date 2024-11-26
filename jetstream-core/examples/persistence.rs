//! This example showcases persisting snapshots
//! to a local filesystem.
//! However you could just as easily use a cloud
//! storage like S3

use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant};

use jetstream::keyed::partitioners::{index_select, rendezvous_select};
use jetstream::operators::*;
use jetstream::runtime::threaded::{MultiThreadRuntime, SingleThreadRuntime};
use jetstream::runtime::{RuntimeFlavor, WorkerBuilder, SnapshotTrigger};
use jetstream::snapshot::{SlateDbBackend, SlateDbClient};
use jetstream::sources::SingleIteratorSource;
use object_store::local::LocalFileSystem;
use object_store::path::Path;

static SNAPSHOT_DIR: &'static str = "/tmp/example_snapshots";

fn main() {
    // we execute in a loop to restart the dataflow everytime
    // even after it finished
    let _ = std::fs::remove_dir_all(SNAPSHOT_DIR);
    loop {
        let rt = SingleThreadRuntime::new(build_dataflow);
        rt.execute().unwrap();
        println!("Restarting");
    }
}

fn build_dataflow<R: RuntimeFlavor>(flavor: R) -> WorkerBuilder<R, SlateDbClient> {
    // We use SlateDB to store snapshots on persistent storage.
    // For this example we will use a temporary directory
    let storage = Arc::new(LocalFileSystem::new());
    let persistence = SlateDbBackend::new(storage, Path::from(SNAPSHOT_DIR)).unwrap();
    let mut worker = WorkerBuilder::new(flavor, TimerTrigger::new(Duration::from_secs(10)), persistence);

    worker
        .new_stream()
        // NOTE: The SingleIteratorSource is stateless and will restart from the beginning
        // every time
        .source(SingleIteratorSource::new(0..30))
        .key_distribute(|_| 0, rendezvous_select)
        // we will simply calculate a sum of all values
        .stateful_map(|_key, value, state: i32| (state + value, Some(state + value)))
        .inspect(|x, _ctx| {
            println!("{:?}", x.value);
            sleep(Duration::from_millis(500));
        })
        .finish();
    worker
}

struct TimerTrigger {
    interval: Duration,
    last_trigger: Instant
}

impl TimerTrigger {
    fn new(interval: Duration) -> Self {
        TimerTrigger { interval, last_trigger: Instant::now() }
    }
}
impl SnapshotTrigger for TimerTrigger {
    fn should_trigger(&mut self) -> bool {
        let now = Instant::now();
        if now.duration_since(self.last_trigger) > self.interval {
            self.last_trigger = now;
            println!("Triggering snapshot");
            true
        } else {
            false
        }
    }
}
