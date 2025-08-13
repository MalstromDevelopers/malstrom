//! Using SlateDB as a persistence backend
use malstrom::keyed::partitioners::rendezvous_select;
use malstrom::operators::*;
use malstrom::sinks::{StatelessSink, StdOutSink};
use malstrom::sources::{StatefulSource, StatefulSourceImpl, StatefulSourcePartition};
use malstrom::{runtime::SingleThreadRuntime, snapshot::SlateDbBackend, worker::StreamProvider};
use object_store::{local::LocalFileSystem, path::Path};
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant};

fn main() {
    let filesystem = LocalFileSystem::new();
    let persistence = SlateDbBackend::new(Arc::new(filesystem), Path::from("/tmp")).unwrap();

    loop {
        let job = SingleThreadRuntime::builder()
            .persistence(persistence.clone())
            .snapshots(Duration::from_secs(1))
            .build(build_dataflow);
        let thread = std::thread::spawn(move || job.execute().unwrap());
        match thread.join() {
            Ok(_) => return,
            Err(_) => {
                println!("Restarting worker");
                continue;
            }
        }
    }
}

fn build_dataflow(provider: &mut dyn StreamProvider) {
    let start_time = Instant::now();
    let fail_interval = Duration::from_secs(10);
    provider
        .new_stream()
        .source("iter-source", StatefulSource::new(StatefulNumberSource(0)))
        .key_distribute("key-by-value", |x| x.value & 1 == 1, rendezvous_select)
        .stateful_map("sum", |_key, value, state: i32| {
            let state = state + value;
            (state, Some(state))
        })
        .inspect("expensive-operation", |_msg, _ctx| {
            // we need this to not overflow the sum before "crashing"
            sleep(Duration::from_millis(100))
        })
        .inspect("fail-random", move |_msg, _ctx| {
            if Instant::now().duration_since(start_time) > fail_interval {
                panic!("Oh no!")
            }
        })
        .sink("stdout", StatelessSink::new(StdOutSink));
}

struct StatefulNumberSource(i32);

impl StatefulSourceImpl<i32, i32> for StatefulNumberSource {
    type Part = ();
    type PartitionState = i32;
    type SourcePartition = Self;

    fn list_parts(&self) -> Vec<Self::Part> {
        vec![()]
    }

    fn build_part(
        &mut self,
        _part: &Self::Part,
        part_state: Option<Self::PartitionState>,
    ) -> Self::SourcePartition {
        println!("Build with {part_state:?}");
        Self(part_state.unwrap_or_default())
    }
}

impl StatefulSourcePartition<i32, i32> for StatefulNumberSource {
    type PartitionState = i32;

    fn poll(&mut self) -> Option<(i32, i32)> {
        let out = Some((self.0, self.0));
        self.0 += 1;
        out
    }

    fn is_finished(&mut self) -> bool {
        false
    }

    fn snapshot(&self) -> Self::PartitionState {
        println!("SNAPSHOTTING SOURCE");
        self.0
    }

    fn collect(self) -> Self::PartitionState {
        self.0
    }
}
