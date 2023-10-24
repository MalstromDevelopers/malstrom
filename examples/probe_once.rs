use std::marker::PhantomData;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::usize;

use jetstream::persistent::{persistent_source, PersistentState, PersistentStateBackend};
use rdkafka::consumer::CommitMode;
use rdkafka::{
    consumer::{BaseConsumer, Consumer, DefaultConsumerContext},
    ClientConfig, TopicPartitionList,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use timely::dataflow::operators::{Filter, Inspect, Map};
use timely::dataflow::scopes::Child;
use timely::dataflow::ProbeHandle;
use timely::worker::Worker;
use timely::communication::allocator::Generic;

fn main() {
    timely::execute_from_args(std::env::args(), move |worker| {
        worker.dataflow::<i64, _, _>(|scope| build_dataflow(scope));
        println!("Built dataflow");
        for _ in 0..10 {
            worker.step();
        }
    })
    .unwrap();
}

fn build_dataflow(scope: &mut Child<'_, Worker<Generic>, i64>) -> Result<(), BuildError> {
    let state_backend = DiskState::new("/tmp/jetstream/probation");
    let progress_probe = ProbeHandle::<i64>::new();
    let source_logic = create_kafka_source::<CommitState>(
        "jetstream_example".into(),
        "localhost:9092".into(),
        progress_probe,
    )?;
    let stream = persistent_source(scope, state_backend, source_logic)
        .filter(|msg| msg.is_some())
        .map(|msg| msg.unwrap())
        // deserialize the message
        .map(|msg| serde_json::from_slice::<Message>(&msg))
        .inspect(|msg| println!("{msg:?}"))
        .probe_with(progress_probe);

    Ok(())
}

#[derive(Serialize, Deserialize)]
struct Message {
    postime: String,
    order: String,
    line: String,
    lon: f32,
    lat: f32,
    speed: f32,
}

#[derive(Error, Debug)]
pub enum BuildError {
    #[error("Error creating source")]
    CreateSource(#[from] CreateSourceError),
}

#[derive(Error, Debug)]
pub enum CreateSourceError {
    #[error("Error from Kafka")]
    Kafka(#[from] rdkafka::error::KafkaError),
}

#[derive(Error, Debug)]
pub enum StateLoadError {
    #[error("IO Error")]
    IO(#[from] std::io::Error),
    #[error("Deserialization Error")]
    Deserialization(#[from] bincode::Error),
}

#[derive(Error, Debug)]
pub enum StateSaveError {
    #[error("IO Error")]
    IO(#[from] std::io::Error),
    #[error("Serialization Error")]
    Serialization(#[from] bincode::Error),
}

/// This is the Backend we use for persisting our state
struct DiskState<P> {
    dir: PathBuf,
    state_type: PhantomData<P>,
}
impl <P> DiskState<P> {
    fn new(path: &str) -> Self {
        Self { dir: PathBuf::from(path), state_type: PhantomData::<CommitState> }
    }
}

impl<T, P> PersistentStateBackend<T, P> for DiskState<P>
where
    P: PersistentState + Default,
{
    type LoadError = StateLoadError;
    type SaveError = StateSaveError;

    fn load_state_latest(&self, worker_index: usize) -> Result<P, Self::LoadError> {
        let mut path = self.dir.clone();
        path.push(format!("worker_{worker_index}.bin"));

        if !path.exists() {
            return Ok(P::default());
        };
        println!("Reading persisted state from disk");
        let fileinp = std::fs::read(path)?;
        let state: P = bincode::deserialize(&fileinp)?;
        Ok(state)
    }

    fn save_state(&self, worker_index: usize, state: &P) -> Result<(), Self::SaveError> {
        // very stupid method to back up every other minute
        let time = Instant::now().duration_since(UNIX_EPOCH);
        if (time.as_secs() / 60) & 1 != 0 {
            return Ok(());
        }

        if !self.dir.exists() {
            std::fs::create_dir_all(self.dir.clone())?;
        };
        let bytes = bincode::serialize(state)?;
        let mut path = self.dir.clone();
        path.push(format!("worker_{worker_index}.bin"));
        std::fs::write(path, bytes)?;
        Ok(())
    }
}

pub fn create_and_subscribe_consumer(
    brokers: &str,
    topic: &str,
) -> BaseConsumer<DefaultConsumerContext> {
    let mut config = ClientConfig::new();
    config
        .set("auto.offset.reset", "smallest")
        .set("group.id", "jetstream-exactly-once")
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        .set("bootstrap.servers", brokers);

    let consumer: BaseConsumer<DefaultConsumerContext> =
        config.create().expect("Couldn't create consumer");
    consumer
        .subscribe(&[topic])
        .expect("Failed to subscribe to topic");
    consumer
}

/// This is the persistent state structure the source
/// uses to maintain internal state
///
/// This allows us to buffer commits for a while before
/// reporting them to Kafka, which in turn enhances performance
#[derive(Serialize, Deserialize, Debug, Clone)]
struct CommitState {
    last_commit_time: SystemTime, // the last time we committed to kafka
    last_commit_offset: i64,      // the last offset we commited to kafka
    last_processed_offset: i64,   // the last offset our processor reported as sinked
}
impl Default for CommitState {
    fn default() -> Self {
        Self {
            last_commit_time: SystemTime::now(),
            last_commit_offset: 0,
            last_processed_offset: 0,
        }
    }
}

/// For performance reasons we do not want to commit every single message
/// So instead we will commit every 5 seconds and keep track of the last
/// processed and committed message internally
fn create_kafka_source<P>(
    topic: String,
    brokers: String,
    progress_probe: ProbeHandle<i64>,
) -> Result<impl FnMut(P) -> (Option<Vec<u8>>, P) + 'static, CreateSourceError> {
    let consumer = create_and_subscribe_consumer(&brokers, &topic);

    let mut topic_partitions = TopicPartitionList::new();
    topic_partitions.add_partition_offset(&topic, 0, rdkafka::Offset::Offset(0))?;

    let logic = move |mut state: P| -> (Option<Vec<u8>>, P) {
        {
            // here we commit a new offset to kafka if
            // A: our producer has progressed
            // B: at least 5_000ms have passed sinc our last commit
            let proc_offset = progress_probe.with_frontier(|x| x);

            state.last_processed_offset = proc_offset;
            let made_progress = proc_offset > state.last_commit_offset;
            let time_elapsed = Instant::now().duration_since(state.last_commit_time);

            if made_progress && time_elapsed.as_millis() > 5_000 {
                topic_partitions
                    .set_partition_offset(&topic, 0, rdkafka::Offset::Offset(proc_offset))
                    .unwrap();
                consumer
                    .commit(&topic_partitions, CommitMode::Sync)
                    .unwrap();

                // update our state to reflect the change
                state.last_commit_time = Instant::now();
                state.last_commit_offset = proc_offset;
            };
        }

        // lets read some kafka messages
        match consumer.poll(Duration::from_millis(2_000)) {
            Some(msg) => (
                Some(
                    msg.expect("Time out waiting for kafka message")
                        .detach()
                        .payload()?
                        .to_owned(),
                ),
                state,
            ),
            None => (None, state),
        }
    };

    Ok(logic)
}
