use std::marker::PhantomData;
use std::ops::AddAssign;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::usize;

use jetstream::persistent::{persistent_source, PersistentState, PersistentStateBackend};
use rdkafka::consumer::CommitMode;
use rdkafka::{
    consumer::{BaseConsumer, Consumer, DefaultConsumerContext},
    ClientConfig, Message, TopicPartitionList,
};
use serde::{Deserialize, Serialize};
use timely::dataflow::operators::{Inspect, Filter};

fn main() {
    timely::execute_from_args(std::env::args(), move |worker| {
        worker.dataflow::<usize, _, _>(|scope| {
            let state_backend = DiskState {
                dir: PathBuf::from("/tmp/jetstream/exactly_once"),
                state_type: PhantomData::<CommitState>,
            };
            let (commit_handle, logic) = create_kafka_source();

            persistent_source(scope, state_backend, logic)
                .inspect(|x| println!("Message {x:?}"))
                .filter(|x| x.is_some())

                // this would be your sink, where you increment the committed
                // offset after having sinked a message successfully
                .inspect(move |_| commit_handle.increment(1));
        });
        println!("Built dataflow");
        for _ in 0..10 {
            worker.step();
        }
    })
    .unwrap();
}

/// This is the Backend we use for persisting our state
struct DiskState<P> {
    dir: PathBuf,
    state_type: PhantomData<P>,
}

impl<T, P> PersistentStateBackend<T, P> for DiskState<P>
where
    P: PersistentState + Default,
{
    type LoadError = std::io::Error;
    type SaveError = std::io::Error;

    fn load_state_latest(&self, worker_index: usize) -> Result<P, Self::LoadError> {
        let mut path = self.dir.clone();
        path.push(format!("worker_{worker_index}.bin"));

        if !path.exists() {
            return Ok(P::default());
        };
        println!("Reading persisted state from disk");
        let fileinp = std::fs::read(path)?;
        let state: P = bincode::deserialize(&fileinp).unwrap();
        Ok(state)
    }

    fn save_state(&self, worker_index: usize, state: &P) -> Result<(), Self::SaveError> {
        // very stupid method to back up every other minute
        let time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        if (time.as_secs() / 60) & 1 != 0 {
            return Ok(());
        }

        if !self.dir.exists() {
            std::fs::create_dir_all(self.dir.clone()).expect("Error creating state directory");
        };
        let bytes = bincode::serialize(state).unwrap();
        let mut path = self.dir.clone();
        path.push(format!("worker_{worker_index}.bin"));
        std::fs::write(path, bytes).unwrap();
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

/// This is a form of shared state the Source will create
/// Effectively it is a counter which the sink can increment
/// to communicate, that it has sinked a message
/// 
/// The source will read that counter to determine which offset
/// to commit
#[derive(Clone)]
struct ProcessOffsetHandle {
    offset: Arc<RwLock<i64>>,
}
impl ProcessOffsetHandle {
    pub fn new() -> Self {
        ProcessOffsetHandle {
            offset: Arc::new(RwLock::new(0)),
        }
    }

    pub fn increment(&self, val: i64) -> () {
        self.offset.write().unwrap().add_assign(val);
    }

    pub fn read(&self) -> i64 {
        *self.offset.read().unwrap()
    }
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
fn create_kafka_source() -> (
    ProcessOffsetHandle,
    impl FnMut(CommitState) -> (Option<Vec<u8>>, CommitState) + 'static,
) {
    let topic = "jetstream_example";
    let consumer = create_and_subscribe_consumer(
        "redpanda-0.customredpandadomain.local:31092,redpanda-1.customredpandadomain.local:31092,redpanda-2.customredpandadomain.local:31092",
        topic);

    // this is the offset our producer at the end of the pipeline
    // can increment to tell us to which number to set the
    // offset
    let p_offset_handle = ProcessOffsetHandle::new();
    let p_offset_handle_source = p_offset_handle.clone();

    let mut topic_partitions = TopicPartitionList::new();
    topic_partitions
        .add_partition_offset(topic, 0, rdkafka::Offset::Offset(0))
        .unwrap();

    let logic = move |mut state: CommitState| -> (Option<Vec<u8>>, CommitState) {
        {
            // here we commit a new offset to kafka if
            // A: our producer has progressed
            // B: at least 5_000ms have passed sinc our last commit
            let proc_offset = p_offset_handle_source.read();
            state.last_processed_offset = proc_offset;
            let made_progress = proc_offset > state.last_commit_offset;
            let time_elapsed = SystemTime::now()
                .duration_since(state.last_commit_time)
                .unwrap();

            if made_progress && time_elapsed.as_millis() > 5_000 {
                topic_partitions
                    .set_partition_offset(topic, 0, rdkafka::Offset::Offset(proc_offset))
                    .unwrap();
                consumer
                    .commit(&topic_partitions, CommitMode::Sync)
                    .unwrap();

                // update our state to reflect the change
                state.last_commit_time = SystemTime::now();
                state.last_commit_offset = proc_offset;
            };
        }

        // lets read some kafka messages
        match consumer.poll(Duration::from_millis(2_000)) {
            Some(msg) => (
                Some(
                    msg.expect("Time out waiting for kafka message")
                        .detach()
                        .payload()
                        .expect("Empty Message from Kafka")
                        .to_owned(),
                ),
                state,
            ),
            None => (None, state),
        }
    };

    (p_offset_handle, logic)
}
