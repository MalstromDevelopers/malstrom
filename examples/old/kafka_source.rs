use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::usize;

use jetstream::persistent::{persistent_source, PersistentState, PersistentStateBackend};
use jetstream::stateful_source;
use rdkafka::Message;
use timely::dataflow::operators::{Inspect, ToStream};
use timely::Data;


fn main() {
    timely::execute_from_args(std::env::args(), move |worker| {
        worker.dataflow::<usize, _, _>(|scope| {
            let state_backend = DiskState {
                dir: PathBuf::from("/tmp/jetstream/kafka_example"),
                state_type: PhantomData::<i64>,
            };
            let logic = create_kafka_source();

            persistent_source(scope, state_backend, logic).inspect(|x| println!("Saw {x:?}"));
        });
        println!("Built dataflow");
        for i in 0..10 {
            worker.step();
        }
    })
    .unwrap();
}

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
use rdkafka::{
    consumer::{BaseConsumer, Consumer, DefaultConsumerContext},
    ClientConfig,
};

pub fn create_and_subscribe_consumer(
    brokers: &str,
    topic: &str,
) -> BaseConsumer<DefaultConsumerContext> {
    let mut config = ClientConfig::new();
    config
        .set("auto.offset.reset", "smallest")
        .set("group.id", "jetstream_example")
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

fn create_kafka_source() -> impl Fn(i64) -> (Vec<u8>, i64) + 'static {
    let topic = "jetstream_example";
    let consumer = create_and_subscribe_consumer(
        "redpanda-0.customredpandadomain.local:31092,redpanda-1.customredpandadomain.local:31092,redpanda-2.customredpandadomain.local:31092",
        topic);

    move |state| {
        // seek to the offset currently in state
        // TODO: This is very very dumb, we should only keep in state what we have not comitted yet
        // consumer.seek(topic, 0, rdkafka::Offset::Offset(state), Duration::from_secs(1)).expect("unable to seek");
        let mut polled = None;
        loop {

            polled = consumer.poll(Duration::from_millis(2_000));
            println!("{polled:?}");
            if polled.is_some() {
                break;
            }
        }
        if let Some(msg) = polled {
            return (
                msg.expect("Time out waiting for kafka message")
                    .detach()
                    .payload()
                    .expect("Empty Message from Kafka")
                    .to_owned(),
                state + 1,
            );
        } else {
            panic!("Kafka topic exhausted");
        }
    }
}
