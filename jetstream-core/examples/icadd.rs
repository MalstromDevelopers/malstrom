use std::{
    collections::VecDeque, iter, path::Path, rc::Rc, sync::atomic::{AtomicBool, Ordering}, time::Instant
};
use indexmap::IndexSet;
use jetstream::{
    config::Config, keyed::{KeyDistribute, }, operators::{
    }, snapshot::NoPersistence, stream::operator::OperatorBuilder, test::get_test_configs, time::NoTime, DataMessage, Message, NoKey, RescaleMessage, Worker
};
use jetstream::operators::stateful_map::StatefulMap;

fn main() {
    let [config0, config1] = get_test_configs::<2>();
    let threads = [
        std::thread::spawn(move || run_stream(config0, "a".to_string())),
        std::thread::spawn(move || run_stream(config1, "b".to_string())),
        ];

    for x in threads {
        let _ = x.join();
    }
}

fn run_stream(config: Config, key: String) {
    let first_msgs = (0..10).map(|i| Message::Data(DataMessage::new(NoKey, i, NoTime)));
    let rescale_message = RescaleMessage::ScaleRemoveWorker(IndexSet::from([1]));
    let last_msgs = (10..10000).map(|i| Message::Data(DataMessage::new(NoKey, i, NoTime)));
    // reconfigure cluster after ten messages
    let mut the_source = first_msgs.chain(iter::once(Message::Rescale(rescale_message))).chain(last_msgs);
    
    let mut worker = Worker::new(NoPersistence::default(), || false);

    let thread_name = key.clone();

    let stream = worker
        .new_stream()
        .then(OperatorBuilder::direct(move |input, output, _ctx| {
            if let Some(msg) = input.recv() {
                println!("{msg:?}");
            }
            if let Some(x) = the_source.next() {
                output.send(x)
            }
        })).key_distribute(move |_| key.clone(), move|k, targets| {
            if k == "b" && targets.len() == 2 {
                &1
            } else {
                &0
            }
        })
        .stateful_map(move |key, msg, state: usize| {
            let state = state + 1;
            if (state < 10) || (state >= 9999) {
                println!("{state:?} for {key} @ {thread_name}");

            }
            (msg, Some(state))
        })
        ;

    worker.add_stream(stream);
    let mut runtime = worker.build(config).unwrap();
    loop {
        runtime.step()
    }
    
    }