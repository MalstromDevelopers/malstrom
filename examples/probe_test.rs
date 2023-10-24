use std::marker::PhantomData;
use std::ops::{Add, AddAssign};
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
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::source;
use timely::dataflow::operators::{Capability, Exchange, Filter, Inspect, Map, Operator, Probe};
use timely::dataflow::InputHandle;
use timely::dataflow::ProbeHandle;

fn main() {
    timely::execute_from_args(std::env::args(), move |worker| {
        let mut input = InputHandle::<usize, String>::new();
        worker.dataflow::<usize, _, _>(|scope| {
            let mut handle_a = ProbeHandle::new();
            let mut handle_b = ProbeHandle::new();

            source(scope, "Source", |capability, info| {
                // Acquire a re-activator for this operator.
                use timely::scheduling::Scheduler;
                let activator = scope.activator_for(&info.address[..]);

                let mut cap = Some(capability);
                move |output| {
                    let mut done = false;
                    if let Some(cap) = cap.as_mut() {
                        // get some data and send it.
                        let time = cap.time().clone();
                        output.session(&cap).give("BRUDI");

                        // downgrade capability.
                        cap.downgrade(&(time + 2));
                        done = time > 20;
                    }

                    if done {
                        cap = None;
                    } else {
                        activator.activate();
                    }
                }
            })
            .probe_with(&mut handle_a)
            .inspect(move |_| {
                handle_a.with_frontier(|t| println!("Handle A: {t:?}"));
            })
            .unary(Pipeline, "increment", |capability, info| {
                let mut cap = Some(capability);
                let mut vector = Vec::new();
                move |input, output| {
                    if let Some((time, msg)) = input.next() {
                        let cp = cap.as_mut().unwrap();
                        cp.downgrade(&(cp.time() + 1));
                        println!("{cp:?} {time:?}");
                        msg.swap(&mut vector);
                        output.session(&cp).give_vec(&mut vector);
                    } else {
                        cap = None;
                    }
                }
            })
            .probe_with(&mut handle_b)
            .inspect(move |_| {
                handle_b.with_frontier(|t| println!("Handle B: {t:?}"));
            });
        });
        // for i in 0..10 {
        //     // input.send("Hola".into());
        //     // input.advance_to(i);
        //     worker.step();
        // }
    })
    .unwrap();
}
