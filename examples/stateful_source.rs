use jetstream::stateful_source;
use timely::dataflow::operators::{Inspect, ToStream};

fn main() {
    let factory = || {
        println!("State factory called");
        0 as usize
    };
    let logic = |state| {
        if state < 20 {
            return (format!("Current state {state}"), state + 1);
        } else {
            panic!("Done")
        }
    };

    timely::execute_from_args(std::env::args(), move |worker| {
        worker.dataflow::<usize, _, _>(|scope| {
            stateful_source(scope, factory, logic).inspect(|x| println!("Saw {x:?}"));
        });
        println!("Built dataflow");
        for i in 0..10 {
            worker.step();
        }
    })
    .unwrap();
}
