//! Adapted from timely dataflow's partition

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::{Scope, Stream};
use timely::Data;

/// Partition a stream of records into multiple streams.
pub trait Split<G: Scope, D: Data, D2: Data, D3: Data, F: Fn(D)->(D2, D3)> {
    /// Splits a stream into two by applying a splitter
    fn split(&self, splitter: F) -> (Stream<G, D2>, Stream<G, D3>);
}

impl<G: Scope, D: Data, D2: Data, D3: Data, F: Fn(D)->(D2, D3)+'static> Split<G, D, D2, D3, F> for Stream<G, D> {
    fn split(&self, splitter: F) -> (Stream<G, D2>, Stream<G, D3>) {

        let mut builder = OperatorBuilder::new("Split".to_owned(), self.scope());

        let mut input = builder.new_input(self, Pipeline);

        let (mut output_a, stream_a) = builder.new_output::<D2>();
        let (mut output_b, stream_b) = builder.new_output::<D3>();

        builder.build(move |_| {
            let mut vector = Vec::new();
            move |_frontiers| {
                let mut handle_a = output_a.activate();
                let mut handle_b = output_b.activate();

                input.for_each(|time, data| {
                    data.swap(&mut vector);
                    let mut session_a = handle_a.session(&time);
                    let mut session_b = handle_b.session(&time);

                    for datum in vector.drain(..) {
                        let (part_a, part_b) = splitter(datum);
                        session_a.give(part_a);
                        session_b.give(part_b);
                    }
                });
            }
        });

        (stream_a, stream_b)
    }
}