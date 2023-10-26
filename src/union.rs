use timely::Data;
use timely::dataflow::operators
use crate::{JetStream, DataUnion};

pub trait UnionStream<'scp, D1: Data, D2: Data> {
    fn union(self, other: JetStream<'_, D2>) -> JetStream<'scp, DataUnion<D1, D2>>;
}

impl <'scp, D1: Data, D2: Data> UnionStream<'scp, D1, D2> for JetStream<'_, D1>{
    fn union(self, other: JetStream<'_, D2>) -> JetStream<'scp, DataUnion<D1, D2>> {
        todo!()
    }
}
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::{Stream, Scope};

/// Merge the contents of two streams.
trait Union<G: Scope, D1: Data, D2: Data> {
    fn union(&self, left: &Stream<G, D1>, right: &Stream<G, D2>) -> Stream<G, DataUnion<D1, D2>>;
}


impl<G: Scope, D1: Data, D2: Data> Union<G, D1, D2> for G {
    fn union(&self, left: &Stream<G, D1>, right: &Stream<G, D2>) -> Stream<G, DataUnion<D1, D2>>
    {

        // create an operator builder.
        use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
        let mut builder = OperatorBuilder::new("Union".to_string(), self.clone());

        // create new input handles for each input stream.
        let mut handle_a = builder.new_input(&left, Pipeline);
        let mut handle_b = builder.new_input(&right, Pipeline);

        // create one output handle for the concatenated results.
        let (mut output, result) = builder.new_output();
        handle_a.
        // build an operator that plays out all input data.
        builder.build(move |_capability| {

            let mut vector = Vec::new();
            move |_frontier| {
                let mut output = output.activate();
                if handle_a.next()
                for handle in handles.iter_mut() {
                    handle.for_each(|time, data| {
                        data.swap(&mut vector);
                        output.session(&time).give_vec(&mut vector);
                    })
                }
            }
        });

        result
    }
}
