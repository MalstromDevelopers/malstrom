use timely::Data;
use timely::dataflow::operators::Operator;
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
/// 
/// Both streams must return Option<D> to allow the
/// other stream to progress if there is no data on one
/// of them. I.e. if a stream has no data at the moment, but
/// might have more data later, it shall yield None.
/// 
/// If both streams have data, union will alternate between them
trait Union<G: Scope, D1: Data, D2: Data> {
    fn union(&self, left: &Stream<G, D1>, right: &Stream<G, D2>) -> Stream<G, DataUnion<D1, D2>>;
}


impl<G: Scope, D1: Data, D2: Data> Union<G, D1, D2> for G {
    fn union(&self, left: &Stream<G, D1>, right: &Stream<G, D2>) -> Stream<G, DataUnion<D1, D2>>
    {
        left.binary(right, Pipeline, Pipeline, "union", |default_cap, _info| {
                 let mut cap = Some(default_cap);
                 let mut vec1: Vec<D1> = Vec::new();
                 let mut vec2: Vec<D2> = Vec::new();


                 // flip flop to alternate between inputs
                 let mut call_1 = true;

                 move |input1, input2, output| {
                    
                    if call_1 {
                        if let Some((time, data)) = input1.next(){
                            data.swap(&mut vec1);
                            let mut as_output: Vec<DataUnion<D1, D2>> = vec1.into_iter().map(
                                |x| DataUnion::Left(x)
                            ).collect();

                            if as_output.len() != 0 {
                                output.session(&time).give_vec(&mut as_output);
                                call_1 = !call_1;
                                return;
                            }
                        }
                    } 
                    if let Some((time, data)) = input2.next(){
                        data.swap(&mut vec2);
                        let mut as_output: Vec<DataUnion<D1, D2>> = vec1.into_iter().map(
                            |x| DataUnion::Left(x)
                        ).collect();

                        if as_output.len() != 0 {
                            output.session(&time).give_vec(&mut as_output);
                            call_1 = !call_1;
                            return;
                        }
                    }
                 }
             })
    }
}
