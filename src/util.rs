use timely::dataflow::operators::generic::operator;
use timely::dataflow::{Scope, Stream};
use timely::Data;
/// provide one value to a stream and then never ever
/// do anything again
/// TODO: put this in a util package
pub fn once<G: Scope, D: Data>(scope: &G, value: D) -> Stream<G, D> {
    operator::source(scope, "Once", |capability, _info| {
        let mut cap = Some(capability);

        // we need to do a bit of a dance here with option
        // since technically our closure could be called more than once
        // but should that happen, we will just not output anything
        // ... it shouldn't because we drop the capability
        let mut val_opt = Some(value);
        move |output| {
            if let (Some(cap), Some(val)) = (cap.as_mut(), val_opt.take()) {
                output.session(&cap).give(val);
            }
            cap = None;
        }
    })
}