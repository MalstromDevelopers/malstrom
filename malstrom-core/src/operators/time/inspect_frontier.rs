use std::marker::PhantomData;

use crate::{
    channels::operator_io::{Input, Output},
    stream::{Malstrom as _, Operator, OperatorContext, SafeLogic, StreamBuilder},
    types::{Kvt, MaybeData, MaybeKey, Message, Sealed, Timestamp},
};

/// Inspect the time frontier on a stream
pub trait InspectFrontier<In: Kvt>: Sealed {
    /// Observe the frontier (i.e. the current epoch) in a stream without modifying
    /// either values or time.
    ///
    /// # Arguments
    /// * `inspector` - A function which gets called with a reference to the timestamp of any Epoch encountered
    fn inspect_frontier(
        self,
        name: impl Into<String>,
        inspector: impl FnMut(&In::Timestamp, &OperatorContext) + 'static,
    ) -> StreamBuilder<In>;
}

impl<Msg> InspectFrontier<Msg> for StreamBuilder<Msg>
where
    Msg: Kvt,
{
    fn inspect_frontier(
        self,
        name: impl Into<String>,
        mut inspector: impl FnMut(&Msg::Timestamp, &OperatorContext) + 'static,
    ) -> StreamBuilder<Msg> {
        self.then(Operator::direct(
            name.into(),
            InspectFrontierOp {
                inspector,
                _msg_type: PhantomData::<Msg>,
            }
            .into_logic(),
        ))
    }
}

struct InspectFrontierOp<Msg: Kvt, F> {
    inspector: F,
    _msg_type: PhantomData<Msg>,
}
impl<Msg, F> SafeLogic<Msg, Msg> for InspectFrontierOp<Msg, F>
where
    Msg: Kvt,
    F: FnMut(&Msg::Timestamp, &OperatorContext) + 'static,
{
    fn on_data(
        &mut self,
        data_message: crate::types::DataMessage<Msg>,
        output: &mut Output<Msg>,
        ctx: &mut OperatorContext,
    ) {
        output.send(Message::Data(data_message));
    }

    fn on_epoch(
        &mut self,
        epoch: &<Msg as Kvt>::Timestamp,
        output: &mut Output<Msg>,
        ctx: &mut OperatorContext,
    ) {
        (self.inspector)(epoch, ctx)
    }
}
