use std::marker::PhantomData;

use crate::{
    channels::operator_io::{Input, Output},
    stream::{Malstrom as _, Operator, OperatorContext, SafeLogic, StreamBuilder},
    types::{DataMessage, Kvt, MaybeData, MaybeKey, Message, Sealed, Timestamp},
};

/// Inspect the time frontier on a stream
pub trait InspectFrontier<In: Kvt, Func>: Sealed {
    /// Observe the frontier (i.e. the current epoch) in a stream without modifying
    /// either values or time.
    ///
    /// # Arguments
    /// * `inspector` - A function which gets called with a reference to the timestamp of any Epoch encountered
    fn inspect_frontier(self, name: impl Into<String>, inspector: Func) -> StreamBuilder<In>;
}

impl<Msg, Func, Fut> InspectFrontier<Msg, Func> for StreamBuilder<Msg>
where
    Msg: Kvt,
    Func: FnMut(&Msg::Timestamp, &OperatorContext) -> Fut + 'static,
    Fut: Future,
{
    fn inspect_frontier(self, name: impl Into<String>, inspector: Func) -> StreamBuilder<Msg> {
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

struct InspectFrontierOp<Msg: Kvt, Func> {
    inspector: Func,
    _msg_type: PhantomData<Msg>,
}
impl<Msg, Func, Fut> SafeLogic<Msg, Msg> for InspectFrontierOp<Msg, Func>
where
    Msg: Kvt,
    Func: FnMut(&Msg::Timestamp, &OperatorContext) -> Fut + 'static,
    Fut: Future,
{
    async fn on_data(
        &mut self,
        data_message: DataMessage<Msg>,
        output: &mut Output<Msg>,
        ctx: &mut OperatorContext<'_>,
    ) {
        output.send(Message::Data(data_message));
    }

    async fn on_epoch(
        &mut self,
        epoch: &<Msg as Kvt>::Timestamp,
        output: &mut Output<Msg>,
        ctx: &mut OperatorContext<'_>,
    ) {
        (self.inspector)(epoch, ctx).await;
    }
}
