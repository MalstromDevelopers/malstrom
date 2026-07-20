use crate::channels::operator_io::{Input, Output, link};
use crate::stream::InitialStreamBuilder;
use crate::stream::{Operator, SafeLogic, StreamBuilder};
use crate::types::{DataMessage, Kvt, MaybeData, MaybeKey, MaybeTime, Message, Sealed};
use std::marker::PhantomData;
use std::rc::Rc;

pub trait Union<Msg: Kvt>: Sealed {
    fn union(
        self,
        name: impl Into<String>,
        inputs: impl IntoIterator<Item = StreamBuilder<Msg>>,
    ) -> StreamBuilder<Msg>;
}

impl<Msg> Union<Msg> for StreamBuilder<Msg>
where
    Msg: Kvt,
    Msg::Key: MaybeKey,
    Msg::Value: MaybeData,
    Msg::Timestamp: MaybeTime,
{
    fn union(
        self,
        name: impl Into<String>,
        inputs: impl IntoIterator<Item = StreamBuilder<Msg>>,
    ) -> StreamBuilder<Msg> {
        let rt = self.get_runtime();
        let mut unioned_input = Input::new_unlinked();
        let name: String = name.into();

        for (i, mut stream) in std::iter::once(self).chain(inputs.into_iter()).enumerate() {
            // add a dummy operator to forward messages, we must do this because we can
            // not get an output out of a StreamBuilder
            let mut forward_op = Operator::direct(
                format!("{}-{i}", name),
                Forward(PhantomData::<Msg>).into_logic(),
            );
            // the forward operator gets the streams tail input, i.e. the input which receives from the
            // last operator in the given stream
            std::mem::swap(&mut stream.tail, &mut forward_op.input);
            // link our forward output to the unioned input
            link(&mut forward_op.output, &mut unioned_input);
            rt.lock().unwrap().add_operator(forward_op);
        }
        StreamBuilder {
            tail: unioned_input,
            runtime: rt,
        }
    }
}

struct Forward<Msg>(PhantomData<Msg>);
impl<Msg> SafeLogic<Msg, Msg> for Forward<Msg>
where
    Msg: Kvt,
{
    async fn on_data(
        &mut self,
        data_message: DataMessage<Msg>,
        output: &mut Output<Msg>,
        ctx: &mut crate::stream::OperatorContext,
    ) {
        output.send(Message::Data(data_message)).await;
    }
}
