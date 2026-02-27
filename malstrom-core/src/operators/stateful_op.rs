use std::marker::PhantomData;

use indexmap::IndexMap;

use itertools::Itertools;
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    channels::operator_io::{Input, Output},
    stream::{
        BuildContext, DirectLogic, Logic, LogicBuilder, Malstrom, Operator, OperatorContext,
        SafeLogic, SafeLogicWrapper, StreamBuilder,
    },
    types::{
        Data, DataMessage, Key, Kvt, MaybeData, MaybeKey, MaybeTime, Message, Sealed, Timestamp,
    },
};

pub trait State: Serialize + DeserializeOwned + Default + 'static {}
impl<X> State for X where X: Default + Serialize + DeserializeOwned + 'static {}

/// Helper trait for implementing arbitrary stateful operators for datastreams.
/// For simpler stateful operations see [malstrom::operators::stateful_map]
pub trait StatefulLogic<In: Kvt, T: Data, S>: 'static {
    /// Process a single datamessage.
    /// This function receives an owned value of the given message and the state for the message's
    /// key. If there is no state for the key, the default state is given.
    /// Returning `Some(x)` retains `x` as the new key state, returning `None` discards the state
    /// for this key.
    ///
    /// This function is essentially free to do almost anythingg: The message value may be changed,
    /// the entire message dropped or mutliple output messages produced. Two important restrictions
    /// apply though:
    ///
    /// 1. All output messages must be of the same key as the input message
    /// 2. The timestamp of any output message may not be smaller than the timestamp of the last
    ///    Epoch received at this operator. If the value of the last Epoch is unknown, it is always
    ///    safe to produce timestamps equal to or greater than the current input message.
    async fn on_data(
        &mut self,
        msg: DataMessage<In>,
        key_state: S,
        output: &mut Output<(In::Key, T, In::Timestamp)>,
    ) -> Option<S>;

    /// Handle an epoch arriving at this operator.
    ///
    /// # Arguments
    /// - epoch: Current Epoch
    /// - state: States of all keys currently located at this worker and this operator
    /// - output: Operator output
    ///
    /// **NOTE:** It **is** allowed to emit messages with a timestamp smaller or equal to the epoch
    /// from this function, as the epoch will be sent into the output **after** the function
    /// returns.
    ///
    /// The default implementation is a no-op
    #[allow(unused)]
    async fn on_epoch(
        &mut self,
        epoch: &<In as Kvt>::Timestamp,
        state: &mut IndexMap<<In as Kvt>::Key, S>,
        output: &mut Output<(In::Key, T, In::Timestamp)>,
    ) {
    }

    /// Called whenever this operator is scheduled. There is no guarantee on whether this function
    /// will be called before or after other handler functions.
    ///
    /// # Arguments
    /// - state: States of all keys currently located at this worker and this operator
    /// - output: Operator output
    ///
    /// The default implementation is a no-op
    #[allow(unused)]
    async fn on_schedule(
        &mut self,
        state: &mut IndexMap<<In as Kvt>::Key, S>,
        output: &mut Output<(In::Key, T, In::Timestamp)>,
    ) {
    }
}

/// Append a stateful operator to the stream
pub trait StatefulOp<In: Kvt, T: Data>: Sealed {
    /// Append an arbitrary stateful operator to the datastream.
    fn stateful_op<L: StatefulLogic<In, T, S>, S: State + Default + 'static>(
        self,
        name: impl Into<String>,
        logic: L,
    ) -> StreamBuilder<(In::Key, T, In::Timestamp)>;
}

impl<In, T> StatefulOp<In, T> for StreamBuilder<In>
where
    In: Kvt,
    <In as Kvt>::Key: State + Key,
    T: Data,
{
    fn stateful_op<L: StatefulLogic<In, T, S>, S: State>(
        self,
        name: impl Into<String>,
        logic: L,
    ) -> StreamBuilder<(In::Key, T, In::Timestamp)> {
        let op = Operator::built_by(name.into(), StatefulLogicBuilder::new(logic));
        self.then(op)
    }
}

#[derive(Default)]
struct StatefulLogicBuilder<In, T, S, L> {
    _io_types: PhantomData<(In, T)>,
    _state_type: PhantomData<S>,
    logic: L,
}

impl<In, T, S, L> StatefulLogicBuilder<In, T, S, L> {
    fn new(logic: L) -> Self {
        StatefulLogicBuilder {
            _io_types: PhantomData::<(In, T)>,
            _state_type: PhantomData::<S>,
            logic,
        }
    }
}

impl<In, T, S, L> LogicBuilder<In, (In::Key, T, In::Timestamp)>
    for StatefulLogicBuilder<In, T, S, L>
where
    In: Kvt,
    <In as Kvt>::Key: State + Key,
    T: Data,
    S: State,
    L: StatefulLogic<In, T, S>,
{
    type Logic = SafeLogicWrapper<StatefulLogicWrapper<In, T, S, L>>;

    async fn build(self, ctx: &mut BuildContext) -> Self::Logic {
        let state: IndexMap<<In as Kvt>::Key, S> = ctx.load_state().await.unwrap_or_default();
        StatefulLogicWrapper {
            state,
            logic: self.logic,
            _output: PhantomData::<T>,
        }
        .into_logic()
    }
}

struct StatefulLogicWrapper<In, T, S, L>
where
    In: Kvt,
{
    state: IndexMap<<In as Kvt>::Key, S>,
    logic: L,
    _output: PhantomData<T>,
}

impl<In, T, S, L> SafeLogic<In, (In::Key, T, In::Timestamp)> for StatefulLogicWrapper<In, T, S, L>
where
    L: StatefulLogic<In, T, S>,
    In: Kvt,
    T: Data,
    <In as Kvt>::Key: Key + State,
    S: State + 'static,
{
    async fn on_schedule(
        &mut self,
        output: &mut Output<(In::Key, T, In::Timestamp)>,
        ctx: &mut OperatorContext,
    ) {
        self.logic.on_schedule(&mut self.state, output).await;
    }

    async fn on_data(
        &mut self,
        msg: DataMessage<In>,
        output: &mut Output<(In::Key, T, In::Timestamp)>,
        ctx: &mut OperatorContext,
    ) {
        let key = msg.key.to_owned();
        let key_state = self.state.swap_remove(&key).unwrap_or_default();
        let new_state = self.logic.on_data(msg, key_state, output).await;
        if let Some(n) = new_state {
            self.state.insert(key.to_owned(), n);
        }
    }

    async fn on_epoch(
        &mut self,
        epoch: &<In as Kvt>::Timestamp,
        output: &mut Output<(In::Key, T, In::Timestamp)>,
        ctx: &mut OperatorContext,
    ) {
        self.logic.on_epoch(epoch, &mut self.state, output).await;
    }

    async fn on_barrier(
        &mut self,
        barrier: &mut crate::snapshot::SnapshotBarrier,
        output: &mut Output<(In::Key, T, In::Timestamp)>,
        ctx: &mut OperatorContext,
    ) {
        barrier.persist(&self.state, &ctx.operator_id);
    }

    async fn on_interrogate(
        &mut self,
        interrogate: &mut crate::keyed::distributed::Interrogate<<In as Kvt>::Key>,
        output: &mut Output<(In::Key, T, In::Timestamp)>,
        ctx: &mut OperatorContext,
    ) {
        interrogate.add_keys(&(self.state.keys().map(|k| k.to_owned()).collect_vec()));
    }

    async fn on_collect(
        &mut self,
        collect: &mut crate::keyed::distributed::Collect<<In as Kvt>::Key>,
        output: &mut Output<(In::Key, T, In::Timestamp)>,
        ctx: &mut OperatorContext,
    ) {
        if let Some(x) = self.state.swap_remove(&collect.key) {
            collect.add_state(ctx.operator_id, x);
        }
    }

    async fn on_acquire(
        &mut self,
        acquire: &mut crate::keyed::distributed::Acquire<<In as Kvt>::Key>,
        output: &mut Output<(In::Key, T, In::Timestamp)>,
        ctx: &mut OperatorContext,
    ) {
        if let Some(st) = acquire.take_state(&ctx.operator_id) {
            self.state.insert(st.0, st.1);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use indexmap::{IndexMap, IndexSet};

    use crate::{
        keyed::distributed::{Acquire, Collect, Interrogate},
        runtime::BiCommunicationClient,
        snapshot::{SnapshotBarrier, PersistenceClient},
        testing::{CapturingPersistenceBackend, OperatorTester},
        types::*,
    };

    use super::*;

    impl<F, In, Val, St> StatefulLogic<In, Val, St> for F
    where
        In: Kvt,
        Val: Data,
        F: AsyncFnMut(
                DataMessage<In>,
                St,
                &mut Output<(In::Key, Val, In::Timestamp)>,
            ) -> Option<St>
            + 'static,
    {
        async fn on_data(
            &mut self,
            msg: DataMessage<In>,
            key_state: St,
            output: &mut Output<(<In as Kvt>::Key, Val, <In as Kvt>::Timestamp)>,
        ) -> Option<St> {
            self(msg, key_state, output).await
        }
    }

    #[tokio::test]
    async fn test_interrogate() {
        // logic which always just sets the last value as state
        let logic =
            async |msg: DataMessage<(i32, String, NoTime)>,
                   _state: String,
                   _output: &mut Output<(i32, (), NoTime)>| { Some(msg.value) };

        let logic_builder = StatefulLogicBuilder::new(logic);
        let mut tester: OperatorTester<(i32, String, NoTime), (i32, (), NoTime), _, ()> =
            OperatorTester::built_by(logic_builder, 0, 0, 0..1).await;

        tester.send_local(Message::Data(DataMessage::new(
            1,
            "foo".to_string(),
            NoTime,
        )));
        tester.step();
        tester.send_local(Message::Data(DataMessage::new(
            5,
            "bar".to_string(),
            NoTime,
        )));
        tester.step();

        let interrogator = Interrogate::new(Rc::new(|_: &i32| true));
        tester.send_local(Message::Interrogate(interrogator.clone()));
        tester.step();

        // receive and drop all messages. We need to drop the interrogator copy
        // so we can unwrap it
        while tester.recv_local().is_some() {}

        let result = interrogator.try_unwrap().unwrap();
        assert_eq!(IndexSet::from([1, 5]), result)
    }

    /// Check we do not add discarded keys
    #[tokio::test]
    async fn test_interrogate_discarded() {
        // logic which only returns state if the String len is <= 3
        let logic = async |msg: DataMessage<(i32, String, NoTime)>,
                           _state: String,
                           _output: &mut Output<(i32, (), NoTime)>| {
            if msg.value.len() > 3 {
                None
            } else {
                Some(msg.value)
            }
        };

        let mut tester: OperatorTester<_, _, _, ()> =
            OperatorTester::built_by(StatefulLogicBuilder::new(logic), 0, 0, 0..1).await;

        tester.send_local(Message::Data(DataMessage::new(
            1,
            "foo".to_string(),
            NoTime,
        )));
        tester.step();
        tester.send_local(Message::Data(DataMessage::new(
            1,
            "hello".to_string(),
            NoTime,
        )));
        tester.step();
        let interrogator = Interrogate::new(Rc::new(|_: &i32| true));
        tester.send_local(Message::Interrogate(interrogator.clone()));
        tester.step();

        // receive and drop all messages. We need to drop the interrogator copy
        // so we can unwrap it
        while tester.recv_local().is_some() {}

        let result = interrogator.try_unwrap().unwrap();
        assert!(result.is_empty());
    }

    /// Check key state is collected
    #[tokio::test]
    async fn test_collect() {
        // logic which always just sets the last value as state
        let logic =
            async |msg: DataMessage<(i32, String, NoTime)>,
                   _state: String,
                   _output: &mut Output<(i32, (), NoTime)>| Some(msg.value);

        let mut tester: OperatorTester<_, _, _, ()> =
            OperatorTester::built_by(StatefulLogicBuilder::new(logic), 0, 42, 0..1).await;

        tester.send_local(Message::Data(DataMessage::new(
            1,
            "foo".to_string(),
            NoTime,
        )));
        tester.step();
        tester.send_local(Message::Data(DataMessage::new(
            5,
            "bar".to_string(),
            NoTime,
        )));
        tester.step();
        let collector = Collect::new(1);
        tester.send_local(Message::Collect(collector.clone()));
        tester.step();

        // receive and drop all messages. We need to drop the interrogator copy
        // so we can unwrap it
        while tester.recv_local().is_some() {}

        let foo_enc = BiCommunicationClient::encode("foo".to_string());
        let (_key, result) = collector.try_unwrap().unwrap();
        // 42 is the operator id
        assert_eq!(IndexMap::from([(42, foo_enc)]), result)
    }

    /// check we do not collect discarded state
    #[tokio::test]
    async fn test_collect_discarded() {
        // logic which only returns state if the String len is <= 3
        let logic = async |msg: DataMessage<(i32, String, NoTime)>,
                           _state: String,
                           _output: &mut Output<(i32, (), NoTime)>| {
            if msg.value.len() > 3 {
                None
            } else {
                Some(msg.value)
            }
        };

        let mut tester: OperatorTester<_, _, _, ()> =
            OperatorTester::built_by(StatefulLogicBuilder::new(logic), 0, 42, 0..1).await;

        tester.send_local(Message::Data(DataMessage::new(
            1,
            "foo".to_string(),
            NoTime,
        )));
        tester.step();
        tester.send_local(Message::Data(DataMessage::new(
            1,
            "hello".to_string(),
            NoTime,
        )));
        tester.step();
        let collector = Collect::new(1);
        tester.send_local(Message::Collect(collector.clone()));
        tester.step();

        // receive and drop all messages. We need to drop the interrogator copy
        // so we can unwrap it
        while tester.recv_local().is_some() {}

        let (_key, result) = collector.try_unwrap().unwrap();
        assert!(result.is_empty());
    }

    // check we acquire state when instructed
    #[tokio::test]
    async fn test_acquire_state() {
        // logic which always returns the state as a message and
        // sets the message value as state
        let logic = async |mut msg: DataMessage<(i32, String, NoTime)>,
                           mut state: String,
                           output: &mut Output<(i32, String, NoTime)>| {
            std::mem::swap(&mut state, &mut msg.value);
            output.send(Message::Data(msg));
            Some(state)
        };

        let mut tester: OperatorTester<_, _, _, ()> =
            OperatorTester::built_by(StatefulLogicBuilder::new(logic), 0, 42, 0..1).await;

        let state = IndexMap::from([(42, BiCommunicationClient::encode("HelloWorld".to_owned()))]);

        tester.send_local(Message::Acquire(Acquire::new(1337, state)));
        tester.step();
        tester.send_local(Message::Data(DataMessage::new(1337, "".to_owned(), NoTime)));
        tester.step();
        assert!(matches!(tester.recv_local().unwrap(), Message::Acquire(_)));
        match tester.recv_local().unwrap() {
            Message::Data(DataMessage {
                key: 1337,
                value: x,
                timestamp: NoTime,
            }) => assert_eq!(x, "HelloWorld"),
            _ => panic!(),
        }
    }

    // check we drop key state when instructed
    #[tokio::test]
    async fn test_drop_key_state() {
        // logic which keeps a total per key and emits it
        let logic = async |msg: DataMessage<(bool, i32, NoTime)>,
                           state: i32,
                           output: &mut Output<(bool, i32, NoTime)>| {
            let new_value = state + msg.value;
            output.send(Message::Data(DataMessage::new(
                msg.key,
                new_value,
                msg.timestamp,
            )));
            Some(new_value)
        };
        // keep a total per key
        let mut tester: OperatorTester<_, _, _, ()> =
            OperatorTester::built_by(StatefulLogicBuilder::new(logic), 0, 42, 0..1).await;

        tester.send_local(Message::Data(DataMessage::new(false, 1, NoTime)));
        tester.step();
        tester.recv_local().unwrap();
        tester.send_local(Message::Data(DataMessage::new(false, 2, NoTime)));
        tester.step();
        match tester.recv_local().unwrap() {
            Message::Data(d) => assert_eq!(d.value, 3),
            _ => panic!(),
        };

        tester.send_local(Message::Collect(Collect::new(false)));
        tester.step();
        tester.recv_local().unwrap();

        tester.send_local(Message::Data(DataMessage::new(false, 1, NoTime)));
        tester.step();
        // sum should be back to 1 since we dropped the state
        match tester.recv_local().unwrap() {
            Message::Data(d) => assert_eq!(d.value, 1),
            _ => panic!(),
        };
    }

    // check we snapshot state
    #[tokio::test]
    async fn test_snapshot_state() {
        // logic which keeps a total per key and emits it
        let logic = async |msg: DataMessage<(bool, i32, NoTime)>,
                           state: i32,
                           output: &mut Output<(bool, i32, NoTime)>| {
            let new_value = state + msg.value;
            output.send(Message::Data(DataMessage::new(
                msg.key,
                new_value,
                msg.timestamp,
            )));
            Some(new_value)
        };
        // keep a total per key
        let mut tester: OperatorTester<_, _, _, ()> =
            OperatorTester::built_by(StatefulLogicBuilder::new(logic), 0, 42, 0..1).await;

        tester.send_local(Message::Data(DataMessage::new(false, 1, NoTime)));
        tester.step();

        let backend = CapturingPersistenceBackend::default();
        tester.send_local(Message::AbsBarrier(SnapshotBarrier::new(Box::new(backend.clone()))));
        tester.step();

        let state: IndexMap<bool, i32> = BiCommunicationClient::decode(&backend.load(&42).unwrap());
        assert_eq!(*state.get(&false).unwrap(), 1);
    }

    #[tokio::test]
    async fn test_forward_system_messages() {
        // logic which does nothing
        let logic = async |_msg: DataMessage<(i32, String, usize)>,
                           _state: String,
                           _output: &mut Output<(i32, (), usize)>| None;

        let mut tester: OperatorTester<_, _, _, ()> =
            OperatorTester::built_by(StatefulLogicBuilder::new(logic), 0, 42, 0..1).await;

        crate::testing::test_forward_system_messages(&mut tester);
    }
}
