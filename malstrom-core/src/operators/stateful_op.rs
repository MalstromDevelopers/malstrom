use indexmap::IndexMap;

use itertools::Itertools;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    channels::operator_io::{Input, Output},
    stream::{BuildContext, Logic, OperatorBuilder, StreamBuilder},
    types::{Data, DataMessage, Key, MaybeData, MaybeKey, MaybeTime, Message, Timestamp},
};

/// Helper trait for implementing arbitrary stateful operators for datastreams.
/// For simpler stateful operations see [malstrom::operators::stateful_map]
pub trait StatefulLogic<K, VI, T, VO, S>: 'static {
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
    fn on_data(
        &mut self,
        msg: DataMessage<K, VI, T>,
        key_state: S,
        output: &mut Output<K, VO, T>,
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
    fn on_epoch(&mut self, epoch: &T, state: &mut IndexMap<K, S>, output: &mut Output<K, VO, T>) {}

    /// Called whenever this operator is scheduled. There is no guarantee on whether this function
    /// will be called before or after other handler functions.
    ///
    /// # Arguments
    /// - state: States of all keys currently located at this worker and this operator
    /// - output: Operator output
    ///
    /// The default implementation is a no-op
    #[allow(unused)]
    fn on_schedule(&mut self, state: &mut IndexMap<K, S>, output: &mut Output<K, VO, T>) {}
}
impl<X, K, VI, T, VO, S> StatefulLogic<K, VI, T, VO, S> for X
where
    for<'a> X: FnMut(DataMessage<K, VI, T>, S, &'a mut Output<K, VO, T>) -> Option<S> + 'static,
    K: MaybeKey,
    VO: MaybeData,
    T: MaybeTime,
{
    fn on_data(
        &mut self,
        msg: DataMessage<K, VI, T>,
        key_state: S,
        output: &mut Output<K, VO, T>,
    ) -> Option<S> {
        self(msg, key_state, output)
    }
}

/// Append a stateful operator to the stream
pub trait StatefulOp<K, VI, T>: super::sealed::Sealed {
    /// Append an arbitrary stateful operator to the datastream.
    fn stateful_op<VO: Data, S: Default + Serialize + DeserializeOwned + 'static>(
        self,
        name: &str,
        logic: impl StatefulLogic<K, VI, T, VO, S>,
    ) -> StreamBuilder<K, VO, T>;
}

impl<K, VI, T> StatefulOp<K, VI, T> for StreamBuilder<K, VI, T>
where
    K: Key + Serialize + DeserializeOwned,
    VI: Data + Serialize + DeserializeOwned,
    T: Timestamp,
{
    fn stateful_op<VO: Data, S: Default + Serialize + DeserializeOwned + 'static>(
        self,
        name: &str,
        logic: impl StatefulLogic<K, VI, T, VO, S>,
    ) -> StreamBuilder<K, VO, T> {
        let op = OperatorBuilder::built_by(name, move |ctx| build_stateful_logic(ctx, logic));
        self.then(op)
    }
}

fn build_stateful_logic<
    K: Key + Serialize + DeserializeOwned,
    VI,
    T: MaybeTime,
    VO: Clone,
    S: Default + Serialize + DeserializeOwned + 'static,
>(
    context: &BuildContext,
    mut logic: impl StatefulLogic<K, VI, T, VO, S>,
) -> impl Logic<K, VI, T, K, VO, T> {
    let mut state: IndexMap<K, S> = context.load_state().unwrap_or_default();

    move |input: &mut Input<K, VI, T>, output: &mut Output<K, VO, T>, ctx| {
        logic.on_schedule(&mut state, output);
        let msg = match input.recv() {
            Some(x) => x,
            None => return,
        };
        match msg {
            Message::Data(msg) => {
                let key = msg.key.to_owned();
                let key_state = state.swap_remove(&key).unwrap_or_default();
                let new_state = logic.on_data(msg, key_state, output);
                if let Some(n) = new_state {
                    state.insert(key.to_owned(), n);
                }
            }
            Message::Interrogate(mut x) => {
                x.add_keys(&(state.keys().map(|k| k.to_owned()).collect_vec()));
                output.send(Message::Interrogate(x))
            }
            Message::Collect(mut c) => {
                if let Some(x) = state.swap_remove(&c.key) {
                    c.add_state(ctx.operator_id, x);
                }
                output.send(Message::Collect(c))
            }
            Message::Acquire(a) => {
                if let Some(st) = a.take_state(&ctx.operator_id) {
                    state.insert(st.0, st.1);
                }
                output.send(Message::Acquire(a))
            }
            Message::AbsBarrier(mut b) => {
                b.persist(&state, &ctx.operator_id);
                output.send(Message::AbsBarrier(b))
            }
            Message::Rescale(x) => output.send(Message::Rescale(x)),
            Message::SuspendMarker(x) => output.send(Message::SuspendMarker(x)),
            Message::Epoch(x) => {
                logic.on_epoch(&x, &mut state, output);
                output.send(Message::Epoch(x))
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use indexmap::{IndexMap, IndexSet};

    use crate::{
        keyed::distributed::{Acquire, Collect, Interrogate},
        runtime::BiCommunicationClient,
        snapshot::{Barrier, PersistenceClient},
        testing::{CapturingPersistenceBackend, OperatorTester},
        types::*,
    };

    use super::*;

    #[test]
    fn test_interrogate() {
        // logic which always just sets the last value as state
        let logic = |msg: DataMessage<i32, String, NoTime>,
                     _state: String,
                     _output: &mut Output<i32, (), NoTime>| Some(msg.value);

        let mut tester: OperatorTester<i32, String, NoTime, i32, (), NoTime, ()> =
            OperatorTester::built_by(move |ctx| build_stateful_logic(ctx, logic), 0, 0, 0..1);

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
    #[test]
    fn test_interrogate_discarded() {
        // logic which only returns state if the String len is <= 3
        let logic = |msg: DataMessage<i32, String, NoTime>,
                     _state: String,
                     _output: &mut Output<i32, (), NoTime>| {
            if msg.value.len() > 3 {
                None
            } else {
                Some(msg.value)
            }
        };

        let mut tester: OperatorTester<i32, String, NoTime, i32, (), NoTime, ()> =
            OperatorTester::built_by(move |ctx| build_stateful_logic(ctx, logic), 0, 0, 0..1);

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
    #[test]
    fn test_collect() {
        // logic which always just sets the last value as state
        let logic = |msg: DataMessage<i32, String, NoTime>,
                     _state: String,
                     _output: &mut Output<i32, (), NoTime>| Some(msg.value);

        let mut tester: OperatorTester<i32, String, NoTime, i32, (), NoTime, ()> =
            OperatorTester::built_by(move |ctx| build_stateful_logic(ctx, logic), 0, 42, 0..1);

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
    #[test]
    fn test_collect_discarded() {
        // logic which only returns state if the String len is <= 3
        let logic = |msg: DataMessage<i32, String, NoTime>,
                     _state: String,
                     _output: &mut Output<i32, (), NoTime>| {
            if msg.value.len() > 3 {
                None
            } else {
                Some(msg.value)
            }
        };

        let mut tester: OperatorTester<i32, String, NoTime, i32, (), NoTime, ()> =
            OperatorTester::built_by(move |ctx| build_stateful_logic(ctx, logic), 0, 42, 0..1);

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
    #[test]
    fn test_acquire_state() {
        // logic which always returns the state as a message and
        // sets the message value as state
        let logic = |mut msg: DataMessage<i32, String, NoTime>,
                     mut state: String,
                     output: &mut Output<i32, String, NoTime>| {
            std::mem::swap(&mut state, &mut msg.value);
            output.send(Message::Data(msg));
            Some(state)
        };

        let mut tester: OperatorTester<i32, String, NoTime, i32, String, NoTime, ()> =
            OperatorTester::built_by(move |ctx| build_stateful_logic(ctx, logic), 0, 42, 0..1);

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
    #[test]
    fn test_drop_key_state() {
        // logic which keeps a total per key and emits it
        let logic = |msg: DataMessage<bool, i32, NoTime>,
                     state: i32,
                     output: &mut Output<bool, i32, NoTime>| {
            let new_value = state + msg.value;
            output.send(Message::Data(DataMessage::new(
                msg.key,
                new_value,
                msg.timestamp,
            )));
            Some(new_value)
        };
        // keep a total per key
        let mut tester: OperatorTester<bool, i32, NoTime, bool, i32, NoTime, ()> =
            OperatorTester::built_by(move |ctx| build_stateful_logic(ctx, logic), 0, 42, 0..1);

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
    #[test]
    fn test_snapshot_state() {
        // logic which keeps a total per key and emits it
        let logic = |msg: DataMessage<bool, i32, NoTime>,
                     state: i32,
                     output: &mut Output<bool, i32, NoTime>| {
            let new_value = state + msg.value;
            output.send(Message::Data(DataMessage::new(
                msg.key,
                new_value,
                msg.timestamp,
            )));
            Some(new_value)
        };
        // keep a total per key
        let mut tester: OperatorTester<bool, i32, NoTime, bool, i32, NoTime, ()> =
            OperatorTester::built_by(move |ctx| build_stateful_logic(ctx, logic), 0, 42, 0..1);

        tester.send_local(Message::Data(DataMessage::new(false, 1, NoTime)));
        tester.step();

        let backend = CapturingPersistenceBackend::default();
        tester.send_local(Message::AbsBarrier(Barrier::new(Box::new(backend.clone()))));
        tester.step();

        let state: IndexMap<bool, i32> = BiCommunicationClient::decode(&backend.load(&42).unwrap());
        assert_eq!(*state.get(&false).unwrap(), 1);
    }

    #[test]
    fn test_forward_system_messages() {
        // logic which does nothing
        let logic = |_msg: DataMessage<i32, String, NoTime>,
                     _state: String,
                     _output: &mut Output<i32, (), NoTime>| None;

        let mut tester: OperatorTester<i32, String, NoTime, i32, (), NoTime, ()> =
            OperatorTester::built_by(move |ctx| build_stateful_logic(ctx, logic), 0, 42, 0..1);

        crate::testing::test_forward_system_messages(&mut tester);
    }
}
