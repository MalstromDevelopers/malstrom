use indexmap::IndexMap;

use itertools::Itertools;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    channels::operator_io::{Input, Output},
    stream::{BuildContext, JetStreamBuilder, OperatorBuilder, OperatorContext},
    types::{Data, DataMessage, Key, MaybeData, MaybeKey, MaybeTime, Message, Timestamp},
};

pub trait StatefulLogic<K, VI, T, VO, S>: 'static {
    /// Return Some to retain the key-state and None to discard it
    fn on_data(
        &mut self,
        msg: DataMessage<K, VI, T>,
        key_state: S,
        output: &mut Output<K, VO, T>,
    ) -> Option<S>;

    #[allow(unused)]
    fn on_epoch(
        &mut self,
        epoch: &T,
        state: &mut IndexMap<K, S>,
        output: &mut Output<K, VO, T>,
    ) -> () {
    }

    #[allow(unused)]
    fn on_schedule(&mut self, state: &mut IndexMap<K, S>, output: &mut Output<K, VO, T>) -> () {}
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

pub trait StatefulOp<K, VI, T>: super::sealed::Sealed {
    fn stateful_op<VO: Data, S: Default + Serialize + DeserializeOwned + 'static>(
        self,
        name: &str,
        logic: impl StatefulLogic<K, VI, T, VO, S>,
    ) -> JetStreamBuilder<K, VO, T>;
}

impl<K, VI, T> StatefulOp<K, VI, T> for JetStreamBuilder<K, VI, T>
where
    K: Key + Serialize + DeserializeOwned,
    VI: Data + Serialize + DeserializeOwned,
    T: Timestamp,
{
    fn stateful_op<VO: Data, S: Default + Serialize + DeserializeOwned + 'static>(
        self,
        name: &str,
        logic: impl StatefulLogic<K, VI, T, VO, S>,
    ) -> JetStreamBuilder<K, VO, T> {
        let op = OperatorBuilder::built_by(name, move |ctx| build_stateful_logic(ctx, logic));
        self.then(op)
    }
}

fn build_stateful_logic<
    K: Key + Serialize + DeserializeOwned,
    VI,
    T: MaybeTime,
    VO: Clone,
    S: Default + Serialize + DeserializeOwned,
>(
    context: &BuildContext,
    mut logic: impl StatefulLogic<K, VI, T, VO, S>,
) -> impl FnMut(&mut Input<K, VI, T>, &mut Output<K, VO, T>, &mut OperatorContext) {
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
                if let Some(x) = state.remove(&c.key) {
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
        runtime::CommunicationClient,
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

        let foo_enc = CommunicationClient::encode("foo".to_string());
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

        let state = IndexMap::from([(42, CommunicationClient::encode("HelloWorld".to_owned()))]);

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

        let state: IndexMap<bool, i32> = CommunicationClient::decode(&backend.load(&42).unwrap());
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
