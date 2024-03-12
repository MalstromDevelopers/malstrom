use itertools::Itertools;

use super::operator::{
    AppendableOperator, BuildContext, BuildableOperator, OperatorBuilder, RunnableOperator,
};
use crate::{
    channels::selective_broadcast::{self, Sender},
    snapshot::PersistenceBackend,
    time::MaybeTime,
    Data, Key, MaybeKey,
};

pub struct JetStreamBuilder<K, V, T, P> {
    operators: Vec<Box<dyn BuildableOperator<P>>>,
    // these are probes for every operator in operators
    tail: Box<dyn AppendableOperator<K, V, T, P>>,
}

impl<K, V, T, P> JetStreamBuilder<K, V, T, P>
where
    K: MaybeKey,
    V: Data,
    T: MaybeTime,
    P: 'static,
{
    pub(crate) fn from_operator<KI: MaybeKey, VI: Data, TI: MaybeTime>(
        operator: OperatorBuilder<KI, VI, TI, K, V, T, P>,
    ) -> JetStreamBuilder<K, V, T, P> {
        JetStreamBuilder {
            operators: Vec::new(),
            tail: Box::new(operator),
        }
    }
}

impl<K, V, T, P> JetStreamBuilder<K, V, T, P>
where
    K: MaybeKey,
    V: Data,
    T: MaybeTime,
    P: 'static,
{
    pub fn get_output_mut(&mut self) -> &mut Sender<K, V, T, P> {
        self.tail.get_output_mut()
    }

    /// add an operator to the end of this stream
    /// and return a new stream where the new operator is last_op
    pub fn then<KO: MaybeKey, VO: Data, TO: MaybeTime>(
        mut self,
        mut operator: OperatorBuilder<K, V, T, KO, VO, TO, P>,
    ) -> JetStreamBuilder<KO, VO, TO, P> {
        // let (tx, rx) = selective_broadcast::<O>();
        selective_broadcast::link(self.tail.get_output_mut(), operator.get_input_mut());

        let old_tail_f = self.tail.into_buildable();
        self.operators.push(old_tail_f);

        JetStreamBuilder {
            operators: self.operators,
            tail: Box::new(operator),
        }
    }

    pub(crate) fn into_operators(mut self) -> Vec<Box<dyn BuildableOperator<P>>> {
        self.operators.push(self.tail.into_buildable());
        self.operators
    }

    pub(crate) fn operator_count(&self) -> usize {
        // + 1 for the tail
        self.operators.len() + 1
    }
}
