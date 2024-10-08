

use crate::types::MaybeTime;
use crate::types::{Data, MaybeKey};

mod builder;
mod context;
mod runnable;
mod standard;
mod traits;

pub use builder::{Logic, OperatorBuilder};
pub use context::{BuildContext, OperatorContext};
pub use runnable::RunnableOperator;
pub use traits::{AppendableOperator, BuildableOperator};

/// An Operator which does nothing except passing data along
pub(crate) fn pass_through_operator<K: MaybeKey, V: Data, T: MaybeTime>(
) -> OperatorBuilder<K, V, T, K, V, T> {
    let mut op = OperatorBuilder::direct(|input, output, _ctx| {
        if let Some(x) = input.recv() {
            output.send(x)
        }
    });
    op.label("pass_through".into());
    op
}

#[cfg(test)]
mod test {
    use std::i32;

    use super::{pass_through_operator, AppendableOperator, OperatorBuilder};
    use crate::{
        channels::selective_broadcast::{full_broadcast, link, Sender}, snapshot::NoPersistence, stream::operator::BuildContext, testing::NoCommunication, types::{Message, NoData, NoKey}
    };

    /// The operator should report as finished once the MAX time has passed it
    #[test]
    fn becomes_finished() {
        let mut builder: OperatorBuilder<NoKey, NoData, i32, NoKey, NoData, i32> =
            pass_through_operator();
        let mut sender = Sender::new_unlinked(full_broadcast);
        link(&mut sender, builder.get_input_mut());

        let buildable = Box::new(builder).into_buildable();
        let mut comm = NoCommunication;
        let mut ctx = BuildContext::new(
            0,
            0,
            "".to_owned(),
            Box::<NoPersistence>::default(),
            &mut comm,
            0..1,
        );
        let mut op = Box::new(buildable).into_runnable(&mut ctx);

        assert!(!op.is_finished());
        op.step(&mut comm);
        assert!(!op.is_finished());

        sender.send(Message::Epoch(42));
        op.step(&mut comm);
        assert!(!op.is_finished());

        sender.send(Message::Epoch(i32::MAX));
        assert!(!op.is_finished());
        op.step(&mut comm);
        // now finally finished
        assert!(op.is_finished());
    }
}
