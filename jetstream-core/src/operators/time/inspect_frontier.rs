use crate::channels::selective_broadcast::Receiver;
use crate::stream::JetStreamBuilder;
use crate::stream::OperatorBuilder;

use crate::stream::OperatorContext;
use crate::types::{MaybeData, MaybeKey, Message, Timestamp};

pub trait InspectFrontier<K, V, T> {
    /// Observe the frontier (i.e. the current epoch) in a stream without modifying
    /// either values or time.
    /// 
    /// # Arguments
    /// * `inspector` - A function which gets called with a reference to the timestamp of any Epoch encountered
    fn inspect_frontier(self, inspector: impl FnMut(&T, &OperatorContext) + 'static) -> JetStreamBuilder<K, V, T>;
}

impl<K, V, T> InspectFrontier<K, V, T> for JetStreamBuilder<K, V, T>
where
    K: MaybeKey,
    V: MaybeData,
    T: Timestamp,
{
    fn inspect_frontier(self, mut inspector: impl FnMut(&T,  &OperatorContext) + 'static) -> JetStreamBuilder<K, V, T> {
        
        self.then(OperatorBuilder::direct(
            move |input: &mut Receiver<K, V, T>, output, ctx| {
                if let Some(msg) = input.recv() {
                    match msg {
                        Message::Epoch(e) => {
                            inspector(&e, &ctx);
                            output.send(Message::Epoch(e))
                        }
                        x => output.send(x),
                    }
                };
            },
        ))
    }
}
