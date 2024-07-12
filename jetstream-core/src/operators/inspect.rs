use crate::{stream::jetstream::JetStreamBuilder, time::MaybeTime, MaybeKey, Data};

use super::map::Map;

pub trait Inspect<K, V, T> {
    /// Observe values in a stream without modifying them.
    /// This is often done for debugging purposes or to record metrics.
    /// 
    /// Inspect takes a closure of function which is called on every data
    /// message.
    /// 
    /// To inspect the current event time see [`crate::operators::timely::InspectFrontier::inspect_frontier`].
    /// 
    /// ```
    /// use tracing::debug;
    /// 
    /// stream: JetStreamBuilder<NoKey, &str, NoTime, NoPersistence>
    /// stream.inspect(|x| debug!(x));
    ///
    /// ```
    fn inspect(self, inspector: impl FnMut(&V) -> () + 'static) -> JetStreamBuilder<K, V, T>;
}

impl<K, V, T,> Inspect<K, V, T> for JetStreamBuilder<K, V, T>
where
    K: MaybeKey,
    V: Data,
    T: MaybeTime,
{
    fn inspect(self, mut inspector: impl FnMut(&V) -> () + 'static) -> JetStreamBuilder<K, V, T> {
        self.map(move |x| {inspector(&x); x})
    }
}

#[cfg(test)]
mod tests {
    use std::{rc::Rc, sync::Mutex};

    use crate::{operators::{inspect::Inspect, source::Source}, stream::jetstream::JetStreamBuilder, test::collect_stream_values};

    #[test]
    fn test_inspect() {
        let cnt = Rc::new(Mutex::new(0));
        let cnt_cloned = Rc::clone(&cnt);

        let input = ["hello", "world", "foo", "bar"];
        let output = input.clone();
        let total_letters = input.iter().fold(0, |folder, x| folder + x.len());

        let stream = JetStreamBuilder::new_test()
            .source(input)
            .inspect(move |x| *cnt.lock().unwrap() += x.len());
        
        // check we still get unmodified output
        assert_eq!(collect_stream_values(stream), output);
        assert_eq!(total_letters, *cnt_cloned.lock().unwrap());
    }
}
