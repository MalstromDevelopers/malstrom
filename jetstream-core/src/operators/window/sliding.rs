use crate::{stream::jetstream::JetStreamBuilder, DataMessage};

pub trait FlexibleWindow<K, V, T, R> {
    fn sliding_window(self, window_length: T, reducer: impl (FnMut(DataMessage<K, V, T>, R) -> R) + 'static) -> JetStreamBuilder<K, R, T>;
}