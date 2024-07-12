use crate::{
    stream::{jetstream::JetStreamBuilder},
};



/// Intermediate builder for a timestamped stream.
/// Turn this type into a stream by calling
/// `generate_epochs` or `generate_periodic_epochs` on it
#[must_use]
pub struct NeedsEpochs<K, V, T>(pub(super) JetStreamBuilder<K, V, T>);
