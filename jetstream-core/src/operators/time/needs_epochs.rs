use crate::stream::JetStreamBuilder;

/// Intermediate builder for a timestamped stream.
/// Turn this type into a stream by calling
/// `generate_epochs` or `generate_periodic_epochs` on it
#[must_use = "Call `.generate_epochs()` or `.generate_periodic_epochs()`"]
pub struct NeedsEpochs<K, V, T>(pub(super) JetStreamBuilder<K, V, T>);
