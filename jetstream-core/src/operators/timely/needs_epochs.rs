use crate::{
    snapshot::PersistenceBackend,
    stream::{jetstream::JetStreamBuilder, operator::OperatorBuilder},
    time::{Epoch, NoTime, Timestamp},
    DataMessage, MaybeData, MaybeKey, Message, Worker,
};

use super::util::{handle_maybe_late_msg, split_mixed_stream};

/// Intermediate builder for a timestamped stream.
/// Turn this type into a stream by calling
/// `generate_epochs` or `generate_periodic_epochs` on it
#[must_use]
pub struct NeedsEpochs<K, V, T, P>(pub(super) JetStreamBuilder<K, V, T, P>);
