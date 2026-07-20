use crate::channels::operator_io;
use crate::{
    channels::operator_io::Input, snapshot::PersistenceBackend, stream::InitialStreamBuilder,
    worker::builder::WorkerBuilder,
};
/// Creates new streams to add to the job
pub trait StreamProvider {
    /// Create a new empty stream. This stream will not contain any data.
    /// Call `.source()` on the stream to add a source.
    fn new_stream(&mut self) -> InitialStreamBuilder;
}

impl<F, P> StreamProvider for WorkerBuilder<F, P>
where
    P: PersistenceBackend,
{
    fn new_stream(&mut self) -> InitialStreamBuilder {
        // link our new stream to the root stream we will build later
        // so it can receive system messages
        let mut input = Input::new_unlinked();
        operator_io::link(&mut self.root_operator.output, &mut input);
        InitialStreamBuilder::new(input, self.inner.clone())
    }
}
