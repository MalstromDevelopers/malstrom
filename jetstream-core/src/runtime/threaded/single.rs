use crate::{
    runtime::{builder::BuildError, RuntimeFlavor, WorkerBuilder},
    snapshot::PersistenceBackend,
};

use super::{communication::InterThreadCommunication, Shared};

/// Runs all dataflows in a single thread on a
/// single machine with no parrallelism.
pub struct SingleThreadRuntime<P> {
    worker_builder: WorkerBuilder<SingleThreadRuntimeFlavor, P>,
}

impl<P> SingleThreadRuntime<P>
where
    P: PersistenceBackend,
{
    /// Create a new runtime for running on a single thread
    pub fn new(
        builder_func: fn(SingleThreadRuntimeFlavor) -> WorkerBuilder<SingleThreadRuntimeFlavor, P>,
    ) -> Self {
        Self {
            worker_builder: builder_func(SingleThreadRuntimeFlavor::default()),
        }
    }

    /// Start execution on this runtime, returning a build error if building the
    /// JetStream worker fails
    pub fn execute(self) -> Result<(), BuildError> {
        self.worker_builder.build()?.execute();
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct SingleThreadRuntimeFlavor;

impl RuntimeFlavor for SingleThreadRuntimeFlavor {
    type Communication = InterThreadCommunication;

    fn establish_communication(
        &mut self,
    ) -> Result<Self::Communication, crate::runtime::runtime_flavor::CommunicationError> {
        Ok(InterThreadCommunication::new(Shared::default(), 0))
    }

    fn runtime_size(&self) -> usize {
        1
    }

    fn this_worker_id(&self) -> usize {
        0
    }
}
