use std::time::Duration;

use crate::{
    coordinator::{Coordinator, CoordinatorExecutionError},
    runtime::RuntimeFlavor,
    snapshot::PersistenceBackend,
    worker::{StreamProvider, WorkerBuilder, WorkerExecutionError},
};

use super::{communication::InterThreadCommunication, Shared};
use bon::Builder;
use thiserror::Error;

/// Runs all dataflows in a single thread on a
/// single machine with no parrallelism.
#[derive(Builder)]
pub struct SingleThreadRuntime<P, F> {
    #[builder(finish_fn)]
    build: F,
    persistence: P,
    snapshots: Option<Duration>,
}

impl<P, F> SingleThreadRuntime<P, F>
where
    P: PersistenceBackend + Clone + Send,
    F: FnMut(&mut dyn StreamProvider),
{
    /// Start execution on this runtime, returning a build error if building the
    /// JetStream worker fails
    pub fn execute(mut self) -> Result<(), ExecutionError> {
        let mut flavor = SingleThreadRuntimeFlavor::default();

        let mut worker = WorkerBuilder::new(flavor.clone(), self.persistence.clone());
        (self.build)(&mut worker);

        let (coordinator, _) = Coordinator::new();
        let communication = flavor.communication().expect("SingleThread communication is infallible");
        let _coord_thread = std::thread::spawn(move || coordinator.execute(
            1,
            self.snapshots,
            self.persistence,
            communication,
        ));
        worker.execute()?;
        // TODO: Coordinator thread does not terminate, which messes with the tests
        //coord_thread.join().map_err(ExecutionError::CoordinatorJoin)??;
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error("Error executing worker")]
    Worker(#[from] WorkerExecutionError),
    #[error("Error executing coordinator")]
    Coordinator(#[from] CoordinatorExecutionError),
    #[error("Error joining coordinator thread: {0:?}")]
    CoordinatorJoin(Box<dyn std::any::Any + std::marker::Send>)
}

/// Runtime which only provides a single thread for a single worker.
/// This runtime is usually not very performant, but very simple.
/// Useful for unit-tests.
#[derive(Debug, Default, Clone)]
pub struct SingleThreadRuntimeFlavor {
    comm_shared: Shared,
}

impl RuntimeFlavor for SingleThreadRuntimeFlavor {
    type Communication = InterThreadCommunication;

    fn communication(
        &mut self,
    ) -> Result<Self::Communication, crate::runtime::runtime_flavor::CommunicationError> {
        Ok(InterThreadCommunication::new(self.comm_shared.clone(), 0))
    }

    fn this_worker_id(&self) -> u64 {
        0
    }
}
