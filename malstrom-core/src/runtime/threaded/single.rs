use std::time::Duration;

use crate::{
    coordinator::Coordinator, runtime::{builder::BuildError, RuntimeFlavor, StreamProvider, WorkerBuilder}, snapshot::{PersistenceBackend, PersistenceClient}
};

use super::{communication::InterThreadCommunication, Shared};
use bon::Builder;
use tracing::debug;

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
    F: Fn(&mut dyn StreamProvider) -> ()
{
    /// Start execution on this runtime, returning a build error if building the
    /// JetStream worker fails
    pub fn execute(self) -> Result<(), BuildError> {
        let mut flavor = SingleThreadRuntimeFlavor::default();

        let mut worker = WorkerBuilder::new(flavor.clone(), self.persistence.clone());
        (self.build)(&mut worker);
        
        let _coordinator = Coordinator::new(1, self.snapshots, self.persistence, flavor.communication().unwrap());
        println!("{:?}", flavor.comm_shared);
        worker.build_and_run()
    }
}

#[derive(Debug, Default, Clone)]
pub struct SingleThreadRuntimeFlavor{
    comm_shared: Shared
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
