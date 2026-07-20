use std::time::Duration;

use crate::{
    coordinator::{Coordinator, CoordinatorExecutionError},
    runtime::{
        OperatorOperatorComm, RuntimeFlavor,
        communication::{
            ReqResReceiver, ReqResSender, StreamReceiver, StreamSender, WorkerCoordinatorComm,
        },
    },
    snapshot::PersistenceBackend,
    types::{OperatorId, WorkerId},
    worker::{StreamProvider, WorkerBuilder, WorkerExecutionError},
};

use async_trait::async_trait;
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
    F: FnOnce(&mut dyn StreamProvider),
{
    /// Start execution on this runtime, returning a build error if building the
    /// JetStream worker fails
    pub fn execute(self) -> Result<(), ExecutionError> {
        let mut flavor = SingleThreadRuntimeFlavor::default();

        let mut worker = WorkerBuilder::new(flavor.clone(), self.persistence.clone());
        (self.build)(&mut worker);

        let (coordinator, _) = Coordinator::new();
        let communication = flavor
            .communication()
            .expect("SingleThread communication is infallible");
        let _coord_thread = std::thread::spawn(move || {
            coordinator.execute(1, self.snapshots, self.persistence, communication)
        });
        worker.execute()?;
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
    CoordinatorJoin(Box<dyn std::any::Any + std::marker::Send>),
}

/// Runtime which only provides a single thread for a single worker.
/// This runtime is usually not very performant, but very simple.
/// Useful for unit-tests.
#[derive(Debug, Default, Clone)]
pub struct SingleThreadRuntimeFlavor {
    // comm_shared: Shared,
}

impl RuntimeFlavor for SingleThreadRuntimeFlavor {
    type Communication = InterThreadCommunication;

    fn communication(
        &mut self,
    ) -> Result<Self::Communication, crate::runtime::runtime_flavor::CommunicationError> {
        todo!()
        // Ok(InterThreadCommunication::new(self.comm_shared.clone(), 0))
    }

    fn this_worker_id(&self) -> u64 {
        0
    }
}

struct InterThreadCommunication;

#[async_trait]
impl OperatorOperatorComm for InterThreadCommunication {
    async fn new_sender(
        &self,
        to_worker: WorkerId,
        channel_id: OperatorId,
    ) -> Result<Box<dyn StreamSender>, Box<dyn std::error::Error>> {
        todo!()
    }

    async fn new_receiver(
        &self,
        from_worker: WorkerId,
        channel_id: OperatorId,
    ) -> Result<Box<dyn StreamReceiver>, Box<dyn std::error::Error>> {
        todo!()
    }
}

impl WorkerCoordinatorComm for InterThreadCommunication {
    async fn worker_to_coordinator(
        &self,
    ) -> Result<impl ReqResReceiver, Box<dyn std::error::Error>> {
        todo!()
    }

    async fn coordinator_to_worker(
        &self,
        to_worker: WorkerId,
    ) -> Result<impl ReqResSender, Box<dyn std::error::Error>> {
        todo!()
    }
}
