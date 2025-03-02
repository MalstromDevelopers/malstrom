use std::time::Duration;

use bon::Builder;
use communication::WorkerGrpcBackend;
use config::CONFIG;
use malstrom::{
    coordinator::{Coordinator, CoordinatorCreationError},
    runtime::{BuildError, CommunicationError, RuntimeFlavor, StreamProvider, WorkerBuilder},
    snapshot::{PersistenceBackend, PersistenceClient},
    types::WorkerId,
};
use thiserror::Error;
mod communication;
mod config;
use crate::communication::CoordinatorGrpcBackend;

#[derive(Builder)]
pub struct KubernetesRuntime<P> {
    #[builder(finish_fn)]
    build: fn(&mut dyn StreamProvider) -> (),
    persistence: P,
    snapshots: Option<Duration>,
}

impl<P> KubernetesRuntime<P>
where
    P: PersistenceBackend + Clone + Send + Sync,
{
    pub fn execute_auto(self) -> Result<(), ExecuteAutoError> {
        if CONFIG.is_coordinator {
            self.execute_coordinator()?;
        } else {
            self.execute_worker()?;
        };
        Ok(())
    }

    pub fn execute_worker(self) -> Result<(), BuildError> {
        let mut worker = WorkerBuilder::new(KubernetesRuntimeFlavor, self.persistence);
        (self.build)(&mut worker);
        worker.build_and_run()
    }

    pub fn execute_coordinator(self) -> Result<(), CoordinatorCreationError> {
        let (api_tx, api_rx) = flume::unbounded();
        let communication = CoordinatorGrpcBackend::new().unwrap();
        let coordinator = Coordinator::new(CONFIG.initial_scale, self.snapshots, self.persistence, communication)?;
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum ExecuteAutoError {
    #[error(transparent)]
    WorkerBuild(#[from] BuildError),
    #[error(transparent)]
    CoordinatorCreate(#[from] CoordinatorCreationError)
}

pub struct KubernetesRuntimeFlavor;

impl RuntimeFlavor for KubernetesRuntimeFlavor {
    type Communication = WorkerGrpcBackend;

    fn communication(&mut self) -> Result<Self::Communication, CommunicationError> {
        WorkerGrpcBackend::new().map_err(CommunicationError::from_error)
    }

    fn this_worker_id(&self) -> WorkerId {
        crate::config::CONFIG.get_worker_id()
    }
}
