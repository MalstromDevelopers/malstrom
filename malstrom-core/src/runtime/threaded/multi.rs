use std::{sync::Arc, time::Duration};

use bon::Builder;
use thiserror::Error;

use crate::{
    coordinator::{
        Coordinator, CoordinatorApi, CoordinatorExecutionError, CoordinatorRequestError,
    },
    runtime::RuntimeFlavor,
    snapshot::PersistenceBackend,
    types::WorkerId,
    worker::{StreamProvider, WorkerBuilder, WorkerExecutionError},
};

use super::{communication::InterThreadCommunication, Shared};

/// Runs all dataflows on multiple threads within one machine
///
/// # Example
/// ```rust
/// use malstrom::operators::*;
/// use malstrom::runtime::MultiThreadRuntime;
/// use malstrom::snapshot::NoPersistence;
/// use malstrom::sources::{SingleIteratorSource, StatelessSource};
/// use malstrom::worker::StreamProvider;
/// use malstrom::keyed::partitioners::rendezvous_select;
///
///
/// MultiThreadRuntime::builder()
///     .parrallelism(4)
///     .persistence(NoPersistence)
///     .build(|provider: &mut dyn StreamProvider| {
///         provider.new_stream()
///         .source("numbers", StatelessSource::new(SingleIteratorSource::new(0..100)))
///         .key_distribute("key-by-value", |x| x.value, rendezvous_select)
///         .inspect("print", |x, ctx| {
///             println!("{x:?} @ Worker {}", ctx.worker_id)
///         });
///     })
///     .execute()
///     .unwrap();
/// ```
#[derive(Builder)]
pub struct MultiThreadRuntime<P> {
    #[builder(finish_fn)]
    build: fn(&mut dyn StreamProvider) -> (),
    persistence: P,
    snapshots: Option<Duration>,
    parrallelism: u64,
    #[builder(default = tokio::sync::watch::Sender::new(None))]
    api_handles: tokio::sync::watch::Sender<Option<CoordinatorApi>>,
    #[builder(default = std::sync::mpsc::channel())]
    rescale_req: (std::sync::mpsc::Sender<u64>, std::sync::mpsc::Receiver<u64>),
}

impl<P> MultiThreadRuntime<P>
where
    P: PersistenceBackend + Clone + Send + Sync,
{
    /// Start job execution an all workers in this runtime.
    pub fn execute(self) -> Result<(), WorkerExecutionError> {
        let mut threads = Vec::with_capacity(self.parrallelism as usize);
        let shared = Shared::default();

        for i in 0..self.parrallelism {
            let thread =
                Self::spawn_worker(self.build, self.persistence.clone(), Arc::clone(&shared), i);
            threads.push(thread);
        }

        let (coordinator, coordinator_api) = Coordinator::new();

        let coordinator_thread = {
            let persistence = self.persistence.clone();
            let shared = Arc::clone(&shared);
            std::thread::spawn(move || {
                coordinator
                    .execute(
                        self.parrallelism,
                        self.snapshots,
                        persistence,
                        InterThreadCommunication::new(shared, u64::MAX),
                    )
                    .map_err(ExecutionError::Coordinator)
            })
        };
        threads.push(coordinator_thread);
        // fails if there are no API handles
        let _ = self.api_handles.send(Some(coordinator_api));
        // Err(_) would mean all senders dropped i.e. all threads finished, which would be
        // perfectly fine with us

        loop {
            if let Ok(desired) = self.rescale_req.1.try_recv() {
                let actual = threads.len() as u64;
                if desired > actual {
                    for i in actual..desired {
                        let thread = Self::spawn_worker(
                            self.build,
                            self.persistence.clone(),
                            Arc::clone(&shared),
                            i,
                        );
                        threads.push(thread);
                    }
                }
            }
            threads.retain(|x| !x.is_finished());
            if threads.is_empty() {
                return Ok(());
            }
        }
    }

    fn spawn_worker(
        build_fn: fn(&mut dyn StreamProvider) -> (),
        persistence: P,
        shared: Shared,
        thread_id: u64,
    ) -> std::thread::JoinHandle<Result<(), ExecutionError>> {
        std::thread::spawn(move || {
            let flavor = MultiThreadRuntimeFlavor::new(shared, thread_id);
            let mut worker_builder = WorkerBuilder::new(flavor, persistence);
            build_fn(&mut worker_builder);
            worker_builder.execute().map_err(ExecutionError::Worker)
        })
    }

    /// Get an API handle for interacting with the Malstrom job, e.g. for triggering rescales.
    pub fn api_handle(&self) -> MultiThreadRuntimeApiHandle {
        MultiThreadRuntimeApiHandle {
            coord_channel: self.api_handles.subscribe(),
            rescale_req: self.rescale_req.0.clone(),
        }
    }
}

#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error("Error executing worker")]
    Worker(#[from] WorkerExecutionError),
    #[error("Error executing coordinator")]
    Coordinator(#[from] CoordinatorExecutionError),
}

/// This is passed to the worker
/// You can not construct this directly use [MultiThreadRuntime] instead
pub struct MultiThreadRuntimeFlavor {
    shared: Shared,
    worker_id: u64,
}
impl MultiThreadRuntimeFlavor {
    fn new(shared: Shared, worker_id: WorkerId) -> Self {
        MultiThreadRuntimeFlavor { shared, worker_id }
    }
}

impl RuntimeFlavor for MultiThreadRuntimeFlavor {
    type Communication = InterThreadCommunication;

    fn communication(
        &mut self,
    ) -> Result<Self::Communication, crate::runtime::runtime_flavor::CommunicationError> {
        Ok(InterThreadCommunication::new(
            self.shared.clone(),
            self.worker_id,
        ))
    }

    fn this_worker_id(&self) -> u64 {
        self.worker_id
    }
}

pub struct MultiThreadRuntimeApiHandle {
    coord_channel: tokio::sync::watch::Receiver<Option<CoordinatorApi>>,
    rescale_req: std::sync::mpsc::Sender<u64>,
}

impl MultiThreadRuntimeApiHandle {
    pub async fn rescale(&self, desired: u64) -> Result<(), CoordinatorRequestError> {
        // instruct the runtime to spawn another thread if needed
        self.rescale_req
            .send(desired)
            .map_err(|_| CoordinatorRequestError::NotRunning)?;
        // instruct the coordinator to re-distribute computation
        self.coord_channel
            .borrow()
            .as_ref()
            .ok_or(CoordinatorRequestError::NotRunning)?
            .rescale(desired)
            .await
    }
}
