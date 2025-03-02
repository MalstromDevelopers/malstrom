use std::{sync::Arc, time::Duration, u64};

use bon::Builder;

use crate::{
    coordinator::{Coordinator, CoordinatorError},
    runtime::{builder::BuildError, RuntimeFlavor, StreamProvider, WorkerBuilder},
    snapshot::PersistenceBackend,
    types::WorkerId,
};

use super::{communication::InterThreadCommunication, Shared};

/// Runs all dataflows on multiple threads within one machine
///
/// # Example
/// ```rust
/// use malstrom::operators::*;
/// use malstrom::operators::Source;
/// use malstrom::runtime::{WorkerBuilder, RuntimeFlavor, threaded::MultiThreadRuntime};
/// use malstrom::snapshot::NoPersistence;
/// use malstrom::keyed::partitioners::rendezvous_select;
/// use malstrom::sources::SingleIteratorSource;
///
/// fn build_dataflow<R: RuntimeFlavor>(rt: R) -> WorkerBuilder<R, NoPersistence>{
///     let mut worker = WorkerBuilder::new(rt);
///     worker
///         .new_stream()
///         .source(SingleIteratorSource::new(0..10))
///         // we just use the value as key here
///         .key_distribute(|msg| msg.value, rendezvous_select)
///         // all operations from this point on are distributed
///         .finish();
///     worker
/// }
///
/// // run with 4 workers in parallel
/// let runtime = MultiThreadRuntime::new(4, build_dataflow);
/// runtime.execute().unwrap()
/// ```
#[derive(Builder)]
pub struct MultiThreadRuntime<P> {
    #[builder(finish_fn)]
    build: fn(&mut dyn StreamProvider) -> (),
    persistence: P,
    snapshots: Option<Duration>,
    parrallelism: u64,
    #[builder(default = tokio::sync::watch::Sender::new(None))]
    api_handles: tokio::sync::watch::Sender<Option<Coordinator>>,
    #[builder(default = std::sync::mpsc::channel())]
    rescale_req: (std::sync::mpsc::Sender<u64>, std::sync::mpsc::Receiver<u64>),
}

impl<P> MultiThreadRuntime<P>
where
    P: PersistenceBackend + Clone + Send + Sync,
{
    pub fn execute(self) -> Result<(), BuildError> {
        let mut threads = Vec::with_capacity(self.parrallelism as usize);
        let shared = Shared::default();

        for i in 0..self.parrallelism {
            let thread =
                Self::spawn_worker(self.build, self.persistence.clone(), Arc::clone(&shared), i);
            threads.push(thread);
        }

        let coordinator = Coordinator::new(
            self.parrallelism,
            self.snapshots,
            self.persistence.clone(),
            InterThreadCommunication::new(Arc::clone(&shared), u64::MAX),
        )
        .unwrap();
        // fails if there are no API handles
        let _ = self.api_handles.send(Some(coordinator));
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
            if threads.len() == 0 {
                return Ok(());
            }
        }
    }

    fn spawn_worker(
        build_fn: fn(&mut dyn StreamProvider) -> (),
        persistence: P,
        shared: Shared,
        thread_id: u64,
    ) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            let flavor = MultiThreadRuntimeFlavor::new(shared, thread_id);
            let mut worker_builder = WorkerBuilder::new(flavor, persistence);
            build_fn(&mut worker_builder);
            worker_builder.build_and_run().unwrap();
        })
    }

    pub fn api_handle(&self) -> MultiThreadRuntimeApiHandle {
        MultiThreadRuntimeApiHandle {
            coord_channel: self.api_handles.subscribe(),
            rescale_req: self.rescale_req.0.clone(),
        }
    }
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
    coord_channel: tokio::sync::watch::Receiver<Option<Coordinator>>,
    rescale_req: std::sync::mpsc::Sender<u64>,
}

impl MultiThreadRuntimeApiHandle {
    pub async fn rescale(&self, desired: u64) -> Result<(), CoordinatorError> {
        // instruct the runtime to spawn another thread if needed
        self.rescale_req.send(desired).unwrap();
        // instruct the coordinator to re-distribute computation
        self.coord_channel
            .borrow()
            .as_ref()
            .unwrap()
            .rescale(desired)
            .await
    }
}
