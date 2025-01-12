use std::{
    sync::{atomic::AtomicU64, Arc, Barrier, Mutex},
    thread::JoinHandle,
};

use indexmap::IndexMap;

use crate::{
    runtime::{builder::WorkerApiHandle, rescaling::RescaleError, RuntimeFlavor, WorkerBuilder},
    snapshot::PersistenceClient,
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
pub struct MultiThreadRuntime<P> {
    /// we need to wait on this for the threads to start executing
    execution_barrier: Arc<Barrier>,
    threads: Mutex<IndexMap<WorkerId, Option<JoinHandle<()>>>>,
    api_handle: WorkerApiHandle,
    builder_func: fn(MultiThreadRuntimeFlavor) -> WorkerBuilder<MultiThreadRuntimeFlavor, P>,
    shared: Shared,
    current_size: Arc<AtomicU64>
}
impl<P> MultiThreadRuntime<P> where P: PersistenceClient{
    pub fn new(
        parrallelism: u64,
        builder_func: fn(MultiThreadRuntimeFlavor) -> WorkerBuilder<MultiThreadRuntimeFlavor, P>,
    ) -> Self {
        let parrallelism_usize: usize = parrallelism
            .try_into()
            .expect("parallelism should be < usie::MAX");
        let execution_barrier = Arc::new(Barrier::new(parrallelism_usize + 1)); // +1 because we are keeping one here
        let mut threads = IndexMap::with_capacity(parrallelism_usize);
        
        let current_size = Arc::new(AtomicU64::new(0));
        let (api_tx, api_rx) = std::sync::mpsc::sync_channel(1);

        let shared = Shared::default();
        for i in 0..parrallelism {
            let shared_this = Arc::clone(&shared);
            let barrier = Arc::clone(&execution_barrier);
            let api_tx = api_tx.clone();
            let size_this = Arc::clone(&current_size);

            let thread = std::thread::spawn(move || {
                size_this.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let flavor = MultiThreadRuntimeFlavor::new(shared_this, i, size_this);
                let worker_builder = builder_func(flavor);
                let (mut worker, api) = worker_builder.build().unwrap();
                if i == 0 {
                    api_tx.send(api).unwrap();
                }
                barrier.wait();
                worker.execute();
            });
            threads.insert(i, Some(thread));
        }
        let api_handle = api_rx.recv().unwrap();
        MultiThreadRuntime {
            execution_barrier,
            threads: Mutex::new(threads),
            api_handle,
            builder_func,
            shared,
            current_size
        }
    }

    // pub fn new_with_args<P, A, F>(
    //     builder_func: fn(MultiThreadRuntimeFlavor, A) -> WorkerBuilder<F, P>,
    //     args: impl IntoIterator<Item = A>,
    // ) -> Self
    // where
    //     P: PersistenceClient,
    //     A: Send + 'static,
    //     F: RuntimeFlavor + 'static,
    // {
    //     let args_vec: Vec<A> = args.into_iter().collect();
    //     let parrallelism = args_vec.len();

    //     let execution_barrier = Arc::new(Barrier::new(parrallelism + 1)); // +1 because we are keeping one here
    //     let mut threads = Vec::with_capacity(parrallelism);

    //     let parrallelism: u64 = parrallelism
    //         .try_into()
    //         .expect("Parallelism should be < usize::MAX");

    //     let shared = Shared::default();
    //     for (i, arg) in args_vec.into_iter().enumerate() {
    //         let i: u64 = i
    //             .try_into()
    //             .expect("Should have less than usize::MAX workers");
    //         let shared_this = Arc::clone(&shared);
    //         let barrier = Arc::clone(&execution_barrier);
    //         let thread = std::thread::spawn(move || {
    //             let flavor = MultiThreadRuntimeFlavor::new(shared_this, i, parrallelism);
    //             let worker = builder_func(flavor, arg);
    //             barrier.wait();
    //             worker.build().unwrap().execute();
    //         });
    //         threads.push(thread);
    //     }
    //     MultiThreadRuntime {
    //         execution_barrier,
    //         threads,
    //     }
    // }

    pub fn execute(&self) -> std::thread::Result<()> {
        // start execution on all threads
        self.execution_barrier.wait();

        // not pretty but pragmatic
        // to eagerly panic if one of the spawned threads panics
        loop {
            let mut threads = self.threads.lock().unwrap();
            for t in threads.values_mut().filter(|x| x.is_some()) {
                if t.as_ref().map_or(false, |x| x.is_finished()) {
                    // PANIC: Can unwrap because we checked is_some above
                    self.current_size.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                    t.take().unwrap().join()?

                }
            }
            threads.retain(|_wid, thread| thread.is_some());
        }
    }

    pub fn with_api(self) -> (Arc<Self>, MultithreadRuntimeApi) {
        let handle = self.api_handle.clone();
        let rt = Arc::new(self);
        let api = MultithreadRuntimeApi{
            inner: handle,
            runtime: Arc::clone(&rt) as Arc<dyn ApiRuntimeInteraction>
        };
        (rt, api)
    }
}

/// We can cast to this trait object for the reference the API handle holds on the runtime to erase
/// types
trait ApiRuntimeInteraction: Send + Sync {
    /// Add a new thread/worker to the runtime
    fn add_thread(&self) ->();
}

impl<P> ApiRuntimeInteraction for MultiThreadRuntime<P> where P: PersistenceClient {
    /// add thread after inital construction
    fn add_thread(&self) {
        let new_idx = self.current_size.load(std::sync::atomic::Ordering::SeqCst);
        let shared_this = Arc::clone(&self.shared);
        let builder = self.builder_func;
        let size_this = Arc::clone(&self.current_size);
        
        let thread = std::thread::spawn(move || {
            size_this.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let flavor = MultiThreadRuntimeFlavor::new(shared_this, new_idx, size_this);
            let worker_builder = builder(flavor);
            let (mut worker, _api) = worker_builder.build().unwrap();
            worker.execute();
        });
        self.threads.lock().unwrap().insert(new_idx, Some(thread));
    }
}

/// This is passed to the worker
/// You can not construct this directly use [MultiThreadRuntime] instead
pub struct MultiThreadRuntimeFlavor {
    shared: Shared,
    worker_id: u64,
    runtime_size: Arc<AtomicU64>,
}
impl MultiThreadRuntimeFlavor {
    fn new(shared: Shared, worker_id: WorkerId, runtime_size: Arc<AtomicU64>) -> Self {
        MultiThreadRuntimeFlavor {
            shared,
            worker_id,
            runtime_size,
        }
    }
}

impl RuntimeFlavor for MultiThreadRuntimeFlavor {
    type Communication = InterThreadCommunication;

    fn establish_communication(
        &mut self,
    ) -> Result<Self::Communication, crate::runtime::runtime_flavor::CommunicationError> {
        Ok(InterThreadCommunication::new(
            self.shared.clone(),
            self.worker_id,
        ))
    }

    fn runtime_size(&self) -> u64 {
        self.runtime_size.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn this_worker_id(&self) -> u64 {
        self.worker_id
    }
}

#[derive(Clone)]
pub struct MultithreadRuntimeApi {
    inner: WorkerApiHandle,
    runtime: Arc<dyn ApiRuntimeInteraction>
}

impl MultithreadRuntimeApi {
    /// Change the scale of the worker pool by the specified amount.
    /// This will trigger zero-downtime rescaling. The returned future completes
    /// once rescaling has completed.
    pub async fn scale(&self, change: i64) -> Result<(), RescaleError> {
        if change > 0 {
            for _ in 0..change {
                self.runtime.add_thread();
            }
        }
        self.inner.rescale(change).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{atomic::AtomicU64, Arc};

    use crate::{
        runtime::WorkerBuilder,
        snapshot::{NoPersistence, NoSnapshots},
    };

    use super::{MultiThreadRuntime, MultiThreadRuntimeFlavor, Shared};

    // #[test]
    // fn test_can_build() {
    //     let rt = MultiThreadRuntime::new_with_args(
    //         |flavor, _| WorkerBuilder::new(flavor, NoSnapshots, NoPersistence::default()),
    //         [(); 2],
    //     );
    //     rt.execute().unwrap();
    // }

    /// It should increment the runtime size by 1 when instantiated and decrement by one when
    /// dropped
    #[test]
    fn update_runtime_size() {
        let rt_size = Arc::new(AtomicU64::new(5));
        let flavor = MultiThreadRuntimeFlavor::new(Shared::default(), 2, Arc::clone(&rt_size));
        assert_eq!(rt_size.load(std::sync::atomic::Ordering::SeqCst), 6);
        drop(flavor);
        assert_eq!(rt_size.load(std::sync::atomic::Ordering::SeqCst), 5);
    }
}
