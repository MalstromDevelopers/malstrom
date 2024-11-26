use std::{
    sync::{Arc, Barrier},
    thread::JoinHandle,
};

use crate::{
    runtime::{RuntimeFlavor, WorkerBuilder},
    snapshot::{PersistenceBackend, PersistenceClient},
    types::WorkerId,
};

use super::{communication::InterThreadCommunication, Shared};

/// Runs all dataflows on multiple threads within one machine
///
/// # Example
/// ```rust
/// use jetstream::operators::*;
/// use jetstream::operators::Source;
/// use jetstream::runtime::{WorkerBuilder, RuntimeFlavor, threaded::MultiThreadRuntime};
/// use jetstream::snapshot::NoPersistence;
/// use jetstream::keyed::partitioners::rendezvous_select;
/// use jetstream::sources::SingleIteratorSource;
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
pub struct MultiThreadRuntime {
    /// we need to wait on this for the threads to start executing
    execution_barrier: Arc<Barrier>,
    threads: Vec<JoinHandle<()>>,
}
impl MultiThreadRuntime {
    pub fn new<P: PersistenceClient>(
        parrallelism: u64,
        builder_func: fn(MultiThreadRuntimeFlavor) -> WorkerBuilder<MultiThreadRuntimeFlavor, P>,
    ) -> Self {
        let parrallelism_usize: usize = parrallelism
            .try_into()
            .expect("parallelism should be < usie::MAX");
        let execution_barrier = Arc::new(Barrier::new(parrallelism_usize + 1)); // +1 because we are keeping one here
        let mut threads = Vec::with_capacity(parrallelism_usize);

        let shared = Shared::default();
        for i in 0..parrallelism {
            let shared_this = Arc::clone(&shared);
            let barrier = Arc::clone(&execution_barrier);
            let thread = std::thread::spawn(move || {
                let flavor = MultiThreadRuntimeFlavor::new(shared_this, i, parrallelism);
                let worker = builder_func(flavor);
                barrier.wait();
                worker.build().unwrap().execute();
            });
            threads.push(thread);
        }
        MultiThreadRuntime {
            execution_barrier,
            threads,
        }
    }
    pub fn new_with_args<P, A, F>(
        builder_func: fn(MultiThreadRuntimeFlavor, A) -> WorkerBuilder<F, P>,
        args: impl IntoIterator<Item = A>,
    ) -> Self
    where
        P: PersistenceClient,
        A: Send + 'static,
        F: RuntimeFlavor + 'static,
    {
        let args_vec: Vec<A> = args.into_iter().collect();
        let parrallelism = args_vec.len();

        let execution_barrier = Arc::new(Barrier::new(parrallelism + 1)); // +1 because we are keeping one here
        let mut threads = Vec::with_capacity(parrallelism);

        let parrallelism: u64 = parrallelism
            .try_into()
            .expect("Parallelism should be < usize::MAX");

        let shared = Shared::default();
        for (i, arg) in args_vec.into_iter().enumerate() {
            let i: u64 = i
                .try_into()
                .expect("Should have less than usize::MAX workers");
            let shared_this = Arc::clone(&shared);
            let barrier = Arc::clone(&execution_barrier);
            let thread = std::thread::spawn(move || {
                let flavor = MultiThreadRuntimeFlavor::new(shared_this, i, parrallelism);
                let worker = builder_func(flavor, arg);
                barrier.wait();
                worker.build().unwrap().execute();
            });
            threads.push(thread);
        }
        MultiThreadRuntime {
            execution_barrier,
            threads,
        }
    }

    pub fn execute(self) -> std::thread::Result<()> {
        // start execution on all threads
        self.execution_barrier.wait();

        // not pretty but pragmatic
        // to eagerly panic if one of the spawned threads panics
        let mut threads: Vec<_> = self.threads.into_iter().map(|x| Some(x)).collect();
        loop {
            if threads.iter().all(|x| x.is_none()) {
                return Ok(());
            }
            for t in threads.iter_mut() {
                match t.take() {
                    Some(running) => {
                        if running.is_finished() {
                            running.join()?;
                        } else {
                            let _ = t.insert(running);
                        }
                    }
                    None => (),
                }
            }
        }
    }
}

/// This is passed to the worker
/// You can not construct this directly use [MultiThreadRuntime] instead
pub struct MultiThreadRuntimeFlavor {
    shared: Shared,
    worker_id: u64,
    total_size: u64,
}
impl MultiThreadRuntimeFlavor {
    fn new(shared: Shared, worker_id: WorkerId, total_size: u64) -> Self {
        MultiThreadRuntimeFlavor {
            shared,
            worker_id,
            total_size,
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
        self.total_size
    }

    fn this_worker_id(&self) -> u64 {
        self.worker_id
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        runtime::{builder::no_snapshots, WorkerBuilder},
        snapshot::NoPersistence,
    };

    use super::MultiThreadRuntime;

    #[test]
    fn test_can_build() {
        let rt = MultiThreadRuntime::new_with_args(
            |flavor, _| WorkerBuilder::new(flavor, no_snapshots, NoPersistence::default()),
            [(); 2],
        );
        rt.execute().unwrap();
    }
}
