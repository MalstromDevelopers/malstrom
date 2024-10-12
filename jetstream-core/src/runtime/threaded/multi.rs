use std::{
    sync::{Arc, Barrier},
    thread::JoinHandle,
};

use crate::{
    runtime::{WorkerBuilder, RuntimeFlavor},
    snapshot::PersistenceBackend,
    types::WorkerId,
};

use super::{communication::InterThreadCommunication, Shared};

/// Runs all dataflows on multiple threads within one machine
///
/// # Example
/// ```
/// let ranges = [(0..10), (10..20)];
/// let rt = MultiThreadRuntime::new_with_args(
///     args,
///     |flavor, range| {
///         // create the stream builder
///         let mut builder = RuntimeBuilder::new(flavor);
///         // create the stream
///         builder
///         .new_stream()
///         .source(MultiIteratorSource::new(range))
///         .inspect(|x| println!("{x}"))    
///         .finish();
///         // return the builder with its streams
///         builder
///     },
/// );
/// // execute on all threads
/// rt.execute();
/// ```
pub struct MultiThreadRuntime {
    /// we need to wait on this for the threads to start executing
    execution_barrier: Arc<Barrier>,
    threads: Vec<JoinHandle<()>>,
}
impl MultiThreadRuntime {
    pub fn new<P: PersistenceBackend>(
        parrallelism: usize,
        builder_func: fn(InnerMultiThreadRuntime) -> WorkerBuilder<InnerMultiThreadRuntime, P>,
    ) -> Self {
        let execution_barrier = Arc::new(Barrier::new(parrallelism + 1)); // +1 because we are keeping one here
        let mut threads = Vec::with_capacity(parrallelism);

        let shared = Shared::default();
        for i in 0..parrallelism {
            let shared_this = Arc::clone(&shared);
            let barrier = Arc::clone(&execution_barrier);
            let thread = std::thread::spawn(move || {
                let flavor = InnerMultiThreadRuntime::new(shared_this, i, parrallelism);
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
    pub fn new_with_args<P, const N: usize, A, F>(
        builder_func: fn(InnerMultiThreadRuntime, A) -> WorkerBuilder<F, P>,
        args: [A; N],
    ) -> Self
    where
        P: PersistenceBackend,
        A: Send + 'static,
        F: RuntimeFlavor + 'static,
    {
        todo!()
    }

    pub fn execute(self) {
        // start execution on all threads
        self.execution_barrier.wait();
        for t in self.threads {
            let _ = t.join();
        }
    }
}

/// This is passed to the worker
/// You can not construct this directly use [MultiThreadRuntime] instead
pub struct InnerMultiThreadRuntime {
    shared: Shared,
    worker_id: usize,
    total_size: usize,
}
impl InnerMultiThreadRuntime {
    fn new(shared: Shared, worker_id: WorkerId, total_size: usize) -> Self {
        InnerMultiThreadRuntime {
            shared,
            worker_id,
            total_size,
        }
    }
}

impl RuntimeFlavor for InnerMultiThreadRuntime {
    type Communication = InterThreadCommunication;

    fn establish_communication(
        &mut self,
    ) -> Result<Self::Communication, crate::runtime::runtime_flavor::CommunicationError> {
        Ok(InterThreadCommunication::new(
            self.shared.clone(),
            self.worker_id,
        ))
    }

    fn runtime_size(&self) -> usize {
        self.total_size
    }

    fn this_worker_id(&self) -> usize {
        self.worker_id
    }
}

#[cfg(test)]
mod tests {
    use crate::runtime::WorkerBuilder;

    use super::MultiThreadRuntime;

    #[test]
    fn test_can_build() {
        let rt =
            MultiThreadRuntime::new_with_args(|flavor, _| WorkerBuilder::new(flavor), [(); 2]);
        rt.execute();
    }
}
