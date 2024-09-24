use std::{
    sync::{Arc, Barrier},
    thread::JoinHandle,
};

use crate::{
    runtime::{RuntimeBuilder, RuntimeFlavor, Worker},
    snapshot::PersistenceBackend,
    types::WorkerId,
};

use super::{communication::InterThreadCommunication, Shared};

/// Runs all dataflows on multiple threads within one machine
pub struct MultiThreadRuntime {
    /// we need to wait on this for the threads to start executing
    execution_barrier: Arc<Barrier>,
    threads: Vec<JoinHandle<()>>,
}
impl MultiThreadRuntime {
    pub fn new_with_args<P, const N: usize, A>(
        builder_func: fn(InnerMultiThreadRuntime, A) -> RuntimeBuilder<InnerMultiThreadRuntime, P>,
        args: [A; N],
    ) -> Self
    where
        P: PersistenceBackend,
        A: Send + 'static,
    {
        let execution_barrier = Arc::new(Barrier::new(N + 1)); // +1 because we are keeping one here
        let mut threads = Vec::with_capacity(N);

        let shared = Shared::default();
        for (i, arg) in args.into_iter().enumerate() {
            let shared_this = Arc::clone(&shared);
            let barrier = Arc::clone(&execution_barrier);
            let thread = std::thread::spawn(move || {
                let flavor = InnerMultiThreadRuntime::new(shared_this, i, N);
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

    pub fn execute(self) -> () {
        // start execution on all threads
        self.execution_barrier.wait();
        for t in self.threads {
            t.join().unwrap();
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
    use crate::runtime::RuntimeBuilder;

    use super::MultiThreadRuntime;

    #[test]
    fn test_can_build() {
        let rt = MultiThreadRuntime::new_with_args(|flavor, _| RuntimeBuilder::new(flavor), [(); 2]);
        rt.execute();
    }
}
