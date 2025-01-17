use std::{
    sync::Arc, time::Duration, u64,
};

use bon::Builder;

use crate::{
    coordinator::{Coordinator}, runtime::{builder::BuildError, RuntimeFlavor, StreamProvider, WorkerBuilder}, snapshot::{PersistenceBackend, PersistenceClient}, types::WorkerId
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
}
impl<P> MultiThreadRuntime<P> where P: PersistenceBackend + Clone + Send {
    pub fn execute(
        self
    ) -> Result<(), BuildError> {
        let mut threads = Vec::with_capacity(self.parrallelism as usize);
        let shared = Shared::default();

        let (finish_tx, finish_rx) = std::sync::mpsc::channel();
        for i in 0..self.parrallelism {
            let shared = Arc::clone(&shared);
            let finish_tx = finish_tx.clone();
            let persistence = self.persistence.clone();
            let thread = std::thread::spawn(move || {
                let flavor = MultiThreadRuntimeFlavor::new(shared, i);
                let mut worker_builder = WorkerBuilder::new(flavor, persistence);
                (self.build)(&mut worker_builder);
                let result = worker_builder.build_and_run();
                finish_tx.send(result).expect("Runtime should be alive");
            });
            threads.push(thread);
        }
        
        // TODO: Rescale immediatly when default_scale != last_scale

        let _coordinator = Coordinator::new(
            self.parrallelism,
            self.snapshots,
            self.persistence,
            InterThreadCommunication::new(shared, u64::MAX)
        );
        // Err(_) would mean all senders dropped i.e. all threads finished, which would be
        // perfectly fine with us
        while let Ok(worker_result) = finish_rx.recv() {
            match worker_result {
                Ok(_) => continue,
                Err(e) => {
                    // TODO kill other threads
                    return Err(e)
                },
            }
        };
        Ok(())
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
        MultiThreadRuntimeFlavor {
            shared,
            worker_id,
        }
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
        let flavor = MultiThreadRuntimeFlavor::new(Shared::default(), 2,);
        assert_eq!(rt_size.load(std::sync::atomic::Ordering::SeqCst), 6);
        drop(flavor);
        assert_eq!(rt_size.load(std::sync::atomic::Ordering::SeqCst), 5);
    }
}
