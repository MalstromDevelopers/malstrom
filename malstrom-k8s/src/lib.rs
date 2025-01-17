use config::CONFIG;
use malstrom::{runtime::{CommunicationError, RuntimeFlavor, WorkerBuilder}, snapshot::{PersistenceBackend, PersistenceClient}, types::WorkerId};

mod communication;
mod config;
use communication::GrpcBackend;

pub struct KubernetesRuntime<P>{
    worker_builder: WorkerBuilder<KubernetesRuntimeFlavor, P>
}


impl<P> KubernetesRuntime<P> where P: PersistenceClient {
    /// Create a new KubernetesRuntime
    /// Call this method inside every pod which should become a Worker pod
    pub fn new(
        builder_func: fn(KubernetesRuntimeFlavor) -> WorkerBuilder<KubernetesRuntimeFlavor, P>,
    ) -> Self {
    // TODO wait until all pods in k8s are available
        let flavor = KubernetesRuntimeFlavor::new(CONFIG.get_worker_id());
        Self { worker_builder: builder_func(flavor)}
    }

    pub fn execute(self) -> () {
        self.worker_builder.build().unwrap().execute();
    }
}

pub struct KubernetesRuntimeFlavor {
    this_worker: WorkerId
}
impl KubernetesRuntimeFlavor {
    fn new(this_worker: WorkerId) -> Self {
        Self { this_worker }
    }
}

impl RuntimeFlavor for KubernetesRuntimeFlavor {
    type Communication = GrpcBackend;

    fn communication(&mut self) -> Result<Self::Communication, CommunicationError> {
        GrpcBackend::new(self.this_worker).map_err(CommunicationError::from_error)
    }

    fn runtime_size(&self) -> u64 {
        // TODO: This is incorrect as it does not account for rescaling
        crate::config::CONFIG.initial_scale
    }

    fn this_worker_id(&self) -> WorkerId {
        crate::config::CONFIG.get_worker_id()
    }
}
