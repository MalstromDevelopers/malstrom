mod discovery;
mod worker_backend;
/// Intra-job communication
mod exchange {
    include!(concat!(env!("OUT_DIR"), "/malstrom_k8s.rs"));
}
/// Communication with K8S operator
mod k8s_operator {
    include!(concat!(env!("OUT_DIR"), "/malstrom_k8s.k8s_operator.rs"));
}


mod client;
mod coordinator_backend;
mod util;

pub(crate) use coordinator_backend::CoordinatorGrpcBackend;
pub(crate) use worker_backend::WorkerGrpcBackend;


pub enum APICommand {
    Rescale(RescaleCommand)
}
pub struct RescaleCommand {
    pub desired: u64,
    pub on_finish: tokio::sync::oneshot::Sender<()>
}