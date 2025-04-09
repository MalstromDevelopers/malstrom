use malstrom::types::WorkerId;
use tonic::transport::Endpoint;

use crate::config::CONFIG;

pub(crate) fn lookup_worker_addr(worker_id: WorkerId) -> Endpoint {
    // Kubernetes uses <pod-name>.<service-name>.<namespace>.svc.cluster.local
    let svc_name = &CONFIG.worker_svc_name;
    let namespace = &CONFIG.namespace;
    let sts_name = &CONFIG.worker_sts_name;
    let port = &CONFIG.network.port;
    // malstrom-app-0.malstrom-svc.default.svc.cluster.local
    // http://malstrom-app-2.malstrom-svc.default.svc.cluster.local:29091
    //http://test-job-worker-3.test-job-worker.default.svc.cluster.local:29091/
    let url =
        format!("http://{sts_name}-{worker_id}.{svc_name}.{namespace}.svc.cluster.local:{port}");
    // PANIC: The string is partially hardcoded and valid unless configured to
    // an invalid value
    Endpoint::try_from(url).unwrap()
}

pub(crate) fn lookup_coordinator_addr() -> Endpoint {
    let svc_name = &CONFIG.coordinator_svc_name;
    let namespace = &CONFIG.namespace;
    let port = &CONFIG.network.port;
    let url = format!("http://{svc_name}.{namespace}.svc.cluster.local:{port}");
    // PANIC: The string is partially hardcoded and valid unless configured to
    // an invalid value
    Endpoint::try_from(url).unwrap()
}
