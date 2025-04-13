use std::sync::Arc;

use crds::MalstromJob;
use k8s_openapi::api::{apps::v1::StatefulSet, core::v1::Service};
use kube::{api::DeleteParams, Api, Client, ResourceExt};
use thiserror::Error;
use tracing::warn;

/// Deletes an existing job.
///
/// # Arguments:
/// - `client` - A Kubernetes client to delete the Deployment with
/// 
/// Note: It is assumed the deployment exists for simplicity. Otherwise returns an Error.
pub async fn delete(client: Client, job: Arc<MalstromJob>) -> Result<(), DeleteJobError> {
    let namespace = job.namespace().unwrap_or("default".to_owned());
    let svc_api: Api<Service> = Api::namespaced(client.clone(), &namespace);
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), &namespace);

    for name in [
        job.coordinator_service_spec().name_any(),
        job.worker_service_spec().name_any(),
    ] {
        let res = svc_api.delete(&name, &DeleteParams::default()).await;
        if let Err(kube::Error::Api(e)) = res {
            if e.reason != "NotFound" {
                return Err(DeleteJobError::Kubernetes(kube::Error::Api(e)));
            }
            warn!("Attempted to delete resource '{name}' but resource does not exist");
        }
    }

    for name in [
        job.coordinator_statefulset().name_any(),
        job.worker_statefulset_spec().name_any(),
    ] {
        let res = sts_api.delete(&name, &DeleteParams::default()).await;
        if let Err(kube::Error::Api(e)) = res {
            if e.reason != "NotFound" {
                return Err(DeleteJobError::Kubernetes(kube::Error::Api(e)));
            }
            warn!("Attempted to delete resource '{name}' but resource does not exist");
        }
    }

    Ok(())
}

#[derive(Debug, Error)]
pub(crate) enum DeleteJobError {
    #[error("Error from Kubernetes: {0}")]
    Kubernetes(#[from] kube::Error),
}
