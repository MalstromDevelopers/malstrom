use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::Service;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client, ResourceExt};
use std::sync::Arc;
use thiserror::Error;

use crds::{JobState, MalstromJob, MalstromJobStatus};

/// Creates a new Malstrom job, this means creating
/// - the coordinator statefulset
/// - the coordinator service
/// - the worker statefulset
/// - the worker service
///
/// Note: It is assumed the resource does not already exists for simplicity.
/// Returns an `Error` if it does.
/// # Arguments
/// - `client` - A Kubernetes client to create the deployment with.
/// - `job_spec` - A reference to the Malstrom job specification.
pub async fn create(client: Client, job_spec: Arc<MalstromJob>) -> Result<(), CreateJobError> {
    let pp = PatchParams::apply("malstrom-operator");
    let namespace = job_spec
        .metadata
        .namespace
        .clone()
        .unwrap_or("default".to_string());

    let resources = job_spec.all_resource_specs();

    let service_api: Api<Service> = Api::namespaced(client.clone(), &namespace);
    for svc in [resources.coordinator_svc, resources.worker_svc] {
        let name = svc.name_unchecked();
        let service_patch = Patch::Apply(svc);
        service_api.patch(&name, &pp, &service_patch).await?;
    }

    let statefulset_api: Api<StatefulSet> = Api::namespaced(client.clone(), &namespace);
    for sts in [resources.coordinator_sts, resources.worker_sts] {
        let name = sts.name_unchecked();
        let statefulset_patch = Patch::Apply(sts);
        statefulset_api
            .patch(&name, &pp, &statefulset_patch)
            .await?;
    }

    // Update status of CRD
    let state = if job_spec.spec.replicas > 0 {
        JobState::Running
    } else {
        JobState::Suspended
    };

    let malstrom_api: Api<MalstromJob> = Api::namespaced(client, &namespace);
    malstrom_api
        .patch_status(
            &job_spec.name_any(),
            &PatchParams::apply("malstrom-operator"),
            &kube::api::Patch::Merge(MalstromJobStatus {
                state,
                replicas: job_spec.spec.replicas,
            }),
        )
        .await?;

    Ok(())
}

#[derive(Debug, Error)]
pub(crate) enum CreateJobError {
    #[error("Error from Kubernetes: {0}")]
    Kubernetes(#[from] kube::Error),
}
