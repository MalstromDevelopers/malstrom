use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::Service;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client, ResourceExt};
use std::sync::Arc;
use tonic::transport::Endpoint;

use crate::coordinator_api::{get_coord_api_client, RescaleRequest};
use crds::{JobState, MalstromJob, MalstromJobStatus};
use thiserror::Error;

pub async fn patch(client: Client, job_spec: Arc<MalstromJob>) -> Result<(), PatchJobError> {
    let pp = PatchParams::apply("malstrom-operator");
    let namespace = job_spec
        .metadata
        .namespace
        .clone()
        .unwrap_or("default".to_string());
    let name = job_spec.name_any();

    let resources = job_spec.all_resource_specs();

    let service_api: Api<Service> = Api::namespaced(client.clone(), &namespace);
    for svc in [resources.coordinator_svc, resources.worker_svc] {
        let service_patch = Patch::Apply(&svc);
        service_api
            .patch(&svc.name_any(), &pp, &service_patch)
            .await?;
    }

    let statefulset_api: Api<StatefulSet> = Api::namespaced(client.clone(), &namespace);

    let deployed_worker_sts = statefulset_api
        .get(&job_spec.worker_statefulset_spec().name_any())
        .await?;
    let deployed_scale = deployed_worker_sts
        .spec
        .and_then(|x| x.replicas)
        .unwrap_or(0);
    let desired_scale = job_spec
        .worker_statefulset_spec()
        .spec
        .and_then(|x| x.replicas)
        .unwrap_or(0);
    if deployed_scale != desired_scale {
        perform_rescale(
            client.clone(),
            job_spec.clone(),
            desired_scale,
            deployed_scale,
        )
        .await?;
    }

    for sts in [resources.coordinator_sts, resources.worker_sts] {
        let statefulset_patch = Patch::Apply(&sts);
        statefulset_api
            .patch(&sts.name_any(), &pp, &statefulset_patch)
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
            &name,
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
pub(crate) enum PatchJobError {
    #[error("Error from Kubernetes: {0}")]
    Kubernetes(#[from] kube::Error),
    #[error("Error perfoming job rescale: {0}")]
    Rescale(#[from] RescaleError),
}

/// Rescale the job. This is always a two step process:
/// - scale up
///     1. Create new worker pod(s)
///     2. Instruct coordinator to add new pods to job
/// - scale down
///     1. Instruct coordinator to remove worker(s)
///     2. Remove pods
async fn perform_rescale(
    client: Client,
    job_spec: Arc<MalstromJob>,
    desired_scale: i32,
    current_scale: i32,
) -> Result<(), RescaleError> {
    let namespace = job_spec
        .namespace()
        .clone()
        .unwrap_or("default".to_string());
    let coord_svc_name = job_spec.coordinator_service_spec().name_any();
    let coord_svc_url = format!("http://{coord_svc_name}.{namespace}.svc.cluster.local:29091");
    let coord_svc_url: Endpoint = coord_svc_url
        .parse()
        .map_err(|e| RescaleError::ServiceUrl(format!("{e:?}")))?;

    let pp = PatchParams::apply("malstrom-operator");
    let statefulset_api: Api<StatefulSet> = Api::namespaced(client.clone(), &namespace);
    let worker_sts_name = job_spec.worker_statefulset_spec().name_any();

    let statefulset_patch = Patch::Merge(serde_json::json!({"spec": {"replicas": desired_scale}}));

    let mut api_client = get_coord_api_client(coord_svc_url).await?;
    match desired_scale - current_scale {
        0 => Ok(()),
        1..=i32::MAX => {
            statefulset_api
                .patch(&worker_sts_name, &pp, &statefulset_patch)
                .await?;

            let desired_scale = desired_scale as u64;
            api_client.rescale(RescaleRequest { desired_scale }).await?;
            Ok(())
        }
        i32::MIN..0 => {
            let desired_scale = desired_scale as u64;
            api_client.rescale(RescaleRequest { desired_scale }).await?;

            statefulset_api
                .patch(&worker_sts_name, &pp, &statefulset_patch)
                .await?;
            Ok(())
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum RescaleError {
    #[error("Error from Kubernetes: {0}")]
    Kubernetes(#[from] kube::Error),
    #[error("Error connecting to job coordinator")]
    Connection(#[from] tonic::transport::Error),
    #[error("Error making rescale request to job coordinator")]
    RescaleRequest(#[from] tonic::Status),
    #[error("Invalid coordinator service URL: {0}")]
    ServiceUrl(String),
}
