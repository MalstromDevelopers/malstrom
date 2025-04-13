use super::job;
use crate::{
    finalizer::{self, AddFinalizerError, DeleteFinalizerError},
    job::{CreateJobError, DeleteJobError, PatchJobError},
    Context,
};
use crds::MalstromJob;
use k8s_openapi::api::apps::v1::StatefulSet;
use kube::{Api, Resource, ResourceExt};
use kube_runtime::controller::Action;
use std::{sync::Arc, time::Duration};
use thiserror::Error;
use tracing::debug;

/// Action to be taken upon an `Echo` resource during reconciliation
#[derive(Debug)]
enum DecisionAction {
    Put,
    Delete,
    Patch,
    NoOp,
}

pub async fn reconcile(
    malstrom_job: Arc<MalstromJob>,
    context: Arc<Context>,
) -> Result<Action, ReconcileError> {
    let namespace: String = malstrom_job
        .namespace()
        .unwrap_or_else(|| "default".to_string());
    let name = malstrom_job.name_any();

    let decision = make_decision(&malstrom_job, &context).await;
    debug!("Decision {decision:?}");
    match decision {
        DecisionAction::Put => {
            Box::pin(job::create(context.client.clone(), malstrom_job)).await?;

            finalizer::add(context.client.clone(), &name, &namespace).await?;
            Ok(Action::requeue(Duration::from_secs(10)))
        }
        DecisionAction::Delete => {
            // First express the desire to delete the resources
            job::delete(context.client.clone(), malstrom_job.clone()).await?;

            // Then remove the finalizer to excute the deletion
            // for all the resources
            finalizer::delete(context.client.clone(), &name, &namespace).await?;
            Ok(Action::requeue(Duration::from_secs(10)))
        }
        DecisionAction::Patch => {
            Box::pin(job::patch(context.client.clone(), malstrom_job)).await?;
            Ok(Action::requeue(Duration::from_secs(10)))
        }
        DecisionAction::NoOp => Ok(Action::requeue(Duration::from_secs(10))),
    }
}

#[derive(Debug, Error)]
pub(crate) enum ReconcileError {
    #[error("Error creating new job: {0}")]
    Create(#[from] CreateJobError),
    #[error("Error patching job: {0}")]
    Patch(#[from] PatchJobError),
    #[error("Error deleting job: {0}")]
    Delete(#[from] DeleteJobError),

    #[error("Error adding finalizer: {0}")]
    AddFinalizer(#[from] AddFinalizerError),
    #[error("Error deleting finalizer: {0}")]
    DeleteFinalizer(#[from] DeleteFinalizerError),
}

/// Decide what action to perform depending on the currently and desired states
async fn make_decision(malstrom_job: &MalstromJob, context: &Context) -> DecisionAction {
    if malstrom_job.meta().deletion_timestamp.is_some() {
        DecisionAction::Delete
    } else if malstrom_job
        .meta()
        .finalizers
        .as_ref()
        .is_none_or(std::vec::Vec::is_empty)
    {
        DecisionAction::Put
    // Sorry this is really ugly, ill fix it later
    } else if let Ok(decision) = compare_with_cluster(malstrom_job, context).await {
        decision
    } else {
        DecisionAction::NoOp
    }
}

/// Check what changes need to be made to the job to comply
/// to the new job spec.
/// Currently this is SUPER simple, as the only allowed change
/// is in parallelism.
/// There are good and bad reasons for this:
/// - Good: K8S disallows many changes to statefulsets anyway
/// - Bad: I am lazy
/// 
/// Honestly this is not toooo bad, since many changes would
/// imply a full job restart, which is something we may want to be
/// more explicit about... anyway TBD
async fn compare_with_cluster(
    malstrom_job: &MalstromJob,
    context: &Context,
) -> Result<DecisionAction, CompareWithClusterError> {
    let worker_sts_name = malstrom_job.worker_statefulset_spec().name_any();
    let namespace = malstrom_job
        .metadata
        .namespace
        .clone()
        .unwrap_or("default".to_owned());

    let statefulset_api: Api<StatefulSet> = Api::namespaced(context.client.clone(), &namespace);

    let sts_actual = statefulset_api.get(&worker_sts_name).await?;
    let sts_desired = malstrom_job.worker_statefulset_spec();

    let replicas_actual = sts_actual.spec.and_then(|x| x.replicas).unwrap_or_default();
    let replicas_desired = sts_desired
        .spec
        .and_then(|x| x.replicas)
        .unwrap_or_default();
    if replicas_actual != replicas_desired {
        Ok(DecisionAction::Patch)
    } else {
        Ok(DecisionAction::NoOp)
    }
}

#[derive(Debug, Error)]
enum CompareWithClusterError {
    #[error("Error from Kubernetes: {0}")]
    Kubernetes(#[from] kube::Error),
}
