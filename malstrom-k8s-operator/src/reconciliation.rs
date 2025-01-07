use std::{sync::Arc, time::Duration};

use kube::{Resource, ResourceExt};
use kube_runtime::controller::Action;

use crate::{crd::MalstromJob, finalizer, job::MalstromJobDeplyoment, Context};

/// Action to be taken upon a `MalstromJob` resource during reconciliation
enum DecisionAction {
    // Create a new job
    Create,
    // Delete an existing job
    Delete,
    // change some parameter
    #[allow(dead_code)]
    Patch,
    NoOp,
}

pub async fn reconcile(
    malstrom_job: Arc<MalstromJob>,
    context: Arc<Context>,
) -> Result<Action, ReconciliationError> {
    let namespace: String = malstrom_job.namespace().unwrap_or("default".to_string());
    let name = malstrom_job.name_any(); // Name of the Echo resource is used to name the subresources as well.

    match make_decision(&malstrom_job) {
        DecisionAction::Create => {
            // Add finalizer to self
            // TODO: factor out client to var
            finalizer::add(context.client.clone(), &name, &namespace).await?;

            MalstromJobDeplyoment::create(context.client.clone(), &name, malstrom_job, &namespace).await?;
            Ok(Action::requeue(Duration::from_secs(10)))
        }
        DecisionAction::Delete => {
            // First express the desire to delete the resources
            MalstromJobDeplyoment::delete(context.client.clone(), &name, &namespace).await?;

            // Then remove the finalizer to excute the deletion
            // for all the resources
            finalizer::delete(context.client.clone(), &name, &namespace).await?;
            Ok(Action::await_change())
        }
        DecisionAction::Patch => unimplemented!(),
        DecisionAction::NoOp => Ok(Action::requeue(Duration::from_secs(10))),
    }
}

/// Decide what action to perform depending on the currently and desired states
fn make_decision(malstrom_job: &MalstromJob) -> DecisionAction {
    if malstrom_job.meta().deletion_timestamp.is_some() {
        DecisionAction::Delete
    } else if malstrom_job
        .meta()
        .finalizers
        .as_ref()
        .map_or(true, |finalizers| finalizers.is_empty())
    {
        DecisionAction::Create
    }
    // TODO: compare against deployed replica count, template, image, ...
    else {
        DecisionAction::NoOp
    }
}

/// All errors possible to occur during reconciliation
#[derive(Debug, thiserror::Error)]
pub enum ReconciliationError {
    // Wrapper for Kubernetes error
    #[error(transparent)]
    KubeError(#[from] kube::Error),
    #[error("A pod spec must be specified")]
    NoPodSpec
}
