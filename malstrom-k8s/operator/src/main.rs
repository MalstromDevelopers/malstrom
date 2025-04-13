//! Malstrom Kubernetes operator
use std::{sync::Arc, time::Duration};

use crate::reconcilation::reconcile;
use crds::MalstromJob;
use futures::stream::StreamExt;
use kube::Api;
use kube::Client;
use kube_runtime::controller::Action;
use kube_runtime::watcher::Config;
use kube_runtime::Controller;
use reconcilation::ReconcileError;
use tracing::debug;
use tracing::{error, info};

mod coordinator_api;
mod finalizer;
mod job;
mod reconcilation;

/// Main entry point
#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let kubernetes_client: Client = Client::try_default()
        .await
        .expect("Expected a valid KUBECONFIG environment variable.");

    let api: Api<MalstromJob> = Api::all(kubernetes_client.clone());
    let context: Arc<Context> = Arc::new(Context {
        client: kubernetes_client.clone(),
    });

    info!("Starting controller");
    Controller::new(api.clone(), Config::default())
        .run(reconcile, on_error, context)
        .map(|result| {
            if let Ok(x) = result {
                debug!("Reconciliation OK: {x:?}");
            }
        })
        .collect::<()>()
        .await;
}

/// Context for `Controller`
struct Context {
    /// Kubernetes client
    client: Client,
}

/// Error function to call when the controller receives an error
#[allow(clippy::needless_pass_by_value)]
fn on_error(_job: Arc<MalstromJob>, error: &ReconcileError, _: Arc<Context>) -> Action {
    error!("Reconciliation error: {error}");
    Action::requeue(Duration::from_secs(5))
}
