//! Malstrom Kubernetes operator
use std::{sync::Arc, time::Duration};

use crate::reconciliation::reconcile;
use actix_web::http::StatusCode;
use actix_web::ResponseError;
use crd::MalstromJob;
use crd::MalstromJobSpec;
use futures::stream::StreamExt;
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::api::PostParams;
use kube::core::ErrorResponse;
use kube::Api;
use kube::Client;
use kube::CustomResourceExt;
use kube_runtime::controller::Action;
use kube_runtime::watcher::Config;
use kube_runtime::Controller;
use log::info;
use log::{debug, error};
use reconciliation::ReconciliationError;

mod crd;
mod finalizer;
mod job;
mod reconciliation;

use actix_multipart::form::{tempfile::TempFile, MultipartForm};
use actix_web::{get, put, App, HttpRequest, HttpResponse, HttpServer, Responder};
use thiserror::Error;

/// Main entry point
#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let kubernetes_client: Client = Client::try_default()
        .await
        .expect("Expected a valid KUBECONFIG environment variable.");
    // create CRD in cluster
    let crd_api: Api<CustomResourceDefinition> = Api::all(kubernetes_client.clone());
    let crd = MalstromJob::crd();
    match crd_api.create(&PostParams::default(), &crd).await {
        Ok(_) => info!("Successfully create CRD"),
        Err(kube_client::error::Error::Api(ErrorResponse {
            status: _,
            message: _,
            reason: _,
            code: 409,
        })) => info!("CRD already exists"),
        e => {
            e.unwrap();
        }
    }

    let api: Api<MalstromJob> = Api::all(kubernetes_client.clone());

    let context: Arc<Context> = Arc::new(Context {
        client: kubernetes_client,
    });

    // let server_thread = tokio::spawn(async move {
    //     let http_server = HttpServer::new(|| App::new().service(health).service(upload_executable))
    //         .bind(("0.0.0.0", 8080))
    //         .expect("Expected to bind HTTP port");
    //     http_server.run().await.unwrap()
    // });

    Controller::new(api, Config::default())
        .run(reconcile, on_error, context)
        .for_each(|res| async move {
            match res {
                Ok(x) => debug!("{x:?}"),
                Err(e) => error!("{e:?}")
            }
        })
        .await;
    // server_thread.await.unwrap()
}

/// Context for `Controller`
struct Context {
    /// Kubernetes client
    client: Client,
}

/// Error function to call when Constroller receives an error
fn on_error(echo: Arc<MalstromJob>, error: &ReconciliationError, _: Arc<Context>) -> Action {
    log::error!("Reconciliation error:\n{:?}.\n{:?}", error, echo);
    Action::requeue(Duration::from_secs(5))
}

#[derive(Debug, MultipartForm)]
struct Upload {
    #[multipart]
    file: TempFile,
}

#[get("/health")]
async fn health(_: HttpRequest) -> impl Responder {
    HttpResponse::Ok().json("healthy")
}

/// Upload a Malstrom job as an executable for fast prototyping
#[put("/executable")]
async fn upload_executable(
    form: MultipartForm<Upload>,
) -> Result<impl Responder, UploadExecutableError> {
    let file = form.into_inner().file;
    let file_name = file
        .file_name
        .as_ref()
        .ok_or(UploadExecutableError::MissingName)?;
    let path = format!("/malstrom/executables/{file_name}");
    file.file
        .persist(path)
        .map_err(UploadExecutableError::from)?;
    Ok(HttpResponse::Ok())
}

#[derive(Debug, Error)]
enum UploadExecutableError {
    #[error("Uploaded file does not have a name")]
    MissingName,
    #[error(transparent)]
    Persist(#[from] tempfile::PersistError),
}
impl ResponseError for UploadExecutableError {
    fn status_code(&self) -> StatusCode {
        match self {
            UploadExecutableError::MissingName => StatusCode::BAD_REQUEST,
            UploadExecutableError::Persist(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
