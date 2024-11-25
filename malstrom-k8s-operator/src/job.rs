use k8s_openapi::api::apps::v1::{StatefulSet, StatefulSetSpec};
use k8s_openapi::api::core::v1::{ConfigMap, ConfigMapEnvSource, EnvFromSource, Service, ServicePort, ServiceSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::{DeleteParams, ObjectMeta, PostParams};
use kube::{Api, Client};
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::crd::MalstromJob;
use crate::reconciliation::ReconciliationError;

pub struct MalstromJobDeplyoment {
    statefulset: StatefulSet,
    service: Service,
    configmap: ConfigMap,
}

impl MalstromJobDeplyoment {
    /// Creates a new stateful set of `n` pods, where `n` is the number of `replicas` given.
    /// Note: It is assumed the resource does not already exists for simplicity. Returns an `Error` if it does.
    /// # Arguments
    /// - `client` - A Kubernetes client to create the deployment with.
    /// - `name` - Name of the deployment to be created
    /// - `replicas` - Number of pod replicas for the Deployment to contain
    /// - `namespace` - Namespace to create the Kubernetes Deployment in.
    pub async fn create(
        client: Client,
        name: &str,
        job_spec: Arc<MalstromJob>,
        namespace: &str,
    ) -> Result<Self, ReconciliationError> {
        // TODO: Support job metadata and labels
        let labels: Option<BTreeMap<String, String>> = job_spec.metadata.labels.clone();

        // this configmap will provide the config as env-vars for the pods
        let configmap = define_configmap(&job_spec, name, namespace, &labels);
        let service = define_service(&job_spec, namespace);

        let mut pod_spec = job_spec.spec.pod_template.clone();
        // TODO: This just loads the config in all pods, we might want to be smarter
        // and only load it in the actual worker pod
        let env_from = EnvFromSource {
            config_map_ref: Some(ConfigMapEnvSource {
                name: name.to_owned(),
                ..ConfigMapEnvSource::default()
            }),
            ..EnvFromSource::default()
        };
        if let Some(pod_spec) = pod_spec.spec.as_mut() {
            for container in pod_spec.containers.iter_mut() {
                container
                    .env_from
                    .get_or_insert_with(Default::default)
                    .push(env_from.clone());
            }
        } else {
            return Err(ReconciliationError::NoPodSpec);
        }

        let statefulset = define_sts(name, namespace, labels, job_spec);

        let deployment = MalstromJobDeplyoment {
            statefulset,
            service,
            configmap,
        };
        deployment.api_create(client, namespace).await?;
        Ok(deployment)
    }

    async fn api_create(&self, client: Client, namespace: &str) -> Result<(), kube::Error> {
        let statefulset_api: Api<StatefulSet> = Api::namespaced(client.to_owned(), namespace);
        let service_api: Api<Service> = Api::namespaced(client.to_owned(), namespace);
        let configmap_api: Api<ConfigMap> = Api::namespaced(client.to_owned(), namespace);
        statefulset_api
            .create(&PostParams::default(), &self.statefulset)
            .await?;
        service_api
            .create(&PostParams::default(), &self.service)
            .await?;
        configmap_api
            .create(&PostParams::default(), &self.configmap)
            .await?;
        Ok(())
    }

    pub async fn delete(client: Client, namespace: &str, name: &str) -> Result<(), kube::Error> {
        let statefulset_api: Api<StatefulSet> = Api::namespaced(client.to_owned(), namespace);
        let configmap_api: Api<ConfigMap> = Api::namespaced(client.to_owned(), namespace);
        statefulset_api
            .delete(name, &DeleteParams::default())
            .await?;
        configmap_api.delete(name, &DeleteParams::default()).await?;
        Ok(())
    }
}

fn define_service(job_spec: &Arc<MalstromJob>, namespace: &str) -> Service {
    let p2p_port: i32 = job_spec.spec.networking.port.into();
    let service = Service {
        metadata: ObjectMeta {
            name: Some(job_spec.spec.networking.service_name.clone()),
            namespace: Some(namespace.to_owned()),
            ..Default::default()
        },
        spec: Some(ServiceSpec{
            cluster_ip: None, // headless,
            ports: Some(vec![
                ServicePort{
                    name: Some("malstrom-p2p".to_owned()),
                    port: p2p_port,
                    target_port: Some(IntOrString::Int(p2p_port)),
                    ..ServicePort::default()
                }
            ]),
            // allows workers to resolve hostnames even if the pod is not yet
            // fully started
            publish_not_ready_addresses: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    };
    service
}

fn define_configmap(job_spec: &Arc<MalstromJob>, name: &str, namespace: &str, labels: &Option<BTreeMap<String, String>>) -> ConfigMap {
    let configmap = ConfigMap {
        data: Some(job_spec.spec.networking.to_map()),
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            namespace: Some(namespace.to_owned()),
            labels: labels.clone(),
            ..Default::default()
        },
        ..Default::default()
    };
    configmap
}

fn define_sts(name: &str, namespace: &str, labels: Option<BTreeMap<String, String>>, job_spec: Arc<MalstromJob>) -> StatefulSet {
    let statefulset: StatefulSet = StatefulSet {
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            namespace: Some(namespace.to_owned()),
            labels: labels.clone(),
            ..Default::default()
        },
        spec: Some(StatefulSetSpec {
            replicas: Some(job_spec.spec.replicas),
            selector: LabelSelector {
                match_expressions: None,
                match_labels: labels,
            },
            template: job_spec.spec.pod_template.clone(),
            // this is important, it tells k8s to start all replicas immediately
            // we need this to ensure our discovery works correctly
            pod_management_policy: Some("Parallel".to_owned()),
            ..Default::default()
        }),
        ..Default::default()
    };
    statefulset
}
