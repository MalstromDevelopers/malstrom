use std::collections::BTreeMap;

use k8s_openapi::{
    api::{
        apps::v1::{
            RollingUpdateStatefulSetStrategy, StatefulSet,
            StatefulSetPersistentVolumeClaimRetentionPolicy, StatefulSetSpec,
            StatefulSetUpdateStrategy,
        },
        core::v1::{
            ConfigMapEnvSource, Container, EmptyDirVolumeSource, EnvFromSource, EnvVar,
            EnvVarSource, GRPCAction, ObjectFieldSelector, PodSecurityContext, PodSpec,
            PodTemplateSpec, Probe, ResourceRequirements, SecretEnvSource, Service, ServicePort,
            ServiceSpec, Volume, VolumeMount,
        },
    },
    apimachinery::pkg::{apis::meta::v1::LabelSelector, util::intstr::IntOrString},
    DeepMerge as _,
};
use kube::{api::ObjectMeta, CustomResource, ResourceExt};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Reexport of CustomResourceExt so the build script can use it
pub use kube::CustomResourceExt;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, JsonSchema, Default)]
pub enum JobState {
    #[default]
    Running,
    Suspended,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, JsonSchema, Default)]
pub struct Binary {
    /// Full URI to the source executable
    pub source: String,

    /// Local path including the file name
    pub destination: String,

    /// Artifact downloader env for configuring authentication.
    /// Note: The environment keys are based on the object store crate.
    pub environment: Option<BTreeMap<String, String>>,
}

/// Kubernetes CRD representing a Malstrom job.
#[derive(CustomResource, Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema, Default)]
#[kube(
    group = "malstrom.io",
    version = "v1alpha",
    kind = "MalstromJob",
    plural = "jobs",
    derive = "PartialEq",
    status = "MalstromJobStatus",
    scale = r#"{"specReplicasPath":".spec.replicas", "statusReplicasPath":".status.replicas"}"#,
    namespaced
)]
#[serde(rename_all = "camelCase")]
pub struct MalstromJobSpec {
    /// Replica count of deployment. Note: Signed int used to be conformant with Kubernetes API
    pub replicas: i32,

    /// Configuration of binary
    pub binary: Option<Binary>,

    /// Desired running status of the job
    pub job_state: JobState,

    /// Job configuration environment variables
    #[serde(default)]
    pub job_environment: Vec<EnvVar>,

    /// Job configuration environment variables from configmap    
    #[serde(default)]
    pub job_environment_configmap_refs: Vec<String>,

    /// Job configuration environment variables from secret
    #[serde(default)]
    pub job_environment_secret_refs: Vec<String>,

    /// Init container envrionment variable from secrest for
    /// configuring the e.g. artifact downloader
    #[serde(default)]
    pub init_environment_secret_refs: Vec<String>,

    #[serde(default)]
    pub pod_spec_template: k8s_openapi::api::core::v1::PodSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct MalstromJobStatus {
    /// Current state of job
    pub state: JobState,

    /// Current replica count
    pub replicas: i32,
}

pub struct MalstromJobResourceSpecs {
    pub coordinator_svc: Service,
    pub coordinator_sts: StatefulSet,
    pub worker_svc: Service,
    pub worker_sts: StatefulSet,
}

impl MalstromJob {
    /// Return all specifications for a full deployment
    pub fn all_resource_specs(&self) -> MalstromJobResourceSpecs {
        MalstromJobResourceSpecs {
            coordinator_svc: self.coordinator_service_spec(),
            coordinator_sts: self.coordinator_statefulset(),
            worker_svc: self.worker_service_spec(),
            worker_sts: self.worker_statefulset_spec(),
        }
    }

    pub fn worker_service_spec(&self) -> Service {
        let name = self.name_any();
        let namespace = self.namespace();

        let mut labels = BTreeMap::new();
        labels.insert("app.malstorm.io".to_owned(), name.to_owned());
        labels.insert("app.malstorm.io/role".to_owned(), "worker".to_owned());

        Service {
            metadata: ObjectMeta {
                name: Some(format!("{name}-worker")),
                namespace,
                labels: Some(labels.clone()),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                cluster_ip: None,
                selector: Some(labels),
                publish_not_ready_addresses: Some(true),
                ports: Some(Vec::from_iter([ServicePort {
                    port: 29091,
                    target_port: Some(IntOrString::Int(29091)),
                    name: Some("malstrom-worker-worker".to_owned()),
                    ..Default::default()
                }])),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    pub fn coordinator_service_spec(&self) -> Service {
        let name = self.name_any();
        let namespace = self.namespace();

        let mut labels = BTreeMap::new();
        labels.insert("app.malstorm.io".to_owned(), name.to_owned());
        labels.insert("app.malstorm.io/role".to_owned(), "coordinator".to_owned());

        Service {
            metadata: ObjectMeta {
                name: Some(format!("{name}-coordinator")),
                namespace,
                labels: Some(labels.clone()),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                cluster_ip: None,
                selector: Some(labels),
                ports: Some(Vec::from_iter([ServicePort {
                    port: 29091,
                    target_port: Some(IntOrString::Int(29091)),
                    name: Some("malstrom-coordinator-worker".to_owned()),
                    ..Default::default()
                }])),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    pub fn worker_statefulset_spec(&self) -> StatefulSet {
        let name = self.name_any();
        let namespace = self.namespace();
        // TODO: Support job metadata and labels
        let mut labels: BTreeMap<String, String> = self.metadata.labels.clone().unwrap_or_default();
        labels.insert("app.malstorm.io".to_owned(), name.to_owned());
        labels.insert("app.malstorm.io/role".to_owned(), "worker".to_owned());

        let volumes = self.volume_spec();
        let init_containers = self.init_containers_spec();
        let containers = self.worker_containers_spec();

        StatefulSet {
            metadata: ObjectMeta {
                name: Some(format!("{name}-worker")),
                namespace: namespace.clone(),
                labels: Some(labels.clone()),
                ..ObjectMeta::default()
            },
            spec: Some(StatefulSetSpec {
                replicas: Some(self.spec.replicas),
                revision_history_limit: Some(10),
                // PANIC: We can use unchecked here since we created
                // the service ourselves and we know it has a name
                service_name: self.worker_service_spec().name_unchecked(),
                selector: LabelSelector {
                    match_expressions: None,
                    match_labels: Some(labels.clone()),
                },
                // this is important since the coordinator will wait
                // for all workers to be available before starting the job
                pod_management_policy: Some("Parallel".to_owned()),
                template: PodTemplateSpec {
                    metadata: Some(ObjectMeta {
                        name: Some(name.to_owned()),
                        namespace: namespace.clone(),
                        labels: Some(labels),
                        ..ObjectMeta::default()
                    }),
                    spec: Some(PodSpec {
                        restart_policy: Some("Always".to_owned()),
                        service_account_name: Some("malstrom-operator".to_owned()),
                        volumes: Some(volumes),
                        containers,
                        init_containers: Some(init_containers),
                        scheduler_name: Some("default-scheduler".to_owned()),
                        security_context: Some(PodSecurityContext {
                            app_armor_profile: None,
                            fs_group: None,
                            fs_group_change_policy: None,
                            run_as_group: None,
                            run_as_non_root: None,
                            run_as_user: None,
                            se_linux_options: None,
                            seccomp_profile: None,
                            supplemental_groups: None,
                            sysctls: None,
                            windows_options: None,
                        }),
                        service_account: Some("malstrom-operator".to_owned()),
                        termination_grace_period_seconds: Some(30),
                        dns_policy: Some("ClusterFirst".to_owned()),
                        ..self.spec.pod_spec_template.clone()
                    }),
                },
                // TODO: is this policy correct?
                persistent_volume_claim_retention_policy: Some(
                    StatefulSetPersistentVolumeClaimRetentionPolicy {
                        when_deleted: Some("Retain".to_owned()),
                        when_scaled: Some("Retain".to_owned()),
                    },
                ),
                update_strategy: Some(StatefulSetUpdateStrategy {
                    rolling_update: Some(RollingUpdateStatefulSetStrategy {
                        max_unavailable: None,
                        partition: Some(0),
                    }),
                    type_: Some("RollingUpdate".to_owned()),
                }),

                ..StatefulSetSpec::default()
            }),
            ..StatefulSet::default()
        }
    }

    pub fn coordinator_statefulset(&self) -> StatefulSet {
        let name = self.name_any();
        let namespace = self.namespace();
        // TODO: Support job metadata and labels
        let mut labels: BTreeMap<String, String> = self.metadata.labels.clone().unwrap_or_default();
        labels.insert("app.malstorm.io".to_owned(), name.to_owned());
        labels.insert("app.malstorm.io/role".to_owned(), "coordinator".to_owned());

        let volumes = self.volume_spec();
        let init_containers = self.init_containers_spec();
        let containers = self.coordinator_container_spec();

        StatefulSet {
            metadata: ObjectMeta {
                name: Some(format!("{name}-coordinator")),
                namespace: namespace.clone(),
                labels: Some(labels.clone()),
                ..ObjectMeta::default()
            },
            spec: Some(StatefulSetSpec {
                replicas: Some(1),
                revision_history_limit: Some(10),
                // PANIC: We can use unchecked here since we created
                // the service ourselves and we know it has a name
                service_name: self.coordinator_service_spec().name_unchecked(),
                selector: LabelSelector {
                    match_expressions: None,
                    match_labels: Some(labels.clone()),
                },
                // this is important since the coordinator will wait
                // for all workers to be available before starting the job
                pod_management_policy: Some("Parallel".to_owned()),
                template: PodTemplateSpec {
                    metadata: Some(ObjectMeta {
                        name: Some(name.to_owned()),
                        namespace: namespace.clone(),
                        labels: Some(labels),
                        ..ObjectMeta::default()
                    }),
                    spec: Some(PodSpec {
                        restart_policy: Some("Always".to_owned()),
                        service_account_name: Some("malstrom-operator".to_owned()),
                        volumes: Some(volumes),
                        containers,
                        init_containers: Some(init_containers),
                        scheduler_name: Some("default-scheduler".to_owned()),
                        security_context: Some(PodSecurityContext {
                            app_armor_profile: None,
                            fs_group: None,
                            fs_group_change_policy: None,
                            run_as_group: None,
                            run_as_non_root: None,
                            run_as_user: None,
                            se_linux_options: None,
                            seccomp_profile: None,
                            supplemental_groups: None,
                            sysctls: None,
                            windows_options: None,
                        }),
                        service_account: Some("malstrom-operator".to_owned()),
                        termination_grace_period_seconds: Some(30),
                        dns_policy: Some("ClusterFirst".to_owned()),
                        ..self.spec.pod_spec_template.clone()
                    }),
                },
                // TODO: is this policy correct?
                persistent_volume_claim_retention_policy: Some(
                    StatefulSetPersistentVolumeClaimRetentionPolicy {
                        when_deleted: Some("Retain".to_owned()),
                        when_scaled: Some("Retain".to_owned()),
                    },
                ),
                update_strategy: Some(StatefulSetUpdateStrategy {
                    rolling_update: Some(RollingUpdateStatefulSetStrategy {
                        max_unavailable: None,
                        partition: Some(0),
                    }),
                    type_: Some("RollingUpdate".to_owned()),
                }),

                ..StatefulSetSpec::default()
            }),
            ..StatefulSet::default()
        }
    }

    /// Create the volume specs for the job containers
    fn volume_spec(&self) -> Vec<Volume> {
        let mut volumes = vec![Volume {
            name: "artifact".to_owned(),
            empty_dir: Some(EmptyDirVolumeSource::default()),
            ..Volume::default()
        }];
        // Add artifact volume to volumes list
        // The user configuration has higher priority
        k8s_openapi::merge_strategies::list::map(
            &mut volumes,
            self.spec
                .pod_spec_template
                .volumes
                .clone()
                .unwrap_or_default(),
            &[|current, new| current.name == new.name],
            |current, new| {
                current.merge_from(new);
            },
        );
        volumes
    }

    /// Create the worker container spec
    fn worker_containers_spec(&self) -> Vec<Container> {
        let mut envs = vec![
            EnvVar {
                name: "MALSTROM_K8S_WORKER_HOSTNAME".to_owned(),
                value_from: Some(EnvVarSource {
                    field_ref: Some(ObjectFieldSelector {
                        field_path: "metadata.name".to_owned(),
                        api_version: Some("v1".to_owned()),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
            EnvVar {
                name: "MALSTROM_K8S_WORKER_SVC_NAME".to_owned(),
                value: self.worker_service_spec().metadata.name,
                ..Default::default()
            },
            EnvVar {
                name: "MALSTROM_K8S_WORKER_STS_NAME".to_owned(),
                value: Some(format!("{}-worker", self.name_any())),
                ..Default::default()
            },
            EnvVar {
                name: "MALSTROM_K8S_COORDINATOR_SVC_NAME".to_owned(),
                value: self.coordinator_service_spec().metadata.name,
                ..Default::default()
            },
            EnvVar {
                name: "MALSTROM_K8S_SCALE".to_owned(),
                value: Some(self.spec.replicas.to_string()),
                ..Default::default()
            },
            EnvVar {
                name: "MALSTROM_K8S_NS".to_owned(),
                value: self.metadata.namespace.clone(),
                ..Default::default()
            },
        ];
        k8s_openapi::merge_strategies::list::map(
            &mut envs,
            self.spec.job_environment.clone(),
            &[|current, new| current.name == new.name],
            |current, new| {
                current.merge_from(new);
            },
        );

        let mut containers = self.containers_spec(envs);
        // Add main container
        // The user configuration has higher priority
        k8s_openapi::merge_strategies::list::map(
            &mut containers,
            self.spec.pod_spec_template.containers.clone(),
            &[|current, new| current.name == new.name],
            |current, new| {
                current.merge_from(new);
            },
        );
        containers
    }

    /// Create the container spec for the coordinator
    fn coordinator_container_spec(&self) -> Vec<Container> {
        let mut envs = vec![
            EnvVar {
                name: "MALSTROM_K8S_IS_COORDINATOR".to_owned(),
                value: Some("true".to_string()),
                ..Default::default()
            },
            EnvVar {
                name: "MALSTROM_K8S_WORKER_SVC_NAME".to_owned(),
                value: self.worker_service_spec().metadata.name,
                ..Default::default()
            },
            EnvVar {
                name: "MALSTROM_K8S_WORKER_STS_NAME".to_owned(),
                value: Some(format!("{}-worker", self.name_any())),
                ..Default::default()
            },
            EnvVar {
                name: "MALSTROM_K8S_COORDINATOR_SVC_NAME".to_owned(),
                value: self.coordinator_service_spec().metadata.name,
                ..Default::default()
            },
            EnvVar {
                name: "MALSTROM_K8S_SCALE".to_owned(),
                value: Some(self.spec.replicas.to_string()),
                ..Default::default()
            },
            EnvVar {
                name: "MALSTROM_K8S_NS".to_owned(),
                value: self.metadata.namespace.clone(),
                ..Default::default()
            },
        ];
        k8s_openapi::merge_strategies::list::map(
            &mut envs,
            self.spec.job_environment.clone(),
            &[|current, new| current.name == new.name],
            |current, new| {
                current.merge_from(new);
            },
        );

        let mut containers = self.containers_spec(envs);
        // Add main container
        // The user configuration has higher priority
        k8s_openapi::merge_strategies::list::map(
            &mut containers,
            self.spec.pod_spec_template.containers.clone(),
            &[|current, new| current.name == new.name],
            |current, new| {
                current.merge_from(new);
            },
        );
        containers
    }

    /// Create the container spec for coordinator and worker pods
    fn containers_spec(&self, envs: Vec<EnvVar>) -> Vec<Container> {
        vec![Container {
            name: "main".to_owned(),
            volume_mounts: Some(vec![VolumeMount {
                mount_path: "/artifact".to_owned(),
                name: "artifact".to_owned(),
                ..VolumeMount::default()
            }]),
            command: self
                .spec
                .binary
                .as_ref()
                .map(|binary| vec![binary.destination.clone()]),
            image_pull_policy: Some("Always".to_owned()),
            resources: Some(ResourceRequirements {
                claims: None,
                limits: None,
                requests: None,
            }),
            readiness_probe: Some(Probe {
                grpc: Some(GRPCAction {
                    port: 29091,
                    service: None,
                }),
                initial_delay_seconds: Some(5),
                period_seconds: Some(5),
                ..Default::default()
            }),
            env: Some(envs),
            env_from: Some(
                self.spec
                    .job_environment_configmap_refs
                    .iter()
                    .map(|n| EnvFromSource {
                        config_map_ref: Some(ConfigMapEnvSource {
                            name: n.to_owned(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })
                    .chain(
                        self.spec
                            .job_environment_secret_refs
                            .iter()
                            .map(|n| EnvFromSource {
                                secret_ref: Some(SecretEnvSource {
                                    name: n.to_owned(),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }),
                    )
                    .collect(),
            ),
            termination_message_path: Some("/dev/termination-log".to_owned()),
            termination_message_policy: Some("File".to_owned()),
            ..Container::default()
        }]
    }

    /// Create the spec for init conatiners, including the artifact
    /// downloader init container
    fn init_containers_spec(&self) -> Vec<Container> {
        let envs = vec![EnvVar {
            name: "RUST_LOG".to_owned(),
            value: Some("artifact_downloader=info".to_owned()),
            ..Default::default()
        }];

        let env_from: Vec<_> = self
            .spec
            .init_environment_secret_refs
            .iter()
            .map(|n| EnvFromSource {
                secret_ref: Some(SecretEnvSource {
                    name: n.to_owned(),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .collect();

        let mut init_containers = self.spec.binary.as_ref().map(|binary| {
            vec![Container {
                name: "artifact-downloader".to_owned(),
                // TODO: Configurable registry
                image: Some("ghcr.io/malstromdevelopers/artifact-downloader:latest".to_owned()),
                image_pull_policy: Some("Always".to_owned()),
                args: Some(vec![
                    "--source".to_owned(),
                    binary.source.clone(),
                    "--destination".to_owned(),
                    binary.destination.clone(),
                ]),
                volume_mounts: Some(vec![VolumeMount {
                    mount_path: "/artifact".to_owned(),
                    name: "artifact".to_owned(),
                    ..VolumeMount::default()
                }]),
                env: Some(envs),
                env_from: Some(env_from),
                resources: Some(ResourceRequirements {
                    claims: None,
                    limits: None,
                    requests: None,
                }),
                termination_message_path: Some("/dev/termination-log".to_owned()),
                termination_message_policy: Some("File".to_owned()),
                ..Container::default()
            }]
        });

        // Add artifact downloader init container
        // The user configuration has higher priority
        k8s_openapi::merge_strategies::list::map(
            &mut init_containers,
            self.spec.pod_spec_template.init_containers.clone(),
            &[|current, new| current.name == new.name],
            |current, new| {
                current.merge_from(new);
            },
        );
        init_containers.unwrap_or_default()
    }
}
