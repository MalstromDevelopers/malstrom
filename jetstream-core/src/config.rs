use crate::{Scale, WorkerId};
use lazy_static::lazy_static;
use serde::Deserialize;
use tonic::transport::Uri;

lazy_static! {
    pub static ref CONFIG: Config = Config::new();
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Config {
    // id of this worker
    pub worker_id: WorkerId,
    // communication port for inter-worker comm
    pub port: u16,
    // name of k8s statefulset
    pub sts_name: Option<String>,
    // total cluster size before any rescaling
    pub initial_scale: Scale,

    pub cluster_addresses: Option<Vec<String>>,
}

impl Config {
    fn new() -> Self {
        let mut builder = config::Config::builder();
        if let Ok(f) = std::env::var("JETSTREAM_CONFIG_PATH") {
            builder = builder.add_source(config::File::with_name(&f))
        }

        builder
            .add_source(config::Environment::with_prefix("JETSTREAM"))
            .build()
            .expect("Error loading configuration")
            .try_deserialize()
            .expect("Invalid configuratiioin")
    }

    pub fn get_peer_uris(&self) -> Vec<(WorkerId, Uri)> {
        if let Some(sts_name) = &self.sts_name {
            let port = self.port;
            (0..self.initial_scale)
                .filter(|i| *i != self.worker_id)
                .map(|i| (i, format!("http://{sts_name}-{i}.{sts_name}:{port}")))
                .map(|(i, x)| (i, x.parse::<Uri>().unwrap()))
                .collect()
        } else {
            self.cluster_addresses.as_ref().expect(
                "If not using kubernetes DNS discovery, cluster addresses must be set through the config"
            ).iter().enumerate().filter(
                |(i, _)| *i != self.worker_id).map(
                    |(i, x)| (i, x.parse::<Uri>().unwrap())).collect()
        }
    }
}
