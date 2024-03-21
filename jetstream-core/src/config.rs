use crate::WorkerId;
use itertools::Itertools;
use serde::{Deserialize, Deserializer};
use tonic::transport::Uri;

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Config {
    // id of this worker
    pub worker_id: WorkerId,
    // communication port for inter-worker comm
    pub port: u16,
    // Addresses of all other workers in the cluster
    #[serde(deserialize_with = "deser_uri_vec")]
    pub cluster_addresses: Vec<Uri>,
}
fn deser_uri_vec<'de, D>(deser: D) -> Result<Vec<Uri>, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error;
    let v: Vec<String> = Vec::deserialize(deser)?;
    let c = v
        .into_iter()
        .map(|x| x.parse::<Uri>().map_err(Error::custom));
    c.try_collect()
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
            .expect("Invalid configuration")
    }

    pub fn get_peer_uris(&self) -> Vec<(WorkerId, Uri)> {
        self.cluster_addresses
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != self.worker_id)
            .map(|x| (x.0, x.1.clone()))
            .collect()
    }
}
