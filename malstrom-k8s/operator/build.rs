//! Simple build scrip which does two things
//! - generate code for the GRPC API used to communicate with the job operator
//! - generate the MalstromJob CRD YAML for the helm chart to use
use std::fs::File;
use std::path::PathBuf;

use crds::{CustomResourceExt, MalstromJob};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("Expected crate parent directory to be accessible")
        .join("runtime")
        .join("proto")
        .join("k8s_operator_api.proto");
    assert!(proto.exists());
    tonic_build::compile_protos(proto)?;
    generate_crd_yaml()?;
    Ok(())
}

fn generate_crd_yaml() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::path::Path::new("../helm/malstrom-operator/crds");
    if dir.exists() {
        let writer = File::create(dir.join("MalstromJob.yaml"))?;
        serde_yaml::to_writer(writer, &MalstromJob::crd())?;
    }

    Ok(())
}
