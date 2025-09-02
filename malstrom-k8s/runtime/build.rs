use std::path::PathBuf;

fn main() {
    let proto_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("proto");
    tonic_build::compile_protos(proto_dir.join("exchange.proto")).unwrap();
    tonic_build::compile_protos(proto_dir.join("k8s_operator_api.proto")).unwrap();
}
