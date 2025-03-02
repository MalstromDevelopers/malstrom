fn main() {
    tonic_build::compile_protos("proto/exchange.proto").unwrap();
    tonic_build::compile_protos("proto/k8s_operator_api.proto").unwrap();
}
