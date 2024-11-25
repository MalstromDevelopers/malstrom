fn main() {
    tonic_build::compile_protos("proto/exchange.proto").unwrap();
}