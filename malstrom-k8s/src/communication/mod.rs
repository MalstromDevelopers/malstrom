mod backend;
mod discovery;
mod grpc_transport;
mod proto {
    include!(concat!(env!("OUT_DIR"), "/malstrom_k8s.rs"));
}

pub(crate) use backend::GrpcBackend;
