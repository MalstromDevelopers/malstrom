//! A very basic program which can run locally or on Kubernetes depending on
use malstrom::operators::*;
use malstrom::runtime::MultiThreadRuntime;
use malstrom::snapshot::NoPersistence;
use malstrom::sources::{SingleIteratorSource, StatelessSource};
use malstrom::worker::StreamProvider;
use malstrom_k8s::KubernetesRuntime;

fn main() {
    let run_k8s = std::env::var("IS_K8S") == Ok("true".to_string());
    if run_k8s {
        KubernetesRuntime::builder()
            .persistence(NoPersistence)
            .build(build_dataflow)
            .execute_auto()
            .unwrap()
    } else {
        MultiThreadRuntime::builder()
            .persistence(NoPersistence)
            .parrallelism(2)
            .build(build_dataflow)
            .execute()
            .unwrap()
    }
}

fn build_dataflow(provider: &mut dyn StreamProvider) -> () {
    provider
        .new_stream()
        .source(
            "iter-source",
            StatelessSource::new(SingleIteratorSource::new(0..=100)),
        )
        .map("double", |x| x * 2)
        .inspect("print", |x, _| println!("{}", x.value));
}
