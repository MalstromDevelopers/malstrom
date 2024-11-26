kind create cluster
kubectl config use-context kind-kind
kubectl cluster-info
CROSS_CONTAINER_ENGINE=podman cross build --target aarch64-unknown-linux-gnu &&\
cp ../target/aarch64-unknown-linux-gnu/debug/k8s-app ./k8s-app &&\
podman build . -t malstrom-k8s:example &&\
kind load docker-image localhost/malstrom-k8s:example &&\
kubectl apply -f ./manifests/sts.yaml