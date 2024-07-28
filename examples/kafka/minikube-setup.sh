minikube start --driver=docker --cpus=2 --memory=3g --nodes=4 -p jetstream

helm repo add jetstack https://charts.jetstack.io
helm repo update
helm install cert-manager jetstack/cert-manager  --set installCRDs=true --namespace cert-manager  --create-namespace --version v1.13.0

helm repo add redpanda https://charts.redpanda.com
helm repo update
helm upgrade --install redpanda redpanda/redpanda --values cluster/values-redpanda.yaml --version 4.0.41

kubectl port-forward service/redpanda-console 8080:8080 &

kubectl port-forward pod/redpanda-0 9093:9093 &
