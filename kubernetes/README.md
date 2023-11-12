# Redpanda Setup

## Prerequisites
- Make sure that you have setup kubectl and helm with the configuration of your target kubernetes cluster.

## Minikube - for local development
- `minikube start --driver=docker --cpus=2 --memory=3g --nodes=4`

## Redpanda
```bash
1. helm repo add redpanda https://charts.redpanda.com
2. helm repo update
3. helm upgrade --install redpanda redpanda/redpanda --values cluster/values-redpanda.yaml --version 4.0.41
4. export DOMAIN=customredpandadomain.local
5. Wait until all the pods are in a running state!
6. sudo true && kubectl -n redpanda get endpoints,node -A -o go-template='{{ range $_ := .items }}{{ if and (eq .kind "Endpoints") (eq .metadata.name "redpanda-external") }}{{ range $_ := (index .subsets 0).addresses }}{{ $nodeName := .nodeName }}{{ $podName := .targetRef.name }}{{ range $node := $.items }}{{ if and (eq .kind "Node") (eq .metadata.name $nodeName) }}{{ range $_ := .status.addresses }}{{ if eq .type "ExternalIP" }}{{ .address }} {{ $podName }}.${DOMAIN}{{ "\n" }}{{ end }}{{ end }}{{ end }}{{ end }}{{ end }}{{ end }}{{ end }}' | envsubst | sudo tee -a /etc/hosts

```

If you are using minikube, in the 6. command you need to exchange `ExternalIP` with `InternalIP` to make the host names work.

## Connect to cluster

To connect to the cluster you must specify the following URL as the broker: `redpanda-<idx>.customredpandadomain.local:31092`, Should you connect to the cluster from within, you must use port 9094.