# Setup

Install [Minikube](https://minikube.sigs.k8s.io/docs/start/)

Set Minikube resources
`minikube config set cpus 4`
`minikube config set memory 8192`

Start minikube: `minikube start --namespace redpanda --nodes 10`

Install RedPanda on minikube: https://docs.redpanda.com/current/deploy/deployment-option/self-hosted/kubernetes/local-guide/?tab=tabs-1-minikube
NOTE: Use version 5.7.0, the helm chart for 5.8.6 does not work