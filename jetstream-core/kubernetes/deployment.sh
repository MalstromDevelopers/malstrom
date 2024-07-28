#!/bin/bash
set -e

minikube start --namespace redpanda --nodes 4
# kubectl taint node \
#   -l node-role.kubernetes.io/control-plane="" \
#     node-role.kubernetes.io/control-plane=:NoSchedule

helm repo add redpanda https://charts.redpanda.com
helm repo add jetstack https://charts.jetstack.io
helm repo update
helm install cert-manager jetstack/cert-manager  --set installCRDs=true --namespace cert-manager  --create-namespace

helm install redpanda redpanda/redpanda \
  --namespace redpanda \
  --create-namespace \
  -f /home/damion/Desktop/jetstream/kubernetes/values-redpanda.yaml

kubectl --namespace redpanda rollout status statefulset redpanda --watch

alias rpk-topic="kubectl --namespace redpanda exec -i -t redpanda-0 -c redpanda -- rpk topic -X brokers=redpanda-0.redpanda.redpanda.svc.cluster.local.:9093,redpanda-1.redpanda.redpanda.svc.cluster.local.:9093,redpanda-2.redpanda.redpanda.svc.cluster.local.:9093 -X tls.ca=/etc/tls/certs/default/ca.crt -X tls.enabled=true"

rpk-topic create jetstream_example

kubectl -n redpanda port-forward svc/redpanda-console 8080:8080