#!/bin/bash

set -e
# minikube addons enable ingress

kubectl port-forward -n redpanda service/redpanda-console 1887:8080 &
kubectl port-forward -n redpanda service/redpanda-external 8083:8083 &

kubectl port-forward -n redpanda service/redpanda-external 9094:9093 &
kubectl port-forward -n redpanda service/redpanda-external 9094:9094 &

echo "Press CTRL-C to stop port forwarding and exit the script"
wait