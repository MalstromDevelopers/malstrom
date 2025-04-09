set -e
kubectl config use-context kind-kind
helm uninstall malstrom-operator || true

CHART_PATH=./operator/helm/malstrom-operator
helm install malstrom-operator \
-f $CHART_PATH/local-values.yaml $CHART_PATH