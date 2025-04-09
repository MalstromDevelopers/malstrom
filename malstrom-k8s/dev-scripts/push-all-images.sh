set -e

services=("k8s-operator" "artifact-manager" "artifact-downloader")
for service in "${services[@]}"; do
  docker build . -f $service.dockerfile -t localhost:5001/malstromdevelopers/$service:latest
  docker push localhost:5001/malstromdevelopers/$service:latest
done