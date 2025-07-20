# Deploying to Kubernetes

Kubernetes is a first class deployment target for Malstrom jobs.
Deploying to Kubernetes is best done using the Malstrom Kubernetes operator.

## Installing the Operator

The operator can be installed via [helm](https://helm.sh).

First add the Malstrom Helm repository:

`helm repo add malstrom https://raw.githubusercontent.com/MalstromDevelopers/helm-charts/gh-pages`

Fetch the repository's helm charts:

`helm repo update malstrom`

Install the operator (see below for supported config values):

`helm install malstrom-operator malstrom/malstrom-operator -f values.yaml`

## Using the Kubernetes Job Runtime

The runtime for executing Malstrom jobs on Kubernetes is supplied via the
[malstrom-k8s](https://docs.rs/malstrom-k8s/) crate. You can use the runtime just like you would
for example use the `MultiThreadRuntime` when running locally. In fact, we recommend letting your
program utilize both, depending on its environment. This makes development simpler, as you can
easily execute the job on your own machine.

See how this example program uses the `MultiThreadRuntime` when run without arguments, but will
use the `KubernetesRuntime` when invoked with the argument `k8s`:

<<< @../../malstrom-k8s/runtime/examples/basic_program.rs

## Deploying an Example job

For production use cases we recommend building an image which includes your jobs compiled binary.
For testing you can also enable the operator's "artifact-manager" which allows you to upload jobs
to the operator directly, which is what we will do for the sake of this example.

By default the "artifact-manager" is disabled. It can be enabled via the `values.yaml` when
installing with helm:

```yaml
artifactManager:
    deploy: true
```

Next we will compile our program: `cargo build --release`
By default the compiled binary is placed in the `target` folder under `target/release/<app-name>`.

Next we will need a connection to the "artifact-manager" running on Kubernetes. We will use
[kubectl](https://kubernetes.io/docs/reference/kubectl/) to port-forward from the Kubernetes cluster
to a port on out local machine. This command makes the "artifact-manager" available under
`localhost:29918`:

`kubectl port-forward artifact-manager 29918:8000`

Next we upload our compiled binary, making it available under the name "foobar":

```bash
curl -F \
"file=@./target/release/<app-name>" \
"localhost:29918/foobar"
```

Note that you might have to cross-compile to the architecture your clusters nodes are running on.

To actully run the job we will create a Kubernetes resource of the type `MalstromJob`:

```yaml
apiVersion: malstrom.io/v1alpha
kind: MalstromJob
metadata:
  name: test-job
spec:
  replicas: 2
  jobState: Running
  binary:
    source: http://artifact-manager.default.svc.cluster.local/foobar
    destination: /artifact/executable
  podSpecTemplate:
    containers:
      - name: main
        image: alpine:3.12
        env:
          - name: IS_K8S
            value: "true"
    initContainers:
      - name: artifact-downloader
        image: ghcr.io/malstrom/artifact-downloader:latest
```

When applying this manifest with `kubectl apply -f path/to/manifest.yaml` we create and start
a Malstrom job with a parallelism of `2`. You should now see the running pods.

## Scaling a job

A job can be rescaled easily, simply change `spec.replicas` in the `Malstromjob` definition to any
number greater than 0. The operator will then automatically take care of rescaling the job.

## Operator values

> [!WARNING]
> Currently the operator only supports modifying the job replica count. Any other modification
> requires manually deleting and re-creating the job. See [issue 25](https://github.com/MalstromDevelopers/malstrom/issues/25)

The operator helm chart supports these configuration values:

<<< @../../malstrom-k8s/operator/helm/malstrom-operator/values.yaml