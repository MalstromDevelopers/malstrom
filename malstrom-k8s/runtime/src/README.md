# Running a Malstrom cluster on Kubernetes!

This crate provides a Kubernetes runtime for the [Malstrom](malstrom.io) stream processing
framework.

You should run this in conjunction with the [Kubernetes](malstrom.io/kubernetes/operator), you can
theoretically deploy the runtime without it, but it will be more difficult.

## Configuration

The worker pods load a configuration TOML file located at `/malstrom/config/k8s_config` by default
however an alternative config can be provided by setting the `MALSTROM_K8S_CONFIG` to the path
of a custom TOML file.

To determine its own id, the worker will read the env var `MALSTROM_K8S_HOSTNAME` which must
contain the hostname in format `foobar-<id>`, this is the format kubernetes uses for pods in
a statefulset

## Env vars

```bash
MALSTROM_K8S_HOSTNAME="foobar-0"
MALSTROM_K8S_SCALE=42
# statefulset namespace
MALSTROM_K8S_STS_NS="bazbar"
```

### Configuration file

```toml
scale = 42 # current cluster scale i.e. worker count

[statefulset]
name = "foobar" # name of the statefulset backing this malstrom cluster
namespace = "bazbar" # name of the namespace containing the statefulset
```
