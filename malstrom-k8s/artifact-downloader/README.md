# Malstrom Artifact Downloader

This small application is supposed to be run as an init container in Kubernetes to fetch the
actual job binary (the "artifact").
This can be useful when you are developing, as it allows you to quickly swap out binaries.

For production deployments we recommend bundling binaries with the deployed image instead.