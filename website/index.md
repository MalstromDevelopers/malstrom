---
# https://vitepress.dev/reference/default-theme-home-page
layout: home

hero:
  name: "Malstrom"
  text: "Distributed Stream Processing"
  tagline: Stateful, Reliable, Efficient
  actions:
    - theme: brand
      text: Quickstart
      link: /guide/quickstart
    - theme: alt
      text: Learn about Stream Processing
      link: /guid/what-is-stream-processing.md
    - theme: alt
      text: API Documentation
      link: docs.rs/malstrom

features:
  - title: Distributed
    details: >
      Malstrom supports Kubernetes out-of-the-box, just deploy your job as a CRD and you are done!
    link: /kubernetes/index.md

  - title: Stateful
    details: > 
      Stateful operators enable you to implement complex logic easily, every serializable
      type can become state, no restrictions.

  - title: Reliable
    details: >
      Malstrom regularly checkpoints application state to local disk or a cloud storage like
      S3, GCS or Azure Blob.

  - title: Efficient
    details: >
      Malstrom can perform zero-downtime rescaling. Scale up compute clusters when demand rises,
      deallocate nodes when you do not need them, all without restarts or downtime.

  - title: Simple
    details: No JVM, no multi tenancy, no config files - just a single binary, compile and run!

  - title: Extensible
    details: >
      Want to store checkpoints on floppy disks? Need encrypted communication? You can!
      Malstrom exposes low level APIs so you can adapt it to your demands.

  - title: Rust API
    details: >
      No proprietary SQL dialect, no bindings: Malstrom offers a native Rust API, finally
      write data pipelines in everyones favourite programming language.

---
