# Currently WIP. First public release soon (as of 2025-07-20)

# What is Malstrom?

Malstrom is a distributed, stateful stream processing framework written in Rust.
In usage it is similar to [Apache Flink](https://flink.apache.org/) or [bytewax](https://bytewax.io),
although implemented fundamentally differently. Malstrom's goal is to offer best-in-class usability,
reliability and performance, enabling everyone to build fast parallel systems with unparalleled up-time.

---
**Distributed**: Malstrom can run on many machines in parallel, sharing the processing workload and
enabling [zero-downtime scaling](https://malstrom.io/guide/Kubernetes.html#scaling-a-job) to fit any demand.
[Kubernetes](https://malstrom.io/guide/Kubernetes) is supported as a first-class deployment environment, others can be added through a public trait interface.

**Stateful**: Processing jobs can hold arbitrary state, which is snapshotted regularly to
[persistent storage](https://malstrom.io/guide/StatefulPrograms.html#persistent-state) like disk or S3. In case of failure or restarts,
the job resumes from the last snapshot.
Malstroms utilizes the [ABS Algorithm](https://arxiv.org/abs/1506.08603), ensuring every message affects the state **exactly once**.

**Usability**: Malstrom provides a straight-forward dataflow API, which can [be extended](https://malstrom.io/guide/CustomOperators) when needed.
A simple threading model means no async, no complex lifetimes, no `Send` or `Sync` needed.
Data only needs to be serialisable when explicitly send to other processes.

**Reliability**: Using the world's safest programming language makes building highly-reliable stream processors a breeze. In any case zero-downtime scaling and zero-downtime upgrades (TBD) allow for awesome uptime.

# Code Example

<<< @../../malstrom-core/examples/look_ma_im_streaming.rs

This outputs

```
{ key: NoKey, value: "LOOK", timestamp: 0 }
{ key: NoKey, value: "MA'", timestamp: 1 }
{ key: NoKey, value: "I'M", timestamp: 2 }
{ key: NoKey, value: "STREAMING", timestamp: 3 }
```