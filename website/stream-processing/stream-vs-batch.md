# Stream vs Batch Processing

When looking at any system which processes data, it will fall into one of two fundamental
categories:

- Batch Processing
- Stream Processing

## Batch Processing

Batch processing essentially means we do our work in two steps. First we collect a set of
datapoints, the "batch", and then we do our processing on this set.
This approach is very common. It can be found in OLAP databases as well as in data-analytics
libraries like [polars](https://pola.rs/), [pandas](https://pandas.pydata.org/) and
[Spark](https://spark.apache.org/).

The disadvantage of this approach is, that we only operate on the data batch once. If we want to
keep up with new or changing data, we need to run our processing job again. This makes it very
difficult to meet low latency requirements or achieve high responsiveness with a batch processing
architecture.

## Stream Processing

Stream processing or event stream processing is a programming model where data is
viewed as a time-dependent stream of information. The information is then transformed
with a sequence of operations that are applied to individual messages.

In stream processing we essentially skip the batching step and process datapoints immediately.
This results in much lower end-to-end latency.
