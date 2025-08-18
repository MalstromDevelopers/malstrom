# Timely Processing

When working with unbounded datastreams we will often want to perform some operation over a finite timeframe.
This could be an aggregation, for example we track vehicle movements and want to know the average
speed over intervals of five minutes, or it could be other operations:
Imagine we try to detect some events, the more datapoints we use the higher the certainty, but we
want to put an upper limit on the time needed for detection.

For both these use cases we need _time_. A naive approach would be to use the system clock, meaning
to get intervals of five minutes (these are called "windows") we would wait until five minutes had
elapsed in real world time. This works, it is usually called a "session window", but comes with a
very big issue: Using system time inherently ties the result of our operation to the throughput of
stream; if we process more messages per second, we end up with more messages per window. In normal
operation, this might not be an issue, but when replaying historic data or trying to catch up on a
datastream we have fallen behind on, this is a big problem. For one the increased window size can
present technical issues, like out-of-memory errors, and on the other hand, using a system clock
means relying on a side effect and therefore makes our computation non-deterministic.
Non-determinism opens the gates to all kinds of hard to troubleshoot and headache-inducing problems.
But don't despair, we can do better.

## Event Time

Event time is a crucial concept in stream processing, referring to the timestamp associated with
each data point that indicates when the event actually occurred. When working with streaming data
you will find that almost all data somehow has a time attached, either explicitly as a timestamp or
implicitly through some other attribute.

Malstrom is extremely flexible in what can be used as event time: You can use actual timestamps,
but really any type works as long as it supports ordering and has defined min and max values.

## Epochs

For some operations, like windowing, it is crucial to know, when a certain timestamp will not
appear anymore, i.e. when time has advanced past this timestamp.

If our inputs were strictly ordered, meaning timestamps came in an always ascending order,
this would be easy, as we would just need to observe the timestamps attached to our data.
Unfortunately, the real world often does not grant us this simplicity: In real world applications
data can be out of order by a few seconds, many hours sometimes even days or weeks.

To still be able to reason about time and close windowing aggregations, Malstrom allows you to
generate a special message type called an **Epoch**.
An Epoch is a special marker message, which acts like a promise: It contains a specific timestamp
and any data messages following it, **must** have a timestamp greater than that of the epoch.
This in turn means, any operator obeserving an Epoch of time _T_, can safely assume, it will
never see any more messages with a timestamp <= _T_

The animation below shows how epochs work to limit the out of orderness. We have a simple stream
consisting of a source, an operator assigning timestamps and the Epoch emitter, which emits epochs
and splits our stream into streams of on-time and late messages.
The epochs ensure, our on-time stream never sees a message more than 1 second out of order.

<video controls="controls" src="./animations\limit_out_of_orderness.mp4" />

# Code Example

Let's see how timestamps and epochs can be created on a datastream. For this example we will track
some made up financial transactions
We will then use the event time to get a monthly balance.
This is something you would usually do with a window, but we will use the `stateful_op` operator for
demonstration purposes here. For a more detailed explanation on `statful_op` see [CustomOperators](./CustomOperators#implementing-a-custom-stateful-operator).

::: tip
The method of generating Epochs in this example is very verbose, see the next example for a simpler
method.
:::

<<< @../../malstrom-core/examples/event_time.rs

::: info Output
```
{ key: (2025, 1), value: 830.0, timestamp: TransactionTime(2025-01-31) }
{ key: (2025, 2), value: -165.0, timestamp: TransactionTime(2025-02-28) }
{ key: (2025, 3), value: 145.0, timestamp: TransactionTime(2025-03-31) }
{ key: (2025, 4), value: -15.0, timestamp: TransactionTime(+262142-12-31) }
```
:::


Running the example above, you will see we get a printout of all monthly balances every time our
datastream advances one week.
But wait, something is weird. Every output is timestamped with the last transaction date of the
corresponding month, except for the last output, here the timestamp is `+262142-12-31` ... whaaat?!

This is a side effect of the semantic meaning behind epochs and how Malstrom indicates the end of a stream.
An epoch with time "X" means: _"There will be no messages with a time <= X after this epoch"_.
Malstrom makes use of this by utilizing the `MAX` value of a timestamp as an end stream marker, as
no more messages can possibly follow an epoch of max value.
You can read the timestamps in the output therefore as "this balance includes all transactions for
this key up to the timestamp.

## Out-of-orderness

The code example above assumes all data to be strictly ordered. In the real world however, this is
rarely the case, especially when dealing with multiple datasources.

Dealing with out-of-order records we fundamentally have to strategies:

1. Ignoring them entirely
2. Compromising on latency and waiting a limited amount of time for late records

Strategy 1 is what the example code is actually doing. If you look closely, the `generate_epochs`
operator returns two streams, one stream of on-time messages and one stream of late messages.
Late messages here are those where their timestamp is smaller or equal to the last emitted epoch.
In the code example we are simply ignoring the late stream.

Strategy 2 is usually more useful for any practical applications. Malstrom comes with a prebuilt utility for this:

<<< @../../malstrom-core/examples/event_time_out_of_order.rs{87}

::: info Output
```
{ key: (2025, 1), value: 830.0, timestamp: TransactionTime(2025-01-13) }
{ key: (2025, 2), value: -165.0, timestamp: TransactionTime(2025-02-02) }
{ key: (2025, 3), value: 145.0, timestamp: TransactionTime(+262142-12-31) }
{ key: (2025, 4), value: -15.0, timestamp: TransactionTime(+262142-12-31) }
```
The last two messages have the MAX timestamp because both of them were emitted due to the stream
ending.
:::

Using `limit_out_of_orderness` functions yields us two streams where in one the records are guaranteed
to be at most 62 days out of order from the previous recorct.
The other stream (the "late" stream) has unlimited out-of-orderness and has **no epochs** issued.
How you handle late messages depends on your applications needs.

::: tip
You can also limit out of orderness to `0` to enforce strict ordering.
:::

## Epochs on unioned streams

Malstrom allows us to join multiple timestamped streams together, as long as they have the same
timestamp type. To understand how epochs are handled on joined streams, we must introduce a new term: "Frontier".
An operator's frontier is the largest epoch timestamp he has received this far. Any operator can safely assume it will never see a record with a timestamp less than or equal to its frontier.

If one a stream union we were to just pass epochs as-is, we would risk increasing a downstream operator's frontier too far if one side of the join is further ahead in time. We would then not be allowed anymore to send the messages of the other join side downstream.
To avoid this, the `union` operator will "merge" epochs when it encounters them. The merge logic depends on the timestamp type, but usually means using the minimum frontier of all union input streams.

For example if we have a union of two streams an one stream sends `Epoch(10)` and the other stream sends `Epoch(125)` , the downstream operators will observe a message `Epoch(10)` .

## Performance considerations

Using timestamps is very close to free in terms of performance. Message size increases by the size of the timestamp however this is expected to be negligible for most applications.

Epochs travel the computation graph like any other messages, therefore issuing lots of epochs can have an adverse effect on performance in the same way a larger overall data volume would have.
It is generally advisable to place the `generate_epochs` operator close to the operation where epochs are needed.

Placing `generate_epochs` after a `key_distribute` rather than before it may also improve performance, since then epochs do not need to cross network boundaries.
