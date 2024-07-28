# Epochs

For some operations, like windowing, it is crucial to know, when a certain timestamp will not appear
anymore, i.e. when time has advanced past this timestamp.

If our inputs were strictly ordered, meaning timestamps came in an always ascending order,
this would be easy, as we would just need to observe the timestamps attached to our data.
Unfortunately, the real world often does not grant us this simplicity: In real world applications
data can be out of order by a few seconds, many hours sometimes even days or weeks.

To still be able to reason about time and close windowing aggregations, JetStream allows us to
generate a special message type called an **Epoch**.

An Epoch is a special marker message, which acts like a promise: It contains a specific timestamp
and any data messages following it, **must** have a timestamp greate than that of the epoch.

This in turn means, any operator that an Epoch of time _T_ reaches, can safely assume, that it will
never see anymore messages with a timestamp <= _T_

## Handling late messages

## Epochs on Unioned Streams

JetStream allows us to join multiple timestamped streams together, as long as they have the same timestamp
type.
This however raises the question, how Epochs should be handled at the junction.

Assume we are joining two streams, A and B, both of which produce data which is timestamped using a 64bit integer:

