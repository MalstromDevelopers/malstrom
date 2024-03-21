# Timestamps and Epochs

Each data message in a JetStream dataflow comes with an attached timestamp.
Timstamps are our main way of reasoning about the progress of the computation.

Many built-in JetStream sources will already provide somewhat meaningful timestamps
when they emit records. For example the Kafka source keys the messages it emits by the
topic and partition they were read from, and provides the partition offset as a timestamp.

However the timestamp our source provides, may not always be the most useful for our computation.
In these cases we can trivially remap message timestamps using the `time_map` operator.

Let's look at one very common case, where we simply assign the current system time as a message
timestamp.

```rust
use std::SystemTime;
use jetstream::JetStreamBuilder;
use jetstream::kafka::KafkaInput;

let stream = (
    JetStreamBuilder::new()
    .source(KafkaInput::new(...))
    .time_map(|_msg| Systemtime::now())
)
```

This is very simple, but using system time is not suitable for all times of computation.
Let's look at a different case.
We have a Kafka topic which receives log messages from many different systems. These log messages
already contain timestamps (from the system generating them). We will therefore extract the timestamp
from the message itself.

```rust
use std::SystemTime;
use jetstream::JetStreamBuilder;
use jetstream::kafka::KafkaInput;
use chrono::{DateTime, Utc};

struct LogMessage {
    time: DateTime<Utc>,
    log: String
}

fn deserialize_log(msg: Vec<u8>) -> LogMessage {
    todo!()
}

let stream = (
    JetStreamBuilder::new()
    .source(KafkaInput::new(...))
    .map(deserialize_log)
    .time_map(|msg| msg.time.clone())
)
```

## Timestamp types

You may already have seen, that the two examples above use different types for the timestamp.
In fact, JetStream is extremely flexible about what you can use as a timestamp.
Any type is allowed, as long as it implements `PartialOrd`.

## Advancing Time

For some operations, like windowing, it is crucial to know, when a certain timestamp will not appear
anymore, i.e. when time has advanced past this timestamp.

If our inputs were strictly ordered, meaning timestamps came in a strictly ascending order,
this would be easy, as we would just need to observe the timestamps attached to our data.
Unfortunately, the real world often does not grant us this simplicity: In real world applications
data can be out of order by a few seconds, many hours or even days.

To be able to still reason about time and close windowing aggregations, JetStream allows you to
generate a special message type calledn an **Epoch**.

### Epochs

An Epoch is a special marker message, that acts like a promise. Essentially when you create the message

```rust
Epoch(1234)
```

and send it downstream, you are promising to all operators obeserving the Epoch, **that no data with a
timestamp less than 1234 will ever reach them again**.

As you see, issuing the epoch is not enough, we need also to ensure we are keeping this promise.
One very simple way of doing this, is to simply drop any messages with a lower timestamp.

Let's look at an operator, which will drop all messages whos timestamp is less than the highest
time it has seen so far.

```rust
use std::SystemTime;
use jetstream::JetStreamBuilder;

let stream = (
    JetStreamBuilder::new()
    .epochs_stateful(move |msg: Option<DataMessage>, state: &mut u64| {
        let msg = msg?;
        if msg.time > state {
            state = msg.time;
            (Some(Epoch(msg.time)), Some(msg))
        } else {
            // drops the message
            (None, None)
        }
    })
)
```

You can observe two things in this code:

1. The input type for `epochs` is an `Option<&DataMessage>`
2. The output type is an `Option<Epoch>`

The input type is an Option, as the epoch operator will also be called on very graph iteration, even when it
has no pending input. This allows you to advance time when there is no data, for example to handle idle sources.

The output type is an Option, as the `epochs` operator can, but does not have to issue a new Epoch when scheduled.

#### Handling late messages

A less brutal way than simply dropping late messages, would be to mark them with a special type:

```rust
use std::SystemTime;
use jetstream::{JetStreamBuilder, DataMessage};

struct LateMessage<Data> {
    original_time: u64,
    data: Data
}

enum MaybeLateMessage<Time, Data> {
    OnTime(DataMessage)
    Late(LateMessage)
}

let stream = (
    JetStreamBuilder::new()
    .epochs_stateful(move |msg: Option<DataMessage>, state: &mut u64| {
        let msg = msg?;
        if msg.time > state {
            state = msg.time;            
            let new_message = DataMessage::new(
                msg.key,
                msg.time,
                MaybeLateMessage::OnTime(msg)
            )
            (Some(Epoch(msg.time)), Some(new_message))
        } else {
            let late_message = DataMessage::new(
                msg.key,
                // we must change the time to keep our promise
                state,
                MaybeLateMessage::OnTime(LateMessage{
                    original_time: msg.time,
                    data: msg.data
                })
            )
            (None, Some(late_message))
        }
    })
)
```

# Epoch distribution

bla bla like AbsBarrier -> aligned broadcast

not keyed

## Redistributing the state of `epochs_stateful`

Just not???

<motion-canvas-player src="/animations/dist/animations/project.js" auto="true"></motion-canvas-player>


# Ways to construct timed stream

```rust
// create 5 seconds of out of orderness
// i.e the epoch lags 5 seconds behind the highest observed
// timestamp
stream
    .assign_timestamps(|msg| msg.value.time_field)
    .generate_epochs(|msg, prev_epoch|
        if (msg.timestamp > prev_epoch) && (x.timestamp.duration_since(prev_epoch) > Duration.from_secs(10)) {
            Some(prev_epoch + Duration.from_secs(5))
        } else {
            None
        }
    )
```

Or create epochs periodically

```rust
stream
    .generate_periodic_epochs(
        |prev_issue| {
            // prev_issue is an Option<(T, Duration)>, which
            // contains last issued epoch and duration since issuance

            // issue an epoch every 5 seconds, lagging 5 seconds behind
            if prev_issue.map(|(epoch, time_since)| time_since > Duration.from_secs(5)).unwrap_or(true) {
                Some(Instant::now() - Duration.from_secs(5))
            } else {
                None
            }
        }
    )

```

You can combine these with timestamp assignement....

```rust
stream
    .assign_timestamps(|msg| msg.value.time_field)
    .generate_periodic_epochs(
        |prev_issue| {
            if prev_issue.map(|(epoch, time_since)| time_since > Duration.from_secs(5)).unwrap_or(true) {
                Some(Instant::now() - Duration.from_secs(5))
            } else {
                None
            }
        }
    )
```

And with other epochs!
The code below limits out of boundedness to 5 seconds (event time) if we get data and
limits it to a maximum of 15 seconds (clock time) if we don't get data.

This means any window aggregation will have a worst case latency of aprox. 15 seconds

```rust
stream
    .assign_timestamps(|msg| msg.value.time_field)
    .generate_epochs(|msg, prev_epoch|
        if (msg.timestamp > prev_epoch) && (x.timestamp.duration_since(prev_epoch) > Duration.from_secs(10)) {
            Some(prev_epoch + Duration.from_secs(5))
        } else {
            None
        }
    )
    .generate_periodic_epochs(
        |prev_issue| {
            if prev_issue.map(|(epoch, time_since)| time_since > Duration.from_secs(15)).unwrap_or(true) {
                Some(Instant::now() - Duration.from_secs(15))
            } else {
                None
            }
        }
    )
```
