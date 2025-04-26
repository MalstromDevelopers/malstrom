# Implementing Custom Operators

For cases where the built-in stream operators do not suffice to solve your problem, Malstrom allows
you to build fully custom operators using the `StatefulLogic` and `StatelessLogic` traits.

These traits give you a lot of control over the processing in the stream, but they do this by
exposing things which Malstrom usually handles in the background, like [keying](./KeyedStreams.md)
and [event time](./TimelyProcessing.md).

Before implementing a custom operator, make sure you thorughly understand these concepts.
There are two important rules, you must not break in an operator. Breaking these rules WILL lead
to weird bugs and unpredictable behaviour.

1. Do not output messages with a different key than the input message[^1]
2. Do not output messages or epochs with a timestamp less than or equal to the current frontier.

Do not be discourage though, as long as you adhere to these rules, creating a custom operator is
not difficult.

## Implementing a Custom Stateless Operator

As an example we will re-implement the `.flatten` operator. Our implementation will take a `Vec`
of any inner type on the input side and output each element of the `Vec` as an individual
message.

<<< @../../malstrom-core/examples/custom_stateless_operator.rs#custom_impl

What does this do?

Firstly we declare a struct `CustomFlatten` so that we have a type to implement the `StatelessLogic`
trait on.

The implementation `StatelessLogic<K, Vec<V>, T, V> .. where K: MaybeKey, T: Timestamp, V: Data`
means we implement our operator for a stream which

- may or may not be keyed (`MaybeKey`)
- has timestamps (`Timestamp`, all streams in Malstrom have timestamps by default)
- has data (`Data`)

We also say our operator takes `Vec<V>` as input and gives `V` as output.

In the trait we implement the `on_data` method. This method gets called by Malstrom for every record
reaching the operator.
The method gets the operator output as a parameter and can send messages into this output.

For our implementation, we simply send all values in each `Vec` we get as individual messages,
always using the exact same key and timestamp we got from the input message.

That's it. Let's see how we can now use our custom operator.

<<< @../../malstrom-core/examples/custom_stateless_operator.rs#usage

### Full Code

You can find the full code below. This example is very close to how the built-in `.flatten` operator
is defined.

::: details Full code
<<< @../../malstrom-core/examples/file_sink_stateless.rs
:::

## Implementing a Custom Stateful Operator

Implementing operators which hold persistent state can be achieved in a very similar fashion.
For this example we will implement a batching operater, which groups messages of the same key
into a `Vec` of configurable size and emit these `Vec`s as messages.

Let's see the code:

<<< @../../malstrom-core/examples/custom_stateful_operator.rs#custom_impl

::: warning
Please do not copy this code. Read the next section regarding caveats.
:::

What does this do?

Firstly we declare a struct `CustomBatching(usize)`, where `usize` is the batch size. We
implement `StatefulLogic` for this type.

The implementation `StatefulLogic<K, V, T, Vec<V>, Vec<V>> .. where K: Key, T: Timestamp, V: Data`
means we implement our operator for a stream which

- is keyed (`Key`), this is not strictly necessary, but `stateful_op` is only implemented for
  keyed streams.
- has timestamps (`Timestamp`, all streams in Malstrom have timestamps by default)
- has data (`Data`)

We also say our operator takes `V` as input, gives `Vec<V>` as output and holds state of type
`Vec<V>`

Again, the method `on_data` gets called for every message reaching the operator. As a parameter
we get the state for the message's key (`key_state`). We get ownership of this state, meaning we
can mutate or drop it as we like.
We push the received message's value into the state `Vec` and if it is of the configured size,
we emit the batch into the output.

The method `on_data` is expected to return an `Option` of the state type. Returning `Some` will
retain the state and we will receive it again on the next invocation. Returning `None` discards the
state and on the next invocation we will get the state-type's default value (an empty `Vec` in
this case).

### Caveats

The implementation has an issue: If our stream is finite we may not emit the last window.

This can be solved by implementing the optional `on_epoch` method of the
`StatefulLogic` trait. This method will be called for every epoch reaching the operator.
After the last data message in a finite stream, Malstrom will automatically emit an epoch
of value `T::MAX` (`T` is the timestamp type).

Let's look at the implementation

<<< @../../malstrom-core/examples/custom_stateful_operator.rs#on_epoch

We simply check if the epoch received is the `MAX` value and if so, send all batches we still hold
into our output.

You could also use the `on_epoch` method to implement a maximum time per batch, but this is left
as an excercise to the reader ;)

Let's take a look at the usage of the operator we built:.

<<< @../../malstrom-core/examples/custom_stateful_operator.rs#usage

### Full Code

You can find the full code below.

::: details Full code
<<< @../../malstrom-core/examples/custom_stateful_operator.rs
:::

[^1]: To un-key a stream you can call `.key_local("name", |_| NoKey)`