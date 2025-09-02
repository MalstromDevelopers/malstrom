# Joining and Splitting Streams

We saw already that a stream has one input and one output, however this does not prevent us from consuming multiple datasources or writing to multiple sinks.

Let's extend the example from the beginning to take multiple inputs:

<<< @../../malstrom-core/examples/joining_streams.rs

If you run this example, you'll see we get each number twice. The `union` operator takes messages from two streams and fuses them into one. Note that this is different from a `zip` operation: `union` does not necessarily alternate between the left and right stream.

For multiple outputs we can split our stream just as easily:

<<< @../../malstrom-core/examples/cloned_streams.rs

Here the `const_cloned` operator will clone each message into a fixed number of output streams.
There is also the `cloned` operator, which allows determining the number of output streams at runtime.

If we want to select into which output stream a message goes, we can use the `const_split` and `split` operators:

<<< @../../malstrom-core/examples/split_streams.rs

The `split` and `const_split` operators take a function as a parameter which determines where messages are routed.
The function receives a mutable slice of booleans representing the split outputs.
By default all slice values are `false`. Setting a value to `true` will send the message to that output,
e.g. the slice `[true, false]` sends a message only to the first (left) output,
`[true, true]` to both and `[false, false]` to neither.
The message will be cloned as often as necessary.
