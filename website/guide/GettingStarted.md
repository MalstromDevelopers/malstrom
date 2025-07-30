# Getting Started

Malstrom can run distributed on many machines in parallel, but getting started locally is just as simple.
Malstom's API language is [Rust](https://en.wikipedia.org/wiki/Rust_(programming_language)),
if you are not familiar with Rust, don't worry, check the Rust language's ["Getting started" guide](https://www.rust-lang.org/learn/get-started),
you don't need to be an expert to write programs with Malstrom.

## Installing Malstrom

Create a new Rust app and Install Malstrom using cargo: `cargo init --bin && cargo install malstrom`

The `malstrom` crate contains the framework core and essentials. We will look at other crates which add more features later.

## Your First Program

In our `main.rs` file we need to create two functions:
- A function to build the dataflow graph
- A function to execute the graph

Lets look at the code and then go through it step-by-step

<<< @../../malstrom-core/examples/basic_noop.rs

Feel free to copy and run this snippet, it does absolutely nothing! Let's go through it:

`SingleThreadRuntime::builder`: This creates a runtime for Malstrom. A "runtime" tells us where the computation will be happening.
In this case, all operations will happen in a single thread, but we will learn more about runtimes shortly.

`.persistence(NoPersistence)`: Means we do not want to persist any snapshots. You can learn more about
persistent state [here](./StatefulPrograms.html#persistent-state)

`fn build_dataflow(provider: &mut dyn StreamProvider)`: This is the function which builds our execution graph.
It takes a special reference to the runtime as an input, we call this a "provider".
We then use this provider to create datastreams.

`provider.new_stream()` : Here we create a new data stream and then... do nothing with it. Let's fix that.

## Adding Operators

A core concept of Malstrom are operators. Operators are the nodes of our execution graph and in the most general sense they do ✨something✨ with our data. Malstrom comes with many pre-built operators (though you can create your [own](CustomOperators)). Let's import them and use the `.source` operator to add some data to our program.

<<< @../../malstrom-core/examples/basic_operators.rs

If you now run this code you'll see every second number from 0 to 200 printed to the console. Let's look at what we did:

`.source(name, source)`: The `source` operator adds data into a stream. It can only be applied to streams which do not yet have a source, see [Joining and Splitting Streams](./JoiningSplitting.md) for how to use multiple sources. The source operator takes a "source" as an input. In this case we used a stateless source which emits items taken from an iterator.

`.map(name, value)`: The `map` operator applies a simple transformation to every value.

`.inspect(name, value, context)`: This operators allows us to observe values without manipulating them. This is ideal for debugging or exporting metrics. We will learn about the `context` argument shortly.

As you can see, all operators must have a **unique** name. Choosing a good name is important; It will greatly help you with debugging and tracing and is essential for [Persistent State](./StatefulPrograms.html#persistent-state)

## More Power

What if we want to do something more complex than doubling numbers? We will need more power! Luckily going from a single to multiple threads ([or machines](Kubernetes)) is super easy:

<<< @../../malstrom-core/examples/multithreading.rs

You will again see all numbers printed, along with the ID of the thread where they where processed (a number between 0 and 3).
Let's reflect on the changes we made:

`MultiThreadRuntime:::builder().parrallelism(4)`: We swapped the `SingleThreadRuntime` for a `MultiThreadRuntime` and created it with 4 threads.


`.key_distribute("key-by-value", |x| x.value, rendezvous_select)`: This operator distributes our data across multiple workers (i.e. threads).
If you want to know more about how this works, check the [Keyed Streams](./KeyedStreams.md) documentation.
