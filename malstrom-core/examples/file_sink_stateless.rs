//! Example of a stateless sink writing to files on the local filesystem
use malstrom::{
    keyed::{partitioners::rendezvous_select, KeyDistribute},
    operators::{Map, Sink, Source},
    runtime::SingleThreadRuntime,
    sinks::{StatelessSink, StatelessSinkImpl},
    snapshot::NoPersistence,
    sources::{SingleIteratorSource, StatelessSource},
    types::DataMessage,
    worker::StreamProvider,
};
use std::{fs::OpenOptions, io::Write};
// #region sink_impl
/// Write records as lines to a file
struct FileSink {
    directory: String,
}

impl FileSink {
    pub fn new(directory: String) -> Self {
        Self { directory }
    }
}

impl<T> StatelessSinkImpl<String, String, T> for FileSink {
    fn sink(&mut self, msg: DataMessage<String, String, T>) {
        let file_path = format!("{}/{}.txt", self.directory, msg.key);
        // open file in append-mode, creating it if it does not exist
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .append(true)
            .open(file_path)
            .unwrap();
        file.write(msg.value.as_bytes()).unwrap();
        file.write(b"\n").unwrap();
    }
}
// #endregion sink_impl

fn build_dataflow(provider: &mut dyn StreamProvider) {
    std::fs::create_dir_all("/tmp/file-sink").unwrap();
    provider
        .new_stream()
        .source(
            "number",
            StatelessSource::new(SingleIteratorSource::new(0..5)),
        )
        .key_distribute(
            "key-by-value",
            |msg| msg.value.to_string(),
            rendezvous_select,
        )
        .map("int-to-string", |value| value.to_string())
        .sink(
            "file-sink",
            StatelessSink::new(FileSink::new("/tmp/file-sink".to_string())),
        );
}

fn main() {
    SingleThreadRuntime::builder()
        .persistence(NoPersistence)
        .build(build_dataflow)
        .execute()
        .unwrap();
}
