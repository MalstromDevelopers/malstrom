//! Example of a stateless sink writing to files on the local filesystem
use malstrom::{
    keyed::{partitioners::rendezvous_select, KeyDistribute},
    operators::{Map, Sink, Source},
    runtime::SingleThreadRuntime,
    sinks::{StatefulSink, StatefulSinkImpl, StatefulSinkPartition},
    snapshot::NoPersistence,
    sources::{SingleIteratorSource, StatelessSource},
    types::DataMessage,
    worker::StreamProvider,
};
use std::{fs::OpenOptions, io::Write};
// #region sink_impl
/// Write records as lines to a file with line numbers
struct FileSink {
    directory: String
}

impl FileSink {
    pub fn new(directory: String) -> Self {
        Self { directory }
    }
}

impl<T> StatefulSinkImpl<String, String, T> for FileSink {
    type Part = String;
    type PartitionState = usize;
    type SinkPartition = FileSinkPartition;

    fn assign_part(&self, msg: &DataMessage<String, String, T>) -> Self::Part {
        format!("{}/{}.txt", self.directory, msg.key)
    }

    fn build_part(
        &mut self,
        part: &Self::Part,
        part_state: Option<Self::PartitionState>,
    ) -> Self::SinkPartition {
        FileSinkPartition::new(part.clone(), part_state)
    }
}
// #endregion sink_impl

// #region partition
struct FileSinkPartition {
    file: std::fs::File,
    next_line_no: usize
}
impl FileSinkPartition {
    fn new(file_path: String, next_line_no: Option<usize>) -> Self {
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .append(true)
            .open(file_path)
            .unwrap();
        Self { file, next_line_no: next_line_no.unwrap_or(0) }
    }
}

impl<T> StatefulSinkPartition<String, String, T> for FileSinkPartition {
    type PartitionState = usize;

    fn sink(&mut self, msg: DataMessage<String, String, T>) {
        self.file.write(format!("{} ", self.next_line_no).as_bytes()).unwrap();
        self.file.write(msg.value.as_bytes()).unwrap();
        self.file.write(b"\n").unwrap();
        self.next_line_no += 1;
    }

    fn snapshot(&self) -> Self::PartitionState {
        self.next_line_no
    }

    fn collect(self) -> Self::PartitionState {
        self.next_line_no
    }
}
// #endregion partition

fn build_dataflow(provider: &mut dyn StreamProvider) {
    std::fs::create_dir_all("/tmp/file-sink").unwrap();
    provider
        .new_stream()
        .source(
            "number",
            StatelessSource::new(SingleIteratorSource::new(0..50)),
        )
        .key_distribute(
            "key-by-mod",
            |msg| (msg.value % 5).to_string(),
            rendezvous_select,
        )
        .map("int-to-string", |value| value.to_string())
        .sink(
            "file-sink",
            StatefulSink::new(FileSink::new("/tmp/file-sink".to_string())),
        );
}

fn main() {
    SingleThreadRuntime::builder()
        .persistence(NoPersistence)
        .build(build_dataflow)
        .execute()
        .unwrap();
}
