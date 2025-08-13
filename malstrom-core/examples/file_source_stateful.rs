//! Example of a stateless source reading from files on the local filesystem
use core::iter::Enumerate;
use malstrom::{
    operators::Source,
    runtime::SingleThreadRuntime,
    snapshot::NoPersistence,
    sources::{StatefulSource, StatefulSourceImpl, StatefulSourcePartition},
    worker::StreamProvider,
};
use std::{
    fs::File,
    io::{BufRead as _, BufReader, Lines},
    iter::{Peekable, Skip},
};
// #region source_impl
/// Reads lines from files and emits them as records
struct FileSource {
    paths: Vec<String>, // file paths
}

impl FileSource {
    pub fn new(paths: Vec<String>) -> Self {
        Self { paths }
    }
}

/// Implement the source emitting String values and usize timestamps
impl StatefulSourceImpl<String, usize> for FileSource {
    // we will create one partition per path (String)
    type Part = String;
    type SourcePartition = FileSourcePartition;
    type PartitionState = usize;

    fn list_parts(&self) -> Vec<Self::Part> {
        self.paths.clone()
    }

    fn build_part(
        &mut self,
        part: &Self::Part,
        state: Option<Self::PartitionState>,
    ) -> Self::SourcePartition {
        FileSourcePartition::new(part.clone(), state.unwrap_or(0))
    }
}
// #endregion source_impl
// #region partition_impl
type FileLines = Peekable<Skip<Enumerate<Lines<BufReader<File>>>>>;
/// Reads lines from a single file
struct FileSourcePartition {
    path: String,
    file: Option<FileLines>,
    next_line: usize,
}
impl FileSourcePartition {
    fn new(path: String, next_line: usize) -> Self {
        Self {
            path,
            file: None,
            next_line,
        }
    }
}

impl StatefulSourcePartition<String, usize> for FileSourcePartition {
    type PartitionState = usize;
    fn poll(&mut self) -> Option<(String, usize)> {
        // open the file
        let file = self.file.get_or_insert_with(|| {
            BufReader::new(File::open(&self.path).unwrap())
                .lines()
                .enumerate()
                .skip(self.next_line)
                .peekable()
        });
        file.next().map(|(i, x)| {
            self.next_line += 1;
            (x.unwrap(), i)
        })
    }

    fn is_finished(&mut self) -> bool {
        match self.file.as_mut() {
            Some(x) => x.peek().is_none(),
            None => false, // not yet started
        }
    }

    fn snapshot(&self) -> Self::PartitionState {
        self.next_line
    }

    fn collect(self) -> Self::PartitionState {
        self.next_line
    }
}
// #endregion partition_impl
// #region usage
fn build_dataflow(provider: &mut dyn StreamProvider) {
    provider.new_stream().source(
        "files",
        StatefulSource::new(FileSource::new(vec![
            "/some/path.txt".to_string(),
            "/some/other/path.txt".to_string(),
        ])),
    );
}
// #endregion usage
fn main() {
    let _rt = SingleThreadRuntime::builder()
        .persistence(NoPersistence)
        .build(build_dataflow);
}
