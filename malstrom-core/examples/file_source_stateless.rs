//! Example of a stateless source reading from files on the local filesystem
use core::iter::Enumerate;
use malstrom::{
    operators::Source,
    runtime::SingleThreadRuntime,
    snapshot::NoPersistence,
    sources::{StatelessSource, StatelessSourceImpl, StatelessSourcePartition},
    worker::StreamProvider,
};
use std::{
    fs::File,
    io::{BufRead as _, BufReader, Lines},
    iter::Peekable,
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
impl StatelessSourceImpl<String, usize> for FileSource {
    // we will create one partition per path (String)
    type Part = String;
    type SourcePartition = FileSourcePartition;

    fn list_parts(&self) -> Vec<Self::Part> {
        self.paths.clone()
    }

    fn build_part(&mut self, part: &Self::Part) -> Self::SourcePartition {
        FileSourcePartition::new(part.clone())
    }
}
// #endregion source_impl
// #region partition_impl
type FileLines = Peekable<Enumerate<Lines<BufReader<File>>>>;
/// Reads lines from a single file
struct FileSourcePartition {
    path: String,
    file: Option<FileLines>,
}
impl FileSourcePartition {
    fn new(path: String) -> Self {
        Self { path, file: None }
    }
}

impl StatelessSourcePartition<String, usize> for FileSourcePartition {
    fn poll(&mut self) -> Option<(String, usize)> {
        // open the file
        let file = self.file.get_or_insert_with(|| {
            BufReader::new(File::open(&self.path).unwrap())
                .lines()
                .enumerate()
                .peekable()
        });
        file.next().map(|(i, x)| (x.unwrap(), i))
    }

    fn is_finished(&mut self) -> bool {
        match self.file.as_mut() {
            Some(x) => x.peek().is_none(),
            None => false, // not yet started
        }
    }
}
// #endregion partition_impl
// #region usage
fn build_dataflow(provider: &mut dyn StreamProvider) {
    provider.new_stream().source(
        "files",
        StatelessSource::new(FileSource::new(vec![
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
