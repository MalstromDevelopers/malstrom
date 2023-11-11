use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::marker::PhantomData;
use std::path::Path;

use bincode::{config, Decode, Encode};
use serde::de::DeserializeOwned;
use serde::Serialize;

pub trait State {}
pub trait PersistentState: State + Encode + Decode {}

pub trait PersistentStateBackend<P: PersistentState> {
    fn load(&self) -> Option<P>;
    fn persist(&self, state: P);
}

struct FilesystemStateBackend<P>
where
    P: PersistentState,
{
    path: String,
    config: config::Configuration,
    pd: PhantomData<P>,
}

impl<P> FilesystemStateBackend<P>
where
    P: PersistentState,
{
    pub fn new(path: String, config: config::Configuration) -> FilesystemStateBackend<P> {
        FilesystemStateBackend {
            path,
            config,
            pd: PhantomData {},
        }
    }
}

impl<P> PersistentStateBackend<P> for FilesystemStateBackend<P>
where
    P: PersistentState,
{
    fn load(&self) -> Option<P> {
        let file_result = File::open(&self.path);
        match file_result {
            Ok(_) => (),
            Err(_) => return None,
        };
        let file = file_result.unwrap();
        let mut buf_reader = BufReader::new(file);
        let mut contents = Vec::new();
        match buf_reader.read_to_end(&mut contents) {
            Ok(_) => (),
            Err(_) => return None,
        };
        let decoded_result = bincode::decode_from_slice(&contents, self.config);
        match decoded_result {
            Ok(_) => (),
            Err(_) => return None,
        };
        let (decoded, _): (P, usize) = decoded_result.unwrap();
        Some(decoded)
    }

    fn persist(&self, state: P) {
        if let Ok(mut file) = File::create(&self.path) {
            if let Ok(bytes) = bincode::encode_to_vec(state, self.config) {
                let _ = file.write_all(&bytes);
            }
        }
    }
}

impl State for i32 {}
impl PersistentState for i32 {}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_persistent_backend() {
        let backend: FilesystemStateBackend<i32> =
            FilesystemStateBackend::new("/tmp/fsbackend".to_string(), config::standard());

        let value: i32 = 42;

        backend.persist(value);
        let deser_value: i32 = backend.load().expect("Could not load state");

        assert_eq!(value, deser_value);
    }

    #[test]
    fn test_persistent_backend_non_existent() {
        let backend: FilesystemStateBackend<i32> =
            FilesystemStateBackend::new("/tmp/fsbackend".to_string(), config::standard());

        let deser_value: Option<i32> = backend.load();

        assert_eq!(None, deser_value);
    }
}
