use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use jetstream::persistent::{PersistentStateBackend, persistent_source, PersistentState};
use timely::Data;
use timely::dataflow::operators::{Inspect, ToStream};

fn main() {
    
    timely::execute_from_args(std::env::args(), move |worker| {
        worker.dataflow::<usize, _, _>(|scope| {
            let state_backend = DiskState{dir: PathBuf::from("/tmp/jetstream/state"), state_type: PhantomData::<i64>};
            
            let logic = |state| if state < 20 {
                let msg = state + 1;
                println!("emitting {msg}");
                (msg, state + 1)
            } else {
                std::process::exit(0);
            };
            persistent_source(scope, state_backend, logic).inspect(|x| println!("Saw {x:?}"));
        });
        println!("Built dataflow");
        for i in 0..10 {
            worker.step();
        }
    })
    .unwrap();
}

struct DiskState<P> {
    dir: PathBuf,
    state_type: PhantomData<P>
}

impl <T, P>PersistentStateBackend<T, P> for DiskState<P> where
P: PersistentState + Default,
{
    type LoadError = std::io::Error;
    type SaveError = std::io::Error;

    fn load_state_latest(&self, worker_index: usize) -> Result<P, Self::LoadError> {
        let mut path = self.dir.clone();
        path.push(format!("worker_{worker_index}.bin"));
        
        if !path.exists() {
            return Ok(P::default());
        };
        println!("Reading persisted state from disk");
        let fileinp = std::fs::read(path)?;
        let state: P = bincode::deserialize(&fileinp).unwrap();
        Ok(state)
    }

    fn save_state(&self, worker_index: usize, state: &P) -> Result<(), Self::SaveError> {
        if !self.dir.exists() {
            std::fs::create_dir_all(self.dir.clone()).expect("Error creating state directory");
        };
        let bytes = bincode::serialize(state).unwrap();
        let mut path = self.dir.clone();
        path.push(format!("worker_{worker_index}.bin"));
        std::fs::write(path, bytes).unwrap();
        Ok(())
    }
}