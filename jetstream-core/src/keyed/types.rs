//! Types and traits specific to keyed streams


// /// Marker trait for functions which determine inter-worker routing
// pub trait WorkerPartitioner<K>:
//     for<'a> fn(&K, &'a IndexSet<WorkerId>) -> &'a WorkerId + 'static
// {
// }
// impl<K, U: for<'a> Fn(&K, &'a IndexSet<WorkerId>) -> &'a WorkerId + 'static> WorkerPartitioner<K>
//     for U
// {
// }
