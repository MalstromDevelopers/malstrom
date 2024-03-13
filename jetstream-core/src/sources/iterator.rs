use crate::{operators::source::{DataCollector, DataSource, DataSourceBuilder, SourceMessage, SystemMessage}, stream::operator::{BuildContext, OperatorContext}, time::NoTime, Data, NoKey};

impl<T: Iterator<Item = V> + 'static, V, P> DataSourceBuilder<P> for T where V: Data{
    type Source = IteratorDataSource<V>;
    fn build(self, _ctx: &BuildContext<P>) -> IteratorDataSource<V> {
        IteratorDataSource{inner: Box::new(self), last_given_index: None}
    }
    
}

/// Naive stateless source which simply yields elements from an iterator
pub struct IteratorDataSource<V> {
    inner: Box<dyn Iterator<Item = V>>,
    last_given_index: Option<usize>
}

impl<V, P> DataSource<NoKey, V, usize, P> for IteratorDataSource<V> where V: Data{

    fn give(&mut self, _ctx: &OperatorContext, collector: &mut DataCollector<NoKey, V, usize, P>) -> () {
        if let Some(value) = self.inner.next() {
            let timestamp = self.last_given_index.get_or_insert(0).to_owned();

            collector.send_data(NoKey, value, timestamp);
            collector.send_epoch(timestamp);
            
            self.last_given_index.map(|x| x + 1);
        }
    }

    fn handle(&mut self, _sys_message: &mut SystemMessage<P>) -> () {
        ()
    }
}
