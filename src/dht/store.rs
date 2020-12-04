
use std::fmt::{Debug, Display};

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::prelude::*;
use futures::channel::mpsc;

use crate::common::*;
use crate::store::Datastore;
use crate::table::NodeTable;

use super::{Dht, OperationKind};

/// Future returned by locate operation
/// Resolves into node entry or error on completion
pub struct StoreFuture<Id, Info> {
    rx: mpsc::Receiver<Result<Vec<Entry<Id, Info>>, Error>>,
}

impl <Id, Info> Future for StoreFuture<Id, Info> {
    type Output = Result<Vec<Entry<Id, Info>>, Error>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.rx.poll_next_unpin(ctx) {
            Poll::Ready(Some(r)) => Poll::Ready(r),
            _ => Poll::Pending,
        }
    }
}


impl<Id, Info, Data, ReqId, Table, Store> Dht<Id, Info, Data, ReqId, Table, Store>
where
    Id: DatabaseId + Clone + Sized + Send + 'static,
    Info: PartialEq + Clone + Sized + Debug + Send + 'static,
    Data: PartialEq + Clone + Sized + Debug + Send + 'static,
    ReqId: RequestId + Clone + Sized + Display + Debug + Send + 'static,
    Table: NodeTable<Id, Info> + Send + 'static,
    Store: Datastore<Id, Data> + Send + 'static,
{
    /// Store data in the DHT by ID
    pub fn store(&mut self, target: Id, data: Vec<Data>) -> Result<(StoreFuture<Id, Info>, ReqId), Error> {

        // Create an operation for the provided target
        let req_id = ReqId::generate();
        let (done_tx, done_rx) = mpsc::channel(1);

        // Register and start operation
        self.exec(req_id.clone(), target, OperationKind::Store(data, done_tx))?;

        // Return locate future to caller
        Ok((StoreFuture{
            rx: done_rx,
        }, req_id))
    }
}


#[cfg(test)]
mod tests {
    use log::*;
    use simplelog::{SimpleLogger, LevelFilter, Config as LogConfig};

    use crate::{Dht, Config};
    use crate::store::HashMapStore;
    use crate::table::KNodeTable;

    use super::*;

    #[async_std::test]
    async fn test_store() {
        let _ = SimpleLogger::init(LevelFilter::Debug, LogConfig::default());

        // Setup nodes
        let n1 = Entry::new([0b1000], 100);
        let n2 = Entry::new([0b0011], 200);
        let n3 = Entry::new([0b0010], 300);
        let n4 = Entry::new([0b1001], 400);
        let n5 = Entry::new([0b1010], 400);

        let (value_id, value_data) = ([0b1100], 500);

        // Create configuration
        let mut config = Config::default();
        config.concurrency = 2;
        config.k = 2;

        // Inject initial nodes into the table
        let store = HashMapStore::new();
        let mut table = KNodeTable::new(n1.id().clone(), 2, 4);
        table.create_or_update(&n2);
        table.create_or_update(&n3);

        // Instantiated DHT
        let (tx, mut rx) = mpsc::channel(10);
        let mut dht: Dht<_, u32, u32, u16> = Dht::custom(n1.id().clone(), config, tx, table, store);
        
        info!("Start store");
        // Issue lookup
        let (store, req_id) = dht.store(value_id, vec![value_data]).expect("Error starting lookup");

        info!("Search round 0");
        // Start the first search pass
        dht.update().await.unwrap();

        // Check requests (query node 2, 3), find node 4
        assert_eq!(rx.try_next().unwrap() , Some((req_id, n3.clone(), Request::FindNode(value_id.clone()))));
        assert_eq!(rx.try_next().unwrap() , Some((req_id, n2.clone(), Request::FindNode(value_id.clone()))));

        // Handle responses (response from 2, 3), node 4, 5 known
        dht.handle_resp(req_id, &n3, &Response::NodesFound(value_id.clone(), vec![n4.clone()])).await.unwrap();
        dht.handle_resp(req_id, &n2, &Response::NodesFound(value_id.clone(), vec![n5.clone()])).await.unwrap();

        info!("Search round 1");

        // Update search state (re-start search)
        dht.update().await.unwrap();
        dht.update().await.unwrap();

        // Check requests (query node 4, 5)
        assert_eq!(rx.try_next().unwrap() , Some((req_id, n4.clone(), Request::FindNode(value_id.clone()))));
        assert_eq!(rx.try_next().unwrap() , Some((req_id, n5.clone(), Request::FindNode(value_id.clone()))));

        // Handle responses for node 4, 5
        dht.handle_resp(req_id, &n4, &Response::NodesFound(value_id.clone(), vec![])).await.unwrap();
        dht.handle_resp(req_id, &n5, &Response::NodesFound(value_id.clone(), vec![])).await.unwrap();

        info!("Store round");

        // Launch store
        dht.update().await.unwrap();
        // Detect completion
        dht.update().await.unwrap();

        // Check requests (store node 4, 5)
        assert_eq!(rx.try_next().unwrap() , Some((req_id, n4.clone(), Request::Store(value_id.clone(), vec![value_data]))));
        assert_eq!(rx.try_next().unwrap() , Some((req_id, n5.clone(), Request::Store(value_id.clone(), vec![value_data]))));

        // Handle responses for node 4, 5
        dht.handle_resp(req_id, &n4, &Response::ValuesFound(value_id.clone(), vec![value_data])).await.unwrap();
        dht.handle_resp(req_id, &n5, &Response::ValuesFound(value_id.clone(), vec![value_data])).await.unwrap();

        // Finalise operation
        dht.update().await.unwrap();
        dht.update().await.unwrap();

        info!("Expecting store completion");
        assert_eq!(store.await, Ok(vec![n4.clone(), n5.clone()]));
    }
}
