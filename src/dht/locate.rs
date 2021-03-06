use std::fmt::{Debug, Display};

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::channel::mpsc;
use futures::prelude::*;

use crate::common::*;
use crate::store::Datastore;
use crate::table::NodeTable;

use super::{Dht, OperationKind};

/// Future returned by locate operation
/// Resolves into node entry or error on completion
pub struct LocateFuture<Id, Info> {
    rx: mpsc::Receiver<Result<Entry<Id, Info>, Error>>,
}

#[cfg(nope)]
impl<Id, Info, ReqId: Clone> LocateFuture<Id, Info> {
    pub fn id(&self) -> ReqId {
        self.req_id.clone()
    }
}

impl<Id, Info> Future for LocateFuture<Id, Info> {
    type Output = Result<Entry<Id, Info>, Error>;

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
    /// Look up a node in the database by Id
    pub fn locate(&mut self, target: Id) -> Result<(LocateFuture<Id, Info>, ReqId), Error> {
        // Create an operation for the provided target
        let req_id = ReqId::generate();
        let (done_tx, done_rx) = mpsc::channel(1);

        // Register and start operation
        self.exec(req_id.clone(), target, OperationKind::FindNode(done_tx))?;

        // Return locate future to caller
        Ok((LocateFuture { rx: done_rx }, req_id))
    }
}

#[cfg(test)]
mod tests {
    use log::*;
    use simplelog::{Config as LogConfig, LevelFilter, SimpleLogger};

    use crate::store::HashMapStore;
    use crate::table::KNodeTable;
    use crate::{Config, Dht};

    use super::*;

    #[async_std::test]
    async fn test_lookup() {
        let _ = SimpleLogger::init(LevelFilter::Debug, LogConfig::default());

        let n1 = Entry::new([0b1000], 100);
        let n2 = Entry::new([0b0011], 200);
        let n3 = Entry::new([0b0010], 300);
        let n4 = Entry::new([0b1001], 400);
        let n5 = Entry::new([0b1010], 400);

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
        let mut dht: Dht<_, u32, u32, u16> = Dht::custom([0u8], config, tx, table, store);

        info!("Start locate");
        // Issue lookup
        let (lookup, req_id) = dht.locate(n4.id().clone()).expect("Error starting lookup");

        info!("Search round 0");
        // Start the first search pass
        dht.update().unwrap();

        // Check requests (query node 2, 3), find node 4
        assert_eq!(
            rx.try_next().unwrap(),
            Some((req_id, n2.clone(), Request::FindNode(n4.id().clone())))
        );
        assert_eq!(
            rx.try_next().unwrap(),
            Some((req_id, n3.clone(), Request::FindNode(n4.id().clone())))
        );

        // Handle responses (response from 2, 3), node 4, 5 known
        dht.handle_resp(
            req_id,
            &n3,
            &Response::NodesFound(n4.id().clone(), vec![n4.clone()]),
        )
        .unwrap();
        dht.handle_resp(
            req_id,
            &n2,
            &Response::NodesFound(n4.id().clone(), vec![n5.clone()]),
        )
        .unwrap();

        info!("Search round 1");

        // Update search state (re-start search)
        dht.update().unwrap();
        dht.update().unwrap();

        // Check requests (query node 4, 5)
        assert_eq!(
            rx.try_next().unwrap(),
            Some((req_id, n4.clone(), Request::FindNode(n4.id().clone())))
        );
        assert_eq!(
            rx.try_next().unwrap(),
            Some((req_id, n5.clone(), Request::FindNode(n4.id().clone())))
        );

        // Handle responses for node 4, 5
        dht.handle_resp(
            req_id,
            &n4,
            &Response::NodesFound(n4.id().clone(), vec![n4.clone()]),
        )
        .unwrap();
        dht.handle_resp(
            req_id,
            &n5,
            &Response::NodesFound(n4.id().clone(), vec![n5.clone()]),
        )
        .unwrap();

        // Launch next search
        dht.update().unwrap();
        // Detect completion
        dht.update().unwrap();
        dht.update().unwrap();

        info!("Expecting completion");
        assert_eq!(lookup.await, Ok(n4));
    }
}
