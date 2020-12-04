

use std::fmt::{Debug, Display};

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::prelude::*;
use futures::channel::mpsc;

use crate::common::*;
use crate::store::Datastore;
use crate::table::NodeTable;

use super::{Dht, OperationKind, OperationState, Operation, RequestState};

/// Future returned by connect operation
/// Resolves into a number of located peers on success
pub struct ConnectFuture<Id, Info> {
    rx: mpsc::Receiver<Result<Vec<Entry<Id, Info>>, Error>>,
}
impl <Id, Info> Future for ConnectFuture<Id, Info> {
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
    /// Connect to the database
    pub fn connect(&mut self, peers: &[Entry<Id, Info>]) -> Result<(ConnectFuture<Id, Info>, ReqId), Error> {

        // Create an operation for the provided target
        let req_id = ReqId::generate();
        let (done_tx, done_rx) = mpsc::channel(1);

        // Create operation
        let mut op = Operation::new(req_id.clone(), self.id.clone(), OperationKind::Connect(done_tx));

        // Seed operation with known peers
        // This is a hack to avoid needing separate logic for the connect operation
        for e in peers {
            op.nodes.insert(e.id().clone(), (e.clone(), RequestState::Active));
        }
        
        // Register operation for response / update handling
        self.operations.insert(req_id.clone(), op);

        // Return connect future to caller
        Ok((ConnectFuture{
            rx: done_rx,
        }, req_id))
    }

    /// Create a connection request _without_ storing nodes or sending messages
    ///
    /// This is useful for integrating with external interfaces that _may not_
    /// be aware of the node ID at connect time. The caller is responsible for issuing
    /// the appropriate request to viable peers, responses will be handled automatically.
    pub fn connect_start(&mut self) -> Result<(ConnectFuture<Id, Info>, ReqId,  Request<Id, Data>), Error> {
        let req_id = ReqId::generate();
        let (done_tx, done_rx) = mpsc::channel(1);

        // Create operation
        let mut op = Operation::new(req_id.clone(), self.id.clone(), OperationKind::Connect(done_tx));

        // Create request
        let req = Request::FindNode(self.id.clone());

        // Skip init state
        op.state = OperationState::Searching(0);

        // Register operation for response / update handling
        self.operations.insert(req_id.clone(), op);

        // Return connect future and request for caller use
        Ok((ConnectFuture{
            rx: done_rx,
        }, req_id, req))
    }
}


#[cfg(test)]
mod tests {
    use log::*;
    use simplelog::{SimpleLogger, LevelFilter, Config as LogConfig};

    use crate::{Dht, Config};

    use super::*;

    #[async_std::test]
    async fn test_connect() {
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

        // Instantiated DHT
        let (tx, mut rx) = mpsc::channel(10);
        let mut dht: Dht<_, u32, u32, u16> = Dht::standard(n1.id().clone(), config, tx);
        
        info!("Start connect");
        // Issue lookup
        let (connect, req_id) = dht.connect(&[n2.clone(), n3.clone()]).expect("Error starting lookup");

        info!("Search round 0");
        // Start the first search pass
        dht.update().await.unwrap();

        // Check requests (query node 2, 3), find node 4
        assert_eq!(rx.try_next().unwrap() , Some((req_id, n3.clone(), Request::FindNode(n1.id().clone()))));
        assert_eq!(rx.try_next().unwrap() , Some((req_id, n2.clone(), Request::FindNode(n1.id().clone()))));

        // Handle responses (response from 2, 3), node 4, 5 known
        dht.handle_resp(req_id, &n2, &Response::NodesFound(n1.id().clone(), vec![n4.clone()])).await.unwrap();
        dht.handle_resp(req_id, &n3, &Response::NodesFound(n1.id().clone(), vec![n5.clone()])).await.unwrap();

        info!("Search round 1");

        // Update search state (re-start search)
        dht.update().await.unwrap();
        dht.update().await.unwrap();

        // Check requests (query node 4, 5)
        assert_eq!(rx.try_next().unwrap() , Some((req_id, n4.clone(), Request::FindNode(n1.id().clone()))));
        assert_eq!(rx.try_next().unwrap() , Some((req_id, n5.clone(), Request::FindNode(n1.id().clone()))));

        // Handle responses for node 4, 5
        dht.handle_resp(req_id, &n4, &Response::NodesFound(n1.id().clone(), vec![n4.clone()])).await.unwrap();
        dht.handle_resp(req_id, &n5, &Response::NodesFound(n1.id().clone(), vec![n5.clone()])).await.unwrap();

        // Launch next search
        dht.update().await.unwrap();
        // Detect completion
        dht.update().await.unwrap();
        dht.update().await.unwrap();

        info!("Expecting completion");
        assert_eq!(connect.await, Ok(vec![n2, n3, n4, n5]));
    }
}
