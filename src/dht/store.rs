
use std::fmt::Debug;

use crate::common::*;
use crate::store::Datastore;
use crate::table::NodeTable;

impl<Id, Info, Data, ReqId, Table, Store>
Dht<Id, Info, Data, ReqId, Table, Store>
where
Id: DatabaseId + Clone + Sized + Send + 'static,
Info: PartialEq + Clone + Sized + Debug + Send + 'static,
Data: PartialEq + Clone + Sized + Debug + Send + 'static,
ReqId: RequestId + Clone + Sized + Debug + Send + 'static,
Table: NodeTable<Id, Info> + Clone + Send + 'static,
Store: Datastore<Id, Data> + Clone + Send + 'static,
{

    /// Store a value in the DHT
    pub async fn store(&mut self, target: Id, data: Vec<Data>) -> Result<usize, Error> {
        // Update local data
        let _values = self.datastore.store(&target, &data);

        // Create a search instance
        let mut search = Search::new(
            self.id.clone(),
            target.clone(),
            Operation::FindNode,
            self.config.clone(),
            self.table.clone(),
            self.conn_mgr.clone(),
        );

        // Execute across K nearest nodes
        let nearest: Vec<_> = self.table.nearest(&target, 0..self.config.concurrency);
        search.seed(&nearest);

        let conn = self.conn_mgr.clone();
        let k = self.config.k;

        // Search for K closest nodes to value
        let _r = search.execute().await?;

        // Send store request to found nodes
        let known = search.completed(0..k);
        trace!("sending store to: {:?}", known);
        let resp = request_all(conn, &Request::Store(target, data), &known).await?;

        // TODO: should we process success here?
        Ok(resp.len())
    }

}