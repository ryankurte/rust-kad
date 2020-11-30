
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
    /// Find value(s) in the DHT
    pub async fn find(&mut self, target: Id) -> Result<Vec<Data>, Error> {
        let mut existing = self.datastore.find(&target);

        warn!("Existing data: {:?}", existing);

        // Create a search instance
        let mut search = Search::new(
            self.id.clone(),
            target.clone(),
            Operation::FindValue,
            self.config.clone(),
            self.table.clone(),
            self.conn_mgr.clone(),
        );

        // Execute across K nearest nodes
        let nearest: Vec<_> = self.table.nearest(&target, 0..self.config.concurrency);
        search.seed(&nearest);

        if nearest.len() == 0 {
            return Err(Error::NoPeers);
        }

        // Execute the recursive search
        let r = search.execute().await;

        // Handle internal search errors
        let _s = match r {
            Err(e) => return Err(e),
            Ok(s) => s,
        };

        // Retrieve search data
        let data = search.data();

        // TODO: Reduce data before returning? (should be done on insertion anyway..?)
        let mut flat_data: Vec<Data> = data.iter().flat_map(|(_k, v)| v.clone()).collect();

        // Append existing data (non-flattened atm)
        if let Some(existing) = &mut existing {
            flat_data.append(existing)
        }

        // TODO: cache data locally for next search

        // TODO: Send updates to any peers that returned outdated data?

        // TODO: forward reduced k:v pairs to closest node in map (that did not return value)

        // Error out if no data found
        if flat_data.len() == 0 {
            return Err(Error::NotFound);
        }

        Ok(flat_data)
    }
}