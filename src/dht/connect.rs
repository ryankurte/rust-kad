
use std::fmt::Debug;

use crate::common::*;
use crate::store::Datastore;
use crate::table::NodeTable;

use super::{Dht, Search, Operation};

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

    /// Connect to a known node
    /// This is used for bootstrapping onto the DHT, however requires a complete Entry
    /// due to limitations of the underlying connector abstraction.
    /// If this is unsuitable, manually issue an initial FindNode request and use the
    /// handle_connect_response method to complete the connection.
    pub async fn connect(
        &mut self,
        target: Entry<Id, Info>,
    ) -> Result<Vec<Entry<Id, Info>>, Error> {
        let id = self.id.clone();

        info!(target: "dht", "[DHT connect] {:?} to: {:?} at: {:?}", id, target.id(), target.info());

        // Issue request
        self.request(ReqId::generate(),
            target.clone(),
            Request::FindNode(self.id.clone())
        );

        // TODO: await response
        
        // TODO: handle response
        s.handle_connect_response(target, resp).await
    }

    /// Handle the response to an initial FindNodes request.
    /// This is used to bootstrap a connection where the `.connect` method is unsuitable.
    pub async fn handle_connect_response(
        &mut self,
        target: Entry<Id, Info>,
        resp: Response<Id, Info, Data>,
    ) -> Result<Vec<Entry<Id, Info>>, Error> {
        let (id, mut found) = match resp {
            Response::NodesFound(id, nodes) => (id, nodes),
            Response::NoResult => {
                warn!(
                    "[DHT connect] Received NoResult response from {:?}",
                    target.id()
                );
                return Ok(vec![]);
            }
            _ => {
                warn!("[DHT connect] invalid response from: {:?}", target.id());
                return Err(Error::InvalidResponse);
            }
        };

        // Check ID matches self
        if id != self.id {
            return Err(Error::InvalidResponseId);
        }

        // Filter self from response
        let our_id = self.id.clone();
        let found: Vec<_> = found.drain(..).filter(|e| e.id() != &our_id).collect();

        // Add responding target to table
        // This only occurs after a response as there's no point adding a non-responding
        // node to the DHT
        let _table = self.table.clone();
        self.table.create_or_update(&target);

        trace!(
            "[DHT connect] response received, searching {} nodes",
            found.len()
        );

        // Create a FIND_NODE search on own id with responded nodes
        // This both registers this node with peers, and finds and relevant closer peers
        let search = self.search(id, Operation::FindNode, &found).await?;

        // We don't care about the search result here
        // Generally it should be that discovered nodes > 0, however not for the first bootstrapping...

        // TODO: Refresh all k-buckets further away than our nearest neighbor

        // Return newly discovered nodes
        Ok(search.all_completed())
    }

}