/**
 * rust-kad
 * Kademlia core implementation
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */


use std::marker::{PhantomData};
use std::fmt::{Debug};

use futures::prelude::*;
use futures::future;

use rr_mux::{Connector};

use crate::{Config, DhtConnector};

use crate::id::{DatabaseId, RequestId};
use crate::error::Error as DhtError;
use crate::nodetable::NodeTable;
use crate::datastore::{Datastore, Reducer};

use crate::message::{Request, Response};
use crate::search::{Search, Operation};
use crate::connection::{request_all};


pub struct Dht<Id, Node, Data, ReqId, Conn, Table, Store, Ctx> {
    id: Id,
    
    config: Config,
    table: Table,
    conn_mgr: Conn,
    datastore: Store,

    _node: PhantomData<Node>,
    _data: PhantomData<Data>,
    _req_id: PhantomData<ReqId>,
    _ctx: PhantomData<Ctx>,
}

impl <Id, Node, Data, ReqId, Conn, Table, Store, Ctx> Clone for Dht<Id, Node, Data, ReqId, Conn, Table, Store, Ctx> 
where
    Id: DatabaseId + Clone + Send + 'static,
    Node: Clone + Debug + Send + 'static,
    Data: Reducer<Item=Data> + Clone + Send + PartialEq + Debug + 'static,
    ReqId: RequestId + Clone + Send + 'static,
    Conn: DhtConnector<Id, Node, Data, ReqId, Ctx> + Clone + Send + 'static,
    Table: NodeTable<Id, Node> + Clone + Sync + Send + 'static,
    Store: Datastore<Id, Data> + Clone + Sync + Send + 'static,
    Ctx: Clone + Debug + PartialEq + Send + 'static,
{
    fn clone(&self) -> Self {
        Dht{
            id: self.id.clone(),
            config: self.config.clone(),
            table: self.table.clone(),
            conn_mgr: self.conn_mgr.clone(),
            datastore: self.datastore.clone(),
            
            _node: PhantomData,
            _data: PhantomData,
            _req_id: PhantomData,
            _ctx: PhantomData,
        }
    }
}

impl <Id, Node, Data, ReqId, Conn, Table, Store, Ctx> Dht<Id, Node, Data, ReqId, Conn, Table, Store, Ctx> 
where 
    Id: DatabaseId + Clone + Send + 'static,
    Node: Clone + Debug + Send + 'static,
    Data: Reducer<Item=Data> + Clone + Send + PartialEq + Debug + 'static,
    ReqId: RequestId + Clone + Send + 'static,
    Conn: DhtConnector<Id, Node, Data, ReqId, Ctx> + Clone + Send + 'static,
    Table: NodeTable<Id, Node> + Clone + Sync + Send + 'static,
    Store: Datastore<Id, Data> + Clone + Sync + Send + 'static,
    Ctx: Clone + Debug + PartialEq + Send + 'static,
{
    pub fn new(id: Id, config: Config, table: Table, conn_mgr: Conn, datastore: Store) -> Self {
        Dht{
            id,
            config, 
            table, 
            conn_mgr, 
            datastore, 

            _node: PhantomData, 
            _data: PhantomData, 
            _req_id: PhantomData, 
            _ctx: PhantomData 
        }
    }

    /// Connect to a known node
    /// This is used for bootstrapping onto the DHT
    pub fn connect(&mut self, target: Node, ctx: Ctx) -> impl Future<Item=(), Error=DhtError> + '_ {
        let table = self.table.clone();
        let conn_mgr = self.conn_mgr.clone();
        let id = self.id.clone();

        info!(target: "dht", "[DHT connect] {:?} to: {:?}", id, target);

        // Launch request
        self.conn_mgr.clone().request(ctx.clone(), ReqId::generate(), target.clone(), Request::FindNode(self.id.clone()))
            .and_then(move |(resp, _ctx)| {
                // Check for correct response
                match resp {
                    Response::NodesFound(_id, nodes) => future::ok((target, nodes)),
                    _ => {
                        warn!("[DHT connect] invalid response from: {:?}", target);
                        future::err(DhtError::InvalidResponse)
                    },
                }
            }).and_then(move |(target, found)| {
                // Add responding target to table
                // This only occurs after a response as there's no point adding a non-responding
                // node to the DHT
                // TODO: should we add the connect node? don't have Id here, perhaps that should come from .request()
                //table.clone().update(&target);

                debug!("[DHT connect] response received, searching {} nodes", found.len());

                // Perform FIND_NODE on own id with responded nodes to register self
                let mut search = Search::new(self.id.clone(), id, Operation::FindNode, self.config.clone(), table, conn_mgr, ctx);
                search.seed(&found); 

                search.execute()
            }).and_then(|_s| {
                // We don't care about the search result here
                // Generally it should be that discovered nodes > 0, however not for the first bootstrapping...

                // TODO: Refresh all k-buckets further away than our nearest neighbor

                future::ok(())
            })
    }




    /// Look up a node in the database by Id
    pub fn lookup(&mut self, target: Id, ctx: Ctx) -> impl Future<Item=Node, Error=DhtError> + '_ {
        // Create a search instance
        let mut search = Search::new(self.id.clone(), target.clone(), Operation::FindNode, self.config.clone(), self.table.clone(), self.conn_mgr.clone(), ctx);

        // Execute across K nearest nodes
        let nearest: Vec<_> = self.table.nearest(&target, 0..self.config.concurrency);
        search.seed(&nearest);

        // Execute the recursive search
        search.execute().then(|r| {
            // Handle internal search errors
            let s = match r {
                Err(e) => return Err(e),
                Ok(s) => s,
            };

            // Return node if found
            let known = s.known();
            if let Some((n, _s)) = known.get(s.target()) {
                Ok(n.clone())
            } else {
                Err(DhtError::NotFound)
            }
        })
    }


    /// Find a value from the DHT
    pub fn find(&mut self, target: Id, ctx: Ctx) -> impl Future<Item=Vec<Data>, Error=DhtError> {
        // Create a search instance
        let mut search = Search::new(self.id.clone(), target.clone(), Operation::FindValue, self.config.clone(), self.table.clone(), self.conn_mgr.clone(), ctx);

        // Execute across K nearest nodes
        let nearest: Vec<_> = self.table.nearest(&target, 0..self.config.concurrency);
        search.seed(&nearest);

        // Execute the recursive search
        search.execute()
        .then(|r| {
            // Handle internal search errors
            let s = match r {
                Err(e) => return Err(e),
                Ok(s) => s,
            };

            // Return data if found
            let data = s.data();
            if data.len() == 0 {
                return Err(DhtError::NotFound)
            }

            // Reduce data before returning
            let mut flat_data: Vec<Data> = data.iter().flat_map(|(_k, v)| v.clone() ).collect();
            Data::reduce(&mut flat_data);

            // TODO: Send updates to any peers that returned outdated data?

            // TODO: forward reduced k:v pairs to closest node in map (that did not return value)

            Ok(flat_data)
        })
    }


    /// Store a value in the DHT
    pub fn store(&mut self, target: Id, data: Vec<Data>, ctx: Ctx) -> impl Future<Item=(), Error=DhtError> {
        // Create a search instance
        let mut search = Search::new(self.id.clone(), target.clone(), Operation::FindNode, self.config.clone(), self.table.clone(), self.conn_mgr.clone(), ctx.clone());

        // Execute across K nearest nodes
        let nearest: Vec<_> = self.table.nearest(&target, 0..self.config.concurrency);
        search.seed(&nearest);

        let conn = self.conn_mgr.clone();
        let k = self.config.k;

        // Search for K closest nodes to value
        search.execute()
        .and_then(move |r| {
            // Send store request to found nodes
            let known = r.completed(0..k);
            debug!("sending store to: {:?}", known);
            request_all(conn, ctx, &Request::Store(target, data), &known)
        }).and_then(|_| {
            // TODO: should we process success here?
            Ok(())
        })
    }


    /// Refresh node table
    pub fn refresh(&mut self) -> impl Future<Item=(), Error=DhtError> {
        // TODO: send refresh to buckets that haven't been looked up recently
        // How to track recently looked up / contacted buckets..?

        // TODO: evict "expired" nodes from buckets
        // Message oldest node, if no response evict
        // Maybe this could be implemented as a periodic ping and timeout instead?

        future::err(DhtError::Unimplemented)
    }

    /// Receive and reply to requests
    pub fn receive(&mut self, from: &Id, req: &Request<Id, Data>) -> impl Future<Item=Response<Id, Node, Data>, Error=DhtError> {
        // Build response
        let resp = match req {
            Request::Ping => {
                Response::NoResult
            },
            Request::FindNode(id) => {
                let nodes = self.table.nearest(id, 0..self.config.k);
                Response::NodesFound(id.clone(), nodes)
            },
            Request::FindValue(id) => {
                // Lockup the value
                if let Some(values) = self.datastore.find(id) {
                    Response::ValuesFound(id.clone(), values)
                } else {
                    let nodes = self.table.nearest(id, 0..self.config.k);
                    Response::NodesFound(id.clone(), nodes)
                }                
            },
            Request::Store(id, value) => {
                // Write value to local storage
                self.datastore.store(id, value);
                // Reply to confirm write was completed
                Response::NoResult
            },
        };

        // Update record for sender
        //TODO: reintroduce
        //self.table.update(&from.0, from.1);

        future::ok(resp)
    }

    #[cfg(test)]
    pub fn contains(&mut self, id: &Id) -> Option<Node> {
        self.table.contains(id)
    }

    /// Create a basic search using the DHT
    /// This is provided for integration of the Dht with other components
    pub fn search(&mut self, id: Id, op: Operation, seed: &[(Id, Node)], ctx: Ctx) -> impl Future<Item=Search<Id, Node, Data, Table, Conn, ReqId, Ctx>, Error=DhtError> {
        let mut search = Search::new(self.id.clone(), id, op, self.config.clone(), self.table.clone(), self.conn_mgr.clone(), ctx);
        search.seed(seed);

        search.execute()
    }
}

/// Stream trait implemented to allow polling on dht object
impl <Id, Node, Data, ReqId, Conn, Table, Store, Ctx> Future for Dht <Id, Node, Data, ReqId, Conn, Table, Store, Ctx> {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        // TODO: poll / update internal state
        Ok(Async::NotReady)
    }
}

/// Helper macro to setup DHT instances for testing
#[cfg(test)] #[macro_export]
#[cfg(test)] macro_rules! mock_dht {
    ($connector: ident, $root: ident, $dht:ident) => {
        let mut config = Config::default();
        config.concurrency = 2;
        mock_dht!($connector, $root, $dht, config);
    };
    ($connector: ident, $root: ident, $dht:ident, $config:ident) => {
        let table = KNodeTable::new($root.0, $config.k, $config.hash_size);
        
        let store: HashMapStore<MockId, u64> = HashMapStore::new();
        
        let mut $dht = Dht::<MockId, u64, _, u64, _, _, _, ()>::new($root.0.clone(), 
                $config, table, $connector.clone(), store);
    }
}

#[cfg(test)]
mod tests {
    use std::clone::Clone;

    use super::*;
    use crate::datastore::{HashMapStore, Datastore};
    use crate::nodetable::{NodeTable, KNodeTable};
    use crate::node::Node;

    use crate::id::MockId;

    use rr_mux::mock::{MockConnector};

    #[test]
    fn test_receive_common() {

        let root = (MockId(0), 001);
        let friend = (MockId(1), 002);

        let mut connector = MockConnector::new().expect(vec![]);
        mock_dht!(connector, root, dht);

        // Check node is unknown
        assert!(dht.table.contains(&friend.0).is_none());

        // Ping
        assert_eq!(
            dht.receive(&friend.0, &Request::Ping).wait().unwrap(),
            Response::NoResult,
        );

        // Adds node to appropriate k bucket
        let friend1 = dht.table.entry(&friend.0).unwrap();

        // Second ping
        assert_eq!(
            dht.receive(&friend.0, &Request::Ping).wait().unwrap(),
            Response::NoResult,
        );

        // Updates node in appropriate k bucket
        let friend2 = dht.table.entry(&friend.0).unwrap();
        
        assert_ne!(friend1.seen(), friend2.seen());

        // Check expectations are done
        connector.finalise();
    }

    #[test]
    fn test_receive_ping() {

        let root = (MockId(0), 001);
        let friend = (MockId(1), 002);

        let mut connector = MockConnector::new().expect(vec![]);
        mock_dht!(connector, root, dht);

        // Ping
        assert_eq!(
            dht.receive(&friend.0, &Request::Ping).wait().unwrap(),
            Response::NoResult,
        );

        // Check expectations are done
        connector.finalise();
    }

    #[test]
    fn test_receive_find_nodes() {

        let root = (MockId(0), 001);
        let friend = (MockId(1), 002);
        let other = (MockId(2), 003);

        let mut connector = MockConnector::new().expect(vec![]);
        mock_dht!(connector, root, dht);

        // Add friend to known table
        dht.table.update(&friend.0);

        // FindNodes
        assert_eq!(
            dht.receive(&friend.0, &Request::FindNode(other.0)).wait().unwrap(),
            Response::NodesFound(other.0, vec![friend]), 
        );

        // Check expectations are done
        connector.finalise();
    }

        #[test]
    fn test_receive_find_values() {

        let root = (MockId(0), 001);
        let friend = (MockId(1), 002);
        let other = (MockId(2), 003);
        let value = MockId(201);

        let mut connector = MockConnector::new().expect(vec![]);
        mock_dht!(connector, root, dht);

        // Add friend to known table
        dht.table.register(&friend.0, friend.1);

        // FindValues (unknown, returns NodesFound)
        assert_eq!(
            dht.receive(&other.0, &Request::FindValue(value)).wait().unwrap(),
            Response::NodesFound(value, vec![friend]), 
        );

        // Add value to store
        dht.datastore.store(&value, &vec![1337]);
        
        // FindValues
        assert_eq!(
            dht.receive(&other.0, &Request::FindValue(value)).wait().unwrap(),
            Response::ValuesFound(value, vec![1337]), 
        );

        // Check expectations are done
        connector.finalise();
    }

    #[test]
    fn test_receive_store() {

        let root = (MockId(0), 001);
        let friend = (MockId(1), 002);
        let _other = (MockId(2), 003);
        let value = MockId(201);

        let mut connector = MockConnector::new().expect(vec![]);
        mock_dht!(connector, root, dht);

        // Store
        assert_eq!(
            dht.receive(&friend.0, &Request::Store(value, vec![1234])).wait().unwrap(),
            Response::NoResult,
        );

        let v = dht.datastore.find(&value).expect("missing value");
        assert_eq!(v, vec![1234]);

        // Check expectations are done
        connector.finalise();
    }

    #[test]
    fn test_clone() {

        let root = (MockId(0), 001);
        let _friend = (MockId(1), 002);
        let _other = (MockId(2), 003);

        let connector = MockConnector::new().expect(vec![]);
        mock_dht!(connector, root, dht);

        let _ = dht.clone();
    }

}
