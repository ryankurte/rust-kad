/**
 * rust-kad
 * Kademlia core implementation
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */


use std::sync::{Arc, Mutex};
use std::marker::{PhantomData};
use std::fmt::{Debug};

use futures::prelude::*;
use futures::future;

use crate::{Config};
use crate::node::Node;
use crate::id::DatabaseId;
use crate::error::Error as DhtError;
use crate::nodetable::NodeTable;
use crate::message::{Request, Response};
use crate::datastore::{Datastore, Reducer};
use crate::search::{Search, Operation};
use crate::connection::{ConnectionManager, request_all};

#[derive(Clone, Debug)]
pub struct Dht<ID, ADDR, DATA, TABLE, CONN, STORE> {
    id: ID,
    addr: ADDR,
    config: Config,
    table: Arc<Mutex<TABLE>>,
    conn_mgr: CONN,
    datastore: Arc<Mutex<STORE>>,
    data: PhantomData<DATA>,
}

impl <ID, ADDR, DATA, TABLE, CONN, STORE> Dht<ID, ADDR, DATA, TABLE, CONN, STORE> 
where 
    ID: DatabaseId + 'static,
    ADDR: Clone + Debug + 'static,
    DATA: Reducer<Item=DATA> + PartialEq + Clone + Debug + 'static,
    TABLE: NodeTable<ID, ADDR> + 'static,
    STORE: Datastore<ID, DATA> + 'static,
    CONN: ConnectionManager<ID, ADDR, DATA, DhtError> + Clone + 'static,
{
    pub fn new(id: ID, addr: ADDR, config: Config, table: TABLE, conn_mgr: CONN, datastore: STORE) -> Dht<ID, ADDR, DATA, TABLE, CONN, STORE> {
        Dht{id, addr, config, table: Arc::new(Mutex::new(table)), conn_mgr, datastore: Arc::new(Mutex::new(datastore)), data: PhantomData}
    }

    /// Connect to a known node
    /// This is used for bootstrapping onto the DHT
    pub fn connect(&mut self, target: Node<ID, ADDR>) -> impl Future<Item=(), Error=DhtError> + '_ {
        let table = self.table.clone();
        let conn_mgr = self.conn_mgr.clone();
        let id = self.id.clone();

        println!("[DHT connect] {:?} to: {:?} at: {:?}", id, target.id(), target.address());

        // Launch request
        self.conn_mgr.clone().request(&target, Request::FindNode(self.id.clone()))
            .and_then(move |resp| {
                // Check for correct response
                match resp {
                    Response::NodesFound(nodes) => future::ok((target, nodes)),
                    _ => {
                        println!("[DHT connect] invalid response from: {:?}", target.id());
                        future::err(DhtError::InvalidResponse)
                    },
                }
            }).and_then(move |(target, found)| {
                // Add responding target to table
                // This only occurs after a response as there's no point adding a non-responding
                // node to the DHT
                table.lock().unwrap().update(&target);

                println!("[DHT connect] response recieved, searching {} nodes", found.len());

                // Perform FIND_NODE on own id with responded nodes to register self
                let mut search = Search::new(self.id.clone(), id, Operation::FindNode, self.config.clone(), table, conn_mgr);
                search.seed(&found);

                search.execute()
            }).and_then(|_s| {
                // We don't care about the search result here
                // Generally it should be that discovered nodes > 0, however not for the first bootstrapping...

                // TODO: Refresh all k-buckets further away than our nearest neighbor

                future::ok(())
            })
    }


    /// Look up a node in the database by ID
    pub fn lookup(&mut self, target: ID) -> impl Future<Item=Node<ID, ADDR>, Error=DhtError> + '_ {
        // Create a search instance
        let mut search = Search::new(self.id.clone(), target.clone(), Operation::FindNode, self.config.clone(), self.table.clone(), self.conn_mgr.clone());

        // Execute across K nearest nodes
        let nearest: Vec<_> = self.table.lock().unwrap().nearest(&target, 0..self.config.concurrency);
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
    pub fn find(&mut self, target: ID) -> impl Future<Item=Vec<DATA>, Error=DhtError> {
        // Create a search instance
        let mut search = Search::new(self.id.clone(), target.clone(), Operation::FindValue, self.config.clone(), self.table.clone(), self.conn_mgr.clone());

        // Execute across K nearest nodes
        let nearest: Vec<_> = self.table.lock().unwrap().nearest(&target, 0..self.config.concurrency);
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
            let mut flat_data: Vec<DATA> = data.iter().flat_map(|(_k, v)| v.clone() ).collect();
            DATA::reduce(&mut flat_data);

            // TODO: Send updates to any peers that returned outdated data?

            // TODO: forward reduced k:v pairs to closest node in map (that did not return value)

            Ok(flat_data)
        })
    }


    /// Store a value in the DHT
    pub fn store(&mut self, target: ID, data: Vec<DATA>) -> impl Future<Item=(), Error=DhtError> {
        // Create a search instance
        let mut search = Search::new(self.id.clone(), target.clone(), Operation::FindNode, self.config.clone(), self.table.clone(), self.conn_mgr.clone());

        // Execute across K nearest nodes
        let nearest: Vec<_> = self.table.lock().unwrap().nearest(&target, 0..self.config.concurrency);
        search.seed(&nearest);

        let conn = self.conn_mgr.clone();
        let k = self.config.k;

        // Search for K closest nodes to value
        search.execute()
        .and_then(move |r| {
            // Send store request to found nodes
            let known = r.completed(0..k);
            println!("sending store to: {:?}", known);
            request_all(conn, &Request::Store(target, data), &known)
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
    pub fn receive(&mut self, from: &Node<ID, ADDR>, req: &Request<ID, DATA>) -> Response<ID, ADDR, DATA> {
        // Build response
        let resp = match req {
            Request::Ping => {
                Response::NoResult
            },
            Request::FindNode(id) => {
                let nodes = self.table.lock().unwrap().nearest(id, 0..self.config.k);
                Response::NodesFound(nodes)
            },
            Request::FindValue(id) => {
                // Lockup the value
                if let Some(values) = self.datastore.lock().unwrap().find(id) {
                    Response::ValuesFound(values)
                } else {
                    let nodes = self.table.lock().unwrap().nearest(id, 0..self.config.k);
                    Response::NodesFound(nodes)
                }                
            },
            Request::Store(id, value) => {
                // Write value to local storage
                self.datastore.lock().unwrap().store(id, value);
                // Reply to confirm write was completed
                Response::NoResult
            },
        };

        // Update record for sender
        self.table.lock().unwrap().update(from);

        resp
    }

    #[cfg(test)]
    pub fn contains(&mut self, id: &ID) -> Option<Node<ID, ADDR>> {
        self.table.lock().unwrap().contains(id)
    }
}

/// Stream trait implemented to allow polling on dht object
impl <ID, ADDR, DATA, TABLE, CONN, STORE> Future for Dht <ID, ADDR, DATA, TABLE, CONN, STORE> {
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
        let table = KNodeTable::new($root.id().clone(), $config.k, $config.hash_size);
        
        let store: HashMapStore<u64, u64> = HashMapStore::new();
        
        let mut $dht = Dht::<u64, u64, _, _, _, _>::new($root.id().clone(), $root.address().clone(), 
                $config, table, $connector.clone(), store);
    }
}

#[cfg(test)]
mod tests {
    use std::clone::Clone;

    use super::*;
    use crate::datastore::{HashMapStore, Datastore};
    use crate::nodetable::{NodeTable, KNodeTable};

    use crate::mock::*;

    #[test]
    fn test_receive_common() {

        let root = Node::new(0, 001);
        let friend = Node::new(1, 002);

        let connector = MockConnector::from(vec![]);
        mock_dht!(connector, root, dht);

        // Check node is unknown
        assert!(dht.table.lock().unwrap().contains(friend.id()).is_none());

        // Ping
        assert_eq!(
            dht.receive(&friend, &Request::Ping),
            Response::NoResult,
        );

        // Adds node to appropriate k bucket
        let friend1 = dht.table.lock().unwrap().contains(friend.id()).unwrap();

        // Second ping
        assert_eq!(
            dht.receive(&friend, &Request::Ping),
            Response::NoResult,
        );

        // Updates node in appropriate k bucket
        let friend2 = dht.table.lock().unwrap().contains(friend.id()).unwrap();
        assert_ne!(friend1.seen(), friend2.seen());

        // Check expectations are done
        connector.done();
    }

    #[test]
    fn test_receive_ping() {

        let root = Node::new(0, 001);
        let friend = Node::new(1, 002);

        let connector = MockConnector::from(vec![]);
        mock_dht!(connector, root, dht);

        // Ping
        assert_eq!(
            dht.receive(&friend, &Request::Ping),
            Response::NoResult,
        );

        // Check expectations are done
        connector.done();
    }

    #[test]
    fn test_receive_find_nodes() {

        let root = Node::new(0, 001);
        let friend = Node::new(1, 002);
        let other = Node::new(2, 003);

        let connector = MockConnector::from(vec![]);
        mock_dht!(connector, root, dht);

        // Add friend to known table
        dht.table.lock().unwrap().update(&friend);

        // FindNodes
        assert_eq!(
            dht.receive(&friend, &Request::FindNode(other.id().clone())),
            Response::NodesFound(vec![friend.clone()]), 
        );

        // Check expectations are done
        connector.done();
    }

        #[test]
    fn test_receive_find_values() {

        let root = Node::new(0, 001);
        let friend = Node::new(1, 002);
        let other = Node::new(2, 003);

        let connector = MockConnector::from(vec![]);
        mock_dht!(connector, root, dht);

        // Add friend to known table
        dht.table.lock().unwrap().update(&friend);

        // FindValues (unknown, returns NodesFound)
        assert_eq!(
            dht.receive(&other, &Request::FindValue(201)),
            Response::NodesFound(vec![friend.clone()]), 
        );

        // Add value to store
        dht.datastore.lock().unwrap().store(&201, &vec![1337]);
        
        // FindValues
        assert_eq!(
            dht.receive(&other, &Request::FindValue(201)),
            Response::ValuesFound(vec![1337]), 
        );

        // Check expectations are done
        connector.done();
    }

    #[test]
    fn test_receive_store() {

        let root = Node::new(0, 001);
        let friend = Node::new(1, 002);

        let connector = MockConnector::from(vec![]);
        mock_dht!(connector, root, dht);

        // Store
        assert_eq!(
            dht.receive(&friend, &Request::Store(2, vec![1234])),
            Response::NoResult,
        );

        let v = dht.datastore.lock().unwrap().find(&2).expect("missing value");
        assert_eq!(v, vec![1234]);

        // Check expectations are done
        connector.done();
    }

}
