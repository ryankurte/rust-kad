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

use rr_mux::{Mux, Connector, Muxed};

use crate::{Config};
use crate::node::Node;
use crate::id::{DatabaseId, RequestId};
use crate::error::Error as DhtError;
use crate::nodetable::NodeTable;
use crate::message::{Message, Request, Response};
use crate::datastore::{Datastore, Reducer};
use crate::search::{Search, Operation};

use crate::connection::{request_all};

/// Generic DHT Implementation
/// This is generic over:
/// -  ID which must fulfill the DatabaseId trait
/// - ADDR which is used by the connector to route messages
/// - REQ_ID which is used by the connector for identifying requests and responses
/// - CONN which must fulfill the Connector trait
/// - CTX which is passed through the connector to provide arbitrary transaction context for implementations
/// - TABLE which must fulfill the NodeTable<ID, ADDR> trait
/// - STORE which must fullfil the Datastore<ID, DATA> trait
pub struct Dht<ID, ADDR, DATA, REQ_ID, CONN, CTX, TABLE, STORE> {
    id: ID,
    
    config: Config,
    table: Arc<Mutex<TABLE>>,
    conn_mgr: CONN,
    datastore: Arc<Mutex<STORE>>,

    addr: PhantomData<ADDR>,
    data: PhantomData<DATA>,
    req_id: PhantomData<REQ_ID>,
    ctx: PhantomData<CTX>,
}

impl <ID, ADDR, DATA, REQ_ID, CONN, CTX, TABLE, STORE> Clone for Dht<ID, ADDR, DATA, REQ_ID, CONN, CTX, TABLE, STORE> 
where
    ID: DatabaseId + Clone + 'static,
    ADDR: Clone + Debug + Clone + 'static,
    DATA: Reducer<Item=DATA> + Clone + PartialEq + Debug + 'static,
    REQ_ID: RequestId + Clone + 'static,
    TABLE: NodeTable<ID, ADDR> + 'static,
    STORE: Datastore<ID, DATA> + 'static,
    CTX: Clone + PartialEq + Debug + 'static,
    CONN: Connector<REQ_ID, Node<ID, ADDR>, Request<ID, DATA>, Response<ID, ADDR, DATA>, DhtError, CTX> + Clone + 'static,
{
    fn clone(&self) -> Dht<ID, ADDR, DATA, REQ_ID, CONN, CTX, TABLE, STORE> {
        Dht{
            id: self.id.clone(),
            config: self.config.clone(),
            table: self.table.clone(),
            conn_mgr: self.conn_mgr.clone(),
            datastore: self.datastore.clone(),
            
            addr: PhantomData,
            data: PhantomData,
            req_id: PhantomData,
            ctx: PhantomData,
        }
    }
}

impl <ID, ADDR, DATA, REQ_ID, CONN, CTX, TABLE, STORE> Dht<ID, ADDR, DATA, REQ_ID, CONN, CTX, TABLE, STORE> 
where 
    ID: DatabaseId + Clone + Send + 'static,
    ADDR: Clone + Debug + Clone + Send + 'static,
    DATA: Reducer<Item=DATA> + Clone + Send + PartialEq + Clone + Debug + 'static,
    REQ_ID: RequestId + Clone + Send + 'static,
    TABLE: NodeTable<ID, ADDR> + Send + 'static,
    STORE: Datastore<ID, DATA> + Send + 'static,
    CTX: Clone + PartialEq + Debug + 'static,
    CONN: Connector<REQ_ID, Node<ID, ADDR>, Request<ID, DATA>, Response<ID, ADDR, DATA>, DhtError, CTX> + Clone + Send + 'static,
{
    pub fn new(id: ID, config: Config, table: TABLE, conn_mgr: CONN, datastore: STORE) -> Dht<ID, ADDR, DATA, REQ_ID, CONN, CTX, TABLE, STORE> {
        Dht{id, config, 
            table: Arc::new(Mutex::new(table)), 
            conn_mgr, 
            datastore: Arc::new(Mutex::new(datastore)), 
            addr: PhantomData, 
            data: PhantomData,
            req_id: PhantomData,
            ctx: PhantomData,
        }
    }

    /// Connect to a known node
    /// This is used for bootstrapping onto the DHT
    pub fn connect(&mut self, ctx: CTX, target: Node<ID, ADDR>) -> impl Future<Item=(), Error=DhtError> + '_ {
        let table = self.table.clone();
        let conn_mgr = self.conn_mgr.clone();
        let id = self.id.clone();

        println!("[DHT connect] {:?} to: {:?} at: {:?}", id, target.id(), target.address());

        // Launch request
        self.conn_mgr.clone().request(ctx.clone(), REQ_ID::generate(), target.clone(), Request::FindNode(self.id.clone()))
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




    /// Look up a node in the database by ID
    pub fn lookup(&mut self, ctx: CTX, target: ID) -> impl Future<Item=Node<ID, ADDR>, Error=DhtError> + '_ {
        // Create a search instance
        let mut search = Search::new(self.id.clone(), target.clone(), Operation::FindNode, self.config.clone(), self.table.clone(), self.conn_mgr.clone(), ctx);

        // Execute across K nearest node
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
    pub fn find(&mut self, ctx: CTX, target: ID) -> impl Future<Item=Vec<DATA>, Error=DhtError> {
        // Create a search instance
        let mut search = Search::new(self.id.clone(), target.clone(), Operation::FindValue, self.config.clone(), self.table.clone(), self.conn_mgr.clone(), ctx);

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
    pub fn store(&mut self, ctx: CTX, target: ID, data: Vec<DATA>) -> impl Future<Item=(), Error=DhtError> {
        // Create a search instance
        let mut search = Search::new(self.id.clone(), target.clone(), Operation::FindNode, self.config.clone(), self.table.clone(), self.conn_mgr.clone(), ctx.clone());

        // Execute across K nearest nodes
        let nearest: Vec<_> = self.table.lock().unwrap().nearest(&target, 0..self.config.concurrency);
        search.seed(&nearest);

        let conn = self.conn_mgr.clone();
        let k = self.config.k;

        // Search for K closest nodes to value
        let ctx = ctx.clone();
        search.execute()
        .and_then(move |r| {
            // Send store request to found nodes
            let known = r.completed(0..k);
            println!("sending store to: {:?}", known);
            request_all(conn, ctx, &Request::Store(target, data), &known)
        }).and_then(|_| {
            // TODO: should we process success here?
            Ok(())
        })
    }


    /// Refresh node table
    pub fn refresh(&mut self, ctx: CTX) -> impl Future<Item=(), Error=DhtError> {
        // TODO: send refresh to buckets that haven't been looked up recently
        // How to track recently looked up / contacted buckets..?

        // TODO: evict "expired" nodes from buckets
        // Message oldest node, if no response evict
        // Maybe this could be implemented as a periodic ping and timeout instead?

        future::err(DhtError::Unimplemented)
    }

    /// Receive and reply to requests
    pub fn receive(&mut self, ctx: CTX, from: &Node<ID, ADDR>, req: &Request<ID, DATA>) -> impl Future<Item=Response<ID, ADDR, DATA>, Error=DhtError> {
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

        future::ok(resp)
    }

    #[cfg(test)]
    pub fn contains(&mut self, id: &ID) -> Option<Node<ID, ADDR>> {
        self.table.lock().unwrap().contains(id)
    }

    /// Create a basic search using the DHT
    /// This is provided for integration of the Dht with other components
    pub fn search(&mut self, ctx: CTX, id: ID, op: Operation, seed: &[Node<ID, ADDR>]) -> impl Future<Item=Search <ID, ADDR, DATA, TABLE, CONN, REQ_ID, CTX>, Error=DhtError> {
        let mut search = Search::new(self.id.clone(), id, op, self.config.clone(), self.table.clone(), self.conn_mgr.clone(), ctx);
        search.seed(&seed);

        search.execute()
    }
}

/// Stream trait implemented to allow polling on dht object
impl <ID, ADDR, DATA, REQ_ID, CONN, CTX, TABLE, STORE> Future for Dht <ID, ADDR, DATA, REQ_ID, CONN, CTX, TABLE, STORE> {
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
        
        let mut $dht = Dht::<u64, u64, _, u64, _, (), _, _>::new($root.id().clone(), 
                $config, table, $connector.clone(), store);
    }
}

#[cfg(test)]
mod tests {
    use std::clone::Clone;

    use super::*;
    use crate::datastore::{HashMapStore, Datastore};
    use crate::nodetable::{NodeTable, KNodeTable};

    use rr_mux::mock::{MockConnector, MockTransaction, MockRequest};

    #[test]
    fn test_receive_common() {

        let root = Node::new(0, 001);
        let friend = Node::new(1, 002);

        let mut connector = MockConnector::new().expect(vec![]);
        mock_dht!(connector, root, dht);

        // Check node is unknown
        assert!(dht.table.lock().unwrap().contains(friend.id()).is_none());

        // Ping
        assert_eq!(
            dht.receive((), &friend, &Request::Ping).wait().unwrap(),
            Response::NoResult,
        );

        // Adds node to appropriate k bucket
        let friend1 = dht.table.lock().unwrap().contains(friend.id()).unwrap();

        // Second ping
        assert_eq!(
            dht.receive((), &friend, &Request::Ping).wait().unwrap(),
            Response::NoResult,
        );

        // Updates node in appropriate k bucket
        let friend2 = dht.table.lock().unwrap().contains(friend.id()).unwrap();
        assert_ne!(friend1.seen(), friend2.seen());

        // Check expectations are done
        connector.finalise();
    }

    #[test]
    fn test_receive_ping() {

        let root = Node::new(0, 001);
        let friend = Node::new(1, 002);

        let mut connector = MockConnector::new().expect(vec![]);
        mock_dht!(connector, root, dht);

        // Ping
        assert_eq!(
            dht.receive((), &friend, &Request::Ping).wait().unwrap(),
            Response::NoResult,
        );

        // Check expectations are done
        connector.finalise();
    }

    #[test]
    fn test_receive_find_nodes() {

        let root = Node::new(0, 001);
        let friend = Node::new(1, 002);
        let other = Node::new(2, 003);

        let mut connector = MockConnector::new().expect(vec![]);
        mock_dht!(connector, root, dht);

        // Add friend to known table
        dht.table.lock().unwrap().update(&friend);

        // FindNodes
        assert_eq!(
            dht.receive((), &friend, &Request::FindNode(other.id().clone())).wait().unwrap(),
            Response::NodesFound(vec![friend.clone()]), 
        );

        // Check expectations are done
        connector.finalise();
    }

        #[test]
    fn test_receive_find_values() {

        let root = Node::new(0, 001);
        let friend = Node::new(1, 002);
        let other = Node::new(2, 003);

        let mut connector = MockConnector::new().expect(vec![]);
        mock_dht!(connector, root, dht);

        // Add friend to known table
        dht.table.lock().unwrap().update(&friend);

        // FindValues (unknown, returns NodesFound)
        assert_eq!(
            dht.receive((), &other, &Request::FindValue(201)).wait().unwrap(),
            Response::NodesFound(vec![friend.clone()]), 
        );

        // Add value to store
        dht.datastore.lock().unwrap().store(&201, &vec![1337]);
        
        // FindValues
        assert_eq!(
            dht.receive((), &other, &Request::FindValue(201)).wait().unwrap(),
            Response::ValuesFound(vec![1337]), 
        );

        // Check expectations are done
        connector.finalise();
    }

    #[test]
    fn test_receive_store() {

        let root = Node::new(0, 001);
        let friend = Node::new(1, 002);

        let mut connector = MockConnector::new().expect(vec![]);
        mock_dht!(connector, root, dht);

        // Store
        assert_eq!(
            dht.receive((), &friend, &Request::Store(2, vec![1234])).wait().unwrap(),
            Response::NoResult,
        );

        let v = dht.datastore.lock().unwrap().find(&2).expect("missing value");
        assert_eq!(v, vec![1234]);

        // Check expectations are done
        connector.finalise();
    }

    #[test]
    fn test_clone() {

        let root = Node::new(0, 001);
        let friend = Node::new(1, 002);

        let mut connector = MockConnector::new().expect(vec![]);
        mock_dht!(connector, root, dht);

        let _ = dht.clone();
    }

}
