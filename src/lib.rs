//! rust-kad
//! A Kademlia DHT implementation in Rust
//!
//! https://github.com/ryankurte/rust-kad
//! Copyright 2018 Ryan Kurte

use std::fmt::Debug;

extern crate futures;

#[macro_use] extern crate log;

#[macro_use] extern crate structopt;

extern crate futures_timer;

extern crate num;
extern crate rand;

extern crate rr_mux;
use rr_mux::{Connector};

pub mod error;
use error::Error as DhtError;

pub mod id;
use id::{DatabaseId, RequestId};

pub mod message;
use crate::message::{Request, Response};

pub mod node;
use node::Node;

pub mod nodetable;
use nodetable::{NodeTable, KNodeTable};

pub mod datastore;
use datastore::{Datastore, HashMapStore, Reducer};

pub mod search;

pub mod dht;
use dht::Dht;

pub mod connection;

pub mod prelude;


#[derive(PartialEq, Clone, Debug, StructOpt)]
pub struct Config {
    /// Length of the hash used (in bits) 
    pub hash_size: usize,

    #[structopt(long = "dht-k")]
    /// Size of buckets and number of nearby nodes to consider when searching
    pub k: usize,
    #[structopt(long = "dht-concurrency")]
    /// Number of concurrent operations to be performed at once (also known as α or alpha)
    pub concurrency: usize,
    #[structopt(long = "dht-recursion-limit")]
    /// Maximum recursion depth for searches
    pub max_recursion: usize,
}

impl Default for Config {
    fn default() -> Config {
            Config {
                hash_size: 512, 
                k: 20, 
                concurrency: 3,
                max_recursion: 10,
            }
    }
}

/// Standard DHT implementation using included KNodeTable and HashMapStore implementations
pub type StandardDht<Id, Addr, Data, ReqId, Conn, Ctx> = Dht<Id, Addr, Data, ReqId, Conn, KNodeTable<Id, Addr>, HashMapStore<Id, Data>, Ctx>;

impl <Id, Addr, Data, ReqId, Conn, Ctx> StandardDht<Id, Addr, Data, ReqId, Conn, Ctx> 
where 
    Id: DatabaseId + Clone + Send + 'static,
    Addr: PartialEq + Clone + Debug + Send + 'static,
    Data: Reducer<Item=Data> + PartialEq + Clone + Send  + Debug + 'static,
    ReqId: RequestId + Clone + Send + 'static,
    Conn: Connector<ReqId, Node<Id, Addr>, Request<Id, Data>, Response<Id, Addr, Data>, DhtError, Ctx> + Send + Clone + 'static,
    Ctx: Clone + PartialEq + Debug + Send + 'static,

{
    /// Helper to construct a standard Dht using crate provided KNodeTable and HashMapStore.
    pub fn standard(id: Id, config: Config, conn: Conn) -> StandardDht<Id, Addr,Data, ReqId, Conn, Ctx> {
        let table = KNodeTable::new(id.clone(), config.k, config.hash_size);
        let store = HashMapStore::new();
        Dht::new(id, config, table, conn, store)
    }
}

/// DhtMux defines an rr_mux::Mux over Dht types for convenience
pub type DhtMux<NodeId, Addr, Data, ReqId, Ctx> = rr_mux::Mux<ReqId, Node<NodeId, Addr>, Request<NodeId, Data>, Response<NodeId, Addr, Data>, DhtError, Ctx>;

/// DhtConnector defines an rr_mux::Connector impl over Dht types for convenience
pub type DhtConnector<NodeId, Addr, Data, ReqId, Ctx> = Connector<ReqId, Node<NodeId, Addr>, Request<NodeId, Data>, Response<NodeId, Addr, Data>, DhtError, Ctx>;

#[cfg(test)]
mod tests {
    use std::clone::Clone;
    use futures::{Future};

    use super::*;
    use crate::prelude::*;
    
    use rr_mux::Mux;
    use rr_mux::mock::{MockTransaction, MockConnector};

    type RequestId = u64;
    type NodeId = u64;
    type Addr = u64;
    type Data = u64;


    #[test]
    fn test_mux() {
        // Create a generic mux
        let dht_mux = Mux::<RequestId, Node<NodeId, Addr>, Request<NodeId, Data>, Response<NodeId, Addr, Data>, DhtError, ()>::new();

        // Bind it to the DHT instance
        let n1 = Node::new(0b0001, 100);
        let dht = Dht::<NodeId, Addr, Data, RequestId, _, _, _, _>::standard(n1.id().clone(), Config::default(), dht_mux);
    }

    #[test]
    fn test_connect() {
        let n1 = Node::new(0b0001, 100);
        let n2 = Node::new(0b0010, 200);
        let n3 = Node::new(0b0011, 300);
        let n4 = Node::new(0b1000, 400);

        // Build expectations
        let mut connector = MockConnector::new().expect(vec![
            // First transaction to bootstrap onto the network
            MockTransaction::request(n2.clone(), Request::FindNode(n1.id().clone()), Ok( (Response::NodesFound(n1.id().clone(), vec![n3.clone(), n4.clone()]), ()) )),

            //bootsrap to found nodes
            MockTransaction::request(n3.clone(), Request::FindNode(n1.id().clone()), Ok( (Response::NodesFound(n1.id().clone(), vec![]), ()) )),
            MockTransaction::request(n4.clone(), Request::FindNode(n1.id().clone()), Ok( (Response::NodesFound(n1.id().clone(), vec![]), ()) )),
        ]);

        // Create configuration
        let mut config = Config::default();
        config.concurrency = 2;

        let knodetable = KNodeTable::new(n1.id().clone(), 2, 4);
        
        // Instantiated DHT
        let store: HashMapStore<u64, u64> = HashMapStore::new();
        let mut dht = Dht::<u64, u64, _, u64, _, _, _, _>::new(n1.id().clone(), 
                config, knodetable, connector.clone(), store);
    
        // Attempt initial bootstrapping
        dht.connect(n2.clone(), ()).wait().unwrap();

        // Check bootstrapped node is added to db
        assert_eq!(Some(n2.clone()), dht.contains(n2.id()));

        // Check Reported nodes are added
        assert_eq!(Some(n3.clone()), dht.contains(n3.id()));
        assert_eq!(Some(n4.clone()), dht.contains(n4.id()));

        // Check expectations are done
        connector.finalise();
    }

   #[test]
    fn test_lookup() {
        let n1 = Node::new(0b1000, 100);
        let n2 = Node::new(0b0011, 200);
        let n3 = Node::new(0b0010, 300);
        let n4 = Node::new(0b1001, 400);
        let n5 = Node::new(0b1010, 400);

        // Build expectations
        let mut connector = MockConnector::new().expect(vec![
            // First transaction to bootstrap onto the network
            MockTransaction::request(n2.clone(), Request::FindNode(n4.id().clone()), Ok( (Response::NodesFound(n4.id().clone(), vec![n4.clone()]), ()) )),
            MockTransaction::request(n3.clone(), Request::FindNode(n4.id().clone()), Ok( (Response::NodesFound(n4.id().clone(), vec![n5.clone()]), ()) )),

            // Second iteration
            MockTransaction::request(n4.clone(), Request::FindNode(n4.id().clone()), Ok( (Response::NodesFound(n4.id().clone(), vec![]), ()) )),
            MockTransaction::request(n5.clone(), Request::FindNode(n4.id().clone()), Ok( (Response::NodesFound(n4.id().clone(), vec![]), ()) )),
        ]);

        // Create configuration
        let mut config = Config::default();
        config.concurrency = 2;
        config.k = 2;

        let mut knodetable = KNodeTable::new(n1.id().clone(), 2, 4);
        
        // Inject initial nodes into the table
        knodetable.update(&n2);
        knodetable.update(&n3);

        // Instantiated DHT
        let store: HashMapStore<u64, u64> = HashMapStore::new();
        let mut dht = Dht::<u64, u64, _, u64, _, _, _, _>::new(n1.id().clone(), 
                config, knodetable, connector.clone(), store);

        // Perform search
        dht.lookup(n4.id().clone(), ()).wait().expect("lookup failed");

        connector.finalise();
    }

       #[test]
    fn test_store() {
        let n1 = Node::new(0b1000, 100);
        let n2 = Node::new(0b0011, 200);
        let n3 = Node::new(0b0010, 300);
        let n4 = Node::new(0b1001, 400);
        let n5 = Node::new(0b1010, 500);

        let id = 0b1011;
        let val = vec![1234];

        // Build expectations
        let mut connector = MockConnector::new().expect(vec![
            // First transaction to bootstrap onto the network
            MockTransaction::request(n2.clone(), Request::FindNode(id), Ok(  (Response::NodesFound(id, vec![n4.clone()]), ()) )),
            MockTransaction::request(n3.clone(), Request::FindNode(id), Ok( (Response::NodesFound(id, vec![n5.clone()]), ()) )),

            // Second iteration to find k nodes closest to v
            MockTransaction::request(n5.clone(), Request::FindNode(id), Ok( (Response::NodesFound(id, vec![]), ()) )),
            MockTransaction::request(n4.clone(), Request::FindNode(id), Ok( (Response::NodesFound(id, vec![]), ()) )),

            // Final iteration pushes data to k nodes
            MockTransaction::request(n5.clone(), Request::Store(id, val.clone()), Ok( (Response::NoResult, ()) )),
            MockTransaction::request(n4.clone(), Request::Store(id, val.clone()), Ok( (Response::NoResult, ()) )),
        ]);

        // Create configuration
        let mut config = Config::default();
        config.concurrency = 2;
        config.k = 2;

        let mut knodetable = KNodeTable::new(n1.id().clone(), 2, 4);
        
        // Inject initial nodes into the table
        knodetable.update(&n2);
        knodetable.update(&n3);

        // Instantiated DHT
        let store: HashMapStore<u64, u64> = HashMapStore::new();
        let mut dht = Dht::<u64, u64, _, u64, _, _, _, _>::new(n1.id().clone(), 
                config, knodetable, connector.clone(), store);

        // Perform store
        dht.store(id, val, ()).wait().expect("store failed");

        connector.finalise();
    }


    #[test]
    fn test_find() {
        let n1 = Node::new(0b1000, 100);
        let n2 = Node::new(0b0011, 200);
        let n3 = Node::new(0b0010, 300);
        let n4 = Node::new(0b1001, 400);
        let n5 = Node::new(0b1010, 500);

        let id = 0b1011;
        let val = vec![1234];

        // Build expectations
        let mut connector = MockConnector::new().expect(vec![
            // First transaction to bootstrap onto the network
            MockTransaction::request(n2.clone(), Request::FindValue(id), Ok( (Response::NodesFound(id, vec![n4.clone()]), ()) )),
            MockTransaction::request(n3.clone(), Request::FindValue(id), Ok( (Response::NodesFound(id, vec![n5.clone()]), ()) )),

            // Next iteration gets node data
            MockTransaction::request(n5.clone(), Request::FindValue(id), Ok( (Response::ValuesFound(id, val.clone()), ()) )),
            MockTransaction::request(n4.clone(), Request::FindValue(id), Ok( (Response::ValuesFound(id, val.clone()), ()) )),
        ]);

        // Create configuration
        let mut config = Config::default();
        config.concurrency = 2;
        config.k = 2;

        let mut knodetable = KNodeTable::new(n1.id().clone(), 2, 4);
        
        // Inject initial nodes into the table
        knodetable.update(&n2);
        knodetable.update(&n3);

        // Instantiated DHT
        let store: HashMapStore<u64, u64> = HashMapStore::new();
        let mut dht = Dht::<u64, u64, _, u64, _, _, _, _>::new(n1.id().clone(), 
                config, knodetable, connector.clone(), store);

        // Perform store
        dht.find(id, ()).wait().expect("find failed");

        connector.finalise();
    }

}
