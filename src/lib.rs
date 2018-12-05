/**
 * rust-kad
 * A Kademlia DHT implementation in Rust
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */

use std::time::{Duration};

extern crate futures;
use futures::{Future};

#[macro_use]
extern crate log;

extern crate futures_timer;

extern crate num;
extern crate rand;

pub mod error;
pub use self::error::Error as DhtError;
pub mod message;
pub use self::message::{Message, Request, Response};
pub mod node;
pub use self::node::Node;
pub mod id;
pub use self::id::{DatabaseId, RequestId};

pub mod nodetable;
pub use self::nodetable::{NodeTable, KNodeTable};

pub mod datastore;
pub use self::datastore::{Datastore};

pub mod search;
pub use self::search::{Search};

pub mod dht;
pub use self::dht::Dht;

pub mod connection;
pub use connection::ConnectionManager;

#[cfg(test)]
pub mod mock;


#[derive(PartialEq, Clone, Debug)]
pub struct Config {
    /// Length of the hash used (in bits) 
    pub hash_size: usize,
    /// Size of buckets and number of nearby nodes to consider when searching
    pub k: usize,
    /// Number of concurrent operations to be performed at once (also known as α or alpha)
    pub concurrency: usize,
    /// Maximum recursion depth for searches
    pub max_recursion: usize,
    /// Timeout period for network operations 
    pub timeout: Duration,
}

impl Default for Config {
    fn default() -> Config {
            Config {
                hash_size: 512, 
                k: 20, 
                concurrency: 3,
                max_recursion: 10,
                timeout: Duration::from_secs(3)
            }
    }
}


#[cfg(test)]
mod tests {
    use std::clone::Clone;

    use super::*;
    use crate::datastore::{HashMapStore};
    use crate::mock::{MockTransaction, MockConnector};

    #[test]
    fn test_connect() {
        let n1 = Node::new(0b0001, 100);
        let n2 = Node::new(0b0010, 200);
        let n3 = Node::new(0b0011, 300);
        let n4 = Node::new(0b1000, 400);

        // Build expectations
        let connector = MockConnector::from(vec![
            // First transaction to bootstrap onto the network
            MockTransaction::<_, _, u64>::new(n2.clone(), Request::FindNode(n1.id().clone()), 
                    n1.clone(), Response::NodesFound(vec![n3.clone(), n4.clone()]), None),

            //bootsrap to found nodes
            MockTransaction::<_, _, u64>::new(n3.clone(), Request::FindNode(n1.id().clone()), 
                    n1.clone(), Response::NodesFound(vec![]), None),
            MockTransaction::<_, _, u64>::new(n4.clone(), Request::FindNode(n1.id().clone()), 
                    n1.clone(), Response::NodesFound(vec![]), None),
        ]);

        // Create configuration
        let mut config = Config::default();
        config.concurrency = 2;

        let knodetable = KNodeTable::new(&n1, 2, 4);
        
        // Instantiated DHT
        let store: HashMapStore<u64, u64> = HashMapStore::new();
        let mut dht = Dht::<u64, u64, _, _, _, _>::new(n1.id().clone(), n1.address().clone(), 
                config, knodetable, connector.clone(), store);
    
        // Attempt initial bootstrapping
        dht.connect(n2.clone()).wait().unwrap();

        // Check bootstrapped node is added to db
        assert_eq!(Some(n2.clone()), dht.contains(n2.id()));

        // Check Reported nodes are added
        assert_eq!(Some(n3.clone()), dht.contains(n3.id()));
        assert_eq!(Some(n4.clone()), dht.contains(n4.id()));

        // Check expectations are done
        connector.done();
    }

   #[test]
    fn test_lookup() {
        let n1 = Node::new(0b1000, 100);
        let n2 = Node::new(0b0011, 200);
        let n3 = Node::new(0b0010, 300);
        let n4 = Node::new(0b1001, 400);
        let n5 = Node::new(0b1010, 400);

        // Build expectations
        let connector = MockConnector::from(vec![
            // First transaction to bootstrap onto the network
            MockTransaction::<_, _, u64>::new(n2.clone(), Request::FindNode(n4.id().clone()), 
                    n1.clone(), Response::NodesFound(vec![n4.clone()]), None),
            MockTransaction::<_, _, u64>::new(n3.clone(), Request::FindNode(n4.id().clone()), 
                    n1.clone(), Response::NodesFound(vec![n5.clone()]), None),

            // Second iteration
            MockTransaction::<_, _, u64>::new(n4.clone(), Request::FindNode(n4.id().clone()), 
                    n1.clone(), Response::NodesFound(vec![]), None),
            MockTransaction::<_, _, u64>::new(n5.clone(), Request::FindNode(n4.id().clone()), 
                    n1.clone(), Response::NodesFound(vec![]), None),
        ]);

        // Create configuration
        let mut config = Config::default();
        config.concurrency = 2;

        let mut knodetable = KNodeTable::new(&n1, 2, 4);
        
        // Inject initial nodes into the table
        knodetable.update(&n2);
        knodetable.update(&n3);

        // Instantiated DHT
        let store: HashMapStore<u64, u64> = HashMapStore::new();
        let mut dht = Dht::<u64, u64, _, _, _, _>::new(n1.id().clone(), n1.address().clone(), 
                config, knodetable, connector.clone(), store);

        // Perform search
        dht.lookup(n4.id().clone()).wait().expect("lookup failed");

        connector.done();
    }

}
