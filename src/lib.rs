#![feature(fn_traits)]
#![feature(unboxed_closures)]
#![feature(extern_crate_item_prelude)]

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

pub mod dht;
pub use self::dht::Dht;

#[cfg(test)]
pub mod mock;

pub trait ConnectionManager<ID, ADDR, DATA, ERR> 
where
    ERR: From<std::io::Error>,
{
    fn request(&mut self, to: &Node<ID, ADDR>, req: Request<ID, ADDR>) -> 
            Box<Future<Item=(Node<ID, ADDR>, Response<ID, ADDR, DATA>), Error=ERR>>;
}

#[derive(PartialEq, Clone, Debug)]
pub struct Config {
    pub k: usize,
    pub ping_timeout: Duration,
}


#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::clone::Clone;

    use super::*;
    use crate::mock::{MockTransaction, MockConnector};

    #[test]
    fn test_dht() {
        let n1 = Node::new(0b0001, 100);
        let n2 = Node::new(0b0010, 200);
        let n3 = Node::new(0b0011, 300);
        let n4 = Node::new(0b1000, 400);
        let n5 = Node::new(0b1001, 500);

        // Build expectations
        let connector = MockConnector::from(vec![
            // First transaction to bootstrap onto the network
            MockTransaction::<_, _, u64>::new(n2.clone(), Request::FindNode(n1.id().clone()), 
                    n1.clone(), Response::NodesFound(vec![n3.clone(), n4.clone()]), None),

        ]);

        // Create configuration
        let config = Config{k: 2, ping_timeout: Duration::from_secs(10)};
        let knodetable = KNodeTable::new(&n1, 2, 4);
        
        // Instantiated DHT
        let mut store: HashMap<u64, u64> = HashMap::new();
        let mut dht = Dht::<u64, u64, u64, _, _, _>::new(n1.id().clone(), n2.id().clone(), 
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


}
