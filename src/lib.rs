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
    pub ping_timeout: Duration,
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock::{MockTransaction, MockConnector};

    #[test]
    fn test_dht() {
        let n1 = Node::new(0, 100);
        let n2 = Node::new(1, 200);

        // Build expectations
        let connector = MockConnector::from(vec![
            MockTransaction::<_, _, u64>::new(n2.clone(), Request::FindNode(n1.id().clone()), 
                    n1.clone(), Response::NodesFound(vec![]), None),
        ]);

        // Create configuration
        let config = Config{ping_timeout: Duration::from_secs(10)};
        let knodetable = KNodeTable::new(&n1, 2, 4);
        
        // Instantiated DHT
        let mut dht = Dht::<u64, u64, u64, _, _>::new(n1.id().clone(), n2.id().clone(), 
                config, knodetable, connector.clone());
    
        // Attempt initial bootstrapping
        dht.connect(n2.clone()).wait().unwrap();

        // Find bootstrapped node
        assert_eq!(Some(n2.clone()), dht.contains(n2.id()));

        // Check expectations are done
        connector.done();
    }


}
