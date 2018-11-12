#![feature(fn_traits)]
#![feature(unboxed_closures)]
#![feature(extern_crate_item_prelude)]

use std::sync::{Arc, Mutex};
use std::time::{SystemTime, Duration};
use std::marker::{PhantomData, Sized};

extern crate futures;
use futures::{future, Future};
use futures::{stream, Stream};
use futures::{sink, Sink};

extern crate dht;

extern crate futures_timer;
use futures_timer::{FutureExt, Delay};

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
pub use self::nodetable::NodeTable;


extern crate proc_macro;
extern crate syn;
#[macro_use]
extern crate quote;

pub trait ConnectionManager<ID, ADDR, DATA, ERR> 
where
    ERR: From<std::io::Error>,
{
    fn request(&mut self, to: &Node<ID, ADDR>, req: Request<ID, ADDR>) -> 
            Box<Future<Item=(Node<ID, ADDR>, Response<ID, ADDR, DATA>), Error=ERR>>;
}

#[derive(PartialEq, Clone, Debug)]
pub struct Config {
    ping_timeout: Duration,
}

#[derive(PartialEq, Clone, Debug)]
pub struct Peer<ID, ADDR> {
    id: ID,
    addr: ADDR,
    seen: SystemTime,
    frozen: bool,
}

#[derive(Clone, Debug)]
pub struct Dht<ID, ADDR, DATA, KBUCKET, CONNECTION> {
    id: ID,
    addr: ADDR,
    config: Config,
    k_bucket: Arc<Mutex<KBUCKET>>,
    conn_mgr: CONNECTION,
    data: PhantomData<DATA>,
}


impl <ID, ADDR, DATA, KBUCKET, CONNECTION> Dht<ID, ADDR, DATA, KBUCKET, CONNECTION> 
where 
    ID: DatabaseId + 'static,
    ADDR: Clone + 'static,
    DATA: Clone + 'static,
    KBUCKET: NodeTable<ID, ADDR>,
    CONNECTION: ConnectionManager<ID, ADDR, DATA, DhtError>,
{
    pub fn new(id: ID, addr: ADDR, config: Config, k_bucket: KBUCKET, conn_mgr: CONNECTION) -> Dht<ID, ADDR, DATA, KBUCKET, CONNECTION> {
        Dht{id, addr, config, k_bucket: Arc::new(Mutex::new(k_bucket)), conn_mgr, data: PhantomData}
    }

    pub fn store(&mut self, _id: ID, _data: DATA) -> impl Future<Item=(), Error=DhtError> {
        future::err(DhtError::Unimplemented)
    }

    pub fn find(&mut self, _id: ID) -> impl Future<Item=DATA, Error=DhtError> {
        future::err(DhtError::Unimplemented)
    }

    fn node(&self) -> Node<ID, ADDR> {
        Node{id: self.id.clone(), address: self.addr.clone()}
    }

    fn update_node(&mut self, node: Node<ID, ADDR>) {
        let k_bucket = self.k_bucket.clone();
        let mut unlocked = k_bucket.lock().unwrap();

        // Update k-bucket for responder
        if !unlocked.update(&node) {
            // If the bucket is full and does not contain the node to be updated
            if let Some(oldest) = unlocked.peek_oldest(&node.id) {
                // Ping oldest
                self.conn_mgr.request(&oldest, Request::Ping).timeout(self.config.ping_timeout)
                .map(|(node, _message)| {
                    // On successful ping, ignore new node
                    k_bucket.lock().unwrap().update(&node);
                    // TODO: should the response to a ping be handled?
                }).map_err(|_err| {
                    // On timeout, replace oldest node with new
                    k_bucket.lock().unwrap().replace(&oldest, &node);
                }).wait().unwrap();
            }
        }
    }

    pub fn receive(&mut self, from: ID, m: &Message<ID, ADDR, DATA>) {
        
        match m {
            Message::Request(req) => {

            },
            Message::Response(resp) => {
                // Update k-bucket for responder
                self.update_node(resp.responder.clone());


            }
        };
    }

}

/// Stream trait implemented to allow polling on dht object
impl <ID, ADDR, DATA, KBUCKET, CONNECTION> Stream for Dht <ID, ADDR, DATA, KBUCKET, CONNECTION> {
    type Item = ();
    type Error = DhtError;

    fn poll(&mut self) -> Result<futures::Async<Option<Self::Item>>, Self::Error> {
        Err(DhtError::Unimplemented)
    }
}

struct MockTransaction<ID, ADDR, VALUE> {
    to: Node<ID, ADDR>,
    request: Request<ID, ADDR>,
    from: Node<ID, ADDR>,
    response: Response<ID, ADDR, VALUE>,
    delay: Option<Duration>,
}

use std::collections::VecDeque;
use std::fmt::Debug;

struct MockConnector<ID, ADDR, VALUE> {
    transactions: Arc<Mutex<VecDeque<MockTransaction<ID, ADDR, VALUE>>>>,
}

impl <ID, ADDR, VALUE> From<Vec<MockTransaction<ID, ADDR, VALUE>>> for MockConnector<ID, ADDR, VALUE> {
    fn from(v: Vec<MockTransaction<ID, ADDR, VALUE>>) -> MockConnector<ID, ADDR, VALUE> {
        MockConnector{transactions: Arc::new(Mutex::new(v.into())) }
    }
}

impl <'a, ID, ADDR, VALUE, ERR> ConnectionManager <ID, ADDR, VALUE, ERR> for MockConnector <ID, ADDR, VALUE>
where
    ID: DatabaseId + 'static,
    ADDR: Debug + Copy + Clone + PartialEq + 'static,
    VALUE: Debug + Copy + Clone + PartialEq + 'static,
    ERR: From<std::io::Error> + 'static,
{
    fn request(&mut self, to: &Node<ID, ADDR>, req: Request<ID, ADDR>) -> 
            Box<Future<Item=(Node<ID, ADDR>, Response<ID, ADDR, VALUE>), Error=ERR>> {
        
        let transaction = self.transactions.lock().unwrap().pop_back().unwrap();

        assert_eq!(&transaction.to, to, "destination mismatch");
        assert_eq!(transaction.request, req, "request mismatch");

        if let Some(d) = transaction.delay {
            std::thread::sleep(d);
        }

        Box::new(future::ok((transaction.from, transaction.response)))
    }
}

#[cfg(test)]
mod tests {

    #[test]
    #[ignore]
    fn receive_message_updates_k_bucket() {
        // If node exists, move to tail of the list

        // If the node isn't in the bucket, and 
    }


}
