#![feature(fn_traits)]
#![feature(unboxed_closures)]

use std::time::SystemTime;
use std::marker::{PhantomData, Sized};

extern crate futures;
use futures::{future, Future};
use futures::{stream, Stream};
use futures::{sink, Sink};

extern crate num;

pub mod error;
pub use self::error::Error as DhtError;
pub mod message;
pub use self::message::{Message, RequestMessage, ResponseMessage};
pub mod node;
pub use self::node::Node;
pub mod id;
pub mod kbucket;
pub use self::kbucket::KBucket;


pub trait ConnectionManager<ID, ADDR, DATA> {
    fn connect(&mut self, node_id: ID, addr: ADDR) -> Peer<ID, ADDR>;
    fn request(&mut self, req: RequestMessage<ID, ADDR>) -> Future<Item=ResponseMessage<ID, ADDR, DATA>, Error=()>;
    fn disconnect(&mut self, node_id: ID);
}


#[derive(PartialEq, Clone, Debug)]
pub struct Peer<ID, ADDR> {
    id: ID,
    addr: ADDR,
    seen: SystemTime,
    frozen: bool,
}

#[derive(PartialEq, Clone, Debug)]
pub struct Dht<ID, ADDR, DATA, KBUCKET, CONNECTION> {
    this: Node<ID, ADDR>,
    k_bucket: KBUCKET,
    conn_mgr: CONNECTION,
    data: PhantomData<DATA>,
}


impl <ID, ADDR, DATA, KBUCKET, CONNECTION> Dht<ID, ADDR, DATA, KBUCKET, CONNECTION> 
where 
    ID: 'static,
    ADDR: 'static,
    DATA: 'static,
    KBUCKET: KBucket<ID, ADDR>,
    CONNECTION: ConnectionManager<ID, ADDR, DATA>,
{
    pub fn new(id: ID, addr: ADDR, k_bucket: KBUCKET, conn_mgr: CONNECTION) -> Dht<ID, ADDR, DATA, KBUCKET, CONNECTION> {
        Dht{this: Node{id, addr}, k_bucket, conn_mgr, data: PhantomData}
    }

    pub fn store(&mut self, _id: ID, _data: DATA) -> impl Future<Item=(), Error=DhtError> {
        future::err(DhtError::Unimplemented)
    }

    pub fn find(&mut self, _id: ID) -> impl Future<Item=DATA, Error=DhtError> {
        future::err(DhtError::Unimplemented)
    }

    pub fn receive(&mut self, from: ID, m: &Message<ID, ADDR, DATA>) {
        match m {
            Message::Request(req) => {

            },
            Message::Response(resp) => {
                // Update k-bucket for responder
                if !self.k_bucket.update(&resp.responder) {
                    if let Some(oldest) = self.k_bucket.peek_oldest(&resp.responder.id) {
                        // Ping oldest
                        self.conn_mgr.request()
                        // On failure, evict and replace
                        // On success, ignore new
                    }
                }
            }
        };
    }

    pub fn update(&mut self) {

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


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }

    #[test]
    #[ignore]
    fn receive_message_updates_k_bucket() {
        // If node exists, move to tail of the list

        // If the node isn't in the bucket, and 
    }


}
