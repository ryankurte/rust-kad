

use std::sync::{Arc, Mutex};
use std::marker::{PhantomData};
use std::fmt::{Debug};

use futures::{future, Future};
use futures::{Stream};

use futures_timer::{FutureExt};

use crate::{Config, Node, DatabaseId, NodeTable, ConnectionManager, DhtError};
use crate::{Request, Response, Message};

#[derive(Clone, Debug)]
pub struct Dht<ID, ADDR, DATA, NODETABLE, CONNECTION> {
    id: ID,
    addr: ADDR,
    config: Config,
    k_bucket: Arc<Mutex<NODETABLE>>,
    conn_mgr: CONNECTION,
    data: PhantomData<DATA>,
}

impl <ID, ADDR, DATA, NODETABLE, CONNECTION> Dht<ID, ADDR, DATA, NODETABLE, CONNECTION> 
where 
    ID: DatabaseId + 'static,
    ADDR: Clone + Debug + 'static,
    DATA: Clone + 'static,
    NODETABLE: NodeTable<ID, ADDR> + 'static,
    CONNECTION: ConnectionManager<ID, ADDR, DATA, DhtError> + Clone + 'static,
{
    pub fn new(id: ID, addr: ADDR, config: Config, k_bucket: NODETABLE, conn_mgr: CONNECTION) -> Dht<ID, ADDR, DATA, NODETABLE, CONNECTION> {
        Dht{id, addr, config, k_bucket: Arc::new(Mutex::new(k_bucket)), conn_mgr, data: PhantomData}
    }

    /// Store a value in the DHT
    pub fn store(&mut self, _id: ID, _data: DATA) -> impl Future<Item=(), Error=DhtError> {
        future::err(DhtError::Unimplemented)
    }

    /// Find a value from the DHT
    pub fn find(&mut self, _id: ID) -> impl Future<Item=DATA, Error=DhtError> {
        future::err(DhtError::Unimplemented)
    }

    /// Find a node in the node table backing the DHT
    pub fn contains(&mut self, id: &ID) -> Option<Node<ID, ADDR>> {
        self.k_bucket.lock().unwrap().contains(id)
    }

    pub fn connect(&self, target: Node<ID, ADDR>) -> impl Future<Item=Node<ID, ADDR>, Error=DhtError> {
        let table = self.k_bucket.clone();
        // Launch request
        self.conn_mgr.clone().request(&target, Request::FindNode(self.id.clone())).map(move |(node, resp)| {
            // Update responding node
            table.lock().unwrap().update(&target);

            // Handle respoonse
            match resp {
                Response::NodesFound(_nodes) => {
                    info!("[DHT connect] to: {:?} from: {:?} nodes found", target, node);
                },
                Response::NoResult => {
                    info!("[DHT connect] to: {:?} from: {:?} no results", target, node);
                },
                Response::ValuesFound(_) => {
                    info!("[DHT connect] to {:?} from: {:?} invalid response", target, node);
                }
            }
            node
        })
    }

    fn update_node(&mut self, node: Node<ID, ADDR>) {
        let k_bucket = self.k_bucket.clone();
        let mut unlocked = k_bucket.lock().unwrap();

        // Update k-bucket for responder
        if !unlocked.update(&node) {
            // If the bucket is full and does not contain the node to be updated
            if let Some(oldest) = unlocked.peek_oldest(node.id()) {
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

    pub fn receive(&mut self, _from: ID, m: &Message<ID, ADDR, DATA>) {
        
        match m {
            Message::Request(_req) => {

            },
            Message::Response(resp) => {
                // Update k-bucket for responder
                self.update_node(resp.responder.clone());


            }
        };
    }

}

/// Stream trait implemented to allow polling on dht object
impl <ID, ADDR, DATA, NODETABLE, CONNECTION> Stream for Dht <ID, ADDR, DATA, NODETABLE, CONNECTION> {
    type Item = ();
    type Error = DhtError;

    fn poll(&mut self) -> Result<futures::Async<Option<Self::Item>>, Self::Error> {
        Err(DhtError::Unimplemented)
    }
}