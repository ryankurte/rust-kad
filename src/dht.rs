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
use std::collections::HashMap;
use std::time::Duration;

use futures::{future, Future, Stream};
use futures::future::{Loop, loop_fn};

use futures_timer::{FutureExt};

use crate::{Config, Node, DatabaseId, NodeTable, ConnectionManager, DhtError};

use crate::{Request, Response, Message};

use crate::datastore::Datastore;
use crate::search::{Search, Operation};

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
    DATA: Clone + Debug + 'static,
    TABLE: NodeTable<ID, ADDR> + 'static,
    STORE: Datastore<ID, DATA> + 'static,
    CONN: ConnectionManager<ID, ADDR, DATA, DhtError> + Clone + 'static,
{
    pub fn new(id: ID, addr: ADDR, config: Config, table: TABLE, conn_mgr: CONN, datastore: STORE) -> Dht<ID, ADDR, DATA, TABLE, CONN, STORE> {
        Dht{id, addr, config, table: Arc::new(Mutex::new(table)), conn_mgr, datastore: Arc::new(Mutex::new(datastore)), data: PhantomData}
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
        self.table.lock().unwrap().contains(id)
    }

    /// Connect to a known node
    /// This is used for bootstrapping onto the DHT
    pub fn connect(&mut self, target: Node<ID, ADDR>) -> impl Future<Item=Node<ID, ADDR>, Error=DhtError> {
        let table = self.table.clone();
        // Launch request
        self.conn_mgr.clone().request(&target, Request::FindNode(self.id.clone()))
        .timeout(self.config.ping_timeout)
        .map(move |resp| {

            // Handle response
            match resp {
                Response::NodesFound(nodes) => {
                    println!("[DHT connect] NodesFound from: {:?}", target.id());

                    // Update responding node
                    table.lock().unwrap().update(&target);

                    // Process responses
                    for n in nodes {
                        table.lock().unwrap().update(&n);
                    }
                },
                _ => {
                    println!("[DHT connect] invalid response from: {:?}", target.id());
                    //return future::err(DhtError::InvalidResponse)
                },
            }
           
            // Return connected node
            target
        })
    }

    /// Look up a node in the database by ID
    pub fn lookup(&mut self, target: ID) -> impl Future<Item=Node<ID, ADDR>, Error=DhtError> + '_ {

        // Create a search instance
        let search = Search::new(target.clone(), Operation::FindNode, 2, 2, |t, k, _| { k.get(t).is_some() }, self.table.clone(), self.conn_mgr.clone());

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

    /// Refresh node table
    pub fn refresh(&mut self) -> impl Future<Item=(), Error=DhtError> {
        // TODO: send refresh to buckets that haven't been looked up recently
        // How to track recently looked up / contacted buckets..?

        // TODO: evict "expired" nodes from buckets
        // Message oldest node, if no response evict
        // Maybe this could be implemented as a periodic ping and timeout instead?

        future::err(DhtError::Unimplemented)
    }


    fn update_node(&mut self, node: Node<ID, ADDR>) {
        let table = self.table.clone();
        let mut unlocked = table.lock().unwrap();

        // Update k-bucket for responder
        if !unlocked.update(&node) {
            // If the bucket is full and does not contain the node to be updated
            if let Some(oldest) = unlocked.peek_oldest(node.id()) {
                // Ping oldest
                self.conn_mgr.request(&oldest, Request::Ping).timeout(self.config.ping_timeout)
                .map(|_message| {
                    // On successful ping, ignore new node
                    table.lock().unwrap().update(&oldest);
                    // TODO: should the response to a ping be handled?
                }).map_err(|_err| {
                    // On timeout, replace oldest node with new
                    table.lock().unwrap().replace(&oldest, &node);
                }).wait().unwrap();
            }
        }
    }

    pub fn receive(&mut self, from: &Node<ID, ADDR>, req: &Request<ID, ADDR>) -> Response<ID, ADDR, DATA> {
        // Update record for sender
        self.table.lock().unwrap().update(from);

        // Build response
        match req {
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
                
                Response::NoResult
            },
        }
    }

}

/// Stream trait implemented to allow polling on dht object
impl <ID, ADDR, DATA, TABLE, CONN, STORE> Stream for Dht <ID, ADDR, DATA, TABLE, CONN, STORE> {
    type Item = ();
    type Error = DhtError;

    fn poll(&mut self) -> Result<futures::Async<Option<Self::Item>>, Self::Error> {
        Err(DhtError::Unimplemented)
    }
}




