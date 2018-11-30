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

use futures::{future, Future};
use futures::{Stream};

use futures_timer::{FutureExt};

use crate::{Config, Node, DatabaseId, NodeTable, ConnectionManager, DhtError};

use crate::{Request, Response, Message};

use crate::datastore::Datastore;

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

#[derive(Clone, Debug, PartialEq)]
enum RequestState {
    Pending,
    Active,
    Timeout,
    Complete,
}

impl <ID, ADDR, DATA, TABLE, CONN, STORE> Dht<ID, ADDR, DATA, TABLE, CONN, STORE> 
where 
    ID: DatabaseId + 'static,
    ADDR: Clone + Debug + 'static,
    DATA: Clone + 'static,
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

    pub fn lookup(&mut self, addr: &ID) -> impl Future<Item=Node<ID, ADDR>, Error=DhtError> {
        let known = Arc::new(Mutex::new(HashMap::<ID, (Node<ID, ADDR>, RequestState)>::new()));
        let table = self.table.clone();
        let addr = addr.clone();

        // Fetch known nearest nodes
        let nearest = table.lock().unwrap().nearest(&addr, self.config.concurrency);
        {
            let mut k = known.lock().unwrap();
            for n in nearest {
                k.insert(n.id().clone(), (n, RequestState::Pending));
            }
            drop(k);
        }

        // Fetch a section of known nodes to process
        let mut chunk: Vec<_> = known.lock().unwrap().clone().iter()
                .filter(|(_k, (_n, s))| *s == RequestState::Pending )
                .map(|(_k, (n, _s))| n.clone() ).collect();
        chunk.sort_by_key(|n| ID::xor(&addr, n.id()) );

        // Update nodes in the chunk to active state
        {
            let mut k = known.lock().unwrap();
            for target in &chunk {
                // Send a FindNodes query
                println!("Sending FindNodes to '{:?}'", target.id());
                k.entry(target.id().clone()).and_modify(|(_n, s)| *s = RequestState::Active );
            }
        }

        // Send requests to nodes in the chunk
        let k = known.clone();
        self.request(&Request::FindNode(addr.clone()), &chunk)
        .map(move |res| {
            for (n, v) in &res {
                // Handle received responses
                if let Some(resp) = v {
                    match resp {
                        Response::NodesFound(nodes) => {
                            // Add nodes to known list
                            let mut known = k.lock().unwrap();
                            for n in nodes {
                                known.entry(n.id().clone()).or_insert((n.clone(), RequestState::Pending));
                            }
                        },
                        _ => { },
                    }

                    // Update node state to completed
                    k.lock().unwrap().entry(n.id().clone()).and_modify(|(_n, s)| *s = RequestState::Complete );
                } else {
                    // Update node state to timed out
                    k.lock().unwrap().entry(n.id().clone()).and_modify(|(_n, s)| *s = RequestState::Timeout );
                }
            }
        }).then(move |_| {
            let k = known.lock().unwrap();
            // See whether a node has been found
            if let Some((node, _)) = k.get(&addr) {
                Ok(node.clone())
            } else {
                Err(DhtError::NotFound)
            }
        })
    }

    pub fn request(&mut self, req: &Request<ID, ADDR>, nodes: &[Node<ID, ADDR>]) -> 
            impl Future<Item=Vec<(Node<ID, ADDR>, Option<Response<ID, ADDR, DATA>>)>, Error=DhtError> {
        let mut queries = Vec::new();

        for n in nodes {
            let n1 = n.clone();
            let n2 = n.clone();
            let q = self.conn_mgr.clone().request(n, req.clone())
                .timeout(self.config.ping_timeout)
                .map(|v| (n1, Some(v)) )
                .or_else(|e| {
                    if e == DhtError::Timeout {
                        Ok((n2, None))
                    } else {
                        Err(e)
                    }
                } );
            queries.push(q);
        }

        future::join_all(queries)
    }

    /// Refresh node table
    pub fn refresh(&mut self) -> impl Future<Item=(), Error=DhtError> {
        // TODO: send refresh to buckets that haven't been looked up recently

        // TODO: evict "expired" nodes from buckets

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
                let nodes = self.table.lock().unwrap().nearest(id, self.config.k);
                Response::NodesFound(nodes)
            },
            Request::FindValue(id) => {
                // Lockup the value
                if let Some(values) = self.datastore.lock().unwrap().find(id) {
                    Response::ValuesFound(values)
                } else {
                    let nodes = self.table.lock().unwrap().nearest(id, self.config.k);
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