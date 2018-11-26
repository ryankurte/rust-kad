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
        self.conn_mgr.clone().request(&target, Request::FindNode(self.id.clone())).map(move |(node, resp)| {
            // Handle response
            match resp {
                Response::NodesFound(nodes) => {
                    println!("[DHT connect] NodesFound from: {:?}", node.id());

                    // Update responding node
                    table.lock().unwrap().update(&target);

                    // Process responses
                    for n in nodes {
                        table.lock().unwrap().update(&n);
                    }
                },
                _ => {
                    println!("[DHT connect] invalid response from: {:?}", node.id());
                    //return future::err(DhtError::InvalidResponse)
                },
            }
           
            // Return connected node
            node
        })
    }

    pub fn lookup(&mut self, addr: &ID) -> impl Future<Item=Node<ID, ADDR>, Error=DhtError> {
        let known = Arc::new(Mutex::new(HashMap::<ID, (Node<ID, ADDR>, RequestState)>::new()));
        let table = self.table.clone();

        // Fetch known nearest nodes
        let nearest = table.lock().unwrap().nearest(addr, self.config.concurrency);
        let mut k = known.lock().unwrap();
        for n in nearest {
            k.insert(n.id().clone(), (n, RequestState::Pending));
        }
        drop(k);

        // Fetch a section of known nodes to process
        let mut chunk: Vec<_> = known.lock().unwrap().clone().iter()
                .filter(|(_k, (_n, s))| *s == RequestState::Pending )
                .map(|(_k, (n, _s))| n.clone() ).collect();
        chunk.sort_by_key(|n| ID::xor(addr, n.id()) );

        let mut queries = Vec::new();
        for target in &chunk {
            
            // Send a FindNodes query
            println!("Sending FindValues to '{:?}'", target.id());
            known.lock().unwrap().entry(target.id().clone()).and_modify(|(_n, s)| *s = RequestState::Pending );

            let k = known.clone();
            let t = table.clone();

            let q = self.conn_mgr.clone().request(target, Request::FindValue(addr.clone()))
                    .timeout(self.config.ping_timeout).map(move |(node, resp)| {

                t.lock().unwrap().update(&node);
                k.lock().unwrap().entry(target.id().clone()).and_modify(|(_n, s)| *s = RequestState::Complete );

                match resp {
                    Response::NoResult => {
                        
                    },
                    Response::NodesFound(nodes) => {
                        // Add nodes to known list
                        let mut known = k.lock().unwrap();
                        for n in nodes {
                            known.entry(n.id().clone()).or_insert((n, RequestState::Pending));
                        }
                    },
                    Response::ValuesFound(v) => {

                    }
                }
            }).map_err(|_e| {

            });

            queries.push(q);
        }

        future::err(DhtError::Unimplemented)
    }

    pub fn recurse(&mut self, req: &Request<ID, ADDR>, nodes: &[Node<ID, ADDR>]) -> Result<Response<ID, ADDR, DATA>, DhtError> {
        let mut queries = Vec::new();

        for n in nodes {
            let q = self.conn_mgr.clone().request(n, req.clone())
                    .timeout(self.config.ping_timeout).map(|(n, v)| {
                        println!("Recurse: received response from {:?}", n.id());
                        v
                    });
            queries.push(q);
        }

        future::join_all(queries).map(|r| {
            // Find any value responses
            let values: Vec<_> = r.iter().filter_map(|v| {
                if let Response::ValuesFound(vals) = v {
                    Some(vals)
                } else {
                    None
                }
            }).flatten().map(|v| v.clone()).collect();
            if values.len() != 0 {
                return Response::ValuesFound::<ID, ADDR, DATA>(values);
            }

            // Find any node responses
            let nodes: Vec<_> = r.iter().filter_map(|v| {
                if let Response::NodesFound(nodes) = v {
                    Some(nodes)
                } else {
                    None
                }
            }).flatten().map(|v| v.clone()).collect();
            if nodes.len() != 0 {
                return Response::NodesFound::<ID, ADDR, DATA>(nodes);
            }

            Response::NoResult
        }).map_err(|_e| {
            DhtError::Unimplemented
        }).wait()
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
                .map(|(node, _message)| {
                    // On successful ping, ignore new node
                    table.lock().unwrap().update(&node);
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