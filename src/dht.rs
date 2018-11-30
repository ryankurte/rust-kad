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

    pub fn lookup(&mut self, target: ID) -> impl Future<Item=Node<ID, ADDR>, Error=DhtError> {

        let mut known = HashMap::<ID, (Node<ID, ADDR>, RequestState)>::new();
        let mut data = HashMap::<ID, Vec<DATA>>::new();

        let table = self.table.clone();
        let depth_limit = 4;

        // Fetch known nearest nodes
        let nearest = table.lock().unwrap().nearest(&target, self.config.concurrency);
        for n in nearest {
            known.insert(n.id().clone(), (n, RequestState::Pending));
        }

        self.recurse(target.clone(), depth_limit, Request::FindNode(target.clone()), known, data)
        .then(|v| self.check_find_nodes(v) )
    }

    fn check_find_nodes(&self, r: Result<(ID, Request<ID, ADDR>, usize, KnownMap<ID, ADDR>, ValueMap<ID, DATA>), DhtError>) 
        -> impl Future<Item=Node<ID, ADDR>, Error=DhtError> {
        // Abort on error
        let (id, req, depth, known, data) = match r {
            Ok(v) => v,
            Err(e) => return future::err(e),
        };

        // Success if node has been found
        if let Some((node, _)) = known.get(&id) {
            return future::ok(node.clone());
        }

        // Recurse again if not
        if depth > 0 {
            self.recurse(id, depth - 1, req, known, data).then(|v| self.check_find_nodes(v) );
        }

        // Error if we hit the recursion limit and have not succeeded
        future::err(DhtError::NotFound)
    }

    fn recurse(&self, id: ID, depth: usize, req: Request<ID, ADDR>, known: KnownMap<ID, ADDR>, data: ValueMap<ID, DATA>) -> 
            impl Future<Item=(ID, Request<ID, ADDR>, usize, KnownMap<ID, ADDR>, ValueMap<ID, DATA>), Error=DhtError> {

        let mut known = known.clone();
        let mut data = data.clone();

        // Fetch a section of known nodes to process
        let mut chunk: Vec<_> = known.iter()
                .filter(|(_k, (_n, s))| *s == RequestState::Pending )
                .map(|(_k, (n, _s))| n.clone() ).collect();
        chunk.sort_by_key(|n| ID::xor(&id, n.id()) );

        // Update nodes in the chunk to active state
        for target in &chunk {
            known.entry(target.id().clone()).and_modify(|(_n, s)| *s = RequestState::Active );
        }

        // Send requests and handle responses
        self.request(&req, &chunk)
        .map(move |res| {
            for (n, v) in &res {
                // Handle received responses
                if let Some(resp) = v {
                    match resp {
                        Response::NodesFound(nodes) => {
                            // Add nodes to known list
                            for n in nodes {
                                known.entry(n.id().clone()).or_insert((n.clone(), RequestState::Pending));
                            }
                        },
                        Response::ValuesFound(values) => {
                            // Add data to data list
                            data.insert(n.id().clone(), values.clone());
                        },
                        Response::NoResult => { },
                    }

                    // Update node state to completed
                    known.entry(n.id().clone()).and_modify(|(_n, s)| *s = RequestState::Complete );
                } else {
                    // Update node state to timed out
                    known.entry(n.id().clone()).and_modify(|(_n, s)| *s = RequestState::Timeout );
                }
            }
            (id, req, depth, known, data)
        })
    }

    fn request(&mut self, req: &Request<ID, ADDR>, nodes: &[Node<ID, ADDR>]) -> 
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

pub struct Search <ID, ADDR, DATA, CONN> {
    known: Arc<Mutex<HashMap<ID, (Node<ID, ADDR>, RequestState)>>>,
    data: Arc<Mutex<HashMap<ID, Vec<DATA>>>>,
    conn: CONN,
    depth: usize,
}

impl <ID, ADDR, DATA, CONN> Search <ID, ADDR, DATA, CONN> 
where
    ID: DatabaseId + 'static,
    ADDR: Clone + Debug + 'static,
    DATA: Clone + 'static,
    CONN: ConnectionManager<ID, ADDR, DATA, DhtError> + Clone + 'static,
{
    pub fn new(id: ID, depth: usize, nearest: &[Node<ID, ADDR>], conn: CONN) -> Search<ID, ADDR, DATA, CONN> {
        let mut known = Arc::new(Mutex::new(HashMap::<ID, (Node<ID, ADDR>, RequestState)>::new()));
        let mut data = Arc::new(Mutex::new(HashMap::<ID, Vec<DATA>>::new()));

        // Add known nearest nodes
        {
            let k = known.lock().unwrap();
            for n in nearest {
                k.insert(n.id().clone(), (n.clone(), RequestState::Pending));
            }
        }

        Search{known, depth, data, conn}
    }
}

impl <ID, ADDR, DATA, CONN> Future for Search <ID, ADDR, DATA, CONN> 
where
    ID: DatabaseId + 'static,
    ADDR: Clone + Debug + 'static,
    DATA: Clone + 'static,
    CONN: ConnectionManager<ID, ADDR, DATA, DhtError> + Clone + 'static,
{
    type Item = ();
    type Error = DhtError;

    fn poll(&mut self) -> std::result::Result<futures::Async<Self::Item>, Self::Error>{

        // Fetch a section of known nodes to process
        let mut chunk: Vec<_> = known.iter()
                .filter(|(_k, (_n, s))| *s == RequestState::Pending )
                .map(|(_k, (n, _s))| n.clone() ).collect();
        chunk.sort_by_key(|n| ID::xor(&id, n.id()) );

        // Update nodes in the chunk to active state
        for target in &chunk {
            known.entry(target.id().clone()).and_modify(|(_n, s)| *s = RequestState::Active );
        }

        Err(DhtError::Unimplemented)
    }
}

type KnownMap<ID, ADDR> = HashMap<ID, (Node<ID, ADDR>, RequestState)>;
type ValueMap<ID, DATA> = HashMap<ID, Vec<DATA>>;
