/**
 * rust-kad
 * Kademlia search implementation
 * This is used to find nodes and values in the database
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */

use std::collections::HashMap;
use std::fmt::Debug;
use std::time::Duration;
use std::sync::{Arc, Mutex};

use futures::prelude::*;
use futures::future;
use futures::future::{Loop};

use futures_timer::{FutureExt};

use crate::{Node, DatabaseId, NodeTable, ConnectionManager, DhtError};
use crate::{Request, Response};

/// Search describes DHT search operations
#[derive(Clone, Debug, PartialEq)]
pub enum Operation {
    FindNode,
    FindValue,
}

/// RequestState is used to store the state of pending requests
#[derive(Clone, Debug, PartialEq)]
pub enum RequestState {
    Pending,
    Active,
    Timeout,
    Complete,
}

/// Search object provides the basis for executing searches on the DHT
pub struct Search <ID, ADDR, DATA, TABLE, CONN, DONE> {
    target: ID,
    op: Operation,
    k: usize,
    concurrency: usize,
    depth: usize,
    done: DONE,
    table: Arc<Mutex<TABLE>>,
    known: HashMap<ID, (Node<ID, ADDR>, RequestState)>,
    data: HashMap<ID, Vec<DATA>>,
    conn: CONN,
}

pub type KnownMap<ID, ADDR> = HashMap<ID, (Node<ID, ADDR>, RequestState)>;
pub type ValueMap<ID, DATA> = HashMap<ID, Vec<DATA>>;

impl <ID, ADDR, DATA, TABLE, CONN, DONE> Search <ID, ADDR, DATA, TABLE, CONN, DONE> 
where
    ID: DatabaseId + 'static,
    ADDR: Clone + Debug + 'static,
    DATA: Clone + Debug + 'static,
    DONE: Fn(&ID, &KnownMap<ID, ADDR>, &ValueMap<ID, DATA>) -> bool + 'static,
    TABLE: NodeTable<ID, ADDR> + 'static,
    CONN: ConnectionManager<ID, ADDR, DATA, DhtError> + Clone + 'static,
{
    pub fn new(target: ID, op: Operation, k: usize, depth: usize, concurrency: usize, table: Arc<Mutex<TABLE>>, conn: CONN, done: DONE) 
        -> Search<ID, ADDR, DATA, TABLE, CONN, DONE> {
        let known = HashMap::<ID, (Node<ID, ADDR>, RequestState)>::new();
        let data = HashMap::<ID, Vec<DATA>>::new();

        Search{target, op, k, depth, concurrency, known, done, table, data, conn}
    }

    /// Fetch a pointer to the search target ID
    pub fn target(&self) -> &ID {
        &self.target
    }

    /// Fetch a copy of the Known Nodes map
    pub fn known(&self) -> KnownMap<ID, ADDR> {
        self.known.clone()
    }

    /// Fetch a copy of the Received Data map
    pub fn data(&self) -> ValueMap<ID, DATA> {
        self.data.clone()
    }

    pub fn execute(self) -> impl Future<Item=Self, Error=DhtError> 
    {
        // Execute recursive search
        future::loop_fn(self, |s1| {
            s1.recurse().map(|mut s| {
                let concurrency = s.concurrency;
                let k = s.k;

                // Exit if search is complete
                if (s.done)(&s.target, &s.known, &s.data) {
                    println!("[search] break, done");
                    return Loop::Break(s);
                }
                
                // Exit at max recursive depth
                if s.depth == 0 {
                    println!("[search] break, reached max recursive depth");
                    return Loop::Break(s);
                }

                // Exit once we've got no more pending in the first k closest known nodes
                let mut known: Vec<_> = s.known.iter()
                        .map(|(key, (_node, status))| (key.clone(), status.clone()) )
                        .collect();
                known.sort_by_key(|(key, _status)| ID::xor(s.target(), key) );
                let pending = &known[0..usize::min(known.len(), k)].iter()
                        .find(|(_key, status)| *status == RequestState::Pending);
                if pending.is_none() {
                    println!("[search] break, found k closest nodes");
                    return Loop::Break(s);
                }

                // If no nodes are pending, add another set of nearest nodes
                let pending = s.pending().len();
                
                if pending == 0 {
                    let nearest: Vec<_> = s.table.lock().unwrap().nearest(s.target(), concurrency..concurrency*2);
                    s.seed(&nearest);
                }
                
                // Continue otherwise
                Loop::Continue(s)
            })
        })
    }

    /// Fetch pending nodes that're known.
    fn pending(&self) -> Vec<Node<ID, ADDR>> {
        let mut chunk: Vec<_> = self.known.iter()
                .filter(|(_k, (_n, s))| *s == RequestState::Pending )
                .map(|(_k, (n, _s))| n.clone() ).collect();
        chunk.sort_by_key(|n| ID::xor(&self.target, n.id()) );
        chunk
    }

    /// Execute a single search round.
    /// This sends messages to a the closest N pending nodes (where N is the concurrency value)
    /// And collects the results into the known node and data maps.
    ///
    /// This is intended to be called using loop_fn for recursion.
    pub fn recurse(mut self) -> impl Future<Item=Self, Error=DhtError> {

        // Fetch a section of known nodes to process
        let pending = self.pending();
        let chunk = &pending[0..usize::min(self.concurrency, pending.len())];

        println!("Executing search iteration {:?} over: {:?}", self.depth, chunk);

        // Update nodes in the chunk to active state
        for target in chunk {
            self.known.entry(target.id().clone()).and_modify(|(_n, s)| *s = RequestState::Active );
        }

        let req = match self.op {
            Operation::FindNode => Request::FindNode(self.target.clone()),
            Operation::FindValue => Request::FindValue(self.target.clone()),
        };

        // Send requests and handle responses
        self.request(&req, chunk)
        .map(move |res| {
            for (n, v) in &res {
                // Handle received responses
                if let Some(resp) = v {
                    match resp {
                        Response::NodesFound(nodes) => {
                            // Add nodes to known list
                            for n in nodes {
                                self.known.entry(n.id().clone()).or_insert((n.clone(), RequestState::Pending));
                            }
                        },
                        Response::ValuesFound(values) => {
                            // Add data to data list
                            self.data.insert(n.id().clone(), values.clone());
                        },
                        Response::NoResult => { },
                    }

                    // Update node state to completed
                    self.known.entry(n.id().clone()).and_modify(|(_n, s)| *s = RequestState::Complete );
                } else {
                    // Update node state to timed out
                    self.known.entry(n.id().clone()).and_modify(|(_n, s)| *s = RequestState::Timeout );
                }

                // Update node table
                self.table.lock().unwrap().update(&n);
            }
            // Update depth limit
            self.depth -= 1;

            self
        })
    }


    /// Seed the search with nearest nodes
    pub fn seed(&mut self, known: &[Node<ID, ADDR>]) {
        for n in known {
            self.known.entry(n.id().clone()).or_insert((n.clone(), RequestState::Pending));
        }
    }

    /// Send a request to a slice of nodes and collect the responses
    fn request(&self, req: &Request<ID, ADDR>, nodes: &[Node<ID, ADDR>]) -> 
            impl Future<Item=Vec<(Node<ID, ADDR>, Option<Response<ID, ADDR, DATA>>)>, Error=DhtError> {
        let mut queries = Vec::new();

        for n in nodes {
            println!("Sending request: '{:?}' to: '{:?}'", req, n.id());
            let n1 = n.clone();
            let n2 = n.clone();
            let q = self.conn.clone().request(n, req.clone())
                .timeout(Duration::from_secs(1))
                .map(|v| {
                    println!("Response: '{:?}' from: '{:?}'", v, n1.id());
                    (n1, Some(v)) 
                })
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
}


#[cfg(test)]
mod tests {
    use std::clone::Clone;

    use super::*;
    use crate::nodetable::{NodeTable, KNodeTable};
    use crate::mock::{MockTransaction, MockConnector};

    #[test]
    fn test_search_nodes() {
        let root = Node::new(0, 001);
        let target = Node::new(10, 600);

        let nodes = vec![
            Node::new(1, 100),
            Node::new(2, 200),
            Node::new(3, 300),
            Node::new(4, 400),
            Node::new(5, 500),
        ];

        // Build expectations
        let connector = MockConnector::from(vec![
            // First execution
            MockTransaction::<_, _, u64>::new(nodes[1].clone(), Request::FindNode(target.id().clone()), 
                    root.clone(), Response::NodesFound(vec![nodes[3].clone()]), None),
            MockTransaction::<_, _, u64>::new(nodes[0].clone(), Request::FindNode(target.id().clone()), 
                    root.clone(), Response::NodesFound(vec![nodes[2].clone()]), None),
            
            // Second execution
            MockTransaction::<_, _, u64>::new(nodes[2].clone(), Request::FindNode(target.id().clone()), 
                    root.clone(), Response::NodesFound(vec![target.clone()]), None),
            MockTransaction::<_, _, u64>::new(nodes[3].clone(), Request::FindNode(target.id().clone()), 
                    root.clone(), Response::NodesFound(vec![nodes[4].clone()]), None),
            
        ]);

        // Create search object
        let table = Arc::new(Mutex::new(KNodeTable::new(&root, 2, 8)));
        let mut s = Search::new(target.id().clone(), Operation::FindNode, 2, 2, 2, table.clone(), connector.clone(), |t, k, _| { k.get(t).is_some() });

        // Seed search with known nearest nodes
        s.seed(&nodes[0..2]);
        {
            // This should add the nearest nodes to the known map
            assert!(s.known.get(nodes[0].id()).is_some());
            assert!(s.known.get(nodes[1].id()).is_some());
            // But not to the node table until they have responded
            let t = table.lock().unwrap();
            assert!(t.contains(nodes[0].id()).is_none());
            assert!(t.contains(nodes[1].id()).is_none());
        }
        
        // Perform first iteration
        s = s.recurse().wait().unwrap();
        {
            // Responding nodes should be added to the node table
            let t = table.lock().unwrap();
            assert!(t.contains(nodes[0].id()).is_some());
            assert!(t.contains(nodes[1].id()).is_some());
            // Viable responses should be added to the known map
            assert!(s.known.get(nodes[2].id()).is_some());
            assert!(s.known.get(nodes[3].id()).is_some());
        }

        // Perform second iteration
        s = s.recurse().wait().unwrap();
        {
            // Responding nodes should be added to the node table
            let t = table.lock().unwrap();
            assert!(t.contains(nodes[2].id()).is_some());
            assert!(t.contains(nodes[3].id()).is_some());
            // Viable responses should be added to the known map
            assert!(s.known.get(nodes[4].id()).is_some());
            assert!(s.known.get(target.id()).is_some());
        }

        connector.done();
    }
}