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

use futures::prelude::*;
use futures::future;
use futures::future::{FutureResult, Loop};

use futures_timer::{FutureExt};

use crate::{Config, Node, DatabaseId, NodeTable, ConnectionManager, DhtError};
use crate::{Request, Response, Message};

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
pub struct Search <ID, ADDR, DATA, CONN> {
    target: ID,
    op: Operation,
    concurrency: usize,
    depth: usize,
    known: HashMap<ID, (Node<ID, ADDR>, RequestState)>,
    data: HashMap<ID, Vec<DATA>>,
    conn: CONN,
}

pub type KnownMap<ID, ADDR> = HashMap<ID, (Node<ID, ADDR>, RequestState)>;
pub type ValueMap<ID, DATA> = HashMap<ID, Vec<DATA>>;

impl <ID, ADDR, DATA, CONN> Search <ID, ADDR, DATA, CONN> 
where
    ID: DatabaseId + 'static,
    ADDR: Clone + Debug + 'static,
    DATA: Clone + Debug + 'static,
    CONN: ConnectionManager<ID, ADDR, DATA, DhtError> + Clone + 'static,
{
    pub fn new(target: ID, op: Operation, depth: usize, concurrency: usize, nearest: &[Node<ID, ADDR>], conn: CONN) -> Search<ID, ADDR, DATA, CONN> {
        let mut known = HashMap::<ID, (Node<ID, ADDR>, RequestState)>::new();
        let data = HashMap::<ID, Vec<DATA>>::new();

        println!("Creating search for '{:?}' via: {:?}", target, nearest);

        // Add known nearest nodes
        for n in nearest {
            known.insert(n.id().clone(), (n.clone(), RequestState::Pending));
        }

        Search{target, op, depth, concurrency, known, data, conn}
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

    pub fn depth(&self) -> usize {
        self.depth
    }

    /// Fetch pending nodes that're known.
    pub fn pending(&self) -> Vec<Node<ID, ADDR>> {
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
    pub fn execute(mut self) -> impl Future<Item=Self, Error=DhtError> {

        // Fetch a section of known nodes to process
        let chunk = &self.pending()[0..self.concurrency];

        println!("Executing search iteration over: {:?}", chunk);

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
                        println!("Handling response from: {:?}", n.id());
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
            }
            self.depth -= 1;

            self
        })
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
    use crate::datastore::{HashMapStore, Datastore};
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
        let n4 = &nodes[4];

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
            

            // TODO: search for nodes for value
        ]);

        let mut s = Search::new(target.id().clone(), Operation::FindNode, 2, 2, &nodes[0..2], connector.clone());

        s = s.execute().wait().unwrap();

        s = s.execute().wait().unwrap();

        connector.done();
    }
}