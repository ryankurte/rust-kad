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
use std::ops::Range;
use std::marker::PhantomData;

use futures::prelude::*;
use futures::future;
use futures::future::{Loop};

use crate::Config;
use crate::node::Node;
use crate::id::{DatabaseId, RequestId};
use crate::error::Error as DhtError;
use crate::nodetable::{NodeTable};
use crate::message::{Request, Response};


use rr_mux::{Connector};
use crate::connection::{request_all};

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
pub struct Search <Id, Addr, Data, Table, Conn, ReqId, Ctx> {
    origin: Id,
    target: Id,
    op: Operation,
    config: Config,
    depth: usize,
    table: Table,
    known: HashMap<Id, (Node<Id, Addr>, RequestState)>,
    data: HashMap<Id, Vec<Data>>,
    conn: Conn,
    ctx: Ctx,
    _req_id: PhantomData<ReqId>,
}

pub type KnownMap<Id, Addr> = HashMap<Id, (Node<Id, Addr>, RequestState)>;
pub type ValueMap<Id, Data> = HashMap<Id, Vec<Data>>;

impl <Id, Addr, Data, Table, Conn, ReqId, Ctx> Search <Id, Addr, Data, Table, Conn, ReqId, Ctx> 
where
    Id: DatabaseId + 'static,
    Addr: Clone + Debug + 'static,
    Data: Clone + Debug + 'static,
    Table: NodeTable<Id, Addr> + Clone + Sync + Send + 'static,
    ReqId: RequestId + 'static,
    Ctx: Clone + Debug + PartialEq + Send + 'static,
    Conn: Connector<ReqId, Node<Id, Addr>, Request<Id, Data>, Response<Id, Addr,Data>, DhtError, Ctx> + Clone + 'static,
{
    pub fn new(origin: Id, target: Id, op: Operation, config: Config, table: Table, conn: Conn, ctx: Ctx) 
        -> Search<Id, Addr,Data, Table, Conn, ReqId, Ctx> {
        let known = HashMap::<Id, (Node<Id, Addr>, RequestState)>::new();
        let data = HashMap::<Id, Vec<Data>>::new();

        let depth = config.max_recursion;

        Search{origin, target, op, config, depth, known, table, data, conn, ctx, _req_id: PhantomData}
    }

    /// Fetch a pointer to the search target Id
    pub fn target(&self) -> &Id {
        &self.target
    }

    /// Fetch a copy of the Known Nodes map
    pub fn known(&self) -> KnownMap<Id, Addr> {
        self.known.clone()
    }

    /// Fetch a copy of the Received Data map
    pub fn data(&self) -> ValueMap<Id, Data> {
        self.data.clone()
    }

    pub fn execute(self) -> impl Future<Item=Self, Error=DhtError> 
    {
        // Execute recursive search
        future::loop_fn(self, |s1| {
            s1.recurse().map(|mut s| {
                let concurrency = s.config.concurrency;
                let k = s.config.k;
                
                // Exit at max recursive depth
                if s.depth == 0 {
                    debug!("[search] break, reached max recursive depth");
                    return Loop::Break(s);
                }

                // Exit once we've got no more pending in the first k closest known nodes
                let mut known: Vec<_> = s.known.iter()
                        .map(|(key, (_node, status))| (key.clone(), status.clone()) )
                        .collect();
                known.sort_by_key(|(key, _status)| Id::xor(s.target(), key) );
                let pending = &known[0..usize::min(known.len(), k)].iter()
                        .find(|(_key, status)| *status == RequestState::Pending );
                if pending.is_none() {
                    debug!("[search] break, found k closest nodes");
                    return Loop::Break(s);
                }

                // If no nodes are pending, add another set of nearest nodes
                let pending = s.pending().len();
                
                if pending == 0 {
                    let mut table = s.table.clone();
                    let nearest: Vec<_> = table.nearest(s.target(), concurrency..concurrency*2);
                    s.seed(&nearest);
                }
                
                // Continue otherwise
                Loop::Continue(s)
            })
        })
    }

    /// Fetch pending known nodes ordered by distance
    fn pending(&self) -> Vec<Node<Id, Addr>> {
        let mut chunk: Vec<_> = self.known.iter()
                .filter(|(_k, (_n, s))| *s == RequestState::Pending )
                .map(|(_k, (n, _s))| n.clone() ).collect();
        chunk.sort_by_key(|n| Id::xor(&self.target, n.id()) );
        chunk
    }

    /// Fetch completed known nodes ordered by distance
    pub(crate) fn completed(&self, range: Range<usize>) -> Vec<Node<Id, Addr>> {
        let mut chunk: Vec<_> = self.known.iter()
                .filter(|(_k, (_n, s))| *s == RequestState::Complete )
                .map(|(_k, (n, _s))| n.clone() ).collect();
        chunk.sort_by_key(|n| Id::xor(&self.target, n.id()) );
        
        // Limit to count or total found
        let mut range = range;
        range.end = usize::min(chunk.len(), range.end);

        let filtered = chunk.drain(range).collect();
        filtered
    }

    /// Execute a single search round.
    /// This sends messages to a the closest N pending nodes (where N is the concurrency value)
    /// And collects the results into the known node and data maps.
    ///
    /// This is intended to be called using loop_fn for recursion.
    pub(crate) fn recurse(mut self) -> impl Future<Item=Self, Error=DhtError> {

        // Fetch a section of known nodes to process
        let pending = self.pending();
        let chunk = &pending[0..usize::min(self.config.concurrency, pending.len())];

        debug!("[search] iteration {:?}/{:?} over: {:?}", 
            self.config.max_recursion - self.depth, self.config.max_recursion, chunk);

        // Update nodes in the chunk to active state
        for target in chunk {
            self.known.entry(target.id().clone()).and_modify(|(_n, s)| *s = RequestState::Active );
        }

        let req = match self.op {
            Operation::FindNode => Request::FindNode(self.target.clone()),
            Operation::FindValue => Request::FindValue(self.target.clone()),
        };

        // Send requests and handle responses
        request_all(self.conn.clone(), self.ctx.clone(), &req, chunk)
        .map(move |res| {
            for (n, v) in &res {
                // Handle received responses
                if let Some((resp, _ctx)) = v {
                    match resp {
                        Response::NodesFound(_id, nodes) => {
                            // Add nodes to known list
                            for n in nodes {
                                if n.id() != &self.origin {
                                    self.known.entry(n.id().clone()).or_insert((n.clone(), RequestState::Pending));
                                }
                            }
                        },
                        Response::ValuesFound(_id, values) => {
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
                self.table.update(&n);
            }
            // Update depth limit
            self.depth -= 1;

            self
        })
    }

    /// Seed the search with nearest nodes in addition to those provided in initialisation
    pub fn seed(&mut self, known: &[Node<Id, Addr>]) {
        for n in known {
            self.known.entry(n.id().clone()).or_insert((n.clone(), RequestState::Pending));
        }
    }
}


#[cfg(test)]
mod tests {
    use std::clone::Clone;

    use super::*;
    use crate::nodetable::{NodeTable, KNodeTable};

    use rr_mux::mock::{MockTransaction, MockConnector};

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
        let mut connector = MockConnector::new().expect(vec![
            // First execution
            MockTransaction::request(nodes[1].clone(), Request::FindNode(target.id().clone()), Ok((Response::NodesFound(target.id().clone(), vec![nodes[3].clone()]), () ))),
            MockTransaction::request(nodes[0].clone(), Request::FindNode(target.id().clone()), Ok((Response::NodesFound(target.id().clone(), vec![nodes[2].clone()]), () ))),
            
            // Second execution
            MockTransaction::request(nodes[2].clone(), Request::FindNode(target.id().clone()), Ok(( Response::NodesFound(target.id().clone(), vec![target.clone()]), () ))),
            MockTransaction::request(nodes[3].clone(), Request::FindNode(target.id().clone()), Ok(( Response::NodesFound(target.id().clone(), vec![nodes[4].clone()]), () ))), 
        ]);

        // Create search object
        let mut config = Config::default();
        config.k = 2;

        let table = KNodeTable::new(root.id().clone(), 2, 8);
        let mut s = Search::<_, _, u64, _, _, u64, _>::new(root.id().clone(), target.id().clone(), Operation::FindNode, config, table.clone(), connector.clone(), ());

        // Seed search with known nearest nodes
        s.seed(&nodes[0..2]);
        {
            // This should add the nearest nodes to the known map
            assert!(s.known.get(nodes[0].id()).is_some());
            assert!(s.known.get(nodes[1].id()).is_some());
            // But not to the node table until they have responded
            let t = table.clone();
            assert!(t.contains(nodes[0].id()).is_none());
            assert!(t.contains(nodes[1].id()).is_none());
        }
        
        // Perform first iteration
        s = s.recurse().wait().unwrap();
        {
            // Responding nodes should be added to the node table
            let t = table.clone();
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
            let t = table.clone();
            assert!(t.contains(nodes[2].id()).is_some());
            assert!(t.contains(nodes[3].id()).is_some());
            // Viable responses should be added to the known map
            assert!(s.known.get(nodes[4].id()).is_some());
            assert!(s.known.get(target.id()).is_some());
        }

        connector.finalise();
    }
}