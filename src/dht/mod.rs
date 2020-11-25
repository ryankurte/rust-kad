use std::fmt::{Debug, Display};

/**
 * rust-kad
 * Kademlia core implementation
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */
use std::marker::PhantomData;
use std::ops::Add;
use std::time::Instant;
use std::collections::HashMap;

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::prelude::*;
use futures::channel::mpsc::{Sender, Receiver};

use log::{trace, debug, info, warn};

use crate::Config;

use crate::common::*;

use crate::store::Datastore;
use crate::table::NodeTable;

//pub mod search;
//pub use self::search::{Operation, Search};

//mod connect;
//pub use connect::*;

mod lookup;
pub use lookup::*;

//mod find;
//pub use find::*;

//mod store;
//pub use store::*;

pub struct Dht<Id, Info, Data, ReqId, Table, Store> {
    id: Id,

    config: Config,
    table: Table,
    conn_mgr: Sender<(Entry<Id, Info>, Request<Id, Data>)>,
    datastore: Store,

    operations: HashMap<ReqId, Operation<Id, Info, Data, ReqId>>,

    _addr: PhantomData<Info>,
    _data: PhantomData<Data>,
    _req_id: PhantomData<ReqId>,
}

struct Operation<Id, Info, Data, ReqId> {
    req_id: ReqId,
    target: Id,
    kind: OperationKind<Id, Info, Data>,

    state: OperationState,
    nodes: HashMap<Id, RequestState>,
    data: HashMap<Id, Vec<Data>>,
}

impl <Id, Info, Data, ReqId> Operation<Id, Info, Data, ReqId> {
    fn new(req_id: ReqId, target: Id, kind: OperationKind<Id, Info, Data>) -> Self {
        Self {
            req_id,
            target,
            kind,
            state: OperationState::Init,
            nodes: HashMap::new(),
            data: HashMap::new(),
        }
    }
}

/// RequestState is used to store the state of pending requests
#[derive(Clone, Debug, PartialEq)]
pub enum RequestState {
    Pending,
    Active,
    Timeout,
    Complete,
    InvalidResponse,
}


pub enum OperationKind<Id, Info, Data> {
    /// Find a node with the specified ID
    FindNode(Sender<Result<Entry<Id, Info>, Error>>),
    /// Find values at the specified ID
    FindValues(Sender<Result<Vec<Data>, Error>>),
    /// Store the provided value(s) at the specified ID
    Store(Vec<Data>, Sender<Result<(), Error>>),
}

impl <Id, Info, Data> Display for OperationKind<Id, Info, Data> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OperationKind::FindNode(_) => write!(f, "FindNode"),
            OperationKind::FindValues(_) => write!(f, "FindValues"),
            OperationKind::Store(_, _) => write!(f, "Store"),
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum OperationState {
    Init,
    Search(usize),
}

impl<Id, Info, Data, ReqId, Table, Store> Dht<Id, Info, Data, ReqId, Table, Store>
where
    Id: DatabaseId + Clone + Sized + Send + 'static,
    Info: PartialEq + Clone + Sized + Debug + Send + 'static,
    Data: PartialEq + Clone + Sized + Debug + Send + 'static,
    ReqId: RequestId + Clone + Sized + Display + Debug + Send + 'static,
    Table: NodeTable<Id, Info> + Clone + Send + 'static,
    Store: Datastore<Id, Data> + Clone + Send + 'static,
{
    /// Create a new generic DHT
    pub fn new(
        id: Id,
        config: Config,
        table: Table,
        conn_mgr: Sender<(Entry<Id, Info>, Request<Id, Data>)>,
        datastore: Store,
    ) -> Dht<Id, Info, Data, ReqId, Table, Store> {
        Dht {
            id,
            config,
            table,
            conn_mgr,
            datastore,

            operations: HashMap::new(),

            _addr: PhantomData,
            _data: PhantomData,
            _req_id: PhantomData,
        }
    }

    /// Receive and reply to requests
    pub fn handle_req(
        &mut self,
        _req_id: ReqId,
        from: &Entry<Id, Info>,
        req: &Request<Id, Data>,
    ) -> Result<Response<Id, Info, Data>, Error> {
        // Build response
        let resp = match req {
            Request::Ping => Response::NoResult,
            Request::FindNode(id) => {
                let nodes = self.table.nearest(id, 0..self.config.k);
                Response::NodesFound(id.clone(), nodes)
            }
            Request::FindValue(id) => {
                // Lockup the value
                if let Some(values) = self.datastore.find(id) {
                    debug!("Found {} values for id: {:?}", values.len(), id);
                    Response::ValuesFound(id.clone(), values)
                } else {
                    debug!("No values found, returning closer nodes for id: {:?}", id);
                    let nodes = self.table.nearest(id, 0..self.config.k);
                    Response::NodesFound(id.clone(), nodes)
                }
            }
            Request::Store(id, value) => {
                // Write value to local storage
                let values = self.datastore.store(id, value);
                // Reply to confirm write was completed
                if values.len() != 0 {
                    debug!("Stored {} values for id: {:?}", values.len(), id);
                    Response::ValuesFound(id.clone(), values)
                } else {
                    debug!("Ignored values for id: {:?}", id);
                    Response::NoResult
                }
            }
        };

        // Update record for sender
        self.table.create_or_update(from);

        Ok(resp)
    }

    pub(crate) async fn request(
        &self,
        req_id: ReqId,
        targets: &[Entry<Id, Info>],
        req: Request<Id, Data>,
    ) -> Result<(), Error> {
        
        trace!("Send req: {} ({:?}) to {:?}", req_id, req, targets);

        for t in targets {
            self.conn_mgr.clone().send((t.clone(), req.clone())).await;
        }

        Ok(())
    }

    // Create a new operation
    pub(crate) fn exec(
        &mut self,
        req_id: ReqId,
        target: Id,
        kind: OperationKind<Id, Info, Data>,
    ) -> Result<(), Error> {

        debug!("Registering operation id: {}", req_id);

        // Create operation object
        let mut op = Operation::new(req_id.clone(), target.clone(), kind);

        // Register operation in tracking
        // Actual execution happens in `Dht::update` methods.
        self.operations.insert(req_id.clone(), op);

        // Return OK
        Ok(())
    }

    pub async fn update(&mut self) {
        let mut req_sink = self.conn_mgr.clone();

        //  For each currently tracked operation
        for (req_id, op) in self.operations.iter_mut() {

            // Generate request objects
            let req = match &op.kind {
                OperationKind::FindNode(tx) => Request::FindNode(op.target.clone()),
                OperationKind::FindValues(tx) => Request::FindValue(op.target.clone()),
                OperationKind::Store(v, tx) => Request::Store(op.target.clone(), v.clone()),
            };

            // Match on states
            let s = match op.state.clone() {
                // Initialise new operation
                OperationState::Init => {
                    let nearest: Vec<_> = self.table.nearest(&op.target, 0..self.config.concurrency);
                    for e in &nearest {
                        op.nodes.insert(e.id().clone(), RequestState::Pending);
                    }

                    debug!("Initiating operation {} ({})  sending {:?} to: {:?}", &op.kind, req_id, req, nearest);

                    // Issue appropriate request
                    for n in &nearest {
                        // TODO: handle sink errors?
                        let _ = req_sink.send((n.clone(), req.clone())).await;
                    }

                    // Set to search state
                    op.state = OperationState::Search(self.config.max_recursion)
                },
                // Update a search iteration
                OperationState::Search(n) => {
                    debug!("Continuing search {}", req_id);

                    // Locate next nearest nodes
                    let nearest: Vec<_> = op.nodes.iter()
                        .filter(|(k, s)| (*s == &RequestState::Pending) & (*k != &self.id))
                        .map(|(_k, (n, _s))| n.clone())
                        .collect();
                    chunk.sort_by_key(|n| Id::xor(&op.target, n.id()));


                },
            };
        }
    }

    // Receive responses and update internal state
    pub async fn handle_resp(
        &mut self,
        req_id: ReqId,
        from: &Entry<Id, Info>,
        resp: &Response<Id, Info, Data>,
    ) -> Result<(), Error> {
        
        // Locate matching operation
        let mut op = match self.operations.remove(&req_id) {
            Some(v) => v,
            None => {
                warn!("No matching operation for request id: {}", req_id);
                return Ok(());
            }
        };

        debug!("Receive response id: {} ({:?}) from: {:?}", req_id, resp, from);

        // TODO: check request is from a valid peer

        // TODO: update global / DHT node and data tables



        // Handle incoming message
        match &resp {
            Response::NodesFound(id, entries) => {
                if id == &op.target {
                    debug!("Operation {}, adding {} entries to map", req_id, entries.len());

                    for e in entries {
                        op.nodes
                            .entry(e.id().clone())
                            .or_insert(RequestState::Pending);
                    }
                } else {
                    // Set response invalid
                    op.nodes
                        .entry(from.id().clone())
                        .or_insert(RequestState::InvalidResponse);
                }
            },
            Response::ValuesFound(id, values) => {
                if id == &op.target {
                    // Add data to data list
                    op.data.insert(id.clone(), values.clone());
                } else {
                    // Set response invalid
                    op.nodes
                        .entry(from.id().clone())
                        .or_insert(RequestState::InvalidResponse);
                }
            },
            Response::NoResult => {}
        }

        // Replace operation
        self.operations.insert(req_id, op);
    
        return Ok(())
    }

    /// Refresh buckets and node table entries
    #[cfg(nope)]
    pub async fn refresh(&mut self) -> Result<(), ()> {
        // TODO: send refresh to buckets that haven't been looked up recently
        // How to track recently looked up / contacted buckets..?

        // Evict "expired" nodes from buckets
        // Message oldest node, if no response evict
        // Maybe this could be implemented as a periodic ping and timeout instead?
        let timeout = self.config.node_timeout;
        let oldest: Vec<_> = self
            .table
            .iter_oldest()
            .filter(move |o| {
                if let Some(seen) = o.seen() {
                    seen.add(timeout) < Instant::now()
                } else {
                    true
                }
            })
            .collect();

        let mut pings = Vec::with_capacity(oldest.len());

        for o in oldest {
            let mut t = self.table.clone();
            let mut o = o.clone();

            let mut conn = self.conn_mgr.clone();

            let p = async move {
                let res = conn
                    .request(ReqId::generate(), o.clone(), Request::Ping)
                    .await;
                match res {
                    Ok(_resp) => {
                        debug!("[DHT refresh] updating node: {:?}", o);
                        o.set_seen(Instant::now());
                        t.create_or_update(&o);
                    }
                    Err(_e) => {
                        debug!("[DHT refresh] expiring node: {:?}", o);
                        t.remove_entry(o.id());
                    }
                }

                ()
            };

            pings.push(p);
        }

        future::join_all(pings).await;

        Ok(())
    }

    #[cfg(test)]
    pub fn contains(&mut self, id: &Id) -> Option<Entry<Id, Info>> {
        self.table.contains(id)
    }
}

impl<Id, Info, Data, ReqId, Table, Store> Future for Dht<Id, Info, Data, ReqId, Table, Store>
where
    Id: DatabaseId + Clone + Sized + Send + 'static,
    Info: PartialEq + Clone + Sized + Debug + Send + 'static,
    Data: PartialEq + Clone + Sized + Debug + Send + 'static,
    ReqId: RequestId + Clone + Sized + Debug + Send + 'static,
    Table: NodeTable<Id, Info> + Clone + Send + 'static,
    Store: Datastore<Id, Data> + Clone + Send + 'static,
{
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        unimplemented!()
    }
}

/// Helper macro to setup DHT instances for testing
#[cfg(test)]
#[macro_export]
#[cfg(test)]
macro_rules! mock_dht {
    ($connector: ident, $root: ident, $dht:ident) => {
        let mut config = Config::default();
        config.concurrency = 2;
        mock_dht!($connector, $root, $dht, config);
    };
    ($connector: ident, $root: ident, $dht:ident, $config:ident) => {
        let table = KNodeTable::new($root.id().clone(), $config.k, $root.id().max_bits());

        let store: HashMapStore<[u8; 1], u64> = HashMapStore::new();

        let mut $dht = Dht::<[u8; 1], u64, _, u64, _, _>::new(
            $root.id().clone(),
            $config,
            table,
            $connector.clone(),
            store,
        );
    };
}

#[cfg(test)]
mod tests {
    use std::clone::Clone;
    use std::time::Duration;

    extern crate futures;
    use futures::channel::mpsc;
    use futures::executor::block_on;

    use super::*;
    use crate::store::{Datastore, HashMapStore};
    use crate::table::{KNodeTable, NodeTable};

    #[test]
    fn test_receive_common() {
        let root = Entry::new([0], 001);
        let friend = Entry::new([1], 002);

        let (tx, _rx) = mpsc::channel(10);
        mock_dht!(tx, root, dht);

        // Check node is unknown
        assert!(dht.table.contains(friend.id()).is_none());

        // Ping
        assert_eq!(
            dht.handle_req(1, &friend, &Request::Ping).unwrap(),
            Response::NoResult,
        );

        // Adds node to appropriate k bucket
        let friend1 = dht.table.contains(friend.id()).unwrap();

        // Second ping
        assert_eq!(
            dht.handle_req(2, &friend, &Request::Ping).unwrap(),
            Response::NoResult,
        );

        // Updates node in appropriate k bucket
        let friend2 = dht.table.contains(friend.id()).unwrap();
        assert_ne!(friend1.seen(), friend2.seen());
    }


    #[test]
    fn test_receive_ping() {
        let root = Entry::new([0], 001);
        let friend = Entry::new([1], 002);

        let (tx, _rx) = mpsc::channel(10);
        mock_dht!(tx, root, dht);

        // Ping
        assert_eq!(
            dht.handle_req(1, &friend, &Request::Ping).unwrap(),
            Response::NoResult,
        );
    }

    #[test]
    fn test_receive_find_nodes() {
        let root = Entry::new([0], 001);
        let friend = Entry::new([1], 002);
        let other = Entry::new([2], 003);

        let (tx, _rx) = mpsc::channel(10);
        mock_dht!(tx, root, dht);

        // Add friend to known table
        dht.table.create_or_update(&friend);

        // FindNodes
        assert_eq!(
            dht.handle_req(1, &friend, &Request::FindNode(other.id().clone()))
                .unwrap(),
            Response::NodesFound(other.id().clone(), vec![friend.clone()]),
        );
    }

    #[test]
    fn test_receive_find_values() {
        let root = Entry::new([0], 001);
        let friend = Entry::new([1], 002);
        let other = Entry::new([2], 003);

        let (tx, _rx) = mpsc::channel(10);
        mock_dht!(tx, root, dht);

        // Add friend to known table
        dht.table.create_or_update(&friend);

        // FindValues (unknown, returns NodesFound)
        assert_eq!(
            dht.handle_req(1, &other, &Request::FindValue([201])).unwrap(),
            Response::NodesFound([201], vec![friend.clone()]),
        );

        // Add value to store
        dht.datastore.store(&[201], &vec![1337]);

        // FindValues
        assert_eq!(
            dht.handle_req(2, &other, &Request::FindValue([201])).unwrap(),
            Response::ValuesFound([201], vec![1337]),
        );
    }

    #[test]
    fn test_receive_store() {
        let root = Entry::new([0], 001);
        let friend = Entry::new([1], 002);

        let (tx, _rx) = mpsc::channel(10);
        mock_dht!(tx, root, dht);

        // Store
        assert_eq!(
            dht.handle_req(1, &friend, &Request::Store([2], vec![1234]))
                .unwrap(),
            Response::ValuesFound([2], vec![1234]),
        );

        let v = dht.datastore.find(&[2]).expect("missing value");
        assert_eq!(v, vec![1234]);
    }

    #[cfg(nope)]
    #[test]
    fn test_expire() {
        let mut config = Config::default();
        config.node_timeout = Duration::from_millis(200);

        let root = Entry::new([0], 001);
        let n1 = Entry::new([1], 002);
        let n2 = Entry::new([2], 003);

        
        let (tx, _rx) = mpsc::channel(10);
        let c = config.clone();
        mock_dht!(tx, root, dht, config);


        // Add known nodes
        dht.table.create_or_update(&n1);
        dht.table.create_or_update(&n2);

        // No timed out nodes
        block_on(dht.refresh(())).unwrap();
        connector.finalise();

        std::thread::sleep(config.node_timeout * 2);

        // Ok response
        connector.expect(vec![
            Mt::request(n1.clone(), Request::Ping, Ok((Response::NoResult, ()))),
            Mt::request(n2.clone(), Request::Ping, Ok((Response::NoResult, ()))),
        ]);
        block_on(dht.refresh(())).unwrap();

        assert!(dht.table.contains(n1.id()).is_some());
        assert!(dht.table.contains(n2.id()).is_some());

        connector.finalise();

        std::thread::sleep(config.node_timeout * 2);

        // No response (evict)
        connector.expect(vec![
            Mt::request(n1.clone(), Request::Ping, Ok((Response::NoResult, ()))),
            Mt::request(n2.clone(), Request::Ping, Err(Error::Timeout)),
        ]);
        block_on(dht.refresh(())).unwrap();

        assert!(dht.table.contains(n1.id()).is_some());
        assert!(dht.table.contains(n2.id()).is_none());

        // Check expectations are done
        connector.finalise();
    }
}
