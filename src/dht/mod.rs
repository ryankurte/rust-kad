use std::fmt::{Debug, Display};

use std::collections::HashMap;
/**
 * rust-kad
 * Kademlia core implementation
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */
use std::marker::PhantomData;
use std::time::Instant;

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::channel::mpsc::Sender;
use futures::prelude::*;

use log::{debug, trace, warn, error};

use crate::Config;

use crate::common::*;

use crate::store::{Datastore, HashMapStore};
use crate::table::{KNodeTable, NodeTable};

mod operation;
pub use operation::*;

mod connect;
pub use connect::*;

mod locate;
pub use locate::*;

mod search;
pub use search::*;

mod store;
pub use store::*;

pub struct Dht<Id, Info, Data, ReqId, Table = KNodeTable<Id, Info>, Store = HashMapStore<Id, Data>>
{
    id: Id,

    config: Config,
    table: Table,
    conn_mgr: Sender<(ReqId, Entry<Id, Info>, Request<Id, Data>)>,
    datastore: Store,

    operations: HashMap<ReqId, Operation<Id, Info, Data, ReqId>>,

    _addr: PhantomData<Info>,
    _data: PhantomData<Data>,
    _req_id: PhantomData<ReqId>,
}

impl<Id, Info, Data, ReqId, Table, Store> Dht<Id, Info, Data, ReqId, Table, Store>
where
    Id: DatabaseId + Clone + Sized + Send + 'static,
    Info: PartialEq + Clone + Sized + Debug + Send + 'static,
    Data: PartialEq + Clone + Sized + Debug + Send + 'static,
    ReqId: RequestId + Clone + Sized + Display + Debug + Send + 'static,
    Table: NodeTable<Id, Info> + Send + 'static,
    Store: Datastore<Id, Data> + Send + 'static,
{
    /// Create a new DHT with custom node table / data store implementation
    pub fn custom(
        id: Id,
        config: Config,
        conn_mgr: Sender<(ReqId, Entry<Id, Info>, Request<Id, Data>)>,
        table: Table,
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
            },
            Request::FindValue(id) => {
                // Lookup the value
                if let Some(values) = self.datastore.find(id) {
                    debug!("FindValue request, {} values for id: {:?}", values.len(), id);
                    Response::ValuesFound(id.clone(), values)
                } else {
                    debug!("FindValue request, no values found, returning closer nodes for id: {:?}", id);
                    let nodes = self.table.nearest(id, 0..self.config.k);
                    Response::NodesFound(id.clone(), nodes)
                }
            },
            Request::Store(id, value) => {
                // Write value to local storage
                let values = self.datastore.store(id, value);
                // Reply to confirm write was completed
                if !values.is_empty() {
                    debug!("Store request, stored {} values for id: {:?}", values.len(), id);
                    Response::ValuesFound(id.clone(), values)
                } else {
                    debug!("Store request, ignored values for id: {:?}", id);
                    Response::NoResult
                }
            },
        };

        // Update record for sender
        self.table.create_or_update(from);

        Ok(resp)
    }

    // Receive responses and update internal state
    pub fn handle_resp(
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

        debug!(
            "Operation {} response from {:?}: {}",
            req_id, from.id(), resp
        );
        trace!("Response: {:?}", resp);

        // Check request is expected / from a valid peer and update state
        match op.nodes.get_mut(from.id()) {
            Some((_e, s)) if *s != RequestState::Active => {
                warn!("Operation {} unexpected response from: {:?}", req_id, from.id());
                *s = RequestState::InvalidResponse;
                return Ok(());
            }
            Some((_e, s)) if *s == RequestState::Active => {
                *s = RequestState::Complete;
            }
            _ => (),
        }

        // Update global / DHT peer table
        self.table.create_or_update(from);

        // Add responding node to nodes
        op.nodes.entry(from.id().clone())
            .or_insert((from.clone(), RequestState::Complete));

        // Handle incoming response message
        let v = match &resp {
            Response::NodesFound(id, entries) if id == &op.target => {
                debug!("Operation {}, adding {} nodes to map", req_id, entries.len());
                trace!("Entries: {:?}", entries);

                for e in entries {
                    debug!("Operation {}, add node {:?}", req_id, e.id());

                    // Skip entries relating to ourself
                    if e.id() == &self.id {
                        continue;
                    }

                    // Update global nodetable
                    self.table.create_or_update(e);

                    // Skip adding responding node to operation again
                    // (state is updated above)
                    if e.id() == from.id() {
                        continue;
                    }

                    // Insert new nodes into tracking
                    // TODO: should we update op node table with responding node info?
                    op.nodes
                        .entry(e.id().clone())
                        .or_insert((e.clone(), RequestState::Pending));
                }

                RequestState::Complete
            }
            Response::NodesFound(id, _entries) => {
                debug!(
                    "Operation {}, invalid nodes response: {:?} from: {:?} (id: {:?})",
                    req_id, resp, from.id(), id
                );
                RequestState::InvalidResponse
            }
            Response::ValuesFound(id, values) if id == &op.target => {
                debug!("Operation {}, adding {} values to map", req_id, values.len());
                trace!("Values: {:?}", values);

                // Add data to data list
                op.data.insert(from.id().clone(), values.clone());

                RequestState::Complete
            }
            Response::ValuesFound(id, _values) => {
                debug!(
                    "Operation {}, invalid values response from: {:?} (id: {:?})",
                    req_id, from.id(), id
                );
                trace!("Invalid response: {:?}", resp);
                RequestState::InvalidResponse
            }
            Response::NoResult => {
                debug!("Operation {}, empty response from: {:?}", req_id, from);
                RequestState::Complete
            }
        };

        // Update operation node state
        let e = op
            .nodes
            .entry(from.id().clone())
            .and_modify(|(_e, s)| *s = v)
            .or_insert((from.clone(), v));

        debug!("update node {:?} state: {:?}", e.0.id(), e.1);

        trace!("Operation state: {:?}", op);

        // Replace operation
        self.operations.insert(req_id, op);

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
        let op = Operation::new(req_id.clone(), target, kind);

        // Register operation in tracking
        // Actual execution happens in `Dht::update` methods.
        self.operations.insert(req_id, op);

        // Return OK
        Ok(())
    }

    /// Update internal state
    /// Usually this should be called via future `Dht::poll()`
    pub fn update(&mut self) -> Result<bool, Error> {
        let mut req_sink = self.conn_mgr.clone();
        let mut done = vec![];

        let mut changed = false;

        //  For each currently tracked operation
        for (req_id, op) in self.operations.iter_mut() {
            // Generate request objects
            let req = match &op.kind {
                OperationKind::Connect(_tx) => Request::FindNode(op.target.clone()),
                OperationKind::FindNode(_tx) => Request::FindNode(op.target.clone()),
                OperationKind::FindValues(_tx) => Request::FindNode(op.target.clone()),
                OperationKind::Store(_v, _tx) => Request::FindNode(op.target.clone()),
            };

            // Match on states
            let last_state = op.state.clone();
            match last_state {
                // Initialise new operation
                OperationState::Init => {
                    debug!("Operation {} ({}) start", req_id, &op.kind);

                    // Identify nearest nodes if available
                    let nearest: Vec<_> =
                        self.table.nearest(&op.target, 0..self.config.concurrency);
                    for e in &nearest {
                        op.nodes
                            .insert(e.id().clone(), (e.clone(), RequestState::Active));
                    }

                    // Build node array (required to include existing op.nodes)
                    let mut nodes: Vec<_> =
                        op.nodes.iter().map(|(_k, (n, _s))| n.clone()).collect();
                    nodes.sort_by_key(|n| Id::xor(&op.target, n.id()));

                    debug!(
                        "Initiating {} operation ({}), sending {} request to {} peers",
                        &op.kind, req_id, req, nodes.len()
                    );

                    // Issue appropriate request
                    for e in nodes.iter() {
                        trace!("Operation {} issuing {} to: {:?}", req_id, req, e.id());

                        // TODO: handle sink errors?
                        let _ = req_sink.try_send((req_id.clone(), e.clone(), req.clone()));
                    }

                    // Set to search state
                    debug!("Operation {} entering searching state", req_id);
                    op.state = OperationState::Searching(0)
                }
                // Awaiting response to connect message
                OperationState::Connecting => {
                    // Check for known nodes (connect responses)
                    let nodes = op.nodes.clone();
                    let mut known: Vec<_> = nodes.iter().collect();
                    known.sort_by_key(|(k, _)| Id::xor(&op.target, k));

                    let pending = known
                        .iter()
                        .filter(|(_key, (_e, s))| *s == RequestState::Pending)
                        .count();

                    // Check for expiry
                    let search_timeout = self.config.search_timeout;
                    let expired = Instant::now()
                        .checked_duration_since(op.last_update)
                        .map(|d| d > search_timeout)
                        .unwrap_or(false);

                    if !nodes.is_empty() {
                        debug!(
                            "Operation {} connect response received ({} peers)",
                            req_id, pending
                        );

                        // Short-circuit if we didn't receive other peer information
                        if pending > 0 {
                            op.state = OperationState::Search(0);
                        } else {
                            op.state = OperationState::Done;
                        }
                    } else if expired {
                        debug!("Operation {} connect timeout", req_id);
                        op.state = OperationState::Done;
                    }
                }
                // Currently searching / awaiting responses
                OperationState::Searching(n) => {
                    // Fetch known nodes
                    let nodes = op.nodes.clone();
                    let mut known: Vec<_> = nodes.iter().collect();
                    known.sort_by_key(|(k, _)| Id::xor(&op.target, k));

                    // Short-circuit to next search if active nodes have completed operations
                    let active: Vec<_> = (&known[0..usize::min(known.len(), self.config.k)])
                        .iter()
                        .filter(|(_key, (_e, s))| *s == RequestState::Active)
                        .collect();

                    // Exit when no pending nodes remain within K bucket
                    let pending: Vec<_> = (&known[0..usize::min(known.len(), self.config.k)])
                        .iter()
                        .filter(|(_key, (_e, s))| *s == RequestState::Pending)
                        .collect();

                    // Check for expiry
                    let search_timeout = self.config.search_timeout;
                    let expired = Instant::now()
                        .checked_duration_since(op.last_update)
                        .map(|d| d > search_timeout)
                        .unwrap_or(false);

                    trace!("active: {:?}", active);
                    trace!("pending: {:?}", pending);
                    trace!("data: {:?}", op.data);

                    match (active.len(), pending.len(), expired, &op.kind) {
                        // No active or pending nodes, all complete (this will basically never happen)
                        (0, 0, _, _) => {
                            debug!("Operation {} search complete!", req_id);
                            op.state = OperationState::Request;
                        }
                        // No active nodes, all replies received, re-start search
                        (0, _, _, _) => {
                            debug!(
                                "Operation {}, all responses received, re-starting search",
                                req_id
                            );
                            op.state = OperationState::Search(n + 1);
                        }
                        // Search iteration timeout
                        (_, _, true, _) => {
                            debug!("Operation {} timeout at iteration {}", req_id, n);

                            // TODO: Update active nodes to timed-out
                            for (id, _n) in active {
                                op.nodes
                                    .entry((*id).clone())
                                    .and_modify(|(_n, s)| *s = RequestState::Timeout);
                            }

                            op.state = OperationState::Search(n + 1);
                        }
                        _ => (),
                    }
                }
                // Update a search iteration
                OperationState::Search(n) => {
                    // Check for max recursion
                    if n > self.config.max_recursion {
                        debug!("Reached recursion limit, aborting search {}", req_id);
                        op.state = OperationState::Pending;
                        continue;
                    }

                    debug!("Operation {} ({}) search iteration {}", req_id, &op.kind, n);

                    // Locate next nearest nodes
                    let own_id = self.id.clone();
                    let mut nearest: Vec<_> = op
                        .nodes
                        .iter()
                        .filter(|(k, (_n, s))| (*s == RequestState::Pending) & (*k != &own_id))
                        .map(|(_k, (n, _s))| n.clone())
                        .collect();

                    debug!("Operation {} nearest: {:?}", req_id, nearest);

                    // Sort and limit
                    nearest.sort_by_key(|n| Id::xor(&op.target, n.id()));
                    let n = usize::min(self.config.concurrency, nearest.len());

                    // Exit search when we have no more requests to make
                    if n == 0 {
                        op.state = OperationState::Request;
                    }

                    // Launch next set of requests
                    debug!(
                        "Operation {} issuing search request: {:?} to: {:?}",
                        req_id,
                        req,
                        &nearest[0..n]
                    );
                    for n in &nearest[0..n] {
                        op.nodes
                            .entry(n.id().clone())
                            .and_modify(|(_n, s)| *s = RequestState::Active);

                        // TODO: handle sink errors?
                        let _ = req_sink.try_send((req_id.clone(), n.clone(), req.clone()));
                    }

                    // Update search state
                    debug!("Operation {} entering searching state", req_id);
                    op.state = OperationState::Searching(n);
                }
                // Issue find / store operation following search
                OperationState::Request => {
                    // TODO: should a search be a find all then query, or a find values with short-circuits?
                    let req = match &op.kind {
                        OperationKind::Store(v, _) => Request::Store(op.target.clone(), v.clone()),
                        OperationKind::FindValues(_) => Request::FindValue(op.target.clone()), 
                        _ => {
                            debug!("Operation {} entering done state", req_id);
                            op.state = OperationState::Done;
                            continue;
                        }
                    };

                    // Locate nearest responding nodes
                    // TODO: this doesn't _need_ to be per search, could be from the global table?
                    let own_id = self.id.clone();
                    let mut nearest: Vec<_> = op
                        .nodes
                        .iter()
                        .filter(|(k, (_n, s))| (*s == RequestState::Complete) & (*k != &own_id))
                        .map(|(_k, (n, _s))| n.clone())
                        .collect();

                    // Sort and limit
                    nearest.sort_by_key(|n| Id::xor(&op.target, n.id()));
                    let range = 0..usize::min(self.config.concurrency, nearest.len());

                    debug!(
                        "Operation {} ({}) issuing {} request to {} peers",
                        req_id,
                        &op.kind,
                        req,
                        nearest[range.clone()].len()
                    );

                    for n in &nearest[range] {
                        op.nodes
                            .entry(n.id().clone())
                            .and_modify(|(_n, s)| *s = RequestState::Active);

                        // TODO: handle sink errors?
                        let _ = req_sink.try_send((req_id.clone(), n.clone(), req.clone()));
                    }

                    op.state = OperationState::Pending;
                }
                // Currently awaiting request responses
                OperationState::Pending => {
                    // Fetch known nodes
                    let nodes = op.nodes.clone();
                    let mut known: Vec<_> = nodes.iter().collect();
                    known.sort_by_key(|(k, _)| Id::xor(&op.target, k));

                    // Exit when no pending nodes remain
                    let active: Vec<_> = (&known[0..usize::min(known.len(), self.config.k)])
                        .iter()
                        .filter(|(_key, (_e, s))| *s == RequestState::Active)
                        .collect();

                    // Check for expiry
                    let search_timeout = self.config.search_timeout;
                    let expired = Instant::now()
                        .checked_duration_since(op.last_update)
                        .map(|d| d > search_timeout)
                        .unwrap_or(false);

                    if active.is_empty() || expired {
                        debug!("Operation {} ({}) entering done state", req_id, &op.kind);
                        op.state = OperationState::Done;
                    }
                }
                // Update completion state
                OperationState::Done => {
                    debug!("Operating {} ({}) done", req_id, &op.kind);

                    match &op.kind {
                        OperationKind::Connect(tx) => {
                            let mut peers: Vec<_> = op
                                .nodes
                                .iter()
                                .filter(|(_k, (_n, s))| *s == RequestState::Complete)
                                .map(|(_k, (e, _s))| e.clone())
                                .collect();

                            let own_id = self.id.clone();
                            peers.sort_by_key(|n| Id::xor(&own_id, n.id()));
                            let res = if !peers.is_empty() {
                                tx.clone().try_send(Ok(peers))
                            } else {
                                tx.clone().try_send(Err(Error::NotFound))
                            };

                            if let Err(e) = res {
                                error!("Failed to send connect complete: {:?}", e);
                            }
                        }
                        OperationKind::FindNode(tx) => {
                            trace!("Found nodes: {:?}", op.nodes);
                            let res = match op.nodes.get(&op.target) {
                                Some((n, _s)) => tx.clone().try_send(Ok(n.clone())),
                                None => tx.clone().try_send(Err(Error::NotFound)),
                            };

                            if let Err(e) = res {
                                error!("Failed to send find node complete: {:?}", e);
                            }
                        }
                        OperationKind::FindValues(tx) => {
                            
                            // Flatten out response data
                            let mut flat_data: Vec<Data> =
                                op.data.iter().flat_map(|(_k, v)| v.clone()).collect();

                            // Append any already known data
                            if let Some(mut existing) = self.datastore.find(&op.target) {
                                flat_data.append(&mut existing);
                            }

                            // TODO: apply reducer?

                            debug!("Operation {} values found: {:?}", req_id, flat_data);

                            let res = if !flat_data.is_empty() {
                                tx.clone().try_send(Ok(flat_data))
                            } else {
                                tx.clone().try_send(Err(Error::NotFound))
                            };

                            if let Err(e) = res {
                                error!("Failed to send find values complete: {:?}", e);
                            }
                        }
                        OperationKind::Store(_values, tx) => {
                            // `Store` responds with a `FoundData` object containing the stored data
                            // this allows us to check `op.data` for the nodes at which data has been stored

                            let res = if !op.data.is_empty() {
                                let flat_ids: Vec<_> =
                                    op.data.iter().map(|(k, _v)| k.clone()).collect();
                                let mut flat_nodes: Vec<_> = flat_ids
                                    .iter()
                                    .filter_map(|id| op.nodes.get(id).map(|(e, _s)| e.clone()))
                                    .collect();
                                flat_nodes.sort_by_key(|n| Id::xor(&op.target, n.id()));

                                // TODO: check the stored _values_ too (as this may be reduced on the upstream side)

                                debug!("Operation {} stored at {} peers", req_id, flat_ids.len());

                                tx.clone().try_send(Ok(flat_nodes))
                            } else {
                                debug!("Operation {} store failed", req_id);
                                tx.clone().try_send(Err(Error::NotFound))
                            };

                            if let Err(e) = res {
                                error!("Failed to send store complete: {:?}", e);
                            }
                        }
                    };


                    // TODO: remove from tracking
                    done.push(req_id.clone());
                }
            };

            if op.state != last_state {
                changed = true;
            }
        }

        for req_id in done {
            self.operations.remove(&req_id);
        }

        Ok(changed)
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

    pub fn nodetable(&self) -> &Table {
        &self.table
    }

    pub fn nodetable_mut(&mut self) -> &mut Table {
        &mut self.table
    }

    pub fn datastore(&self) -> &Store {
        &self.datastore
    }

    pub fn datastore_mut(&mut self) -> &mut Store {
        &mut self.datastore
    }

    #[cfg(test)]
    pub fn contains(&mut self, id: &Id) -> Option<Entry<Id, Info>> {
        self.table.contains(id)
    }
}

impl<Id, Info, Data, ReqId, Table, Store> Unpin for Dht<Id, Info, Data, ReqId, Table, Store> {}

impl<Id, Info, Data, ReqId, Table, Store> Future for Dht<Id, Info, Data, ReqId, Table, Store>
where
    Id: DatabaseId + Clone + Sized + Send + 'static,
    Info: PartialEq + Clone + Sized + Debug + Send + 'static,
    Data: PartialEq + Clone + Sized + Debug + Send + 'static,
    ReqId: RequestId + Clone + Sized + Display + Debug + Send + 'static,
    Table: NodeTable<Id, Info> + Send + 'static,
    Store: Datastore<Id, Data> + Send + 'static,
{
    type Output = Result<(), Error>;

    // Poll calls internal update function
    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        let updated = self.update()?;
        if updated {
            ctx.waker().clone().wake();
        }

        Poll::Pending
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
        let mut $dht =
            Dht::<[u8; 1], _, u64, u64>::standard($root.id().clone(), $config, $connector.clone());
    };
}

#[cfg(test)]
mod tests {
    use std::clone::Clone;

    extern crate futures;
    use futures::channel::mpsc;

    use super::*;
    use crate::store::Datastore;

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
            dht.handle_req(1, &friend, &Request::FindNode(*other.id()))
                .unwrap(),
            Response::NodesFound(*other.id(), vec![friend.clone()]),
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
            dht.handle_req(1, &other, &Request::FindValue([201]))
                .unwrap(),
            Response::NodesFound([201], vec![friend.clone()]),
        );

        // Add value to store
        dht.datastore.store(&[201], &vec![1337]);

        // FindValues
        assert_eq!(
            dht.handle_req(2, &other, &Request::FindValue([201]))
                .unwrap(),
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
