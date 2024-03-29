//! DHT implementation and high-level operations
//!
// https://github.com/ryankurte/rust-kad
// Copyright 2018-2023 ryan kurte

use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Add;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures::channel::mpsc::{self, channel, Receiver, Sender};
use futures::prelude::*;
use tokio::time::timeout;
use tracing::{debug, error, trace, warn};

use crate::common::*;
use crate::store::{Datastore, HashMapStore};
use crate::table::{KNodeTable, NodeTable};
use crate::Config;

mod base;
pub use base::*;

mod shared;
pub use shared::*;

mod connect;
pub use connect::*;

mod lookup;
pub use lookup::*;

mod search;
pub use search::*;

mod store;
pub use store::*;

/// Network / communication abstraction trait
#[async_trait::async_trait]
pub trait Net<Id: Send, Info: Send, Data: Send>: Sync + Send {
    /// Issue the provided request to a set of peers,
    /// returning a hashmap of responses by peer ID
    async fn request(
        &self,
        peers: Vec<Entry<Id, Info>>,
        req: Request<Id, Data>,
    ) -> Result<HashMap<Id, Response<Id, Info, Data>>, Error>;
}

/// Type alias for request channels
pub type RequestSender<Id, Info, Data> = Sender<(
    Vec<Entry<Id, Info>>,
    Request<Id, Data>,
    ResponseSender<Id, Info, Data>,
)>;

pub type RequestReceiver<Id, Info, Data> = Receiver<(
    Vec<Entry<Id, Info>>,
    Request<Id, Data>,
    ResponseSender<Id, Info, Data>,
)>;

/// Type alias for response channels
pub type ResponseSender<Id, Info, Data> =
    Sender<Result<HashMap<Id, Response<Id, Info, Data>>, Error>>;

/// [Net] implementation for a generic [RequestSender] channel
#[async_trait::async_trait]
impl<Id: Clone + Send, Info: Clone + Send, Data: Clone + Send> Net<Id, Info, Data>
    for RequestSender<Id, Info, Data>
{
    async fn request(
        &self,
        peers: Vec<Entry<Id, Info>>,
        req: Request<Id, Data>,
    ) -> Result<HashMap<Id, Response<Id, Info, Data>>, Error> {
        let (tx, mut rx) = channel(1);

        if let Err(e) = self.clone().send((peers, req, tx)).await {
            error!("Failed to send request: {:?}", e);
            return Err(Error::Connector);
        }

        match rx.next().await {
            Some(Ok(r)) => Ok(r),
            Some(Err(e)) => Err(e),
            None => {
                warn!("op channel closed by sender");
                Err(Error::Cancelled)
            }
        }
    }
}

type OpSender<Id, Info, Data> = Sender<Result<OpResp<Id, Info, Data>, Error>>;

/// [Reducer] used to reduce values at a given database ID
pub type Reducer<Id, Data> = dyn Fn(Id, Vec<Data>) -> Vec<Data> + Send;

pub struct Dht<
    Id,
    Info,
    Data,
    Io = RequestSender<Id, Info, Data>,
    Table = KNodeTable<Id, Info>,
    Store = HashMapStore<Id, Data>,
> {
    id: Id,

    config: Config,
    table: Table,
    datastore: Store,
    net: Io,

    reducer: Option<Box<Reducer<Id, Data>>>,

    op_rx: Receiver<(OpReq<Id, Info, Data>, OpSender<Id, Info, Data>)>,
    op_tx: Sender<(OpReq<Id, Info, Data>, OpSender<Id, Info, Data>)>,
}

impl<Id, Info, Data, Io, Table, Store> Dht<Id, Info, Data, Io, Table, Store>
where
    Id: DatabaseId + Clone + Sized + Sync + Send + 'static,
    Info: PartialEq + Clone + Sized + Debug + Sync + Send + 'static,
    Data: PartialEq + Clone + Sized + Debug + Sync + Send + 'static,
    Io: Net<Id, Info, Data> + Clone + Debug + Send + 'static,
    Table: NodeTable<Id, Info> + Send + 'static,
    Store: Datastore<Id, Data> + Send + 'static,
{
    /// Create a new DHT with custom node table / data store implementation
    pub fn custom(
        id: Id,
        config: Config,
        net: Io,
        table: Table,
        datastore: Store,
    ) -> Dht<Id, Info, Data, Io, Table, Store> {
        let (op_tx, op_rx) = mpsc::channel(0);

        let d = Dht {
            id,
            config,
            table,
            datastore,
            net,
            reducer: None,

            op_rx,
            op_tx,
        };

        // Start self-update task
        if d.config.update_period != Duration::from_secs(0) {
            let update_period = d.config.update_period.clone();
            let h = d.get_handle();

            tokio::task::spawn(async move {
                loop {
                    tokio::time::sleep(update_period).await;
                    let _ = h.update(false).await;
                }
            });
        }

        d
    }

    pub fn set_reducer(&mut self, r: Box<Reducer<Id, Data>>) {
        self.reducer = Some(r);
    }

    pub fn get_handle(&self) -> DhtHandle<Id, Info, Data> {
        DhtHandle {
            id: self.id.clone(),
            tx: self.op_tx.clone(),
        }
    }

    /// Receive and reply to requests
    pub fn handle_req(
        &mut self,
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
                // Lookup the value
                if let Some(values) = self.datastore.find(id) {
                    debug!(
                        "FindValue request, {} values for id: {:?}",
                        values.len(),
                        id
                    );
                    Response::ValuesFound(id.clone(), values)
                } else {
                    debug!(
                        "FindValue request, no values found, returning closer nodes for id: {:?}",
                        id
                    );
                    let nodes = self.table.nearest(id, 0..self.config.k);
                    Response::NodesFound(id.clone(), nodes)
                }
            }
            Request::Store(id, value) => {
                // Write value to local storage
                let values = self.datastore.store(id, value);
                // Reply to confirm write was completed
                if !values.is_empty() {
                    debug!(
                        "Store request, stored {} values for id: {:?}",
                        values.len(),
                        id
                    );
                    Response::ValuesFound(id.clone(), values)
                } else {
                    debug!("Store request, ignored values for id: {:?}", id);
                    Response::NoResult
                }
            }
        };

        // Update record for sender
        self.table.create_or_update(from);

        Ok(resp)
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

impl<Id, Info, Data, Io, Table, Store> Unpin for Dht<Id, Info, Data, Io, Table, Store> {}

/// [Future] impl for polling and updating DHT state
impl<Id, Info, Data, Io, Table, Store> Future for Dht<Id, Info, Data, Io, Table, Store>
where
    Id: DatabaseId + Clone + Sized + Sync + Send + 'static,
    Info: PartialEq + Clone + Sized + Debug + Sync + Send + 'static,
    Data: PartialEq + Clone + Sized + Debug + Sync + Send + 'static,
    Io: Net<Id, Info, Data> + Clone + Debug + Send + 'static,
    Table: NodeTable<Id, Info> + Send + 'static,
    Store: Datastore<Id, Data> + Send + 'static,
{
    type Output = Result<(), Error>;

    // Poll calls internal update function
    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        // Poll for new operations
        if let Poll::Ready(Some((op, mut tx))) = self.op_rx.poll_next_unpin(ctx) {
            trace!("New op: {:?}", op);

            match op {
                OpReq::GetNearest(id) => {
                    let peers = self.nodetable().nearest(&id, 0..self.config.k);

                    if let Err(e) = tx.start_send_unpin(Ok(OpResp::Peers(peers))) {
                        warn!("Failed to send op response: {:?}", e);
                    }
                }
                OpReq::UpdatePeers(peers) => {
                    let t = self.nodetable_mut();
                    // TODO: return updated peer info?
                    for p in &peers {
                        t.create_or_update(p);
                    }

                    if let Err(e) = tx.start_send_unpin(Ok(OpResp::Peers(peers))) {
                        warn!("Failed to send op response: {:?}", e);
                    }
                }
                OpReq::Store(id, data) => {
                    let data = self.datastore_mut().store(&id, &data);

                    if let Err(e) = tx.start_send_unpin(Ok(OpResp::Data(data))) {
                        warn!("Failed to send op response: {:?}", e);
                    }
                }
                OpReq::Reduce(id, data) => {
                    // Apply reducer if available
                    let data = match &self.reducer {
                        Some(r) => r(id, data),
                        None => data,
                    };

                    if let Err(e) = tx.start_send_unpin(Ok(OpResp::Data(data))) {
                        warn!("Failed to send op response: {:?}", e);
                    }
                }
                OpReq::Net(peers, req) => {
                    let net = self.net.clone();
                    // TODO: use timeouts from config here
                    let t = Duration::from_secs(10);

                    // Create task for underlying network op
                    // TODO: maybe able to remove this and just pass the response channel down?
                    tokio::task::spawn(async move {
                        let r = match timeout(t, net.request(peers, req)).await {
                            Ok(Ok(v)) => Ok(OpResp::Net(v)),
                            Ok(Err(e)) => Err(e),
                            Err(_) => Err(Error::Timeout),
                        };

                        if let Err(e) = tx.send(r).await {
                            warn!("Failed to send op response: {:?}", e);
                        }
                    });
                }
                OpReq::Update(forced) => {
                    // Check for buckets in need of updates

                    // Fetch bucket info
                    let bucket_info: Vec<_> = (0..self.table.buckets())
                        .filter_map(|i| self.table.bucket_info(i))
                        .collect();

                    // TODO: issue pings and expire nodes

                    // Find oldest nodes in each bucket to issue pings or expire
                    let mut oldest_nodes = vec![];
                    for i in 0..self.table.buckets() {
                        // Lookup oldest node
                        let n = match self.table.oldest(i) {
                            Some(v) => v,
                            None => continue,
                        };

                        // If this is expired, remove it
                        // TODO: what if seen is none? make sure it's impossible to add a node without
                        // communicating it so this can never be none?
                        let now = Instant::now();
                        let seen = n.seen();
                        if seen.add(self.config.node_timeout) < now {
                            debug!(
                                "Expiring node: {:?} at {:?} (last seen {:?})",
                                n.id(),
                                now,
                                seen
                            );
                            self.table.remove_entry(n.id());
                        } else if seen.add(self.config.update_period) < now {
                            trace!("Ping node: {:?} at {:?}", n.id(), now);
                            oldest_nodes.push(n);
                        }
                    }

                    // Execute update queries
                    let h = self.get_handle();
                    let c = self.config.search_options();
                    let update_period = self.config.update_period;

                    tokio::task::spawn(async move {
                        // Issue ping to expiring nodes
                        if oldest_nodes.len() > 0 {
                            debug!("Ping {} nodes", oldest_nodes.len());

                            match h.net_req(oldest_nodes.clone(), Request::Ping).await {
                                Ok(resps) => {
                                    // Filter responders to update responding peers
                                    let responders: Vec<_> = oldest_nodes
                                        .drain(..)
                                        .filter(|n| resps.contains_key(n.id()))
                                        .map(|mut n| {
                                            n.set_seen(Instant::now());
                                            n
                                        })
                                        .collect();
                                    // Updated responding peers
                                    let _ = h.update_peers(responders).await;
                                }
                                Err(e) => {
                                    error!("Failed to issue ping: {:?}", e);
                                }
                            }
                        }

                        // Execute bucket updates
                        // TODO: should this be independent of per-node updates?
                        for (i, info) in bucket_info.iter().enumerate() {
                            // TODO: ping and expire oldest nodes in buckets

                            // Check for bucket update timeouts
                            match info.updated {
                                Some(t) if !forced && t.add(update_period) > Instant::now() => {
                                    continue
                                }
                                _ => (),
                            }

                            debug!("Update bucket {} ({:?})", i, info.id);

                            // Fire lookup to bucket address to find nearby nodes
                            // this will fail as the bucket id is likely not a node,
                            // but achieve the goal of updating the table around the bucket
                            let _ = h.lookup(info.id.clone(), c.clone()).await;
                        }

                        // TODO: return update info?
                        if let Err(e) = tx.send(Ok(OpResp::Peers(vec![]))).await {
                            warn!("Failed to send op response: {:?}", e);
                        }
                    });
                }
            }

            // Force wake next cycle to make sure we re-poll on available ops
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
        config.update_period = Duration::from_secs(0);
        mock_dht!($connector, $root, $dht, config);
    };
    ($connector: ident, $root: ident, $dht:ident, $config:ident) => {
        let mut $dht =
            Dht::<[u8; 1], _, u64>::standard($root.id().clone(), $config, $connector.clone());
    };
}

#[cfg(test)]
mod dht_tests {
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
            dht.handle_req(&friend, &Request::Ping).unwrap(),
            Response::NoResult,
        );

        // Adds node to appropriate k bucket
        let friend1 = dht.table.contains(friend.id()).unwrap();

        // Second ping
        assert_eq!(
            dht.handle_req(&friend, &Request::Ping).unwrap(),
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
            dht.handle_req(&friend, &Request::Ping).unwrap(),
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
            dht.handle_req(&friend, &Request::FindNode(*other.id()))
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
            dht.handle_req(&other, &Request::FindValue([201])).unwrap(),
            Response::NodesFound([201], vec![friend.clone()]),
        );

        // Add value to store
        dht.datastore.store(&[201], &vec![1337]);

        // FindValues
        assert_eq!(
            dht.handle_req(&other, &Request::FindValue([201])).unwrap(),
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
            dht.handle_req(&friend, &Request::Store([2], vec![1234]))
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
