/**
 * rust-kad
 * Kademlia core implementation
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */


use std::marker::{PhantomData};
use std::fmt::{Debug};
use std::time::Instant;
use std::ops::Add;

use futures::prelude::*;
use futures::{future};

use crate::{Config};

use crate::common::*;

use crate::table::NodeTable;
use crate::store::{Datastore};

use crate::connector::{Connector, request_all};

pub mod search;
pub use self::search::{Search, Operation};

#[derive(Clone)]
pub struct Dht<Id, Info, Data, ReqId, Conn, Table, Store, Ctx> {
    id: Id,
    
    config: Config,
    table: Table,
    conn_mgr: Conn,
    datastore: Store,

    _addr: PhantomData<Info>,
    _data: PhantomData<Data>,
    _req_id: PhantomData<ReqId>,
    _ctx: PhantomData<Ctx>,
}

impl <Id, Info, Data, ReqId, Conn, Table, Store, Ctx> Dht<Id, Info, Data, ReqId, Conn, Table, Store, Ctx> 
where 
    Id: DatabaseId + Clone + Sized + Send + 'static,
    Info: PartialEq + Clone + Sized  + Debug+ Send + 'static,
    Data: PartialEq + Clone + Sized + Debug + Send + 'static,
    ReqId: RequestId + Clone + Sized + Debug + Send + 'static,
    Conn: Connector<Id, Info, Data, ReqId, Ctx> + Clone + Send + 'static,
    Table: NodeTable<Id, Info> + Clone + Send + 'static,
    Store: Datastore<Id, Data> + Clone + Send + 'static,
    Ctx: Clone + Debug + PartialEq + Send + 'static,
{
    /// Create a new generic DHT
    pub fn new(id: Id, config: Config, table: Table, conn_mgr: Conn, datastore: Store) -> Dht<Id, Info, Data, ReqId, Conn, Table, Store, Ctx> {
        Dht{
            id,
            config, 
            table, 
            conn_mgr, 
            datastore, 

            _addr: PhantomData, 
            _data: PhantomData, 
            _req_id: PhantomData, 
            _ctx: PhantomData,
        }
    }

    /// Connect to a known node
    /// This is used for bootstrapping onto the DHT, however requires a complete Entry
    /// due to limitations of the underlying connector abstraction.
    /// If this is unsuitable, manually issue an initial FindNode request and use the 
    /// handle_connect_response method to complete the connection.
    pub async fn connect(&mut self, target: Entry<Id, Info>, ctx: Ctx) -> Result<Vec<Entry<Id, Info>>, Error> {
        let id = self.id.clone();
        let mut s = self.clone();

        info!(target: "dht", "[DHT connect] {:?} to: {:?} at: {:?}", id, target.id(), target.info());

        // Launch request
        let mut conn = self.conn_mgr.clone();

        let resp = conn.request(ctx.clone(), ReqId::generate(), target.clone(), Request::FindNode(self.id.clone())).await?;

        s.handle_connect_response(target, resp, ctx).await
    }

    /// Handle the response to an initial FindNodes request.
    /// This is used to bootstrap a connection where the `.connect` method is unsuitable.
    pub async fn handle_connect_response(&mut self, target: Entry<Id, Info>, resp: Response<Id, Info, Data>, ctx: Ctx) -> Result<Vec<Entry<Id, Info>>, Error> {
        let (id, found) = match resp {
            Response::NodesFound(id, nodes) => (id, nodes),
            Response::NoResult => {
                warn!("[DHT connect] Received NoResult response from {:?}", target.id());
                return Ok(vec![])
            },
            _ => {
                warn!("[DHT connect] invalid response from: {:?}", target.id());
                return Err(Error::InvalidResponse)
            },
        };

        // Check ID matches self 
        if id != self.id {
            return Err(Error::InvalidResponseId)
        }

        // Add responding target to table
        // This only occurs after a response as there's no point adding a non-responding
        // node to the DHT
        let _table = self.table.clone();
        self.table.create_or_update(&target);

        trace!("[DHT connect] response received, searching {} nodes", found.len());

        // Create a FIND_NODE search on own id with responded nodes
        // This both registers this node with peers, and finds and relevant closer peers
        let search = self.search(id, Operation::FindNode, &found, ctx).await?;

        // We don't care about the search result here
        // Generally it should be that discovered nodes > 0, however not for the first bootstrapping...

        // TODO: Refresh all k-buckets further away than our nearest neighbor

        // Return newly discovered nodes
        Ok(search.all_completed())

    }

    /// Look up a node in the database by Id
    pub async fn lookup(&mut self, target: Id, ctx: Ctx) -> Result<Entry<Id, Info>, Error> {
        // Create a search instance
        let mut search = Search::new(self.id.clone(), target.clone(), Operation::FindNode, self.config.clone(), self.table.clone(), self.conn_mgr.clone(), ctx);

        // Execute across K nearest nodes
        let nearest: Vec<_> = self.table.nearest(&target, 0..self.config.concurrency);
        search.seed(&nearest);

        // Execute the recursive search
        let r = search.execute().await;
        
        // Handle internal search errors
        let s = match r {
            Err(e) => return Err(e),
            Ok(s) => s,
        };

        // Return node if found
        let known = search.known();
        if let Some((n, _s)) = known.get(search.target()) {
            Ok(n.clone())
        } else {
            Err(Error::NotFound)
        }
    }


    /// Find a value from the DHT
    pub async fn find(&mut self, target: Id, ctx: Ctx) -> Result<Vec<Data>, Error> {
        let mut existing = self.datastore.find(&target);

        // Create a search instance
        let mut search = Search::new(self.id.clone(), target.clone(), Operation::FindValue, self.config.clone(), self.table.clone(), self.conn_mgr.clone(), ctx);

        // Execute across K nearest nodes
        let nearest: Vec<_> = self.table.nearest(&target, 0..self.config.concurrency);
        search.seed(&nearest);

        if nearest.len() == 0 {
            return Err(Error::NoPeers)
        }

        // Execute the recursive search
        let r = search.execute().await;

        // Handle internal search errors
        let s = match r {
            Err(e) => return Err(e),
            Ok(s) => s,
        };

        // Return data if found
        let data = search.data();
        if data.len() == 0 {
            return Err(Error::NotFound)
        }

        // TODO: Reduce data before returning? (should be done on insertion anyway..?)
        let mut flat_data: Vec<Data> = data.iter().flat_map(|(_k, v)| v.clone() ).collect();

        // Append existing data (non-flattened atm)
        if let Some(existing) = &mut existing {
            flat_data.append(existing)
        }

        // TODO: cache data locally for next search

        // TODO: Send updates to any peers that returned outdated data?

        // TODO: forward reduced k:v pairs to closest node in map (that did not return value)

        Ok(flat_data)
    }


    /// Store a value in the DHT
    pub async fn store(&mut self, target: Id, data: Vec<Data>, ctx: Ctx) -> Result<usize, Error> {
        // Update local data
        let _values = self.datastore.store(&target, &data);

        // Create a search instance
        let mut search = Search::new(self.id.clone(), target.clone(), Operation::FindNode, self.config.clone(), self.table.clone(), self.conn_mgr.clone(), ctx.clone());

        // Execute across K nearest nodes
        let nearest: Vec<_> = self.table.nearest(&target, 0..self.config.concurrency);
        search.seed(&nearest);

        let conn = self.conn_mgr.clone();
        let k = self.config.k;

        // Search for K closest nodes to value
        let r = search.execute().await?;

        // Send store request to found nodes
        let known = search.completed(0..k);
        trace!("sending store to: {:?}", known);
        let resp = request_all(conn, ctx, &Request::Store(target, data), &known).await?;

        // TODO: should we process success here?
        Ok(resp.len())
    }


    /// Refresh buckets and node table entries
    pub async fn refresh(&mut self, ctx: Ctx) -> Result<(), ()> {
        
        // TODO: send refresh to buckets that haven't been looked up recently
        // How to track recently looked up / contacted buckets..?

        // Evict "expired" nodes from buckets
        // Message oldest node, if no response evict
        // Maybe this could be implemented as a periodic ping and timeout instead?
        let timeout = self.config.node_timeout;
        let oldest: Vec<_> = self.table.iter_oldest().filter(move |o| {
            if let Some(seen) = o.seen() {
                seen.add(timeout) < Instant::now()
            } else {
                true
            }            
        }).collect();

        let mut pings = Vec::with_capacity(oldest.len());

        

        for o in oldest {
            let mut t = self.table.clone();
            let mut o = o.clone();

            let mut conn = self.conn_mgr.clone();
            let ctx_ = ctx.clone();

            let p = async move {
                let res = conn.request(ctx_, ReqId::generate(), o.clone(), Request::Ping).await;
                match res {
                    Ok(_resp) => {
                        debug!("[DHT refresh] updating node: {:?}", o);
                        o.set_seen(Instant::now());
                        t.create_or_update(&o);
                    },
                    Err(_e) => {
                        debug!("[DHT refresh] expiring node: {:?}", o);
                        t.remove_entry(o.id());
                    },
                }

                ()
            };

            pings.push(p);
        }

        future::join_all(pings).await;

        Ok(())
    }

    /// Receive and reply to requests
    pub fn handle(&mut self, from: &Entry<Id, Info>, req: &Request<Id, Data>) -> Result<Response<Id, Info, Data>, Error> {
        // Build response
        let resp = match req {
            Request::Ping => {
                Response::NoResult
            },
            Request::FindNode(id) => {
                let nodes = self.table.nearest(id, 0..self.config.k);
                Response::NodesFound(id.clone(), nodes)
            },
            Request::FindValue(id) => {
                // Lockup the value
                if let Some(values) = self.datastore.find(id) {
                    Response::ValuesFound(id.clone(), values)
                } else {
                    let nodes = self.table.nearest(id, 0..self.config.k);
                    Response::NodesFound(id.clone(), nodes)
                }                
            },
            Request::Store(id, value) => {
                // Write value to local storage
                let values = self.datastore.store(id, value);
                // Reply to confirm write was completed
                if values.len() != 0 {
                    Response::ValuesFound(id.clone(), values)
                } else {
                    Response::NoResult
                }
            },
        };

        // Update record for sender
        self.table.create_or_update(from);

        Ok(resp)
    }

    #[cfg(test)]
    pub fn contains(&mut self, id: &Id) -> Option<Entry<Id, Info>> {
        self.table.contains(id)
    }

    /// Create a basic search using the DHT
    /// This is provided for integration of the Dht with other components
    pub async fn search(&mut self, id: Id, op: Operation, seed: &[Entry<Id, Info>], ctx: Ctx) -> Result<Search<Id, Info, Data, Table, Conn, ReqId, Ctx>, Error> {
        let mut search = Search::new(self.id.clone(), id, op, self.config.clone(), self.table.clone(), self.conn_mgr.clone(), ctx);
        search.seed(seed);

        trace!("Starting search with nodes: ");
        for e in seed {
            trace!("    {:?} - {:?}", e.id(), e.info());
        }

        search.execute().await?;

        Ok(search)
    }
}


/// Helper macro to setup DHT instances for testing
#[cfg(test)] #[macro_export]
#[cfg(test)] macro_rules! mock_dht {
    ($connector: ident, $root: ident, $dht:ident) => {
        let mut config = Config::default();
        config.concurrency = 2;
        mock_dht!($connector, $root, $dht, config);
    };
    ($connector: ident, $root: ident, $dht:ident, $config:ident) => {
        let table = KNodeTable::new($root.id().clone(), $config.k, $config.hash_size);
        
        let store: HashMapStore<[u8; 1], u64> = HashMapStore::new();
        
        let mut $dht = Dht::<[u8; 1], u64, _, u64, _, _, _, ()>::new($root.id().clone(), 
                $config, table, $connector.clone(), store);
    }
}

#[cfg(test)]
mod tests {
    use std::clone::Clone;
    use std::time::Duration;

    extern crate futures;
    use futures::executor::block_on;

    use super::*;
    use crate::store::{HashMapStore, Datastore};
    use crate::table::{NodeTable, KNodeTable};

    use rr_mux::mock::{MockConnector, MockTransaction as Mt};

    #[test]
    fn test_receive_common() {

        let root = Entry::new([0], 001);
        let friend = Entry::new([1], 002);

        let mut connector = MockConnector::new().expect(vec![]);
        mock_dht!(connector, root, dht);

        // Check node is unknown
        assert!(dht.table.contains(friend.id()).is_none());

        // Ping
        assert_eq!(
            dht.handle(&friend, &Request::Ping).unwrap(),
            Response::NoResult,
        );

        // Adds node to appropriate k bucket
        let friend1 = dht.table.contains(friend.id()).unwrap();

        // Second ping
        assert_eq!(
            dht.handle(&friend, &Request::Ping).unwrap(),
            Response::NoResult,
        );

        // Updates node in appropriate k bucket
        let friend2 = dht.table.contains(friend.id()).unwrap();
        assert_ne!(friend1.seen(), friend2.seen());

        // Check expectations are done
        connector.finalise();
    }

    #[test]
    fn test_receive_ping() {

        let root = Entry::new([0], 001);
        let friend = Entry::new([1], 002);

        let mut connector = MockConnector::new().expect(vec![]);
        mock_dht!(connector, root, dht);

        // Ping
        assert_eq!(
            dht.handle(&friend, &Request::Ping).unwrap(),
            Response::NoResult,
        );

        // Check expectations are done
        connector.finalise();
    }

    #[test]
    fn test_receive_find_nodes() {

        let root = Entry::new([0], 001);
        let friend = Entry::new([1], 002);
        let other = Entry::new([2], 003);

        let mut connector = MockConnector::new().expect(vec![]);
        mock_dht!(connector, root, dht);

        // Add friend to known table
        dht.table.create_or_update(&friend);

        // FindNodes
        assert_eq!(
            dht.handle(&friend, &Request::FindNode(other.id().clone())).unwrap(),
            Response::NodesFound(other.id().clone(), vec![friend.clone()]), 
        );

        // Check expectations are done
        connector.finalise();
    }

        #[test]
    fn test_receive_find_values() {

        let root = Entry::new([0], 001);
        let friend = Entry::new([1], 002);
        let other = Entry::new([2], 003);

        let mut connector = MockConnector::new().expect(vec![]);
        mock_dht!(connector, root, dht);

        // Add friend to known table
        dht.table.create_or_update(&friend);

        // FindValues (unknown, returns NodesFound)
        assert_eq!(
            dht.handle(&other, &Request::FindValue([201])).unwrap(),
            Response::NodesFound([201], vec![friend.clone()]), 
        );

        // Add value to store
        dht.datastore.store(&[201], &vec![1337]);
        
        // FindValues
        assert_eq!(
            dht.handle(&other, &Request::FindValue([201])).unwrap(),
            Response::ValuesFound([201], vec![1337]), 
        );

        // Check expectations are done
        connector.finalise();
    }

    #[test]
    fn test_receive_store() {

        let root = Entry::new([0], 001);
        let friend = Entry::new([1], 002);

        let mut connector = MockConnector::new().expect(vec![]);
        mock_dht!(connector, root, dht);

        // Store
        assert_eq!(
            dht.handle(&friend, &Request::Store([2], vec![1234])).unwrap(),
            Response::ValuesFound([2], vec![1234]),
        );

        let v = dht.datastore.find(&[2]).expect("missing value");
        assert_eq!(v, vec![1234]);

        // Check expectations are done
        connector.finalise();
    }

    #[test]
    fn test_expire() {
        let mut config = Config::default();
        config.node_timeout = Duration::from_millis(200);

        let root = Entry::new([0], 001);
        let n1 = Entry::new([1], 002);
        let n2 = Entry::new([2], 003);

        let mut connector = MockConnector::new().expect(vec![]);
        let c = config.clone();
        mock_dht!(connector, root, dht, c);

        // Add known nodes
        dht.table.create_or_update(&n1);
        dht.table.create_or_update(&n2);

        // No timed out nodes
        block_on( dht.refresh(()) ).unwrap();
        connector.finalise();

        std::thread::sleep(config.node_timeout * 2);

        // Ok response
        connector.expect(vec![
            Mt::request(n1.clone(), Request::Ping, Ok((Response::NoResult, ()))),
            Mt::request(n2.clone(), Request::Ping, Ok((Response::NoResult, ()))),
        ]);
        block_on( dht.refresh(()) ).unwrap();

        assert!(dht.table.contains(n1.id()).is_some());
        assert!(dht.table.contains(n2.id()).is_some());

        connector.finalise();

        std::thread::sleep(config.node_timeout * 2);

        // No response (evict)
        connector.expect(vec![
            Mt::request(n1.clone(), Request::Ping, Ok((Response::NoResult, ()))),
            Mt::request(n2.clone(), Request::Ping, Err(Error::Timeout) ),
        ]);
        block_on( dht.refresh(()) ).unwrap();

        assert!(dht.table.contains(n1.id()).is_some());
        assert!(dht.table.contains(n2.id()).is_none());

        // Check expectations are done
        connector.finalise();
    }

    #[test]
    fn test_clone() {

        let root = Entry::new([0], 001);
        let _friend = Entry::new([1], 002);

        let connector = MockConnector::new().expect(vec![]);
        mock_dht!(connector, root, dht);

        let _ = &mut dht;

        let _ = dht.clone();
    }

}
