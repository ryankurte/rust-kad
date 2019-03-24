/**
 * rust-kad
 * Kademlia core implementation
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */


use std::marker::{PhantomData};
use std::fmt::{Debug};

use futures::prelude::*;
use futures::future;

use rr_mux::{Connector};

use crate::{Config};

use crate::common::*;

use crate::table::NodeTable;
use crate::store::{Datastore};

use crate::connector::{request_all};

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
    Id: DatabaseId + Clone + Send + 'static,
    Info: PartialEq + Clone + Debug + Send + 'static,
    Data: PartialEq + Clone + Send + Debug + 'static,
    ReqId: RequestId + Clone + Send + 'static,
    Conn: Connector<ReqId, Entry<Id, Info>, Request<Id, Data>, Response<Id, Info, Data>, Error, Ctx> + Clone + Send + 'static,
    Table: NodeTable<Id, Info> + Clone + Sync + Send + 'static,
    Store: Datastore<Id, Data> + Clone + Sync + Send + 'static,
    Ctx: Clone + Debug + PartialEq + Send + 'static,
{
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
    /// This is used for bootstrapping onto the DHT
    pub fn connect(&mut self, target: Entry<Id, Info>, ctx: Ctx) -> impl Future<Item=(), Error=Error> + '_ {
        let table = self.table.clone();
        let conn_mgr = self.conn_mgr.clone();
        let id = self.id.clone();

        info!(target: "dht", "[DHT connect] {:?} to: {:?} at: {:?}", id, target.id(), target.info());

        // Launch request
        self.conn_mgr.clone().request(ctx.clone(), ReqId::generate(), target.clone(), Request::FindNode(self.id.clone()))
            .and_then(move |(resp, _ctx)| {
                // Check for correct response
                match resp {
                    Response::NodesFound(_id, nodes) => future::ok((target, nodes)),
                    _ => {
                        warn!("[DHT connect] invalid response from: {:?}", target.id());
                        future::err(Error::InvalidResponse)
                    },
                }
            }).and_then(move |(target, found)| {
                // Add responding target to table
                // This only occurs after a response as there's no point adding a non-responding
                // node to the DHT
                table.clone().update(&target);

                debug!("[DHT connect] response received, searching {} nodes", found.len());

                // Perform FIND_NODE on own id with responded nodes to register self
                let mut search = Search::new(self.id.clone(), id, Operation::FindNode, self.config.clone(), table, conn_mgr, ctx);
                search.seed(&found);

                search.execute()
            }).and_then(|_s| {
                // We don't care about the search result here
                // Generally it should be that discovered nodes > 0, however not for the first bootstrapping...

                // TODO: Refresh all k-buckets further away than our nearest neighbor

                future::ok(())
            })
    }




    /// Look up a node in the database by Id
    pub fn lookup(&mut self, target: Id, ctx: Ctx) -> impl Future<Item=Entry<Id, Info>, Error=Error> + '_ {
        // Create a search instance
        let mut search = Search::new(self.id.clone(), target.clone(), Operation::FindNode, self.config.clone(), self.table.clone(), self.conn_mgr.clone(), ctx);

        // Execute across K nearest nodes
        let nearest: Vec<_> = self.table.nearest(&target, 0..self.config.concurrency);
        search.seed(&nearest);

        // Execute the recursive search
        search.execute().then(|r| {
            // Handle internal search errors
            let s = match r {
                Err(e) => return Err(e),
                Ok(s) => s,
            };

            // Return node if found
            let known = s.known();
            if let Some((n, _s)) = known.get(s.target()) {
                Ok(n.clone())
            } else {
                Err(Error::NotFound)
            }
        })
    }


    /// Find a value from the DHT
    pub fn find(&mut self, target: Id, ctx: Ctx) -> impl Future<Item=Vec<Data>, Error=Error> {
        // Create a search instance
        let mut search = Search::new(self.id.clone(), target.clone(), Operation::FindValue, self.config.clone(), self.table.clone(), self.conn_mgr.clone(), ctx);

        // Execute across K nearest nodes
        let nearest: Vec<_> = self.table.nearest(&target, 0..self.config.concurrency);
        search.seed(&nearest);

        // Execute the recursive search
        search.execute()
        .then(|r| {
            // Handle internal search errors
            let s = match r {
                Err(e) => return Err(e),
                Ok(s) => s,
            };

            // Return data if found
            let data = s.data();
            if data.len() == 0 {
                return Err(Error::NotFound)
            }

            // TODO: Reduce data before returning? (should be done on insertion anyway..?)
            let mut flat_data: Vec<Data> = data.iter().flat_map(|(_k, v)| v.clone() ).collect();

            // TODO: Send updates to any peers that returned outdated data?

            // TODO: forward reduced k:v pairs to closest node in map (that did not return value)

            Ok(flat_data)
        })
    }


    /// Store a value in the DHT
    pub fn store(&mut self, target: Id, data: Vec<Data>, ctx: Ctx) -> impl Future<Item=(), Error=Error> {
        // Create a search instance
        let mut search = Search::new(self.id.clone(), target.clone(), Operation::FindNode, self.config.clone(), self.table.clone(), self.conn_mgr.clone(), ctx.clone());

        // Execute across K nearest nodes
        let nearest: Vec<_> = self.table.nearest(&target, 0..self.config.concurrency);
        search.seed(&nearest);

        let conn = self.conn_mgr.clone();
        let k = self.config.k;

        // Search for K closest nodes to value
        search.execute()
        .and_then(move |r| {
            // Send store request to found nodes
            let known = r.completed(0..k);
            debug!("sending store to: {:?}", known);
            request_all(conn, ctx, &Request::Store(target, data), &known)
        }).and_then(|_| {
            // TODO: should we process success here?
            Ok(())
        })
    }


    /// Refresh node table
    pub fn refresh(&mut self) -> impl Future<Item=(), Error=Error> {
        // TODO: send refresh to buckets that haven't been looked up recently
        // How to track recently looked up / contacted buckets..?

        // TODO: evict "expired" nodes from buckets
        // Message oldest node, if no response evict
        // Maybe this could be implemented as a periodic ping and timeout instead?

        future::err(Error::Unimplemented)
    }

    /// Receive and reply to requests
    pub fn receive(&mut self, from: &Entry<Id, Info>, req: &Request<Id, Data>) -> impl Future<Item=Response<Id, Info, Data>, Error=Error> {
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
                self.datastore.store(id, value);
                // Reply to confirm write was completed
                Response::NoResult
            },
        };

        // Update record for sender
        self.table.update(from);

        future::ok(resp)
    }

    #[cfg(test)]
    pub fn contains(&mut self, id: &Id) -> Option<Entry<Id, Info>> {
        self.table.contains(id)
    }

    /// Create a basic search using the DHT
    /// This is provided for integration of the Dht with other components
    pub fn search(&mut self, id: Id, op: Operation, seed: &[Entry<Id, Info>], ctx: Ctx) -> impl Future<Item=Search<Id, Info, Data, Table, Conn, ReqId, Ctx>, Error=Error> {
        let mut search = Search::new(self.id.clone(), id, op, self.config.clone(), self.table.clone(), self.conn_mgr.clone(), ctx);
        search.seed(seed);

        info!("Starting search with nodes: ");
        for e in seed {
            info!("{:?}", e.info());
        }

        search.execute()
    }
}

/// Stream trait implemented to allow polling on dht object
impl <Id, Info, Data, ReqId, Conn, Table, Store, Ctx> Future for Dht <Id, Info, Data, ReqId, Conn, Table, Store, Ctx> {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        // TODO: poll / update internal state
        Ok(Async::NotReady)
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
        
        let store: HashMapStore<u64, u64> = HashMapStore::new();
        
        let mut $dht = Dht::<u64, u64, _, u64, _, _, _, ()>::new($root.id().clone(), 
                $config, table, $connector.clone(), store);
    }
}

#[cfg(test)]
mod tests {
    use std::clone::Clone;

    use super::*;
    use crate::store::{HashMapStore, Datastore};
    use crate::table::{NodeTable, KNodeTable};

    use rr_mux::mock::{MockConnector};

    #[test]
    fn test_receive_common() {

        let root = Entry::new(0, 001);
        let friend = Entry::new(1, 002);

        let mut connector = MockConnector::new().expect(vec![]);
        mock_dht!(connector, root, dht);

        // Check node is unknown
        assert!(dht.table.contains(friend.id()).is_none());

        // Ping
        assert_eq!(
            dht.receive(&friend, &Request::Ping).wait().unwrap(),
            Response::NoResult,
        );

        // Adds node to appropriate k bucket
        let friend1 = dht.table.contains(friend.id()).unwrap();

        // Second ping
        assert_eq!(
            dht.receive(&friend, &Request::Ping).wait().unwrap(),
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

        let root = Entry::new(0, 001);
        let friend = Entry::new(1, 002);

        let mut connector = MockConnector::new().expect(vec![]);
        mock_dht!(connector, root, dht);

        // Ping
        assert_eq!(
            dht.receive(&friend, &Request::Ping).wait().unwrap(),
            Response::NoResult,
        );

        // Check expectations are done
        connector.finalise();
    }

    #[test]
    fn test_receive_find_nodes() {

        let root = Entry::new(0, 001);
        let friend = Entry::new(1, 002);
        let other = Entry::new(2, 003);

        let mut connector = MockConnector::new().expect(vec![]);
        mock_dht!(connector, root, dht);

        // Add friend to known table
        dht.table.update(&friend);

        // FindNodes
        assert_eq!(
            dht.receive(&friend, &Request::FindNode(other.id().clone())).wait().unwrap(),
            Response::NodesFound(other.id().clone(), vec![friend.clone()]), 
        );

        // Check expectations are done
        connector.finalise();
    }

        #[test]
    fn test_receive_find_values() {

        let root = Entry::new(0, 001);
        let friend = Entry::new(1, 002);
        let other = Entry::new(2, 003);

        let mut connector = MockConnector::new().expect(vec![]);
        mock_dht!(connector, root, dht);

        // Add friend to known table
        dht.table.update(&friend);

        // FindValues (unknown, returns NodesFound)
        assert_eq!(
            dht.receive(&other, &Request::FindValue(201)).wait().unwrap(),
            Response::NodesFound(201, vec![friend.clone()]), 
        );

        // Add value to store
        dht.datastore.store(&201, &vec![1337]);
        
        // FindValues
        assert_eq!(
            dht.receive(&other, &Request::FindValue(201)).wait().unwrap(),
            Response::ValuesFound(201, vec![1337]), 
        );

        // Check expectations are done
        connector.finalise();
    }

    #[test]
    fn test_receive_store() {

        let root = Entry::new(0, 001);
        let friend = Entry::new(1, 002);

        let mut connector = MockConnector::new().expect(vec![]);
        mock_dht!(connector, root, dht);

        // Store
        assert_eq!(
            dht.receive(&friend, &Request::Store(2, vec![1234])).wait().unwrap(),
            Response::NoResult,
        );

        let v = dht.datastore.find(&2).expect("missing value");
        assert_eq!(v, vec![1234]);

        // Check expectations are done
        connector.finalise();
    }

    #[test]
    fn test_clone() {

        let root = Entry::new(0, 001);
        let _friend = Entry::new(1, 002);

        let connector = MockConnector::new().expect(vec![]);
        mock_dht!(connector, root, dht);

        let _ = dht.clone();
    }

}
