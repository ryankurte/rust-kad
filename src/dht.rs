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


use futures::{future, Future, Stream};

use futures_timer::{FutureExt};

use crate::{Config, Node, DatabaseId, NodeTable, ConnectionManager, DhtError};
use crate::{Request, Response};
use crate::datastore::Datastore;
use crate::search::{Search, Operation};

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

impl <ID, ADDR, DATA, TABLE, CONN, STORE> Dht<ID, ADDR, DATA, TABLE, CONN, STORE> 
where 
    ID: DatabaseId + 'static,
    ADDR: Clone + Debug + 'static,
    DATA: Clone + Debug + 'static,
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


    /// Connect to a known node
    /// This is used for bootstrapping onto the DHT
    pub fn connect(&mut self, target: Node<ID, ADDR>) -> impl Future<Item=Node<ID, ADDR>, Error=DhtError> {
        let table = self.table.clone();
        // Launch request
        self.conn_mgr.clone().request(&target, Request::FindNode(self.id.clone()))
        .timeout(self.config.timeout)
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

    /// Look up a node in the database by ID
    pub fn lookup(&mut self, target: ID) -> impl Future<Item=Node<ID, ADDR>, Error=DhtError> + '_ {

        // Create a search instance
        let search = Search::new(target.clone(), Operation::FindNode, 2, 2, |t, k, _| { k.get(t).is_some() }, self.table.clone(), self.conn_mgr.clone());

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
                Err(DhtError::NotFound)
            }
        })
    }


    /// Refresh node table
    pub fn refresh(&mut self) -> impl Future<Item=(), Error=DhtError> {
        // TODO: send refresh to buckets that haven't been looked up recently
        // How to track recently looked up / contacted buckets..?

        // TODO: evict "expired" nodes from buckets
        // Message oldest node, if no response evict
        // Maybe this could be implemented as a periodic ping and timeout instead?

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
                self.conn_mgr.request(&oldest, Request::Ping).timeout(self.config.timeout)
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

    pub fn receive(&mut self, from: &Node<ID, ADDR>, req: &Request<ID, DATA>) -> Response<ID, ADDR, DATA> {
        // Build response
        let resp = match req {
            Request::Ping => {
                Response::NoResult
            },
            Request::FindNode(id) => {
                let nodes = self.table.lock().unwrap().nearest(id, 0..self.config.k);
                Response::NodesFound(nodes)
            },
            Request::FindValue(id) => {
                // Lockup the value
                if let Some(values) = self.datastore.lock().unwrap().find(id) {
                    Response::ValuesFound(values)
                } else {
                    let nodes = self.table.lock().unwrap().nearest(id, 0..self.config.k);
                    Response::NodesFound(nodes)
                }                
            },
            Request::Store(id, value) => {
                // Write value to local storage
                self.datastore.lock().unwrap().store(id, value);
                // Reply to confirm write was completed
                Response::NoResult
            },
        };

        // Update record for sender
        self.table.lock().unwrap().update(from);

        resp
    }

    #[cfg(test)]
    pub fn contains(&mut self, id: &ID) -> Option<Node<ID, ADDR>> {
        self.table.lock().unwrap().contains(id)
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

#[cfg(test)]
macro_rules! mock_dht {
    ($connector: ident, $root: ident, $dht:ident) => {
        let mut config = Config::default();
        config.concurrency = 2;

        let table = KNodeTable::new(&$root, config.k, config.hash_size);
        
        let store: HashMapStore<u64, u64> = HashMapStore::new();
        
        let mut $dht = Dht::<u64, u64, _, _, _, _>::new($root.id().clone(), $root.address().clone(), 
                config, table, $connector.clone(), store);
    }
}


#[cfg(test)]
mod tests {
    use std::clone::Clone;

    use super::*;
    use crate::datastore::{HashMapStore, Datastore};
    use crate::nodetable::{NodeTable, KNodeTable};
    use crate::mock::{MockConnector};

     #[test]
    fn test_receive_common() {

        let root = Node::new(0, 001);
        let friend = Node::new(1, 002);

        let connector = MockConnector::from(vec![]);
        mock_dht!(connector, root, dht);

        // Check node is unknown
        assert!(dht.table.lock().unwrap().find(friend.id()).is_none());

        // Ping
        assert_eq!(
            dht.receive(&friend, &Request::Ping),
            Response::NoResult,
        );

        // Adds node to appropriate k bucket
        let friend1 = dht.table.lock().unwrap().find(friend.id()).unwrap();

        // Second ping
        assert_eq!(
            dht.receive(&friend, &Request::Ping),
            Response::NoResult,
        );

        // Updates node in appropriate k bucket
        let friend2 = dht.table.lock().unwrap().find(friend.id()).unwrap();
        assert_ne!(friend1.seen(), friend2.seen());

        // Check expectations are done
        connector.done();
    }

    #[test]
    fn test_receive_ping() {

        let root = Node::new(0, 001);
        let friend = Node::new(1, 002);

        let connector = MockConnector::from(vec![]);
        mock_dht!(connector, root, dht);

        // Ping
        assert_eq!(
            dht.receive(&friend, &Request::Ping),
            Response::NoResult,
        );

        // Check expectations are done
        connector.done();
    }

    #[test]
    fn test_receive_find_nodes() {

        let root = Node::new(0, 001);
        let friend = Node::new(1, 002);
        let other = Node::new(2, 003);

        let connector = MockConnector::from(vec![]);
        mock_dht!(connector, root, dht);

        // Add friend to known table
        dht.table.lock().unwrap().update(&friend);

        // FindNodes
        assert_eq!(
            dht.receive(&friend, &Request::FindNode(other.id().clone())),
            Response::NodesFound(vec![friend.clone()]), 
        );

        // Check expectations are done
        connector.done();
    }

        #[test]
    fn test_receive_find_values() {

        let root = Node::new(0, 001);
        let friend = Node::new(1, 002);
        let other = Node::new(2, 003);

        let connector = MockConnector::from(vec![]);
        mock_dht!(connector, root, dht);

        // Add friend to known table
        dht.table.lock().unwrap().update(&friend);

        // FindValues (unknown, returns NodesFound)
        assert_eq!(
            dht.receive(&other, &Request::FindValue(201)),
            Response::NodesFound(vec![friend.clone()]), 
        );

        // Add value to store
        dht.datastore.lock().unwrap().insert(201, vec![1337]);
        
        // FindValues
        assert_eq!(
            dht.receive(&other, &Request::FindValue(201)),
            Response::ValuesFound(vec![1337]), 
        );

        // Check expectations are done
        connector.done();
    }

    #[test]
    fn test_receive_store() {

        let root = Node::new(0, 001);
        let friend = Node::new(1, 002);

        let connector = MockConnector::from(vec![]);
        mock_dht!(connector, root, dht);

        // Store
        assert_eq!(
            dht.receive(&friend, &Request::Store(2, vec![1234])),
            Response::NoResult,
        );

        let v = dht.datastore.lock().unwrap().find(&2).expect("missing value");
        assert_eq!(v, vec![1234]);

        // Check expectations are done
        connector.done();
    }

}
