/**
 * rust-kad
 * Integration / external library user tests
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */

use std::fmt::Debug;
use std::collections::{HashMap};
use std::sync::{Arc, Mutex};
use std::marker::PhantomData;

extern crate kad;
use kad::prelude::*;

use kad::Config;
use kad::dht::Dht;
use kad::nodetable::KNodeTable;
use kad::node::Node;
use kad::id::DatabaseId;
use kad::error::Error as DhtError;
use kad::message::{Request, Response};
use kad::datastore::{HashMapStore, Reducer};

extern crate futures;
use futures::prelude::*;
use futures::future;

extern crate rr_mux;
use rr_mux::Connector;

type MockPeer<ID, ADDR, DATA> = Dht<ID, ADDR, DATA, KNodeTable<ID, ADDR>, MockConnector<ID, ADDR, DATA, u64>, u64, HashMapStore<ID, DATA>>;

type PeerMap<ID, ADDR, DATA> = HashMap<ID, MockPeer<ID, ADDR, DATA>>;

struct MockNetwork < ID, ADDR, DATA> {
    peers: Arc<Mutex<PeerMap<ID, ADDR, DATA>>>, 
}

impl <ID, ADDR, DATA> MockNetwork < ID, ADDR, DATA> 
where
    ID: DatabaseId + 'static,
    ADDR: Debug + Clone + PartialEq + Send + 'static,
    DATA: Reducer<Item=DATA> + Debug + Clone + PartialEq + Send + 'static,
    
{
    pub fn new(config: Config, nodes: &[Node<ID, ADDR>]) -> MockNetwork<ID, ADDR, DATA> {
        let m = MockNetwork{ peers: Arc::new(Mutex::new(HashMap::new())) };

        for n in nodes {
            let config = config.clone();

            let table = KNodeTable::<ID, ADDR>::new(n.id().clone(), config.k, config.hash_size);
            let store = HashMapStore::<ID, DATA>::new();

            let conn = MockConnector::new(n.id().clone(), n.address().clone(), m.peers.clone());
            
            let dht = Dht::new(n.id().clone(), n.address().clone(), config, table, conn, store);

            m.peers.lock().unwrap().insert(n.id().clone(), dht);
        }

        m
    }
}

#[derive(Clone)]
struct MockConnector<ID, ADDR, DATA, REQ_ID> {
    id: ID,
    addr: ADDR,
    peers: Arc<Mutex<PeerMap<ID, ADDR, DATA>>>, 
    _req_id: PhantomData<REQ_ID>,
}

impl <ID, ADDR, DATA, REQ_ID> MockConnector < ID, ADDR, DATA, REQ_ID> 
where
    ID: DatabaseId + 'static,
    ADDR: Debug + Clone + PartialEq + Send + 'static,
    DATA: Reducer<Item=DATA> + Debug + Clone + PartialEq + Send + 'static,
    
{
    pub fn new( id: ID, addr: ADDR, peers: Arc<Mutex<PeerMap<ID, ADDR, DATA>>>) -> MockConnector<ID, ADDR, DATA, REQ_ID> {
         MockConnector{ id, addr, peers, _req_id: PhantomData }
    }
}

impl <ID, ADDR, DATA, REQ_ID> Connector<REQ_ID, Node<ID, ADDR>, Request<ID, DATA>, Response<ID, ADDR, DATA>, DhtError, ()> for MockConnector <ID, ADDR, DATA, REQ_ID>
where
    ID: DatabaseId + 'static,
    ADDR: Debug + Clone + PartialEq + Send + 'static,
    DATA: Reducer<Item=DATA> + Debug + Clone + PartialEq + Send + 'static,
{
    fn request(&mut self, _ctx: (), req_id: REQ_ID, to: Node<ID, ADDR>, req: Request<ID, DATA>) -> 
            Box<Future<Item=Response<ID, ADDR, DATA>, Error=DhtError> + Send + 'static> {

        // Fetch peer instance
        let mut peer = { self.peers.lock().unwrap().remove(to.id()).unwrap() };

        let resp = peer.receive(&Node::new(self.id.clone(), self.addr.clone()), &req);

        self.peers.lock().unwrap().insert(to.id().clone(), peer);

        Box::new(future::ok(resp))
    }

    fn respond(&mut self, _ctx: (), req_id: REQ_ID, to: Node<ID, ADDR>, resp: Response<ID, ADDR, DATA>) -> Box<Future<Item=(), Error=DhtError> + Send + 'static> {
        Box::new(future::ok(()))
    }
}


#[test]
fn integration() {
    // TODO: split into separate tests, add benchmarks

    // Setup config
    let mut config = Config::default();
    config.k = 2;
    config.hash_size = 8;

    // Build basic nodes
    let mut nodes = Vec::new();
    for i in 0..16 {
        nodes.push(Node::new(i * 16, i));
    }
    let n0 = &nodes[0];

    // Create mock network
    let mgr = MockNetwork::<u64, u64, u64>::new(config, &nodes);

    println!("Bootstrapping Network");
    for n in nodes.iter().skip(1) {
        let mut peer = { mgr.peers.lock().unwrap().remove(n.id()).unwrap() };

        peer.connect(n0.clone()).wait().expect("Error connecting to network");

        mgr.peers.lock().unwrap().insert(n.id().clone(), peer);
    }
    
    println!("Testing locate across all nodes");
    for n1 in &nodes {
        for n2 in &nodes {

            if n1 == n2 {
                continue;
            }
            
            let mut peer = { mgr.peers.lock().unwrap().remove(n1.id()).unwrap() };

            let _node = peer.lookup(n2.id().clone()).wait().expect("Error finding node in network");

            mgr.peers.lock().unwrap().insert(n1.id().clone(), peer);
        }
    }

    println!("Testing store");
    let addr = 132;
    let val = vec![112];
    {
        let mut peer = { mgr.peers.lock().unwrap().remove(n0.id()).unwrap() };

        let _res = peer.store(addr, val).wait().expect("Error storing value");

        mgr.peers.lock().unwrap().insert(n0.id().clone(), peer);
    }


    println!("Testing find values for each node");
    for n in &nodes {
            
        let mut peer = { mgr.peers.lock().unwrap().remove(n.id()).unwrap() };

        let val = peer.find(addr).wait().expect("Error finding values");
        assert!(val.len() > 0);

        mgr.peers.lock().unwrap().insert(n.id().clone(), peer);
    }
}
