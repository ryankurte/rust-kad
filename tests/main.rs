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

use kad::Config;
use kad::common::*;
use kad::dht::Dht;
use kad::table::KNodeTable;
use kad::store::{HashMapStore};
use kad::connector::Connector;

extern crate futures;
use futures::prelude::*;
use futures::executor::block_on;

extern crate async_trait;
use async_trait::async_trait;

extern crate rr_mux;

type MockPeer<Id, Info, Data> = Dht<Id, Info, Data, u64, MockConnector<Id, Info, Data, u64, ()>, KNodeTable<Id, Info>, HashMapStore<Id, Data>, ()>;

type PeerMap<Id, Info, Data> = HashMap<Id, MockPeer<Id, Info, Data>>;

struct MockNetwork <Id: Debug, Info, Data: Debug> {
    peers: Arc<Mutex<PeerMap<Id, Info, Data>>>, 
}

impl <Id, Info, Data> MockNetwork < Id, Info, Data> 
where
    Id: DatabaseId + Debug + Send + 'static,
    Info: Debug + Clone + PartialEq + Send + 'static,
    Data: Debug + Clone + PartialEq + Send + 'static,
    
{
    pub fn new(config: Config, nodes: &[Entry<Id, Info>]) -> MockNetwork<Id, Info,Data> {
        let m = MockNetwork{ peers: Arc::new(Mutex::new(HashMap::new())) };

        for n in nodes {
            let config = config.clone();

            let table = KNodeTable::<Id, Info>::new(n.id().clone(), config.k, config.hash_size);
            let store = HashMapStore::<Id, Data>::new();

            let conn = MockConnector::new(n.id().clone(), n.info().clone(), m.peers.clone());
            
            let dht = Dht::new(n.id().clone(), config, table, conn, store);

            m.peers.lock().unwrap().insert(n.id().clone(), dht);
        }

        m
    }
}

#[derive(Clone)]
struct MockConnector<Id: Debug, Info, Data: Debug, ReqId, Ctx> {
    id: Id,
    addr: Info,
    peers: Arc<Mutex<PeerMap<Id, Info, Data>>>, 
    _req_id: PhantomData<ReqId>,
    _ctx: PhantomData<Ctx>,
}

impl <Id, Info, Data, ReqId, Ctx> MockConnector <Id, Info, Data, ReqId, Ctx> 
where
    Id: DatabaseId + Debug + Send + 'static,
    Info: Debug + Clone + PartialEq + Send + 'static,
    Data: Debug + Clone + PartialEq + Send + 'static,
    
{
    pub fn new( id: Id, addr: Info, peers: Arc<Mutex<PeerMap<Id, Info, Data>>>) -> Self {
         MockConnector{ id, addr, peers, _req_id: PhantomData , _ctx: PhantomData}
    }
}

#[async_trait]
impl <Id, Info, Data, ReqId, Ctx> Connector<Id, Info, Data, ReqId, Ctx> for MockConnector <Id, Info, Data, ReqId, Ctx>
where
    ReqId: Debug + Send + 'static,
    Id: DatabaseId + Debug + Send + 'static,
    Info: Debug + Clone + PartialEq + Send + 'static,
    Data: Debug + Clone + PartialEq + Send + 'static,
    Ctx: Debug + Clone + Send + 'static,
{
    async fn request(&mut self, ctx: Ctx, _req_id: ReqId, to: Entry<Id, Info>, req: Request<Id, Data>) -> 
            Result<(Response<Id, Info, Data>, Ctx), Error> {

        // Fetch peer instance
        let mut peer = { self.peers.lock().unwrap().remove(to.id()).unwrap() };

        // Update peer with request
        let resp = peer.handle(&Entry::new(self.id.clone(), self.addr.clone()), &req).unwrap();

        // Re-insert peer into database
        self.peers.lock().unwrap().insert(to.id().clone(), peer);

        Ok((resp, ctx))
    }

    async fn respond(&mut self, _ctx: Ctx, _req_id: ReqId, _to: Entry<Id, Info>, _resp: Response<Id, Info, Data>) -> Result<(), Error> {
        Ok(())
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
        nodes.push(Entry::<[u8; 1], u64>::new([i * 16], i as u64));
    }
    let n0 = &nodes[0];

    // Create mock network
    let mgr = MockNetwork::<[u8; 1], u64, u64>::new(config, &nodes);

    println!("Bootstrapping Network");
    for n in nodes.iter().skip(1) {
        let mut peer = { mgr.peers.lock().unwrap().remove(n.id()).unwrap() };

        block_on( peer.connect(n0.clone(), ()) ).expect("Error connecting to network");

        mgr.peers.lock().unwrap().insert(n.id().clone(), peer);
    }
    
    println!("Testing locate across all nodes");
    for n1 in &nodes {
        for n2 in &nodes {

            if n1 == n2 {
                continue;
            }
            
            let mut peer = { mgr.peers.lock().unwrap().remove(n1.id()).unwrap() };

            let _node = block_on( peer.lookup(n2.id().clone(), ()) ).expect("Error finding node in network");

            mgr.peers.lock().unwrap().insert(n1.id().clone(), peer);
        }
    }

    println!("Testing store");
    let addr = [132];
    let val = vec![112];
    {
        let mut peer = { mgr.peers.lock().unwrap().remove(n0.id()).unwrap() };

        let _res = block_on( peer.store(addr, val, ()) ).expect("Error storing value");

        mgr.peers.lock().unwrap().insert(n0.id().clone(), peer);
    }


    println!("Testing find values for each node");
    for n in &nodes {
            
        let mut peer = { mgr.peers.lock().unwrap().remove(n.id()).unwrap() };

        let val = block_on( peer.find(addr, ()) ).expect("Error finding values");
        assert!(val.len() > 0);

        mgr.peers.lock().unwrap().insert(n.id().clone(), peer);
    }
}
