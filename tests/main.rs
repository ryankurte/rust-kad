use std::collections::HashMap;
/**
 * rust-kad
 * Integration / external library user tests
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */
use std::fmt::Debug;

use std::sync::{Arc, Mutex};

use kad::common::*;
use kad::dht::Dht;

use kad::Config;

use futures::channel::mpsc;

struct MockPeer<Id: Debug, Info: Debug, Data: Debug> {
    dht: Dht<Id, Info, Data, u64>,
}

type PeerMap<Id, Info, Data> = HashMap<Id, MockPeer<Id, Info, Data>>;

struct MockNetwork<Id: Debug, Info: Debug, Data: Debug> {
    peers: Arc<Mutex<PeerMap<Id, Info, Data>>>,
}

impl<Id, Info, Data> MockNetwork<Id, Info, Data>
where
    Id: DatabaseId + Debug + Send + Sync + 'static,
    Info: Debug + Clone + PartialEq + Send + Sync + 'static,
    Data: Debug + Clone + PartialEq + Send + Sync + 'static,
{
    pub fn new(config: Config, nodes: &[Entry<Id, Info>]) -> MockNetwork<Id, Info, Data> {
        let m = MockNetwork {
            peers: Arc::new(Mutex::new(HashMap::new())),
        };

        for n in nodes {
            let config = config.clone();

            let (sink_tx, _sink_rx) = mpsc::channel(10);

            let dht = Dht::standard(n.id().clone(), config, sink_tx);

            let peer = MockPeer { dht };

            m.peers.lock().unwrap().insert(n.id().clone(), peer);
        }

        m
    }
}

type MockSync = u64;

// TODO: reimplement this
#[ignore]
#[async_std::test]
async fn integration() {
    // TODO: split into separate tests, add benchmarks

    // Setup config
    let mut config = Config::default();
    config.k = 2;

    // Build basic nodes
    let mut nodes = Vec::new();
    for i in 0..16 {
        nodes.push(Entry::<[u8; 1], MockSync>::new([i * 16], i as u64));
    }
    let n0 = &nodes[0];

    // Create mock network
    let mgr = MockNetwork::<[u8; 1], MockSync, u64>::new(config, &nodes);

    println!("Bootstrapping Network");
    for n in nodes.iter().skip(1) {
        let mut peer = { mgr.peers.lock().unwrap().remove(n.id()).unwrap() };

        let (conn, _id) = peer.dht.connect(&[n0.clone()]).unwrap();
        conn.await.expect("Error connecting to network");

        mgr.peers.lock().unwrap().insert(*n.id(), peer);
    }

    println!("Testing locate across all nodes");
    for n1 in &nodes {
        for n2 in &nodes {
            if n1 == n2 {
                continue;
            }

            let mut peer = { mgr.peers.lock().unwrap().remove(n1.id()).unwrap() };

            let (locate, _req_id) = peer.dht.locate(*n2.id()).unwrap();
            let _node = locate.await.expect("Error finding node in network");

            mgr.peers.lock().unwrap().insert(*n1.id(), peer);
        }
    }

    println!("Testing store");
    let addr = [132];
    let val = vec![112];
    {
        let mut peer = { mgr.peers.lock().unwrap().remove(n0.id()).unwrap() };

        let (store, _req_id) = peer.dht.store(addr, val).unwrap();
        let _res = store.await.expect("Error storing value");

        mgr.peers.lock().unwrap().insert(*n0.id(), peer);
    }

    println!("Testing find values for each node");
    for n in &nodes {
        let mut peer = { mgr.peers.lock().unwrap().remove(n.id()).unwrap() };

        let (find, _req_id) = peer.dht.search(addr).unwrap();
        let val = find.await.expect("Error finding values");
        assert!(!val.is_empty());

        mgr.peers.lock().unwrap().insert(*n.id(), peer);
    }
}
