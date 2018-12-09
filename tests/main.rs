/**
 * rust-kad
 * Integration / external library user tests
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */

use std::fmt::Debug;
use std::marker::PhantomData;
use std::collections::{HashMap, hash_map::Entry};
use std::sync::{Arc, Mutex};

extern crate kad;

use kad::{Node, DatabaseId, ConnectionManager, DhtError};
use kad::message::{Request, Response};

extern crate futures;
use futures::prelude::*;
use futures::future;
use futures::{sync::oneshot, sync::mpsc};

struct MockNetwork <'a, ID: 'static, ADDR: 'static, DATA: 'static> {
    peers: HashMap<ID, Dht<ID, ADDR, DATA, KNodeTable<ID, ADDR>, HashMapStore<ID, DATA>, MockConnector<'a, ID, ADDR, DATA>>>, 
}

type Transaction<ID, ADDR, DATA> = (Request<ID, DATA>, oneshot::Sender<Response<ID, ADDR, DATA>>);

impl <'a, ID, ADDR, DATA> MockNetwork  <'a, ID, ADDR, DATA> 
where
    ID: DatabaseId + 'static,
    ADDR: Debug + Copy + Clone + PartialEq + 'static,
    DATA: Updates + Debug + Copy + Clone + PartialEq + 'static,
    
{
    pub fn new(config: Config, nodes: &[Node<ID, ADDR>]) -> MockNetwork<ID, ADDR, DATA> {
        let mut peers = Vec::new();
        let mut m = MockNetwork{ peers: HashMap::new() };

        //peers: Arc::new(Mutex::new(HashMap::new()));
        for n in nodes {
            let config = config.clone();

            let table = KNodeTable::<ID, ADDR>::new(n, config.k, config.hash_size);
            let store = HashMapStore::<ID, DATA>::new();

            let conn = m.connector(n.id().clone(), n.address().clone());
            
            let dht = Dht::new(n.id().clone(), n.address().clone(), config, table, conn, store);

            m.peers.insert(n.id().clone(), dht);
        }

        m
    }

    pub fn connector(&'a self, id: ID, _addr: ADDR) -> MockConnector<'a, ID, ADDR, DATA> {
        MockConnector{ parent: self }
    }
}

#[derive(Clone)]
struct MockConnector<'a, ID: 'static, ADDR: 'static, DATA: 'static> {
    parent: &'a MockNetwork<'a, ID, ADDR, DATA>,
}

impl <'a, ID, ADDR, DATA> ConnectionManager <ID, ADDR, DATA, DhtError> for MockConnector <'a, ID, ADDR, DATA>
where
    ID: DatabaseId + 'static,
    ADDR: Debug + Copy + Clone + PartialEq + 'static,
    DATA: Debug + Copy + Clone + PartialEq + 'static,
{
    fn request(&mut self, to: &Node<ID, ADDR>, req: Request<ID, DATA>) -> 
            Box<Future<Item=Response<ID, ADDR, DATA>, Error=DhtError>> {
        
        // Setup response channel
        let (p, c) = oneshot::channel::<Response<ID, ADDR, DATA>>();

        // Fetch peer connection
        let peer = match self.parent.peers.get(to.id()) {
            Some(v) => v,
            _ => { return Box::new(future::err(DhtError::Unimplemented)) },
        };

        Box::new(future::err(DhtError::Unimplemented))
    }
}


use kad::{Config, KNodeTable, Dht};
use kad::datastore::{HashMapStore, Datastore, Updates};

#[test]
fn integration() {
    // TODO

    
    let mut config = Config::default();
    config.k = 2;
    config.hash_size = 8;

    let mut mgr = MockNetwork::<u64, (), u64>::new(16, config);

    println!("Creating nodes");
    for i in 0..16 {
        let node = Node::new(i * 16, ());
        let config = config.clone();

        let table = KNodeTable::new(&node, config.k, config.hash_size);
        let store = HashMapStore::new();
        let conn = mgr.connector(i, ());
        
        let dht = Dht::new(i, (), config, table, conn, store);

    }

    println!("Bootstrapping Network");
    for i in 1..16 {
        peers[i].1.connect(Node::new(0, ())).wait().expect("Node connect failed");
    }

}
