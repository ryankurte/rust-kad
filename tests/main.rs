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

struct MockNetwork <ID, ADDR, DATA> {
    connectors: Arc<Mutex<HashMap<ID, mpsc::Sender<Transaction<ID, ADDR, DATA>>>>>, 
}

type Transaction<ID, ADDR, DATA> = (Request<ID, DATA>, oneshot::Sender<Response<ID, ADDR, DATA>>);

impl <ID, ADDR, DATA> MockNetwork  <ID, ADDR, DATA> 
where
    ID: DatabaseId + 'static,
    ADDR: Debug + Copy + Clone + PartialEq + 'static,
    DATA: Debug + Copy + Clone + PartialEq + 'static,
{
    pub fn new() -> Self {
        MockNetwork{ connectors: Arc::new(Mutex::new(HashMap::new())) }
    }

    pub fn connector(&mut self, id: ID, addr: ADDR) -> MockConnector<ID, ADDR, DATA> {

        let (p, c) = mpsc::channel::<Transaction<ID, ADDR, DATA>>(0);

        self.connectors.lock().unwrap().insert(id.clone(), p);

        MockConnector{ recv: Arc::new(Mutex::new(c)), connectors: self.connectors.clone() }
    }
}

#[derive(Clone)]
struct MockConnector<ID, ADDR, DATA> {
    recv: Arc<Mutex<mpsc::Receiver<Transaction<ID, ADDR, DATA>>>>,
    connectors: Arc<Mutex<HashMap<ID, mpsc::Sender<Transaction<ID, ADDR, DATA>>>>>,
}

impl <'a, ID, ADDR, DATA> ConnectionManager <ID, ADDR, DATA, DhtError> for MockConnector <ID, ADDR, DATA>
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
        let conn = self.connectors.lock().unwrap();
        let peer = match conn.get(to.id()) {
            Some(v) => v,
            _ => { return Box::new(future::err(DhtError::Unimplemented)) },
        };

        // Send message to peer then poll on result, all in chained futures
        return Box::new(peer.clone().send((req, p))
        .then(|_| {
            c
        })
        .then(|r| {
            match r {
                Ok(v) => future::ok(v),
                _ => future::err(DhtError::Cancelled),
            }
        }))
    }
}

impl <ID, ADDR, DATA> Stream for MockConnector <ID, ADDR, DATA>
where
    ID: DatabaseId + 'static,
    ADDR: Debug + Copy + Clone + PartialEq + 'static,
    DATA: Debug + Copy + Clone + PartialEq + 'static,
{
    type Item = Transaction<ID, ADDR, DATA>;
    type Error = ();

    fn poll(&mut self) ->  Result<futures::Async<Option<Self::Item>>, Self::Error> {
        self.recv.lock().unwrap().poll()
    }
}



#[test]
#[ignore]
fn integration() {
    // TODO 
}
