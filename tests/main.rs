/**
 * rust-kad
 * Integration / external library user tests
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */

use std::fmt::Debug;
use std::marker::PhantomData;
use std::collections::HashMap;

extern crate kad;

use kad::{Node, DatabaseId, ConnectionManager};
use kad::message::{Request, Response};

extern crate futures;
use futures::prelude::*;
use futures::future;
use futures::{sync::oneshot, sync::mpsc::Sender};

struct MockNetwork <ID, ADDR, DATA> {
    id: PhantomData<ID>,
    addr: PhantomData<ADDR>,
    data: PhantomData<DATA>,

    connectors: HashMap<ID, Sender<()>>, 
}

impl <ID, ADDR, DATA> MockNetwork  <ID, ADDR, DATA> {
    pub fn new() -> Self {
        MockNetwork{ id: PhantomData{}, addr: PhantomData{}, data: PhantomData{} }
    }

    pub fn connector(&mut self, id: ID, addr: ADDR) -> MockConnector<ID, ADDR, DATA> {
        MockConnector{ id, addr, data: PhantomData{} }
    }
}

#[derive(Clone)]
struct MockConnector<ID, ADDR, DATA> {
    id: ID,
    addr: ADDR,
    data: PhantomData<DATA>,
}

impl <ID, ADDR, DATA> MockConnector <ID, ADDR, DATA>
where
    ID: DatabaseId + 'static,
    ADDR: Debug + Copy + Clone + PartialEq + 'static,
    DATA: Debug + Copy + Clone + PartialEq + 'static,
{
    pub fn request(&mut self, to: &Node<ID, ADDR>, req: Request<ID, DATA>) -> 
            Box<Future<Item=Response<ID, ADDR, DATA>, Error=()>> {
        
        let (p, c) = oneshot::channel::<Response<ID, ADDR, DATA>>();

        Box::new(c.then(|r| {
            match r {
                Ok(v) => future::ok(v),
                _ => future::err(()),
            }
        }).or_else(|e| {
            future::err(())
        }))
    }
}


#[derive(Clone)]
struct MockTransaction<ID, ADDR, DATA> {
    id: ID,
    addr: ADDR,
    data: PhantomData<DATA>,
}

impl <ID, ADDR, DATA> MockTransaction <ID, ADDR, DATA>
where
    ID: DatabaseId + 'static,
    ADDR: Debug + Copy + Clone + PartialEq + 'static,
    DATA: Debug + Copy + Clone + PartialEq + 'static,
{
    
}

#[test]
#[ignore]
fn integration() {
    // TODO 
}
