/**
 * rust-kad
 * Mock interfaces to assist with testing
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */


use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use std::time::{Duration};
use futures::{future, Future};


use crate::id::DatabaseId;
use crate::ConnectionManager;
use crate::{Node, Request, Response};

/// Mock transaction for test use
#[derive(PartialEq, Clone, Debug)]
pub struct MockTransaction<ID, ADDR, DATA> {
    to: Node<ID, ADDR>,
    request: Request<ID, ADDR>,
    from: Node<ID, ADDR>,
    response: Response<ID, ADDR, DATA>,
    delay: Option<Duration>,
}

impl <ID, ADDR, DATA> MockTransaction <ID, ADDR, DATA> {
    pub fn new(to: Node<ID, ADDR>, request: Request<ID, ADDR>, from: Node<ID, ADDR>, 
                response: Response<ID, ADDR, DATA>, delay: Option<Duration>)
                    -> MockTransaction<ID, ADDR, DATA> {
        MockTransaction{to, request, from, response, delay}
    }
}

/// Mock Connector models a set of transactions expected to be executed
/// for testing use
pub struct MockConnector<ID, ADDR, DATA> {
    transactions: Arc<Mutex<VecDeque<MockTransaction<ID, ADDR, DATA>>>>,
}

impl <ID, ADDR, DATA> From<Vec<MockTransaction<ID, ADDR, DATA>>> for MockConnector<ID, ADDR, DATA> {
    fn from(v: Vec<MockTransaction<ID, ADDR, DATA>>) -> MockConnector<ID, ADDR, DATA> {
        MockConnector{transactions: Arc::new(Mutex::new(v.into())) }
    }
}

impl <ID, ADDR, DATA> MockConnector <ID, ADDR, DATA>
where
    ID: DatabaseId + 'static,
    ADDR: Debug + Copy + Clone + PartialEq + 'static,
    DATA: Debug + Copy + Clone + PartialEq + 'static,
{
    pub fn done(self) {
        let transactions = self.transactions.lock().unwrap();
        assert_eq!(0, transactions.len(), "Mock transaction list non-empty (remaining: {:?})", *transactions);
    }
}

impl <ID, ADDR, DATA> Clone for MockConnector <ID, ADDR, DATA>
where
    ID: DatabaseId + 'static,
    ADDR: Debug + Copy + Clone + PartialEq + 'static,
    DATA: Debug + Copy + Clone + PartialEq + 'static,
{
    fn clone(&self) -> Self {
        MockConnector{transactions: self.transactions.clone() }
    }
}

impl <'a, ID, ADDR, DATA, ERR> ConnectionManager <ID, ADDR, DATA, ERR> for MockConnector <ID, ADDR, DATA>
where
    ID: DatabaseId + 'static,
    ADDR: Debug + Copy + Clone + PartialEq + 'static,
    DATA: Debug + Copy + Clone + PartialEq + 'static,
    ERR: From<std::io::Error> + 'static,
{
    fn request(&mut self, to: &Node<ID, ADDR>, req: Request<ID, ADDR>) -> 
            Box<Future<Item=Response<ID, ADDR, DATA>, Error=ERR>> {
        
        let transaction = self.transactions.lock().unwrap().pop_front().expect("no more transactions available");

        assert_eq!(&transaction.to, to, "destination mismatch");
        assert_eq!(transaction.request, req, "request mismatch");

        if let Some(d) = transaction.delay {
            std::thread::sleep(d);
        }

        Box::new(future::ok(transaction.response))
    }
}
