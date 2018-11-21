

use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use std::time::{Duration};
use futures::{future, Future};


use crate::id::DatabaseId;
use crate::ConnectionManager;
use crate::{Node, Request, Response};


pub struct MockTransaction<ID, ADDR, VALUE> {
    to: Node<ID, ADDR>,
    request: Request<ID, ADDR>,
    from: Node<ID, ADDR>,
    response: Response<ID, ADDR, VALUE>,
    delay: Option<Duration>,
}

impl <ID, ADDR, VALUE> MockTransaction <ID, ADDR, VALUE> {
    pub fn new(to: Node<ID, ADDR>, request: Request<ID, ADDR>, from: Node<ID, ADDR>, 
                response: Response<ID, ADDR, VALUE>, delay: Option<Duration>)
                    -> MockTransaction<ID, ADDR, VALUE> {
        MockTransaction{to, request, from, response, delay}
    }
}


pub struct MockConnector<ID, ADDR, VALUE> {
    transactions: Arc<Mutex<VecDeque<MockTransaction<ID, ADDR, VALUE>>>>,
}

impl <ID, ADDR, VALUE> From<Vec<MockTransaction<ID, ADDR, VALUE>>> for MockConnector<ID, ADDR, VALUE> {
    fn from(v: Vec<MockTransaction<ID, ADDR, VALUE>>) -> MockConnector<ID, ADDR, VALUE> {
        MockConnector{transactions: Arc::new(Mutex::new(v.into())) }
    }
}

impl <ID, ADDR, VALUE> MockConnector <ID, ADDR, VALUE>
where
    ID: DatabaseId + 'static,
    ADDR: Debug + Copy + Clone + PartialEq + 'static,
    VALUE: Debug + Copy + Clone + PartialEq + 'static,
{
    pub fn done(self) {
        assert_eq!(0, self.transactions.lock().unwrap().len());
    }
}

impl <ID, ADDR, VALUE> Clone for MockConnector <ID, ADDR, VALUE>
where
    ID: DatabaseId + 'static,
    ADDR: Debug + Copy + Clone + PartialEq + 'static,
    VALUE: Debug + Copy + Clone + PartialEq + 'static,
{
    fn clone(&self) -> Self {
        MockConnector{transactions: self.transactions.clone() }
    }
}

impl <'a, ID, ADDR, VALUE, ERR> ConnectionManager <ID, ADDR, VALUE, ERR> for MockConnector <ID, ADDR, VALUE>
where
    ID: DatabaseId + 'static,
    ADDR: Debug + Copy + Clone + PartialEq + 'static,
    VALUE: Debug + Copy + Clone + PartialEq + 'static,
    ERR: From<std::io::Error> + 'static,
{
    fn request(&mut self, to: &Node<ID, ADDR>, req: Request<ID, ADDR>) -> 
            Box<Future<Item=(Node<ID, ADDR>, Response<ID, ADDR, VALUE>), Error=ERR>> {
        
        let transaction = self.transactions.lock().unwrap().pop_back().unwrap();

        assert_eq!(&transaction.to, to, "destination mismatch");
        assert_eq!(transaction.request, req, "request mismatch");

        if let Some(d) = transaction.delay {
            std::thread::sleep(d);
        }

        Box::new(future::ok((transaction.from, transaction.response)))
    }
}
