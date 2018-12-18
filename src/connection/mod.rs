
use std::time::Duration;
use std::fmt::Debug;

use futures::prelude::*;
use futures::future;

use futures_timer::{FutureExt};


#[cfg(tokio)]
pub mod tokio;


use crate::{DatabaseId, DhtError};
use crate::node::Node;
use crate::message::{Request, Response};

pub trait ConnectionManager<ID, ADDR, DATA, ERR>
where
    ERR: From<std::io::Error>,
{
    /// Send a request to a specified node, returns a future that contains
    /// a Response on success and an Error if something went wrong.
    ///
    /// This interface is responsible for pairing requests/responses and
    /// any underlying validation (ie. crypto) required.
    /// s
    /// Note that timeouts are created on top of this.
    fn request(&mut self, to: &Node<ID, ADDR>, req: Request<ID, DATA>) -> 
            Box<Future<Item=Response<ID, ADDR, DATA>, Error=ERR>>;
}


/// Send a request to a slice of nodes and collect the responses.
/// This returns an array of Option<Responses>>'s corresponding to the nodes passed in.
/// Timeouts result in a None return, any individual error condition will cause an error to be bubbled up.
pub fn request_all<ID, ADDR, DATA, CONN>(conn: CONN, req: &Request<ID, DATA>, nodes: &[Node<ID, ADDR>]) -> 
        impl Future<Item=Vec<(Node<ID, ADDR>, Option<Response<ID, ADDR, DATA>>)>, Error=DhtError> 
where
    ID: DatabaseId + Clone + Debug + 'static,
    ADDR: Clone + Debug + 'static,
    DATA: Clone + Debug + 'static,
    CONN: ConnectionManager<ID, ADDR, DATA, DhtError> + Clone + 'static,        
{
    let mut queries = Vec::new();

    for n in nodes {
        println!("Sending request: '{:?}' to: '{:?}'", req, n.id());
        let n1 = n.clone();
        let n2 = n.clone();
        let mut c = conn.clone();
        let q = c.request(n, req.clone())
            .map(|v| {
                println!("Response: '{:?}' from: '{:?}'", v, n1.id());
                (n1, Some(v)) 
            })
            .or_else(|e| {
                if e == DhtError::Timeout {
                    Ok((n2, None))
                } else {
                    Err(e)
                }
            } );
        queries.push(q);
    }

    future::join_all(queries)
}

