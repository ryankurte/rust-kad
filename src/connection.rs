
use std::fmt::Debug;

use futures::prelude::*;
use futures::future;

#[cfg(tokio)]
pub mod tokio;

use crate::id::{DatabaseId, RequestId};
use crate::error::Error as DhtError;
use crate::node::Node;
use crate::message::{Request, Response};

use rr_mux::{Connector};


/// Send a request to a slice of nodes and collect the responses.
/// This returns an array of Option<Responses>>'s corresponding to the nodes passed in.
/// Timeouts result in a None return, any individual error condition will cause an error to be bubbled up.
pub fn request_all<ID, ADDR, DATA, CONN, REQ_ID, CTX>(conn: CONN, ctx: CTX, req: &Request<ID, DATA>, nodes: &[Node<ID, ADDR>]) -> 
        impl Future<Item=Vec<(Node<ID, ADDR>, Option<Response<ID, ADDR, DATA>>)>, Error=DhtError> 
where
    ID: DatabaseId + Clone + Debug + 'static,
    ADDR: Clone + Debug + 'static,
    DATA: Clone + Debug + 'static,
    REQ_ID: RequestId + 'static,
    CTX: Clone + PartialEq + Debug + 'static,
    CONN: Connector<REQ_ID, Node<ID, ADDR>, Request<ID, DATA>, Response<ID, ADDR, DATA>, DhtError, CTX> + Clone + 'static,       
{
    let mut queries = Vec::new();

    for n in nodes {
        println!("Sending request: '{:?}' to: '{:?}'", req, n.id());
        let n1 = n.clone();
        let n2 = n.clone();
        let mut c = conn.clone();
        let q = c.request(ctx.clone(), REQ_ID::generate(), n.clone(), req.clone())
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

