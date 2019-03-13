
use std::fmt::Debug;

use futures::prelude::*;
use futures::future;

#[cfg(tokio)]
pub mod tokio;

use crate::id::{DatabaseId, RequestId};
use crate::error::Error as DhtError;
use crate::message::{Request, Response};

use rr_mux::{Connector};


/// Send a request to a slice of nodes and collect the responses.
/// This returns an array of Option<Responses>>'s corresponding to the nodes passed in.
/// Timeouts result in a None return, any individual error condition will cause an error to be bubbled up.
pub fn request_all<Id, Node, Data, Conn, ReqId, Ctx>(conn: Conn, ctx: Ctx, req: &Request<Id, Data>, nodes: &[(Id, Node)]) -> 
        impl Future<Item=Vec<(Id, Node, Option<(Response<Id, Node, Data>, Ctx)>)>, Error=DhtError> 
where
    Id: DatabaseId + Clone + Debug + 'static,
    Node: Clone + Debug + 'static,
    Data: Clone + Debug + 'static,
    ReqId: RequestId + 'static,
    Ctx: Clone + Debug + PartialEq + Send + 'static,
    Conn: Connector<ReqId, Node, Request<Id, Data>, Response<Id, Node, Data>, DhtError, Ctx> + Clone + 'static,       
{
    let mut queries = Vec::new();

    for n in nodes {
        debug!("Sending request: '{:?}' to: '{:?}'", req, n.0);
        let (a1, n1) = n.clone();
        let (a2, n2) = n.clone();
        let mut c = conn.clone();

        let q = c.request(ctx.clone(), ReqId::generate(), n1.clone(), req.clone())
            .map(|v| {
                debug!("Response: '{:?}' from: '{:?}'", v, a1);
                (a1, n1, Some(v)) 
            })
            .or_else(|e| {
                if e == DhtError::Timeout {
                    Ok((a2, n2, None))
                } else {
                    Err(e)
                }
            } );
        queries.push(q);
    }

    future::join_all(queries)
}

