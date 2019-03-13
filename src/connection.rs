
use std::fmt::Debug;

use futures::prelude::*;
use futures::future;

#[cfg(tokio)]
pub mod tokio;

use crate::id::{DatabaseId, RequestId};
use crate::error::Error as DhtError;
use crate::entry::Entry;
use crate::message::{Request, Response};

use rr_mux::{Connector};


/// Send a request to a slice of nodes and collect the responses.
/// This returns an array of Option<Responses>>'s corresponding to the nodes passed in.
/// Timeouts result in a None return, any individual error condition will cause an error to be bubbled up.
pub fn request_all<Id, Info, Data, Conn, ReqId, Ctx>(conn: Conn, ctx: Ctx, req: &Request<Id, Data>, nodes: &[Entry<Id, Info>]) -> 
        impl Future<Item=Vec<(Entry<Id, Info>, Option<(Response<Id, Info, Data>, Ctx)>)>, Error=DhtError> 
where
    Id: DatabaseId + Clone + Debug + 'static,
    Info: Clone + Debug + 'static,
    Data: Clone + Debug + 'static,
    ReqId: RequestId + 'static,
    Ctx: Clone + Debug + PartialEq + Send + 'static,
    Conn: Connector<ReqId, Entry<Id, Info>, Request<Id, Data>, Response<Id, Info, Data>, DhtError, Ctx> + Clone + 'static,       
{
    let mut queries = Vec::new();

    for n in nodes {
        debug!("Sending request: '{:?}' to: '{:?}'", req, n.id());
        let n1 = n.clone();
        let n2 = n.clone();
        let mut c = conn.clone();
        let q = c.request(ctx.clone(), ReqId::generate(), n.clone(), req.clone())
            .map(|v| {
                debug!("Response: '{:?}' from: '{:?}'", v, n1.id());
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

