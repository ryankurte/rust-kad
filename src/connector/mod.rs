
use std::fmt::Debug;

use futures::prelude::*;
use futures::future::join_all;
use async_trait::async_trait;

#[cfg(test)]
pub mod mux;

use crate::common::*;

#[async_trait]
pub trait Connector<Id, Info, Data, ReqId, Ctx>: Sync + Send {
    // Send a request and receive a response or error at some time in the future
    async fn request(&mut self, ctx: Ctx, req_id: ReqId, target: Entry<Id, Info>, req: Request<Id, Data>) -> Result<Response<Id, Info, Data>, Error>;

    // Send a response message
    async fn respond(&mut self, ctx: Ctx, req_id: ReqId, target: Entry<Id, Info>, resp: Response<Id, Info, Data>) -> Result<(), Error>;
}

/// Send a request to a slice of nodes and collect the responses.
/// This returns an array of Option<Responses>>'s corresponding to the nodes passed in.
/// Timeouts result in a None return, any individual error condition will cause an error to be bubbled up.
pub async fn request_all<Id, Info, Data, ReqId, Conn, Ctx>(conn: Conn, ctx: Ctx, req: &Request<Id, Data>, nodes: &[Entry<Id, Info>]) -> 
        Result<Vec<(Entry<Id, Info>, Option<Response<Id, Info, Data>>)>, Error> 
where
    Id: DatabaseId + Clone + Sized + Debug + Send + 'static,
    Info: PartialEq + Clone + Sized + Debug + Send + 'static,
    Data: PartialEq + Clone + Sized + Debug + Send + 'static,
    ReqId: RequestId + Clone + Sized + Send + 'static,
    Ctx: Clone + PartialEq + Debug + Send + 'static,
    Conn: Connector<Id, Info, Data, ReqId, Ctx> + Clone + Send + 'static,
{
    let mut queries = Vec::new();

    for n in nodes {
        trace!("Sending request: '{:?}' to: '{:?}'", req, n.id());
        let n1 = n.clone();
        let mut c = conn.clone();
        let ctx_ = ctx.clone();

        let q = async move {
            let v = c.request(ctx_, ReqId::generate(), n.clone(), req.clone()).await;
            trace!("Response: '{:?}' from: '{:?}'", v, n1.id());
            match v {
                Ok(v) => (n1, Some(v)),
                Err(e) => (n1, None),
            }
        };

        queries.push(q);        
    }

    let res = join_all(queries).await;

    Ok(res)
}

