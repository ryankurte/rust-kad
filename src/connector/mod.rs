
use std::fmt::Debug;

use futures::prelude::*;
use futures::future::join_all;
use async_trait::async_trait;

#[cfg(tokio)]
pub mod tokio;

use crate::common::*;


use rr_mux;


/// Mux defines an rr_mux::Mux over Dht types for convenience
pub type Mux<Id, Info, Data, ReqId, Ctx> = 
    rr_mux::Mux<ReqId, Entry<Id, Info>, Request<Id, Data>, Response<Id, Info, Data>, Error, Ctx>;

#[async_trait]
pub trait Connector<Id, Info, Data, ReqId, Ctx>: Sync + Send {
    // Send a request and receive a response or error at some time in the future
    async fn request(&mut self, ctx: Ctx, req_id: ReqId, target: Entry<Id, Info>, req: Request<Id, Data>) -> Result<(Response<Id, Info, Data>, Ctx), Error>;

    // Send a response message
    async fn respond(&mut self, ctx: Ctx, req_id: ReqId, target: Entry<Id, Info>, resp: Response<Id, Info, Data>) -> Result<(), Error>;
}


/// Generic impl of Connector over matching rr_mux::Connector impls
#[async_trait]
impl <Id, Info, Data, ReqId, Ctx> Connector<Id, Info, Data, ReqId, Ctx> for (dyn rr_mux::Connector<ReqId, Entry<Id, Info>, Request<Id, Data>, Response<Id, Info, Data>, Error, Ctx> + Send + Sync)
where 
    Id: DatabaseId + Clone + Send + 'static,
    Info: PartialEq + Clone + Debug + Send + 'static,
    Data: PartialEq + Clone + Send + Debug + 'static,
    ReqId: RequestId + Clone + Send + 'static,
    Ctx: Clone + PartialEq + Debug + Send + 'static,
{
    async fn request(&mut self, ctx: Ctx, req_id: ReqId, target: Entry<Id, Info>, req: Request<Id, Data>) -> Result<(Response<Id, Info, Data>, Ctx), Error> {
        let res = rr_mux::Connector::request(self, ctx, req_id, target, req).await;

        res
    }

    // Send a response message
    async fn respond(&mut self, ctx: Ctx, req_id: ReqId, target: Entry<Id, Info>, resp: Response<Id, Info, Data>) -> Result<(), Error> {
        let res = rr_mux::Connector::respond(self, ctx, req_id, target, resp).await;
        
        res
    }
}

/// Generic impl of Connector over matching rr_mux::mock::MockConnector impls
#[async_trait]
impl <Id, Info, Data, ReqId, Ctx> Connector<Id, Info, Data, ReqId, Ctx> for rr_mux::mock::MockConnector<Entry<Id, Info>, Request<Id, Data>, Response<Id, Info, Data>, Error, Ctx>
where 
    Id: DatabaseId + Clone + Send + 'static,
    Info: PartialEq + Clone + Debug + Send + 'static,
    Data: PartialEq + Clone + Send + Debug + 'static,
    ReqId: RequestId + Clone + Send + 'static,
    Ctx: Clone + PartialEq + Debug + Sync + Send + 'static,
{
    async fn request(&mut self, ctx: Ctx, req_id: ReqId, target: Entry<Id, Info>, req: Request<Id, Data>) -> Result<(Response<Id, Info, Data>, Ctx), Error> {
        let res = rr_mux::Connector::request(self, ctx, req_id, target, req).await;

        res
    }

    // Send a response message
    async fn respond(&mut self, ctx: Ctx, req_id: ReqId, target: Entry<Id, Info>, resp: Response<Id, Info, Data>) -> Result<(), Error> {
        let res = rr_mux::Connector::respond(self, ctx, req_id, target, resp).await;
        
        res
    }

}


/// Send a request to a slice of nodes and collect the responses.
/// This returns an array of Option<Responses>>'s corresponding to the nodes passed in.
/// Timeouts result in a None return, any individual error condition will cause an error to be bubbled up.
pub async fn request_all<Id, Info, Data, ReqId, Conn, Ctx>(conn: Conn, ctx: Ctx, req: &Request<Id, Data>, nodes: &[Entry<Id, Info>]) -> 
        Result<Vec<(Entry<Id, Info>, Option<(Response<Id, Info, Data>, Ctx)>)>, Error> 
where
    Id: DatabaseId + Clone + Sized + Debug + Send + 'static,
    Info: PartialEq + Clone + Sized + Debug + Send + 'static,
    Data: PartialEq + Clone + Sized + Debug + Send + 'static,
    ReqId: RequestId + Clone + Sized + Send + 'static,
    Ctx: Clone + PartialEq + Debug + Send + 'static,
    Conn: Connector<Id, Info, Data, ReqId, Ctx> + Send + Clone + 'static,
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

