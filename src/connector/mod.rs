
use std::fmt::Debug;

use futures::prelude::*;
use futures::future;

#[cfg(tokio)]
pub mod tokio;

use crate::common::*;

use store::Reducer;

use rr_mux;


/// Mux defines an rr_mux::Mux over Dht types for convenience
pub type Mux<NodeId, Node, Data, ReqId, Ctx> = 
    rr_mux::Mux<ReqId, Node, Request<NodeId, Data>, Response<NodeId, Node, Data>, Error, Ctx>;

/// Connector defines an rr_mux::Connector impl over Dht types for convenience
pub trait Connector<NodeId, Node, Data, ReqId, Ctx>: 
    rr_mux::Connector<ReqId, Node, Request<NodeId, Data>, Response<NodeId, Node, Data>, Error, Ctx> {}


/// Generic impl of Connector over matching rr_mux::Connector impls
impl <NodeId, Node, Data, ReqId, Ctx> Connector<NodeId, Node, Data, ReqId, Ctx> for rr_mux::Connector<ReqId, Node, Request<NodeId, Data>, Response<NodeId, Node, Data>, Error, Ctx> 
where 
    NodeId: DatabaseId + Clone + Send + 'static,
    Node: PartialEq + Clone + Debug + Send + 'static,
    Data: Reducer<Item=Data> + PartialEq + Clone + Send  + Debug + 'static,
    ReqId: RequestId + Clone + Send + 'static,
    Ctx: Clone + PartialEq + Debug + Send + 'static,
{}

/// Generic impl of Connector over matching rr_mux::mock::MockConnector impls
impl <NodeId, Node, Data, ReqId, Ctx> Connector<NodeId, Node, Data, ReqId, Ctx> for rr_mux::mock::MockConnector<Node, Request<NodeId, Data>, Response<NodeId, Node, Data>, Error, Ctx> 
where 
    NodeId: DatabaseId + Clone + Send + 'static,
    Node: PartialEq + Clone + Debug + Send + 'static,
    Data: Reducer<Item=Data> + PartialEq + Clone + Send  + Debug + 'static,
    ReqId: RequestId + Clone + Send + 'static,
    Ctx: Clone + PartialEq + Debug + Send + 'static,
{}

/// Send a request to a slice of nodes and collect the responses.
/// This returns an array of Option<Responses>>'s corresponding to the nodes passed in.
/// Timeouts result in a None return, any individual error condition will cause an error to be bubbled up.
pub fn request_all<Id, Info, Data, Conn, ReqId, Ctx>(conn: Conn, ctx: Ctx, req: &Request<Id, Data>, nodes: &[Entry<Id, Info>]) -> 
        impl Future<Item=Vec<(Entry<Id, Info>, Option<(Response<Id, Info, Data>, Ctx)>)>, Error=Error> 
where
    Id: DatabaseId + Clone + Debug + 'static,
    Info: Clone + Debug + 'static,
    Data: Clone + Debug + 'static,
    ReqId: RequestId + 'static,
    Ctx: Clone + Debug + PartialEq + Send + 'static,
    Conn: rr_mux::Connector<ReqId, Entry<Id, Info>, Request<Id, Data>, Response<Id, Info, Data>, Error, Ctx> + Clone + 'static,       
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
                if e == Error::Timeout {
                    Ok((n2, None))
                } else {
                    Err(e)
                }
            } );
        queries.push(q);
    }

    future::join_all(queries)
}

