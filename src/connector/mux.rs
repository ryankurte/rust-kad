use std::fmt::Debug;

extern crate rr_mux;

use async_trait::async_trait;

use crate::common::*;
use crate::Connector;

/// Mux defines an rr_mux::Mux over Dht types for convenience
pub type Mux<Id, Info, Data, ReqId, Ctx> =
    rr_mux::Mux<ReqId, Entry<Id, Info>, Request<Id, Data>, Response<Id, Info, Data>, Error, Ctx>;

/// Genericish impl of Connector over matching rr_mux::Connector impls
#[cfg(nope)]
#[async_trait]
impl<Id, Info, Data, ReqId, Ctx> Connector<Id, Info, Data, ReqId, Ctx>
    for (dyn rr_mux::Connector<
        ReqId,
        Entry<Id, Info>,
        Request<Id, Data>,
        Response<Id, Info, Data>,
        Error,
        Ctx,
    > + Send
         + Sync)
where
    Id: DatabaseId + Clone + Send + 'static,
    Info: PartialEq + Clone + Debug + Send + 'static,
    Data: PartialEq + Clone + Debug + Send + 'static,
    ReqId: RequestId + Clone + Send + 'static,
    Ctx: Clone + PartialEq + Debug + Send + 'static,
{
    async fn request(
        &mut self,
        ctx: Ctx,
        req_id: ReqId,
        target: Entry<Id, Info>,
        req: Request<Id, Data>,
    ) -> Result<(Response<Id, Info, Data>, Ctx), Error> {
        let res = rr_mux::Connector::request(self, ctx, req_id, target, req).await;

        res
    }

    // Send a response message
    async fn respond(
        &mut self,
        ctx: Ctx,
        req_id: ReqId,
        target: Entry<Id, Info>,
        resp: Response<Id, Info, Data>,
    ) -> Result<(), Error> {
        let res = rr_mux::Connector::respond(self, ctx, req_id, target, resp).await;

        res
    }
}

/// Genericish impl over rr_mux::Mux type
#[async_trait]
impl<Id, Info, Data, ReqId, Ctx> Connector<Id, Info, Data, ReqId, Ctx>
    for rr_mux::mux::Mux<
        ReqId,
        Entry<Id, Info>,
        Request<Id, Data>,
        Response<Id, Info, Data>,
        Error,
        Ctx,
    >
where
    Id: DatabaseId + Clone + Send + Sync + 'static,
    Info: PartialEq + Clone + Debug + Send + Sync + 'static,
    Data: PartialEq + Clone + Debug + Send + Sync + 'static,
    ReqId: RequestId + Clone + Send + Sync + 'static,
    Ctx: Clone + PartialEq + Debug + Send + Sync + 'static,
{
    async fn request(
        &mut self,
        ctx: Ctx,
        req_id: ReqId,
        target: Entry<Id, Info>,
        req: Request<Id, Data>,
    ) -> Result<Response<Id, Info, Data>, Error> {
        let ctx = ctx.clone();
        let target = target.clone();

        let res = rr_mux::Connector::request(self, ctx, req_id, target, req).await;

        res
    }

    // Send a response message
    async fn respond(
        &mut self,
        ctx: Ctx,
        req_id: ReqId,
        target: Entry<Id, Info>,
        resp: Response<Id, Info, Data>,
    ) -> Result<(), Error> {
        let ctx = ctx.clone();
        let target = target.clone();

        let res = rr_mux::Connector::respond(self, ctx, req_id, target, resp).await;

        res
    }
}

/// Generic impl of Connector over matching rr_mux::mock::MockConnector impls
#[async_trait]
impl<Id, Info, Data, ReqId, Ctx> Connector<Id, Info, Data, ReqId, Ctx>
    for rr_mux::mock::MockConnector<
        Entry<Id, Info>,
        Request<Id, Data>,
        Response<Id, Info, Data>,
        Error,
        Ctx,
    >
where
    Id: DatabaseId + Clone + Send + Sync + 'static,
    Info: PartialEq + Clone + Debug + Send + Sync + 'static,
    Data: PartialEq + Clone + Send + Sync + Debug + 'static,
    ReqId: RequestId + Clone + Send + Sync + 'static,
    Ctx: Clone + PartialEq + Debug + Send + Sync + 'static,
{
    async fn request(
        &mut self,
        ctx: Ctx,
        req_id: ReqId,
        target: Entry<Id, Info>,
        req: Request<Id, Data>,
    ) -> Result<Response<Id, Info, Data>, Error> {
        let ctx = ctx.clone();
        let target = target.clone();

        let res = rr_mux::Connector::request(self, ctx, req_id, target, req).await;

        res
    }

    // Send a response message
    async fn respond(
        &mut self,
        ctx: Ctx,
        req_id: ReqId,
        target: Entry<Id, Info>,
        resp: Response<Id, Info, Data>,
    ) -> Result<(), Error> {
        let ctx = ctx.clone();
        let target = target.clone();

        let res = rr_mux::Connector::respond(self, ctx, req_id, target, resp).await;

        res
    }
}
