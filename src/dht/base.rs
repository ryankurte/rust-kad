//! [Base] provides an abstraction over core DHT functionality to
//! support higher-level operations while enabling isolated testing.

use std::collections::HashMap;
use std::fmt::Debug;

use futures::{
    channel::mpsc::{channel, Sender},
    SinkExt, StreamExt,
};
use log::trace;
use tracing::{error, warn};

use crate::common::{Entry, Error, Request, Response};

/// [Base] functionality required for higher-level DHT operations
pub trait Base<Id, Info, Data>: Sync + Send {
    /// Fetch our ID for nearest operations
    fn our_id(&self) -> Id;

    /// Fetch nearest known (and active) nodes from KNodeTable
    async fn get_nearest(&self, id: Id) -> Result<Vec<Entry<Id, Info>>, Error>;

    /// Create or update peers with new information
    async fn update_peers(&self, peer: Vec<Entry<Id, Info>>) -> Result<(), Error>;

    /// Store values in the local data store
    async fn store_data(&self, id: Id, data: Vec<Data>) -> Result<(), Error>;

    /// Issue a request to the specified peers
    async fn net_req(
        &self,
        peers: Vec<Entry<Id, Info>>,
        req: Request<Id, Data>,
    ) -> Result<HashMap<Id, Response<Id, Info, Data>>, Error>;

    /// Reduce data at a specific ID
    async fn reduce(&self, id: Id, data: Vec<Data>) -> Result<Vec<Data>, Error>;

    /// Update the DHT
    async fn update(&self, forced: bool) -> Result<(), Error>;
}

/// Async DHT handle implementing [Base] for higher-level DHT operations
pub struct DhtHandle<Id: Debug, Info: Debug, Data: Debug> {
    pub(crate) id: Id,
    pub(crate) tx: Sender<(
        OpReq<Id, Info, Data>,
        Sender<Result<OpResp<Id, Info, Data>, Error>>,
    )>,
}

impl<Id: Debug, Info: Debug, Data: Debug> DhtHandle<Id, Info, Data> {
    /// Execute an operation via remote channel
    pub(crate) async fn exec(
        &self,
        req: OpReq<Id, Info, Data>,
    ) -> Result<OpResp<Id, Info, Data>, Error> {
        let (resp_tx, mut resp_rx) = channel(1);

        // Send operation request
        if let Err(e) = self.tx.clone().send((req, resp_tx)).await {
            error!("Failed to send op request: {:?}", e);
            return Err(Error::Connector);
        };

        // Await response
        let r = resp_rx.next().await;

        trace!("response: {:?}", r);

        match r {
            Some(Ok(r)) => Ok(r),
            Some(Err(e)) => Err(e),
            None => {
                warn!("op channel closed by sender");
                Err(Error::Cancelled)
            }
        }
    }
}

/// [Base] implementation for [DhtHandle]
impl<
        Id: Clone + Debug + Sync + Send,
        Info: Clone + Debug + Sync + Send,
        Data: Clone + Debug + Sync + Send,
    > Base<Id, Info, Data> for DhtHandle<Id, Info, Data>
{
    fn our_id(&self) -> Id {
        self.id.clone()
    }

    async fn get_nearest(&self, id: Id) -> Result<Vec<Entry<Id, Info>>, Error> {
        match self.exec(OpReq::GetNearest(id)).await {
            Ok(OpResp::Peers(p)) => Ok(p),
            Err(e) => Err(e),
            _ => unimplemented!(),
        }
    }

    async fn update_peers(&self, peers: Vec<Entry<Id, Info>>) -> Result<(), Error> {
        match self.exec(OpReq::UpdatePeers(peers)).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    async fn store_data(&self, id: Id, data: Vec<Data>) -> Result<(), Error> {
        match self.exec(OpReq::Store(id, data)).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    async fn net_req(
        &self,
        peers: Vec<Entry<Id, Info>>,
        req: Request<Id, Data>,
    ) -> Result<HashMap<Id, Response<Id, Info, Data>>, Error> {
        match self.exec(OpReq::Net(peers, req)).await {
            Ok(OpResp::Net(resps)) => Ok(resps),
            Err(e) => Err(e),
            _ => unimplemented!(),
        }
    }

    async fn reduce(&self, id: Id, data: Vec<Data>) -> Result<Vec<Data>, Error> {
        match self.exec(OpReq::Reduce(id, data)).await {
            Ok(OpResp::Data(v)) => Ok(v),
            Err(e) => Err(e),
            _ => unimplemented!(),
        }
    }

    async fn update(&self, forced: bool) -> Result<(), Error> {
        match self.exec(OpReq::Update(forced)).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }
}

#[derive(Clone, PartialEq)]
pub(crate) enum OpReq<Id, Info, Data> {
    GetNearest(Id),
    UpdatePeers(Vec<Entry<Id, Info>>),
    Store(Id, Vec<Data>),
    Net(Vec<Entry<Id, Info>>, Request<Id, Data>),
    Reduce(Id, Vec<Data>),
    Update(bool),
}

impl<Id: std::fmt::Debug, Info, Data: std::fmt::Debug> std::fmt::Debug for OpReq<Id, Info, Data> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OpReq::GetNearest(id) => write!(f, "GetNearest({:?})", id),
            OpReq::UpdatePeers(_) => write!(f, "UpdatePeers"),
            OpReq::Store(id, _) => write!(f, "Store({:?})", id),
            OpReq::Net(_, r) => write!(f, "Net({:?})", r),
            OpReq::Reduce(id, _) => write!(f, "Reduce({:?})", id),
            OpReq::Update(forced) => write!(f, "Update({:?})", forced),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum OpResp<Id, Info, Data> {
    Peers(Vec<Entry<Id, Info>>),
    Net(HashMap<Id, Response<Id, Info, Data>>),
    Data(Vec<Data>),
}

#[cfg(test)]
pub mod tests {
    use super::*;

    use std::{
        clone::Clone,
        collections::VecDeque,
        iter::FromIterator,
        sync::{Arc, Mutex},
    };

    pub type Id = [u8; 1];
    pub type Info = u32;
    pub type Data = u64;
    pub type TestEntry = Entry<Id, Info>;

    /// [Base] implementation for unit testing
    pub struct TestCore {
        id: Id,
        ops: Arc<Mutex<VecDeque<TestOp>>>,
    }

    #[derive(Clone, Debug, PartialEq)]
    pub enum TestOp {
        GetNearest(Id, Vec<TestEntry>),
        NetReq(
            Vec<TestEntry>,
            Request<Id, Data>,
            HashMap<Id, Response<Id, Info, Data>>,
        ),
        Reduce(Id, Vec<Data>),
        Store(Id, Vec<Data>),
        UpdatePeers(Vec<TestEntry>),
    }

    impl TestOp {
        pub fn net_req(
            mut peers: Vec<TestEntry>,
            req: Request<Id, Data>,
            resps: &[(Id, Response<Id, Info, Data>)],
        ) -> Self {
            peers.sort_by_key(|p| p.id().clone());

            Self::NetReq(
                peers,
                req,
                HashMap::from_iter(resps.iter().map(|(i, r)| (i.clone(), r.clone()))),
            )
        }

        pub fn update_peers(mut peers: Vec<TestEntry>) -> Self {
            peers.sort_by_key(|p| p.id().clone());
            Self::UpdatePeers(peers)
        }
    }

    impl TestCore {
        pub fn new(id: Id, ops: &[TestOp]) -> Self {
            Self {
                id,
                ops: Arc::new(Mutex::new(VecDeque::from_iter(
                    ops.iter().map(|o| o.clone()),
                ))),
            }
        }

        fn op(&self) -> Option<TestOp> {
            self.ops.lock().unwrap().pop_front()
        }

        pub fn finalise(&self) {
            let ops: Vec<_> = self.ops.lock().unwrap().drain(..).collect();
            assert_eq!(&ops[..], &[]);
        }
    }

    impl Base<Id, Info, Data> for TestCore {
        fn our_id(&self) -> Id {
            self.id.clone()
        }

        async fn get_nearest(&self, id: Id) -> Result<Vec<TestEntry>, Error> {
            let op = self.op();
            match op {
                Some(TestOp::GetNearest(i, entries)) if id == i => Ok(entries.clone()),
                _ => panic!(
                    "unexpected get_nearest for id: {:?} (expected: {:?})",
                    id, op
                ),
            }
        }

        async fn store_data(&self, id: Id, mut data: Vec<Data>) -> Result<(), Error> {
            let op = self.op();
            data.sort();
            match op {
                Some(TestOp::Store(i, d)) if id == i && data == d => Ok(()),
                _ => panic!(
                    "unexpected store_data for id: {:?} (expected: {:?})",
                    id, op
                ),
            }
        }

        async fn update_peers(&self, mut peers: Vec<Entry<Id, Info>>) -> Result<(), Error> {
            let op = self.op();
            peers.sort_by_key(|p| p.id().clone());
            match op {
                Some(TestOp::UpdatePeers(p)) if peers == p => Ok(()),
                _ => panic!("unexpected update_peers (expected: {:?})", op),
            }
        }

        async fn net_req(
            &self,
            mut peers: Vec<TestEntry>,
            req: Request<Id, Data>,
        ) -> Result<HashMap<Id, Response<Id, Info, Data>>, Error> {
            let op = self.op();
            peers.sort_by_key(|p| p.id().clone());
            match op {
                Some(TestOp::NetReq(p, r, o)) if p == peers && r == req => Ok(o.clone()),
                _ => panic!("unexpected net {req:?} for peers: {peers:?} (expected: {op:?})"),
            }
        }

        async fn reduce(&self, id: Id, mut data: Vec<Data>) -> Result<Vec<Data>, Error> {
            let op = self.op();
            match op {
                Some(TestOp::Reduce(i, d)) if i == id && d == data => {
                    data.sort();
                    data.reverse();
                    Ok(vec![data[0]])
                }
                _ => panic!("unexpected reduce for id: {id:?} data: {data:?} (expected: {op:?})"),
            }
        }

        async fn update(&self, forced: bool) -> Result<(), Error> {
            Ok(())
        }
    }
}
