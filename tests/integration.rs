/**
 * rust-kad
 * Integration / external library user tests
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */
use std::collections::HashMap;
use std::fmt::Debug;
use std::task::Poll;

use futures::channel::mpsc::{self, Receiver};
use futures::{Future, FutureExt, StreamExt};
use simplelog::{Config as LogConfig, LevelFilter, SimpleLogger};
use tracing::{debug, error};

use kad::common::*;
use kad::dht::{Base, Connect, Dht, Lookup, ResponseSender, Search, SearchOptions, Store};
use kad::Config;

type PeerMap<Id, Info, Data> = HashMap<Id, Dht<Id, Info, Data>>;

struct MockNetwork<Id: Debug, Info: Debug, Data: Debug> {
    peers: PeerMap<Id, Info, Data>,
    sink_rx: TestRequestReceiver<Id, Info, Data>,
}

pub type TestRequestReceiver<Id, Info, Data> = Receiver<(
    Entry<Id, Info>,
    Vec<Entry<Id, Info>>,
    Request<Id, Data>,
    ResponseSender<Id, Info, Data>,
)>;

impl<Id, Info, Data> MockNetwork<Id, Info, Data>
where
    Id: DatabaseId + Debug + Send + Sync + 'static,
    Info: Debug + Clone + PartialEq + Send + Sync + 'static,
    Data: Debug + Clone + PartialEq + Ord + Send + Sync + 'static,
{
    pub fn new(
        config: Config,
        nodes: &[Entry<Id, Info>],
    ) -> (MockNetwork<Id, Info, Data>, Vec<impl Base<Id, Info, Data>>) {
        let mut peers = HashMap::new();
        let (sink_tx, sink_rx) = mpsc::channel(0);

        let mut handles = vec![];

        // Setup nodes
        for n in nodes {
            let config = config.clone();

            let node = n.clone();
            let sink_tx = sink_tx.clone();
            let (dht_tx, dht_rx) = mpsc::channel(0);
            tokio::task::spawn(async move {
                let _ = dht_rx
                    .map(|(p, r, d)| Ok((node.clone(), p, r, d)))
                    .forward(sink_tx)
                    .await;
                error!("Exiting pipe task");
            });

            let mut dht = Dht::standard(n.id().clone(), config, dht_tx);

            dht.set_reducer(Box::new(|_id, mut data| {
                data.sort();
                data.reverse();
                data.truncate(1);
                data
            }));

            handles.push(dht.get_handle());

            peers.insert(n.id().clone(), dht);
        }

        (Self { peers, sink_rx }, handles)
    }
}

impl<Id, Info, Data> Unpin for MockNetwork<Id, Info, Data>
where
    Id: DatabaseId + Debug + Send + Sync + 'static,
    Info: Debug + Clone + PartialEq + Send + Sync + 'static,
    Data: Debug + Clone + PartialEq + Send + Sync + 'static,
{
}

impl<Id, Info, Data> Future for MockNetwork<Id, Info, Data>
where
    Id: DatabaseId + Debug + Send + Sync + 'static,
    Info: Debug + Clone + PartialEq + Send + Sync + 'static,
    Data: Debug + Clone + PartialEq + Send + Sync + 'static,
{
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        ctx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut s = self.as_mut();

        // Poll to update DHTs
        for (_id, p) in s.peers.iter_mut() {
            let _ = p.poll_unpin(ctx);
        }

        // Poll on requests
        if let Poll::Ready(Some((from, peers, req, mut tx))) = s.sink_rx.poll_next_unpin(ctx) {
            debug!("req: {req:?} to: {peers:?}");

            // Issue requests to listed peers
            let mut resps = HashMap::new();
            for peer in peers {
                if let Some(dht) = s.peers.get_mut(peer.id()) {
                    let resp = dht.handle_req(&from, &req).unwrap();
                    resps.insert(peer.id().clone(), resp);
                }
            }

            debug!("resps: {resps:?}");

            if let Err(e) = tx.start_send(Ok(resps)) {
                error!("Failed to forward responses: {e:?}")
            }
        }

        ctx.waker().clone().wake();

        Poll::Pending
    }
}

type MockSync = u64;

const N: usize = 4;

#[tokio::test]
async fn integration() {
    let _ = SimpleLogger::init(LevelFilter::Debug, LogConfig::default());

    // Setup config
    let mut config = Config::default();
    config.k = 2;

    let opts = SearchOptions {
        concurrency: 2,
        depth: 3,
    };

    // Build basic nodes
    let mut nodes = Vec::new();
    for i in 0..N {
        nodes.push(Entry::<[u8; 1], MockSync>::new([i as u8 * 16], i as u64));
    }
    let n0 = &nodes[0];

    // Create mock network
    let (mgr, handles) = MockNetwork::<[u8; 1], MockSync, u64>::new(config, &nodes);

    // Poll on DHT instances
    tokio::task::spawn(async move {
        let _ = mgr.await;
    });

    println!("Bootstrapping Network");
    for i in 1..N {
        let found = handles[i]
            .connect(vec![n0.clone()], opts.clone())
            .await
            .expect("bootstrap error");

        println!("Bootstrap {i:} found {} peers", found.len());
    }

    println!("Storing entry");
    let addr = [132];
    let val = vec![112];

    handles[0]
        .store(addr, val.clone(), opts.clone())
        .await
        .unwrap();

    println!("Testing FindValues");
    for i in 0..N {
        let v = handles[i].search(addr, opts.clone()).await.unwrap();
        assert_eq!(v, val);
    }
}
