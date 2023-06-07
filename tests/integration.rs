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
use rand::random;
use simplelog::{Config as LogConfig, LevelFilter, SimpleLogger};
use tracing::{error, info, trace, debug};

use kad::common::*;
use kad::dht::{Base, Connect, Dht, ResponseSender, Search, SearchOptions, Store};
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
            trace!("req: {req:?} to: {peers:?}");

            // Issue requests to listed peers
            let mut resps = HashMap::new();
            for peer in peers {
                if let Some(dht) = s.peers.get_mut(peer.id()) {
                    let resp = dht.handle_req(&from, &req).unwrap();
                    resps.insert(peer.id().clone(), resp);
                }
            }

            trace!("resps: {resps:?}");

            if let Err(e) = tx.start_send(Ok(resps)) {
                error!("Failed to forward responses: {e:?}")
            }
        }

        ctx.waker().clone().wake();

        Poll::Pending
    }
}

type MockSync = u64;

#[tokio::test]
async fn tiny_scale() {
    integration::<4>(LevelFilter::Debug).await;
}

#[tokio::test]
async fn msmall_scale() {
    integration::<16>(LevelFilter::Info).await;
}

#[tokio::test]
async fn med_scale() {
    integration::<128>(LevelFilter::Info).await;
}

#[tokio::test]
#[ignore = "takes too long to run all the time"]
async fn huge_scale() {
    integration::<1024>(LevelFilter::Info).await;
}

/// End-to-end integration tests, generic over the number of nodes
async fn integration<const N: usize>(log_level: LevelFilter) {
    let _ = SimpleLogger::init(log_level, LogConfig::default());

    // Setup config
    let mut config = Config::default();

    // Set K between 2 and 16 depending on network size
    config.k = (N / 4).max(2).min(16);

    let opts = SearchOptions {
        concurrency: (N / 4).max(2),
        depth: N / config.k + 4,
    };

    // Build basic nodes
    let mut nodes = Vec::new();
    let spacing = u16::MAX as usize / N;
    for i in 0..N {
        let addr = (i * spacing) as u16;

        nodes.push(Entry::<[u8; 2], MockSync>::new(
            addr.to_le_bytes(),
            i as u64,
        ));
    }
    let n0 = &nodes[0];

    // Create mock network
    let (mgr, handles) = MockNetwork::<[u8; 2], MockSync, u64>::new(config, &nodes);

    // Poll on DHT instances
    tokio::task::spawn(async move {
        let _ = mgr.await;
    });

    info!("Bootstrapping Network");
    for i in 1..N {
        let info = handles[i]
            .connect(vec![n0.clone()], opts.clone())
            .await
            .expect("bootstrap error");

        info!("Bootstrap {i:} found {} peers", info.nearest.len());
    }

    info!("Storing entries");

    let vals: Vec<(u16, u64)> = (0..N)
        .map(|i| ((i * spacing + random::<usize>() % spacing) as u16, i as u64))
        .collect();

    for (id, val) in &vals {
        let n = random::<usize>() % N;
        handles[n]
            .store(id.to_le_bytes(), vec![*val], opts.clone())
            .await
            .unwrap();
    }

    info!("Searching for entries");

    for (i, (id, val)) in vals.iter().enumerate() {
        let n = random::<usize>() % N;

        for r in 0..3 {
            let (v, _) = handles[n]
                .search(id.to_le_bytes(), opts.clone())
                .await
                .unwrap();

            debug!("Search {i}.{r} result: {:?}", v);

            if !v.is_empty() {
                assert_eq!(v, vec![*val]);
                break;
            } else if r == 3 {
                panic!("Search {i}.{r} failed");
            }
        }
    }
}
