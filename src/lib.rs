//! rust-kad
//! A Kademlia DHT implementation in Rust
//!
//! https://github.com/ryankurte/rust-kad
//! Copyright 2018-2023 Ryan Kurte

#![feature(async_fn_in_trait)]

use std::fmt::Debug;
use std::time::Duration;

pub mod common;
use crate::common::*;

pub mod table;
use table::KNodeTable;

pub mod store;
use store::HashMapStore;

pub mod dht;
use dht::{Dht, RequestSender, SearchOptions};

pub mod prelude;

pub mod mock;

/// DHT Configuration object
#[derive(PartialEq, Clone, Debug)]
#[cfg_attr(feature = "clap", derive(clap::Parser))]
pub struct Config {
    #[cfg_attr(feature = "clap", clap(long = "dht-bucket-size", default_value = "16"))]
    /// Size of buckets and number of nearby nodes to consider when searching
    pub k: usize,

    #[cfg_attr(feature = "clap", clap(long = "dht-concurrency", default_value = "4"))]
    /// Number of concurrent operations to be performed at once (also known as α or alpha)
    pub concurrency: usize,

    #[cfg_attr(
        feature = "clap",
        clap(long = "dht-recursion-limit", default_value = "16")
    )]
    /// Maximum recursion depth for searches
    pub max_recursion: usize,

    #[cfg_attr(feature = "clap", clap(long = "dht-search-timeout", value_parser = parse_duration, default_value = "2s"))]
    /// Timeout for search iterations with missing responses
    pub search_timeout: Duration,

    #[cfg_attr(feature = "clap", clap(long = "dht-node-timeout", value_parser = parse_duration, default_value = "15m"))]
    /// Timeout for no-contact from oldest node (before ping and expiry occurs)
    pub node_timeout: Duration,

    #[cfg_attr(feature = "clap", clap(long = "dht-update-period", value_parser = parse_duration, default_value = "10m"))]
    /// Period for bucket updates
    pub update_period: Duration,
}

fn parse_duration(s: &str) -> Result<Duration, humantime::DurationError> {
    use std::str::FromStr;
    let d = humantime::Duration::from_str(s)?;
    Ok(d.into())
}

impl Default for Config {
    fn default() -> Config {
        Config {
            k: 16,
            concurrency: 4,
            max_recursion: 16,
            search_timeout: Duration::from_secs(10),
            node_timeout: Duration::from_secs(15 * 60 * 60),
            update_period: Duration::from_secs(10 * 60 * 60),
        }
    }
}

impl Config {
    pub(crate) fn search_options(&self) -> SearchOptions {
        SearchOptions {
            depth: self.max_recursion,
            concurrency: self.concurrency,
        }
    }
}

/// Standard DHT implementation using included KNodeTable and HashMapStore implementations
pub type StandardDht<Id, Info, Data> = Dht<
    Id,
    Info,
    Data,
    RequestSender<Id, Info, Data>,
    KNodeTable<Id, Info>,
    HashMapStore<Id, Data>,
>;

impl<Id, Info, Data> Dht<Id, Info, Data>
where
    Id: DatabaseId + Clone + Sync + Send + 'static,
    Info: PartialEq + Clone + Debug + Sync + Send + 'static,
    Data: PartialEq + Clone + Debug + Sync + Send + 'static,
{
    /// Create a new DHT using the standard node table and data store implementations
    pub fn standard(
        id: Id,
        config: Config,
        req_sink: RequestSender<Id, Info, Data>,
    ) -> StandardDht<Id, Info, Data> {
        let table = KNodeTable::new(id.clone(), config.k, id.max_bits());
        let store = HashMapStore::new();
        Dht::custom(id, config, req_sink, table, store)
    }
}

#[cfg(nope)]
#[cfg(test)]
mod tests {
    use futures::executor::block_on;
    use std::clone::Clone;

    use super::*;

    use crate::mock::MockSync;
    use crate::store::HashMapStore;
    use crate::table::{KNodeTable, NodeTable};

    use rr_mux::mock::{MockConnector, MockTransaction};
    use rr_mux::Mux;

    type RequestId = u64;
    type NodeId = [u8; 1];
    type Info = MockSync;
    type Data = MockSync;

    #[test]
    fn test_mux() {
        // Create a generic mux
        let dht_mux = Mux::<
            RequestId,
            Entry<NodeId, Info>,
            Request<NodeId, Data>,
            Response<NodeId, Info, Data>,
            Error,
            (),
        >::new();

        // Bind it to the DHT instance
        let n1 = Entry::new([0b0001], MockSync::new(100));
        let _dht = Dht::<NodeId, Info, Data, RequestId, _, _, _, _>::standard(
            n1.id().clone(),
            Config::default(),
            dht_mux,
        );
    }

    #[test]
    fn test_connect() {
        let n1 = Entry::new([0b0001], MockSync::new(100));
        let n2 = Entry::new([0b0010], MockSync::new(200));
        let n3 = Entry::new([0b0011], MockSync::new(300));
        let n4 = Entry::new([0b1000], MockSync::new(400));

        // Build expectations
        let mut connector = MockConnector::new().expect(vec![
            // First transaction to bootstrap onto the network
            MockTransaction::request(
                n2.clone(),
                Request::FindNode(n1.id().clone()),
                Ok((
                    Response::NodesFound(n1.id().clone(), vec![n3.clone(), n4.clone()]),
                    (),
                )),
            ),
            //bootsrap to found nodes
            MockTransaction::request(
                n3.clone(),
                Request::FindNode(n1.id().clone()),
                Ok((Response::NodesFound(n1.id().clone(), vec![]), ())),
            ),
            MockTransaction::request(
                n4.clone(),
                Request::FindNode(n1.id().clone()),
                Ok((Response::NodesFound(n1.id().clone(), vec![]), ())),
            ),
        ]);

        // Create configuration
        let mut config = Config::default();
        config.concurrency = 2;

        let knodetable = KNodeTable::new(n1.id().clone(), 2, 4);

        // Instantiated DHT
        let store: HashMapStore<NodeId, MockSync> = HashMapStore::new();
        let mut dht = Dht::<NodeId, MockSync, _, u64, _, _, _, _>::new(
            n1.id().clone(),
            config,
            knodetable,
            connector.clone(),
            store,
        );

        // Attempt initial bootstrapping
        block_on(dht.connect(n2.clone(), ())).unwrap();

        // Check bootstrapped node is added to db
        assert_eq!(Some(n2.clone()), dht.contains(n2.id()));

        // Check Reported nodes are added
        assert_eq!(Some(n3.clone()), dht.contains(n3.id()));
        assert_eq!(Some(n4.clone()), dht.contains(n4.id()));

        // Check expectations are done
        connector.finalise();
    }

    #[test]
    fn test_lookup() {
        let n1 = Entry::new([0b1000], 100);
        let n2 = Entry::new([0b0011], 200);
        let n3 = Entry::new([0b0010], 300);
        let n4 = Entry::new([0b1001], 400);
        let n5 = Entry::new([0b1010], 400);

        // Build expectations
        let mut connector = MockConnector::new().expect(vec![
            // First transaction to bootstrap onto the network
            MockTransaction::request(
                n2.clone(),
                Request::FindNode(n4.id().clone()),
                Ok((Response::NodesFound(n4.id().clone(), vec![n4.clone()]), ())),
            ),
            MockTransaction::request(
                n3.clone(),
                Request::FindNode(n4.id().clone()),
                Ok((Response::NodesFound(n4.id().clone(), vec![n5.clone()]), ())),
            ),
            // Second iteration
            MockTransaction::request(
                n4.clone(),
                Request::FindNode(n4.id().clone()),
                Ok((Response::NodesFound(n4.id().clone(), vec![]), ())),
            ),
            MockTransaction::request(
                n5.clone(),
                Request::FindNode(n4.id().clone()),
                Ok((Response::NodesFound(n4.id().clone(), vec![]), ())),
            ),
        ]);

        // Create configuration
        let mut config = Config::default();
        config.concurrency = 2;
        config.k = 2;

        let mut knodetable = KNodeTable::new(n1.id().clone(), 2, 4);

        // Inject initial nodes into the table
        knodetable.create_or_update(&n2);
        knodetable.create_or_update(&n3);

        // Instantiated DHT
        let store: HashMapStore<NodeId, u64> = HashMapStore::new();
        let mut dht = Dht::<NodeId, u64, _, u64, _, _, _, _>::new(
            n1.id().clone(),
            config,
            knodetable,
            connector.clone(),
            store,
        );

        // Perform search
        block_on(dht.lookup(n4.id().clone(), ())).expect("lookup failed");

        connector.finalise();
    }

    #[test]
    fn test_store() {
        let n1 = Entry::new([0b1000], 100);
        let n2 = Entry::new([0b0011], 200);
        let n3 = Entry::new([0b0010], 300);
        let n4 = Entry::new([0b1001], 400);
        let n5 = Entry::new([0b1010], 500);

        let id: [u8; 1] = [0b1011];
        let val = vec![1234];

        // Build expectations
        let mut connector = MockConnector::new().expect(vec![
            // First transaction to bootstrap onto the network
            MockTransaction::request(
                n2.clone(),
                Request::FindNode(id),
                Ok((Response::NodesFound(id, vec![n4.clone()]), ())),
            ),
            MockTransaction::request(
                n3.clone(),
                Request::FindNode(id),
                Ok((Response::NodesFound(id, vec![n5.clone()]), ())),
            ),
            // Second iteration to find k nodes closest to v
            MockTransaction::request(
                n5.clone(),
                Request::FindNode(id),
                Ok((Response::NodesFound(id, vec![]), ())),
            ),
            MockTransaction::request(
                n4.clone(),
                Request::FindNode(id),
                Ok((Response::NodesFound(id, vec![]), ())),
            ),
            // Final iteration pushes data to k nodes
            MockTransaction::request(
                n5.clone(),
                Request::Store(id, val.clone()),
                Ok((Response::NoResult, ())),
            ),
            MockTransaction::request(
                n4.clone(),
                Request::Store(id, val.clone()),
                Ok((Response::NoResult, ())),
            ),
        ]);

        // Create configuration
        let mut config = Config::default();
        config.concurrency = 2;
        config.k = 2;

        let mut knodetable = KNodeTable::new(n1.id().clone(), 2, 4);

        // Inject initial nodes into the table
        knodetable.create_or_update(&n2);
        knodetable.create_or_update(&n3);

        // Instantiated DHT
        let store: HashMapStore<NodeId, u64> = HashMapStore::new();
        let mut dht = Dht::<NodeId, u64, _, u64, _, _, _, _>::new(
            n1.id().clone(),
            config,
            knodetable,
            connector.clone(),
            store,
        );

        // Perform store
        block_on(dht.store(id, val, ())).expect("store failed");

        connector.finalise();
    }

    #[test]
    fn test_find() {
        let n1 = Entry::new([0b1000], 100);
        let n2 = Entry::new([0b0011], 200);
        let n3 = Entry::new([0b0010], 300);
        let n4 = Entry::new([0b1001], 400);
        let n5 = Entry::new([0b1010], 500);

        let id: [u8; 1] = [0b1011];
        let val = vec![1234];

        // Build expectations
        let mut connector = MockConnector::new().expect(vec![
            // First transaction to bootstrap onto the network
            MockTransaction::request(
                n2.clone(),
                Request::FindValue(id),
                Ok((Response::NodesFound(id, vec![n4.clone()]), ())),
            ),
            MockTransaction::request(
                n3.clone(),
                Request::FindValue(id),
                Ok((Response::NodesFound(id, vec![n5.clone()]), ())),
            ),
            // Next iteration gets node data
            MockTransaction::request(
                n5.clone(),
                Request::FindValue(id),
                Ok((Response::ValuesFound(id, val.clone()), ())),
            ),
            MockTransaction::request(
                n4.clone(),
                Request::FindValue(id),
                Ok((Response::ValuesFound(id, val.clone()), ())),
            ),
        ]);

        // Create configuration
        let mut config = Config::default();
        config.concurrency = 2;
        config.k = 2;

        let mut knodetable = KNodeTable::new(n1.id().clone(), 2, 4);

        // Inject initial nodes into the table
        knodetable.create_or_update(&n2);
        knodetable.create_or_update(&n3);

        // Instantiated DHT
        let store: HashMapStore<NodeId, u64> = HashMapStore::new();
        let mut dht = Dht::<NodeId, u64, _, u64, _, _, _, _>::new(
            n1.id().clone(),
            config,
            knodetable,
            connector.clone(),
            store,
        );

        // Perform store
        block_on(dht.find(id, ())).expect("find failed");

        connector.finalise();
    }
}
