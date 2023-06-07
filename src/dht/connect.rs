use std::fmt::Debug;

use tracing::{debug, instrument};

use super::SearchOptions;
use crate::common::*;
use crate::dht::{find_nearest, SearchInfo};

pub trait Connect<Id, Info, Data> {
    /// Connect to the DHT using the provided peers
    async fn connect(
        &self,
        peers: Vec<Entry<Id, Info>>,
        opts: SearchOptions,
    ) -> Result<SearchInfo<Id>, Error>;
}

impl<T, Id, Info, Data> Connect<Id, Info, Data> for T
where
    T: super::Base<Id, Info, Data>,
    Id: DatabaseId + Clone + Debug + 'static,
    Info: Clone + Debug + 'static,
    Data: Clone + Debug + 'static,
{
    /// Connect to the DHT using the provided peers
    #[instrument(skip(self, peers, opts))]
    async fn connect(
        &self,
        peers: Vec<Entry<Id, Info>>,
        opts: SearchOptions,
    ) -> Result<SearchInfo<Id>, Error> {
        debug!("Connect {:?} -> {:?}", self.our_id(), peers);

        // Lookup peers near our address using the provided new peers
        let (nearest, depth) = find_nearest(self, self.our_id(), peers, opts.clone()).await?;

        // TODO: Register ourselves with these peers
        // If this is required? the act of polling for nodes should
        // do everything we need...

        // Update the node table
        let _ = self.update(true).await;

        let nearest = nearest.iter().map(|p| p.id().clone() ).collect();

        // return registered peers
        Ok(SearchInfo{ depth, nearest })
    }
}

#[cfg(test)]
mod tests {

    use crate::dht::base::tests::{TestCore, TestOp};
    use simplelog::{Config as LogConfig, LevelFilter, SimpleLogger};

    use super::*;

    #[tokio::test]
    async fn test_connect() {
        let _ = SimpleLogger::init(LevelFilter::Debug, LogConfig::default());

        // Setup nodes
        let n1 = Entry::new([0b1000], 100);
        let n2 = Entry::new([0b0011], 200);
        let n3 = Entry::new([0b0010], 300);
        let n4 = Entry::new([0b1001], 400);
        let n5 = Entry::new([0b1010], 400);

        let target_id = *n1.id();

        // Setup testcore with expectations
        let c = TestCore::new(
            n1.id().clone(),
            &[
                // Issue FindNodes to new peer(s)
                TestOp::net_req(
                    vec![n2.clone(), n3.clone()],
                    Request::FindNode(target_id),
                    &[
                        (
                            n2.id().clone(),
                            Response::NodesFound(target_id, vec![n4.clone()]),
                        ),
                        (
                            n3.id().clone(),
                            Response::NodesFound(target_id, vec![n5.clone()]),
                        ),
                    ],
                ),
                // And another round of FindNodes to the newly discovered nodes
                TestOp::net_req(
                    vec![n4.clone(), n5.clone()],
                    Request::FindNode(target_id),
                    &[
                        (n4.id().clone(), Response::NodesFound(target_id, vec![])),
                        (n5.id().clone(), Response::NodesFound(target_id, vec![])),
                    ],
                ),
                // And finally update the DHT with these nodes
                TestOp::update_peers(vec![n2.clone(), n3.clone(), n4.clone(), n5.clone()]),
            ],
        );

        // Execute search
        let r = c
            .connect(
                vec![n2.clone(), n3.clone()],
                SearchOptions {
                    depth: 3,
                    concurrency: 2,
                },
            )
            .await
            .unwrap();

        // Check response is as expected
        let mut e = vec![*n2.id(), *n3.id(), *n4.id(), *n5.id()];
        e.sort_by_key(|p| DatabaseId::xor(n1.id(), p));

        assert_eq!(&r.nearest, &e);

        // Finalise test
        c.finalise();
    }
}
