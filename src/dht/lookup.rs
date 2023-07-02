//! DHT Lookup operation
//!
// https://github.com/ryankurte/rust-kad
// Copyright 2018-2023 ryan kurte

use std::fmt::Debug;

use tracing::instrument;

use super::{find_nearest, SearchInfo, SearchOptions};
use crate::common::*;

pub trait Lookup<Id, Info, Data> {
    /// Lookup a specific peer via the DHT
    async fn lookup(
        &self,
        id: Id,
        opts: SearchOptions,
    ) -> Result<(Entry<Id, Info>, SearchInfo<Id>), Error>;
}

impl<T, Id, Info, Data> Lookup<Id, Info, Data> for T
where
    T: super::Base<Id, Info, Data>,
    Id: DatabaseId + Clone + Debug + 'static,
    Info: Clone + Debug + 'static,
    Data: Clone + Debug + 'static,
{
    /// Lookup a specific peer via the DHT
    #[instrument(skip_all, fields(our_id=?self.our_id(), target_id=?id))]
    async fn lookup(
        &self,
        id: Id,
        opts: SearchOptions,
    ) -> Result<(Entry<Id, Info>, SearchInfo<Id>), Error> {
        // Fetch nearest nodes from the table
        let nearest = self.get_nearest(id.clone()).await?;
        if nearest.len() == 0 {
            return Err(Error::NoPeers);
        }

        // Perform DHT lookup for nearest nodes to the search ID
        let (resolved, depth) = find_nearest(self, id.clone(), nearest, opts.clone()).await?;

        let e = match resolved.iter().find(|p| p.id() == &id) {
            Some(v) => v.clone(),
            None => return Err(Error::NotFound),
        };

        // Map nearest to IDs
        // TODO: this should probably only be k nearest nodes?
        let nearest: Vec<_> = resolved.iter().map(|p| p.id().clone()).collect();

        Ok((e, SearchInfo { depth, nearest }))
    }
}

#[cfg(test)]
mod tests {
    use simplelog::{Config as LogConfig, LevelFilter, SimpleLogger};

    use crate::dht::base::tests::{TestCore, TestOp};

    use super::*;

    #[tokio::test]
    async fn test_lookup() {
        let _ = SimpleLogger::init(LevelFilter::Debug, LogConfig::default());

        // Setup nodes
        let n1 = Entry::new([0b1000], 100);
        let n2 = Entry::new([0b0011], 200);
        let n3 = Entry::new([0b0010], 300);
        let n4 = Entry::new([0b1001], 400);
        let n5 = Entry::new([0b1010], 400);

        let target_id = *n5.id();

        // Setup testcore with expectations
        let c = TestCore::new(
            n1.id().clone(),
            &[
                // First lookup for known closest nodes
                TestOp::GetNearest(target_id, vec![n2.clone(), n3.clone()]),
                // Then issue FindNodes to these
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
                // UpdatePeers for discovered peers
                TestOp::update_peers(vec![n2.clone(), n3.clone(), n4.clone(), n5.clone()]),
            ],
        );

        // Execute search
        let (r, _i) = c
            .lookup(
                n5.id().clone(),
                SearchOptions {
                    depth: 3,
                    concurrency: 2,
                },
            )
            .await
            .unwrap();

        // Check response is as expected
        assert_eq!(&r, &n5);

        // Finalise test
        c.finalise();
    }
}
