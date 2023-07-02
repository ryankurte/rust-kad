//! DHT Search Operation
//!
// https://github.com/ryankurte/rust-kad
// Copyright 2018-2023 ryan kurte

use std::fmt::Debug;

use tracing::{debug, instrument, warn};

use super::{find_nearest, Base, SearchInfo, SearchOptions};
use crate::common::*;

pub trait Search<Id, Info, Data> {
    /// Search for DHT stored values at a specific ID
    async fn search(
        &self,
        id: Id,
        opts: SearchOptions,
    ) -> Result<(Vec<Data>, SearchInfo<Id>), Error>;
}

impl<T, Id, Info, Data> Search<Id, Info, Data> for T
where
    T: Base<Id, Info, Data>,
    Id: DatabaseId + Clone + Debug + 'static,
    Info: Clone + Debug + 'static,
{
    /// Search for DHT stored values at a specific ID
    #[instrument(skip_all, fields(our_id=?self.our_id(), target_id=?id))]
    async fn search(
        &self,
        id: Id,
        opts: SearchOptions,
    ) -> Result<(Vec<Data>, SearchInfo<Id>), Error> {
        // Fetch nearest nodes from the table
        let nearest = self.get_nearest(id.clone()).await?;
        if nearest.len() == 0 {
            return Err(Error::NoPeers);
        }

        // Perform DHT lookup for nearest nodes to the search ID
        let (mut resolved, depth) = find_nearest(self, id.clone(), nearest, opts.clone()).await?;

        // Search for values using these nearest nodes

        // limit to closest subset based on concurrency
        let count = resolved.len().min(opts.concurrency);

        debug!("Issuing FindValues request to {} peers", count);

        // Issue FindValues request
        let peers: Vec<_> = resolved[..count].iter().map(|p| p.clone()).collect();

        let mut resps = match self
            .net_req(peers.clone(), Request::FindValue(id.clone()))
            .await
        {
            Ok(v) => v,
            Err(e) => {
                warn!("FindValue request failed: {:?}", e);
                return Err(e.into());
            }
        };

        // Handle responses
        let mut values = vec![];

        let peers: Vec<_> = resolved
            .drain(..)
            .filter_map(|p| {
                match resps.remove(p.id()) {
                    Some(resp) => {
                        // Add located values to list
                        if let Response::ValuesFound(_id, mut v) = resp {
                            values.append(&mut v);
                        }
                        Some(p)
                    }
                    None => None,
                }
            })
            .collect();

        // Filter used nearest nodes
        let nearest: Vec<_> = peers.iter().map(|p| p.id().clone()).collect();

        // Apply reducer to values
        let values = self.reduce(id.clone(), values).await?;

        // Return reduced found values and search info
        Ok((values, SearchInfo { depth, nearest }))
    }
}

#[cfg(test)]
mod tests {
    use simplelog::{Config as LogConfig, LevelFilter, SimpleLogger};

    use super::*;
    use crate::dht::base::tests::{TestCore, TestOp};

    #[tokio::test]
    async fn test_search() {
        let _ = SimpleLogger::init(LevelFilter::Debug, LogConfig::default());

        // Setup nodes
        let n1 = Entry::new([0b1000], 100);
        let n2 = Entry::new([0b0011], 200);
        let n3 = Entry::new([0b0010], 300);
        let n4 = Entry::new([0b1001], 400);
        let n5 = Entry::new([0b1010], 400);

        // Setup value to be stored
        let (value_id, value_data) = ([0b1100], 500);

        // Setup testcore with expectations
        let c = TestCore::new(
            n1.id().clone(),
            &[
                // First lookup for known closest nodes
                TestOp::GetNearest(value_id, vec![n2.clone(), n3.clone()]),
                // Then issue FindNodes to these
                TestOp::net_req(
                    vec![n2.clone(), n3.clone()],
                    Request::FindNode(value_id),
                    &[
                        (
                            n2.id().clone(),
                            Response::NodesFound(value_id, vec![n4.clone()]),
                        ),
                        (
                            n3.id().clone(),
                            Response::NodesFound(value_id, vec![n5.clone()]),
                        ),
                    ],
                ),
                // And another round of FindNodes to the newly discovered nodes
                TestOp::net_req(
                    vec![n4.clone(), n5.clone()],
                    Request::FindNode(value_id),
                    &[
                        (n4.id().clone(), Response::NodesFound(value_id, vec![])),
                        (n5.id().clone(), Response::NodesFound(value_id, vec![])),
                    ],
                ),
                // UpdatePeers for discovered peers
                TestOp::update_peers(vec![n2.clone(), n3.clone(), n4.clone(), n5.clone()]),
                // And finally a FindValues to the nearest set of nodes
                TestOp::net_req(
                    vec![n4.clone(), n5.clone()],
                    Request::FindValue(value_id),
                    &[
                        (
                            n4.id().clone(),
                            Response::ValuesFound(value_id, vec![value_data]),
                        ),
                        (
                            n5.id().clone(),
                            Response::ValuesFound(value_id, vec![value_data]),
                        ),
                    ],
                ),
                TestOp::Reduce(value_id, vec![500, 500]),
            ],
        );

        // Execute search
        let (r, _) = c
            .search(
                value_id,
                SearchOptions {
                    depth: 3,
                    concurrency: 2,
                },
            )
            .await
            .unwrap();

        // Check response is as expected
        assert_eq!(&r, &[value_data]);

        // Finalise test
        c.finalise();
    }
}
