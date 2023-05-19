use std::fmt::Debug;

use tracing::{debug, instrument, warn};

use super::{find_nearest, SearchOptions};
use crate::common::*;

pub trait Store<Id, Info, Data> {
    /// Store DHT data at the provided ID
    async fn store(&self, id: Id, data: Vec<Data>, opts: SearchOptions)
        -> Result<Vec<Entry<Id, Info>>, Error>;
}

impl<T, Id, Info, Data> Store<Id, Info, Data> for T
where
    T: super::Base<Id, Info, Data>,
    Id: DatabaseId + Clone + Debug + 'static,
    Info: Clone + Debug + 'static,
    Data: Clone + Debug + 'static,
{
    /// Store DHT data at the provided ID
    #[instrument(skip_all, fields(our_id=?self.our_id(), target_id=?id))]
    async fn store(
        &self,
        id: Id,
        data: Vec<Data>,
        opts: SearchOptions,
    ) -> Result<Vec<Entry<Id, Info>>, Error> {
        // Write to our own store
        self.store_data(id.clone(), data.clone()).await?;

        // Fetch nearest nodes from the table
        let nearest = self.get_nearest(id.clone()).await?;
        if nearest.len() == 0 {
            return Err(Error::NoPeers);
        }

        // Perform DHT lookup for nearest nodes to the search ID
        let mut resolved = find_nearest(self, id.clone(), nearest, opts.clone()).await?;

        // limit to closest subset based on concurrency
        let count = resolved.len().min(opts.concurrency);

        debug!("Issuing Store request to {} peers", count);

        // Issue StoreValues requests
        let peers = resolved[..count].iter().map(|p| p.clone()).collect();
        let mut resps = match self
            .net_req(peers, Request::Store(id.clone(), data.clone()))
            .await
        {
            Ok(v) => v,
            Err(e) => {
                warn!("Store request failed: {:?}", e);
                return Err(e.into());
            }
        };

        // Handle responses
        let mut values = vec![];

        let peers = resolved.drain(..).filter_map(|p| {
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
        }).collect();

        // Apply reducer to values
        let _values = self.reduce(id.clone(), values).await?;

        // Return reduced stored values
        // TODO: would it be more useful to return store information (number of peers, actual peers?)
        Ok(peers)
    }
}

#[cfg(test)]
mod tests {
    use simplelog::{Config as LogConfig, LevelFilter, SimpleLogger};

    use super::*;
    use crate::dht::base::tests::{TestCore, TestOp};

    #[tokio::test]
    async fn test_store() {
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
                // Before we start, write to our store
                TestOp::Store(value_id, vec![value_data]),
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
                // And finally a FindValues to the nearest set of nodes
                TestOp::net_req(
                    vec![n4.clone(), n5.clone()],
                    Request::Store(value_id, vec![value_data]),
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
        let r = c
            .store(
                value_id,
                vec![value_data],
                SearchOptions {
                    depth: 3,
                    concurrency: 2,
                },
            )
            .await
            .unwrap();

        // Check response is as expected
        assert_eq!(&r, &[n4.clone(), n5.clone()]);

        // Finalise test
        c.finalise();
    }
}
