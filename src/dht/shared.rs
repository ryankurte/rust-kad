//! Shared / helpers for high level DHT operations
//!
// https://github.com/ryankurte/rust-kad
// Copyright 2018-2023 ryan kurte

use std::fmt::Debug;
use std::{collections::HashMap, time::Instant};

use tracing::{debug, trace, warn};

use super::Base;
use crate::{common::*, Config};

/// RequestState is used to store the state of pending requests
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum RequestState {
    Pending,
    Active,
    Timeout,
    Complete,
    InvalidResponse,
    Failed,
}

#[derive(Clone, PartialEq, Debug)]
pub struct SearchOptions {
    pub depth: usize,
    pub concurrency: usize,
}

impl From<&Config> for SearchOptions {
    fn from(value: &Config) -> Self {
        Self {
            depth: value.max_recursion,
            concurrency: value.concurrency,
        }
    }
}

impl Default for SearchOptions {
    fn default() -> Self {
        Self {
            depth: 8,
            concurrency: 4,
        }
    }
}

/// Helper to find the nearest reachable node subset for search and store operations
pub(crate) async fn find_nearest<
    C: Base<Id, Info, Data>,
    Id: DatabaseId + Clone + Debug + 'static,
    Info: Clone + Debug + 'static,
    Data,
>(
    ctx: &C,
    id: Id,
    mut nearest: Vec<Entry<Id, Info>>,
    opts: SearchOptions,
) -> Result<Vec<Entry<Id, Info>>, Error> {
    debug!("Using {} nearest nodes", nearest.len());

    let our_id = ctx.our_id();

    // Setup initial node state
    let mut known = HashMap::new();
    nearest.sort_by_key(|n| DatabaseId::xor(n.id(), &id));
    for e in nearest.drain(..) {
        known.insert(e.id().clone(), (e, RequestState::Pending));
    }

    // Issue FindNode requests to nearest nodes up to maximum search depth
    for i in 0..opts.depth {
        // Fetch pending nodes from known list
        let pending: Vec<_> = known
            .iter_mut()
            .filter(|(_id, (_e, s))| *s == RequestState::Pending)
            .collect();

        if pending.len() == 0 {
            debug!("Search round {i}, no pending nodes, exiting search");
            break;
        }

        debug!(
            "Search round {i}, issuing FindNodes request to {} peers",
            pending.len()
        );

        // Search for closer nodes using pending entries

        // TODO: limit to closest subset based on concurrency?

        // Issue FindNode request to pending peers
        let peers = pending.iter().map(|(_, (p, _s))| p.clone()).collect();
        let mut resps = match ctx.net_req(peers, Request::FindNode(id.clone())).await {
            Ok(v) => v,
            Err(e) => {
                warn!("FindNode network request failed: {:?}", e);
                continue;
            }
        };

        // Handle responses
        let mut found = vec![];
        for (peer_id, (_peer_info, state)) in pending {
            match resps.remove(peer_id) {
                Some(resp) => {
                    *state = RequestState::Complete;

                    // Add located nodes to list
                    if let Response::NodesFound(_id, mut nodes) = resp {
                        found.append(&mut nodes);
                    }
                }
                None => {
                    *state = RequestState::Failed;
                }
            }
        }

        debug!("Round {i}, found {} new nodes", found.len());

        // Update known entries with newly found nodes
        for f in found {
            if !known.contains_key(f.id()) && f.id() != &our_id {
                known.insert(f.id().clone(), (f, RequestState::Pending));
            }
        }
    }

    trace!("Known: {known:?}");

    // Extract resolved peers
    let mut resolved: Vec<_> = known
        .iter()
        .filter(|(_id, (e, s))| *s == RequestState::Complete && e.id() != &our_id)
        .map(|(_id, (e, _s))| {
            let mut e = e.clone();
            e.set_seen(Instant::now());
            e
        })
        .collect();
    resolved.sort_by_key(|p| Id::xor(&id, p.id()));

    debug!("Resolved {} peers", resolved.len());

    // Update nodetable with resolved peers
    ctx.update_peers(resolved.clone());

    Ok(resolved)
}
