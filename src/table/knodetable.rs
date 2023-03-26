use std::fmt::Debug;
/**
 * rust-kad
 * Kademlia Node Table Implementation
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */
use std::ops::Range;
use std::time::Instant;

use crate::common::{DatabaseId, Entry};

use super::kbucket::KBucket;
use super::nodetable::{BucketInfo, NodeTable};

/// KNodeTable Implementation
/// This uses a flattened approach whereby buckets are pre-allocated and indexed by their distance from the local Id
/// as a simplification of the allocation based branching approach introduced in the paper.
pub struct KNodeTable<Id, Info> {
    id: Id,
    buckets: Vec<KBucket<Id, Info>>,
}

impl<Id, Info> KNodeTable<Id, Info>
where
    Id: DatabaseId + Clone + 'static,
    Info: Clone + Debug + 'static,
{
    /// Create a new KNodeTable with the provded bucket and hash sizes
    pub fn new(id: Id, bucket_size: usize, hash_size: usize) -> KNodeTable<Id, Info> {
        // Create a new bucket and assign own Id
        let buckets = (0..hash_size).map(|_| KBucket::new(bucket_size)).collect();
        // Generate KNodeTable object
        KNodeTable { id, buckets }
    }

    // Calculate the distance between two Ids.
    fn distance(a: &Id, b: &Id) -> Id {
        Id::xor(a, b)
    }

    /// Fetch a reference to the bucket containing the provided Id
    fn bucket(&self, id: &Id) -> &KBucket<Id, Info> {
        let index = self.bucket_index(id);
        &self.buckets[index]
    }

    fn bucket_mut(&mut self, id: &Id) -> &mut KBucket<Id, Info> {
        let index = self.bucket_index(id);
        &mut self.buckets[index]
    }

    /// Fetch the bucket index for a given Id
    /// This is basically just the number of bits in the distance between the local and target Ids
    fn bucket_index(&self, id: &Id) -> usize {
        // Find difference
        let diff = KNodeTable::<Id, Info>::distance(&self.id, id);
        assert!(!diff.is_zero(), "Distance cannot be zero");

        diff.bits() - 1
    }

    #[allow(dead_code)]
    fn update_buckets(&self) {
        unimplemented!()
    }
}

impl<Id, Info> NodeTable<Id, Info> for KNodeTable<Id, Info>
where
    Id: DatabaseId + Clone + 'static,
    Info: Clone + Debug + 'static,
{
    fn buckets(&self) -> usize {
        self.buckets.len()
    }

    /// Create or update a node in the NodeTable
    fn create_or_update(&mut self, node: &Entry<Id, Info>) -> bool {
        if node.id() == &self.id {
            return false;
        }

        let bucket = self.bucket_mut(node.id());
        let mut node = node.clone();
        node.set_seen(Instant::now());
        bucket.create_or_update(&node)
    }

    /// Find the nearest nodes to the provided Id in the given range
    fn nearest(&self, id: &Id, range: Range<usize>) -> Vec<Entry<Id, Info>> {
        // Create a list of all nodes
        let mut all: Vec<_> = self
            .buckets
            .iter()
            .flat_map(|b| b.nodes())
            //.filter(|n| n.id() != &self.id )
            .collect();
        let count = all.len();

        // Sort by distance
        all.sort_by_key(|n| KNodeTable::<Id, Info>::distance(id, n.id()));

        // Limit to count or total found
        let mut range = range;
        range.end = usize::min(count, range.end);

        let limited = all.drain(range).collect();
        limited
    }

    /// Check if the node NodeTable contains a given node by Id
    /// This returns the node object if found
    fn contains(&self, id: &Id) -> Option<Entry<Id, Info>> {
        let bucket = self.bucket(id);
        bucket.find(id)
    }

    /// Fetch the oldest node in the specified bucket
    fn oldest(&self, index: usize) -> Option<Entry<Id, Info>> {
        self.buckets[index].oldest()
    }

    /// Update an entry by ID
    fn update_entry<F>(&mut self, id: &Id, f: F) -> bool
    where
        F: Fn(&mut Entry<Id, Info>),
    {
        let bucket = self.bucket_mut(id);
        bucket.update_entry(id, f)
    }

    /// Remove an entry by ID
    fn remove_entry(&mut self, id: &Id) {
        let bucket = self.bucket_mut(id);
        bucket.remove_entry(id, false);
    }

    /// Fetch information from each bucket
    fn bucket_info(&self) -> Vec<BucketInfo> {
        let mut info = Vec::with_capacity(self.buckets.len());

        for i in 0..self.buckets.len() {
            let b = &self.buckets[i];
            info.push(BucketInfo {
                index: i,
                nodes: b.node_count(),
                updated: b.updated(),
            });
        }
        info
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use super::{KNodeTable, NodeTable};

    #[test]
    fn test_k_node_table() {
        let n = Entry::new([0b0100], 1);

        let mut t = KNodeTable::<[u8; 1], u64>::new(*n.id(), 10, 4);

        let nodes = vec![
            Entry::new([0b0000], 1),
            Entry::new([0b0001], 2),
            Entry::new([0b0110], 3),
            Entry::new([0b1011], 4),
        ];

        // Add some nodes
        for n in &nodes {
            assert!(t.contains(n.id()).is_none());
            assert!(t.create_or_update(n));
            assert_eq!(*n, t.contains(n.id()).unwrap());
        }

        // Find closest nodes
        assert_eq!(
            vec![nodes[2].clone(), nodes[0].clone()],
            t.nearest(n.id(), 0..2)
        );
        assert_eq!(
            vec![nodes[0].clone(), nodes[1].clone()],
            t.nearest(&[0b0010], 0..2)
        );
    }
}
