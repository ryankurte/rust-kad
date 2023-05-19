/**
 * rust-kad
 * Kademlia Node Table Implementation
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */
use std::fmt::Debug;
use std::ops::Range;
use std::time::Instant;

use crate::common::{DatabaseId, Entry};

use super::kbucket::KBucket;
use super::nodetable::{BucketInfo, NodeTable};

/// KNodeTable Implementation
/// This uses a flattened approximation whereby buckets are pre-allocated and indexed by their distance from the local Id
/// as a simplification of the allocation based branching approach introduced in the paper.
//
// TODO: some assumptions here are not ideal, for example, bucket 0 will only ever contain the single entry where target == id.
// and the lower index buckets may not be possible to fill to bucket_size / should probably offset bucket count based on bucket
// size to compensate (eg. if buckets are sized for two entries, reduce distance by 1 bit)
pub struct KNodeTable<Id, Info> {
    id: Id,
    buckets: Vec<KBucket<Id, Info>>,
}

impl<Id, Info> KNodeTable<Id, Info>
where
    Id: DatabaseId + Clone + 'static,
    Info: Clone + Debug + 'static,
{
    /// Create a new KNodeTable with the provided bucket and hash sizes
    // TODO: hash_size approx isn't -quite- right and the use of the wrong hash size results in bucket indexing errors...
    // perhaps this could be abstracted / const generic-ified to make it more resilient?
    pub fn new(id: Id, bucket_size: usize, hash_size: usize) -> KNodeTable<Id, Info> {
        // Create a new bucket and assign own Id
        let buckets = (0..hash_size + 1)
            .map(|_| KBucket::new(bucket_size))
            .collect();
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

        // Count bits of difference
        diff.bits()
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
    type NodeIter<'a> = KNodeIter<'a, Id, Info>;

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
        // TODO: this is exceedingly inefficient, replace with iter and filters.

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
    fn bucket_info(&self, index: usize) -> Option<BucketInfo> {
        if index >= self.buckets.len() {
            return None;
        }

        let b = &self.buckets[index];

        Some(BucketInfo {
            index,
            nodes: b.node_count(),
            updated: b.updated(),
        })
    }

    /// Iterate through entries in the node table
    fn entries<'b>(&'b self) -> Self::NodeIter<'b> {
        KNodeIter {
            ctx: self,
            bucket_index: 0,
            entry_index: 0,
        }
    }
}

pub struct KNodeIter<'a, Id, Info> {
    ctx: &'a KNodeTable<Id, Info>,
    bucket_index: usize,
    entry_index: usize,
}

impl<'a, Id, Info> Iterator for KNodeIter<'a, Id, Info>
where
    Id: DatabaseId + Clone + 'static,
    Info: Clone + Debug + 'static,
{
    type Item = &'a Entry<Id, Info>;

    fn next(&mut self) -> Option<Self::Item> {
        // If we have no buckets, return
        if self.ctx.buckets.len() == 0 {
            return None;
        }

        let mut current = &self.ctx.buckets[self.bucket_index];

        // If we're out of entries in a bucket, look for the next bucket
        if self.entry_index >= current.node_count() {
            // Find the next bucket containing one or more nodes
            let mut next_bucket = 0;
            for i in self.bucket_index + 1..self.ctx.buckets.len() {
                if self.ctx.buckets[i].node_count() > 0 {
                    next_bucket = i;
                    break;
                }
            }

            // If we have no more nodes, return none
            if next_bucket == 0 {
                return None;
            }

            // Update the bucket index
            current = &self.ctx.buckets[next_bucket];
            self.bucket_index = next_bucket;
            self.entry_index = 0;
        }

        // Grab the current entry
        let v = current.node(self.entry_index);

        // Increment entry index
        self.entry_index += 1;

        Some(v)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use super::{KNodeTable, NodeTable};

    #[test]
    fn test_k_node_buckets() {
        let table = KNodeTable::<[u8; 1], u64>::new([0b1000], 10, 4);

        let tests = &[
            // Our ID ends ip in the zero-th bucket
            // (TODO: do we _need_ this bucket at all? should combine with first bucket?)
            ([0b1000], 0),
            // Next is one bit distance
            ([0b1001], 1),
            // Then two bit distance
            ([0b1010], 2),
            ([0b1011], 2),
            // Three bit distance
            ([0b1100], 3),
            ([0b1101], 3),
            ([0b1110], 3),
            ([0b1111], 3),
            // And four bit distance
            ([0b0001], 4),
            ([0b0010], 4),
            ([0b0011], 4),
            ([0b0100], 4),
            ([0b0101], 4),
            ([0b0110], 4),
            ([0b0111], 4),
        ];

        for (id, index) in tests {
            assert_eq!(
                table.bucket_index(&id),
                *index,
                "Expected bucket {index} for id {id:?}"
            );
        }
    }

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

    #[test]
    fn test_k_node_iter() {
        let n = Entry::new([0b0100], 1);

        let mut t = KNodeTable::<[u8; 1], u64>::new(*n.id(), 2, 4);

        let nodes = vec![
            Entry::new([0b0000], 1),
            Entry::new([0b0001], 2),
            Entry::new([0b0110], 3),
            Entry::new([0b1011], 4),
        ];

        // Add some nodes
        for (i, n) in nodes.iter().enumerate() {
            assert!(t.create_or_update(n));

            let mut n1: Vec<_> = t.entries().map(|e| e.clone()).collect();
            n1.sort_by_key(|e| e.id().clone());
            assert_eq!(&n1, &nodes[..i + 1]);
        }
    }
}
